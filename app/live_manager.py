"""Quản lý chạy chiến lược MA live và cung cấp trạng thái cho dashboard."""

from __future__ import annotations

import asyncio
import os
from collections import deque
from dataclasses import dataclass
from datetime import datetime, timezone
import contextlib
import logging
from typing import Any, Deque, Dict, List, Optional, TYPE_CHECKING

from pydantic import BaseModel, Field

from .commands.live_ma import run_live_strategy
from .storage import Storage

if TYPE_CHECKING:  # pragma: no cover
    from .quote_service import QuoteService


@dataclass
class _RuntimeState:
    running: bool = False
    started_at: Optional[datetime] = None
    config: Optional[Dict[str, Any]] = None
    last_quote: Optional[float] = None
    last_quote_time: Optional[datetime] = None
    current_position: Optional[Dict[str, Any]] = None
    cumulative_pnl: float = 0.0
    last_signal: Optional[Dict[str, Any]] = None
    waiting_reason: Optional[str] = None
    cli_command: Optional[str] = None


class LiveStartRequest(BaseModel):
    db_url: Optional[str] = Field(default=None, description="Chuỗi kết nối DB async")
    symbol: Optional[str] = None
    fast: int = 21
    slow: int = 89
    ma_type: str = "ema"
    timeframe: str = "1min"
    trend: int = 200
    spread_atr_max: float = 0.2
    reverse_exit: bool = False
    market_state_window: int = 20
    capital: float = 100.0
    risk_pct: float = 0.02
    contract_size: float = 100.0
    sl_atr: float = 2.0
    tp_atr: float = 3.0
    trail_trigger_atr: float = 1.0
    trail_atr_mult: float = 1.0
    sl_pips: Optional[float] = None
    tp_pips: Optional[float] = None
    pip_size: float = 0.01
    momentum_type: str = "macd"
    momentum_window: int = 14
    momentum_threshold: float = 0.1
    macd_fast: int = 12
    macd_slow: int = 26
    macd_signal: int = 9
    macd_threshold: float = 0.0
    rsi_threshold_long: float = 60.0
    rsi_threshold_short: float = 40.0
    range_lookback: int = 40
    range_min_atr: float = 0.8
    range_min_points: float = 0.5
    breakout_buffer_atr: float = 0.5
    breakout_confirmation_bars: int = 2
    atr_baseline_window: int = 14
    atr_multiplier_min: float = 0.8
    atr_multiplier_max: float = 4.0
    trading_hours: Optional[str] = None
    adx_window: int = 14
    adx_threshold: float = 0.0
    poll: float = 1.0
    live: bool = False
    ensure_history_hours: float = 0.0
    history_batch: int = 2000
    history_max_days: int = 1
    ingest_live_db: bool = True
    max_daily_loss: Optional[float] = None
    max_loss_streak: Optional[int] = None
    max_losses_per_session: Optional[int] = None
    cooldown_minutes: Optional[int] = None
    allow_buy: bool = True
    allow_sell: bool = True
    max_holding_minutes: Optional[int] = None


class LiveStatus(BaseModel):
    running: bool
    started_at: Optional[datetime]
    last_quote: Optional[float]
    last_quote_time: Optional[datetime]
    current_position: Optional[Dict[str, Any]]
    cumulative_pnl: float
    config: Optional[Dict[str, Any]]
    last_signal: Optional[Dict[str, Any]]
    waiting_reason: Optional[str]
    cli_command: Optional[str]


logger = logging.getLogger(__name__)


class LiveStrategyManager:
    def __init__(self, default_symbol: str) -> None:
        self._task: Optional[asyncio.Task] = None
        self._state = _RuntimeState()
        self._events: Deque[Dict[str, Any]] = deque(maxlen=200)
        self._lock = asyncio.Lock()
        self._default_symbol = default_symbol

    async def start(self, request: LiveStartRequest, quote_service: Optional[QuoteService] = None) -> None:
        async with self._lock:
            if self._task:
                raise RuntimeError("Chiến lược đang chạy")
            db_url = request.db_url or os.getenv("DATABASE_URL")
            if not db_url:
                raise RuntimeError("Thiếu db_url. Truyền db_url hoặc đặt biến DATABASE_URL")
            config_data = request.model_dump()
            config_data["db_url"] = db_url
            if not config_data.get("symbol"):
                config_data["symbol"] = self._default_symbol
            start_time = datetime.now(timezone.utc)
            sanitized_config = {k: v for k, v in config_data.items() if k != "db_url"}
            await self._record_run_config(db_url, start_time, sanitized_config)
            self._state = _RuntimeState(
                running=True,
                started_at=start_time,
                config=sanitized_config,
                cumulative_pnl=0.0,
                waiting_reason="Đang khởi động",
                cli_command=self._build_cli_command(config_data),
            )
            self._events.clear()
            self._task = asyncio.create_task(self._run(config_data, quote_service))

    async def stop(self) -> None:
        async with self._lock:
            task = self._task
            self._task = None
            self._state.running = False
        if task:
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await task

    async def _run(self, config: Dict[str, Any], quote_service: Optional[QuoteService]) -> None:
        async def _event_handler(event: Dict[str, Any]) -> None:
            await self._handle_event(event)

        try:
            await run_live_strategy(event_handler=_event_handler, quote_service=quote_service, **config)
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # pragma: no cover - runtime errors
            await self._handle_event(
                {
                    "type": "error",
                    "timestamp": datetime.now(timezone.utc),
                    "message": str(exc),
                }
            )
        finally:
            async with self._lock:
                self._task = None
                self._state.running = False

    async def get_status(self) -> LiveStatus:
        async with self._lock:
            # Nếu quá lâu không có quote, gán waiting_reason gợi ý thị trường đóng hoặc mất nguồn giá
            stale_seconds = None
            if self._state.last_quote_time:
                stale_seconds = (datetime.now(timezone.utc) - self._state.last_quote_time).total_seconds()
            if stale_seconds is not None and stale_seconds > 300 and self._state.running:
                self._state.waiting_reason = "Không có quote mới (có thể thị trường đang đóng hoặc mất kết nối MT5)"

            events = list(self._events)
            last_signal = self._state.last_signal or next(
                (evt for evt in events if evt.get("type") in {"position_open", "position_close"}),
                None,
            )
            return LiveStatus(
                running=self._state.running,
                started_at=self._state.started_at,
                last_quote=self._state.last_quote,
                last_quote_time=self._state.last_quote_time,
                current_position=self._state.current_position,
                cumulative_pnl=self._state.cumulative_pnl,
                config=self._state.config,
                last_signal=last_signal,
                waiting_reason=self._state.waiting_reason,
                cli_command=self._state.cli_command,
            )

    async def _handle_event(self, event: Dict[str, Any]) -> None:
        timestamp = event.get("timestamp")
        if isinstance(timestamp, str):
            try:
                timestamp = datetime.fromisoformat(timestamp)
            except ValueError:
                timestamp = datetime.now(timezone.utc)
        if timestamp is None:
            timestamp = datetime.now(timezone.utc)
        event["timestamp"] = timestamp

        async with self._lock:
            event_type = event.get("type")
            if event_type == "quote":
                price = event.get("price") or event.get("bid")
                if price is not None:
                    self._state.last_quote = float(price)
                self._state.last_quote_time = timestamp
            elif event_type == "position_open":
                self._state.current_position = {
                    "side": event.get("side"),
                    "price": event.get("price"),
                    "volume": event.get("volume"),
                    "stop_loss": event.get("stop_loss"),
                    "take_profit": event.get("take_profit"),
                    "timestamp": timestamp,
                }
                self._state.last_signal = event
                self._state.waiting_reason = "Đang quản lý vị thế"
            elif event_type == "position_close":
                pnl_value = float(event.get("pnl_value") or 0.0)
                self._state.cumulative_pnl += pnl_value
                self._state.current_position = None
                self._state.last_signal = event
                self._state.waiting_reason = "Đang chờ tín hiệu mới"
            elif event_type == "error":
                # đánh dấu trạng thái lỗi
                self._state.running = False
                self._state.last_signal = event
                self._state.waiting_reason = event.get("message") or "Đã dừng vì lỗi"
            elif event_type == "status":
                self._state.waiting_reason = event.get("reason")
                self._state.last_signal = event

            self._events.appendleft(event)

    def _build_cli_command(self, config: Dict[str, Any]) -> str:
        parts = ["python", "-m", "app.cli", "run-live-ma"]
        for key, value in config.items():
            if key in {"db_url"}:
                parts.extend(["--db-url", str(value)])
            elif isinstance(value, bool):
                flag = f"--{key.replace('_', '-')}"
                # allow_buy/allow_sell luôn xuất hiện (0/1) để không default về True
                if key in {"allow_buy", "allow_sell"}:
                    parts.extend([flag, "1" if value else "0"])
                elif value:
                    parts.append(flag)
            else:
                if value is None:
                    continue
                flag = f"--{key.replace('_', '-')}"
                parts.extend([flag, str(value)])
        return " ".join(parts)

    async def _record_run_config(self, db_url: str, started_at: datetime, config: Dict[str, Any]) -> None:
        """Persist dashboard start parameters for later auditing/export."""
        storage = Storage(db_url)
        try:
            await storage.init()
            await storage.insert_run_config(started_at, config)
        except Exception as exc:  # pragma: no cover - logging only
            logger.warning("Không thể lưu cấu hình run: %s", exc)
        finally:
            await storage.close()
