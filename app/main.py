import logging
import asyncio
from datetime import datetime, timezone, timedelta
from typing import Optional, Dict, Any

from sqlalchemy import select

from fastapi import FastAPI, HTTPException, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse
from pydantic import BaseModel

from .config import get_settings
from .models import HealthStatus, Quote
from .quote_service import QuoteService, QuoteCache, QuotePoller, WebSocketManager
from .live_manager import LiveStrategyManager, LiveStartRequest
from .commands.backtest_ma import run_backtest as run_backtest_ma
from .commands import history as history_cmd
from .storage import Storage, saved_backtests_table


class BacktestRequest(BaseModel):
    db_url: str
    symbol: str
    start: str
    end: str
    fast: int = 8
    slow: int = 21
    ma_type: str = "ema"
    timeframe: str = "5min"
    trend: int = 200
    risk_pct: float = 0.02
    capital: float = 10000.0
    trail_trigger_atr: float = 1.8
    trail_atr_mult: float = 1.1
    spread_atr_max: float = 0.08
    reverse_exit: bool = False
    market_state_window: int = 40
    sl_atr: float = 1.5
    tp_atr: float = 2.5
    volume: float = 0.1
    contract_size: float = 100.0
    sl_pips: Optional[float] = None
    tp_pips: Optional[float] = None
    pip_size: float = 0.01
    size_from_risk: bool = False
    momentum_type: str = "hybrid"
    momentum_window: int = 14
    momentum_threshold: float = 0.07
    macd_fast: int = 12
    macd_slow: int = 26
    macd_signal: int = 9
    macd_threshold: float = 0.0002
    range_lookback: int = 40
    range_min_atr: float = 0.8
    range_min_points: float = 1.0
    breakout_buffer_atr: float = 0.3
    breakout_confirmation_bars: int = 1
    atr_baseline_window: int = 14
    atr_multiplier_min: float = 1.1
    atr_multiplier_max: float = 3.2
    trading_hours: Optional[str] = None
    adx_window: int = 14
    adx_threshold: float = 25.0
    rsi_threshold_long: float = 60.0
    rsi_threshold_short: float = 40.0
    max_daily_loss: Optional[float] = None
    max_loss_streak: Optional[int] = None
    max_losses_per_session: Optional[int] = None
    cooldown_minutes: Optional[int] = None
    allow_buy: bool = True
    allow_sell: bool = True
    max_holding_minutes: Optional[int] = None
    order_retry_times: int = 0
    order_retry_delay_ms: int = 0
    # Safety/entry controls
    safety_entry_atr_mult: float = 0.1
    spread_samples: int = 5
    spread_sample_delay_ms: int = 8
    allowed_deviation_points: int = 300
    volatility_spike_atr_mult: float = 0.8
    spike_delay_ms: int = 50
    skip_reset_window: bool = True
    latency_min_ms: int = 200
    latency_max_ms: int = 400
    slippage_usd: float = 0.05
    order_reject_prob: float = 0.03
    base_spread_points: int = 50
    spread_spike_chance: float = 0.02
    spread_spike_min_points: int = 80
    spread_spike_max_points: int = 300
    slip_per_atr_ratio: float = 0.2
    requote_prob: float = 0.01
    offquotes_prob: float = 0.005
    timeout_prob: float = 0.005
    stop_hunt_chance: float = 0.015
    stop_hunt_min_atr_ratio: float = 0.2
    stop_hunt_max_atr_ratio: float = 1.0
    missing_tick_chance: float = 0.005


class FetchHistoryRequest(BaseModel):
    db_url: str
    symbol: str
    start: str
    end: str
    batch: int = 2000
    max_days: int = 1


class SaveBacktestRequest(BaseModel):
    db_url: str
    config: Dict[str, Any]
    summary: Optional[Dict[str, Any]] = None
    note: Optional[str] = None


class BacktestDashboardRequest(BaseModel):
    db_url: str
    symbol: str
    start: str
    end: str
    saved_config_id: int


logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(name)s | %(message)s")
logger = logging.getLogger(__name__)

app = FastAPI(
    title="Dịch vụ quote realtime XAUUSD",
    version="0.2.0",
    description="Dịch vụ phát giá XAUUSD thời gian thực từ MetaTrader5 cục bộ qua REST và WebSocket.",
)

settings = get_settings()
cache = QuoteCache()
ws_manager = WebSocketManager()
live_manager = LiveStrategyManager(settings.quote_symbol)

# Quản lý cancel backtest phía server
backtest_cancel_event: Optional[asyncio.Event] = None

quote_provider: Optional[QuoteService] = None
quote_poller: Optional[QuotePoller] = None


@app.on_event("startup")
async def _startup() -> None:
    global quote_provider, quote_poller

    try:
        quote_provider = QuoteService(settings)

        quote_poller = QuotePoller(
            provider=quote_provider,
            cache=cache,
            ws_manager=ws_manager,
            interval_seconds=settings.poll_interval_seconds,
        )

        await quote_poller.start()
        logger.info("Quote service started. Poll interval: %ss", settings.poll_interval_seconds)
    except Exception:
        # Ghi đầy đủ stacktrace để chẩn đoán lỗi khởi tạo MT5 hoặc chọn symbol
        logger.exception("Không thể khởi tạo provider hoặc khởi động poller trong lúc startup")
        # Re-raise để uvicorn hiển thị lỗi và tránh ứng dụng ở trạng thái bán khởi tạo
        raise


@app.on_event("shutdown")
async def _shutdown() -> None:
    if quote_poller:
        await quote_poller.stop()
    if quote_provider:
        await quote_provider.aclose()
    await live_manager.stop()
    logger.info("Quote service stopped.")


@app.get("/quotes/xauusd", response_model=Quote, summary="Lấy quote XAU/USD mới nhất")
async def get_latest_quote() -> Quote:
    quote = await cache.get()
    if not quote:
        raise HTTPException(status_code=503, detail="Chưa lấy được dữ liệu từ upstream.")
    return quote


@app.get("/healthz", response_model=HealthStatus, summary="Kiểm tra trạng thái")
async def health_check() -> HealthStatus:
    quote = await cache.get()
    return HealthStatus(status="ok" if quote else "initializing", last_quote_timestamp=quote.updated_at if quote else None)


@app.websocket("/ws/xauusd")
async def xauusd_stream(websocket: WebSocket) -> None:
    await ws_manager.connect(websocket)
    try:
        latest = await cache.get()
        if latest:
            await websocket.send_json(latest.model_dump())
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        logger.info("WebSocket client disconnected.")
    finally:
        await ws_manager.disconnect(websocket)


@app.get("/dashboard", response_class=HTMLResponse)
async def dashboard() -> HTMLResponse:
    return HTMLResponse(DASHBOARD_HTML)


@app.get("/dashboard/backtest", response_class=HTMLResponse)
async def backtest_dashboard_page() -> HTMLResponse:
    return HTMLResponse(BACKTEST_DASHBOARD_HTML)


@app.get("/api/live/status")
async def get_live_status() -> dict:
    status = await live_manager.get_status()
    return status.model_dump()


@app.post("/api/live/start")
async def start_live_strategy(payload: LiveStartRequest) -> dict:
    try:
        await live_manager.start(payload, quote_provider)
    except RuntimeError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return {"status": "started"}


@app.post("/api/live/stop")
async def stop_live_strategy() -> dict:
    await live_manager.stop()
    return {"status": "stopped"}



@app.post("/api/backtest/run")
async def run_backtest_endpoint(payload: BacktestRequest) -> dict:
    global backtest_cancel_event
    backtest_cancel_event = asyncio.Event()
    # Auto-save cấu hình trước khi backtest để gắn saved_config_id vào kết quả/trades
    saved_id: Optional[int] = None
    storage = Storage(payload.db_url)
    await storage.init()
    try:
        saved_id = await storage.insert_saved_backtest(config=payload.model_dump())
    except Exception as exc:
        logger.warning("Không thể auto-save cấu hình backtest: %s", exc)
    finally:
        try:
            await storage.close()
        except Exception:
            pass

    try:
        summary = await run_backtest_ma(
            db_url=payload.db_url,
            symbol=payload.symbol,
            start_str=payload.start,
            end_str=payload.end,
            saved_config_id=saved_id,
            cancel_event=backtest_cancel_event,
            fast=payload.fast,
            slow=payload.slow,
            timeframe=payload.timeframe,
            ma_type=payload.ma_type,
            trend=payload.trend,
            risk_pct=payload.risk_pct,
            capital=payload.capital,
            trail_trigger_atr=payload.trail_trigger_atr,
            trail_atr_mult=payload.trail_atr_mult,
            spread_atr_max=payload.spread_atr_max,
            reverse_exit=payload.reverse_exit,
            market_state_window=payload.market_state_window,
            sl_atr=payload.sl_atr,
            tp_atr=payload.tp_atr,
            volume=payload.volume,
            contract_size=payload.contract_size,
            sl_pips=payload.sl_pips,
            tp_pips=payload.tp_pips,
            pip_size=payload.pip_size,
            size_from_risk=payload.size_from_risk,
            momentum_type=payload.momentum_type,
            momentum_window=payload.momentum_window,
            momentum_threshold=payload.momentum_threshold,
            macd_fast=payload.macd_fast,
            macd_slow=payload.macd_slow,
            macd_signal=payload.macd_signal,
            macd_threshold=payload.macd_threshold,
            range_lookback=payload.range_lookback,
            range_min_atr=payload.range_min_atr,
            range_min_points=payload.range_min_points,
            breakout_buffer_atr=payload.breakout_buffer_atr,
            breakout_confirmation_bars=payload.breakout_confirmation_bars,
            atr_baseline_window=payload.atr_baseline_window,
            atr_multiplier_min=payload.atr_multiplier_min,
            atr_multiplier_max=payload.atr_multiplier_max,
            trading_hours=payload.trading_hours,
            adx_window=payload.adx_window,
            adx_threshold=payload.adx_threshold,
            rsi_threshold_long=payload.rsi_threshold_long,
            rsi_threshold_short=payload.rsi_threshold_short,
            max_daily_loss=payload.max_daily_loss,
            max_loss_streak=payload.max_loss_streak,
            max_losses_per_session=payload.max_losses_per_session,
            cooldown_minutes=payload.cooldown_minutes,
            allow_buy=payload.allow_buy,
            allow_sell=payload.allow_sell,
            order_retry_times=payload.order_retry_times,
            order_retry_delay_ms=payload.order_retry_delay_ms,
            safety_entry_atr_mult=payload.safety_entry_atr_mult,
            spread_samples=payload.spread_samples,
            spread_sample_delay_ms=payload.spread_sample_delay_ms,
            allowed_deviation_points=payload.allowed_deviation_points,
            volatility_spike_atr_mult=payload.volatility_spike_atr_mult,
            spike_delay_ms=payload.spike_delay_ms,
            skip_reset_window=payload.skip_reset_window,
            latency_min_ms=payload.latency_min_ms,
            latency_max_ms=payload.latency_max_ms,
            slippage_usd=payload.slippage_usd,
            order_reject_prob=payload.order_reject_prob,
            base_spread_points=payload.base_spread_points,
            spread_spike_chance=payload.spread_spike_chance,
            spread_spike_min_points=payload.spread_spike_min_points,
            spread_spike_max_points=payload.spread_spike_max_points,
            slip_per_atr_ratio=payload.slip_per_atr_ratio,
            requote_prob=payload.requote_prob,
            offquotes_prob=payload.offquotes_prob,
            timeout_prob=payload.timeout_prob,
            stop_hunt_chance=payload.stop_hunt_chance,
            stop_hunt_min_atr_ratio=payload.stop_hunt_min_atr_ratio,
            stop_hunt_max_atr_ratio=payload.stop_hunt_max_atr_ratio,
            missing_tick_chance=payload.missing_tick_chance,
            return_summary=True,
        )
    except asyncio.CancelledError:
        raise HTTPException(status_code=499, detail="Backtest bị hủy")
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    finally:
        backtest_cancel_event = None
    if summary is None:
        summary = {}
    if saved_id is not None:
        summary["saved_config_id"] = saved_id
    summary["cli_command"] = _build_backtest_cli(payload)
    return summary


@app.post("/api/backtest/save")
async def save_backtest_endpoint(payload: SaveBacktestRequest) -> dict:
    storage = Storage(payload.db_url)
    await storage.init()
    try:
        saved_id = await storage.insert_saved_backtest(
            config=payload.config,
            note=payload.note,
        )
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    finally:
        await storage.close()
    return {"status": "saved", "id": saved_id}


@app.post("/api/backtest/dashboard")
async def backtest_dashboard_endpoint(payload: BacktestDashboardRequest) -> dict:
    start = _parse_iso_dt(payload.start)
    end = _parse_iso_dt(payload.end)
    storage = Storage(payload.db_url)
    await storage.init()
    try:
        trades = await storage.fetch_backtest_trades(
            symbol=payload.symbol,
            start_dt=start,
            end_dt=end,
            saved_config_id=payload.saved_config_id,
        )
    except Exception as exc:
        await storage.close()
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    await storage.close()
    return _build_backtest_dashboard(trades)


@app.post("/api/backtest/cancel")
async def cancel_backtest_endpoint() -> dict:
    global backtest_cancel_event
    if backtest_cancel_event:
        backtest_cancel_event.set()
        return {"status": "cancelling"}
    return {"status": "idle"}


@app.get("/api/backtest/saved-list")
async def list_saved_configs(db_url: str, limit: int = 50) -> dict:
    storage = Storage(db_url)
    await storage.init()
    limit_val = max(1, min(limit, 200))
    async with storage.engine.connect() as conn:
        result = await conn.execute(
            select(
                saved_backtests_table.c.id,
                saved_backtests_table.c.symbol,
                saved_backtests_table.c.preset,
                saved_backtests_table.c.timeframe,
                saved_backtests_table.c.fast,
                saved_backtests_table.c.slow,
                saved_backtests_table.c.created_at,
                saved_backtests_table.c.last_run_at,
            )
            .order_by(saved_backtests_table.c.id.desc())
            .limit(limit_val)
        )
        rows = result.fetchall()
    await storage.close()
    return {
        "items": [
            {
                "id": int(row.id),
                "symbol": row.symbol,
                "preset": row.preset,
                "timeframe": row.timeframe,
                "fast": row.fast,
                "slow": row.slow,
                "created_at": row.created_at,
                "last_run_at": row.last_run_at,
            }
            for row in rows
        ]
    }


@app.get("/api/backtest/config/{config_id}")
async def get_saved_config(config_id: int, db_url: str) -> dict:
    storage = Storage(db_url)
    await storage.init()
    async with storage.engine.connect() as conn:
        result_cfg = await conn.execute(select(saved_backtests_table).where(saved_backtests_table.c.id == config_id))
        cfg_row = result_cfg.first()
    await storage.close()
    if not cfg_row:
        raise HTTPException(status_code=404, detail="Không tìm thấy cấu hình")

    cfg_dict = dict(cfg_row._mapping)
    meta_keys = {
        "id",
        "created_at",
        "last_run_at",
        "note",
        "config_hash",
    }
    config_only = {k: v for k, v in cfg_dict.items() if k not in meta_keys}
    return {"id": int(config_id), "config": config_only}


@app.post("/api/history/fetch")
async def fetch_history_endpoint(payload: FetchHistoryRequest) -> dict:
    try:
        start = _parse_iso_dt(payload.start)
        end = _parse_iso_dt(payload.end)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=f"Thời gian không hợp lệ: {exc}") from exc

    try:
        await history_cmd.fetch_history(
            symbol=payload.symbol,
            start=start,
            end=end,
            db_url=payload.db_url,
            batch=payload.batch,
            max_days=payload.max_days,
        )
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return {
        "status": "fetched",
        "symbol": payload.symbol,
        "start": start.isoformat(),
        "end": end.isoformat(),
        "batch": payload.batch,
        "max_days": payload.max_days,
    }


def _build_backtest_cli(payload: BacktestRequest) -> str:
    parts = ["python", "-m", "app.cli", "backtest-ma"]
    data = payload.model_dump()
    bool_fields = {"reverse_exit", "size_from_risk", "skip_reset_window"}
    for key, value in data.items():
        flag = f"--{key.replace('_', '-')}"
        if value is None:
            continue
        if key in {"allow_buy", "allow_sell"}:
            parts.extend([flag, "1" if bool(value) else "0"])
            continue
        if key in bool_fields:
            if value:
                parts.append(flag)
            continue
        parts.extend([flag, str(value)])
    return " ".join(parts)


def _parse_iso_dt(value: str) -> datetime:
    cleaned = value.strip()
    if cleaned.upper().endswith("Z"):
        cleaned = cleaned[:-1] + "+00:00"
    dt = datetime.fromisoformat(cleaned)
    return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)


def _build_backtest_dashboard(trades: list[dict[str, Any]]) -> dict:
    if not trades:
        return {
            "total_trades": 0,
            "wins": 0,
            "losses": 0,
            "win_rate": 0.0,
            "pnl": 0.0,
            "usd_pnl": 0.0,
            "equity_curve": [],
            "daily": [],
            "by_side": [],
            "hourly": [],
            "tp_sl": [],
            "min_run_start": None,
            "max_run_end": None,
        }

    total_pnl = 0.0
    total_usd = 0.0
    wins = 0
    losses = 0
    equity = 0.0
    equity_curve = []
    daily: dict[str, dict[str, Any]] = {}
    side_map: dict[str, dict[str, Any]] = {}
    hour_counts = {h: 0 for h in range(24)}
    tp_sl_map: dict[str, dict[str, Any]] = {}
    run_start_min: Optional[datetime] = None
    run_end_max: Optional[datetime] = None

    for tr in trades:
        pnl_raw = float(tr.get("pnl") or 0.0)
        usd_val = float(tr.get("usd_pnl") or pnl_raw)
        total_pnl += pnl_raw
        total_usd += usd_val
        if pnl_raw > 0:
            wins += 1
        else:
            losses += 1

        exit_time = tr.get("exit_time")
        if isinstance(exit_time, str):
            exit_dt = _parse_iso_dt(exit_time)
        else:
            exit_dt = exit_time
        if exit_dt is None:
            continue
        exit_dt = exit_dt if exit_dt.tzinfo else exit_dt.replace(tzinfo=timezone.utc)
        equity += usd_val
        equity_curve.append({"time": exit_dt.isoformat(), "equity": equity})

        date_key = exit_dt.date().isoformat()
        bucket = daily.setdefault(
            date_key, {"date": date_key, "pnl": 0.0, "usd_pnl": 0.0, "trades": 0}
        )
        bucket["pnl"] += pnl_raw
        bucket["usd_pnl"] += usd_val
        bucket["trades"] += 1

        side = (tr.get("side") or "").lower() or "unknown"
        side_bucket = side_map.setdefault(
            side,
            {"side": side, "trades": 0, "usd_pnl": 0.0, "losses": 0, "loss_usd_pnl": 0.0, "profit_usd_pnl": 0.0},
        )
        side_bucket["trades"] += 1
        side_bucket["usd_pnl"] += usd_val
        if pnl_raw <= 0:
            side_bucket["losses"] += 1
            # tích lũy giá trị âm (giữ âm) để hiển thị phần lỗ
            side_bucket["loss_usd_pnl"] += usd_val
        else:
            side_bucket["profit_usd_pnl"] += usd_val

        hour_counts[exit_dt.hour] = hour_counts.get(exit_dt.hour, 0) + 1

        # Phân loại TP/SL/khác
        exit_price = tr.get("exit_price")
        stop_loss = tr.get("stop_loss")
        take_profit = tr.get("take_profit")
        hit = "other"
        try:
            if side == "buy":
                if take_profit is not None and exit_price is not None and float(exit_price) >= float(take_profit):
                    hit = "tp"
                elif stop_loss is not None and exit_price is not None and float(exit_price) <= float(stop_loss):
                    hit = "sl"
            elif side == "sell":
                if take_profit is not None and exit_price is not None and float(exit_price) <= float(take_profit):
                    hit = "tp"
                elif stop_loss is not None and exit_price is not None and float(exit_price) >= float(stop_loss):
                    hit = "sl"
        except Exception:
            hit = "other"
        tp_bucket = tp_sl_map.setdefault(side, {"side": side, "tp": 0, "sl": 0, "other": 0})
        tp_bucket[hit] = tp_bucket.get(hit, 0) + 1

        # Ghi nhận min run_start / max run_end
        rs = tr.get("run_start")
        re = tr.get("run_end")
        if isinstance(rs, str):
            try:
                rs_dt = _parse_iso_dt(rs)
            except Exception:
                rs_dt = None
        else:
            rs_dt = rs
        if isinstance(re, str):
            try:
                re_dt = _parse_iso_dt(re)
            except Exception:
                re_dt = None
        else:
            re_dt = re
        if rs_dt:
            run_start_min = rs_dt if run_start_min is None or rs_dt < run_start_min else run_start_min
        if re_dt:
            run_end_max = re_dt if run_end_max is None or re_dt > run_end_max else run_end_max

    total = wins + losses
    return {
        "total_trades": total,
        "wins": wins,
        "losses": losses,
        "win_rate": (wins / total * 100) if total else 0.0,
        "pnl": total_pnl,
        "usd_pnl": total_usd,
        "equity_curve": equity_curve,
        "daily": sorted(daily.values(), key=lambda x: x["date"]),
        "by_side": list(side_map.values()),
        "hourly": [{"hour": h, "trades": c} for h, c in sorted(hour_counts.items())],
        "tp_sl": list(tp_sl_map.values()),
        "min_run_start": run_start_min.isoformat() if run_start_min else None,
        "max_run_end": run_end_max.isoformat() if run_end_max else None,
    }


DASHBOARD_HTML = """
<!doctype html>
<html lang=\"vi\">
  <head>
    <meta charset=\"utf-8\" />
    <title>Breakout Strategy Control</title>
    <style>
      body { font-family: Arial, sans-serif; margin: 20px; background-color: #0f172a; color: #f8fafc; }
      h1 { margin-bottom: 0.5rem; }
      .note { margin-top: 0; margin-bottom: 1rem; color: #cbd5f5; }
      section { border: 1px solid #1e293b; padding: 1rem; margin-bottom: 1rem; border-radius: 8px; background-color: #1e293b; }
      label { display: block; margin-top: 0.5rem; font-size: 0.9rem; }
      input, select { width: 100%; padding: 0.4rem; border-radius: 4px; border: none; margin-top: 0.2rem; }
      button { margin-top: 0.8rem; padding: 0.6rem 1.2rem; border: none; border-radius: 6px; cursor: pointer; background-color: #38bdf8; color: #0f172a; font-weight: bold; }
      #status-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 1rem; }
      .event-list { max-height: 250px; overflow-y: auto; background-color: #0f172a; padding: 0.5rem; border-radius: 4px; font-size: 0.85rem; }
      .event-item { border-bottom: 1px solid #1e293b; padding: 0.4rem 0; }
      .event-item:last-child { border-bottom: none; }
      .error { color: #f87171; }
      .success { color: #4ade80; }
      .flex { display: flex; gap: 1rem; flex-wrap: wrap; }
      .flex > div { flex: 1 1 200px; }
      input[type=checkbox] { width: auto; display: inline-block; }
      .modal { position: fixed; inset: 0; background: rgba(15, 23, 42, 0.85); display: none; align-items: center; justify-content: center; z-index: 1000; }
      .modal.show { display: flex; }
      .modal-content { background: #1e293b; border-radius: 10px; padding: 1.5rem; width: min(800px, 90%); box-shadow: 0 15px 40px rgba(0, 0, 0, 0.45); }
      .modal textarea { width: 100%; min-height: 260px; border: none; border-radius: 6px; padding: 0.7rem; font-family: monospace; font-size: 0.95rem; background: #0f172a; color: #f8fafc; }
      .modal-actions { justify-content: flex-end; margin-top: 0.8rem; }
      .btn-secondary { background-color: #475569; color: #f8fafc; }
      #start-form { counter-reset: param; }
      #start-form label { position: relative; padding-left: 1.4rem; }
      #start-form label.no-counter { padding-left: 0; }
      #start-form label:not(.no-counter)::before {
        counter-increment: param;
        content: counter(param) ".";
        position: absolute;
        left: 0;
        top: 0;
        color: #94a3b8;
      }
      .tz-hint { font-size: 0.7rem; color: #94a3b8; display: inline-block; margin-left: 0.3rem; }
      .tz-note strong { color: #facc15; }
    </style>
  </head>
  <body>
    <h1>Breakout Strategy Control</h1>
    <p class="note">
      Nhập vùng tích luỹ, tham số breakout và nhấn <strong>Start</strong> để chạy bot live hoặc dùng khung Backtest/FETCH history phía dưới.
      Mọi tham số sẽ được gửi tới chiến lược breakout (ATR filter, EMA trend, time filter, momentum...).
    </p>
    <p class="note tz-note">Múi giờ hiển thị &amp; nhập liệu: <strong data-tz-label="text">Local time</strong></p>
    <section>
      <h2 style=\"margin-top:0\">Cấu hình breakout</h2>
      <p class=\"note\" style=\"margin-bottom:0.5rem;\">Điền trực tiếp tham số (hoặc nạp config đã lưu). Không còn dùng preset cố định.</p>
      <form id=\"start-form\">
        <div class=\"flex\">
          <div>
            <label>DB URL*</label>
            <input name=\"db_url\" placeholder=\"postgresql+asyncpg://user:pass@host/db\" required />
          </div>
          <div>
            <label>Symbol</label>
            <input name=\"symbol\" placeholder=\"XAUUSD\" value=\"__DEFAULT_SYMBOL__\" />
          </div>
        </div>
        <div class=\"flex\" style=\"margin-top:1rem;\">
          <div style=\"flex:1 1 360px;\">
            <label>Paste config (key = value)</label>
            <button type=\"button\" id=\"bulk-config-open-btn\" style=\"width:100%;\">Mở popup dán cấu hình</button>
            <p class=\"note\" style=\"margin-top:0.4rem;\">Dán danh sách tham số dạng <code>key = value</code> hoặc JSON đầy đủ trong popup để auto điền toàn bộ form.</p>
            <p id=\"bulk-config-status\" class=\"note\" style=\"margin-top:0.2rem;\">Chưa áp dụng cấu hình nào.</p>
          </div>
        </div>

        <section style="margin-top:1rem;">
          <h3>I. Timeframe &amp; General</h3>
          <div class="flex">
            <div>
              <label>Fast / Slow EMA</label>
              <input name="fast" type="number" value="8" />
              <input name="slow" type="number" value="21" />
            </div>
            <div>
              <label>MA type</label>
              <select name="ma_type">
                <option value="ema" selected>EMA</option>
                <option value="sma">SMA</option>
              </select>
            </div>
            <div>
              <label>Timeframe</label>
              <input name="timeframe" value="5min" />
            </div>
            <div>
              <label>Volume (lot)</label>
              <input name="volume" type="number" step="0.01" value="0.1" />
            </div>
            <div>
              <label>Capital</label>
              <input name="capital" type="number" value="10000" />
            </div>
            <div>
              <label>Risk % (fraction)</label>
              <input name="risk_pct" type="number" step="0.001" value="0.02" />
            </div>
          </div>
          <label class="no-counter" style="display:block; margin-top:0.5rem;"><input type="checkbox" name="size_from_risk" checked /> Size theo % risk</label>
        </section>

        <section>
          <h3>II. Trend Filter</h3>
          <div class="flex">
            <div>
              <label>EMA trend (chu kỳ)</label>
              <input name="trend" type="number" value="200" />
            </div>
            <div>
              <label>Market state window</label>
              <input name="market_state_window" type="number" value="40" />
            </div>
            <div>
              <label>Trading hours</label>
              <input name="trading_hours" placeholder="14:00-16:00,20:00-23:00" />
            </div>
            <div>
              <label>ADX window</label>
              <input name="adx_window" type="number" value="14" />
            </div>
            <div>
              <label>ADX threshold</label>
              <input name="adx_threshold" type="number" step="0.1" value="25" />
            </div>
          </div>
        </section>

        <section>
          <h3>III. Volatility &amp; Runtime</h3>
          <div class="flex">
            <div>
              <label>ATR baseline window</label>
              <input name="atr_baseline_window" type="number" value="14" />
            </div>
            <div>
              <label>ATR multiplier min</label>
              <input name="atr_multiplier_min" type="number" step="0.01" value="1.1" />
            </div>
            <div>
              <label>ATR multiplier max</label>
              <input name="atr_multiplier_max" type="number" step="0.01" value="3.2" />
            </div>
            <div>
              <label>Spread ATR max</label>
              <input name="spread_atr_max" type="number" step="0.01" value="0.08" />
            </div>
            <div>
              <label>Ensure history (hours)</label>
              <input name="ensure_history_hours" type="number" value="6" />
            </div>
            <div>
              <label>Poll (s)</label>
              <input name="poll" type="number" step="0.1" value="1.0" />
            </div>
          </div>
        </section>

        <section>
          <h3>IV. Breakout Conditions</h3>
          <div class="flex">
            <div>
              <label>Range lookback (bars)</label>
              <input name="range_lookback" type="number" value="40" />
            </div>
            <div>
              <label>Range min ATR (x)</label>
              <input name="range_min_atr" type="number" step="0.1" value="0.8" />
            </div>
            <div>
              <label>Range min points (USD)</label>
              <input name="range_min_points" type="number" step="0.1" value="1.0" />
            </div>
            <div>
              <label>Breakout buffer (ATR)</label>
              <input name="breakout_buffer_atr" type="number" step="0.01" value="0.3" />
            </div>
            <div>
              <label>Confirm bars</label>
              <input name="breakout_confirmation_bars" type="number" value="1" />
            </div>
          </div>
        </section>

        <section>
          <h3>V. Momentum Confirmation</h3>
          <div class="flex">
            <div>
              <label>Momentum type</label>
              <select name="momentum_type">
                <option value="hybrid" selected>Hybrid RSI + MACD</option>
                <option value="macd">MACD</option>
                <option value="pct">% Change</option>
              </select>
            </div>
            <div>
              <label>Momentum window</label>
              <input name="momentum_window" type="number" value="14" />
            </div>
            <div>
              <label>Momentum threshold (%)</label>
              <input name="momentum_threshold" type="number" step="any" value="0.07" />
            </div>
            <div>
              <label>MACD fast / slow / signal</label>
              <input name="macd_fast" type="number" value="12" />
              <input name="macd_slow" type="number" value="26" />
              <input name="macd_signal" type="number" value="9" />
            </div>
            <div>
              <label>MACD threshold</label>
              <input name="macd_threshold" type="number" step="any" value="0.0002" />
            </div>
            <div>
              <label>RSI long / short</label>
              <input name="rsi_threshold_long" type="number" step="any" value="60" />
              <input name="rsi_threshold_short" type="number" step="any" value="40" />
            </div>
          </div>
        </section>

        <section>
          <h3>VI. Trade Management</h3>
          <div class="flex">
            <div>
              <label>SL ATR</label>
              <input name="sl_atr" type="number" step="any" value="1.5" />
            </div>
            <div>
              <label>TP ATR</label>
              <input name="tp_atr" type="number" step="any" value="2.5" />
            </div>
            <div>
              <label>Trail trigger ATR</label>
              <input name="trail_trigger_atr" type="number" step="any" value="1.8" />
            </div>
            <div>
              <label>Trail ATR multiplier</label>
              <input name="trail_atr_mult" type="number" step="any" value="1.1" />
            </div>
            <div>
              <label class="no-counter" style="display:block; margin-top:1.2rem;">
                <input type="checkbox" name="reverse_exit" /> Reverse exit khi có tín hiệu ngược
              </label>
              <small class="note">Bật để đóng/đảo chiều ngay khi xuất hiện tín hiệu ngược, thay vì chờ SL/TP.</small>
            </div>
            <div>
              <label>Retry đặt lệnh (số lần)</label>
              <input name="order_retry_times" type="number" min="0" value="0" />
            </div>
            <div>
              <label>Delay giữa các lần retry (ms)</label>
              <input name="order_retry_delay_ms" type="number" min="0" value="0" />
            </div>
            <div>
              <label>Contract size</label>
              <input name="contract_size" type="number" value="100" />
            </div>
            <div>
              <label>Pip size</label>
              <input name="pip_size" type="number" step="any" value="0.01" />
            </div>
            <div>
              <label>SL (pips)</label>
              <input name="sl_pips" type="number" step="any" placeholder="Nếu muốn cố định" />
            </div>
            <div>
              <label>TP (pips)</label>
              <input name="tp_pips" type="number" step="any" placeholder="Nếu muốn cố định" />
            </div>
          </div>
        </section>

        <section>
          <h3>VII. Risk Guard</h3>
          <div class="flex">
            <div>
              <label>Max daily loss (USD)</label>
              <input name="max_daily_loss" type="number" step="any" value="30" />
            </div>
            <div>
              <label>Max loss streak</label>
              <input name="max_loss_streak" type="number" min="0" value="3" />
            </div>
            <div>
              <label>Max losses / session</label>
              <input name="max_losses_per_session" type="number" min="0" value="2" />
            </div>
            <div>
              <label>Cooldown (phút)</label>
              <input name="cooldown_minutes" type="number" min="0" value="60" />
            </div>
            <div>
              <label>Max holding time (phút)</label>
              <input name="max_holding_minutes" type="number" min="0" placeholder="VD: 180" />
            </div>
          </div>
          <!-- Luôn ghi tick realtime vào DB, bỏ lựa chọn -->
          <label class="no-counter" style="display:block;"><input type="checkbox" name="live" /> Gửi lệnh MT5 thật (live)</label>
          <label class="no-counter" style="display:block;"><input type="checkbox" name="allow_buy" checked /> Cho phép BUY</label>
          <label class="no-counter" style="display:block;"><input type="checkbox" name="allow_sell" checked /> Cho phép SELL</label>
        </section>

        <section>
          <h3>VIII. Backtest breakout</h3>
          <p class="note" style="margin-bottom:0.4rem;">Sử dụng toàn bộ tham số cấu hình phía trên. Nếu bỏ trống thời gian sẽ auto lấy 24h gần nhất. Cấu hình backtest sẽ tự lưu sau mỗi lần chạy.</p>
          <div class="flex">
            <div>
              <label>Backtest start <span class="tz-hint" data-tz-label="suffix">(Local time)</span></label>
              <input name="backtest_start" type="datetime-local" />
            </div>
            <div>
              <label>Backtest end <span class="tz-hint" data-tz-label="suffix">(Local time)</span></label>
              <input name="backtest_end" type="datetime-local" />
            </div>
          </div>
          <div class="flex" style="gap:0.5rem;">
            <button type="button" id="backtest-btn" style="flex:1;">Chạy backtest</button>
            <button type="button" id="backtest-stop-btn" style="flex:1; background:#b23b3b;">Dừng backtest</button>
          </div>
          <div class="flex" style="gap:0.5rem; margin-top:0.5rem;">
            <select id="saved-config-select" style="flex:1; min-width:200px;">
              <option value="">-- Chọn saved config --</option>
            </select>
            <button type="button" id="refresh-saved-config-btn" style="background:#2b5c99;">Lấy danh sách</button>
            <button type="button" id="load-config-btn" style="background:#4869b1;">Nạp config</button>
          </div>
          <div id="backtest-result" class="event-list" style="margin-top:0.5rem;">Chưa chạy backtest breakout</div>
        </section>

        <section>
          <h3>IX. Lịch sử</h3>
          <p class="note" style="margin-bottom:0.4rem;">Fetch tick về DB (copy_ticks_range).</p>
          <div class="flex">
            <div>
              <label>History start <span class="tz-hint" data-tz-label="suffix">(Local time)</span></label>
              <input name="history_start" type="datetime-local" />
            </div>
            <div>
              <label>History end <span class="tz-hint" data-tz-label="suffix">(Local time)</span></label>
              <input name="history_end" type="datetime-local" />
            </div>
            <div>
              <label>Batch size</label>
              <input name="history_batch" type="number" value="2000" />
            </div>
            <div>
              <label>Max days</label>
              <input name="history_max_days" type="number" value="5" />
            </div>
          </div>
          <div class="flex">
            <button type="button" id="fetch-history-btn" style="flex:1;">Fetch history</button>
          </div>
          <div id="history-result" class="event-list" style="margin-top:0.5rem;">Chưa fetch history breakout</div>
        </section>

        <div class="flex" style="gap:0.5rem;">
          <button type="submit" style="flex:1;">Start</button>
          <button type="button" id="stop-btn" style="flex:1;">Stop</button>
        </div>
      </form>

      <div id="message"></div>
      <div id="strategy-summary" class="event-list" style="margin-top:0.5rem;">Chưa có cấu hình breakout</div>
    </section>

    <div id="bulk-config-modal" class="modal" aria-hidden="true">
      <div class="modal-content">
        <h3 style="margin-top:0;">Dán cấu hình</h3>
        <p class="note" style="margin-top:0; margin-bottom:0.8rem;">
          Bộ tham số tối ưu theo các tiêu chí trong dashboard:
          timeframe &amp; fast/slow EMA/MA type, trend EMA + market_state_window, khung giờ giao dịch từng phiên, ADX window/threshold,
          ATR baseline &amp; multiplier/spread guard, breakout range/buffer/confirmation, momentum MACD+RSI (window, threshold),
          SL/TP ATR, trailing trigger/multiplier, risk_pct/capital/size_from_risk, cùng các guard về max_daily_loss/loss_streak/session/cooldown.
        </p>
        <textarea id="bulk-config-input" placeholder="fast = 8&#10;slow = 21&#10;timeframe = 5min&#10;..."></textarea>
        <div class="flex modal-actions" style="gap:0.5rem;">
          <button type="button" id="bulk-config-apply-btn">Áp dụng</button>
          <button type="button" id="bulk-config-close-btn" class="btn-secondary">Đóng</button>
        </div>
      </div>
    </div>

    <section>
      <h2>Trạng thái breakout</h2>
      <p class=\"note\" style=\"margin-bottom:0.5rem;\">Theo dõi quote, tín hiệu gần nhất và lý do chờ breakout (range/ATR/momentum). Tổng hợp cấu hình nằm ở khối dưới.</p>
      <div id=\"status-grid\">
        <div>Trạng thái: <span id=\"st-running\">-</span></div>
        <div>Symbol: <span id=\"st-symbol\">-</span></div>
        <div>Last quote: <span id=\"st-quote\">-</span></div>
        <div>PNL tích luỹ: <span id=\"st-pnl\">-</span></div>
        <div>Vị thế: <span id=\"st-position\">-</span></div>
        <div style=\"grid-column:1 / -1\">Đang chờ: <span id=\"st-waiting\">-</span></div>
      </div>
      <div class=\"flex\">
        <div>
          <h3>Tín hiệu mới nhất</h3>
          <div class=\"event-list\" id=\"signal-block\">-</div>
        </div>
        <div>
          <h3>Chi tiết vị thế</h3>
          <div class=\"event-list\" id=\"position-detail\">-</div>
        </div>
      </div>
    </section>

    <script>
      // Bắt buộc dùng dấu chấm cho mọi input số (tránh locale thêm dấu phẩy)
      document.documentElement.setAttribute('lang', 'en');
      document.addEventListener('DOMContentLoaded', () => {
        document.querySelectorAll('input[type="number"]').forEach((el) => {
          el.setAttribute('inputmode', 'decimal');
          el.setAttribute('lang', 'en');
          if (!el.hasAttribute('step')) el.setAttribute('step', 'any');
        });
      });
      const form = document.getElementById('start-form');
      const stopBtn = document.getElementById('stop-btn');
      const messageEl = document.getElementById('message');
      const signalBlock = document.getElementById('signal-block');
      const positionDetail = document.getElementById('position-detail');
      const summaryBox = document.getElementById('strategy-summary');
      const backtestBtn = document.getElementById('backtest-btn');
      const backtestStopBtn = document.getElementById('backtest-stop-btn');
      const savedConfigSelect = document.getElementById('saved-config-select');
      const refreshSavedConfigBtn = document.getElementById('refresh-saved-config-btn');
      const loadConfigBtn = document.getElementById('load-config-btn');
      const backtestResult = document.getElementById('backtest-result');
      const fetchHistoryBtn = document.getElementById('fetch-history-btn');
      const historyResult = document.getElementById('history-result');
      const bulkConfigInput = document.getElementById('bulk-config-input');
      const bulkConfigApplyBtn = document.getElementById('bulk-config-apply-btn');
      const bulkConfigStatus = document.getElementById('bulk-config-status');
      const bulkConfigModal = document.getElementById('bulk-config-modal');
      const bulkConfigOpenBtn = document.getElementById('bulk-config-open-btn');
      const bulkConfigCloseBtn = document.getElementById('bulk-config-close-btn');
      let lastBacktestSummary = null;
      let lastPositionKey = null;
      const stRunning = document.getElementById('st-running');
      const stSymbol = document.getElementById('st-symbol');
      const stQuote = document.getElementById('st-quote');
      const stPnl = document.getElementById('st-pnl');
      const stPosition = document.getElementById('st-position');
      const stWaiting = document.getElementById('st-waiting');
      const latestSignal = document.getElementById('latest-signal');
      const DEFAULT_FETCH_TRADING_DAYS = 2;
      const timezoneText = getTimezoneText();
      const timezoneSuffix = timezoneText ? ` (${timezoneText})` : '';
      let backtestController = null;
      document.querySelectorAll('[data-tz-label="text"]').forEach((el) => {
        el.textContent = timezoneText;
      });
      document.querySelectorAll('[data-tz-label="suffix"]').forEach((el) => {
        el.textContent = timezoneSuffix || '';
      });

      const fieldHelp = {
        db_url: 'Chuỗi kết nối CSDL async (Postgres/SQLite). Ví dụ: postgresql+asyncpg://user:pass@host/db',
        symbol: 'Mã giao dịch trong MT5, ví dụ XAUUSD',
        fast: 'Chu kỳ MA nhanh (số bar) được dùng làm trigger',
        slow: 'Chu kỳ MA chậm (số bar) để so sánh với MA nhanh',
        ma_type: 'Loại trung bình động: EMA phản ứng nhanh hơn, SMA mượt hơn',
        timeframe: 'Khung thời gian để resample tick (ví dụ 1min, 5min, 15min)',
        volume: 'Khối lượng mặc định (lot). Nếu bật size theo % risk sẽ bị ghi đè',
        capital: 'Vốn quy đổi USD dùng để tính khối lượng khi bật size-from-risk',
        risk_pct: 'Tỷ lệ rủi ro mỗi lệnh (dạng thập phân, ví dụ 0.02 = 2% vốn)',
        ensure_history_hours: 'Số giờ dữ liệu tối thiểu cần có trong DB trước khi chạy. Thiếu sẽ tự fetch MT5',
        poll: 'Chu kỳ lấy quote từ MT5 (giây). Giá trị nhỏ => phản ứng nhanh hơn',
        spread_atr_max: 'Ngưỡng spread tối đa (tính theo ATR). Spread cao hơn sẽ bỏ qua tín hiệu',
        trend: 'Chu kỳ EMA trend giúp xác nhận hướng chính (ví dụ EMA200)',
        market_state_window: 'Số bar để đánh giá vùng sideway/trạng thái thị trường',
        trading_hours: 'Giới hạn khung giờ trade, ví dụ 14:00-16:00,20:00-23:00 (giờ VN)',
        adx_window: 'Số nến dùng để tính ADX nhằm đo sức mạnh xu hướng',
        adx_threshold: 'ADX phải lớn hơn hoặc bằng ngưỡng này mới kích hoạt tín hiệu (0 = bỏ qua)',
        momentum_type: 'Chọn bộ lọc xung lực: MACD, %Change hoặc Hybrid (MACD + RSI)',
        momentum_window: 'Số bar nhìn lại khi dùng %change hoặc RSI',
        momentum_threshold: 'Ngưỡng % thay đổi tối thiểu để xác nhận breakout (%change/hybrid)',
        rsi_threshold_long: 'RSI tối thiểu cho lệnh Long (ví dụ >60)',
        rsi_threshold_short: 'RSI tối đa cho lệnh Short (ví dụ <40)',
        range_lookback: 'Số bar dùng để đo vùng tích luỹ gần nhất',
        range_min_atr: 'Độ cao range tối thiểu tính theo ATR (range >= ATR * hệ số)',
        range_min_points: 'Độ cao range tối thiểu tuyệt đối (USD)',
        breakout_buffer_atr: 'Khoảng đệm cộng thêm vào S/R trước khi xác nhận breakout',
        breakout_confirmation_bars: 'Số nến đóng ngoài vùng cần có để xác nhận',
        atr_baseline_window: 'Số bar để tính ATR trung bình làm baseline so sánh',
        atr_multiplier_min: 'ATR phải lớn hơn baseline * hệ số này',
        atr_multiplier_max: 'ATR phải nhỏ hơn baseline * hệ số này',
        trail_trigger_atr: 'ATR tại đó bật trailing stop',
        trail_atr_mult: 'Khoảng dịch SL mỗi lần trail = ATR * hệ số này',
        reverse_exit: 'Bật để đóng/đảo chiều ngay khi tín hiệu ngược xuất hiện (không chờ SL/TP)',
        max_daily_loss: 'Giới hạn lỗ tuyệt đối (USD) trong 1 ngày, vượt ngưỡng sẽ ngưng mở lệnh',
        max_loss_streak: 'Số lệnh thua liên tục tối đa trước khi bot tạm dừng',
        max_losses_per_session: 'Số lệnh thua tối đa trong một phiên giao dịch',
        cooldown_minutes: 'Thời gian nghỉ sau khi chạm guard trước khi trade lại',
        max_holding_minutes: 'Tự đóng vị thế nếu nắm giữ quá số phút này (time-based exit)',
        macd_fast: 'Chu kỳ EMA nhanh trong MACD (mặc định 12)',
        macd_slow: 'Chu kỳ EMA chậm trong MACD (mặc định 26)',
        macd_signal: 'Chu kỳ đường tín hiệu MACD (mặc định 9)',
        macd_threshold: 'Ngưỡng histogram MACD ( >0 mới xác nhận mua, <0 xác nhận bán )',
        contract_size: 'Hệ số quy đổi PnL (ví dụ XAUUSD ~100 oz/lot)',
        sl_atr: 'Hệ số ATR dùng để đặt Stop Loss',
        tp_atr: 'Hệ số ATR dùng để đặt Take Profit',
        size_from_risk: 'Nếu bật, bot sẽ tính volume dựa trên capital/risk_pct/contract_size',
        // ingest_live_db: luôn bật, không cần hướng dẫn
        live: 'Bật để gửi lệnh thật tới MT5. Nếu tắt sẽ chạy chế độ paper',
        backtest_start: 'Thời điểm bắt đầu backtest (ISO 8601)',
        backtest_end: 'Thời điểm kết thúc backtest (ISO 8601)',
        history_start: 'Thời điểm bắt đầu fetch lịch sử từ MT5 (ISO 8601). Để trống sẽ tự lùi 2 ngày giao dịch gần nhất',
        history_end: 'Thời điểm kết thúc fetch lịch sử (ISO 8601). Để trống sẽ dùng thời gian hiện tại',
        history_batch: 'Số dòng ghi mỗi batch insert vào DB',
        history_max_days: 'Số ngày tối đa cho mỗi lần gọi MT5 copy_ticks_range',
      };

      form.addEventListener('submit', async (e) => {
        e.preventDefault();
        const payload = buildPayload(new FormData(form));
        try {
          const res = await fetch('/api/live/start', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(payload),
          });
          const data = await res.json();
          if (!res.ok) {
            throw new Error(data.detail || 'Không thể start');
          }
          messageEl.textContent = 'Đã start breakout live';
          messageEl.className = 'success';
        } catch (err) {
          messageEl.textContent = err.message;
          messageEl.className = 'error';
        }
        refreshStatus();
      });

      stopBtn.addEventListener('click', async () => {
        try {
          await fetch('/api/live/stop', { method: 'POST' });
          messageEl.textContent = 'Đã stop breakout';
          messageEl.className = 'success';
        } catch (err) {
          messageEl.textContent = err.message;
          messageEl.className = 'error';
        }
        refreshStatus();
      });

      if (backtestBtn) {
        backtestBtn.addEventListener('click', runBacktest);
      }
      if (backtestStopBtn) {
        backtestStopBtn.addEventListener('click', stopBacktest);
      }
      if (refreshSavedConfigBtn) {
        refreshSavedConfigBtn.addEventListener('click', fetchSavedConfigList);
      }
      if (loadConfigBtn) {
        loadConfigBtn.addEventListener('click', loadSelectedConfig);
      }
      if (fetchHistoryBtn) {
        fetchHistoryBtn.addEventListener('click', runFetchHistory);
      }
      if (bulkConfigOpenBtn) {
        bulkConfigOpenBtn.addEventListener('click', () => openBulkConfigModal());
      }
      if (bulkConfigCloseBtn) {
        bulkConfigCloseBtn.addEventListener('click', () => closeBulkConfigModal());
      }
      if (bulkConfigModal) {
        bulkConfigModal.addEventListener('click', (event) => {
          if (event.target === bulkConfigModal) {
            closeBulkConfigModal();
          }
        });
      }
      if (bulkConfigApplyBtn) {
        bulkConfigApplyBtn.addEventListener('click', () => applyBulkConfig(bulkConfigInput?.value || ''));
      }

      function buildPayload(formData) {
        const payload = {};
        for (const [key, raw] of formData.entries()) {
          if (raw === '' && key !== 'symbol') continue;
          const rawTrim = typeof raw === 'string' ? raw.trim() : raw;
          const numericVal = typeof raw === 'string' ? raw.replace(',', '.').trim() : raw;
          if (
            [
              'fast',
              'slow',
              'trend',
              'market_state_window',
              'history_batch',
              'history_max_days',
              'momentum_window',
              'macd_fast',
              'macd_slow',
              'macd_signal',
              'range_lookback',
              'breakout_confirmation_bars',
              'atr_baseline_window',
              'adx_window',
              'max_loss_streak',
              'max_losses_per_session',
              'cooldown_minutes',
            ].includes(key)
          ) {
            payload[key] = parseInt(numericVal, 10);
          } else if (
            [
              'volume',
              'capital',
              'risk_pct',
              'spread_atr_max',
              'sl_atr',
              'tp_atr',
              'trail_trigger_atr',
              'trail_atr_mult',
              'poll',
              'ensure_history_hours',
              'pip_size',
              'contract_size',
              'momentum_threshold',
              'macd_threshold',
              'rsi_threshold_long',
              'rsi_threshold_short',
              'max_holding_minutes',
              'range_min_atr',
              'range_min_points',
              'breakout_buffer_atr',
              'atr_multiplier_min',
              'atr_multiplier_max',
              'max_daily_loss',
              'adx_threshold',
            ].includes(key)
          ) {
            payload[key] = parseFloat(numericVal);
          } else {
            payload[key] = rawTrim;
          }
        }
        // Các checkbox không xuất hiện trong FormData khi unchecked -> set thủ công
        ['size_from_risk', 'live', 'allow_buy', 'allow_sell', 'reverse_exit'].forEach((name) => {
          if (Object.prototype.hasOwnProperty.call(form, name) && form[name]) {
            payload[name] = form[name].checked;
          }
        });
        return payload;
      }

      function prepareBacktestPayload() {
        let start = form.backtest_start.value;
        let end = form.backtest_end.value;
        const now = new Date();
        if (!end) {
          end = formatLocalInput(now);
          form.backtest_end.value = end;
        }
        if (!start) {
          const endDate = new Date(end);
          const startDate = new Date(endDate.getTime() - 24 * 60 * 60 * 1000);
          start = formatLocalInput(startDate);
          form.backtest_start.value = start;
        }
        const payload = buildPayload(new FormData(form));
        payload.start = normalizeDateInput(start);
        payload.end = normalizeDateInput(end);
        payload.db_url = form.db_url.value.trim();
        payload.symbol = form.symbol.value || 'XAUUSD';
        return payload;
      }

      function applySuggestionToForm(suggestion = {}) {
        Object.entries(suggestion).forEach(([key, value]) => {
          const control = form.elements[key];
          if (!control) return;
          if (typeof RadioNodeList !== 'undefined' && control instanceof RadioNodeList) {
            control.value = value ?? '';
            return;
          }
          if (control.type === 'checkbox') {
            control.checked = Boolean(value);
          } else {
            control.value = value ?? '';
          }
        });
      }

      
      async function runBacktest() {
        const payload = prepareBacktestPayload();
        if (!payload.db_url) {
          alert('Vui lòng nhập DB URL trước khi backtest.');
          return;
        }
        if (backtestController) {
          backtestController.abort();
        }
        backtestController = new AbortController();
        backtestResult.textContent = 'Đang chạy backtest breakout...';
        lastBacktestSummary = null;
        try {
          const res = await fetch('/api/backtest/run', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(payload),
            signal: backtestController.signal,
          });
          const data = await res.json();
          if (!res.ok) {
            throw new Error(data.detail || 'Backtest thất bại');
          }
          lastBacktestSummary = data;
          backtestResult.innerHTML = formatBacktestResult(data);
        } catch (err) {
          if (err.name === 'AbortError') {
            backtestResult.textContent = 'Backtest đã dừng.';
            return;
          }
          lastBacktestSummary = null;
          backtestResult.textContent = err.message;
        }
        backtestController = null;
      }

      function stopBacktest() {
        const doStop = async () => {
          try {
            await fetch('/api/backtest/cancel', { method: 'POST' });
          } catch (_) {
            /* noop */
          }
        };

        if (backtestController) {
          backtestController.abort();
          backtestController = null;
        }
        backtestResult.textContent = 'Đã gửi yêu cầu dừng backtest...';
        doStop();
      }

      async function fetchSavedConfigList() {
        const dbUrl = form.db_url.value.trim();
        if (!dbUrl) {
          alert('Vui lòng nhập DB URL trước khi lấy danh sách config.');
          return;
        }
        backtestResult.textContent = 'Đang tải danh sách cấu hình...';
        try {
          const res = await fetch(`/api/backtest/saved-list?db_url=${encodeURIComponent(dbUrl)}`);
          const data = await res.json();
          if (!res.ok) {
            throw new Error(data.detail || 'Không thể tải config');
          }
          const items = data.items || [];
          savedConfigSelect.innerHTML = '<option value="">-- Chọn saved config --</option>';
          items.forEach((item) => {
            const opt = document.createElement('option');
            opt.value = item.id;
            const labelParts = [
              `#${item.id}`,
              item.symbol || '',
              item.timeframe || '',
              item.fast && item.slow ? `(${item.fast}/${item.slow})` : '',
            ].filter(Boolean);
            opt.textContent = labelParts.join(' ');
            savedConfigSelect.appendChild(opt);
          });
          backtestResult.textContent = `Đã tải ${items.length} cấu hình.`;
        } catch (err) {
          backtestResult.textContent = err.message || 'Không thể tải danh sách config';
        }
      }

      async function loadSelectedConfig() {
        const dbUrl = form.db_url.value.trim();
        const selectedId = savedConfigSelect?.value;
        if (!dbUrl) {
          alert('Vui lòng nhập DB URL trước khi nạp config.');
          return;
        }
        if (!selectedId) {
          alert('Vui lòng chọn một config trong danh sách.');
          return;
        }
        backtestResult.textContent = `Đang nạp config #${selectedId}...`;
        try {
          const res = await fetch(`/api/backtest/config/${selectedId}?db_url=${encodeURIComponent(dbUrl)}`);
          const data = await res.json();
          if (!res.ok) {
            throw new Error(data.detail || 'Không thể nạp config');
          }
          if (!data || !data.config) {
            backtestResult.textContent = 'Không tìm thấy cấu hình.';
            return;
          }
          applySuggestionToForm(data.config);
          // Đảm bảo các trường risk guard được nạp đầy đủ (kể cả giá trị 0)
          const riskFields = ['max_daily_loss', 'max_loss_streak', 'max_losses_per_session', 'cooldown_minutes', 'max_holding_minutes'];
          riskFields.forEach((key) => {
            if (Object.prototype.hasOwnProperty.call(data.config, key) && form.elements[key]) {
              form.elements[key].value = data.config[key] ?? '';
            }
          });
          backtestResult.textContent = `Đã nạp config #${data.id}`;
        } catch (err) {
          backtestResult.textContent = err.message || 'Không thể nạp config';
        }
      }

      // Lưu cấu hình backtest đã được tự động thực hiện phía server sau mỗi lần backtest

      async function runFetchHistory() {
        const dbUrl = form.db_url.value.trim();
        if (!dbUrl) {
          setHistoryMessage('Vui lòng nhập DB URL trước', 'error');
          return;
        }
        let end = form.history_end.value;
        let start = form.history_start.value;
        const now = new Date();
        if (!end) {
          end = formatLocalInput(now);
          form.history_end.value = end;
        }
        if (!start) {
          const endDate = new Date(end);
          const startDate = subtractTradingDays(endDate, DEFAULT_FETCH_TRADING_DAYS);
          start = formatLocalInput(startDate);
          form.history_start.value = start;
        }
        const batch = parseInt(form.history_batch.value || '2000', 10);
        const maxDays = parseInt(form.history_max_days.value || '5', 10);
        if (!Number.isFinite(batch) || batch <= 0) {
          setHistoryMessage('Batch size phải > 0', 'error');
          return;
        }
        if (!Number.isFinite(maxDays) || maxDays <= 0) {
          setHistoryMessage('Max days phải > 0', 'error');
          return;
        }
        const payload = {
          db_url: dbUrl,
          symbol: form.symbol.value || 'XAUUSD',
          start: normalizeDateInput(start),
          end: normalizeDateInput(end),
          batch,
          max_days: maxDays,
        };
        setHistoryMessage('Đang fetch dữ liệu từ MT5...', null);
        try {
          const res = await fetch('/api/history/fetch', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(payload),
          });
          const data = await res.json();
          if (!res.ok) {
            throw new Error(data.detail || 'Fetch history thất bại');
          }
          setHistoryMessage(
            `Đã fetch ${data.symbol} (${formatTimestamp(data.start)} -> ${formatTimestamp(data.end)})`,
            'success',
          );
        } catch (err) {
          setHistoryMessage(err.message || 'Fetch history thất bại', 'error');
        }
      }

      function formatSignal(signal) {
        if (!signal) return 'Chưa có tín hiệu';
        const ts = formatTimestamp(signal.timestamp);
        const side = signal.side ? String(signal.side).toUpperCase() : '';
        if (signal.type === 'position_open') {
          return `${ts}<br />OPEN ${side} @ ${formatNum(signal.price)}<br />Vol: ${signal.volume} | SL: ${formatNum(signal.stop_loss)} | TP: ${formatNum(signal.take_profit)}`;
        }
        if (signal.type === 'position_close') {
          return `${ts}<br />CLOSE ${side} @ ${formatNum(signal.close_price)}<br />PnL: ${formatNum(signal.pnl_value, 2)} (${formatNum(signal.pnl_points)})`;
        }
        if (signal.type === 'error') {
          return `${ts}<br />ERROR ${signal.message}`;
        }
        return `${ts}<br />${signal.type || 'event'}`;
      }

      function formatWaitingReason(signal, fallback) {
        if (signal && signal.type === 'status') {
          const extra = signal.extra || {};
          const info = Object.entries(extra)
            .map(([k, v]) => `${k}: ${v}`)
            .join(' | ');
          return info ? `${signal.reason} (${info})` : signal.reason;
        }
        return fallback || 'Đang chờ tín hiệu';
      }

      function formatPositionDetail(pos) {
        if (!pos) return 'Không có vị thế';
        const lines = [
          `Side: <strong>${String(pos.side).toUpperCase()}</strong>`,
          `Entry: ${formatNum(pos.price)}`,
          `Volume: ${pos.volume}`,
          `SL: ${formatNum(pos.stop_loss)} | TP: ${formatNum(pos.take_profit)}`,
          `Opened: ${formatTimestamp(pos.timestamp)}`,
        ];
        return lines.join('<br />');
      }

      function formatConfigSummary(cfg, cliCmd) {
        if (!cfg) return 'Chưa có cấu hình';
        const riskPctDisplay =
          typeof cfg.risk_pct === 'number'
            ? `${(cfg.risk_pct * 100).toFixed(2)}%`
            : cfg.risk_pct ?? '-';
        const items = [
          `MA: ${cfg.fast_ma}/${cfg.slow_ma} (${cfg.ma_type?.toUpperCase()})`,
          `Timeframe: ${cfg.timeframe}`,
          `Trend EMA: ${cfg.trend_ma || '-'} | ADX ≥ ${cfg.adx_threshold || 0}`,
          `Momentum: ${cfg.momentum_type?.toUpperCase()} (MACD ${cfg.macd_fast || '-'} / ${cfg.macd_slow || '-'} / ${cfg.macd_signal || '-'}, RSI ${cfg.rsi_threshold_long ?? '-'} / ${cfg.rsi_threshold_short ?? '-'})`,
          `Breakout: lookback ${cfg.range_lookback || '-'} | buffer ATR ${cfg.breakout_buffer_atr || '-'}`,
          `ATR filter: baseline ${cfg.atr_baseline_window || '-'} | min ${cfg.atr_multiplier_min || '-'} | max ${cfg.atr_multiplier_max || '-'}`,
          cfg.trading_hours
            ? `Sessions: ${
                Array.isArray(cfg.trading_hours) ? cfg.trading_hours.join(', ') : cfg.trading_hours
              }`
            : '',
          `Risk: capital ${cfg.capital || '-'} | risk% ${riskPctDisplay}`,
          `ATR SL/TP: ${cfg.sl_atr || '-'} / ${cfg.tp_atr || '-'}`,
          `Trailing: trigger ${cfg.trail_trigger_atr || '-'} ATR | mult ${cfg.trail_atr_mult || '-'}`,
          cfg.max_daily_loss
            ? `Risk guard: daily ${cfg.max_daily_loss} | streak ${cfg.max_consecutive_losses ?? '-'} | session ${cfg.max_losses_per_session ?? '-'}`.trim()
            : '',
          `Spread ATR max: ${cfg.spread_atr_max || '-'}`,
          cliCmd ? `CLI: <code>${cliCmd}</code>` : '',
        ];
        return items.filter(Boolean).join('<br />');
      }

      function formatBacktestResult(data) {
        if (!data || !data.total_trades) {
          return 'Không có kết quả backtest';
        }
        return [
          `Số lệnh: ${data.total_trades} | Thắng: ${data.wins} | Thua: ${data.losses}`,
          `PnL (price units): ${formatNum(data.total_pnl, 6)} | Avg: ${formatNum(data.avg_pnl, 6)}`,
          `PnL (USD): ${formatNum(data.total_usd_pnl, 2)} | Avg: ${formatNum(data.avg_usd_pnl, 2)}`,
          data.db_run_id ? `Đã lưu ${data.stored_trades ?? data.total_trades} lệnh vào DB (run_id = ${data.db_run_id})` : '',
          data.saved_config_id ? `Đã lưu cấu hình backtest (ID ${data.saved_config_id})` : '',
          data.save_error ? `Lưu cấu hình lỗi: ${data.save_error}` : '',
          data.cli_command ? `CLI: <code>${data.cli_command}</code>` : '',
        ].filter(Boolean).join('<br />');
      }

      function setHistoryMessage(text, status) {
        historyResult.textContent = text;
        historyResult.classList.remove('error', 'success');
        if (status) {
          historyResult.classList.add(status);
        }
      }

      function formatTimestamp(value) {
        if (!value) return '-';
        try {
          return new Date(value).toLocaleString() + timezoneSuffix;
        } catch (err) {
          return String(value);
        }
      }

      function formatLocalInput(date) {
        const d = new Date(date);
        if (Number.isNaN(d.getTime())) return '';
        const year = d.getFullYear();
        const month = String(d.getMonth() + 1).padStart(2, '0');
        const day = String(d.getDate()).padStart(2, '0');
        const hour = String(d.getHours()).padStart(2, '0');
        const minute = String(d.getMinutes()).padStart(2, '0');
        return `${year}-${month}-${day}T${hour}:${minute}`;
      }

      function normalizeDateInput(value) {
        if (!value) return value;
        const d = new Date(value);
        if (Number.isNaN(d.getTime())) return value;
        const pad = (num) => String(num).padStart(2, '0');
        const year = d.getFullYear();
        const month = pad(d.getMonth() + 1);
        const day = pad(d.getDate());
        const hour = pad(d.getHours());
        const minute = pad(d.getMinutes());
        const second = pad(d.getSeconds());
        const offsetMinutes = -d.getTimezoneOffset(); // positive if ahead of UTC
        const sign = offsetMinutes >= 0 ? '+' : '-';
        const abs = Math.abs(offsetMinutes);
        const offsetHour = pad(Math.floor(abs / 60));
        const offsetMin = pad(abs % 60);
        return `${year}-${month}-${day}T${hour}:${minute}:${second}${sign}${offsetHour}:${offsetMin}`;
      }

      function getTimezoneText() {
        try {
          const tz = Intl.DateTimeFormat().resolvedOptions().timeZone;
          const offsetMinutes = -new Date().getTimezoneOffset();
          const sign = offsetMinutes >= 0 ? '+' : '-';
          const abs = Math.abs(offsetMinutes);
          const hours = String(Math.floor(abs / 60)).padStart(2, '0');
          const minutes = String(abs % 60).padStart(2, '0');
          const offset = `UTC${sign}${hours}:${minutes}`;
          return tz ? `${tz} · ${offset}` : offset;
        } catch (err) {
          return 'UTC';
        }
      }

      function formatNum(val, digits = 3) {
        const num = Number(val);
        return Number.isFinite(num) ? num.toFixed(digits) : '-';
      }

      function openBulkConfigModal() {
        if (!bulkConfigModal) return;
        bulkConfigModal.classList.add('show');
        setTimeout(() => bulkConfigInput?.focus(), 50);
      }

      function closeBulkConfigModal() {
        bulkConfigModal?.classList.remove('show');
      }

      function subtractTradingDays(endDate, tradingDays) {
        if (!(endDate instanceof Date) || Number.isNaN(endDate)) {
          return new Date();
        }
        const cursor = new Date(endDate);
        let remaining = Math.max(1, Math.ceil(tradingDays));
        while (remaining > 0) {
          cursor.setDate(cursor.getDate() - 1);
          const dow = cursor.getUTCDay();
          if (dow !== 0 && dow !== 6) {
            remaining -= 1;
          }
        }
        return cursor;
      }

      function applyBulkConfig(rawText) {
        const content = (rawText || '').trim();
        if (!content) {
          setBulkConfigStatus('Vui lòng dán danh sách tham số theo định dạng key = value', 'error');
          return;
        }
        try {
          const parsed = parseBulkConfigText(content);
          const keys = Object.keys(parsed);
          if (!keys.length) {
            throw new Error('Không tìm thấy tham số hợp lệ trong nội dung đã dán');
          }
          applySuggestionToForm(parsed);
          setBulkConfigStatus(`Đã áp dụng ${keys.length} tham số: ${keys.slice(0, 5).join(', ')}${keys.length > 5 ? '…' : ''}`, 'success');
          closeBulkConfigModal();
        } catch (err) {
          setBulkConfigStatus(err.message || 'Không thể phân tích danh sách tham số', 'error');
        }
      }

      function parseBulkConfigText(rawText) {
        const result = {};
        const listFields = new Set(['trading_hours']);
        if (rawText.trim().startsWith('{')) {
          let data;
          try {
            data = JSON.parse(rawText);
          } catch (err) {
            throw new Error('JSON không hợp lệ, vui lòng kiểm tra lại cú pháp');
          }
          flattenBulkConfig(data, result, listFields);
          return result;
        }
        rawText.split(/\\n+/).forEach((line, idx) => {
          const stripped = line.replace(/#.*/, '').replace(/\/\/.*/, '').trim();
          if (!stripped) return;
          const match = stripped.match(/^([^=:]+)\s*[:=]\s*(.+)$/);
          if (!match) {
            throw new Error(`Không nhận diện được dòng ${idx + 1}: "${line.trim()}"`);
          }
          const key = match[1].trim();
          let value = match[2].trim();
          if (
            (value.startsWith('"') && value.endsWith('"')) ||
            (value.startsWith("'") && value.endsWith("'"))
          ) {
            value = value.slice(1, -1);
          }
          result[key] = coerceBulkValue(key, value, listFields);
        });
        return result;
      }

      function flattenBulkConfig(source, target, listFields) {
        if (!source || typeof source !== 'object') return;
        Object.entries(source || {}).forEach(([key, value]) => {
          if (value && typeof value === 'object' && !Array.isArray(value)) {
            flattenBulkConfig(value, target, listFields);
          } else {
            target[key] = coerceBulkValue(key, value, listFields);
          }
        });
      }

      function coerceBulkValue(key, value, listFields) {
        if (value === null || value === undefined) return '';
        if (typeof value === 'number' || typeof value === 'boolean') {
          return value;
        }
        let text = String(value).trim();
        if (!text) return '';
        const lower = text.toLowerCase();
        if (['true', 'yes', 'on', '1'].includes(lower)) return true;
        if (['false', 'no', 'off', '0'].includes(lower)) return false;
        if (/^\d+(\.\d+)?%$/.test(text)) {
          return parseFloat(text.replace('%', ''));
        }
        const numVal = Number(text);
        if (!Number.isNaN(numVal)) {
          return numVal;
        }
        if (listFields.has(key)) {
          return text
            .split(',')
            .map((token) => token.trim())
            .filter(Boolean)
            .join(',');
        }
        return text;
      }

      function setBulkConfigStatus(message, type) {
        if (!bulkConfigStatus) return;
        bulkConfigStatus.className = type ? `note ${type}` : 'note';
        bulkConfigStatus.textContent = message;
      }

      function formatLatestSignal(signal) {
        if (!signal) return '-';
        const type = signal.type || '';
        if (type === 'position_open') {
          return `OPEN ${String(signal.side || '').toUpperCase()} @ ${formatNum(signal.price)} vol ${signal.volume ?? '-'}`;
        }
        if (type === 'position_close') {
          return `CLOSE ${String(signal.side || '').toUpperCase()} pnl ${formatNum(signal.pnl_value, 2)}`;
        }
        if (type === 'status') return signal.reason || 'Status';
        if (type === 'error') return signal.message || 'Error';
        return JSON.stringify(signal);
      }

      async function refreshStatus() {
        try {
          const res = await fetch('/api/live/status');
          const data = await res.json();
          if (!res.ok) throw new Error(data.detail || 'Không lấy được trạng thái live');
          if (stRunning) stRunning.textContent = data.running ? 'Đang chạy' : 'Đã dừng';
          if (stSymbol) stSymbol.textContent = data.config?.symbol || '-';
          if (stQuote) stQuote.textContent = formatNum(data.last_quote, 3);
          if (stPnl) stPnl.textContent = formatNum(data.cumulative_pnl, 2);
          if (stWaiting) stWaiting.textContent = data.waiting_reason || '-';
          const pos = data.current_position;
          if (stPosition) stPosition.textContent = pos ? `${String(pos.side || '').toUpperCase()} @ ${formatNum(pos.price)}` : '-';
          if (positionDetail) positionDetail.innerHTML = formatPositionDetail(pos);
          if (latestSignal) latestSignal.textContent = formatLatestSignal(data.last_signal);
          if (summaryBox) summaryBox.innerHTML = formatConfigSummary(data.config, data.cli_command);
        } catch (err) {
          if (stRunning) stRunning.textContent = '-';
          if (stSymbol) stSymbol.textContent = '-';
          if (stQuote) stQuote.textContent = '-';
          if (stPnl) stPnl.textContent = '-';
          if (stWaiting) stWaiting.textContent = err.message || '-';
          if (stPosition) stPosition.textContent = '-';
          if (positionDetail) positionDetail.textContent = '-';
          if (latestSignal) latestSignal.textContent = '-';
        }
      }

      function applyFieldHelp() {
        Object.entries(fieldHelp).forEach(([name, text]) => {
          const el = form.elements[name];
          if (!el) return;
          if (el instanceof RadioNodeList) {
            el.forEach((child) => {
              if (child && child.title !== undefined) child.title = text;
            });
          } else if (el instanceof HTMLElement) {
            el.title = text;
          }
        });
      }

      applyFieldHelp();
      refreshStatus();
      setInterval(refreshStatus, 5000);
    </script>
  </body>
</html>
""".replace("__DEFAULT_SYMBOL__", settings.quote_symbol)




BACKTEST_DASHBOARD_HTML = """
<!doctype html>
<html lang="vi">
  <head>
    <meta charset="utf-8" />
    <title>Backtest Dashboard</title>
    <style>
      :root { color-scheme: dark; }
      * { box-sizing: border-box; }
      body { font-family: 'Inter', Arial, sans-serif; background:#0f172a; color:#f8fafc; margin:0; padding:0; }
      header { padding: 1rem 1.5rem; background:#0b1220; border-bottom:1px solid #1e293b; }
      main { padding: 1.5rem; max-width: 1200px; margin: 0 auto; }
      h1 { margin:0; }
      section { background:#0b1220; border:1px solid #1e293b; border-radius:10px; padding:1rem 1.2rem; margin-bottom:1rem; }
      label { display:block; margin-top:0.6rem; font-size:0.9rem; color:#cbd5f5; }
      input, select { width:100%; padding:0.6rem 0.7rem; border-radius:8px; border:1px solid #1e293b; margin-top:0.25rem; background:#111827; color:#e2e8f0; line-height:1.4; }
      input::placeholder, select::placeholder { color:#94a3b8; }
      input:focus, select:focus { outline:1px solid #38bdf8; border-color:#38bdf8; }
      button { margin-top:0.8rem; padding:0.7rem 1.2rem; border:none; border-radius:8px; cursor:pointer; background:linear-gradient(120deg,#38bdf8,#34d399); color:#041223; font-weight:700; }
      .grid { display:grid; grid-template-columns: repeat(auto-fit, minmax(240px,1fr)); gap:1rem; }
      .chart-grid { display:grid; grid-template-columns: repeat(auto-fit, minmax(320px,1fr)); gap:1rem; }
      .card { background:#0f172a; border:1px solid #1e293b; border-radius:10px; padding:0.9rem; }
      .status { padding:0.65rem; background:#0f172a; border-radius:8px; border:1px solid #1e293b; font-size:0.95rem; }
      .success { color:#4ade80; }
      .error { color:#f87171; }
      canvas { width:100%; }
      small { color:#94a3b8; }
    </style>
    <script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.1/dist/chart.umd.min.js"></script>
  </head>
  <body>
    <header>
      <h1>Backtest Dashboard</h1>
      <p style="margin:0.2rem 0 0; color:#cbd5f5;">Đọc bảng backtest_trades rồi vẽ equity/daily PnL/phân bổ BUY-SELL.</p>
    </header>
    <main>
      <section>
        <div class="grid">
          <div>
            <label>DB URL</label>
            <input id="db-url" placeholder="postgresql+asyncpg://user:pass@host/db" />
          </div>
          <div>
            <label>Symbol</label>
            <input id="symbol" value="XAUUSD" />
          </div>
          <div>
            <label>Saved Config ID A (bắt buộc)</label>
            <select id="saved-config-a">
              <option value="">-- Chọn cấu hình đã lưu --</option>
            </select>
            <small style="color:#cbd5f5;">Cấu hình A để so sánh.</small>
          </div>
          <div>
            <label>Saved Config ID B (bắt buộc)</label>
            <select id="saved-config-b">
              <option value="">-- Chọn cấu hình đã lưu --</option>
            </select>
            <small style="color:#cbd5f5;">Cấu hình B để so sánh.</small>
          </div>
          <div>
            <label>Start</label>
            <input id="start" type="datetime-local" />
          </div>
      <div>
            <label>End</label>
            <input id="end" type="datetime-local" />
          </div>
        </div>
        <div style="display:flex; gap:0.5rem; flex-wrap:wrap;">
          <button id="load-configs-btn" style="background:#22c55e; color:#0b1220;">Tải danh sách cấu hình</button>
          <button id="load-btn">Tải 2 dashboard</button>
        </div>
        <div id="state" class="status" style="margin-top:0.6rem;">Chưa tải dữ liệu.</div>
      </section>

      <section>
        <h2 style="margin:0 0 0.5rem;">Dashboard A</h2>
        <div class="chart-grid">
          <div class="card">
            <h3 style="margin-top:0;">Equity (USD)</h3>
            <p class="note" style="margin:0 0 0.5rem;">Đường tích lũy PnL theo thời gian (lãi/lỗ cộng dồn từng lệnh).</p>
            <canvas id="a-equity-canvas" height="160"></canvas>
          </div>
          <div class="card">
            <h3 style="margin-top:0;">Daily PnL (USD)</h3>
            <p class="note" style="margin:0 0 0.5rem;">Cột lãi/lỗ mỗi ngày để xem phiên nào hiệu quả.</p>
            <canvas id="a-daily-canvas" height="160"></canvas>
          </div>
          <div class="card">
            <h3 style="margin-top:0;">Side breakdown</h3>
            <p class="note" style="margin:0 0 0.5rem;">Tỷ trọng số lệnh BUY/SELL.</p>
            <canvas id="a-side-canvas" height="160"></canvas>
          </div>
          <div class="card">
            <h3 style="margin-top:0;">PnL theo side (USD)</h3>
            <p class="note" style="margin:0 0 0.5rem;">Lãi/lỗ quy đổi USD cho BUY vs SELL.</p>
            <canvas id="a-side-pnl-canvas" height="160"></canvas>
          </div>
          <div class="card">
            <h3 style="margin-top:0;">Lỗ theo side (USD)</h3>
            <p class="note" style="margin:0 0 0.5rem;">Tổng lỗ (chỉ phần âm) của BUY/SELL để tìm hướng yếu.</p>
            <canvas id="a-side-loss-canvas" height="160"></canvas>
          </div>
          <div class="card">
            <h3 style="margin-top:0;">Lãi theo side (USD)</h3>
            <p class="note" style="margin:0 0 0.5rem;">Tổng lãi (chỉ phần dương) của BUY/SELL để biết hướng mạnh.</p>
            <canvas id="a-side-profit-canvas" height="160"></canvas>
          </div>
          <div class="card">
            <h3 style="margin-top:0;">Số lệnh theo giờ</h3>
            <p class="note" style="margin:0 0 0.5rem;">Mật độ giao dịch từng giờ (theo exit_time) để nhận biết phiên hoạt động.</p>
            <canvas id="a-hour-canvas" height="160"></canvas>
          </div>
          <div class="card">
            <h3 style="margin-top:0;">TP/SL theo side</h3>
            <p class="note" style="margin:0 0 0.5rem;">Đếm số lệnh BUY/SELL chạm TP, SL hoặc đóng lý do khác.</p>
            <canvas id="a-tp-sl-canvas" height="160"></canvas>
          </div>
        </div>
      </section>

      <section>
        <h2 style="margin:0 0 0.5rem;">Dashboard B</h2>
        <div class="chart-grid">
          <div class="card">
            <h3 style="margin-top:0;">Equity (USD)</h3>
            <p class="note" style="margin:0 0 0.5rem;">Đường tích lũy PnL theo thời gian (lãi/lỗ cộng dồn từng lệnh).</p>
            <canvas id="b-equity-canvas" height="160"></canvas>
          </div>
          <div class="card">
            <h3 style="margin-top:0;">Daily PnL (USD)</h3>
            <p class="note" style="margin:0 0 0.5rem;">Cột lãi/lỗ mỗi ngày để xem phiên nào hiệu quả.</p>
            <canvas id="b-daily-canvas" height="160"></canvas>
          </div>
          <div class="card">
            <h3 style="margin-top:0;">Side breakdown</h3>
            <p class="note" style="margin:0 0 0.5rem;">Tỷ trọng số lệnh BUY/SELL.</p>
            <canvas id="b-side-canvas" height="160"></canvas>
          </div>
          <div class="card">
            <h3 style="margin-top:0;">PnL theo side (USD)</h3>
            <p class="note" style="margin:0 0 0.5rem;">Lãi/lỗ quy đổi USD cho BUY vs SELL.</p>
            <canvas id="b-side-pnl-canvas" height="160"></canvas>
          </div>
          <div class="card">
            <h3 style="margin-top:0;">Lỗ theo side (USD)</h3>
            <p class="note" style="margin:0 0 0.5rem;">Tổng lỗ (chỉ phần âm) của BUY/SELL để tìm hướng yếu.</p>
            <canvas id="b-side-loss-canvas" height="160"></canvas>
          </div>
          <div class="card">
            <h3 style="margin-top:0;">Lãi theo side (USD)</h3>
            <p class="note" style="margin:0 0 0.5rem;">Tổng lãi (chỉ phần dương) của BUY/SELL để biết hướng mạnh.</p>
            <canvas id="b-side-profit-canvas" height="160"></canvas>
          </div>
          <div class="card">
            <h3 style="margin-top:0;">Số lệnh theo giờ</h3>
            <p class="note" style="margin:0 0 0.5rem;">Mật độ giao dịch từng giờ (theo exit_time) để nhận biết phiên hoạt động.</p>
            <canvas id="b-hour-canvas" height="160"></canvas>
          </div>
          <div class="card">
            <h3 style="margin-top:0;">TP/SL theo side</h3>
            <p class="note" style="margin:0 0 0.5rem;">Đếm số lệnh BUY/SELL chạm TP, SL hoặc đóng lý do khác.</p>
            <canvas id="b-tp-sl-canvas" height="160"></canvas>
          </div>
        </div>
      </section>
    </main>
    <script>
      const stateEl = document.getElementById('state');
      const dbUrlEl = document.getElementById('db-url');
      const symbolEl = document.getElementById('symbol');
      const savedConfigEls = { a: document.getElementById('saved-config-a'), b: document.getElementById('saved-config-b') };
      const startEl = document.getElementById('start');
      const endEl = document.getElementById('end');
      const charts = { a: {}, b: {} };
      let savedCache = [];

      function pad(num) { return String(num).padStart(2, '0'); }
      function formatLocalInput(date) {
        const d = new Date(date);
        if (Number.isNaN(d.getTime())) return '';
        return `${d.getFullYear()}-${pad(d.getMonth()+1)}-${pad(d.getDate())}T${pad(d.getHours())}:${pad(d.getMinutes())}`;
      }
      function normalizeDateInput(value) {
        if (!value) return value;
        const d = new Date(value);
        if (Number.isNaN(d.getTime())) return value;
        const offsetMinutes = -d.getTimezoneOffset();
        const sign = offsetMinutes >= 0 ? '+' : '-';
        const abs = Math.abs(offsetMinutes);
        return `${d.getFullYear()}-${pad(d.getMonth()+1)}-${pad(d.getDate())}T${pad(d.getHours())}:${pad(d.getMinutes())}:${pad(d.getSeconds())}${sign}${pad(Math.floor(abs/60))}:${pad(abs%60)}`;
      }
      function formatNum(val, digits=2) {
        const num = Number(val);
        return Number.isFinite(num) ? num.toFixed(digits) : '-';
      }
      function setState(msg, cls='') {
        stateEl.textContent = msg;
        stateEl.className = cls ? `status ${cls}` : 'status';
      }
      function destroyCharts(prefix) {
        Object.keys(charts[prefix]).forEach(key => {
          const ch = charts[prefix][key];
          if (ch) ch.destroy();
          charts[prefix][key] = null;
        });
      }
      function ctx(prefix, id) {
        const el = document.getElementById(`${prefix}-${id}`);
        return el ? el.getContext('2d') : null;
      }
      function renderCharts(prefix, data) {
        destroyCharts(prefix);
        const eqLabels = (data.equity_curve||[]).map(p => new Date(p.time).toLocaleString());
        const eqVals = (data.equity_curve||[]).map(p => p.equity);
        const store = charts[prefix];

        if (ctx(prefix, 'equity-canvas') && eqLabels.length) {
          store.equity = new Chart(ctx(prefix, 'equity-canvas'), {
            type:'line',
            data:{ labels:eqLabels, datasets:[{ label:'Equity USD', data:eqVals, borderColor:'#38bdf8', backgroundColor:'rgba(56,189,248,0.2)', fill:true, tension:0.25 }]},
            options:{ plugins:{ legend:{ display:false } }, scales:{ x:{ ticks:{ autoSkip:true, maxTicksLimit:8 }}, y:{ beginAtZero:false } } },
          });
        }
        const dailyLabels = (data.daily||[]).map(d => d.date);
        const dailyVals = (data.daily||[]).map(d => d.usd_pnl);
        if (ctx(prefix, 'daily-canvas') && dailyLabels.length) {
          store.daily = new Chart(ctx(prefix, 'daily-canvas'), {
            type:'bar',
            data:{ labels: dailyLabels, datasets:[{ label:'USD PnL', data: dailyVals, backgroundColor: dailyVals.map(v => v>=0 ? 'rgba(74,222,128,0.75)' : 'rgba(248,113,113,0.75)') }]},
            options:{ plugins:{ legend:{ display:false } }, scales:{ y:{ beginAtZero:true } } },
          });
        }
        const sideLabels = (data.by_side||[]).map(s => s.side.toUpperCase());
        const sideVals = (data.by_side||[]).map(s => s.trades);
        if (ctx(prefix, 'side-canvas') && sideLabels.length) {
            store.side = new Chart(ctx(prefix, 'side-canvas'), { type:'doughnut', data:{ labels: sideLabels, datasets:[{ data: sideVals, backgroundColor:['#38bdf8','#fbbf24','#a78bfa'] }] }, options:{ plugins:{ legend:{ position:'bottom' }}}});
        }
        const sidePnlVals = (data.by_side||[]).map(s => s.usd_pnl);
        if (ctx(prefix, 'side-pnl-canvas') && sideLabels.length) {
          store.sidePnl = new Chart(ctx(prefix, 'side-pnl-canvas'), {
            type:'bar',
            data:{ labels: sideLabels, datasets:[{ label:'USD PnL', data: sidePnlVals, backgroundColor: sidePnlVals.map(v => v>=0 ? '#4ade8066' : '#f8717166') }] },
            options:{ plugins:{ legend:{ display:false } }, scales:{ y:{ beginAtZero:true } } },
          });
        }
        const sideLossVals = (data.by_side||[]).map(s => Math.min(0, s.loss_usd_pnl || 0));
        if (ctx(prefix, 'side-loss-canvas') && sideLabels.length) {
          store.sideLoss = new Chart(ctx(prefix, 'side-loss-canvas'), {
            type:'bar',
            data:{ labels: sideLabels, datasets:[{ label:'USD lỗ', data: sideLossVals, backgroundColor:'#f8717166' }] },
            options:{ plugins:{ legend:{ display:false } }, scales:{ y:{ beginAtZero:true } } },
          });
        }
        const sideProfitVals = (data.by_side||[]).map(s => Math.max(0, s.profit_usd_pnl || 0));
        if (ctx(prefix, 'side-profit-canvas') && sideLabels.length) {
          store.sideProfit = new Chart(ctx(prefix, 'side-profit-canvas'), {
            type:'bar',
            data:{ labels: sideLabels, datasets:[{ label:'USD lãi', data: sideProfitVals, backgroundColor:'#4ade8066' }] },
            options:{ plugins:{ legend:{ display:false } }, scales:{ y:{ beginAtZero:true } } },
          });
        }
        const hourLabels = (data.hourly||[]).map(h => String(h.hour).padStart(2,'0'));
        const hourVals = (data.hourly||[]).map(h => h.trades);
        if (ctx(prefix, 'hour-canvas') && hourLabels.length) {
          store.hour = new Chart(ctx(prefix, 'hour-canvas'), {
            type:'bar',
            data:{ labels: hourLabels, datasets:[{ label:'Trades', data: hourVals, backgroundColor:'#38bdf8' }] },
            options:{ plugins:{ legend:{ display:false } }, scales:{ x:{ ticks:{ autoSkip:true, maxTicksLimit:12 }}, y:{ beginAtZero:true, precision:0 } } },
          });
        }
        const tpSlData = data.tp_sl || [];
        if (ctx(prefix, 'tp-sl-canvas') && tpSlData.length) {
          const labels = tpSlData.map(item => item.side.toUpperCase());
          const tpCounts = tpSlData.map(item => item.tp || 0);
          const slCounts = tpSlData.map(item => item.sl || 0);
          const otherCounts = tpSlData.map(item => item.other || 0);
          store.tpSl = new Chart(ctx(prefix, 'tp-sl-canvas'), {
            type:'bar',
            data:{ labels, datasets:[
              { label:'TP', data: tpCounts, backgroundColor:'#4ade8066' },
              { label:'SL', data: slCounts, backgroundColor:'#f8717166' },
              { label:'Khác', data: otherCounts, backgroundColor:'#94a3b866' },
            ]},
            options:{ responsive:true, plugins:{ legend:{ position:'bottom' } }, scales:{ x:{ stacked:true }, y:{ stacked:true, beginAtZero:true, precision:0 } } },
          });
        }
      }

      async function loadSavedConfigs() {
        const dbUrl = dbUrlEl.value.trim();
        if (!dbUrl) { alert('Nhập DB URL trước khi tải cấu hình.'); return; }
        setState('Đang tải danh sách cấu hình...', '');
        try {
          const res = await fetch(`/api/backtest/saved-list?db_url=${encodeURIComponent(dbUrl)}&limit=200`);
          const data = await res.json();
          if (!res.ok) throw new Error(data.detail || 'Không lấy được danh sách cấu hình');
          savedCache = data.items || [];
          ['a','b'].forEach(key => {
            const selectEl = savedConfigEls[key];
            selectEl.innerHTML = '<option value="">-- Chọn cấu hình đã lưu --</option>';
            savedCache.forEach((item) => {
              const label = `#${item.id} ${item.symbol || ''} ${item.timeframe || ''} (fast=${item.fast}, slow=${item.slow})`;
              const opt = document.createElement('option');
              opt.value = item.id;
              opt.textContent = label;
              selectEl.appendChild(opt);
            });
          });
          setState(`Đã tải ${savedCache.length} cấu hình.`, 'success');
        } catch (err) {
          setState(err.message || 'Lỗi tải cấu hình', 'error');
        }
      }

      async function loadDashboard() {
        const dbUrl = dbUrlEl.value.trim();
        if (!dbUrl) { alert('Nhập DB URL trước.'); return; }
        const start = normalizeDateInput(startEl.value);
        const end = normalizeDateInput(endEl.value);
        const jobs = ['a','b'].map(async (key) => {
          const savedId = savedConfigEls[key].value ? Number(savedConfigEls[key].value) : null;
          if (!savedId) return { key, summary: `Bảng ${key.toUpperCase()}: chưa chọn Saved Config ID` };
          const payload = { db_url: dbUrl, symbol: symbolEl.value.trim() || 'XAUUSD', saved_config_id: savedId, start, end };
          const res = await fetch('/api/backtest/dashboard', {
            method:'POST',
            headers:{ 'Content-Type':'application/json' },
            body: JSON.stringify(payload),
          });
          const data = await res.json();
          if (!res.ok) throw new Error(data.detail || `Lỗi tải dashboard ${key.toUpperCase()}`);
          renderCharts(key, data);
          const rangeNote = data.min_run_start && data.max_run_end
            ? `${data.min_run_start} → ${data.max_run_end}`
            : 'Không có thời gian chạy';
          return { key, summary: `Bảng ${key.toUpperCase()}: ${data.total_trades} lệnh | Win-rate ${Number(data.win_rate).toFixed(1)}% | USD ${formatNum(data.usd_pnl)} | ${rangeNote}` };
        });
        setState('Đang tải dữ liệu A/B...', '');
        try {
          const results = await Promise.allSettled(jobs);
          const msgs = results.map(r => r.status === 'fulfilled'
            ? (r.value?.summary || '')
            : (r.reason?.message || 'Lỗi'));
          setState(msgs.filter(Boolean).join(' || '), 'success');
        } catch (err) {
          setState(err.message || 'Lỗi tải dashboard', 'error');
        }
      }

      document.getElementById('load-configs-btn').addEventListener('click', loadSavedConfigs);
      document.getElementById('load-btn').addEventListener('click', loadDashboard);
      // auto set default start/end = 7 ngày gần nhất
      const now = new Date();
      const weekAgo = new Date(now.getTime() - 7*24*60*60*1000);
      startEl.value = formatLocalInput(weekAgo);
      endEl.value = formatLocalInput(now);
</script>
  </body>
</html>
"""




BREAKOUT_DASHBOARD_HTML = """
<!doctype html>
<html lang="vi">
  <head>
    <meta charset="utf-8" />
    <title>Breakout Strategy Planner</title>
    <style>
      :root {
        color-scheme: dark;
      }
      body { font-family: 'Inter', Arial, sans-serif; margin: 24px; background-color: #020617; color: #e2e8f0; }
      h1, h2, h3 { margin-bottom: 0.4rem; }
      section { border: 1px solid #1d273b; padding: 1.25rem; border-radius: 10px; background: #0f172a; margin-bottom: 1.5rem; }
      .grid { display: grid; gap: 1rem; grid-template-columns: repeat(auto-fit, minmax(220px, 1fr)); }
      .flex { display: flex; flex-wrap: wrap; gap: 1rem; }
      label { font-size: 0.85rem; opacity: 0.8; }
      input, select { width: 100%; padding: 0.45rem 0.55rem; background: #020617; color: #f8fafc; border: 1px solid #1e293b; border-radius: 6px; margin-top: 0.25rem; }
      button { padding: 0.65rem 1.5rem; border-radius: 999px; border: none; background: linear-gradient(120deg, #34d399, #38bdf8); color: #041223; font-weight: 600; cursor: pointer; }
      button.secondary { background: #1e293b; color: #e2e8f0; }
      .note { font-size: 0.85rem; opacity: 0.9; }
      .list { margin: 0.4rem 0 0 1rem; padding: 0; }
      .list li { margin-bottom: 0.25rem; }
      .card { background: #020617; border-radius: 10px; padding: 0.75rem; border: 1px solid #1e293b; }
      .badge { display: inline-block; padding: 0.15rem 0.5rem; font-size: 0.75rem; border-radius: 999px; background: #1e40af; }
      #plan-output, #quote-box { background: #020617; border: 1px dashed #334155; padding: 0.75rem; border-radius: 8px; font-size: 0.9rem; min-height: 100px; }
      table { width: 100%; border-collapse: collapse; margin-top: 0.5rem; font-size: 0.9rem; }
      th, td { padding: 0.35rem 0.45rem; border-bottom: 1px solid #1e293b; text-align: left; }
      code { background: #1e293b; padding: 0.2rem 0.35rem; border-radius: 4px; }
    </style>
  </head>
  <body>
    <h1>Breakout Strategy Planner</h1>
    <p class="note">
      Chiến lược Breakout tập trung vào việc bắt đầu xu hướng mới ngay khi giá phá vỡ vùng tích lũy quan trọng. Trang này giúp bạn lập kế hoạch giao dịch
      (entry/stop/take-profit, sizing) dựa trên vùng hỗ trợ/kháng cự vừa quan sát được và theo dõi quote XAUUSD thời gian thực.
    </p>

    <section>
      <h2>1. Tư duy chiến lược</h2>
      <div class="grid">
        <div class="card">
          <span class="badge">Bước 1</span>
          <h3>Xác định phạm vi</h3>
          <p>Chọn khung thời gian chính, quan sát vùng tích lũy gần nhất (support/resistance), đo độ cao range để ước lượng biên độ breakout.</p>
        </div>
        <div class="card">
          <span class="badge">Bước 2</span>
          <h3>Xác nhận tín hiệu</h3>
          <p>Dùng khối lượng hoặc biến động gia tốc (ATR, % thay đổi) để tránh phá vỡ giả. Cần ít nhất <strong>n</strong> nến đóng cửa ngoài vùng.</p>
        </div>
        <div class="card">
          <span class="badge">Bước 3</span>
          <h3>Quản trị rủi ro</h3>
          <p>Đặt stop-loss ở mép đối diện range hoặc dưới hỗ trợ cũ/kháng cự cũ. Chốt lời dựa trên RR cố định hoặc vùng cản kế tiếp.</p>
        </div>
        <div class="card">
          <span class="badge">Bước 4</span>
          <h3>Theo dõi</h3>
          <p>Cập nhật quote realtime, ghi chú lý do vào lệnh và yếu tố vô hiệu (invalidations) để cải thiện kỷ luật chiến lược.</p>
        </div>
      </div>
    </section>

    <section>
      <h2>2. Lập kế hoạch Breakout</h2>
      <form id="breakout-form">
        <div class="grid">
          <div>
            <label>Symbol</label>
            <input name="symbol" value="__DEFAULT_SYMBOL__" />
          </div>
          <div>
            <label>Timeframe chính</label>
            <input name="timeframe" value="15min" />
          </div>
          <div>
            <label>Hướng giao dịch</label>
            <select name="direction">
              <option value="bullish">Breakout tăng (mua)</option>
              <option value="bearish">Breakdown giảm (bán)</option>
            </select>
          </div>
          <div>
            <label>Số nến quan sát (lookback)</label>
            <input type="number" name="lookback" value="40" min="5" />
          </div>
        </div>
        <div class="grid" style="margin-top: 1rem;">
          <div>
            <label>Kháng cự gần nhất</label>
            <input type="number" step="any" name="resistance" placeholder="Ví dụ 2435.0" />
          </div>
          <div>
            <label>Hỗ trợ gần nhất</label>
            <input type="number" step="any" name="support" placeholder="Ví dụ 2422.0" />
          </div>
          <div>
            <label>Biên đệm breakout (%)</label>
            <input type="number" step="any" name="buffer_pct" value="0.15" />
          </div>
          <div>
            <label>Xác nhận khối lượng (hệ số)</label>
            <input type="number" step="any" name="volume_mult" value="1.5" />
          </div>
        </div>
        <div class="grid" style="margin-top: 1rem;">
          <div>
            <label>Số nến đóng cửa xác nhận</label>
            <input type="number" name="confirmation" value="2" min="1" />
          </div>
          <div>
            <label>Khoảng cách Stop (USD)</label>
            <input type="number" step="any" name="stop_offset" value="1.5" />
          </div>
          <div>
            <label>Tỷ lệ RR mục tiêu</label>
            <input type="number" step="any" name="rr" value="2.0" />
          </div>
          <div>
            <label>Risk % / Trade</label>
            <input type="number" step="any" name="risk_pct" value="0.02" />
          </div>
        </div>
        <div class="grid" style="margin-top: 1rem;">
          <div>
            <label>Capital (USD)</label>
            <input type="number" name="capital" value="10000" />
          </div>
          <div>
            <label>Contract size (oz/lot)</label>
            <input type="number" name="contract" value="100" />
          </div>
          <div>
            <label>Trailing kích hoạt (ATR)</label>
            <input type="number" step="any" name="trail_atr" value="1.0" />
          </div>
          <div>
            <label>Ghi chú</label>
            <input name="notes" placeholder="Tin tức, vùng invalidate..." />
          </div>
        </div>
        <div style="margin-top: 1rem;" class="flex">
          <button type="submit">Tạo kế hoạch</button>
          <button type="button" class="secondary" id="clear-btn">Xóa</button>
        </div>
      </form>
      <div style="margin-top: 1rem;">
        <h3>Kế hoạch giao dịch</h3>
        <div id="plan-output">Nhập vùng giá + tham số để tính toán entry/stop/take-profit.</div>
      </div>
    </section>

    <section>
      <h2>3. Quote realtime & checklist</h2>
      <div class="grid">
        <div>
          <h3>Quote XAUUSD</h3>
          <div id="quote-box">Đang tải...</div>
        </div>
        <div>
          <h3>Checklist Breakout</h3>
          <table>
            <tbody>
              <tr><td>Range rõ ràng?</td><td id="chk-range">-</td></tr>
              <tr><td>Khối lượng tăng ≥ hệ số?</td><td id="chk-volume">-</td></tr>
              <tr><td>Nến đóng ngoài vùng ≥ yêu cầu?</td><td id="chk-close">-</td></tr>
              <tr><td>Tin tức lớn?</td><td id="chk-news">Theo dõi lịch</td></tr>
              <tr><td>Stop đặt đúng logic?</td><td id="chk-stop">-</td></tr>
            </tbody>
          </table>
        </div>
        <div>
          <h3>Ghi chú</h3>
          <div id="note-box" class="card" style="min-height: 100px;">Thêm ghi chú chiến lược ở mục trên để hiển thị tại đây.</div>
        </div>
      </div>
    </section>

    <section>
      <h2>4. Hướng dẫn triển khai thực tế</h2>
      <ul class="list">
        <li>Sử dụng form trên để xác định tham số nhanh trước khi triển khai chiến lược thật (CLI hoặc dashboard MA hiện tại).</li>
        <li>Khi cần tự động hóa: triển khai <code>breakout_strategy</code> riêng (logic: quét range, xác nhận breakout, gửi lệnh) và tái sử dụng API / storage sẵn có.</li>
        <li>Nên backtest bằng <code>app.cli</code> với dữ liệu tick hiện hữu để điều chỉnh buffer, RR và xác nhận khối lượng phù hợp với từng phiên.</li>
      </ul>
    </section>

    <script>
      const form = document.getElementById('breakout-form');
      const planOutput = document.getElementById('plan-output');
      const quoteBox = document.getElementById('quote-box');
      const clearBtn = document.getElementById('clear-btn');
      const checklist = {
        range: document.getElementById('chk-range'),
        volume: document.getElementById('chk-volume'),
        close: document.getElementById('chk-close'),
        stop: document.getElementById('chk-stop'),
      };
      const noteBox = document.getElementById('note-box');

      form.addEventListener('submit', (e) => {
        e.preventDefault();
        const data = Object.fromEntries(new FormData(form).entries());
        const parsed = parseConfig(data);
        if (!parsed) return;
        const plan = buildPlan(parsed);
        renderPlan(plan);
      });

      clearBtn.addEventListener('click', () => {
        form.reset();
        planOutput.textContent = 'Nhập vùng giá + tham số để tính toán entry/stop/take-profit.';
        Object.values(checklist).forEach((el) => (el.textContent = '-'));
        noteBox.textContent = 'Thêm ghi chú chiến lược ở mục trên để hiển thị tại đây.';
      });

      function parseConfig(data) {
        const required = ['resistance', 'support'];
        for (const key of required) {
          if (!data[key]) {
            planOutput.textContent = 'Vui lòng nhập đầy đủ hỗ trợ/kháng cự.';
            return null;
          }
        }
        const config = {
          symbol: data.symbol || '__DEFAULT_SYMBOL__',
          timeframe: data.timeframe || '15min',
          direction: data.direction,
          resistance: parseFloat(data.resistance),
          support: parseFloat(data.support),
          bufferPct: parseFloat(data.buffer_pct) / 100,
          volumeMult: parseFloat(data.volume_mult),
          confirmation: parseInt(data.confirmation, 10),
          stopOffset: parseFloat(data.stop_offset),
          rr: parseFloat(data.rr),
          riskPct: parseFloat(data.risk_pct),
          capital: parseFloat(data.capital),
          contract: parseFloat(data.contract),
          lookback: parseInt(data.lookback, 10),
          trailAtr: parseFloat(data.trail_atr),
          notes: data.notes || '',
        };
        if (!Number.isFinite(config.bufferPct) || config.bufferPct < 0) config.bufferPct = 0;
        return config;
      }

      function buildPlan(cfg) {
        const rangeHeight = Math.max(cfg.resistance - cfg.support, 0);
        const buffer = rangeHeight * cfg.bufferPct;
        const directionFactor = cfg.direction === 'bearish' ? -1 : 1;
        const breakoutLevel = cfg.direction === 'bearish' ? cfg.support : cfg.resistance;
        const entry = breakoutLevel + directionFactor * buffer;
        const stop = cfg.direction === 'bearish'
          ? cfg.resistance + cfg.stopOffset
          : cfg.support - cfg.stopOffset;
        const stopDistance = Math.max(Math.abs(entry - stop), 0.01);
        const tpDistance = rangeHeight * cfg.rr;
        const takeProfit = cfg.direction === 'bearish' ? entry - tpDistance : entry + tpDistance;
        const riskCapital = cfg.capital * cfg.riskPct;
        const pipValue = cfg.contract;
        const perLotRisk = stopDistance * pipValue;
        const volume = perLotRisk > 0 ? Math.min(riskCapital / perLotRisk, 10) : 0;
        return {
          cfg,
          rangeHeight,
          entry,
          stop,
          takeProfit,
          volume: Number(volume.toFixed(2)),
          stopDistance,
          tpDistance,
        };
      }

      function renderPlan(plan) {
        const {
          cfg,
          rangeHeight,
          entry,
          stop,
          takeProfit,
          volume,
          stopDistance,
        } = plan;
        const rr = plan.tpDistance / stopDistance;
        const summary = [
          `<strong>Symbol:</strong> ${cfg.symbol} (${cfg.timeframe})`,
          `<strong>Hướng:</strong> ${cfg.direction === 'bullish' ? 'Breakout tăng' : 'Breakdown giảm'}`,
          `<strong>Range Height:</strong> ${rangeHeight.toFixed(2)} USD`,
          `<strong>Entry gợi ý:</strong> ${entry.toFixed(2)}`,
          `<strong>Stop-loss:</strong> ${stop.toFixed(2)} (khoảng ${stopDistance.toFixed(2)} USD)`,
          `<strong>Take-profit:</strong> ${takeProfit.toFixed(2)} (RR ≈ ${rr.toFixed(2)})`,
          `<strong>Khối lượng đề xuất:</strong> ${volume} lot (risk ${(cfg.riskPct * 100).toFixed(2)}% trên capital ${cfg.capital})`,
          `<strong>Trailing kích hoạt khi ATR ≥</strong> ${cfg.trailAtr}`,
        ];
        planOutput.innerHTML = summary.join('<br />');
        checklist.range.textContent = cfg.lookback >= 30 ? 'OK - range rõ' : 'Xem xét lại';
        checklist.volume.textContent = `${cfg.volumeMult}x`;
        checklist.close.textContent = `>= ${cfg.confirmation} nến`;
        checklist.stop.textContent = stopDistance > 0 ? 'Đã tính' : 'Chưa hợp lệ';
        noteBox.textContent = cfg.notes || 'Chưa có ghi chú.';
      }

      async function refreshQuote() {
        try {
          const res = await fetch('/quotes/xauusd');
          if (!res.ok) throw new Error('HTTP ' + res.status);
          const data = await res.json();
          quoteBox.innerHTML = [
            `Giá: <strong>${Number(data.price || data.bid || 0).toFixed(2)}</strong>`,
            `Bid / Ask: ${Number(data.bid || 0).toFixed(2)} / ${Number(data.ask || 0).toFixed(2)}`,
            `Cập nhật: ${data.updated_at ? formatTimestamp(data.updated_at) : '-'}`,
          ].join('<br />');
        } catch (err) {
          quoteBox.textContent = 'Không thể tải quote: ' + err.message;
        }
      }
      refreshQuote();
      setInterval(refreshQuote, 5000);
    </script>
  </body>
</html>
""".replace("__DEFAULT_SYMBOL__", settings.quote_symbol)


if __name__ == "__main__":
    import uvicorn

    uvicorn.run("app.main:app", host="0.0.0.0", port=8000, reload=True)
