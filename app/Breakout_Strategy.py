"""Donchian breakout strategy implementation (Donchian 20 + EMA200)."""
from dataclasses import dataclass
from datetime import datetime, timezone, timedelta, date
import pandas as pd
import numpy as np
import logging
import inspect
import asyncio
from collections import defaultdict, Counter
from typing import Any, Awaitable, Callable, List, Tuple, Dict, Optional

try:  # MetaTrader5 chỉ khả dụng khi cài đặt trên Windows
    import MetaTrader5 as mt5  # type: ignore
except ImportError:  # pragma: no cover - môi trường không có MT5
    mt5 = None  # type: ignore

from .indicators import get_ma_series
from .storage import Storage
from .quote_service import QuoteService
from .config import Settings


EventHandler = Callable[[Dict[str, Any]], Awaitable[None] | None]


@dataclass
class MAConfig:
    """Donchian breakout configuration."""

    # Chiến lược chính
    symbol: str = "XAUUSD"
    timeframe: str = "1H"
    donchian_period: int = 20
    ema_trend_period: int = 200
    atr_period: int = 14
    entry_buffer_points: float = 0.0
    exit_on_opposite: bool = True
    breakeven_after_rr: Optional[float] = None

    # Vốn & khối lượng
    capital: float = 100.0
    risk_pct: float = 0.015
    size_from_risk: bool = True
    contract_size: float = 100.0
    pip_size: float = 0.1
    volume: float = 0.01
    min_volume: float = 0.01
    volume_step: float = 0.01
    max_positions: int = 1

    # Quản lý lệnh
    sl_atr: float = 2.0
    tp_atr: float = 5.0
    trail_trigger_atr: float = 2.0
    trail_atr_mult: float = 2.2
    breakeven_atr: float = 1.5
    partial_close: bool = True
    partial_close_atr: float = 2.8
    ignore_gaps: bool = True
    closed_sessions: Optional[List[str]] = None

    # Lọc phiên / biến động
    trading_hours: Optional[List[str]] = None
    min_atr_multiplier: float = 0.8
    max_atr_multiplier: float = 3.0
    max_spread_points: float = 50.0
    allowed_deviation_points: float = 30.0
    slippage_points: float = 0.0
    spread_samples: int = 10
    spread_sample_delay_ms: int = 100

    # Risk control
    max_daily_loss: Optional[float] = None
    max_loss_streak: Optional[int] = None
    max_losses_per_session: Optional[int] = None
    cooldown_minutes: Optional[int] = None
    session_cooldown_minutes: int = 0

    # Hạ tầng
    order_retry_times: int = 5
    order_retry_delay_ms: int = 500
    magic_number: int = 20251230
    paper_mode: bool = True  # giữ để tương thích
    sl_pips: Optional[float] = None
    tp_pips: Optional[float] = None
    live: bool = False


@dataclass
class Position:
    symbol: str
    type: str  # 'buy' hoặc 'sell'
    volume: float
    open_price: float
    open_time: datetime
    stop_loss: float
    take_profit: float
    order_id: Optional[int] = None
    trailing_active: bool = False
    realized_pnl: float = 0.0
    partial_closes: int = 0


class MACrossoverStrategy:
    """Donchian breakout trading strategy."""
    
    def __init__(
        self,
        config: MAConfig,
        quote_service: QuoteService,
        storage: Storage,
        event_handler: Optional[EventHandler] = None,
    ):
        self.config = config
        self.quote_service = quote_service
        self.storage = storage
        self.logger = logging.getLogger(__name__)
        self._tz = timezone(timedelta(hours=0))
        
        # Trạng thái
        self.current_position: Optional[Position] = None
        self._last_update = None
        self._df = None
        self._running = False
        self._event_handler = event_handler
        self._risk_day: Optional[date] = None
        self._daily_pnl: float = 0.0
        self._loss_streak: int = 0
        self._session_losses: Dict[str, int] = {}
        self._cooldown_until: Optional[datetime] = None
        # Debug counters for live visibility
        self._debug_counters: Dict[str, int] = defaultdict(int)
        self._debug_reasons: List[str] = []
        self._sample_bars_logged = 0
        self._MAX_SAMPLE_BARS = 20

    async def start(self):
        """Bắt đầu chạy chiến lược."""
        if self._running:
            return
            
        self._running = True
        self.logger.info(f"Bắt đầu chiến lược Donchian cho {self.config.symbol}")
        
        # Subscribe để nhận dữ liệu
        await self.quote_service.subscribe(self.config.symbol, self._on_quote)
        
    async def stop(self):
        """Dừng chiến lược."""
        if not self._running:
            return
            
        self._running = False
        await self.quote_service.unsubscribe(self.config.symbol, self._on_quote)
        self.logger.info(f"Đã dừng chiến lược MA Crossover cho {self.config.symbol}")
        
    async def _on_quote(self, symbol: str, bid: float, ask: float):
        """Xử lý khi có quote mới."""
        if not self._running or symbol != self.config.symbol:
            return
            
        now_utc = datetime.now(timezone.utc)
        now_local = now_utc.astimezone(self._tz)
        self.logger.debug("Quote %s bid=%.5f ask=%.5f @ %s", symbol, bid, ask, now_local.isoformat())
        if now_local.weekday() >= 5:
            await self._emit_status("Bỏ qua quote cuối tuần", {"now": now_local.isoformat()})
            return
        if (not self._last_update or
            (now_utc - self._last_update).total_seconds() > 300):
            await self._update_data()
        if self._df is None or self._df.empty:
            await self._emit_status("Chưa có dữ liệu Donchian để xử lý quote")
            return

        if not self._within_trading_hours(now_local):
            await self._emit_status(
                "Ngoài khung giờ giao dịch",
                {"now": now_local.strftime("%H:%M:%S")},
            )
            return
        self._reset_risk_counters(now_local)

        # Tính giá trung bình bid/ask
        price = (bid + ask) / 2
        spread = ask - bid
        current_bar = self._df.iloc[-1]

        # Kiểm tra điều kiện đóng lệnh nếu đang có vị thế
        if self.current_position:
            await self._manage_open_position(current_bar, price)
            return

        # Kiểm tra tín hiệu mở lệnh mới nếu chưa có vị thế
        if not self.current_position:
            max_spread_points = float(getattr(self.config, "max_spread_points", 0) or 0)
            if max_spread_points > 0 and spread > max_spread_points * self.config.pip_size:
                await self._emit_status(f"Spread {spread:.5f} vượt ngưỡng {max_spread_points}")
                return
            allowed, guard_reason = self._risk_guard_allows(now_local)
            if not allowed:
                await self._emit_status(guard_reason or "Risk guard đang kích hoạt", {"now": now_local.isoformat()})
                return

            buy_signal, sell_signal, reason = self._check_signals(current_bar)

            if not buy_signal and not sell_signal:
                await self._emit_status(reason)
                return

            if buy_signal:
                await self._open_position('buy', ask)  # Mua ở giá ask
            elif sell_signal:
                await self._open_position('sell', bid)  # Bán ở giá bid
                
    async def _update_data(self) -> None:
        """Cập nhật dữ liệu và tính toán chỉ báo."""
        end_time = datetime.now(timezone.utc)
        start_time = end_time - self._calculate_lookback_duration()
        self._df = await self._build_indicator_df(start_time, end_time)
        self._last_update = end_time

    def _enrich_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        """Bổ sung các cột phục vụ breakout Donchian."""
        if df.empty:
            return df

        df = df.copy()
        donchian_period = max(2, int(getattr(self.config, "donchian_period", 20)))
        df["donchian_high"] = df["high"].rolling(window=donchian_period, min_periods=donchian_period).max().shift(1)
        df["donchian_low"] = df["low"].rolling(window=donchian_period, min_periods=donchian_period).min().shift(1)

        ema_trend_period = max(2, int(getattr(self.config, "ema_trend_period", 200)))
        df["ema_trend"] = df["close"].ewm(span=ema_trend_period, adjust=False).mean()

        atr_window = max(2, int(getattr(self.config, "atr_period", 14)))
        df["atr_baseline"] = df["atr"].rolling(window=atr_window, min_periods=1).mean()

        return df
        
    def _check_signals(self, current_bar: pd.Series) -> Tuple[bool, bool, str]:
        """Kiểm tra tín hiệu breakout Donchian."""
        if self._df is None or len(self._df) < 2:
            return False, False, "Chưa đủ dữ liệu để tính breakout"
        return self._evaluate_breakout_signal(self._df, len(self._df) - 1)

    def _evaluate_breakout_signal(self, df: pd.DataFrame, idx: int) -> Tuple[bool, bool, str]:
        if idx <= 0 or df is None or df.empty:
            return False, False, "Chưa đủ dữ liệu breakout"

        current = df.iloc[idx]
        self._debug_counters["bars_checked"] += 1
        range_high = current.get("donchian_high")
        range_low = current.get("donchian_low")

        if pd.isna(range_high) or pd.isna(range_low):
            ignore_gaps = bool(getattr(self.config, "ignore_gaps", False))
            donchian_period = max(2, int(getattr(self.config, "donchian_period", 20)))
            valid_range = df[["donchian_high", "donchian_low"]].dropna()
            bars_ready = len(valid_range)
            if ignore_gaps and bars_ready >= donchian_period and idx > 0:
                fallback_high = df["donchian_high"].ffill().iloc[idx]
                fallback_low = df["donchian_low"].ffill().iloc[idx]
                if not pd.isna(fallback_high) and not pd.isna(fallback_low):
                    range_high, range_low = fallback_high, fallback_low
                else:
                    ignore_gaps = False  # fallback failed
            if pd.isna(range_high) or pd.isna(range_low):
                self._debug_counters["missing_range"] += 1
                try:
                    current_dt = pd.to_datetime(current.get("datetime")).to_pydatetime()
                    tf_min = self._timeframe_minutes()
                    need_from = current_dt - timedelta(minutes=donchian_period * tf_min)
                    have_from = pd.to_datetime(df.iloc[0].get("datetime")).to_pydatetime()
                    gap_info = f"thiếu dữ liệu từ {need_from.isoformat()} (hiện có từ {have_from.isoformat()})"
                    recent_na = df[["datetime", "donchian_high", "donchian_low"]].iloc[max(0, idx-3):idx+1]
                    na_detail = recent_na.to_dict("records")
                except Exception:
                    gap_info = "thiếu dữ liệu đầu kỳ"
                    na_detail = []
                self._debug_reasons.append("Chưa có range đủ dài")
                return False, False, f"Chưa xác định vùng sideway đủ dài (range_ready={bars_ready}, cần >= {donchian_period}; {gap_info}; near={na_detail})"

        atr_value = float(current.get('atr') or 0.0)
        if atr_value <= 0 or np.isnan(atr_value):
            self._debug_counters["atr_invalid"] += 1
            self._debug_reasons.append("ATR không hợp lệ")
            return False, False, "ATR không hợp lệ"

        atr_baseline = float(current.get("atr_baseline") or 0.0)
        if atr_baseline <= 0 or np.isnan(atr_baseline):
            atr_baseline = atr_value

        atr_min_mult = max(0.1, float(getattr(self.config, "min_atr_multiplier", 0.8)))
        atr_max_mult = max(atr_min_mult + 0.1, float(getattr(self.config, "max_atr_multiplier", 4.0)))
        atr_min = atr_baseline * atr_min_mult
        atr_max = atr_baseline * atr_max_mult
        if atr_value < atr_min:
            self._debug_counters["atr_low"] += 1
            self._debug_reasons.append("ATR thấp")
            return False, False, f"ATR {atr_value:.2f} thấp hơn ngưỡng {atr_min:.2f}"
        if atr_value > atr_max:
            self._debug_counters["atr_high"] += 1
            self._debug_reasons.append("ATR cao")
            return False, False, f"ATR {atr_value:.2f} vượt ngưỡng {atr_max:.2f} (quá biến động)"

        range_height = range_high - range_low
        buffer_pts = float(getattr(self.config, "entry_buffer_points", 0.0) or 0.0)
        buffer = buffer_pts * self.config.pip_size

        bullish_ready = current.close > (range_high + buffer)
        bearish_ready = current.close < (range_low - buffer)
        # Giảm spam log: nếu vẫn trong range và trạng thái không đổi thì không log thêm
        cur_state = "bull" if bullish_ready else "bear" if bearish_ready else "none"
        last_state = getattr(self, "_last_ready_state", None)
        self._last_ready_state = cur_state

        # EMA trend filter
        reason = f"Range {range_low:.2f}-{range_high:.2f}"
        if not bullish_ready and not bearish_ready:
            self._debug_counters["no_breakout"] += 1
            self._debug_reasons.append("Chưa phá range")
            if cur_state == "none" and last_state == "none":
                return False, False, ""
            return False, False, f"{reason} | close {current.close:.2f} chưa phá vùng (buffer {buffer:.2f})"

        trend_val = current.get("ema_trend")
        if trend_val is not None and not np.isnan(trend_val):
            if bullish_ready and current.close < trend_val:
                bullish_ready = False
                reason = "Chưa vượt EMA trend"
                self._debug_reasons.append("Chưa vượt EMA trend")
                self._debug_counters["trend_filter"] += 1
            if bearish_ready and current.close > trend_val:
                bearish_ready = False
                reason = "Chưa nằm dưới EMA trend"
                self._debug_reasons.append("Chưa nằm dưới EMA trend")
                self._debug_counters["trend_filter"] += 1

        if bullish_ready:
            self._debug_counters["entry_allowed"] += 1
            self._maybe_log_debug_counter()
            return True, False, f"Breakout BUY xác nhận @ {current.close:.2f}"
        if bearish_ready:
            self._debug_counters["entry_allowed"] += 1
            self._maybe_log_debug_counter()
            return False, True, f"Breakout SELL xác nhận @ {current.close:.2f}"
        self._maybe_log_debug_counter()
        return False, False, reason

    async def _manage_open_position(self, current_bar: pd.Series, price: float) -> None:
        """Quản lý vị thế đang mở: trailing, partial close, kiểm tra thoát."""
        self._maybe_trail_stop(current_bar, price)
        await self._maybe_partial_close(current_bar, price)
        if self._check_exit(price):
            await self._close_position(price)

    def _maybe_trail_stop(self, current_bar: pd.Series, price: float) -> None:
        if not self.current_position:
            return
        trigger = float(getattr(self.config, "trail_trigger_atr", 0.0) or 0.0)
        mult = float(getattr(self.config, "trail_atr_mult", 0.0) or 0.0)
        breakeven_atr = float(getattr(self.config, "breakeven_atr", 0.0) or 0.0)
        breakeven_rr = getattr(self.config, "breakeven_after_rr", None)
        atr = current_bar.atr if hasattr(current_bar, "atr") else None
        if atr is None or np.isnan(atr) or atr <= 0:
            return
        pos = self.current_position
        move = price - pos.open_price if pos.type == 'buy' else pos.open_price - price
        # Breakeven
        if breakeven_atr > 0 and move >= breakeven_atr * atr:
            if pos.type == 'buy' and pos.stop_loss < pos.open_price:
                pos.stop_loss = pos.open_price
            elif pos.type == 'sell' and pos.stop_loss > pos.open_price:
                pos.stop_loss = pos.open_price
            pos.trailing_active = True
        if breakeven_rr:
            risk_points = abs(pos.open_price - pos.stop_loss)
            if risk_points > 0 and move >= breakeven_rr * risk_points:
                if pos.type == "buy":
                    pos.stop_loss = max(pos.stop_loss, pos.open_price)
                else:
                    pos.stop_loss = min(pos.stop_loss, pos.open_price)
                pos.trailing_active = True
        if trigger <= 0 or mult <= 0 or move < trigger * atr:
            return
        if pos.type == 'buy':
            new_stop = price - mult * atr
            if new_stop > pos.stop_loss and new_stop < price:
                pos.stop_loss = new_stop
                pos.trailing_active = True
        else:
            new_stop = price + mult * atr
            if new_stop < pos.stop_loss and new_stop > price:
                pos.stop_loss = new_stop
                pos.trailing_active = True

    async def _maybe_partial_close(self, current_bar: pd.Series, price: float) -> None:
        """Thực hiện chốt lời một phần khi đạt ngưỡng ATR."""
        if not self.current_position or not getattr(self.config, "partial_close", False):
            return
        pos = self.current_position
        if pos.partial_closes > 0:
            return
        threshold = float(getattr(self.config, "partial_close_atr", 0.0) or 0.0)
        if threshold <= 0:
            return
        atr_val = getattr(current_bar, "atr", None)
        if atr_val is None or np.isnan(atr_val) or atr_val <= 0:
            return
        move = price - pos.open_price if pos.type == "buy" else pos.open_price - price
        if move < threshold * atr_val:
            return

        vol_step = max(float(getattr(self.config, "volume_step", 0.01) or 0.01), 0.01)
        min_vol = max(float(getattr(self.config, "min_volume", vol_step) or vol_step), vol_step)
        # đóng 50% khối lượng, chuẩn hoá theo step/min
        target_close = pos.volume / 2
        normalized = max(min_vol, round(target_close / vol_step) * vol_step)
        close_vol = min(normalized, pos.volume)
        if close_vol <= 0 or close_vol >= pos.volume:
            return

        contract_size = float(getattr(self.config, "contract_size", 1.0) or 1.0)
        realized = move * close_vol * contract_size
        if not getattr(self.config, "paper_mode", True) and pos.order_id:
            await self._close_partial_mt5(close_vol, price, pos.type, pos.order_id)

        pos.volume -= close_vol
        pos.realized_pnl += realized
        pos.partial_closes += 1
        # Khoá lợi nhuận ở mức entry
        pos.stop_loss = pos.open_price

        payload = {
            "type": "partial_close",
            "timestamp": datetime.now(timezone.utc),
            "symbol": pos.symbol,
            "side": pos.type,
            "price": price,
            "closed_volume": close_vol,
            "remaining_volume": pos.volume,
            "realized_pnl": realized,
        }
        await self._emit_event(payload)
        await self._persist_trade_event("partial_close", payload)

    @staticmethod
    def _compute_rsi(series: pd.Series, window: int) -> pd.Series:
        return pd.Series(50.0, index=series.index)

    @staticmethod
    def _compute_adx(df: pd.DataFrame, window: int) -> pd.Series:
        return pd.Series(0.0, index=df.index)
        
    async def _open_position(self, order_type: str, price: float) -> None:
        """Mở vị thế mới."""
        current_bar = self._df.iloc[-1]
        if getattr(self.config, "max_positions", 1) <= 0:
            await self._emit_status("max_positions=0, không mở lệnh mới")
            return
        self._debug_counters["open_attempts"] += 1

        # Tính SL/TP: ưu tiên theo pip nếu có cấu hình, ngược lại dùng ATR
        atr = current_bar.atr
        use_pips = (self.config.sl_pips is not None) and (self.config.tp_pips is not None)
        if use_pips:
            if order_type == 'buy':
                sl = price - float(self.config.sl_pips) * self.config.pip_size
                tp = price + float(self.config.tp_pips) * self.config.pip_size
            else:
                sl = price + float(self.config.sl_pips) * self.config.pip_size
                tp = price - float(self.config.tp_pips) * self.config.pip_size
        else:
            if order_type == 'buy':
                sl = price - self.config.sl_atr * atr
                tp = price + self.config.tp_atr * atr
            else:
                sl = price + self.config.sl_atr * atr
                tp = price - self.config.tp_atr * atr

        volume = self.config.volume
        if getattr(self.config, 'size_from_risk', False):
            capital = getattr(self.config, 'capital', None)
            risk_pct = getattr(self.config, 'risk_pct', None)
            contract_size = getattr(self.config, 'contract_size', None)
            stop_dist = abs(price - sl)
            if stop_dist > 0 and capital and risk_pct and contract_size:
                risk_fraction = risk_pct if risk_pct <= 1 else risk_pct / 100.0
                risk_amount = capital * risk_fraction
                volume = risk_amount / (stop_dist * contract_size)
        volume_step = max(float(getattr(self.config, "volume_step", 0.01) or 0.01), 0.01)
        min_volume = max(float(getattr(self.config, "min_volume", volume_step) or volume_step), volume_step)
        volume = max(min_volume, round(volume / volume_step) * volume_step)

        # Tạo position object
        position = Position(
            symbol=self.config.symbol,
            type=order_type,
            volume=volume,
            open_price=price,
            open_time=current_bar.datetime,
            stop_loss=sl,
            take_profit=tp
        )

        if not self.config.paper_mode:
            if mt5 is None:
                raise RuntimeError("MetaTrader5 library không khả dụng cho chế độ live")
            # Gửi lệnh qua MT5
            mt5_type = mt5.ORDER_TYPE_BUY if order_type == 'buy' else mt5.ORDER_TYPE_SELL
            price_with_slip = price + (self.config.slippage_points * self.config.pip_size if order_type == "buy" else -self.config.slippage_points * self.config.pip_size)
            request = {
                "action": mt5.TRADE_ACTION_DEAL,
                "symbol": self.config.symbol,
                "volume": volume,
                "type": mt5_type,
                "price": price_with_slip,
                "sl": sl,
                "tp": tp,
                "deviation": int(max(0, getattr(self.config, "allowed_deviation_points", 10))),
                "magic": getattr(self.config, "magic_number", 20251230),
                "comment": "donchian_breakout",
                "type_time": mt5.ORDER_TIME_GTC,
                "type_filling": mt5.ORDER_FILLING_IOC,
            }
            
            result = await self._send_mt5_order(request)
            if not result:
                await self._emit_status("Lỗi mở lệnh MT5", {"request": request})
                return

            position.order_id = result.order
            
        self.current_position = position
        self.logger.info(
            f"Mở lệnh {order_type} {self.config.symbol}: "
            f"Price={price:.5f}, SL={sl:.5f}, TP={tp:.5f}"
        )
        open_payload = {
            "type": "position_open",
            "timestamp": datetime.now(timezone.utc),
            "symbol": self.config.symbol,
            "side": order_type,
            "price": price,
            "volume": volume,
            "stop_loss": sl,
            "take_profit": tp,
        }
        await self._emit_event(open_payload)
        await self._persist_trade_event("open", open_payload)
        self._log_debug_counter(force=True)

    async def _send_mt5_order(self, request: Dict[str, Any]):
        """Gửi lệnh MT5 với retry đơn giản."""
        if mt5 is None:
            raise RuntimeError("MetaTrader5 library không khả dụng cho chế độ live")
        retries = max(1, int(getattr(self.config, "order_retry_times", 1) or 1))
        delay_ms = max(0, int(getattr(self.config, "order_retry_delay_ms", 0) or 0))
        last_result = None
        for attempt in range(retries):
            result = mt5.order_send(request)
            last_result = result
            if result and result.retcode == mt5.TRADE_RETCODE_DONE:
                return result
            if attempt < retries - 1 and delay_ms > 0:
                await asyncio.sleep(delay_ms / 1000)
        if last_result:
            self.logger.error(f"MT5 order thất bại: retcode={last_result.retcode}, comment={getattr(last_result, 'comment', '')}")
        return None

    async def _close_partial_mt5(self, volume: float, price: float, side: str, position_id: int) -> None:
        """Gửi lệnh đóng một phần volume trên MT5."""
        if mt5 is None:
            raise RuntimeError("MetaTrader5 library không khả dụng cho chế độ live")
        mt5_type = mt5.ORDER_TYPE_SELL if side == "buy" else mt5.ORDER_TYPE_BUY
        price_with_slip = price - self.config.slippage_points * self.config.pip_size if side == "buy" else price + self.config.slippage_points * self.config.pip_size
        request = {
            "action": mt5.TRADE_ACTION_DEAL,
            "position": position_id,
            "symbol": self.config.symbol,
            "volume": volume,
            "type": mt5_type,
            "price": price_with_slip,
            "deviation": int(max(0, getattr(self.config, "allowed_deviation_points", 10))),
            "magic": getattr(self.config, "magic_number", 20251230),
            "comment": "partial_close",
            "type_time": mt5.ORDER_TIME_GTC,
            "type_filling": mt5.ORDER_FILLING_IOC,
        }
        await self._send_mt5_order(request)
        
    async def _close_position(self, price: float) -> None:
        """Đóng vị thế hiện tại."""
        if not self.current_position:
            return
            
        side = self.current_position.type
        open_price = self.current_position.open_price
        volume = self.current_position.volume
        contract_size = getattr(self.config, 'contract_size', 1.0)
        if side == 'buy':
            pnl_points = price - open_price
        else:
            pnl_points = open_price - price
        pnl_value = pnl_points * volume * contract_size
        realized_partial = getattr(self.current_position, "realized_pnl", 0.0)
        total_pnl_value = pnl_value + realized_partial

        if not self.config.paper_mode and self.current_position.order_id:
            if mt5 is None:
                raise RuntimeError("MetaTrader5 library không khả dụng cho chế độ live")
            mt5_type = mt5.ORDER_TYPE_SELL if side == "buy" else mt5.ORDER_TYPE_BUY
            price_with_slip = price - self.config.slippage_points * self.config.pip_size if side == "buy" else price + self.config.slippage_points * self.config.pip_size
            request = {
                "action": mt5.TRADE_ACTION_DEAL,
                "position": self.current_position.order_id,
                "symbol": self.config.symbol,
                "volume": volume,
                "type": mt5_type,
                "price": price_with_slip,
                "deviation": int(max(0, getattr(self.config, "allowed_deviation_points", 10))),
                "magic": getattr(self.config, "magic_number", 20251230),
                "comment": "close position",
                "type_time": mt5.ORDER_TIME_GTC,
                "type_filling": mt5.ORDER_FILLING_IOC,
            }
            result = await self._send_mt5_order(request)
            if not result:
                self.logger.error("Lỗi đóng lệnh MT5")
                return
                
        self.logger.info(
            f"Đóng lệnh {side} {self.config.symbol}: "
            f"Open={open_price:.5f}, Close={price:.5f}, PnL={total_pnl_value:.2f}"
        )
        close_payload = {
            "type": "position_close",
            "timestamp": datetime.now(timezone.utc),
            "symbol": self.config.symbol,
            "side": side,
            "open_price": open_price,
            "close_price": price,
            "volume": volume,
            "pnl_points": pnl_points,
            "pnl_value": total_pnl_value,
            "realized_partial": realized_partial,
        }
        await self._emit_event(close_payload)
        await self._persist_trade_event("close", close_payload)
        self._update_risk_after_close(total_pnl_value)
        self.current_position = None
        self._log_debug_counter(force=True)
        
    def _check_exit(self, price: float) -> bool:
        """Kiểm tra điều kiện đóng lệnh (SL/TP)."""
        if not self.current_position:
            return False
            
        pos = self.current_position
        if pos.type == 'buy':
            if price <= pos.stop_loss:  # Hit stop loss
                self.logger.info(f"Hit stop loss: {price:.5f}")
                return True
            if price >= pos.take_profit:  # Hit take profit
                self.logger.info(f"Hit take profit: {price:.5f}")
                return True
                
        else:  # pos.type == 'sell'
            if price >= pos.stop_loss:  # Hit stop loss
                self.logger.info(f"Hit stop loss: {price:.5f}")
                return True
            if price <= pos.take_profit:  # Hit take profit
                self.logger.info(f"Hit take profit: {price:.5f}")
                return True

        if getattr(self.config, "exit_on_opposite", False) and self._df is not None and len(self._df) >= 2:
            buy_sig, sell_sig, _ = self._evaluate_breakout_signal(self._df, len(self._df) - 1)
            if pos.type == "buy" and sell_sig:
                self.logger.info("Đóng lệnh vì có tín hiệu SELL ngược chiều")
                return True
            if pos.type == "sell" and buy_sig:
                self.logger.info("Đóng lệnh vì có tín hiệu BUY ngược chiều")
                return True

        return False

    def _log_debug_counter(self, force: bool = False) -> None:
        """In thống kê lý do vào/không vào lệnh để dễ theo dõi khi live."""
        counters = dict(self._debug_counters)
        counters["reasons_logged"] = len(self._debug_reasons)
        if not force and counters.get("bars_checked", 0) < 1:
            return
        self.logger.info("=" * 60)
        self.logger.info("DEBUG COUNTERS @ %s", datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M"))
        self.logger.info("Bars checked   : %d", counters.get("bars_checked", 0))
        self.logger.info("Breakout ready : %d", counters.get("entry_allowed", 0))
        self.logger.info("Range missing  : %d", counters.get("missing_range", 0))
        self.logger.info("ATR low/high   : %d / %d", counters.get("atr_low", 0), counters.get("atr_high", 0))
        self.logger.info("Trend filter   : %d", counters.get("trend_filter", 0))
        self.logger.info("No breakout    : %d", counters.get("no_breakout", 0))
        self.logger.info("Open attempts  : %d", counters.get("open_attempts", 0))
        if self._debug_reasons:
            top_reasons = Counter(self._debug_reasons).most_common(10)
            self.logger.info("Top reasons    : %s", top_reasons)
        self.logger.info("=" * 60)
        self._reset_debug_counters()

    def _maybe_log_debug_counter(self) -> None:
        """Log định kỳ để tránh spam."""
        total = self._debug_counters.get("bars_checked", 0)
        if total and total % 200 == 0:
            self._log_debug_counter(force=True)

    def _reset_debug_counters(self) -> None:
        self._debug_counters = defaultdict(int)
        self._debug_reasons = []
        self._sample_bars_logged = 0

    async def _emit_event(self, payload: Dict[str, Any]) -> None:
        if not self._event_handler:
            return
        try:
            result = self._event_handler(payload)
            if inspect.isawaitable(result):
                await result
        except Exception:
            self.logger.exception("Event handler lỗi")

    async def _persist_trade_event(self, event_type: str, payload: Dict[str, Any]) -> None:
        """Persist trade lifecycle events to the trades table."""
        if not self.storage:
            return
        side = payload.get("side")
        price = payload.get("price") or payload.get("close_price") or payload.get("open_price")
        if side is None or price is None:
            return
        timestamp = payload.get("timestamp") or datetime.now(timezone.utc)
        if isinstance(timestamp, datetime):
            ts_utc = timestamp.astimezone(timezone.utc)
        else:
            try:
                ts_utc = datetime.fromisoformat(str(timestamp))
            except ValueError:
                ts_utc = datetime.now(timezone.utc)
        time_msc = int(ts_utc.timestamp() * 1000)
        pnl_value = payload.get("pnl_value")
        meta = {k: v for k, v in payload.items() if k != "type"}
        meta["event_type"] = event_type
        try:
            await self.storage.insert_trade(
                side=str(side),
                price=float(price),
                time_msc=time_msc,
                pnl=float(pnl_value) if pnl_value is not None else None,
                meta=meta,
            )
        except Exception as exc:
            self.logger.error(f"Không thể lưu trade {event_type}: {exc}")

    async def _emit_status(self, reason: str, extra: Optional[Dict[str, Any]] = None) -> None:
        payload = {
            "type": "status",
            "timestamp": datetime.now(timezone.utc),
            "reason": reason,
            "extra": extra or {},
        }
        # Log ra console để live dễ theo dõi nếu không có handler
        try:
            self.logger.info("STATUS: %s | extra=%s", reason, payload["extra"])
        except Exception:
            pass
        await self._emit_event(payload)

    def _reset_risk_counters(self, now: datetime) -> None:
        if self._risk_day != now.date():
            self._risk_day = now.date()
            self._daily_pnl = 0.0
            self._loss_streak = 0
            self._session_losses = {}
            self._cooldown_until = None
            self._reset_debug_counters()

    def _current_session_label(self, now: datetime) -> Optional[str]:
        sessions = getattr(self.config, 'trading_hours', None)
        if not sessions:
            return "default"
        now_local = self._to_trading_timezone(now)
        current_minutes = now_local.hour * 60 + now_local.minute
        for session in sessions:
            try:
                start_str, end_str = session.split('-', 1)
            except ValueError:
                continue
            start_min = self._session_to_minutes(start_str.strip())
            end_min = self._session_to_minutes(end_str.strip())
            if start_min is None or end_min is None:
                continue
            if end_min < start_min:
                if current_minutes >= start_min or current_minutes <= end_min:
                    return session
            else:
                if start_min <= current_minutes <= end_min:
                    return session
        return "off"

    def _risk_guard_allows(self, now: datetime) -> Tuple[bool, Optional[str]]:
        self._reset_risk_counters(now)
        cooldown_until = getattr(self, "_cooldown_until", None)
        if cooldown_until and now < cooldown_until:
            return False, f"Đang cooldown tới {cooldown_until.astimezone().strftime('%H:%M:%S')}"
        max_daily = getattr(self.config, 'max_daily_loss', None)
        if max_daily is not None and self._daily_pnl <= -abs(max_daily):
            return False, "Đã vượt giới hạn lỗ ngày"
        max_streak = getattr(self.config, 'max_loss_streak', None) or getattr(self.config, 'max_consecutive_losses', None)
        if max_streak is not None and self._loss_streak >= max_streak:
            return False, "Đạt giới hạn chuỗi thua"
        session_limit = getattr(self.config, 'max_losses_per_session', None)
        if session_limit:
            session_label = self._current_session_label(now)
            if session_label and self._session_losses.get(session_label, 0) >= session_limit:
                return False, f"Đạt giới hạn thua trong phiên {session_label}"
        return True, None

    def _update_risk_after_close(self, pnl_value: float) -> None:
        now = self._now_trading()
        self._reset_risk_counters(now)
        self._daily_pnl += pnl_value
        session_label = self._current_session_label(now)
        cooldown_minutes = getattr(self.config, 'cooldown_minutes', None)
        max_daily = getattr(self.config, 'max_daily_loss', None)
        max_streak = getattr(self.config, 'max_loss_streak', None) or getattr(self.config, 'max_consecutive_losses', None)
        session_limit = getattr(self.config, 'max_losses_per_session', None)
        triggered = False
        session_triggered = False
        if pnl_value < 0:
            self._loss_streak += 1
            if session_label:
                self._session_losses[session_label] = self._session_losses.get(session_label, 0) + 1
            if max_daily is not None and self._daily_pnl <= -abs(max_daily):
                triggered = True
            if max_streak is not None and self._loss_streak >= max_streak:
                triggered = True
            if session_limit and session_label and self._session_losses.get(session_label, 0) >= session_limit:
                triggered = True
                session_triggered = True
        else:
            self._loss_streak = 0

        if triggered:
            session_cd = getattr(self.config, "session_cooldown_minutes", 0) or 0
            cooldown = session_cd if session_triggered and session_cd else cooldown_minutes
            if cooldown:
                self._cooldown_until = now + timedelta(minutes=cooldown)
    
    async def calculate_signals(self, 
                              storage: Storage,
                              start_time: datetime,
                              end_time: datetime) -> pd.DataFrame:
        """Calculate trading signals for the given period (Donchian breakout)."""
        df = await self._build_indicator_df(start_time, end_time, storage_override=storage)
        df['action'] = 0
        reasons: List[str] = []
        for idx in range(len(df)):
            bar_time = df.iloc[idx]['datetime']
            if isinstance(bar_time, pd.Timestamp):
                dt_obj = bar_time.to_pydatetime()
            elif isinstance(bar_time, np.datetime64):
                dt_obj = pd.Timestamp(bar_time).to_pydatetime()
            else:
                dt_obj = bar_time
            if isinstance(dt_obj, datetime):
                dt_local = self._to_trading_timezone(dt_obj)
                if not self._within_trading_hours(dt_local):
                    reasons.append("Ngoài giờ giao dịch")
                    continue
            else:
                reasons.append("Ngoài giờ giao dịch")
                continue
            if idx == 0:
                reasons.append("Chưa đủ dữ liệu")
                continue
            buy, sell, reason = self._evaluate_breakout_signal(df, idx)
            reasons.append(reason)
            if buy:
                df.at[df.index[idx], 'action'] = 2
            elif sell:
                df.at[df.index[idx], 'action'] = -2
        df['breakout_reason'] = reasons

        return df

    def _within_trading_hours(self, now: datetime) -> bool:
        now_local = self._to_trading_timezone(now)
        if now_local.weekday() >= 5:
            return False
        sessions = getattr(self.config, "trading_hours", None)
        if not sessions:
            return True
        current_minutes = now_local.hour * 60 + now_local.minute
        for session in sessions:
            try:
                start_str, end_str = session.split("-", 1)
                start_min = self._session_to_minutes(start_str.strip())
                end_min = self._session_to_minutes(end_str.strip())
            except ValueError:
                continue
            if start_min is None or end_min is None:
                continue
            if end_min < start_min:
                if current_minutes >= start_min or current_minutes <= end_min:
                    return True
            else:
                if start_min <= current_minutes <= end_min:
                    return True
        return False

    @staticmethod
    def _session_to_minutes(value: str) -> Optional[int]:
        if not value:
            return None
        parts = value.split(":")
        try:
            hour = int(parts[0])
            minute = int(parts[1]) if len(parts) > 1 else 0
        except (ValueError, IndexError):
            return None
        return hour * 60 + minute

    def _calculate_lookback_duration(self) -> timedelta:
        bars = self._calculate_required_bars()
        minutes = self._timeframe_minutes()
        return timedelta(minutes=bars * minutes)

    def _calculate_required_bars(self) -> int:
        donchian = max(2, int(getattr(self.config, "donchian_period", 20)))
        ema_trend = max(2, int(getattr(self.config, "ema_trend_period", 200)))
        atr_window = max(2, int(getattr(self.config, "atr_period", 14)))
        buffer = 50
        return max(donchian, ema_trend, atr_window) + buffer

    def _timeframe_minutes(self) -> int:
        tf = str(getattr(self.config, 'timeframe', '5min')).lower()
        if tf.endswith('min'):
            try:
                return max(1, int(tf[:-3]))
            except ValueError:
                return 5
        if tf.endswith('h'):
            try:
                return max(1, int(tf[:-1])) * 60
            except ValueError:
                return 60
        if tf.endswith('d'):
            try:
                return max(1, int(tf[:-1])) * 1440
            except ValueError:
                return 1440
        return 5

    def _to_trading_timezone(self, dt_obj: datetime) -> datetime:
        if dt_obj.tzinfo is None:
            dt_obj = dt_obj.replace(tzinfo=timezone.utc)
        return dt_obj.astimezone(self._tz)

    def _now_trading(self) -> datetime:
        return datetime.now(timezone.utc).astimezone(self._tz)

    async def _build_indicator_df(
        self,
        start_time: datetime,
        end_time: datetime,
        storage_override: Optional[Storage] = None,
    ) -> pd.DataFrame:
        storage = storage_override or self.storage
        atr_window = max(2, int(getattr(self.config, "atr_period", 14)))
        ignore_gaps = bool(getattr(self.config, "ignore_gaps", False))
        df = await get_ma_series(
            storage,
            self.config.symbol,
            start_time,
            end_time,
            self.config.timeframe,
            atr_window,
            "ema",
            atr_window=atr_window,
            ignore_gaps=ignore_gaps,
            closed_sessions=getattr(self.config, "closed_sessions", None),
        )
        df = df.rename(columns={"ma": "ema"})
        df = self._enrich_dataframe(df)
        return df
