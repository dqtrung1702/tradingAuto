"""Moving Average Crossover strategy implementation."""
from dataclasses import dataclass
from datetime import datetime, timezone, timedelta, date
import pandas as pd
import numpy as np
import logging
import inspect
from typing import Any, Awaitable, Callable, List, Tuple, Dict, Optional

try:  # MetaTrader5 chỉ khả dụng khi cài đặt trên Windows
    import MetaTrader5 as mt5  # type: ignore
except ImportError:  # pragma: no cover - môi trường không có MT5
    mt5 = None  # type: ignore

from ..indicators import get_ma_series
from ..storage import Storage
from ..quote_service import QuoteService
from ..config import Settings


EventHandler = Callable[[Dict[str, Any]], Awaitable[None] | None]


@dataclass
class MAConfig:
    """Moving Average strategy configuration."""
    # MA parameters
    fast_ma: int = 8  # Fast MA period (EMA)
    slow_ma: int = 21  # Slow MA period (EMA)
    ma_type: str = 'ema'  # MA type: 'sma' or 'ema'
    timeframe: str = '5min'  # Chart timeframe

        # Trading parameters
    symbol: str = 'XAUUSD'
    volume: float = 0.01  # Fixed lot size
    capital: float = 10000.0  # Account size dùng để tính risk sizing
    risk_pct: float = 1.0  # % vốn rủi ro mỗi lệnh
    contract_size: float = 100.0  # Quy đổi PnL: ví dụ XAUUSD ~100 oz/lot
    size_from_risk: bool = False  # Nếu True -> tính volume từ risk_pct
    sl_atr: float = 1.5  # Stop loss ATR multiplier
    tp_atr: float = 2.5  # Take profit ATR multiplier
    trail_trigger_atr: float = 1.8
    trail_atr_mult: float = 1.1
    paper_mode: bool = True  # Paper trading mode
    # Optional SL/TP theo pip (ưu tiên hơn ATR nếu được cấu hình)
    sl_pips: Optional[float] = None
    tp_pips: Optional[float] = None
    pip_size: float = 0.01  # 1 pip = pip_size đơn vị giá (ví dụ XAUUSD = 0.01)
    momentum_type: str = "hybrid"  # "macd" hoặc "pct"
    momentum_window: int = 14  # dùng cho pct
    momentum_threshold: float = 0.07  # pct threshold
    macd_fast: int = 12
    macd_slow: int = 26
    macd_signal: int = 9
    macd_threshold: float = 0.0002  # histogram threshold
    rsi_threshold_long: float = 60.0
    rsi_threshold_short: float = 40.0
    # Breakout specific tuning
    range_lookback: int = 30  # Bars to detect range high/low
    range_min_atr: float = 0.8  # Range height must be >= ATR * this
    range_min_points: float = 1.0  # Absolute minimum range height (USD)
    breakout_buffer_atr: float = 0.3  # Buffer added to S/R based on ATR
    breakout_confirmation_bars: int = 1  # Number of closes required outside range
    atr_baseline_window: int = 14
    atr_multiplier_min: float = 1.1
    atr_multiplier_max: float = 3.2
    trading_hours: Optional[List[str]] = None  # e.g. ["19:30-23:00", "01:00-02:30"]
    trend_ma: int = 200
    spread_atr_max: float = 0.08
    market_state_window: int = 40
    adx_window: int = 14
    adx_threshold: float = 25.0
    max_daily_loss: Optional[float] = None
    max_consecutive_losses: Optional[int] = None
    max_losses_per_session: Optional[int] = None
    cooldown_minutes: Optional[int] = None


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


class MACrossoverStrategy:
    """Moving Average Crossover trading strategy.
    
    Rules:
    - Buy when fast MA crosses above slow MA
    - Sell when fast MA crosses below slow MA
    - Fixed stop loss and take profit based on ATR
    """
    
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
        self._tz = timezone(timedelta(hours=7))
        
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

    async def start(self):
        """Bắt đầu chạy chiến lược."""
        if self._running:
            return
            
        self._running = True
        self.logger.info(f"Bắt đầu chiến lược MA Crossover cho {self.config.symbol}")
        
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
        if (not self._last_update or
            (now_utc - self._last_update).total_seconds() > 300):
            await self._update_data()
        if self._df is None or self._df.empty:
            await self._emit_status("Chưa có dữ liệu MA để xử lý quote")
            return

        if not self._within_trading_hours(now_local):
            await self._emit_status(
                "Ngoài khung giờ breakout",
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
            self._maybe_trail_stop(current_bar, price)
            if self._check_exit(price):
                await self._close_position(price)
                return

        # Kiểm tra tín hiệu mở lệnh mới nếu chưa có vị thế
        if not self.current_position:
            coeff = getattr(self.config, 'spread_atr_max', 0.1) or 0.1
            atr_val = current_bar.atr if pd.notna(current_bar.atr) else None
            max_spread = (atr_val * coeff) if atr_val and atr_val > 0 else coeff
            if spread > max_spread:
                atr_str = f"{atr_val:.5f}" if atr_val is not None else "NA"
                await self._emit_status(
                    f"Spread hiện tại {spread:.5f} > {max_spread:.5f} (ATR={atr_str}, hệ số={coeff})"
                )
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
        lookback = self._calculate_lookback_duration()
        start_time = end_time - lookback
        
        # Lấy dữ liệu và tính MA nhanh
        atr_window = max(2, int(getattr(self.config, 'atr_baseline_window', self.config.fast_ma)))
        df_fast = await get_ma_series(
            self.storage,
            self.config.symbol,
            start_time,
            end_time,
            self.config.timeframe,
            self.config.fast_ma,
            self.config.ma_type,
            atr_window=atr_window,
        )
        df_fast = df_fast.rename(columns={'ma': 'fast_ma'})
        
        # Tính MA chậm
        df_slow = await get_ma_series(
            self.storage,
            self.config.symbol,
            start_time,
            end_time,
            self.config.timeframe,
            self.config.slow_ma,
            self.config.ma_type,
            atr_window=atr_window,
        )
        df_slow = df_slow.rename(columns={'ma': 'slow_ma'})
        
        # Merge 2 dataframe và bổ sung chỉ báo breakout
        merged = pd.merge(df_fast, df_slow[['datetime', 'slow_ma']], on='datetime')
        self._df = self._enrich_dataframe(merged)
        self._last_update = end_time

    def _enrich_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        """Bổ sung các cột phục vụ breakout (MACD, range high/low, ATR baseline)."""
        if df.empty:
            return df

        df = df.copy()
        momentum_type = getattr(self.config, 'momentum_type', 'macd').lower()
        macd_needed = momentum_type in {'macd', 'hybrid'}
        if macd_needed:
            close = df['close']
            fast_span = max(1, int(getattr(self.config, 'macd_fast', 12)))
            slow_span = max(fast_span + 1, int(getattr(self.config, 'macd_slow', 26)))
            signal_span = max(1, int(getattr(self.config, 'macd_signal', 9)))
            ema_fast = close.ewm(span=fast_span, adjust=False).mean()
            ema_slow = close.ewm(span=slow_span, adjust=False).mean()
            macd_line = ema_fast - ema_slow
            macd_signal = macd_line.ewm(span=signal_span, adjust=False).mean()
            df['macd_line'] = macd_line
            df['macd_signal'] = macd_signal
            df['macd_hist'] = macd_line - macd_signal

        momentum_window = max(2, int(getattr(self.config, 'momentum_window', 14)))
        df['rsi'] = self._compute_rsi(df['close'], momentum_window)

        range_window = max(
            5,
            int(
                max(
                    getattr(self.config, 'range_lookback', 30),
                    getattr(self.config, 'market_state_window', 30),
                )
            ),
        )
        df['range_high'] = df['high'].rolling(window=range_window, min_periods=range_window).max().shift(1)
        df['range_low'] = df['low'].rolling(window=range_window, min_periods=range_window).min().shift(1)

        atr_window = max(2, int(getattr(self.config, 'atr_baseline_window', 14)))
        df['atr_baseline'] = df['atr'].rolling(window=atr_window, min_periods=1).mean()
        trend_period = getattr(self.config, 'trend_ma', None)
        if trend_period and trend_period > 1:
            df['trend_ma'] = df['close'].ewm(span=int(trend_period), adjust=False).mean()
        adx_window = max(3, int(getattr(self.config, 'adx_window', 14)))
        df['adx'] = self._compute_adx(df, adx_window)
        return df
        
    def _check_signals(self, current_bar: pd.Series) -> Tuple[bool, bool, str]:
        """Kiểm tra tín hiệu breakout."""
        if self._df is None or len(self._df) < 2:
            return False, False, "Chưa đủ dữ liệu để tính breakout"
        return self._evaluate_breakout_signal(self._df, len(self._df) - 1)

    def _evaluate_breakout_signal(self, df: pd.DataFrame, idx: int) -> Tuple[bool, bool, str]:
        if idx <= 0 or df is None or df.empty:
            return False, False, "Chưa đủ dữ liệu breakout"

        current = df.iloc[idx]
        range_high = current.get('range_high')
        range_low = current.get('range_low')

        if pd.isna(range_high) or pd.isna(range_low):
            return False, False, "Chưa xác định vùng sideway đủ dài"

        atr_value = float(current.get('atr') or 0.0)
        if atr_value <= 0 or np.isnan(atr_value):
            return False, False, "ATR không hợp lệ"

        atr_baseline = float(current.get('atr_baseline') or 0.0)
        if atr_baseline <= 0 or np.isnan(atr_baseline):
            window = max(2, int(getattr(self.config, 'atr_baseline_window', 14)))
            tail = df['atr'].iloc[max(0, idx - window): idx + 1]
            atr_baseline = float(tail.mean()) if not tail.empty else atr_value

        atr_min_mult = max(0.1, float(getattr(self.config, 'atr_multiplier_min', 0.8)))
        atr_max_mult = max(atr_min_mult + 0.1, float(getattr(self.config, 'atr_multiplier_max', 4.0)))
        atr_min = atr_baseline * atr_min_mult
        atr_max = atr_baseline * atr_max_mult
        if atr_value < atr_min:
            return False, False, f"ATR {atr_value:.2f} thấp hơn ngưỡng {atr_min:.2f}"
        if atr_value > atr_max:
            return False, False, f"ATR {atr_value:.2f} vượt ngưỡng {atr_max:.2f} (quá biến động)"

        range_height = range_high - range_low
        min_height = max(
            float(getattr(self.config, 'range_min_points', 0.5)),
            atr_value * float(getattr(self.config, 'range_min_atr', 0.8)),
        )
        if range_height < min_height:
            return False, False, f"Vùng range quá hẹp ({range_height:.2f} < {min_height:.2f})"

        buffer = atr_value * float(getattr(self.config, 'breakout_buffer_atr', 0.5))
        confirm_bars = max(1, int(getattr(self.config, 'breakout_confirmation_bars', 1)))
        start_idx = max(0, idx - confirm_bars + 1)
        recent = df.iloc[start_idx: idx + 1]

        closes = recent['close']
        bullish_ready = bool(
            closes.isna().sum() == 0
            and closes.min() > range_high
            and current.close > range_high + buffer
        )
        bearish_ready = bool(
            closes.isna().sum() == 0
            and closes.max() < range_low
            and current.close < range_low - buffer
        )

        # EMA trend filter
        if bullish_ready and not (current.fast_ma > current.slow_ma):
            bullish_ready = False
            reason = "EMA chưa đồng thuận cho BUY"
        if bearish_ready and not (current.fast_ma < current.slow_ma):
            bearish_ready = False
            reason = "EMA chưa đồng thuận cho SELL"

        reason = f"Range {range_low:.2f}-{range_high:.2f}"
        if not bullish_ready and not bearish_ready:
            return False, False, f"{reason} | close {current.close:.2f} chưa phá vùng (buffer {buffer:.2f})"

        trend_ma_val = current.get('trend_ma') if isinstance(current, pd.Series) else None
        if trend_ma_val is not None and not np.isnan(trend_ma_val):
            if bullish_ready and current.close < trend_ma_val:
                bullish_ready = False
                reason = "Chưa vượt EMA trend"
            if bearish_ready and current.close > trend_ma_val:
                bearish_ready = False
                reason = "Chưa nằm dưới EMA trend"

        adx_threshold = float(getattr(self.config, 'adx_threshold', 0.0) or 0.0)
        if adx_threshold > 0:
            adx_val = current.get('adx') if isinstance(current, pd.Series) else None
            formatted_adx = f"{adx_val:.2f}" if (adx_val is not None and not np.isnan(adx_val)) else "NA"
            if adx_val is None or np.isnan(adx_val) or adx_val < adx_threshold:
                return False, False, f"ADX {formatted_adx} < {adx_threshold}"

        momentum_type = getattr(self.config, 'momentum_type', 'macd').lower()
        if momentum_type == 'macd':
            hist = current.get('macd_hist') if isinstance(current, pd.Series) else None
            thresh = getattr(self.config, 'macd_threshold', 0.0) or 0.0
            if hist is None or np.isnan(hist):
                return False, False, "MACD chưa đủ dữ liệu"
            if bullish_ready and hist < thresh:
                bullish_ready = False
                reason = f"MACD hist {hist:.4f} < ngưỡng {thresh}"
            if bearish_ready and hist > -thresh:
                bearish_ready = False
                reason = f"MACD hist {hist:.4f} > {-thresh}"
        elif momentum_type == 'pct':
            threshold = max(0.0, getattr(self.config, 'momentum_threshold', 0.0))
            window = max(1, getattr(self.config, 'momentum_window', 1))
            if threshold > 0 and len(df) > window:
                ref = df.iloc[max(0, idx - window)]
                ref_close = ref.close if hasattr(ref, 'close') else None
                pct_change = ((current.close - ref_close) / ref_close * 100) if ref_close else 0.0
                if bullish_ready and pct_change < threshold:
                    bullish_ready = False
                    reason = f"%change {pct_change:.2f}% < {threshold}%"
                if bearish_ready and pct_change > -threshold:
                    bearish_ready = False
                    reason = f"%change {pct_change:.2f}% > {-threshold}%"
        elif momentum_type == 'hybrid':
            hist = current.get('macd_hist') if isinstance(current, pd.Series) else None
            rsi_val = current.get('rsi') if isinstance(current, pd.Series) else None
            macd_thresh = getattr(self.config, 'macd_threshold', 0.0) or 0.0
            rsi_long = getattr(self.config, 'rsi_threshold_long', 60.0) or 60.0
            rsi_short = getattr(self.config, 'rsi_threshold_short', 40.0) or 40.0
            if hist is None or np.isnan(hist) or rsi_val is None or np.isnan(rsi_val):
                return False, False, "RSI/MACD chưa đủ dữ liệu"
            if bullish_ready and (hist < macd_thresh or rsi_val < rsi_long):
                bullish_ready = False
                reason = f"Momentum BUY yếu (MACD {hist:.4f}, RSI {rsi_val:.2f})"
            if bearish_ready and (hist > -macd_thresh or rsi_val > rsi_short):
                bearish_ready = False
                reason = f"Momentum SELL yếu (MACD {hist:.4f}, RSI {rsi_val:.2f})"

        if bullish_ready:
            return True, False, f"Breakout BUY xác nhận @ {current.close:.2f}"
        if bearish_ready:
            return False, True, f"Breakout SELL xác nhận @ {current.close:.2f}"
        return False, False, reason

    def _maybe_trail_stop(self, current_bar: pd.Series, price: float) -> None:
        if not self.current_position:
            return
        trigger = float(getattr(self.config, 'trail_trigger_atr', 0.0) or 0.0)
        mult = float(getattr(self.config, 'trail_atr_mult', 0.0) or 0.0)
        if trigger <= 0 or mult <= 0:
            return
        atr = current_bar.atr if hasattr(current_bar, 'atr') else None
        if atr is None or np.isnan(atr) or atr <= 0:
            return
        pos = self.current_position
        move = price - pos.open_price if pos.type == 'buy' else pos.open_price - price
        if move < trigger * atr:
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

    @staticmethod
    def _compute_rsi(series: pd.Series, window: int) -> pd.Series:
        if window <= 1:
            return pd.Series(50.0, index=series.index)
        delta = series.diff()
        gain = delta.clip(lower=0)
        loss = -delta.clip(upper=0)
        avg_gain = gain.ewm(alpha=1 / window, adjust=False).mean()
        avg_loss = loss.ewm(alpha=1 / window, adjust=False).mean()
        rs = avg_gain / avg_loss.replace(0, np.nan)
        rsi = 100 - (100 / (1 + rs))
        return rsi.fillna(50.0)

    @staticmethod
    def _compute_adx(df: pd.DataFrame, window: int) -> pd.Series:
        if window <= 1 or df.empty:
            return pd.Series(0.0, index=df.index)
        high = df['high']
        low = df['low']
        close = df['close']
        up_move = high.diff()
        down_move = -low.diff()
        plus_dm = np.where((up_move > down_move) & (up_move > 0), up_move, 0.0)
        minus_dm = np.where((down_move > up_move) & (down_move > 0), down_move, 0.0)
        tr1 = high - low
        tr2 = (high - close.shift()).abs()
        tr3 = (low - close.shift()).abs()
        tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
        atr = tr.ewm(alpha=1 / window, adjust=False).mean()
        plus_di = 100 * pd.Series(plus_dm, index=df.index).ewm(alpha=1 / window, adjust=False).mean() / atr
        minus_di = 100 * pd.Series(minus_dm, index=df.index).ewm(alpha=1 / window, adjust=False).mean() / atr
        di_sum = (plus_di + minus_di).replace(0, np.nan)
        dx = ((plus_di - minus_di).abs() / di_sum) * 100
        adx = dx.ewm(alpha=1 / window, adjust=False).mean()
        return adx.fillna(0.0)
        
    async def _open_position(self, order_type: str, price: float) -> None:
        """Mở vị thế mới."""
        current_bar = self._df.iloc[-1]

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
                risk_amount = capital * (risk_pct / 100.0)
                volume = risk_amount / (stop_dist * contract_size)
                volume = max(round(volume, 2), 0.01)

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
            order_type = mt5.ORDER_TYPE_BUY if order_type == 'buy' else mt5.ORDER_TYPE_SELL
            request = {
                "action": mt5.TRADE_ACTION_DEAL,
                "symbol": self.config.symbol,
                "volume": volume,
                "type": order_type,
                "price": price,
                "sl": sl,
                "tp": tp,
                "deviation": 10,
                "magic": 234000,
                "comment": "MA crossover",
                "type_time": mt5.ORDER_TIME_GTC,
                "type_filling": mt5.ORDER_FILLING_IOC,
            }
            
            result = mt5.order_send(request)
            if result.retcode != mt5.TRADE_RETCODE_DONE:
                self.logger.error(f"Lỗi mở lệnh: {result.comment}")
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
        
    async def _close_position(self, price: float) -> None:
        """Đóng vị thế hiện tại."""
        if not self.current_position:
            return
            
        if not self.config.paper_mode and self.current_position.order_id:
            if mt5 is None:
                raise RuntimeError("MetaTrader5 library không khả dụng cho chế độ live")
            # Đóng lệnh qua MT5
            position = mt5.positions_get(ticket=self.current_position.order_id)[0]
            
            request = {
                "action": mt5.TRADE_ACTION_DEAL,
                "position": position.ticket,
                "symbol": self.config.symbol,
                "volume": self.current_position.volume,
                "type": mt5.ORDER_TYPE_SELL if position.type == 0 else mt5.ORDER_TYPE_BUY,
                "price": price,
                "deviation": 10,
                "magic": 234000,
                "comment": "close position",
                "type_time": mt5.ORDER_TIME_GTC,
                "type_filling": mt5.ORDER_FILLING_IOC,
            }
            
            result = mt5.order_send(request)
            if result.retcode != mt5.TRADE_RETCODE_DONE:
                self.logger.error(f"Lỗi đóng lệnh: {result.comment}")
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
        self.logger.info(
            f"Đóng lệnh {side} {self.config.symbol}: "
            f"Open={open_price:.5f}, Close={price:.5f}, PnL={pnl_value:.2f}"
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
            "pnl_value": pnl_value,
        }
        await self._emit_event(close_payload)
        await self._persist_trade_event("close", close_payload)
        self._update_risk_after_close(pnl_value)
        self.current_position = None
        
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
                
        return False

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
        await self._emit_event(
            {
                "type": "status",
                "timestamp": datetime.now(timezone.utc),
                "reason": reason,
                "extra": extra or {},
            }
        )

    def _reset_risk_counters(self, now: datetime) -> None:
        if self._risk_day != now.date():
            self._risk_day = now.date()
            self._daily_pnl = 0.0
            self._loss_streak = 0
            self._session_losses = {}
            self._cooldown_until = None

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
        max_streak = getattr(self.config, 'max_consecutive_losses', None)
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
        max_streak = getattr(self.config, 'max_consecutive_losses', None)
        session_limit = getattr(self.config, 'max_losses_per_session', None)
        triggered = False
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
        else:
            self._loss_streak = 0

        if cooldown_minutes and triggered:
            self._cooldown_until = now + timedelta(minutes=cooldown_minutes)
    
    async def calculate_signals(self, 
                              storage: Storage,
                              start_time: datetime,
                              end_time: datetime) -> pd.DataFrame:
        """Calculate trading signals for the given period."""
        atr_window = max(2, int(getattr(self.config, 'atr_baseline_window', self.config.fast_ma)))
        # Get fast MA
        df_fast = await get_ma_series(
            storage,
            self.config.symbol,
            start_time,
            end_time,
            self.config.timeframe,
            self.config.fast_ma,
            self.config.ma_type,
            atr_window=atr_window,
        )
        df_fast = df_fast.rename(columns={'ma': 'fast_ma'})
        
        # Get slow MA
        df_slow = await get_ma_series(
            storage,
            self.config.symbol,
            start_time, 
            end_time,
            self.config.timeframe,
            self.config.slow_ma,
            self.config.ma_type,
            atr_window=atr_window,
        )
        df_slow = df_slow.rename(columns={'ma': 'slow_ma'})
        
        # Merge and calculate signals
        df = pd.merge(df_fast, df_slow[['datetime', 'slow_ma']], on='datetime', how='inner')
        df = self._enrich_dataframe(df)
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
        sessions = getattr(self.config, "trading_hours", None)
        if not sessions:
            return True
        now_local = self._to_trading_timezone(now)
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
        fast = max(1, int(getattr(self.config, 'fast_ma', 1)))
        slow = max(fast + 1, int(getattr(self.config, 'slow_ma', fast + 1)))
        trend = max(slow, int(getattr(self.config, 'trend_ma', slow)))
        range_window = max(
            slow,
            int(getattr(self.config, 'range_lookback', slow)),
            int(getattr(self.config, 'market_state_window', slow)),
        )
        atr_window = max(2, int(getattr(self.config, 'atr_baseline_window', slow)))
        adx_window = max(2, int(getattr(self.config, 'adx_window', atr_window)))
        momentum_window = max(2, int(getattr(self.config, 'momentum_window', 14)))
        buffer = 50
        return max(slow, trend, range_window + momentum_window, atr_window + adx_window) + buffer

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
