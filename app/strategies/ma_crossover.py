"""Moving Average Crossover strategy implementation."""
from dataclasses import dataclass
from datetime import datetime, timezone, timedelta
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
    fast_ma: int = 21  # Fast MA period (EMA)
    slow_ma: int = 89  # Slow MA period (EMA)
    ma_type: str = 'ema'  # MA type: 'sma' or 'ema'
    timeframe: str = '1min'  # Chart timeframe

        # Trading parameters
    symbol: str = 'XAUUSDc'
    volume: float = 0.01  # Fixed lot size
    capital: float = 10000.0  # Account size dùng để tính risk sizing
    risk_pct: float = 1.0  # % vốn rủi ro mỗi lệnh
    contract_size: float = 100.0  # Quy đổi PnL: ví dụ XAUUSD ~100 oz/lot
    size_from_risk: bool = False  # Nếu True -> tính volume từ risk_pct
    sl_atr: float = 2.0  # Stop loss ATR multiplier
    tp_atr: float = 3.0  # Take profit ATR multiplier
    paper_mode: bool = True  # Paper trading mode
    # Optional SL/TP theo pip (ưu tiên hơn ATR nếu được cấu hình)
    sl_pips: Optional[float] = None
    tp_pips: Optional[float] = None
    pip_size: float = 0.01  # 1 pip = pip_size đơn vị giá (ví dụ XAUUSD = 0.01)
    momentum_type: str = "macd"  # "macd" hoặc "pct"
    momentum_window: int = 14  # dùng cho pct
    momentum_threshold: float = 0.1  # pct threshold
    macd_fast: int = 12
    macd_slow: int = 26
    macd_signal: int = 9
    macd_threshold: float = 0.0  # histogram threshold
    # Breakout specific tuning
    range_lookback: int = 30  # Bars to detect range high/low
    range_min_atr: float = 0.8  # Range height must be >= ATR * this
    range_min_points: float = 0.5  # Absolute minimum range height (USD)
    breakout_buffer_atr: float = 0.25  # Buffer added to S/R based on ATR
    breakout_confirmation_bars: int = 1  # Number of closes required outside range
    atr_baseline_window: int = 14
    atr_multiplier_min: float = 0.8
    atr_multiplier_max: float = 4.0
    trading_hours: Optional[List[str]] = None  # e.g. ["19:30-23:00", "01:00-02:30"]


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
        
        # Trạng thái
        self.current_position: Optional[Position] = None
        self._last_update = None
        self._df = None
        self._running = False
        self._event_handler = event_handler

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
            
        # Cập nhật data mỗi 5 phút
        now = datetime.now()
        if (not self._last_update or 
            (now - self._last_update).total_seconds() > 300):
            await self._update_data()
        if self._df is None or self._df.empty:
            await self._emit_status("Chưa có dữ liệu MA để xử lý quote")
            return

        if not self._within_trading_hours(now):
            await self._emit_status(
                "Ngoài khung giờ breakout",
                {"now": now.strftime("%H:%M:%S")},
            )
            return

        # Tính giá trung bình bid/ask
        price = (bid + ask) / 2
        spread = ask - bid

        # Kiểm tra điều kiện đóng lệnh nếu đang có vị thế
        if self.current_position and self._check_exit(price):
            await self._close_position(price)
            return

        # Kiểm tra tín hiệu mở lệnh mới nếu chưa có vị thế
        if not self.current_position:
            current_bar = self._df.iloc[-1]
            coeff = getattr(self.config, 'spread_atr_max', 0.1) or 0.1
            atr_val = current_bar.atr if pd.notna(current_bar.atr) else None
            max_spread = (atr_val * coeff) if atr_val and atr_val > 0 else coeff
            if spread > max_spread:
                await self._emit_status(
                    f"Spread hiện tại {spread:.5f} > {max_spread:.5f} (ATR={atr_val:.5f if atr_val is not None else 'NA'}, hệ số={coeff})"
                )
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
        # Lấy dữ liệu 100 bars gần nhất
        end_time = datetime.now()
        if self.config.timeframe == '1min':
            lookback = timedelta(minutes=100)
        elif self.config.timeframe == '5min':
            lookback = timedelta(minutes=500)
        else:
            lookback = timedelta(hours=100)
            
        start_time = end_time - lookback
        
        # Lấy dữ liệu và tính MA nhanh
        df_fast = await get_ma_series(
            self.storage,
            self.config.symbol,
            start_time,
            end_time,
            self.config.timeframe,
            self.config.fast_ma,
            self.config.ma_type
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
            self.config.ma_type
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
        if momentum_type == 'macd':
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

        range_window = max(5, int(getattr(self.config, 'range_lookback', 30)))
        df['range_high'] = df['high'].rolling(window=range_window, min_periods=range_window).max().shift(1)
        df['range_low'] = df['low'].rolling(window=range_window, min_periods=range_window).min().shift(1)

        atr_window = max(2, int(getattr(self.config, 'atr_baseline_window', 14)))
        df['atr_baseline'] = df['atr'].rolling(window=atr_window, min_periods=1).mean()
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
        else:
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

        if bullish_ready:
            return True, False, f"Breakout BUY xác nhận @ {current.close:.2f}"
        if bearish_ready:
            return False, True, f"Breakout SELL xác nhận @ {current.close:.2f}"
        return False, False, reason
        
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
        await self._emit_event(
            {
                "type": "position_open",
                "timestamp": datetime.now(timezone.utc),
                "symbol": self.config.symbol,
                "side": order_type,
                "price": price,
                "volume": volume,
                "stop_loss": sl,
                "take_profit": tp,
            }
        )
        
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
        await self._emit_event(
            {
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
        )
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

    async def _emit_status(self, reason: str, extra: Optional[Dict[str, Any]] = None) -> None:
        await self._emit_event(
            {
                "type": "status",
                "timestamp": datetime.now(timezone.utc),
                "reason": reason,
                "extra": extra or {},
            }
        )
    
    async def calculate_signals(self, 
                              storage: Storage,
                              start_time: datetime,
                              end_time: datetime) -> pd.DataFrame:
        """Calculate trading signals for the given period."""
        # Get fast MA
        df_fast = await get_ma_series(
            storage,
            self.config.symbol,
            start_time,
            end_time,
            self.config.timeframe,
            self.config.fast_ma,
            self.config.ma_type
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
            self.config.ma_type
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
            if isinstance(dt_obj, datetime) and not self._within_trading_hours(dt_obj):
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
    
    def calculate_position_size(self, 
                              capital: float,
                              entry_price: float,
                              stop_loss: float) -> float:
        """Calculate position size based on risk parameters."""
        risk_amount = capital * (self.config.risk_pct / 100)
        pip_value = 0.01  # Adjust based on symbol
        
        stop_loss_pips = abs(entry_price - stop_loss) / pip_value
        position_size = risk_amount / stop_loss_pips
        
        return position_size
    
    def get_trade_levels(self, 
                        entry_price: float, 
                        side: int) -> Tuple[float, float]:
        """Calculate stop loss and take profit levels."""
        pip_value = 0.01  # Adjust based on symbol
        
        if side > 0:  # Long
            stop_loss = entry_price - (self.config.stop_loss_pips * pip_value)
            take_profit = entry_price + (self.config.stop_loss_pips * 
                                       self.config.take_profit_ratio * pip_value)
        else:  # Short
            stop_loss = entry_price + (self.config.stop_loss_pips * pip_value)
            take_profit = entry_price - (self.config.stop_loss_pips * 
                                       self.config.take_profit_ratio * pip_value)
            
        return stop_loss, take_profit

    def _within_trading_hours(self, now: datetime) -> bool:
        sessions = getattr(self.config, "trading_hours", None)
        if not sessions:
            return True
        current_minutes = now.hour * 60 + now.minute
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
