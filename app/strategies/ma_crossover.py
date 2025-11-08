"""Moving Average Crossover strategy implementation."""
from dataclasses import dataclass
from datetime import datetime, timezone, timedelta
import pandas as pd
import numpy as np
import logging
import MetaTrader5 as mt5
from typing import List, Tuple, Dict, Optional

from ..indicators import get_ma_series
from ..storage import Storage
from ..quote_service import QuoteService
from ..config import Settings


@dataclass
class MAConfig:
    """Moving Average strategy configuration."""
    # MA parameters
    fast_ma: int = 20  # Fast MA period
    slow_ma: int = 50  # Slow MA period
    ma_type: str = 'sma'  # MA type: 'sma' or 'ema'
    timeframe: str = '1min'  # Chart timeframe
    
        # Trading parameters
    symbol: str = 'XAUUSDc'
    volume: float = 0.01  # Fixed lot size
    sl_atr: float = 2.0  # Stop loss ATR multiplier
    tp_atr: float = 3.0  # Take profit ATR multiplier
    paper_mode: bool = True  # Paper trading mode


class MACrossoverStrategy:
    """Moving Average Crossover trading strategy.
    
    Rules:
    - Buy when fast MA crosses above slow MA
    - Sell when fast MA crosses below slow MA
    - Fixed stop loss and take profit based on ATR
    """
    
    def __init__(self, config: MAConfig, quote_service: QuoteService, storage: Storage):
        self.config = config
        self.quote_service = quote_service
        self.storage = storage
        self.logger = logging.getLogger(__name__)
        
        # Trạng thái
        self.current_position: Optional[Position] = None
        self._last_update = None
        self._df = None
        self._running = False

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
        
        # Tính giá trung bình bid/ask
        price = (bid + ask) / 2
        spread = ask - bid
        
        # Kiểm tra điều kiện đóng lệnh nếu đang có vị thế
        if self.current_position and self._check_exit(price):
            await self._close_position(price)
            return
            
        # Kiểm tra tín hiệu mở lệnh mới nếu chưa có vị thế
        if not self.current_position:
            buy_signal, sell_signal = self._check_signals()
            
            # Chỉ mở lệnh nếu spread trong ngưỡng cho phép
            max_spread = self._df.iloc[-1].atr * 0.1  # Max spread 10% ATR
            if spread > max_spread:
                self.logger.warning(f"Spread ({spread:.5f}) quá lớn, bỏ qua tín hiệu")
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
        
        # Merge 2 dataframe
        self._df = pd.merge(df_fast, df_slow[['datetime', 'slow_ma']], on='datetime')
        self._last_update = end_time
        
    def _check_signals(self) -> Tuple[bool, bool]:
        """Kiểm tra tín hiệu giao dịch từ dữ liệu hiện tại."""
        if len(self._df) < 2:
            return False, False
            
        # Lấy 2 bars gần nhất
        current = self._df.iloc[-1]
        prev = self._df.iloc[-2]
        
        # Kiểm tra golden cross
        buy_signal = (prev.fast_ma <= prev.slow_ma and 
                     current.fast_ma > current.slow_ma)
                     
        # Kiểm tra death cross
        sell_signal = (prev.fast_ma >= prev.slow_ma and
                      current.fast_ma < current.slow_ma)
                      
        return buy_signal, sell_signal
        
    async def _open_position(self, order_type: str, price: float) -> None:
        """Mở vị thế mới."""
        current_bar = self._df.iloc[-1]
        
        # Tính SL/TP dựa trên ATR
        atr = current_bar.atr
        if order_type == 'buy':
            sl = price - self.config.sl_atr * atr
            tp = price + self.config.tp_atr * atr
        else:
            sl = price + self.config.sl_atr * atr
            tp = price - self.config.tp_atr * atr
            
        # Tạo position object
        position = Position(
            symbol=self.config.symbol,
            type=order_type,
            volume=self.config.volume,
            open_price=price,
            open_time=current_bar.datetime,
            stop_loss=sl,
            take_profit=tp
        )
        
        if not self.config.paper_mode:
            # Gửi lệnh qua MT5
            order_type = mt5.ORDER_TYPE_BUY if order_type == 'buy' else mt5.ORDER_TYPE_SELL
            request = {
                "action": mt5.TRADE_ACTION_DEAL,
                "symbol": self.config.symbol,
                "volume": self.config.volume,
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
        
    async def _close_position(self, price: float) -> None:
        """Đóng vị thế hiện tại."""
        if not self.current_position:
            return
            
        if not self.config.paper_mode and self.current_position.order_id:
            # Đóng lệnh qua MT5
            position = mt5.positions_get(ticket=self.current_position.order_id)[0]
            
            request = {
                "action": mt5.TRADE_ACTION_DEAL,
                "position": position.ticket,
                "symbol": self.config.symbol,
                "volume": self.config.volume,
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
                
        self.logger.info(
            f"Đóng lệnh {self.current_position.type} {self.config.symbol}: "
            f"Open={self.current_position.open_price:.5f}, Close={price:.5f}"
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
        df = pd.merge(df_fast, df_slow[['datetime', 'slow_ma']], 
                     on='datetime', how='inner')
        
        # Calculate crossovers
        df['signal'] = 0
        df.loc[df['fast_ma'] > df['slow_ma'], 'signal'] = 1  # Bullish
        df.loc[df['fast_ma'] < df['slow_ma'], 'signal'] = -1  # Bearish
        
        # Detect changes in signal (actual entry points)
        df['action'] = df['signal'].diff()
        
        # ATR filter - nới lỏng ngưỡng
        atr_threshold = df['atr'].mean()  # Use mean ATR as baseline
        # Only keep signals when ATR is reasonable (not too high/low)
        df.loc[df['atr'] > atr_threshold * 2.0, 'action'] = 0  # Too volatile
        df.loc[df['atr'] < atr_threshold * 0.3, 'action'] = 0  # Too quiet
        
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