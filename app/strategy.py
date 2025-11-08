from collections import deque
from dataclasses import dataclass
from typing import Deque, Dict, Optional


@dataclass
class Tick:
    time_msc: int
    bid: float
    ask: float
    last: Optional[float]


class TickMovingAverageStrategy:
    """Chiến lược MA trên tick: trung bình của last price trên N tick.

    Nếu MA ngắn cắt lên MA dài -> BUY (mua 1 unit).
    Nếu MA ngắn cắt xuống MA dài -> CLOSE (đóng vị thế nếu có).
    Đây là mô hình đơn giản để demo/backtest trên dữ liệu tick.
    """

    def __init__(self, short_window: int = 10, long_window: int = 50):
        if short_window >= long_window:
            raise ValueError("short_window phải nhỏ hơn long_window")
        self.short_w = short_window
        self.long_w = long_window
        self._short_buf: Deque[float] = deque(maxlen=short_window)
        self._long_buf: Deque[float] = deque(maxlen=long_window)
        self._last_signal: Optional[str] = None

    def _ma(self, buf: Deque[float]) -> Optional[float]:
        if len(buf) == 0:
            return None
        return sum(buf) / len(buf)

    def handle_tick(self, tick: Tick) -> Optional[Dict]:
        """Xử lý 1 tick, trả về signal hoặc None.

        Signal dạng dict: {"action": "BUY"|"CLOSE", "price": float}
        """
        price = tick.last if tick.last is not None else (tick.ask + tick.bid) / 2.0
        self._short_buf.append(price)
        self._long_buf.append(price)

        short_ma = self._ma(self._short_buf)
        long_ma = self._ma(self._long_buf)
        if short_ma is None or long_ma is None:
            return None

        # Crossover logic
        if short_ma > long_ma and self._last_signal != "LONG":
            self._last_signal = "LONG"
            return {"action": "BUY", "price": price}
        if short_ma < long_ma and self._last_signal == "LONG":
            self._last_signal = "FLAT"
            return {"action": "CLOSE", "price": price}

        return None


class PaperExecutionSimulator:
    """Mô phỏng đơn giản thực thi lệnh trong chế độ paper.

    Không mô phỏng slippage/commission phức tạp; tính toán basic P&L.
    """

    def __init__(self, initial_balance: float = 10000.0):
        self.balance = initial_balance
        self.position = 0.0  # +ve = long size in units
        self.entry_price: Optional[float] = None
        self.trades = []

    def execute(self, signal: Dict, tick: Tick) -> None:
        action = signal.get("action")
        price = signal.get("price")
        # For buy, assume we buy 1 unit
        size = 1.0
        if action == "BUY":
            if self.position == 0:
                self.position = size
                self.entry_price = price
                self.trades.append({"side": "BUY", "price": price, "time": tick.time_msc})
        elif action == "CLOSE":
            if self.position > 0 and self.entry_price is not None:
                pnl = (price - self.entry_price) * self.position
                self.balance += pnl
                self.trades.append({"side": "CLOSE", "price": price, "time": tick.time_msc, "pnl": pnl})
                # reset
                self.position = 0.0
                self.entry_price = None

    def current_unrealized(self, mark_price: float) -> float:
        if self.position == 0 or self.entry_price is None:
            return 0.0
        return (mark_price - self.entry_price) * self.position
