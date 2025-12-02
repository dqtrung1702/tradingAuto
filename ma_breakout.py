# -*- coding: utf-8 -*-
"""Chiến lược MA Breakout + Range filter + Momentum xác nhận."""

from __future__ import annotations

import logging
from datetime import datetime, timezone, timedelta
from typing import Optional

import pandas as pd
import asyncio

try:
    import MetaTrader5 as mt5  # type: ignore
except ImportError:  # pragma: no cover - tuỳ môi trường
    mt5 = None

from app.indicators import get_full_indicators
from app.quote_service import QuoteService
from app.storage import Storage
from .ma_config import MAConfig, Position

from collections import defaultdict
from collections import deque

logger = logging.getLogger(__name__)


class MACrossoverStrategy:
    """Chiến lược MA Breakout chính (dùng cho cả backtest và live)."""

    def __init__(
        self,
        config: MAConfig,
        quote_service: Optional[QuoteService],
        storage: Storage,
        event_handler=None,
    ):
        self.config = config
        self.quote_service = quote_service
        self.storage = storage
        self._event_handler = event_handler

        self._df: Optional[pd.DataFrame] = None
        self.current_position: Optional[Position] = None
        self._last_signal_time: Optional[datetime] = None
        self._cooldown_until: Optional[datetime] = None
        
        if self.config.debug_bars:
            logger.setLevel(logging.DEBUG)
        self._debug_counters = defaultdict(int)
        self._debug_reasons = []
        self._sample_bars_logged = 0
        self._MAX_SAMPLE_BARS = 10

        # === Quản lý rủi ro nâng cao ===
        self._cooldown_until: Optional[datetime] = None
        self._daily_loss = 0.0
        self._current_day = datetime.now(timezone.utc).date()
        self._consecutive_losses = 0
        self._session_losses = 0
        self._recent_trades = deque(maxlen=10)  # lưu 10 lệnh gần nhất để tính streak
    
    # ------------------------------------------------------------------ #
    #  Kiểm tra có được phép mở lệnh mới không?
    # ------------------------------------------------------------------ #
    def _can_open_new_position(self, current_time: datetime) -> tuple[bool, str]:
        """
        Trả về (có được mở lệnh không, lý do nếu bị từ chối)
        """
        # 1. Cooldown cá nhân (sau 1 lệnh lỗ)
        if self._cooldown_until and current_time < self._cooldown_until:
            remaining = int((self._cooldown_until - current_time).total_seconds() / 60)
            return False, f"Đang trong cooldown sau lỗ: còn {remaining} phút"

        # 2. Max daily loss
        if self.config.max_daily_loss:
            today = current_time.date()
            if today != self._current_day:
                # Reset daily tracker khi qua ngày mới
                self._daily_loss = 0.0
                self._current_day = today

            if self._daily_loss >= self.config.capital * self.config.max_daily_loss:
                return False, f"Đã đạt max daily loss ({self.config.max_daily_loss:.1%})"

        # 3. Max consecutive losses (chuỗi lỗ liên tiếp)
        if self.config.max_consecutive_losses and self._consecutive_losses >= self.config.max_consecutive_losses:
            return False, f"Đã đạt {self._consecutive_losses} lệnh lỗ liên tiếp"

        # 4. Max losses per session
        if self.config.max_losses_per_session and self._session_losses >= self.config.max_losses_per_session:
            return False, f"Đã đạt giới hạn {self._session_losses} lệnh lỗ trong phiên"

        # 5. Đã có vị thế mở rồi?
        if self.current_position:
            return False, "Đã có vị thế đang mở"

        return True, "OK"

    # ------------------------------------------------------------------ #
    #  Gọi sau khi đóng lệnh → cập nhật thống kê + kích hoạt cooldown
    # ------------------------------------------------------------------ #
    def _on_position_closed(self, pnl_usd: float, close_time: datetime):
        """
        Gọi khi lệnh đóng (SL/TP/Time) để cập nhật rủi ro
        """
        self._recent_trades.append(pnl_usd)

        # Cập nhật daily loss
        if close_time.date() == self._current_day:
            self._daily_loss += pnl_usd
        else:
            self._daily_loss = pnl_usd
            self._current_day = close_time.date()

        # Đếm chuỗi lỗ + session loss
        if pnl_usd < 0:
            self._consecutive_losses += 1
            self._session_losses += 1

            # Kích hoạt cooldown nếu có thiết lập
            if self.config.cooldown_minutes and self.config.cooldown_minutes > 0:
                self._cooldown_until = close_time + timedelta(minutes=self.config.cooldown_minutes)
                logger.warning(f"LỖ → Kích hoạt COOLDOWN {self.config.cooldown_minutes} phút "
                              f"đến {self._cooldown_until.strftime('%H:%M:%S')}")
        else:
            # Thắng → reset chuỗi lỗ
            self._consecutive_losses = 0



    # ------------------------------------------------------------------ #
    #  Helper tính thời gian + giờ giao dịch
    # ------------------------------------------------------------------ #
    def _timeframe_minutes(self) -> int:
        tf = str(self.config.timeframe).lower()
        if tf.endswith("min"):
            return max(1, int(tf[:-3]))
        if tf.endswith("h"):
            return max(1, int(tf[:-1])) * 60
        if tf.endswith("d"):
            return max(1, int(tf[:-1])) * 1440
        return 60

    def _calculate_required_bars(self) -> int:
        lookback = max(
            self.config.range_lookback,
            self.config.market_state_window,
            100,
        )
        return lookback + 200  # dư dả cho ATR, ADX, MA

    def _within_trading_hours(self, dt: datetime) -> bool:
        if not self.config.trading_hours:
            return True
        minutes = dt.hour * 60 + dt.minute
        for sess in self.config.trading_hours:
            try:
                s, e = sess.split("-")
                start = sum(int(x) * 60 ** i for i, x in enumerate(reversed(s.split(":"))))
                end = sum(int(x) * 60 ** i for i, x in enumerate(reversed(e.split(":"))))
                if end < start:  # qua đêm
                    if minutes >= start or minutes <= end:
                        return True
                else:
                    if start <= minutes <= end:
                        return True
            except Exception:
                continue
        return False

    # ------------------------------------------------------------------ #
    #  Cập nhật dữ liệu (live + backtest đều dùng hàm này)
    # ------------------------------------------------------------------ #
    async def _update_data(self) -> None:
        """Lấy dữ liệu mới nhất và tính toàn bộ chỉ báo."""
        if not self.config.symbol:
            return

        end = datetime.now(timezone.utc)
        start = end - timedelta(minutes=self._calculate_required_bars() * self._timeframe_minutes() * 2)

        df = await get_full_indicators(
            storage=self.storage,
            symbol=self.config.symbol,
            start_time=start,
            end_time=end,
            timeframe=self.config.timeframe,
            fast=self.config.fast_ma,
            slow=self.config.slow_ma,
            trend=getattr(self.config, "trend_ma", 55),
            ma_type=self.config.ma_type,
            config=self.config,
        )

        if df is not None and not df.empty:
            self._df = df.copy()
    
    # ------------------------------------------------------------------ #
    # Log counter
    # ------------------------------------------------------------------ #
    def _log_debug_counter(self, force: bool = False):
        counters = dict(self._debug_counters)
        counters["sample_bars_logged"] = self._sample_bars_logged
        counters["reasons_logged"] = len(self._debug_reasons)
        counters["actions_logged"] = counters.get("entry_allowed", 0)

        logger.info("="*80)  # Dùng INFO để backtest thấy luôn
        logger.info("DEBUG COUNTERS - %s", datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M"))
        logger.info("Total bars     : %d", counters.get("total_bars", 0))
        logger.info("Breakout ready : %d", counters.get("breakout_ready", 0))
        logger.info("Entry allowed  : %d", counters.get("entry_allowed", 0))
        logger.info("Range filter   : %d", counters.get("range_filter", 0))
        logger.info("Momentum/ADX   : %d", counters.get("adx_weak", 0))
        logger.info("Spread/ATR     : %d", counters.get("spread_high", 0))
        logger.info("Sample logged  : %d/%d", self._sample_bars_logged, self._MAX_SAMPLE_BARS)
        logger.info("="*80)

        if self._debug_reasons:
            from collections import Counter
            top_reasons = Counter(self._debug_reasons).most_common(10)
            logger.info("Top 10 reasons: %s", top_reasons)

        # Reset SAU KHI ĐÃ IN XONG!
        self._reset_debug_counters()

    def _reset_debug_counters(self):
        # Reset counters
        self._debug_counters = defaultdict(int)
        self._debug_reasons = []
        self._sample_bars_logged = 0
    # ------------------------------------------------------------------ #
    #  KIỂM TRA TÍN HIỆU – LIVE = BACKTEST 100%
    # ------------------------------------------------------------------ #
    def _check_single_bar(self, row: pd.Series, prev_row: pd.Series, dt: datetime) -> tuple[Optional[str], str]:
        """
        Hàm kiểm tra tín hiệu cho 1 bar
        Trả về: (signal: 'buy'/'sell'/None, reason: str)
        """
        self._debug_counters["total_bars"] += 1

        # 1. Giờ giao dịch
        if not self._within_trading_hours(dt):
            self._debug_counters["trading_hours"] += 1
            self._debug_reasons.append("Ngoài giờ")
            return None, "Ngoài giờ"

        # 2. Đang có lệnh (chỉ áp dụng cho live)
        if self.current_position and hasattr(self, 'check_signal'):  # chỉ live mới có current_position
            self._debug_counters["open_trades"] += 1
            self._debug_reasons.append("Đang có lệnh")
            return None, "Đang có lệnh"

        # 3. Cooldown
        if self._cooldown_until and datetime.now(timezone.utc) < self._cooldown_until:
            self._debug_counters["cooldown"] += 1
            self._debug_reasons.append("Đang Cooldown")
            return None, "Đang Cooldown"

        # 4. Breakout
        if self._sample_bars_logged < 5:
            logger.info("DEBUG RANGE | %s | High=%.3f RangeHigh=%.3f (+%.3f) | Low=%.3f RangeLow=%.3f (-%.3f)",
                        dt.strftime("%m-%d %H"), 
                        row["high"], row["range_high"], row["high"] - row["range_high"],
                        row["low"], row["range_low"], row["range_low"] - row["low"])
            self._sample_bars_logged += 1
        buffer = (self.config.breakout_buffer_atr) * row["atr"]
        buy_breakout = row["high"] >= row["range_high"] + buffer and prev_row["high"] < prev_row["range_high"] + buffer
        sell_breakout = row["low"] <= row["range_low"] - buffer and prev_row["low"] > prev_row["range_low"] - buffer

        if (buy_breakout or sell_breakout) and self._sample_bars_logged < self._MAX_SAMPLE_BARS:
            side = "BUY" if buy_breakout else "SELL"
            logger.info(
                "[SAMPLE %s] %s | C=%.3f | Range %.3f–%.3f | ATR=%.2f | MACD_H=%.5f ADX=%.1f | "
                "→ ĐANG KIỂM TRA CÁC BỘ LỌC...",
                side,
                dt.strftime("%m-%d %H:%M"),
                row["close"],
                row["range_low"], row["range_high"],
                row["atr"],
                row["macd_hist"], row["adx"]
            )
            self._sample_bars_logged += 1
            
        if not (buy_breakout or sell_breakout):
            self._debug_counters["range_filter"] += 1
            self._debug_reasons.append("Chưa breakout")
            return None, "Chưa breakout"

        self._debug_counters["breakout_ready"] += 1

        # 5. Spread, ATR, Range guard
        if (self.config.spread_atr_max or 0) > 0 and row.get("spread", 0) > (self.config.spread_atr_max or 0) * row["atr"]:
            self._debug_counters["spread_high"] += 1
            self._debug_reasons.append("Spread cao")
            return None, "Spread cao"

        if (self.config.range_min_atr or 0) > 0 and row["atr"] < self.config.range_min_atr:
            self._debug_counters["atr_low"] += 1
            self._debug_reasons.append("ATR thấp")
            return None, "ATR thấp"

        if (self.config.range_min_points or 0) > 0:
            width = row["range_high"] - row["range_low"]
            if width < self.config.range_min_points:
                self._debug_counters["range_narrow"] += 1
                self._debug_reasons.append("Range hẹp")
                return None, "Range hẹp"

        # 6. Side permission
        if buy_breakout and not getattr(self.config, "allow_buy", True):
            self._debug_counters["buy_disabled"] += 1
            self._debug_reasons.append("Không cho BUY")
            return None, "Không cho BUY"
        if sell_breakout and not getattr(self.config, "allow_sell", True):
            self._debug_counters["sell_disabled"] += 1
            self._debug_reasons.append("Không cho SELL")
            return None, "Không cho SELL"

        # 7. Momentum + ADX + Trend
        # Kiểm tra MACD
        if self.config.momentum_type in ("macd", "hybrid"):
            macd_hist = row["macd_hist"]

            if buy_breakout and macd_hist <= 0:
                self._debug_counters["momentum_macd_buy"] = (
                    self._debug_counters.get("momentum_macd_buy", 0) + 1
                )
                self._debug_reasons.append(f"Momentum yếu: MACD không ủng hộ BUY (macd_hist={macd_hist:.4f} <= 0)")
                return None, f"Momentum yếu: MACD không ủng hộ BUY (macd_hist={macd_hist:.4f} <= 0)"

            if sell_breakout and macd_hist >= 0:
                self._debug_counters["momentum_macd_sell"] = (
                    self._debug_counters.get("momentum_macd_sell", 0) + 1
                )
                self._debug_reasons.append(f"Momentum yếu: MACD không ủng hộ SELL (macd_hist={macd_hist:.4f} >= 0)")
                return None, f"Momentum yếu: MACD không ủng hộ SELL (macd_hist={macd_hist:.4f} >= 0)"
        # Kiểm tra RSI
        if self.config.momentum_type in ("rsi", "hybrid"):
            rsi = row["rsi"]
            long_thr = self.config.rsi_threshold_long
            short_thr = self.config.rsi_threshold_short

            if buy_breakout and rsi <= long_thr:
                self._debug_counters["momentum_rsi_buy"] = (
                    self._debug_counters.get("momentum_rsi_buy", 0) + 1
                )
                self._debug_reasons.append(f"Momentum yếu: RSI chưa đủ mạnh cho BUY (rsi={rsi:.2f} <= long_thr={long_thr})")
                return None, f"Momentum yếu: RSI chưa đủ mạnh cho BUY (rsi={rsi:.2f} <= long_thr={long_thr})"

            if sell_breakout and rsi >= short_thr:
                self._debug_counters["momentum_rsi_sell"] = (
                    self._debug_counters.get("momentum_rsi_sell", 0) + 1
                )
                self._debug_reasons.append(f"Momentum yếu: RSI chưa đủ yếu cho SELL (rsi={rsi:.2f} >= short_thr={short_thr})")
                return None, f"Momentum yếu: RSI chưa đủ yếu cho SELL (rsi={rsi:.2f} >= short_thr={short_thr})"
        # Kiểm tra ADX
        adx_thr = self.config.adx_threshold
        if row["adx"] < adx_thr:
            self._debug_counters["adx_weak"] = (
                self._debug_counters.get("adx_weak", 0) + 1
            )
            self._debug_reasons.append(f"ADX yếu: adx={row['adx']:.2f} < adx_thr={adx_thr}")
            return None, f"ADX yếu: adx={row['adx']:.2f} < adx_thr={adx_thr}"
        # Kiểm tra so với trend MA (filter theo xu hướng lớn)
        trend_ma_len = getattr(self.config, "trend_ma", 0)
        if trend_ma_len > 0:
            close_price = row["close"]
            trend_ma_val = row["trend_ma"]

            if buy_breakout and close_price <= trend_ma_val:
                self._debug_counters["trend_filter_buy"] = (
                    self._debug_counters.get("trend_filter_buy", 0) + 1
                )
                self._debug_reasons.append(
                    f"Không cùng xu hướng: giá chưa ở trên trend_ma cho BUY - "
                    f"(close={close_price:.2f}, trend_ma={trend_ma_val:.2f})"                    
                )
                return None, (
                    f"Không cùng xu hướng: giá chưa ở trên trend_ma cho BUY - "
                    f"(close={close_price:.2f}, trend_ma={trend_ma_val:.2f})"
                )

            if sell_breakout and close_price >= trend_ma_val:
                self._debug_counters["trend_filter_sell"] = (
                    self._debug_counters.get("trend_filter_sell", 0) + 1
                )
                self._debug_reasons.append(
                    f"Không cùng xu hướng: giá chưa ở dưới trend_ma cho SELL - "
                    f"(close={close_price:.2f}, trend_ma={trend_ma_val:.2f})"
                )
                return None, (
                    f"Không cùng xu hướng: giá chưa ở dưới trend_ma cho SELL - "
                    f"(close={close_price:.2f}, trend_ma={trend_ma_val:.2f})"
                )

        # 8. Volume
        if self.config.min_volume_multiplier and "tick_volume_avg" in row:
            if row["tick_volume"] < self.config.min_volume_multiplier * row["tick_volume_avg"]:
                self._debug_counters["volume_low"] += 1
                self._debug_reasons.append("Volume yếu")                
                return None, "Volume yếu"

        # TẤT CẢ QUA → CHO PHÉP VÀO LỆNH
        self._debug_counters["entry_allowed"] += 1

        side = "BUY" if buy_breakout else "SELL"
        logger.info("ENTRY %s OK | %s | Price=%.3f | Range %.3f–%.3f (+%.3f ATR buffer) | "
                    "MACD_H=%.5f | ADX=%.1f | SL/TP=%.1fx/%.1fx",
                    side,
                    dt.strftime("%m-%d %H:%M"),
                    row["close"],
                    row["range_low"], row["range_high"],
                    buffer / row["atr"],
                    row["macd_hist"], row["adx"],
                    self.config.sl_atr, self.config.tp_atr)

        return ("buy" if buy_breakout else "sell"), "OK"
    # ------------------------------------------------------------------ #
    #  Tính tín hiệu breakout (dùng trong backtest)
    # ------------------------------------------------------------------ #
    def calculate_signals(self, df: pd.DataFrame) -> pd.DataFrame:
        """Dùng cho backtest – df đã có warmup đầy đủ"""
        df = df.copy()
        df["action"] = 0
        df["reason"] = ""

        for i in range(1, len(df)):
            row = df.iloc[i]
            prev = df.iloc[i-1]
            dt = pd.to_datetime(row["datetime"])

            signal, _ = self._check_single_bar(row, prev, dt)
            if signal == "buy":
                df.at[df.index[i], "action"] = 2
            elif signal == "sell":
                df.at[df.index[i], "action"] = -2

        self._log_debug_counter(force=True)
        return df
    # ------------------------------------------------------------------ #
    #  Kiểm tra tín hiệu live (gọi mỗi poll)
    # ------------------------------------------------------------------ #
    def check_signal(self) -> Optional[str]:
        if self._df is None or len(self._df) < 10:
            return None
        row = self._df.iloc[-1]
        prev = self._df.iloc[-2]
        dt = pd.Timestamp(row["datetime"]).to_pydatetime()

        signal, reason = self._check_single_bar(row, prev, dt)
        
        if signal:
            return signal
        
        
            self._log_debug_counter(force=True)
        now = datetime.now(timezone.utc)
        if now.hour == 23 and 55 <= now.minute <= 59:
            self._reset_debug_counters()
        return None

    # ------------------------------------------------------------------ #
    #  Mở / đóng lệnh live (paper mode hoặc real)
    # ------------------------------------------------------------------ #
    async def open_position(self, side: str, price: float, dt: datetime, atr: float) -> bool:
        can_open, reason = self._can_open_new_position(dt)
        if not can_open:
            logger.warning(f"Không mở lệnh được: {reason}")
            return False
        
        if self.config.paper_mode:
            return await self._open_paper_position(side, price, dt, atr)

        if mt5 is None:
            logger.error("MT5 không khả dụng → không thể mở lệnh real")
            return False

        # === Tính volume theo risk ===
        volume = self.config.volume
        if self.config.size_from_risk and self.config.capital > 0 and self.config.risk_pct > 0:
            risk_amount = self.config.capital * self.config.risk_pct
            sl_distance_points = self.config.sl_atr * atr
            # khoảng cách SL tính theo giá
            sl_distance_pips = sl_distance_points / self.config.pip_size
            if sl_distance_pips > 0:
                volume = risk_amount / (sl_distance_pips * self.config.contract_size)
                volume = max(round(volume, 2), 0.01)  # lot tối thiểu

        sl = price - self.config.sl_atr * atr if side == "buy" else price + self.config.sl_atr * atr
        tp = price + self.config.tp_atr * atr if side == "buy" else price - self.config.tp_atr * atr

        request = {
            "action": mt5.TRADE_ACTION_DEAL,
            "symbol": self.config.symbol,
            "volume": volume,
            "type": mt5.ORDER_TYPE_BUY if side == "buy" else mt5.ORDER_TYPE_SELL,
            "price": price,
            "sl": round(sl, 6),
            "tp": round(tp, 6),
            "deviation": self.config.allowed_deviation_points,  # dùng tham số config
            "magic": 123456,
            "comment": "MA_Breakout_Live",
            "type_time": mt5.ORDER_TIME_GTC,
            "type_filling": mt5.ORDER_FILLING_IOC,
        }

        # === Dùng đúng tham số từ config ===
        max_attempts = self.config.order_retry_times
        base_delay_ms = self.config.order_retry_delay_ms

        for attempt in range(1, max_attempts + 1):
            result = mt5.order_send(request)

            # Khi lệnh thành công:
            if result and result.retcode == mt5.TRADE_RETCODE_DONE:
                logger.info(f"MỞ LỆNH THÀNH CÔNG | {side.upper()} {volume} lots @ {result.price:.5f}")
                self.current_position = Position(
                    symbol=self.config.symbol,
                    type=side,
                    volume=volume,
                    open_price=result.price,
                    open_time=dt,
                    stop_loss=result.sl,
                    take_profit=result.tp,
                    order_id=result.order,
                )
                # Reset session loss counter khi mở lệnh mới (tùy chiến lược)
                # self._session_losses = 0  # có thể bỏ nếu muốn giữ xuyên suốt
                return True

            # Xử lý lỗi phổ biến
            error_map = {
                mt5.TRADE_RETCODE_REQUOTE: "Requote → thử lại với giá mới",
                mt5.TRADE_RETCODE_PRICE_OFF: "Giá thay đổi quá nhanh",
                mt5.TRADE_RETCODE_INVALID_PRICE: "Giá không hợp lệ",
                mt5.TRADE_RETCODE_INVALID_STOPS: "SL/TP quá gần giá thị trường",
                mt5.TRADE_RETCODE_MARKET_CLOSED: "Thị trường đang đóng cửa",
                mt5.TRADE_RETCODE_NO_MONEY: "Không đủ margin!",
                mt5.TRADE_RETCODE_TOO_MANY_REQUESTS: "Gửi lệnh quá nhanh",
            }
            reason = error_map.get(result.retcode if result else -1, f"retcode={result.retcode if result else 'None'}")
            logger.warning(f"Lệnh thất bại [{attempt}/{max_attempts}] | {reason} | {result.comment if result else ''}")

            if attempt < max_attempts:
                delay_sec = base_delay_ms / 1000.0 * attempt  # tăng dần: 0.8s, 1.6s, 2.4s...
                await asyncio.sleep(delay_sec)

        logger.error(f"ĐÃ THẤT BẠI HOÀN TOÀN mở lệnh {side.upper()} sau {max_attempts} lần thử")
        return False

    async def _open_paper_position(self, side: str, price: float, dt: datetime, atr: float) -> bool:
        """Giả lập mở lệnh trong paper mode"""
        sl = price - self.config.sl_atr * atr if side == "buy" else price + self.config.sl_atr * atr
        tp = price + self.config.tp_atr * atr if side == "buy" else price - self.config.tp_atr * atr
        volume = self.config.volume or 0.01

        self.current_position = Position(
            symbol=self.config.symbol,
            type=side,
            volume=volume,
            open_price=price,
            open_time=dt,
            stop_loss=sl,
            take_profit=tp,
        )
        logger.info(f"[PAPER] MỞ {side.upper()} | {volume} lots | Giá: {price:.5f}")
        return True
    
    def close_position(self, price: float, dt: datetime, reason: str = "manual"):
        if not self.current_position:
            return

        pos = self.current_position
        pnl_points = (price - pos.open_price) if pos.type == "buy" else (pos.open_price - price)
        pnl_usd = pnl_points * pos.volume * self.config.contract_size

        logger.info(f"ĐÓNG LỆNH {pos.type.upper()} | PnL: {pnl_usd:+.2f} USD | Lý do: {reason}")

        # Cập nhật thống kê rủi ro
        self._on_position_closed(pnl_usd, dt)

        self.current_position = None