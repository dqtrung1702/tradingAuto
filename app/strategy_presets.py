"""Định nghĩa các bộ chiến lược MA/EMA preset cho dashboard/CLI."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, Literal, Optional


@dataclass(frozen=True)
class StrategyPreset:
    name: str
    description: str
    fast_ma: int
    slow_ma: int
    ma_type: Literal["sma", "ema"]
    timeframe: str
    spread_atr_max: float
    momentum_type: Literal["macd", "pct", "hybrid"]
    momentum_window: int
    momentum_threshold: float
    macd_fast: int
    macd_slow: int
    macd_signal: int
    macd_threshold: float
    sl_atr: float
    tp_atr: float
    range_lookback: int
    range_min_atr: float
    range_min_points: float
    breakout_buffer_atr: float
    breakout_confirmation_bars: int
    atr_baseline_window: int
    atr_multiplier_min: float
    atr_multiplier_max: float
    trading_hours: Optional[str] = None
    max_daily_loss: Optional[float] = None
    max_consecutive_losses: Optional[int] = None
    adx_window: int = 14
    adx_threshold: float = 0.0
    rsi_threshold_long: Optional[float] = None
    rsi_threshold_short: Optional[float] = None
    trail_trigger_atr: float = 1.0
    trail_atr_mult: float = 1.0
    max_losses_per_session: Optional[int] = None
    cooldown_minutes: Optional[int] = None


PRESETS: Dict[str, StrategyPreset] = {
    "ma_10_50": StrategyPreset(
        name="MA 10/50",
        description="Trend mạnh, breakout",
        fast_ma=10,
        slow_ma=50,
        ma_type="sma",
        timeframe="1min",
        spread_atr_max=0.15,
        momentum_type="macd",
        momentum_window=10,
        momentum_threshold=0.2,
        macd_fast=12,
        macd_slow=26,
        macd_signal=9,
        macd_threshold=0.05,
        sl_atr=2.0,
        tp_atr=4.0,
        range_lookback=40,
        range_min_atr=0.9,
        range_min_points=0.6,
        breakout_buffer_atr=0.6,
        breakout_confirmation_bars=2,
        atr_baseline_window=14,
        atr_multiplier_min=0.9,
        atr_multiplier_max=3.5,
        trading_hours="19:30-23:30,01:00-02:30",
    ),
    "ma_20_50": StrategyPreset(
        name="MA 20/50",
        description="Trend trung bình",
        fast_ma=20,
        slow_ma=50,
        ma_type="sma",
        timeframe="5min",
        spread_atr_max=0.2,
        momentum_type="macd",
        momentum_window=12,
        momentum_threshold=0.15,
        macd_fast=12,
        macd_slow=26,
        macd_signal=9,
        macd_threshold=0.03,
        sl_atr=1.8,
        tp_atr=3.0,
        range_lookback=50,
        range_min_atr=0.8,
        range_min_points=0.8,
        breakout_buffer_atr=0.5,
        breakout_confirmation_bars=2,
        atr_baseline_window=20,
        atr_multiplier_min=0.8,
        atr_multiplier_max=3.5,
        trading_hours="19:30-23:00",
    ),
    "ema_21_89": StrategyPreset(
        name="EMA 21/89",
        description="Pullback rõ",
        fast_ma=21,
        slow_ma=89,
        ma_type="ema",
        timeframe="5min",
        spread_atr_max=0.2,
        momentum_type="macd",
        momentum_window=14,
        momentum_threshold=0.1,
        macd_fast=12,
        macd_slow=26,
        macd_signal=9,
        macd_threshold=0.0,
        sl_atr=2.0,
        tp_atr=3.0,
        range_lookback=45,
        range_min_atr=0.85,
        range_min_points=0.5,
        breakout_buffer_atr=0.5,
        breakout_confirmation_bars=2,
        atr_baseline_window=18,
        atr_multiplier_min=0.85,
        atr_multiplier_max=3.0,
        trading_hours="19:30-23:30",
    ),
    "ma_50_200": StrategyPreset(
        name="MA 50/200",
        description="Trend dài, swing",
        fast_ma=50,
        slow_ma=200,
        ma_type="sma",
        timeframe="15min",
        spread_atr_max=0.25,
        momentum_type="macd",
        momentum_window=20,
        momentum_threshold=0.2,
        macd_fast=12,
        macd_slow=26,
        macd_signal=9,
        macd_threshold=0.02,
        sl_atr=2.5,
        tp_atr=5.0,
        range_lookback=60,
        range_min_atr=1.0,
        range_min_points=1.0,
        breakout_buffer_atr=0.6,
        breakout_confirmation_bars=2,
        atr_baseline_window=21,
        atr_multiplier_min=0.9,
        atr_multiplier_max=4.0,
        trading_hours="19:30-23:30",
    ),
    "ema_8_21": StrategyPreset(
        name="EMA 8/21",
        description="Scalp nhanh",
        fast_ma=8,
        slow_ma=21,
        ma_type="ema",
        timeframe="5min",
        spread_atr_max=0.12,
        momentum_type="macd",
        momentum_window=8,
        momentum_threshold=0.07,
        macd_fast=12,
        macd_slow=26,
        macd_signal=9,
        macd_threshold=0.0002,
        sl_atr=1.5,
        tp_atr=2.5,
        range_lookback=30,
        range_min_atr=0.6,
        range_min_points=0.0,
        breakout_buffer_atr=0.25,
        breakout_confirmation_bars=1,
        atr_baseline_window=14,
        atr_multiplier_min=1.0,
        atr_multiplier_max=3.0,
        trading_hours="19:30-23:30",
    ),
    "asia_open_range": StrategyPreset(
        name="Asia Open Range Guard",
        description="00:00-05:00 VN – breakout chọn lọc khi ATR ~6 USD",
        fast_ma=8,
        slow_ma=21,
        ma_type="ema",
        timeframe="5min",
        spread_atr_max=0.09,
        momentum_type="hybrid",
        momentum_window=12,
        momentum_threshold=0.05,
        macd_fast=12,
        macd_slow=26,
        macd_signal=9,
        macd_threshold=0.00015,
        sl_atr=1.5,
        tp_atr=2.4,
        range_lookback=40,
        range_min_atr=0.9,
        range_min_points=5.0,
        breakout_buffer_atr=0.24,
        breakout_confirmation_bars=2,
        atr_baseline_window=14,
        atr_multiplier_min=0.95,
        atr_multiplier_max=3.1,
        trading_hours="00:00-05:00",
        max_daily_loss=250,
        max_consecutive_losses=3,
        adx_window=14,
        adx_threshold=22,
        rsi_threshold_long=60,
        rsi_threshold_short=40,
        trail_trigger_atr=1.4,
        trail_atr_mult=0.9,
        max_losses_per_session=2,
        cooldown_minutes=45,
    ),
    "us_late_momentum": StrategyPreset(
        name="US Late Momentum",
        description="22:00-24:00 VN – breakout ATR cao (~9-10 USD)",
        fast_ma=8,
        slow_ma=21,
        ma_type="ema",
        timeframe="5min",
        spread_atr_max=0.12,
        momentum_type="hybrid",
        momentum_window=14,
        momentum_threshold=0.09,
        macd_fast=12,
        macd_slow=26,
        macd_signal=9,
        macd_threshold=0.00025,
        sl_atr=1.6,
        tp_atr=2.7,
        range_lookback=35,
        range_min_atr=1.2,
        range_min_points=7.0,
        breakout_buffer_atr=0.35,
        breakout_confirmation_bars=1,
        atr_baseline_window=14,
        atr_multiplier_min=1.25,
        atr_multiplier_max=3.8,
        trading_hours="22:00-24:00",
        max_daily_loss=400,
        max_consecutive_losses=3,
        adx_window=14,
        adx_threshold=28,
        rsi_threshold_long=62,
        rsi_threshold_short=38,
        trail_trigger_atr=2.1,
        trail_atr_mult=1.3,
        max_losses_per_session=3,
        cooldown_minutes=30,
    ),
    "asia_spike_breakout": StrategyPreset(
        name="Asia Spike Breakout",
        description="Kịch bản ATR >7.5 ngay đầu phiên Á",
        fast_ma=8,
        slow_ma=21,
        ma_type="ema",
        timeframe="5min",
        spread_atr_max=0.1,
        momentum_type="hybrid",
        momentum_window=10,
        momentum_threshold=0.08,
        macd_fast=12,
        macd_slow=26,
        macd_signal=9,
        macd_threshold=0.0002,
        sl_atr=1.7,
        tp_atr=2.8,
        range_lookback=30,
        range_min_atr=1.1,
        range_min_points=5.5,
        breakout_buffer_atr=0.3,
        breakout_confirmation_bars=1,
        atr_baseline_window=14,
        atr_multiplier_min=1.05,
        atr_multiplier_max=3.4,
        trading_hours="00:00-03:00",
        max_daily_loss=320,
        max_consecutive_losses=2,
        adx_window=14,
        adx_threshold=24,
        rsi_threshold_long=61,
        rsi_threshold_short=39,
        trail_trigger_atr=1.8,
        trail_atr_mult=1.05,
        max_losses_per_session=2,
        cooldown_minutes=60,
    ),
}


def resolve_preset(preset_id: Optional[str]) -> Optional[StrategyPreset]:
    if preset_id and preset_id in PRESETS:
        return PRESETS[preset_id]
    return None
