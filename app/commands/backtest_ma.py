# -*- coding: utf-8 -*-
"""Backtest chiến lược MA Crossover sử dụng dữ liệu tick trong CSDL."""

from __future__ import annotations

from datetime import datetime, timezone, timedelta, time
from typing import Optional, List, Dict, Any

import pandas as pd
from uuid import uuid4
import asyncio
import random

from app.storage import Storage
from app.strategies.ma_crossover import MAConfig, MACrossoverStrategy



async def run_backtest(
    *,
    db_url: str,
    symbol: str,
    start_str: str,
    end_str: str,
    saved_config_id: Optional[int] = None,
    saved_backtest_id: Optional[int] = None,
    visible_start_str: Optional[str] = None,
    cancel_event: Optional[asyncio.Event] = None,
    max_holding_minutes: Optional[int] = None,
    fast: int,
    slow: int,
    timeframe: str,
    ma_type: str,
    trend: int,
    risk_pct: float,
    capital: float = 100.0,
    trail_trigger_atr: float,
    trail_atr_mult: float,
    spread_atr_max: float,
    reverse_exit: bool,
    market_state_window: int,
    sl_atr: float,
    tp_atr: float,
    volume: float = 0.1,
    contract_size: float,
    sl_pips: Optional[float],
    tp_pips: Optional[float],
    pip_size: float,
    size_from_risk: bool = True,
    momentum_type: str,
    momentum_window: int,
    momentum_threshold: float,
    macd_fast: int,
    macd_slow: int,
    macd_signal: int,
    macd_threshold: float,
    range_lookback: int,
    range_min_atr: float,
    range_min_points: float,
    breakout_buffer_atr: float,
    breakout_confirmation_bars: int,
    atr_baseline_window: int,
    atr_multiplier_min: float,
    atr_multiplier_max: float,
    trading_hours: Optional[str],
    adx_window: int,
    adx_threshold: float,
    rsi_threshold_long: float,
    rsi_threshold_short: float,
    max_daily_loss: Optional[float],
    max_loss_streak: Optional[int],
    max_losses_per_session: Optional[int],
    cooldown_minutes: Optional[int],
    allow_buy: bool,
    allow_sell: bool,
    order_retry_times: int = 1,
    order_retry_delay_ms: int = 0,
    safety_entry_atr_mult: float = 0.0,
    spread_samples: int = 1,
    spread_sample_delay_ms: int = 0,
    allowed_deviation_points: int = 300,
    volatility_spike_atr_mult: float = 0.0,
    spike_delay_ms: int = 0,
    skip_reset_window: bool = True,
    latency_min_ms: int = 0,
    latency_max_ms: int = 0,
    slippage_usd: float = 0.0,
    order_reject_prob: float = 0.0,
    base_spread_points: int = 0,
    spread_spike_chance: float = 0.0,
    spread_spike_min_points: int = 0,
    spread_spike_max_points: int = 0,
    slip_per_atr_ratio: float = 0.0,
    requote_prob: float = 0.0,
    offquotes_prob: float = 0.0,
    timeout_prob: float = 0.0,
    stop_hunt_chance: float = 0.0,
    stop_hunt_min_atr_ratio: float = 0.0,
    stop_hunt_max_atr_ratio: float = 0.0,
    missing_tick_chance: float = 0.0,
    return_summary: bool = False,
    min_volume_multiplier: float = 1.1,
    slippage_pips: Optional[float] = 3.0,
) -> None:
    start = datetime.fromisoformat(start_str)
    end = datetime.fromisoformat(end_str)
    visible_start = datetime.fromisoformat(visible_start_str) if visible_start_str else start
    if saved_backtest_id is not None:
        saved_config_id = saved_backtest_id
    if end <= start:
        raise ValueError('--end phải lớn hơn --start')

    storage = Storage(db_url)
    await storage.init()

    # Auto-link backtest results với saved_backtests dựa trên hash cấu hình
    if saved_config_id is None:
        config_payload = {
            "symbol": symbol,
            "fast": fast,
            "slow": slow,
            "ma_type": ma_type,
            "timeframe": timeframe,
            "trend": trend,
            "risk_pct": risk_pct,
            "capital": capital,
            "trail_trigger_atr": trail_trigger_atr,
            "trail_atr_mult": trail_atr_mult,
            "spread_atr_max": spread_atr_max,
            "reverse_exit": reverse_exit,
            "market_state_window": market_state_window,
            "sl_atr": sl_atr,
            "tp_atr": tp_atr,
            "volume": volume,
            "contract_size": contract_size,
            "sl_pips": sl_pips,
            "tp_pips": tp_pips,
            "pip_size": pip_size,
            "size_from_risk": size_from_risk,
            "momentum_type": momentum_type,
            "momentum_window": momentum_window,
            "momentum_threshold": momentum_threshold,
            "macd_fast": macd_fast,
            "macd_slow": macd_slow,
            "macd_signal": macd_signal,
            "macd_threshold": macd_threshold,
            "range_lookback": range_lookback,
            "range_min_atr": range_min_atr,
            "range_min_points": range_min_points,
            "breakout_buffer_atr": breakout_buffer_atr,
            "breakout_confirmation_bars": breakout_confirmation_bars,
            "atr_baseline_window": atr_baseline_window,
            "atr_multiplier_min": atr_multiplier_min,
            "atr_multiplier_max": atr_multiplier_max,
            "trading_hours": trading_hours,
            "adx_window": adx_window,
            "adx_threshold": adx_threshold,
            "rsi_threshold_long": rsi_threshold_long,
            "rsi_threshold_short": rsi_threshold_short,
            "max_daily_loss": max_daily_loss,
            "max_loss_streak": max_loss_streak,
            "max_losses_per_session": max_losses_per_session,
            "cooldown_minutes": cooldown_minutes,
            "max_holding_minutes": max_holding_minutes,
            "allow_buy": allow_buy,
            "allow_sell": allow_sell,
            # Các trường còn lại trong SAVED_BACKTEST_CONFIG_FIELDS không dùng cho backtest -> set None
            "ensure_history_hours": None,
            "poll": None,
            "live": False,
            "ingest_live_db": None,
            "history_batch": None,
            "history_max_days": None,
            "order_retry_times": order_retry_times,
            "order_retry_delay_ms": order_retry_delay_ms,
            "safety_entry_atr_mult": safety_entry_atr_mult,
            "spread_samples": spread_samples,
            "spread_sample_delay_ms": spread_sample_delay_ms,
            "allowed_deviation_points": allowed_deviation_points,
            "volatility_spike_atr_mult": volatility_spike_atr_mult,
            "spike_delay_ms": spike_delay_ms,
            "skip_reset_window": skip_reset_window,
            "latency_min_ms": latency_min_ms,
            "latency_max_ms": latency_max_ms,
            "slippage_usd": slippage_usd,
            "order_reject_prob": order_reject_prob,
            "base_spread_points": base_spread_points,
            "spread_spike_chance": spread_spike_chance,
            "spread_spike_min_points": spread_spike_min_points,
            "spread_spike_max_points": spread_spike_max_points,
            "slip_per_atr_ratio": slip_per_atr_ratio,
            "requote_prob": requote_prob,
            "offquotes_prob": offquotes_prob,
            "timeout_prob": timeout_prob,
            "stop_hunt_chance": stop_hunt_chance,
            "stop_hunt_min_atr_ratio": stop_hunt_min_atr_ratio,
            "stop_hunt_max_atr_ratio": stop_hunt_max_atr_ratio,
            "missing_tick_chance": missing_tick_chance,
            "min_volume_multiplier": min_volume_multiplier,
            "slippage_pips": slippage_pips,
        }
        try:
            saved_config_id = await storage.insert_saved_backtest(config=config_payload)
        except Exception:
            saved_config_id = None

    config = MAConfig()
    config.symbol = symbol
    config.fast_ma = fast
    config.slow_ma = slow
    config.timeframe = timeframe
    config.ma_type = ma_type
    config.paper_mode = True

    config.trend_ma = trend
    config.risk_pct = risk_pct
    config.capital = capital
    config.trail_trigger_atr = trail_trigger_atr
    config.trail_atr_mult = trail_atr_mult
    config.spread_atr_max = spread_atr_max
    config.reverse_exit = bool(reverse_exit)
    config.market_state_window = market_state_window
    config.sl_atr = sl_atr
    config.tp_atr = tp_atr
    config.volume = volume
    config.contract_size = contract_size
    config.size_from_risk = size_from_risk
    config.max_holding_minutes = max_holding_minutes
    config.momentum_type = momentum_type
    config.momentum_window = momentum_window
    config.momentum_threshold = momentum_threshold
    config.macd_fast = macd_fast
    config.macd_slow = macd_slow
    config.macd_signal = macd_signal
    config.macd_threshold = macd_threshold
    config.range_lookback = range_lookback
    config.range_min_atr = range_min_atr
    config.range_min_points = range_min_points
    config.breakout_buffer_atr = breakout_buffer_atr
    config.breakout_confirmation_bars = (
        breakout_confirmation_bars
    )
    config.atr_baseline_window = atr_baseline_window
    config.atr_multiplier_min = atr_multiplier_min
    config.atr_multiplier_max = atr_multiplier_max
    config.trading_hours = [h.strip() for h in trading_hours.split(',')] if trading_hours else None
    config.adx_window = adx_window
    config.adx_threshold = adx_threshold
    config.rsi_threshold_long = rsi_threshold_long
    config.rsi_threshold_short = rsi_threshold_short
    config.max_daily_loss = max_daily_loss
    config.max_consecutive_losses = max_loss_streak
    config.max_losses_per_session = max_losses_per_session
    config.cooldown_minutes = cooldown_minutes
    config.allow_buy = allow_buy if allow_buy is not None else True
    config.allow_sell = allow_sell if allow_sell is not None else True
    config.order_retry_times = order_retry_times
    config.order_retry_delay_ms = order_retry_delay_ms
    config.safety_entry_atr_mult = safety_entry_atr_mult
    config.spread_samples = max(1, int(spread_samples or 1))
    config.spread_sample_delay_ms = max(0, int(spread_sample_delay_ms or 0))
    config.allowed_deviation_points = max(0, int(allowed_deviation_points or 0))
    config.volatility_spike_atr_mult = max(0.0, float(volatility_spike_atr_mult or 0.0))
    config.spike_delay_ms = max(0, int(spike_delay_ms or 0))
    config.skip_reset_window = bool(skip_reset_window)
    config.min_volume_multiplier = min_volume_multiplier
    config.slippage_pips = slippage_pips

    try:
        config.sl_pips = sl_pips  # type: ignore[attr-defined]
        config.tp_pips = tp_pips  # type: ignore[attr-defined]
        config.pip_size = pip_size  # type: ignore[attr-defined]
    except Exception:
        pass

    strategy = MACrossoverStrategy(config, None, storage)

    lat_min = max(0, latency_min_ms)
    lat_max = max(lat_min, latency_max_ms)
    slip_val = max(0.0, float(slippage_usd))
    reject_prob = max(0.0, float(order_reject_prob or 0.0))
    requote_prob = max(0.0, float(requote_prob or 0.0))
    offquotes_prob = max(0.0, float(offquotes_prob or 0.0))
    timeout_prob = max(0.0, float(timeout_prob or 0.0))
    slip_atr_ratio = max(0.0, float(slip_per_atr_ratio or 0.0))
    hunt_chance = max(0.0, float(stop_hunt_chance or 0.0))
    hunt_min = max(0.0, float(stop_hunt_min_atr_ratio or 0.0))
    hunt_max = max(hunt_min, float(stop_hunt_max_atr_ratio or hunt_min))
    missing_tick_chance = max(0.0, float(missing_tick_chance or 0.0))
    base_spread_pts = max(0, int(base_spread_points or 0))
    spike_chance = max(0.0, float(spread_spike_chance or 0.0))
    spike_min = max(0, int(spread_spike_min_points or 0))
    spike_max = max(spike_min, int(spread_spike_max_points or spike_min))
    safety_mult = max(0.0, float(safety_entry_atr_mult or 0.0))
    vol_spike_mult = max(0.0, float(volatility_spike_atr_mult or 0.0))
    skip_reset = bool(skip_reset_window)
    volume_mult = max(0.0, float(min_volume_multiplier or 0.0))
    slip_pips = float(slippage_pips) if slippage_pips is not None else None

    async def _run_range(range_start: datetime, range_end: datetime, visible_start_dt: Optional[datetime] = None) -> Dict[str, Any]:
        df = await strategy.calculate_signals(storage, range_start, range_end)
        if 'action' in df.columns:
            try:
                df['action'] = df['action'].fillna(0).astype(int)
            except Exception:
                df['action'] = df['action'].fillna(0).apply(lambda x: int(x) if pd.notna(x) else 0)
        else:
            df['action'] = 0
        if volume_mult > 0 and 'tick_volume' in df.columns:
            vol_window = max(5, int(range_lookback or 5))
            df['tick_volume_avg'] = df['tick_volume'].rolling(window=vol_window, min_periods=vol_window).mean().shift(1)

        required = ['datetime', 'open', 'high', 'low', 'close', 'fast_ma', 'slow_ma', 'atr']
        for col in required:
            if col not in df.columns:
                raise RuntimeError(f"Thiếu cột bắt buộc: {col}")

        trades: List[Dict] = []
        visible_start_dt = visible_start_dt or range_start

        def _compute_spread(pip: float) -> float:
            if pip <= 0:
                return 0.0
            spread_pts = base_spread_pts
            if spike_chance > 0 and random.random() < spike_chance:
                spread_pts += random.randint(spike_min, spike_max) if spike_max > spike_min else spike_min
            return spread_pts * pip

        def _apply_slippage(price: float, side: str, atr: float) -> float:
            slip_total = slip_val
            if slip_atr_ratio > 0 and atr > 0:
                slip_total += random.uniform(-slip_atr_ratio * atr, slip_atr_ratio * atr)
            if slip_total == 0:
                return price
            return price + slip_total if side == 'buy' else price - slip_total

        async def _simulate_fill(
            side: str,
            entry_price: float,
            entry_time: datetime,
            sl: float,
            tp: float,
            atr: float,
            pip: float,
        ) -> Optional[Dict[str, Any]]:
            attempts = max(1, int(order_retry_times) + 1)
            delay = max(0.0, float(order_retry_delay_ms or 0) / 1000.0)
            for attempt in range(attempts):
                r = random.random()
                if r < requote_prob:
                    # requote -> retry
                    pass
                elif r < requote_prob + offquotes_prob:
                    pass
                elif r < requote_prob + offquotes_prob + timeout_prob:
                    pass
                elif reject_prob > 0 and random.random() < reject_prob:
                    if attempt < attempts - 1 and delay > 0:
                        await asyncio.sleep(delay)
                    continue
                latency_ms = random.uniform(lat_min, lat_max) if lat_max > 0 else 0.0
                entry_time_adj = entry_time + timedelta(milliseconds=latency_ms)
                spread_val = _compute_spread(pip)
                mid_price = entry_price
                slipped_entry = _apply_slippage(mid_price, side, atr)
                if slip_pips is not None and pip > 0:
                    max_slip = abs(slip_pips) * pip
                    if side == 'buy' and slipped_entry - entry_price > max_slip:
                        slipped_entry = entry_price + max_slip
                    if side == 'sell' and entry_price - slipped_entry > max_slip:
                        slipped_entry = entry_price - max_slip
                if spread_val > 0:
                    half = spread_val / 2.0
                    slipped_entry = slipped_entry + half if side == 'buy' else slipped_entry - half
                if side == 'buy':
                    dist_sl = entry_price - sl
                    dist_tp = tp - entry_price
                    sl_adj = slipped_entry - dist_sl
                    tp_adj = slipped_entry + dist_tp
                else:
                    dist_sl = sl - entry_price
                    dist_tp = entry_price - tp
                    sl_adj = slipped_entry + dist_sl
                    tp_adj = slipped_entry - dist_tp
                return {
                    "entry_price": slipped_entry,
                    "entry_time": entry_time_adj,
                    "sl": sl_adj,
                    "tp": tp_adj,
                }
            return None
        i = 0
        while i < len(df) - 1:
            row = df.iloc[i]
            action = int(row.get('action', 0))
            if missing_tick_chance > 0 and random.random() < missing_tick_chance:
                i += 1
                continue
            if vol_spike_mult > 0 and i > 0:
                prev_close = float(df.iloc[i - 1].get('close'))
                if abs(float(row.get('close')) - prev_close) > float(row.get('atr') or 0.0) * vol_spike_mult:
                    i += 1
                    continue

            if action == 2:
                entry_bar = df.iloc[i + 1]
                if volume_mult > 0:
                    vol_avg = float(entry_bar.get('tick_volume_avg') or 0.0)
                    vol_cur = float(entry_bar.get('tick_volume') or 0.0)
                    if vol_avg > 0 and vol_cur < vol_avg * volume_mult:
                        i += 1
                        continue
                entry_price = float(entry_bar['open'])
                atr_val = float(entry_bar['atr'])
                entry_time = _to_datetime(entry_bar['datetime'])
                if skip_reset:
                    minute_of_day = entry_time.hour * 60 + entry_time.minute
                    if minute_of_day >= 23 * 60 + 59 or minute_of_day <= 10:
                        i += 1
                        continue
                if entry_time < visible_start_dt:
                    i += 1
                    continue
                range_high = float(row.get('range_high') or 0.0)
                buffer = atr_val * float(breakout_buffer_atr or 0.0)
                if safety_mult > 0:
                    max_buy_price = range_high + buffer + atr_val * safety_mult
                    if entry_price > max_buy_price:
                        i += 1
                        continue
                if entry_price < range_high + buffer:
                    i += 1
                    continue

                if sl_pips is not None:
                    sl = entry_price - float(sl_pips) * pip_size
                else:
                    sl = entry_price - sl_atr * atr_val
                if tp_pips is not None:
                    tp = entry_price + float(tp_pips) * pip_size
                else:
                    tp = entry_price + tp_atr * atr_val

                fill = await _simulate_fill('buy', entry_price, entry_time, sl, tp, atr_val, pip_size)
                if fill is None:
                    i += 1
                    continue
                entry_price = fill["entry_price"]
                entry_time = fill["entry_time"]
                sl = fill["sl"]
                tp = fill["tp"]

                trade_volume = volume
                if size_from_risk:
                    stop_dist = max(entry_price - sl, 0.0)
                    risk_amount = max(0.0, capital * risk_pct)
                    if stop_dist > 0 and risk_amount > 0 and contract_size > 0:
                        trade_volume = risk_amount / (stop_dist * contract_size)
                        trade_volume = max(round(trade_volume, 2), 0.01)

                exit_price = None
                exit_time = None
                hold_deadline = None
                if max_holding_minutes and max_holding_minutes > 0:
                    hold_deadline = entry_time + timedelta(minutes=max_holding_minutes)
                j = i + 1
                while j < len(df):
                    bar = df.iloc[j]
                    bar_dt = _to_datetime(bar['datetime'])
                    # stop-hunt mô phỏng
                    eff_low = float(bar['low'])
                    eff_high = float(bar['high'])
                    if hunt_chance > 0 and random.random() < hunt_chance and atr_val > 0:
                        hunt_depth = random.uniform(hunt_min, hunt_max) * atr_val
                        eff_low = min(eff_low, sl - hunt_depth)
                        eff_high = max(eff_high, tp + hunt_depth)
                    if missing_tick_chance > 0 and random.random() < missing_tick_chance:
                        j += 1
                        continue
                    if hold_deadline and bar_dt >= hold_deadline:
                        exit_price = float(bar['open']) if 'open' in bar else float(bar['close'])
                        exit_time = bar_dt
                        break
                    if eff_low <= sl:
                        exit_price = sl
                        exit_time = bar_dt
                        break
                    if eff_high >= tp:
                        exit_price = tp
                        exit_time = bar_dt
                        break
                    if reverse_exit and j + 1 < len(df) and int(df.iloc[j]['action']) == -2:
                        exit_bar = df.iloc[j + 1]
                        exit_price = float(exit_bar['open'])
                        exit_time = _to_datetime(exit_bar['datetime'])
                        break
                    j += 1
                if exit_price is None:
                    exit_price = float(df.iloc[-1]['close'])
                    exit_time = _to_datetime(df.iloc[-1]['datetime'])

                exit_price = _apply_slippage(exit_price, 'buy', atr_val)
                pnl = float(exit_price) - entry_price
                pct = pnl / entry_price if entry_price != 0 else 0.0
                trades.append({
                    'side': 'buy',
                    'entry_time': entry_time,
                    'entry_price': entry_price,
                    'exit_time': exit_time,
                    'exit_price': exit_price,
                    'stop_loss': sl,
                    'take_profit': tp,
                    'pnl': pnl,
                    'pct': pct,
                    'volume': trade_volume,
                    'usd_pnl': pnl * contract_size * trade_volume,
                })
                i = j + 1
                continue

            if action == -2:
                entry_bar = df.iloc[i + 1]
                if volume_mult > 0:
                    vol_avg = float(entry_bar.get('tick_volume_avg') or 0.0)
                    vol_cur = float(entry_bar.get('tick_volume') or 0.0)
                    if vol_avg > 0 and vol_cur < vol_avg * volume_mult:
                        i += 1
                        continue
                entry_price = float(entry_bar['open'])
                atr_val = float(entry_bar['atr'])
                entry_time = _to_datetime(entry_bar['datetime'])
                if skip_reset:
                    minute_of_day = entry_time.hour * 60 + entry_time.minute
                    if minute_of_day >= 23 * 60 + 59 or minute_of_day <= 10:
                        i += 1
                        continue
                range_low = float(row.get('range_low') or 0.0)
                buffer = atr_val * float(breakout_buffer_atr or 0.0)
                if safety_mult > 0:
                    min_sell_price = range_low - buffer - atr_val * safety_mult
                    if entry_price < min_sell_price:
                        i += 1
                        continue
                if entry_price > range_low - buffer:
                    i += 1
                    continue
                if sl_pips is not None:
                    sl = entry_price + float(sl_pips) * pip_size
                else:
                    sl = entry_price + sl_atr * atr_val
                if tp_pips is not None:
                    tp = entry_price - float(tp_pips) * pip_size
                else:
                    tp = entry_price - tp_atr * atr_val

                fill = await _simulate_fill('sell', entry_price, entry_time, sl, tp, atr_val, pip_size)
                if fill is None:
                    i += 1
                    continue
                entry_price = fill["entry_price"]
                entry_time = fill["entry_time"]
                sl = fill["sl"]
                tp = fill["tp"]

                trade_volume = volume
                if size_from_risk:
                    stop_dist = max(sl - entry_price, 0.0)
                    risk_amount = max(0.0, capital * risk_pct)
                    if stop_dist > 0 and risk_amount > 0 and contract_size > 0:
                        trade_volume = risk_amount / (stop_dist * contract_size)
                        trade_volume = max(round(trade_volume, 2), 0.01)

                exit_price = None
                exit_time = None
                hold_deadline = None
                if max_holding_minutes and max_holding_minutes > 0:
                    hold_deadline = entry_time + timedelta(minutes=max_holding_minutes)
                j = i + 1
                while j < len(df):
                    bar = df.iloc[j]
                    bar_dt = _to_datetime(bar['datetime'])
                    eff_low = float(bar['low'])
                    eff_high = float(bar['high'])
                    if hunt_chance > 0 and random.random() < hunt_chance and atr_val > 0:
                        hunt_depth = random.uniform(hunt_min, hunt_max) * atr_val
                        eff_low = min(eff_low, tp - hunt_depth)
                        eff_high = max(eff_high, sl + hunt_depth)
                    if missing_tick_chance > 0 and random.random() < missing_tick_chance:
                        j += 1
                        continue
                    if hold_deadline and bar_dt >= hold_deadline:
                        exit_price = float(bar['open']) if 'open' in bar else float(bar['close'])
                        exit_time = bar_dt
                        break
                    if eff_high >= sl:
                        exit_price = sl
                        exit_time = bar_dt
                        break
                    if eff_low <= tp:
                        exit_price = tp
                        exit_time = bar_dt
                        break
                    if reverse_exit and j + 1 < len(df) and int(df.iloc[j]['action']) == 2:
                        exit_bar = df.iloc[j + 1]
                        exit_price = float(exit_bar['open'])
                        exit_time = _to_datetime(exit_bar['datetime'])
                        break
                    j += 1
                if exit_price is None:
                    exit_price = float(df.iloc[-1]['close'])
                    exit_time = _to_datetime(df.iloc[-1]['datetime'])

                exit_price = _apply_slippage(exit_price, 'sell', atr_val)
                pnl = entry_price - float(exit_price)
                pct = pnl / entry_price if entry_price != 0 else 0.0
                trades.append({
                    'side': 'sell',
                    'entry_time': entry_time,
                    'entry_price': entry_price,
                    'exit_time': exit_time,
                    'exit_price': exit_price,
                    'stop_loss': sl,
                    'take_profit': tp,
                    'pnl': pnl,
                    'pct': pct,
                    'volume': trade_volume,
                    'usd_pnl': pnl * contract_size * trade_volume,
                })
                i = j + 1
                continue

            i += 1

        if cancel_event and cancel_event.is_set():
            raise asyncio.CancelledError("Backtest bị hủy")

        day_tag = range_start.strftime('%Y%m%d')
        run_id = f"bt_{symbol}_{day_tag}_{uuid4().hex[:6]}"
        run_start = _ensure_aware(visible_start_dt or range_start)
        run_end = _ensure_aware(range_end)
        now_utc = datetime.now(timezone.utc)
        db_rows = [
            {
                "run_id": run_id,
                "symbol": symbol,
                "saved_config_id": saved_config_id,
                "side": trade["side"],
                "entry_time": trade["entry_time"],
                "exit_time": trade["exit_time"],
                "entry_price": trade["entry_price"],
                "exit_price": trade["exit_price"],
                "stop_loss": trade["stop_loss"],
                "take_profit": trade["take_profit"],
                "volume": trade["volume"],
                "pnl": trade["pnl"],
                "pct": trade["pct"],
                "usd_pnl": trade["usd_pnl"],
                "run_start": run_start,
                "run_end": run_end,
                "created_at": now_utc,
            }
            for trade in trades
        ]
        await storage.insert_backtest_trades(db_rows, saved_backtest_id=saved_config_id, run_start=run_start)

        total = len(trades)
        wins = sum(1 for t in trades if t['pnl'] > 0)
        losses = sum(1 for t in trades if t['pnl'] <= 0)
        total_pnl = sum(float(t['pnl']) for t in trades)
        avg_pnl = total_pnl / total if total else 0.0
        total_usd_pnl = sum(float(t.get('usd_pnl') or 0.0) for t in trades)
        avg_usd_pnl = total_usd_pnl / total if total else 0.0

        return {
            'range_start': run_start,
            'range_end': run_end,
            'saved_backtest_id': saved_config_id,
            'total_trades': total,
            'wins': wins,
            'losses': losses,
            'total_pnl': total_pnl,
            'avg_pnl': avg_pnl,
            'total_usd_pnl': total_usd_pnl,
            'avg_usd_pnl': avg_usd_pnl,
            'db_run_id': run_id,
            'stored_trades': len(db_rows),
        }

    summaries: List[Dict[str, Any]] = []
    tzinfo = start.tzinfo or end.tzinfo
    if tzinfo is None:
        tzinfo = datetime.now().astimezone().tzinfo or timezone.utc

    def _week_ranges(start_dt: datetime, end_dt: datetime) -> List[tuple[datetime, datetime]]:
        """Chia đoạn thời gian thành các tuần (7 ngày) để giảm tải RAM."""
        ranges: List[tuple[datetime, datetime]] = []
        cur = start_dt
        while cur < end_dt:
            seg_end = cur + timedelta(days=7)
            if seg_end > end_dt:
                seg_end = end_dt
            ranges.append((cur, seg_end))
            cur = seg_end
        return ranges

    # Bảo đảm start/end có tz
    start = start if start.tzinfo else start.replace(tzinfo=tzinfo)
    end = end if end.tzinfo else end.replace(tzinfo=tzinfo)

    # Chạy song song nhiều tuần nhưng giới hạn concurrency để tránh quá tải RAM/DB
    week_ranges = _week_ranges(start, end)
    max_concurrency = 1
    sem = asyncio.Semaphore(max_concurrency)

    async def _run_segment(range_start: datetime, range_end: datetime) -> Dict[str, Any]:
        if cancel_event and cancel_event.is_set():
            raise asyncio.CancelledError("Backtest bị hủy")
        async with sem:
            warmup_start = range_start - timedelta(days=7)
            if warmup_start < start:
                warmup_start = start
            deleted = await storage.delete_backtest_trades_in_range(
                symbol,
                range_start,
                range_end,
                saved_backtest_id=saved_config_id,
            )
            print(
                f"🧹 Đã xoá {deleted} dòng backtest cũ cho khoảng {range_start.date()} -> {(range_end - timedelta(days=1)).date()}"
                if deleted
                else f"✨ Không có backtest cũ trong khoảng {range_start.date()} -> {(range_end - timedelta(days=1)).date()}"
            )
            summary_seg = await _run_range(warmup_start, range_end, range_start)
            summary_seg['deleted_rows'] = deleted or 0
            return summary_seg

    segment_tasks = [asyncio.create_task(_run_segment(r_start, r_end)) for r_start, r_end in week_ranges]
    summaries = await asyncio.gather(*segment_tasks)
    summaries.sort(key=lambda x: x.get('range_start') or datetime.now(timezone.utc))

    combined = _aggregate_summaries(summaries)

    if not return_summary:
        for idx, summary in enumerate(summaries, start=1):
            span = f"{summary['range_start'].date()} -> {(summary['range_end'] - timedelta(days=1)).date()}"
            print(f"\nTổng kết giai đoạn {span} (#{idx})")
            print('--------------------------------')
            print(f"Số lệnh: {summary['total_trades']}, Thắng: {summary['wins']}, Thua: {summary['losses']}")
            print(f"Tổng PnL (giá): {summary['total_pnl']:.6f}, Trung bình: {summary['avg_pnl']:.6f}")
            if size_from_risk:
                print(f"Tổng PnL (USD): {summary['total_usd_pnl']:.2f}")
            else:
                print(f"Tổng PnL (USD): {summary['total_usd_pnl']:.2f} (volume cố định {volume})")
        if len(summaries) > 1:
            print('\n=== Tổng kết toàn bộ khoảng thời gian ===')
            print(f"Số ngày: {len(summaries)}, Tổng lệnh: {combined['total_trades']}")
            print(f"Thắng: {combined['wins']} | Thua: {combined['losses']}")
            print(f"Tổng PnL (giá): {combined['total_pnl']:.6f}, Trung bình: {combined['avg_pnl']:.6f}")
            print(f"Tổng PnL (USD): {combined['total_usd_pnl']:.2f}, Trung bình: {combined['avg_usd_pnl']:.2f}")

    await storage.close()
    if return_summary:
        combined['period_breakdown'] = summaries
        combined['daily_breakdown'] = summaries  # giữ tương thích
        return combined

def _to_datetime(value) -> datetime:
    local_tz = datetime.now().astimezone().tzinfo or timezone.utc
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=local_tz)
    if isinstance(value, pd.Timestamp):
        dt = value.to_pydatetime()
        return dt if dt.tzinfo else dt.replace(tzinfo=local_tz)
    if isinstance(value, str):
        dt = datetime.fromisoformat(value)
        return dt if dt.tzinfo else dt.replace(tzinfo=local_tz)
    raise TypeError(f"Không thể chuyển '{value}' sang datetime")


def _ensure_aware(dt: datetime) -> datetime:
    if dt.tzinfo:
        return dt
    local_tz = datetime.now().astimezone().tzinfo or timezone.utc
    return dt.replace(tzinfo=local_tz)


def _aggregate_summaries(items: List[Dict[str, Any]]) -> Dict[str, Any]:
    if not items:
        return {
            'total_trades': 0,
            'wins': 0,
            'losses': 0,
            'total_pnl': 0.0,
            'avg_pnl': 0.0,
            'total_usd_pnl': 0.0,
            'avg_usd_pnl': 0.0,
        }

    total_trades = sum(s['total_trades'] for s in items)
    wins = sum(s['wins'] for s in items)
    losses = sum(s['losses'] for s in items)
    total_pnl = sum(s['total_pnl'] for s in items)
    total_usd_pnl = sum(s['total_usd_pnl'] for s in items)
    avg_pnl = total_pnl / total_trades if total_trades else 0.0
    avg_usd_pnl = total_usd_pnl / total_trades if total_trades else 0.0
    return {
        'total_trades': total_trades,
        'wins': wins,
        'losses': losses,
        'total_pnl': total_pnl,
        'avg_pnl': avg_pnl,
        'total_usd_pnl': total_usd_pnl,
        'avg_usd_pnl': avg_usd_pnl,
    }
