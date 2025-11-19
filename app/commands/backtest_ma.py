# -*- coding: utf-8 -*-
"""Backtest chiến lược MA Crossover sử dụng dữ liệu tick trong CSDL."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Optional, List, Dict

import pandas as pd
from uuid import uuid4

from app.storage import Storage
from app.strategy_presets import resolve_preset
from app.strategies.ma_crossover import MAConfig, MACrossoverStrategy


async def run_backtest(
    *,
    db_url: str,
    symbol: str,
    preset: Optional[str] = None,
    start_str: str,
    end_str: str,
    fast: int,
    slow: int,
    timeframe: str,
    ma_type: str,
    trend: int,
    risk_pct: float,
    capital: float,
    trail_trigger_atr: float,
    trail_atr_mult: float,
    spread_atr_max: float,
    reverse_exit: bool,
    market_state_window: int,
    sl_atr: float,
    tp_atr: float,
    volume: float,
    contract_size: float,
    sl_pips: Optional[float],
    tp_pips: Optional[float],
    pip_size: float,
    size_from_risk: bool,
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
    return_summary: bool = False,
) -> None:
    start = datetime.fromisoformat(start_str)
    end = datetime.fromisoformat(end_str)

    storage = Storage(db_url)
    await storage.init()

    preset_cfg = resolve_preset(preset)

    config = MAConfig()
    config.symbol = symbol
    config.fast_ma = preset_cfg.fast_ma if preset_cfg else fast
    config.slow_ma = preset_cfg.slow_ma if preset_cfg else slow
    config.timeframe = preset_cfg.timeframe if preset_cfg else timeframe
    config.ma_type = preset_cfg.ma_type if preset_cfg else ma_type
    config.paper_mode = True

    config.trend_ma = trend
    config.risk_pct = risk_pct
    config.capital = capital
    config.trail_trigger_atr = trail_trigger_atr
    config.trail_atr_mult = trail_atr_mult
    config.spread_atr_max = preset_cfg.spread_atr_max if preset_cfg else spread_atr_max
    config.reverse_exit = bool(reverse_exit)
    config.market_state_window = market_state_window
    config.sl_atr = preset_cfg.sl_atr if preset_cfg else sl_atr
    config.tp_atr = preset_cfg.tp_atr if preset_cfg else tp_atr
    config.volume = volume
    config.contract_size = contract_size
    config.size_from_risk = size_from_risk
    config.momentum_type = preset_cfg.momentum_type if preset_cfg else momentum_type
    config.momentum_window = preset_cfg.momentum_window if preset_cfg else momentum_window
    config.momentum_threshold = preset_cfg.momentum_threshold if preset_cfg else momentum_threshold
    config.macd_fast = preset_cfg.macd_fast if preset_cfg else macd_fast
    config.macd_slow = preset_cfg.macd_slow if preset_cfg else macd_slow
    config.macd_signal = preset_cfg.macd_signal if preset_cfg else macd_signal
    config.macd_threshold = preset_cfg.macd_threshold if preset_cfg else macd_threshold
    config.range_lookback = preset_cfg.range_lookback if preset_cfg else range_lookback
    config.range_min_atr = preset_cfg.range_min_atr if preset_cfg else range_min_atr
    config.range_min_points = preset_cfg.range_min_points if preset_cfg else range_min_points
    config.breakout_buffer_atr = preset_cfg.breakout_buffer_atr if preset_cfg else breakout_buffer_atr
    config.breakout_confirmation_bars = (
        preset_cfg.breakout_confirmation_bars if preset_cfg else breakout_confirmation_bars
    )
    config.atr_baseline_window = preset_cfg.atr_baseline_window if preset_cfg else atr_baseline_window
    config.atr_multiplier_min = preset_cfg.atr_multiplier_min if preset_cfg else atr_multiplier_min
    config.atr_multiplier_max = preset_cfg.atr_multiplier_max if preset_cfg else atr_multiplier_max
    if trading_hours:
        config.trading_hours = [h.strip() for h in trading_hours.split(',')]
    elif preset_cfg and preset_cfg.trading_hours:
        config.trading_hours = [h.strip() for h in preset_cfg.trading_hours.split(',')]
    else:
        config.trading_hours = None
    config.adx_window = preset_cfg.adx_window if preset_cfg else adx_window
    config.adx_threshold = preset_cfg.adx_threshold if preset_cfg else adx_threshold
    config.rsi_threshold_long = (
        preset_cfg.rsi_threshold_long if (preset_cfg and preset_cfg.rsi_threshold_long is not None) else rsi_threshold_long
    )
    config.rsi_threshold_short = (
        preset_cfg.rsi_threshold_short if (preset_cfg and preset_cfg.rsi_threshold_short is not None) else rsi_threshold_short
    )
    config.max_daily_loss = (
        preset_cfg.max_daily_loss if (preset_cfg and preset_cfg.max_daily_loss is not None) else max_daily_loss
    )
    config.max_consecutive_losses = (
        preset_cfg.max_consecutive_losses
        if (preset_cfg and preset_cfg.max_consecutive_losses is not None)
        else max_loss_streak
    )
    config.max_losses_per_session = (
        preset_cfg.max_losses_per_session
        if (preset_cfg and preset_cfg.max_losses_per_session is not None)
        else max_losses_per_session
    )
    config.cooldown_minutes = (
        preset_cfg.cooldown_minutes if (preset_cfg and preset_cfg.cooldown_minutes is not None) else cooldown_minutes
    )

    try:
        config.sl_pips = sl_pips  # type: ignore[attr-defined]
        config.tp_pips = tp_pips  # type: ignore[attr-defined]
        config.pip_size = pip_size  # type: ignore[attr-defined]
    except Exception:
        pass

    strategy = MACrossoverStrategy(config, None, storage)

    df = await strategy.calculate_signals(storage, start, end)
    if 'action' in df.columns:
        try:
            df['action'] = df['action'].fillna(0).astype(int)
        except Exception:
            df['action'] = df['action'].fillna(0).apply(lambda x: int(x) if pd.notna(x) else 0)
    else:
        df['action'] = 0

    required = ['datetime', 'open', 'high', 'low', 'close', 'fast_ma', 'slow_ma', 'atr']
    for col in required:
        if col not in df.columns:
            raise RuntimeError(f"Thiếu cột bắt buộc: {col}")

    trades: List[Dict] = []

    i = 0
    while i < len(df) - 1:
        row = df.iloc[i]
        action = int(row.get('action', 0))

        if action == 2:
            entry_bar = df.iloc[i + 1]
            entry_price = float(entry_bar['open'])
            atr_val = float(entry_bar['atr'])
            entry_time = _to_datetime(entry_bar['datetime'])

            if sl_pips is not None:
                sl = entry_price - float(sl_pips) * pip_size
            else:
                sl = entry_price - sl_atr * atr_val
            if tp_pips is not None:
                tp = entry_price + float(tp_pips) * pip_size
            else:
                tp = entry_price + tp_atr * atr_val

            trade_volume = volume
            if size_from_risk:
                stop_dist = max(entry_price - sl, 0.0)
                risk_amount = max(0.0, capital * (risk_pct / 100.0))
                if stop_dist > 0 and risk_amount > 0 and contract_size > 0:
                    trade_volume = risk_amount / (stop_dist * contract_size)
                    trade_volume = max(round(trade_volume, 2), 0.01)

            exit_price = None
            exit_time = None
            j = i + 1
            while j < len(df):
                bar = df.iloc[j]
                if float(bar['low']) <= sl:
                    exit_price = sl
                    exit_time = _to_datetime(bar['datetime'])
                    break
                if float(bar['high']) >= tp:
                    exit_price = tp
                    exit_time = _to_datetime(bar['datetime'])
                    break
                if j + 1 < len(df) and int(df.iloc[j]['action']) == -2:
                    exit_bar = df.iloc[j + 1]
                    exit_price = float(exit_bar['open'])
                    exit_time = _to_datetime(exit_bar['datetime'])
                    break
                j += 1
            if exit_price is None:
                exit_price = float(df.iloc[-1]['close'])
                exit_time = _to_datetime(df.iloc[-1]['datetime'])

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
            entry_price = float(entry_bar['open'])
            atr_val = float(entry_bar['atr'])
            entry_time = _to_datetime(entry_bar['datetime'])
            if sl_pips is not None:
                sl = entry_price + float(sl_pips) * pip_size
            else:
                sl = entry_price + sl_atr * atr_val
            if tp_pips is not None:
                tp = entry_price - float(tp_pips) * pip_size
            else:
                tp = entry_price - tp_atr * atr_val

            trade_volume = volume
            if size_from_risk:
                stop_dist = max(sl - entry_price, 0.0)
                risk_amount = max(0.0, capital * (risk_pct / 100.0))
                if stop_dist > 0 and risk_amount > 0 and contract_size > 0:
                    trade_volume = risk_amount / (stop_dist * contract_size)
                    trade_volume = max(round(trade_volume, 2), 0.01)

            exit_price = None
            exit_time = None
            j = i + 1
            while j < len(df):
                bar = df.iloc[j]
                if float(bar['high']) >= sl:
                    exit_price = sl
                    exit_time = _to_datetime(bar['datetime'])
                    break
                if float(bar['low']) <= tp:
                    exit_price = tp
                    exit_time = _to_datetime(bar['datetime'])
                    break
                if j + 1 < len(df) and int(df.iloc[j]['action']) == 2:
                    exit_bar = df.iloc[j + 1]
                    exit_price = float(exit_bar['open'])
                    exit_time = _to_datetime(exit_bar['datetime'])
                    break
                j += 1
            if exit_price is None:
                exit_price = float(df.iloc[-1]['close'])
                exit_time = _to_datetime(df.iloc[-1]['datetime'])

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

    run_id = f"bt_{symbol}_{uuid4().hex[:8]}"
    run_start = _ensure_aware(start)
    run_end = _ensure_aware(end)
    now_utc = datetime.now(timezone.utc)
    db_rows = [
        {
            "run_id": run_id,
            "symbol": symbol,
            "preset": preset,
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
    await storage.insert_backtest_trades(db_rows)

    total = len(trades)
    wins = sum(1 for t in trades if t['pnl'] > 0)
    losses = sum(1 for t in trades if t['pnl'] <= 0)
    total_pnl = sum(float(t['pnl']) for t in trades)
    avg_pnl = total_pnl / total if total else 0.0
    total_usd_pnl = sum(float(t.get('usd_pnl') or 0.0) for t in trades)
    avg_usd_pnl = total_usd_pnl / total if total else 0.0

    summary = {
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

    if not return_summary:
        print('Tổng kết Backtest')
        print('--------------------------------')
        print(f'Số lệnh: {total}, Thắng: {wins}, Thua: {losses}')
        print(f'Tổng PnL (đơn vị giá): {total_pnl:.6f}, Trung bình/lệnh: {avg_pnl:.6f}')
        if size_from_risk:
            print(f'Tổng PnL (USD): {total_usd_pnl:.2f} (khối lượng theo rủi ro)')
        else:
            print(f'Tổng PnL (USD): {total_usd_pnl:.2f} với volume={volume}, contract_size={contract_size}')
        print(f'Trung bình/lệnh (USD): {avg_usd_pnl:.2f}')
        print(f'Đã lưu {len(db_rows)} lệnh vào bảng backtest_trades (run_id={run_id})')

    await storage.close()
    if return_summary:
        return summary


def _to_datetime(value) -> datetime:
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    if isinstance(value, pd.Timestamp):
        dt = value.to_pydatetime()
        return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
    if isinstance(value, str):
        dt = datetime.fromisoformat(value)
        return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
    raise TypeError(f"Không thể chuyển '{value}' sang datetime")


def _ensure_aware(dt: datetime) -> datetime:
    return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
