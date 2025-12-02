# -*- coding: utf-8 -*-
"""Backtest chiến lược MA Crossover sử dụng dữ liệu tick trong CSDL."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Optional, List, Dict

import pandas as pd
from uuid import uuid4
import logging

from app.storage import Storage
from app.Breakout_Strategy import MAConfig, MACrossoverStrategy

logger = logging.getLogger(__name__)


async def run_backtest(
    *,
    db_url: str,
    symbol: str,
    start_str: str,
    end_str: str,
    timeframe: str,
    donchian_period: int,
    ema_trend_period: int,
    atr_period: int,
    entry_buffer_points: float,
    breakeven_after_rr: Optional[float],
    exit_on_opposite: bool,
    capital: float,
    risk_pct: float,
    size_from_risk: bool,
    contract_size: float,
    pip_size: float,
    volume: float,
    min_volume: float,
    volume_step: float,
    max_positions: int,
    sl_atr: float,
    tp_atr: float,
    trail_trigger_atr: float,
    trail_atr_mult: float,
    breakeven_atr: float,
    partial_close: bool,
    partial_close_atr: float,
    trading_hours: Optional[str],
    min_atr_multiplier: float,
    max_atr_multiplier: float,
    max_spread_points: float,
    allowed_deviation_points: float,
    slippage_points: float,
    closed_sessions: Optional[str],
    max_daily_loss: Optional[float],
    max_loss_streak: Optional[int],
    max_losses_per_session: Optional[int],
    cooldown_minutes: Optional[int],
    ignore_gaps: bool,
    session_cooldown_minutes: int,
    return_summary: bool = False,
) -> None:
    start = _parse_iso_utc(start_str)
    end = _parse_iso_utc(end_str)

    storage = Storage(db_url)
    await storage.init()

    config = MAConfig()
    config.symbol = symbol
    config.timeframe = timeframe
    config.donchian_period = donchian_period
    config.ema_trend_period = ema_trend_period
    config.atr_period = atr_period
    config.entry_buffer_points = entry_buffer_points
    config.breakeven_after_rr = breakeven_after_rr
    config.exit_on_opposite = exit_on_opposite
    config.capital = capital
    config.risk_pct = risk_pct
    config.size_from_risk = size_from_risk
    config.contract_size = contract_size
    config.pip_size = pip_size
    config.volume = volume
    config.min_volume = min_volume
    config.volume_step = volume_step
    config.max_positions = max_positions
    config.sl_atr = sl_atr
    config.tp_atr = tp_atr
    config.trail_trigger_atr = trail_trigger_atr
    config.trail_atr_mult = trail_atr_mult
    config.breakeven_atr = breakeven_atr
    config.partial_close = partial_close
    config.partial_close_atr = partial_close_atr
    config.min_atr_multiplier = min_atr_multiplier
    config.max_atr_multiplier = max_atr_multiplier
    config.max_spread_points = max_spread_points
    config.ignore_gaps = ignore_gaps
    config.allowed_deviation_points = allowed_deviation_points
    config.slippage_points = slippage_points
    config.trading_hours = [h.strip() for h in trading_hours.split(',')] if trading_hours else None
    config.closed_sessions = [h.strip() for h in closed_sessions.split(',')] if closed_sessions else None
    config.max_daily_loss = max_daily_loss
    config.max_loss_streak = max_loss_streak
    config.max_losses_per_session = max_losses_per_session
    config.cooldown_minutes = cooldown_minutes
    config.session_cooldown_minutes = session_cooldown_minutes
    config.paper_mode = True

    strategy = MACrossoverStrategy(config, None, storage)

    df = await strategy.calculate_signals(storage, start, end)
    if 'action' in df.columns:
        try:
            df['action'] = df['action'].fillna(0).astype(int)
        except Exception:
            df['action'] = df['action'].fillna(0).apply(lambda x: int(x) if pd.notna(x) else 0)
    else:
        df['action'] = 0

    required = ['datetime', 'open', 'high', 'low', 'close', 'atr', 'donchian_high', 'donchian_low']
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

            sl = entry_price - sl_atr * atr_val
            tp = entry_price + tp_atr * atr_val

            trade_volume = volume
            if size_from_risk:
                stop_dist = max(entry_price - sl, 0.0)
                risk_fraction = risk_pct if risk_pct <= 1 else risk_pct / 100.0
                risk_amount = max(0.0, capital * risk_fraction)
                if stop_dist > 0 and risk_amount > 0 and contract_size > 0:
                    trade_volume = risk_amount / (stop_dist * contract_size)
                    trade_volume = max(round(trade_volume, 2), 0.01)
            step = volume_step if volume_step > 0 else 0.01
            min_vol = min_volume if min_volume > 0 else step
            trade_volume = max(min_vol, round(trade_volume / step) * step)

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
            sl = entry_price + sl_atr * atr_val
            tp = entry_price - tp_atr * atr_val

            trade_volume = volume
            if size_from_risk:
                stop_dist = max(sl - entry_price, 0.0)
                risk_fraction = risk_pct if risk_pct <= 1 else risk_pct / 100.0
                risk_amount = max(0.0, capital * risk_fraction)
                if stop_dist > 0 and risk_amount > 0 and contract_size > 0:
                    trade_volume = risk_amount / (stop_dist * contract_size)
                    trade_volume = max(round(trade_volume, 2), 0.01)
            step = volume_step if volume_step > 0 else 0.01
            min_vol = min_volume if min_volume > 0 else step
            trade_volume = max(min_vol, round(trade_volume / step) * step)

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
    try:
        await storage.insert_backtest_trades(db_rows)
    except Exception as exc:
        logger.warning("Bỏ qua lưu backtest (table không khả dụng): %s", exc)

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
        logger.info('Tổng kết Backtest')
        logger.info('--------------------------------')
        logger.info('Số lệnh: %s, Thắng: %s, Thua: %s', total, wins, losses)
        logger.info('Tổng PnL (đơn vị giá): %.6f, Trung bình/lệnh: %.6f', total_pnl, avg_pnl)
        if size_from_risk:
            logger.info('Tổng PnL (USD): %.2f (khối lượng theo rủi ro)', total_usd_pnl)
        else:
            logger.info('Tổng PnL (USD): %.2f với volume=%s, contract_size=%s', total_usd_pnl, volume, contract_size)
        logger.info('Trung bình/lệnh (USD): %.2f', avg_usd_pnl)
        logger.info('Đã lưu %d lệnh vào bảng backtest_trades (run_id=%s)', len(db_rows), run_id)

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


def _parse_iso_utc(value: str) -> datetime:
    """Parse ISO string, allow trailing Z, always return timezone-aware UTC."""
    cleaned = value.strip()
    if cleaned.endswith("Z"):
        cleaned = cleaned[:-1] + "+00:00"
    dt = datetime.fromisoformat(cleaned)
    return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
