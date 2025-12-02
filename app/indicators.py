"""Technical indicators calculation module.

This module provides functions for calculating various technical indicators
using tick data from the database.
"""
from typing import List, Optional
import numpy as np
import pandas as pd
from sqlalchemy import select, and_
from datetime import datetime, timezone

from .storage import Storage, ticks_table


async def get_tick_data(storage: Storage,
                       symbol: str,
                       start_time: datetime,
                       end_time: datetime) -> pd.DataFrame:
    """Get tick data from database and convert to DataFrame."""
    query = select(ticks_table).where(
        and_(
            ticks_table.c.symbol == symbol,
            ticks_table.c.time_msc >= int(start_time.timestamp() * 1000),
            ticks_table.c.time_msc <= int(end_time.timestamp() * 1000)
        )
    ).order_by(ticks_table.c.time_msc)

    await storage.ensure_initialized()
    async with storage.engine.connect() as conn:
        result = await conn.execute(query)
        rows = result.fetchall()

    # Convert to DataFrame
    df = pd.DataFrame(rows, columns=ticks_table.columns.keys())
    if df.empty:
        return df
    df['datetime'] = pd.to_datetime(df['datetime'], utc=True, errors='coerce')
    df = df.dropna(subset=['datetime'])
    return df


def resample_ticks(df: pd.DataFrame, timeframe: str = '1min') -> pd.DataFrame:
    """Resample tick data to OHLCV format.
    
    Args:
        df: Tick data DataFrame with datetime and bid/ask columns
        timeframe: Pandas resample rule (e.g. '1min', '5min', '1H')
    
    Returns:
        DataFrame with OHLCV data
    """
    if df.empty:
        return pd.DataFrame(columns=['datetime', 'open', 'high', 'low', 'close', 'bid', 'ask'])
    timeframe = timeframe.lower()  # pandas cảnh báo 'H' viết hoa sẽ bị loại bỏ
    # Use mid price for OHLC
    df['price'] = (df['bid'] + df['ask']) / 2
    df_resampled = df.set_index('datetime').resample(timeframe).agg({
        'price': ['first', 'max', 'min', 'last'],
        'bid': 'last',
        'ask': 'last',
    })

    # Flatten column names
    df_resampled.columns = ['open', 'high', 'low', 'close', 'bid', 'ask']
    return df_resampled.reset_index()


def moving_average(data: pd.Series, window: int, ma_type: str = 'sma') -> pd.Series:
    """Calculate moving average.
    
    Args:
        data: Price series to calculate MA
        window: MA period
        ma_type: Type of MA - 'sma' or 'ema'
    
    Returns:
        Series with MA values
    """
    if ma_type == 'sma':
        return data.rolling(window=window, min_periods=1).mean()
    elif ma_type == 'ema':
        return data.ewm(span=window, adjust=False).mean()
    else:
        raise ValueError(f"Unknown MA type: {ma_type}")


def atr(df: pd.DataFrame, window: int = 14) -> pd.Series:
    """Calculate Average True Range.
    
    Args:
        df: DataFrame with high, low, close columns
        window: ATR period
        
    Returns:
        Series with ATR values
    """
    tr1 = df['high'] - df['low']
    tr2 = abs(df['high'] - df['close'].shift())
    tr3 = abs(df['low'] - df['close'].shift())
    
    tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
    return tr.rolling(window=window).mean()


async def get_ma_series(
    storage: Storage,
    symbol: str,
    start_time: datetime,
    end_time: datetime,
    timeframe: str = '1min',
    window: int = 20,
    ma_type: str = 'sma',
    atr_window: Optional[int] = None,
    ignore_gaps: bool = False,
    closed_sessions: Optional[List[str]] = None,
) -> pd.DataFrame:
    """Get moving average series for a symbol.
    
    Args:
        storage: Database connection
        symbol: Trading symbol
        start_time: Start time for data
        end_time: End time for data
        timeframe: Chart timeframe
        window: MA period
        ma_type: Type of MA
        
    Returns:
        DataFrame with datetime, price and MA columns
    """
    # Get tick data
    df_ticks = await get_tick_data(storage, symbol, start_time, end_time)
    
    # Resample to candles
    df = resample_ticks(df_ticks, timeframe)
    if ignore_gaps and not df.empty:
        tf = timeframe.lower()
        full_index = pd.date_range(start=df['datetime'].min(), end=df['datetime'].max(), freq=tf)
        df = (
            df.set_index('datetime')
            .reindex(full_index)
            .ffill()
            .reset_index()
            .rename(columns={'index': 'datetime'})
        )
    df = _filter_closed_sessions(df, closed_sessions or [])
    # Calculate indicators
    df['ma'] = moving_average(df['close'], window, ma_type)
    atr_period = atr_window or window
    df['atr'] = atr(df[['high', 'low', 'close']], atr_period)
    
    return df[['datetime', 'open', 'high', 'low', 'close', 'ma', 'atr']]


def _filter_closed_sessions(df: pd.DataFrame, closed_sessions: List[str]) -> pd.DataFrame:
    if df.empty:
        return df
    df = df.copy()
    df['datetime'] = pd.to_datetime(df['datetime'], utc=True, errors='coerce')
    df = df.dropna(subset=['datetime'])
    df = df[df['datetime'].dt.weekday < 5]
    if not closed_sessions:
        return df
    minutes = df['datetime'].dt.hour * 60 + df['datetime'].dt.minute
    closed_mask = pd.Series(False, index=df.index)
    for session in closed_sessions:
        try:
            start_str, end_str = session.split('-', 1)
            start_min = _to_minutes(start_str.strip())
            end_min = _to_minutes(end_str.strip())
        except ValueError:
            continue
        if start_min is None or end_min is None:
            continue
        if end_min < start_min:
            mask = (minutes >= start_min) | (minutes <= end_min)
        else:
            mask = (minutes >= start_min) & (minutes <= end_min)
        closed_mask |= mask
    return df[~closed_mask]


def _to_minutes(val: str) -> Optional[int]:
    try:
        parts = val.split(":")
        h = int(parts[0])
        m = int(parts[1]) if len(parts) > 1 else 0
        return h * 60 + m
    except Exception:
        return None
