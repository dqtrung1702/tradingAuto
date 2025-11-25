"""Technical indicators calculation module.

This module provides functions for calculating various technical indicators
using tick data from the database.
"""
from typing import List, Optional
import re
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
    df['datetime'] = pd.to_datetime(df['datetime'], format='ISO8601')
    return df


def _normalize_timeframe(timeframe: str) -> str:
    """Convert common aliases to pandas-friendly frequencies (avoids warnings)."""
    tf = str(timeframe).strip()
    if not tf:
        return tf

    match = re.fullmatch(r"(\d+)([A-Za-z]+)", tf)
    if match:
        value, unit = match.groups()
        unit_lower = unit.lower()
        if unit_lower in {'t', 'min'}:
            return f"{value}min"
        if unit_lower == 'm':
            return f"{value}ME" if unit.isupper() else f"{value}min"
        if unit_lower == 'h':
            return f"{value}h"
        if unit_lower == 'd':
            return f"{value}d"
        if unit_lower in {'me', 'ms'}:
            return f"{value}{unit.upper()}"
        return f"{value}{unit_lower}"

    if tf.lower() == 'm':
        return 'ME' if tf.isupper() else 'min'
    return tf


def resample_ticks(df: pd.DataFrame, timeframe: str = '1min') -> pd.DataFrame:
    """Resample tick data to OHLCV format.
    
    Args:
        df: Tick data DataFrame with datetime and bid/ask columns
        timeframe: Pandas resample rule (e.g. '1min', '5min', '1H')
    
    Returns:
        DataFrame with OHLCV data
    """
    tf = _normalize_timeframe(timeframe)
    # Use mid price for OHLC
    df['price'] = (df['bid'] + df['ask']) / 2
    df_resampled = df.set_index('datetime').resample(tf).agg({
        'price': ['first', 'max', 'min', 'last', 'count'],
        'time_msc': 'last',  # keep last timestamp
        'bid': 'last',  # keep last bid/ask for reference
        'ask': 'last',
    })
    
    # Flatten column names
    df_resampled.columns = ['open', 'high', 'low', 'close', 'tick_volume', 'timestamp', 'bid', 'ask']
    
    # Calculate mid price and spread
    df_resampled['spread'] = df_resampled['ask'] - df_resampled['bid']
    
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
    # Calculate indicators
    df['ma'] = moving_average(df['close'], window, ma_type)
    atr_period = atr_window or window
    df['atr'] = atr(df[['high', 'low', 'close']], atr_period)
    
    return df[['datetime', 'open', 'high', 'low', 'close', 'ma', 'atr', 'spread']]
