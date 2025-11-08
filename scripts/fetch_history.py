"""Fetch ticks from a local MetaTrader5 terminal and store them into Postgres.

Usage:
  py scripts\fetch_history.py --symbol XAUUSDc --start 2025-01-01 --end 2025-02-01 --db-url postgresql+asyncpg://trader:admin@localhost:5432/mt5

Notes:
 - MT5 terminal must be running and logged in.
 - The script fetches all ticks in the time range and inserts into DB in batches.
"""
from __future__ import annotations

import argparse
from datetime import datetime, timezone, timedelta
from typing import List

try:
    import MetaTrader5 as mt5
except Exception:
    mt5 = None

# Make sure the project root is on sys.path so we can import the local `app` package
# This helps when running the script as: `py scripts/fetch_history.py ...`
import sys
from pathlib import Path
_PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from app.storage import Storage
from app.strategy import Tick
import time
from decimal import Decimal


def parse_mt5_tick(raw_tick) -> Tick:
    """Parse một MT5 tick tuple thành Tick object."""
    # MT5 returns a tuple with fixed order: (time, bid, ask, last, volume, time_msc, flags, volume_real)
    fields = ['time', 'bid', 'ask', 'last', 'volume', 'time_msc', 'flags', 'volume_real']
    values = dict(zip(fields, raw_tick))
    
    try:
        time_msc = int(values['time_msc']) if values['time_msc'] else int(values['time'] * 1000)
        bid = float(values['bid'])
        ask = float(values['ask'])
        last = float(values['last']) if values['last'] else None
        
        if bid <= 0 or ask <= 0:
            raise ValueError(f"Invalid bid/ask prices: {bid}/{ask}")
            
        return Tick(time_msc=time_msc, bid=bid, ask=ask, last=last)
        
    except (TypeError, ValueError) as e:
        print(f"Error parsing tick: {e}")
        print("Raw tick data for debugging:")
        print(raw_tick)
        raise  # Re-raise to see where bad ticks are coming from


def list_available_symbols() -> None:
    """In danh sách các symbols có sẵn trong Market Watch và thông tin của chúng."""
    if not mt5.initialize():
        raise RuntimeError('MT5 initialize failed')
    try:
        symbols = mt5.symbols_get()
        if symbols is None:
            print('Không lấy được danh sách symbols')
            return
        print(f'Có {len(symbols)} symbols:')
        for s in symbols:
            print(f'- {s.name}: visible={s.visible}, select={s.select}, '
                  f'trade_mode={s.trade_mode}, bid={s.bid}, ask={s.ask}')
    finally:
        mt5.shutdown()


def fetch_ticks_mt5(symbol: str, start: datetime, end: datetime, 
                   max_days_per_call: int = 1,
                   max_retries: int = 3, 
                   retry_delay: float = 1.0) -> List[Tick]:
    if mt5 is None:
        raise RuntimeError('MetaTrader5 not available')
    if not mt5.initialize():
        raise RuntimeError('MT5 initialize failed')
    # Ensure symbol is selected in MarketWatch; many brokers require symbol to be visible
    try:
        sel = mt5.symbol_select(symbol, True)
    except Exception:
        sel = False
    if not sel:
        err = mt5.last_error()
        # include available similar symbols to help the user (if any)
        raise RuntimeError(f'Không thể chọn symbol {symbol}: {err}')

    # Print symbol metadata for debugging
    si = mt5.symbol_info(symbol)
    sit = mt5.symbol_info_tick(symbol)
    print('MT5 symbol_info:', si)
    print('MT5 symbol_info_tick:', sit)

    # Validate time range
    now = datetime.now(timezone.utc)
    if start > now or end > now:
        print(f'WARNING: Thời gian yêu cầu ({start} -> {end}) vượt quá thời điểm hiện tại ({now})')
        print('Đang điều chỉnh để lấy dữ liệu từ 7 ngày qua...')
        end = now
        start = end - timedelta(days=7)

    # Split into smaller windows and fetch with retries
    all_ticks = []
    window_start = start
    while window_start < end:
        window_end = min(window_start + timedelta(days=max_days_per_call), end)
        utc_from = int(window_start.replace(tzinfo=timezone.utc).timestamp())
        utc_to = int(window_end.replace(tzinfo=timezone.utc).timestamp())
        
        print(f'Đang lấy ticks từ {window_start} đến {window_end}...')
        
        # Try with retries
        raw = None
        last_error = None
        for attempt in range(max_retries):
            raw = mt5.copy_ticks_range(symbol, utc_from, utc_to, mt5.COPY_TICKS_ALL)
            if raw is not None and len(raw) > 0:
                break
            last_error = mt5.last_error()
            if attempt < max_retries - 1:
                print(f'Lần thử {attempt + 1}/{max_retries} thất bại: {last_error}')
                print(f'Thử lại sau {retry_delay}s...')
                time.sleep(retry_delay)
        
        if raw is None or len(raw) == 0:
            print(f'Không lấy được dữ liệu cho {window_start} -> {window_end}: {last_error}')
        else:
            window_ticks = []
            for r in raw:
                try:
                    tick = parse_mt5_tick(r)
                    window_ticks.append(tick)
                except (ValueError, TypeError):
                    continue
            print(f'Parsed {len(window_ticks)} ticks')
            all_ticks.extend(window_ticks)
        
        window_start = window_end
    
    if not all_ticks:
        # If we got nothing, try last hour as fallback
        print('Thử lấy 1 giờ dữ liệu gần nhất...')
        end = now
        start = end - timedelta(hours=1)
        utc_from = int(start.replace(tzinfo=timezone.utc).timestamp())
        utc_to = int(end.replace(tzinfo=timezone.utc).timestamp())
        raw = mt5.copy_ticks_range(symbol, utc_from, utc_to, mt5.COPY_TICKS_ALL)
        if raw is not None:
            for r in raw:
                try:
                    tick = parse_mt5_tick(r)
                    all_ticks.append(tick)
                except (ValueError, TypeError):
                    continue
    
    mt5.shutdown()
    return all_ticks


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument('--symbol', required=True)
    p.add_argument('--start', required=True)
    p.add_argument('--end', required=True)
    p.add_argument('--db-url', required=True)
    p.add_argument('--batch', type=int, default=2000)
    p.add_argument('--list-symbols', action='store_true', help='Chỉ in danh sách symbols có sẵn')
    p.add_argument('--max-days', type=int, default=1, 
                  help='Số ngày tối đa cho mỗi lần gọi copy_ticks_range')
    args = p.parse_args()

    if args.list_symbols:
        list_available_symbols()
        return

    # Parse dates and ensure they're UTC
    start = datetime.fromisoformat(args.start).replace(tzinfo=timezone.utc)
    end = datetime.fromisoformat(args.end).replace(tzinfo=timezone.utc)
    print('Fetching ticks from MT5...')
    ticks = fetch_ticks_mt5(args.symbol, start, end, max_days_per_call=args.max_days)
    print(f'Fetched {len(ticks)} ticks total')

    # prepare rows for DB insert with UTC+7 timezone
    utc7 = timezone(timedelta(hours=7))
    rows = []
    for t in ticks:
        # Convert from UTC to UTC+7
        dt_utc = datetime.fromtimestamp(t.time_msc / 1000.0, tz=timezone.utc)
        dt_vn = dt_utc.astimezone(utc7)
        
        row = {
            'symbol': args.symbol,
            'time_msc': t.time_msc,
            'datetime': dt_vn.isoformat(),
            'bid': float(t.bid) if t.bid is not None else None,
            'ask': float(t.ask) if t.ask is not None else None,
            'last': float(t.last) if t.last is not None else None
        }
        rows.append(row)

    import asyncio

    async def async_write():
        try:
            print(f"Inserting {len(rows)} ticks into database...")
            storage = Storage(db_url=args.db_url)
            await storage.init()
            await storage.insert_ticks_batch(rows, batch_size=args.batch)
            print("Insert completed")
        except Exception as e:
            print(f"Database error: {e}")
            raise
        finally:
            await storage.close()

    asyncio.run(async_write())


if __name__ == '__main__':
    main()
