"""Backtest ticks using TickMovingAverageStrategy and PaperExecutionSimulator.

Usage (PowerShell):
    py scripts\backtest.py --symbol XAUUSDc --start 2025-01-01 --end 2025-02-01 --short 10 --long 50

The script will try to fetch ticks via MetaTrader5 if available; otherwise you can pass --csv path.
"""
from __future__ import annotations

import argparse
import csv
import os
from datetime import datetime, timezone
from typing import List

try:
    import MetaTrader5 as mt5
except Exception:
    mt5 = None

# Ensure project root is on sys.path so `app` package imports work when running the script
import sys
from pathlib import Path
_PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from app.strategy import PaperExecutionSimulator, Tick, TickMovingAverageStrategy
from app.storage import Storage
import asyncio


def load_ticks_from_csv(path: str) -> List[Tick]:
    ticks = []
    with open(path, newline='') as fh:
        reader = csv.DictReader(fh)
        for r in reader:
            time_msc = int(r.get('time_msc') or r.get('time') or 0)
            bid = float(r.get('bid') or 0)
            ask = float(r.get('ask') or 0)
            last = float(r['last']) if r.get('last') else None
            ticks.append(Tick(time_msc=time_msc, bid=bid, ask=ask, last=last))
    return ticks


def fetch_ticks_mt5(symbol: str, start: datetime, end: datetime) -> List[Tick]:
    if mt5 is None:
        raise RuntimeError('MetaTrader5 not available')
    if not mt5.initialize():
        raise RuntimeError('MT5 initialize failed')
    # mt5.copy_ticks_range expects unix timestamp in seconds; we use time_msc conversion
    utc_from = int(start.replace(tzinfo=timezone.utc).timestamp())
    utc_to = int(end.replace(tzinfo=timezone.utc).timestamp())
    raw = mt5.copy_ticks_range(symbol, utc_from, utc_to, mt5.COPY_TICKS_ALL)
    ticks = []
    for r in raw:
        # r.time_msc, r.bid, r.ask, getattr(r, 'last', None)
        ticks.append(Tick(time_msc=int(getattr(r, 'time_msc', getattr(r, 'time', 0)) or 0),
                          bid=float(getattr(r, 'bid', 0)),
                          ask=float(getattr(r, 'ask', 0)),
                          last=getattr(r, 'last', None)))
    mt5.shutdown()
    return ticks


def ms_to_dt(ms: int) -> datetime:
    return datetime.fromtimestamp(ms / 1000.0, tz=timezone.utc)


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument('--symbol', required=True)
    p.add_argument('--start', required=False)
    p.add_argument('--end', required=False)
    p.add_argument('--csv', required=False)
    p.add_argument('--db-url', required=False, help='Postgres DATABASE_URL, e.g. postgresql+asyncpg://user:pass@host:5432/db')
    p.add_argument('--short', type=int, default=10)
    p.add_argument('--long', type=int, default=50)
    args = p.parse_args()

    if args.csv:
        ticks = load_ticks_from_csv(args.csv)
    else:
        if not args.start or not args.end:
            raise SystemExit('When using MT5 backend, --start and --end are required')
        start = datetime.fromisoformat(args.start)
        end = datetime.fromisoformat(args.end)
        ticks = fetch_ticks_mt5(args.symbol, start, end)

    print(f'Loaded {len(ticks)} ticks')

    # DB URL: prefer --db-url, else env DATABASE_URL
    db_url = args.db_url or os.getenv('DATABASE_URL')

    strat = TickMovingAverageStrategy(short_window=args.short, long_window=args.long)
    sim = PaperExecutionSimulator()

    async def async_main() -> None:
        storage = None
        if db_url:
            print('Initializing DB...')
            storage = Storage(db_url=db_url)
            await storage.init()

        batch = []
        batch_size = 1000

        for t in ticks:
            # prepare tick row
            row = {
                'symbol': args.symbol,
                'time_msc': t.time_msc,
                'bid': t.bid,
                'ask': t.ask,
                'last': t.last,
            }
            if storage:
                batch.append(row)
                if len(batch) >= batch_size:
                    try:
                        await storage.insert_ticks_batch(batch, batch_size=batch_size)
                    except Exception as exc:
                        print('Batch insert failed:', exc)
                    batch = []

            signal = strat.handle_tick(t)
            if signal:
                sim.execute(signal, t)
                if signal.get('action') == 'CLOSE' and storage:
                    # write trade record
                    last_trade = sim.trades[-1]
                    pnl = last_trade.get('pnl')
                    try:
                        await storage.insert_trade(side='CLOSE', price=signal.get('price'), time_msc=t.time_msc, pnl=pnl, meta=None)
                    except Exception as exc:
                        print('Insert trade failed:', exc)

        # flush remaining batch
        if storage and batch:
            try:
                await storage.insert_ticks_batch(batch, batch_size=batch_size)
            except Exception as exc:
                print('Final batch insert failed:', exc)

        if storage:
            await storage.close()

    asyncio.run(async_main())

    # Summary
    pnl = sim.balance - 10000.0
    wins = sum(1 for tr in sim.trades if tr.get('side') == 'CLOSE' and tr.get('pnl', 0) > 0)
    closes = sum(1 for tr in sim.trades if tr.get('side') == 'CLOSE')
    print('Final balance:', sim.balance)
    print('Total PnL:', pnl)
    print('Total trades (closes):', closes)
    if closes:
        print('Win rate:', wins / closes)


if __name__ == '__main__':
    main()
