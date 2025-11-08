"""Simple backtest harness for MA crossover using DB ticks.

Usage:
    py scripts/backtest_ma.py --db-url <DBURL> --symbol XAUUSDc --start 2025-11-05 --end 2025-11-08 --fast 10 --slow 20 --timeframe 5min

Outputs a small CSV of trades and prints summary to console.
"""
import asyncio
import argparse
from datetime import datetime
import os
import csv

from app.storage import Storage
from app.strategies.ma_crossover import MAConfig, MACrossoverStrategy


async def run_backtest(db_url, symbol, start_str, end_str, fast, slow, timeframe):
    # Parse datetimes (expect YYYY-MM-DD)
    start = datetime.fromisoformat(start_str)
    end = datetime.fromisoformat(end_str)

    storage = Storage(db_url)
    await storage.init()

    config = MAConfig()
    config.symbol = symbol
    config.fast_ma = fast
    config.slow_ma = slow
    config.timeframe = timeframe
    config.paper_mode = True

    # strategy instance (quote_service not needed for calculate_signals)
    strategy = MACrossoverStrategy(config, None, storage)

    df = await strategy.calculate_signals(storage, start, end)

    # Ensure required columns
    required = ['datetime', 'open', 'high', 'low', 'close', 'fast_ma', 'slow_ma', 'atr']
    for c in required:
        if c not in df.columns:
            raise RuntimeError(f"Missing column {c} in signal dataframe")

    trades = []

    i = 0
    while i < len(df) - 1:
        row = df.iloc[i]
        action = row['action']
        # bullish entry if action == 2 (from -1 to 1)
        if action == 2:
            entry_bar = df.iloc[i + 1]  # enter at next bar open
            entry_price = entry_bar['open']
            atr = entry_bar['atr']
            sl = entry_price - config.sl_atr * atr
            tp = entry_price + config.tp_atr * atr

            # scan forward for exit
            exit_price = None
            exit_time = None
            j = i + 1
            while j < len(df):
                bar = df.iloc[j]
                # if low <= sl -> stop loss hit
                if bar['low'] <= sl:
                    exit_price = sl
                    exit_time = bar['datetime']
                    break
                # if high >= tp -> take profit
                if bar['high'] >= tp:
                    exit_price = tp
                    exit_time = bar['datetime']
                    break
                # if opposite signal appears, exit at next open
                if j + 1 < len(df) and df.iloc[j]['action'] == -2:
                    exit_bar = df.iloc[j + 1]
                    exit_price = exit_bar['open']
                    exit_time = exit_bar['datetime']
                    break
                j += 1
            if exit_price is None:
                # close at last available close
                exit_price = df.iloc[-1]['close']
                exit_time = df.iloc[-1]['datetime']

            pnl = exit_price - entry_price
            pct = pnl / entry_price if entry_price != 0 else 0
            trades.append({
                'side': 'buy',
                'entry_time': entry_bar['datetime'],
                'entry_price': entry_price,
                'exit_time': exit_time,
                'exit_price': exit_price,
                'pnl': pnl,
                'pct': pct,
            })
            # continue after exit
            i = j + 1
            continue

        # bearish entry
        if action == -2:
            entry_bar = df.iloc[i + 1]
            entry_price = entry_bar['open']
            atr = entry_bar['atr']
            sl = entry_price + config.sl_atr * atr
            tp = entry_price - config.tp_atr * atr

            exit_price = None
            exit_time = None
            j = i + 1
            while j < len(df):
                bar = df.iloc[j]
                if bar['high'] >= sl:
                    exit_price = sl
                    exit_time = bar['datetime']
                    break
                if bar['low'] <= tp:
                    exit_price = tp
                    exit_time = bar['datetime']
                    break
                if j + 1 < len(df) and df.iloc[j]['action'] == 2:
                    exit_bar = df.iloc[j + 1]
                    exit_price = exit_bar['open']
                    exit_time = exit_bar['datetime']
                    break
                j += 1
            if exit_price is None:
                exit_price = df.iloc[-1]['close']
                exit_time = df.iloc[-1]['datetime']

            pnl = entry_price - exit_price  # profit for short is entry - exit
            pct = pnl / entry_price if entry_price != 0 else 0
            trades.append({
                'side': 'sell',
                'entry_time': entry_bar['datetime'],
                'entry_price': entry_price,
                'exit_time': exit_time,
                'exit_price': exit_price,
                'pnl': pnl,
                'pct': pct,
            })
            i = j + 1
            continue

        i += 1

    # Save trades to CSV
    out_file = f"backtest_{symbol}_{start_str}_{end_str}.csv"
    with open(out_file, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=['side','entry_time','entry_price','exit_time','exit_price','pnl','pct'])
        writer.writeheader()
        for t in trades:
            writer.writerow(t)

    # Summary
    total = len(trades)
    wins = sum(1 for t in trades if t['pnl'] > 0)
    losses = sum(1 for t in trades if t['pnl'] <= 0)
    total_pnl = sum(t['pnl'] for t in trades)
    avg_pnl = total_pnl / total if total else 0

    print('Backtest summary')
    print('--------------------------------')
    print(f'Trades: {total}, Wins: {wins}, Losses: {losses}')
    print(f'Total PnL (price units): {total_pnl:.6f}, Avg per trade: {avg_pnl:.6f}')
    print(f'Output saved to {out_file}')

    await storage.close()


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--db-url', required=False, help='Database URL (async)')
    parser.add_argument('--symbol', required=True)
    parser.add_argument('--start', required=True)
    parser.add_argument('--end', required=True)
    parser.add_argument('--fast', type=int, default=10)
    parser.add_argument('--slow', type=int, default=20)
    parser.add_argument('--timeframe', default='5min')
    args = parser.parse_args()

    db_url = args.db_url or os.getenv('DATABASE_URL')
    if not db_url:
        raise RuntimeError('Provide --db-url or set DATABASE_URL env')

    asyncio.run(run_backtest(db_url, args.symbol, args.start, args.end, args.fast, args.slow, args.timeframe))
