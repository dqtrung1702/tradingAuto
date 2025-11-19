#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Resample tick data into 5-minute bars and store them in a dedicated table.

Usage:
------
python resample_ticks_to_bars.py \
  --db-url postgresql://user:pass@localhost:5432/mt5 \
  --tick-table ticks \
  --bars-table bars_5m \
  --symbol XAUUSD \
  --start 2025-11-01T00:00:00 \
  --end 2025-11-19T00:00:00
"""

import argparse

import pandas as pd
from sqlalchemy import create_engine, text

from optimize_breakout_params_v2 import load_ticks_from_db, resample_bars


def ensure_bars_table(engine, table_name: str) -> None:
    ddl = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        symbol VARCHAR(64) NOT NULL,
        time TIMESTAMPTZ NOT NULL,
        open DOUBLE PRECISION NOT NULL,
        high DOUBLE PRECISION NOT NULL,
        low DOUBLE PRECISION NOT NULL,
        close DOUBLE PRECISION NOT NULL,
        spread DOUBLE PRECISION,
        PRIMARY KEY (symbol, time)
    );
    """
    with engine.begin() as conn:
        conn.execute(text(ddl))


def main() -> None:
    parser = argparse.ArgumentParser(description="Resample ticks into bars_5m table.")
    parser.add_argument("--db-url", required=True)
    parser.add_argument("--tick-table", default="ticks")
    parser.add_argument("--bars-table", default="bars_5m")
    parser.add_argument("--symbol", default="XAUUSD")
    parser.add_argument("--start", help="ISO datetime (UTC). Nếu bỏ trống sẽ lấy toàn bộ.")
    parser.add_argument("--end", help="ISO datetime (UTC). Nếu bỏ trống sẽ lấy toàn bộ.")
    parser.add_argument("--timeframe", default="5min", help="Chu kỳ resample, ví dụ 5min/15min.")
    args = parser.parse_args()

    engine = create_engine(args.db_url)
    ticks = load_ticks_from_db(
        args.db_url,
        args.tick_table,
        start_time=args.start,
        end_time=args.end,
        tz_offset_hours=0,
        symbol=args.symbol,
    )
    if ticks.empty:
        print("❌ Không có tick phù hợp điều kiện.")
        return

    print(f"📉 Loaded {len(ticks)} ticks. Resampling to {args.timeframe} bars...")
    bars = resample_bars(ticks, tf=args.timeframe)
    bars = bars.dropna()
    if bars.empty:
        print("❌ Không tạo được bar nào sau resample.")
        return

    ensure_bars_table(engine, args.bars_table)
    bars_reset = bars.reset_index().rename(columns={"index": "time"})
    bars_reset["time"] = pd.to_datetime(bars_reset["time"], utc=True)
    bars_reset.insert(0, "symbol", args.symbol)

    start_ts = bars_reset["time"].min()
    end_ts = bars_reset["time"].max()
    assert isinstance(start_ts, pd.Timestamp)
    assert isinstance(end_ts, pd.Timestamp)
    start_dt = start_ts.to_pydatetime()
    end_dt = end_ts.to_pydatetime()

    with engine.begin() as conn:
        conn.execute(
            text(f"DELETE FROM {args.bars_table} WHERE symbol = :symbol AND time BETWEEN :start AND :end"),
            {"symbol": args.symbol, "start": start_dt, "end": end_dt},
        )

    bars_reset.to_sql(args.bars_table, engine, if_exists="append", index=False, method="multi")
    print(f"✅ Đã ghi {len(bars_reset)} bar vào {args.bars_table} ({args.symbol}).")


if __name__ == "__main__":
    main()
