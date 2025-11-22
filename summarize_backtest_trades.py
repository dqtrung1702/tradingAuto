#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Utility script to aggregate results stored in backtest_trades.

Examples
--------
python summarize_backtest_trades.py \
  --db-url postgresql+asyncpg://user:pass@localhost:5432/mt5 \
  --top 5

python summarize_backtest_trades.py \
  --db-url postgresql+asyncpg://user:pass@localhost:5432/mt5 \
  --run-id bt_XAUUSD_ab12cd34
"""

import argparse
from typing import Optional

from sqlalchemy import create_engine, text


def _normalize_db_url(url: str) -> str:
    """Allow passing async-style URLs by stripping driver hints."""
    if "+asyncpg" in url:
        return url.replace("+asyncpg", "")
    return url


def summarize_runs(db_url: str, run_id: Optional[str], top: int) -> None:
    engine = create_engine(_normalize_db_url(db_url))
    base_query = """
        SELECT
            run_id,
            symbol,
            COALESCE(preset, 'custom') AS preset,
            MIN(run_start) AS run_start,
            MAX(run_end) AS run_end,
            COUNT(*) AS total_trades,
            SUM(CASE WHEN pnl > 0 THEN 1 ELSE 0 END) AS wins,
            SUM(CASE WHEN pnl <= 0 THEN 1 ELSE 0 END) AS losses,
            SUM(pnl) AS total_pnl,
            AVG(pnl) AS avg_pnl,
            SUM(COALESCE(usd_pnl, 0)) AS total_usd_pnl,
            AVG(COALESCE(usd_pnl, 0)) AS avg_usd_pnl
        FROM backtest_trades
        {where_clause}
        GROUP BY run_id, symbol, COALESCE(preset, 'custom')
        ORDER BY run_start DESC
        {limit_clause}
    """
    where_clause = ""
    params = {}
    if run_id:
        where_clause = "WHERE run_id = :run_id"
        params["run_id"] = run_id
        limit_clause = ""
    else:
        limit_clause = "LIMIT :limit"
        params["limit"] = top

    query = base_query.format(where_clause=where_clause, limit_clause=limit_clause)
    with engine.connect() as conn:
        rows = conn.execute(text(query), params).fetchall()
        if not rows:
            print("❌ Không tìm thấy bản ghi phù hợp trong backtest_trades.")
            return

        for row in rows:
            print("=" * 60)
            print(f"Run ID      : {row.run_id}")
            print(f"Symbol/Preset: {row.symbol} / {row.preset}")
            print(f"Range       : {row.run_start} ➜ {row.run_end}")
            print(f"Trades      : {row.total_trades} (wins {row.wins}, losses {row.losses})")
            print(f"PnL (pts)   : total {row.total_pnl:.4f} | avg {row.avg_pnl:.4f}")
            print(f"PnL (USD)   : total {row.total_usd_pnl:.2f} | avg {row.avg_usd_pnl:.2f}")
        print("=" * 60)


def main() -> None:
    parser = argparse.ArgumentParser(description="Summarize runs stored in backtest_trades.")
    parser.add_argument("--db-url", required=True, help="Chuỗi kết nối DB (có thể dùng +asyncpg).")
    parser.add_argument("--run-id", help="run_id cụ thể cần xem.")
    parser.add_argument("--top", type=int, default=5, help="Số run mới nhất hiển thị (khi không truyền run_id).")
    args = parser.parse_args()

    summarize_runs(args.db_url, args.run_id, args.top)


if __name__ == "__main__":
    main()
