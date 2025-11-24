#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Wrap optimizer script for breakout strategy.

Runs:
- Load pre-built 5m bars from DB
- Grid-search optimize using evaluate_breakout()
- Selects best param by sharpe - DD penalty
- Saves best param to JSON for MT5 bot

Usage:
------
python optimize_wrap.py \
--db-url postgresql://trader:admin@localhost:5432/mt5 \
--bars-table bars_5m \
--symbol XAUUSD \
--session all \
--start "2025-10-01" \
--end "2025-11-15" \
--tz 7

📥 Loading bars from DB...
🔄 Running optimization for ASIA session...
🧮 Testing 36 parameter combos...
🏆 Best for ASIA: Score=-1.60, PnL=0.00, Sharpe=0.00, MaxDD=-16.00
{
  "range_lookback": 40,
  "range_min_atr": 0.5,
  "breakout_buffer_atr": 0.3,
  "sl_atr": 1.5,
  "tp_atr": 2.0,
  "rsi_period": 14,
  "rsi_long": 60,
  "rsi_short": 40,
  "macd_fast": 12,
  "macd_slow": 26,
  "macd_signal": 9,
  "macd_threshold": 0.00015
}
🔄 Running optimization for EUROPE session...
🧮 Testing 36 parameter combos...
🏆 Best for EUROPE: Score=-0.18, PnL=4.20, Sharpe=0.38, MaxDD=-5.60
{
  "range_lookback": 40,
  "range_min_atr": 0.5,
  "breakout_buffer_atr": 0.3,
  "sl_atr": 1.3,
  "tp_atr": 2.0,
  "rsi_period": 14,
  "rsi_long": 60,
  "rsi_short": 40,
  "macd_fast": 12,
  "macd_slow": 26,
  "macd_signal": 9,
  "macd_threshold": 0.00015
}
🔄 Running optimization for US session...
🧮 Testing 36 parameter combos...
🏆 Best for US: Score=1.17, PnL=40.00, Sharpe=2.22, MaxDD=-10.50
{
  "range_lookback": 40,
  "range_min_atr": 0.5,
  "breakout_buffer_atr": 0.2,
  "sl_atr": 1.5,
  "tp_atr": 2.5,
  "rsi_period": 14,
  "rsi_long": 60,
  "rsi_short": 40,
  "macd_fast": 12,
  "macd_slow": 26,
  "macd_signal": 9,
  "macd_threshold": 0.00015
}

💾 Saved best config to best_breakout_config.json
✨ Best session: US
{
  "range_lookback": 40,
  "range_min_atr": 0.5,
  "breakout_buffer_atr": 0.2,
  "sl_atr": 1.5,
  "tp_atr": 2.5,
  "rsi_period": 14,
  "rsi_long": 60,
  "rsi_short": 40,
  "macd_fast": 12,
  "macd_slow": 26,
  "macd_signal": 9,
  "macd_threshold": 0.00015
}
"""

import json
import argparse
import pandas as pd
from optimize_breakout_params_v2 import (
    load_bars_from_db,
    extract_session,
    generate_param_grid,
    evaluate_breakout,
)

def optimize_for_session(bars, session, capital, risk_pct):
    print(f"🔄 Running optimization for {session.upper()} session...")

    sess_bars = extract_session(bars, session)
    if len(sess_bars) < 100:
        print(f"⚠️ Not enough bars for session '{session}'. Skipping.")
        return None

    param_grid = generate_param_grid()
    print(f"🧮 Testing {len(param_grid)} parameter combos...")

    results = []
    for params in param_grid:
        pnl, sharpe, dd = evaluate_breakout(
            sess_bars,
            params,
            capital=capital,
            risk_pct=risk_pct,
        )
        score = sharpe - 0.1 * abs(dd)  # penalty on DD
        results.append((score, pnl, sharpe, dd, params))

    # sort by score
    best = sorted(results, reverse=True, key=lambda x: x[0])[0]
    score, pnl, sharpe, dd, params = best

    print(
        f"🏆 Best for {session.upper()}: Score={score:.2f}, PnL={pnl:.2f}, Sharpe={sharpe:.2f}, MaxDD={dd:.2f}%"
    )
    print(json.dumps(params, indent=2))

    return {
        "session": session,
        "params": params,
        "pnl": pnl,
        "sharpe": sharpe,
        "max_dd": dd,
        "score": score,
    }

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--db-url", required=True)
    parser.add_argument("--bars-table", default="bars_5m")
    parser.add_argument("--symbol", default="XAUUSD")
    parser.add_argument("--start", help="YYYY-MM-DD")
    parser.add_argument("--end", help="YYYY-MM-DD")
    parser.add_argument("--tz", type=int, default=0)
    parser.add_argument("--session", choices=["asia", "europe", "us", "all"], default="all")
    parser.add_argument("--capital", type=float, default=100.0, help="Account capital for sizing (USD).")
    parser.add_argument(
        "--risk-pct",
        type=float,
        default=0.06,
        help="Risk per trade expressed as fraction of capital (e.g. 0.02 = 2%).",
    )
    args = parser.parse_args()

    # Load tick data
    print(f"📥 Loading ticks from DB...")
    bars = load_bars_from_db(
        db_url=args.db_url,
        table_name=args.bars_table,
        symbol=args.symbol,
        start_time=args.start,
        end_time=args.end,
        tz_offset_hours=args.tz,
    )
    if bars.empty:
        print("❌ Không có dữ liệu bars trong khoảng thời gian yêu cầu.")
        return

    sessions = ["asia", "europe", "us"] if args.session == "all" else [args.session]
    session_results = []

    for sess in sessions:
        result = optimize_for_session(bars, sess, args.capital, args.risk_pct)
        if result:
            session_results.append(result)

    if not session_results:
        print("❌ No valid optimization results.")
        return

    # Save best config
    best_overall = sorted(session_results, reverse=True, key=lambda x: x["score"])[0]
    with open("best_breakout_config.json", "w") as f:
        json.dump(best_overall, f, indent=2)

    print("\n💾 Saved best config to best_breakout_config.json")
    print(f"✨ Best session: {best_overall['session'].upper()}")
    print(json.dumps(best_overall["params"], indent=2))

if __name__ == "__main__":
    main()
