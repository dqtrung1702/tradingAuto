#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Version 2: Breakout Strategy Optimizer with PostgreSQL support.
Supports:
- Tick data ingestion from database
- Resampling to bars
- ATR / RSI / MACD calculation
- Grid-search breakout backtesting
- Session-based parameter optimization
- Score by Sharpe ratio and Drawdown

Example usage:
--------------
python optimize_breakout_params_v2.py \
--db-url postgresql://user:pass@localhost:5432/mt5 \
--table-name ticks \
--symbol XAUUSD \
--session us \
--tz-offset 7 \
--start-time "2025-10-01" \
--end-time "2025-11-15" \
--capital 10000 \
--risk-pct 0.01

python optimize_breakout_params_v2.py \
--db-url postgresql://trader:admin@localhost:5432/mt5 \
--table-name ticks \
--session us \
--tz-offset 7 \
--start-time "2025-10-01" \
--end-time "2025-11-18"

{
  "timeframe": "5min",
  "fast": 8,
  "slow": 21,
  "ma_type": "ema",
  "volume": null,
  "capital": 10000.0,
  "risk_pct": 0.1,
  "size_from_risk": true,
  "breakout_conditions": {
    "range_lookback": 40,
    "range_min_atr": 0.6,
    "range_min_points": 0.0,
    "breakout_buffer_atr": 0.3,
    "breakout_confirmation_bars": 1
  },
  "momentum_confirmation": {
    "momentum_type": "hybrid",
    "momentum_window": 10,
    "momentum_threshold": 0.1,
    "rsi_threshold_long": 60,
    "rsi_threshold_short": 40,
    "macd_fast": 12,
    "macd_slow": 26,
    "macd_signal": 9,
    "macd_threshold": 0.0002
  },
  "trade_management": {
    "sl_atr": 1.5,
    "tp_atr": 2.5,
    "trail_trigger_atr": 2.0,
    "trail_atr_mult": 1.1,
    "contract_size": 100
  },
  "risk_guard": {
    "max_daily_loss": 0.02,
    "max_loss_streak": 2,
    "max_loss_session": 2,
    "cooldown_minutes": 45
  },
  "symbol": "XAUUSD"
}
"""

import argparse
import json
import numpy as np
import pandas as pd
from sqlalchemy import create_engine, text

# --------------------------------------------
# Indicator utilities: ATR, RSI, MACD
# --------------------------------------------

def atr(series, period=14):
    prev_close = series["close"].shift(1)
    tr = pd.concat([
        (series["high"] - series["low"]).abs(),
        (series["high"] - prev_close).abs(),
        (series["low"] - prev_close).abs(),
    ], axis=1).max(axis=1)
    return tr.ewm(span=period, adjust=False).mean()

def rsi(series, period=14):
    diff = series["close"].diff()
    gain = diff.clip(lower=0).rolling(period).mean()
    loss = (-diff).clip(lower=0).rolling(period).mean()
    rs = gain / loss
    return 100 - (100 / (1 + rs))

def macd(series, fast=12, slow=26, signal=9):
    close = series["close"]
    ema_fast = close.ewm(span=fast, adjust=False).mean()
    ema_slow = close.ewm(span=slow, adjust=False).mean()
    macd_line = ema_fast - ema_slow
    signal_line = macd_line.ewm(span=signal, adjust=False).mean()
    macd_hist = macd_line - signal_line
    return macd_hist

# --------------------------------------------
# Tick ingestion via PostgreSQL
# --------------------------------------------

def load_ticks_from_db(db_url, table_name, start_time=None, end_time=None, tz_offset_hours=0, symbol=None):
    engine = create_engine(db_url)
    clauses = ["bid IS NOT NULL", "ask IS NOT NULL"]
    params = {}
    if symbol:
        clauses.append("symbol = :symbol")
        params["symbol"] = symbol
    if start_time:
        clauses.append("datetime >= :start")
        params["start"] = start_time
    if end_time:
        clauses.append("datetime <= :end")
        params["end"] = end_time
    where_sql = " AND ".join(clauses)
    query = text(f"SELECT datetime, bid, ask FROM {table_name} WHERE {where_sql}")

    df = pd.read_sql_query(query, engine, params=params)
    df["time"] = pd.to_datetime(df["datetime"], utc=True, errors="coerce")
    df = df.dropna(subset=["time"]).set_index("time").sort_index()

    df["mid"] = (df["bid"] + df["ask"]) / 2
    df["spread"] = df["ask"] - df["bid"]

    if tz_offset_hours != 0:
        df.index = df.index + pd.Timedelta(hours=tz_offset_hours)

    return df


def load_bars_from_db(db_url, table_name, symbol, start_time=None, end_time=None, tz_offset_hours=0):
    engine = create_engine(db_url)
    clauses = ["symbol = :symbol"]
    params = {"symbol": symbol}
    if start_time:
        clauses.append("time >= :start")
        params["start"] = start_time
    if end_time:
        clauses.append("time <= :end")
        params["end"] = end_time
    where_sql = " AND ".join(clauses)
    query = text(f"""
        SELECT time, open, high, low, close, spread
        FROM {table_name}
        WHERE {where_sql}
        ORDER BY time
    """)
    df = pd.read_sql_query(query, engine, params=params)
    if df.empty:
        return df
    df["time"] = pd.to_datetime(df["time"], utc=True, errors="coerce")
    df = df.dropna(subset=["time"]).set_index("time").sort_index()
    if tz_offset_hours:
        df.index = df.index + pd.Timedelta(hours=tz_offset_hours)
    return df

# --------------------------------------------
# Resampling & Session Filtering
# --------------------------------------------

def resample_bars(df, tf="5min"):
    ohlc = df["mid"].resample(tf).ohlc()
    spread = df["spread"].resample(tf).mean()
    bars = ohlc.join(spread.rename("spread")).dropna()
    return bars

def extract_session(bars, session):
    hours = bars.index.hour
    if session == "asia":
        return bars[(hours >= 6) & (hours <= 11)]
    elif session == "europe":
        return bars[(hours >= 13) & (hours <= 17)]
    elif session == "us":
        return bars[(hours >= 20) | (hours <= 2)]
    else:
        raise ValueError(f"Unknown session = {session}")

# --------------------------------------------
# Breakout Strategy Backtest (per params)
# --------------------------------------------

def evaluate_breakout(bars: pd.DataFrame, params: dict):
    if len(bars) < 100:
        return 0.0, 0.0, 0.0

    rl = int(params.get("range_lookback", 40))
    min_atr = float(params.get("range_min_atr", 0.5))
    buf_atr = float(params.get("breakout_buffer_atr", 0.2))
    sl_atr = float(params.get("sl_atr", 1.5))
    tp_atr = float(params.get("tp_atr", 2.5))

    rsi_p = int(params.get("momentum_window", 14))
    rsi_long = float(params.get("rsi_threshold_long", 60))
    rsi_short = float(params.get("rsi_threshold_short", 40))

    macd_fast = int(params.get("macd_fast", 12))
    macd_slow = int(params.get("macd_slow", 26))
    macd_signal = int(params.get("macd_signal", 9))
    macd_thresh = float(params.get("macd_threshold", 0.0002))

    atr_vals = atr(bars, period=14).fillna(0)
    rsi_vals = rsi(bars, period=rsi_p).fillna(50)
    macd_vals = macd(bars, fast=macd_fast, slow=macd_slow, signal=macd_signal).fillna(0)

    highs = bars["high"].values
    lows = bars["low"].values
    closes = bars["close"].values
    atr_arr = atr_vals.values
    rsi_arr = rsi_vals.values
    macd_arr = macd_vals.values

    trade_R = []
    pos = None

    for i in range(max(rl, rsi_p + macd_signal), len(bars)):
        hi_i, lo_i, cl_i, atr_i, rsi_i, macd_i = (
            highs[i], lows[i], closes[i], atr_arr[i], rsi_arr[i], macd_arr[i]
        )
        if atr_i < min_atr:
            continue

        # exit logic
        if pos is not None:
            if pos["dir"] == "long":
                if lo_i <= pos["sl"]:
                    trade_R.append(-sl_atr); pos = None
                elif hi_i >= pos["tp"]:
                    trade_R.append(tp_atr); pos = None
            else:
                if hi_i >= pos["sl"]:
                    trade_R.append(-sl_atr); pos = None
                elif lo_i <= pos["tp"]:
                    trade_R.append(tp_atr); pos = None
            if pos:
                continue

        # breakout detection
        hi_range = highs[i - rl:i].max()
        lo_range = lows[i - rl:i].min()

        long_break = cl_i > hi_range + buf_atr * atr_i
        short_break = cl_i < lo_range - buf_atr * atr_i

        # momentum filter
        if long_break and rsi_i >= rsi_long and macd_i > macd_thresh:
            pos = {
                "dir": "long", "entry": cl_i,
                "sl": cl_i - sl_atr * atr_i,
                "tp": cl_i + tp_atr * atr_i,
            }
        elif short_break and rsi_i <= rsi_short and macd_i < -macd_thresh:
            pos = {
                "dir": "short", "entry": cl_i,
                "sl": cl_i + sl_atr * atr_i,
                "tp": cl_i - tp_atr * atr_i,
            }

    if not trade_R:
        return 0.0, 0.0, 0.0

    R = pd.Series(trade_R, dtype=float)
    sharpe = float((R.mean() / (R.std() + 1e-9)) * np.sqrt(len(R)))
    dd = float((R.cumsum() - R.cumsum().cummax()).min())
    return float(R.sum()), sharpe, dd

# --------------------------------------------
# Parameter Grid
# --------------------------------------------

def generate_param_grid():
    grid = []
    for rmin in [0.5, 0.6, 0.7]:
        for buf in [0.1, 0.2, 0.3]:
            for sl in [1.3, 1.5]:
                for tp in [2.0, 2.5]:
                    grid.append({
                        "range_lookback": 40,
                        "range_min_atr": rmin,
                        "breakout_buffer_atr": buf,
                        "sl_atr": sl,
                        "tp_atr": tp,
                        "momentum-window": 14,
                        "rsi_threshold_long": 60,
                        "rsi_threshold_short": 40,
                        "macd_fast": 12,
                        "macd_slow": 26,
                        "macd_signal": 9,
                        "macd_threshold": 0.00015,
                    })
    return grid
# def generate_param_grid():
#     grid = []

#     # 1) Các tham số breakout / ATR
#     range_lookback_list = [30, 40]              # Nến dùng để tính range
#     range_min_atr_list = [0.5, 0.6, 0.7]        # ATR tối thiểu
#     breakout_buffer_list = [0.1, 0.2, 0.3]      # Buffer * ATR
#     sl_atr_list = [1.3, 1.5]                    # SL theo ATR
#     tp_atr_list = [2.0, 2.5]                    # TP theo ATR

#     # 2) Momentum – RSI
#     rsi_period_list = [10, 14]                  # Momentum window (ngắn / chuẩn)
#     rsi_long_list = [58, 60, 62]                # Long threshold
#     rsi_short_list = [42, 40, 38]               # Short threshold

#     # 3) Momentum – MACD
#     macd_fast_list = [8, 12]                    # EMA nhanh
#     macd_slow_list = [21, 26]                   # EMA chậm
#     macd_signal_list = [5, 9]                   # Signal
#     macd_threshold_list = [0.0001, 0.00015, 0.0002]

#     for rl in range_lookback_list:
#         for rmin in range_min_atr_list:
#             for buf in breakout_buffer_list:
#                 for sl in sl_atr_list:
#                     for tp in tp_atr_list:
#                         for rsi_p in rsi_period_list:
#                             for rsi_long in rsi_long_list:
#                                 for rsi_short in rsi_short_list:
#                                     for mf in macd_fast_list:
#                                         for ms in macd_slow_list:
#                                             # bỏ các combo MACD vô lý
#                                             if ms <= mf:
#                                                 continue
#                                             for sig in macd_signal_list:
#                                                 for mth in macd_threshold_list:
#                                                     grid.append({
#                                                         "range_lookback": rl,
#                                                         "range_min_atr": rmin,
#                                                         "breakout_buffer_atr": buf,
#                                                         "sl_atr": sl,
#                                                         "tp_atr": tp,
#                                                         "rsi_period": rsi_p,
#                                                         "rsi_long": rsi_long,
#                                                         "rsi_short": rsi_short,
#                                                         "macd_fast": mf,
#                                                         "macd_slow": ms,
#                                                         "macd_signal": sig,
#                                                         "macd_threshold": mth,
#                                                     })
#     return grid
# --------------------------------------------
# Main Script
# --------------------------------------------

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--db-url", required=True)
    parser.add_argument("--bars-table", default="bars_5m", help="Tên bảng OHLC đã resample (mặc định bars_5m)")
    parser.add_argument("--session", choices=["asia", "europe", "us"], required=True)
    parser.add_argument("--tz-offset", type=int, default=0)
    parser.add_argument("--start-time")
    parser.add_argument("--end-time")
    parser.add_argument("--symbol", default="XAUUSD")
    parser.add_argument("--capital", type=float, default=10000)
    parser.add_argument("--risk-pct", type=float, default=2)
    args = parser.parse_args()

    bars = load_bars_from_db(
        args.db_url,
        args.bars_table,
        symbol=args.symbol,
        start_time=args.start_time,
        end_time=args.end_time,
        tz_offset_hours=args.tz_offset,
    )
    if bars.empty:
        raise RuntimeError("Không có dữ liệu bars trong khoảng thời gian yêu cầu.")
    bars_sess = extract_session(bars, args.session)

    print(f"🔍 Optimizing on {args.session.upper()} session with {len(bars_sess)} bars...")

    grid = generate_param_grid()
    best = None

    for params in grid:
        pnl_R, sharpe, dd = evaluate_breakout(bars_sess, params)
        score = sharpe - abs(dd) * 0.1
        if best is None or score > best["score"]:
            best = {
                "params": params,
                "pnl": pnl_R,
                "sharpe": sharpe,
                "dd": dd,
                "score": score,
            }

    print("\n🔥 Best Params:")
    print(json.dumps(best["params"], indent=2))
    print(f"PnL (R): {best['pnl']:.2f}, Sharpe: {best['sharpe']:.2f}, Max DD: {best['dd']:.2f}")

if __name__ == "__main__":
    main()
