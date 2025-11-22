"""CLI hợp nhất cho toàn bộ công cụ tradingAuto.

Chạy bằng: ``python -m app.cli <command> [options]``
"""

from __future__ import annotations

import argparse
import asyncio
from datetime import datetime, timezone
from typing import Optional

from app.commands import backtest_ma, history, ingest, live_ma
from app import strategy_presets


def _parse_dt(value: Optional[str]) -> Optional[datetime]:
    if not value:
        return None
    dt = datetime.fromisoformat(value)
    return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)


def main() -> None:
    parser = argparse.ArgumentParser(description="Bộ công cụ tradingAuto")
    sub = parser.add_subparsers(dest="command", required=True)

    ingest_parser = sub.add_parser("ingest-live", help="Ingest realtime ticks từ MT5 vào DB")
    ingest_parser.add_argument("--db-url", required=True)
    ingest_parser.add_argument("--symbol", required=True)
    ingest_parser.add_argument("--interval", type=float, default=1.0)

    hist_parser = sub.add_parser("fetch-history", help="Lấy ticks lịch sử từ MT5 vào DB")
    hist_parser.add_argument("--symbol", required=True)
    hist_parser.add_argument("--start", required=True, help="YYYY-MM-DD")
    hist_parser.add_argument("--end", required=True, help="YYYY-MM-DD")
    hist_parser.add_argument("--db-url", required=True)
    hist_parser.add_argument("--batch", type=int, default=2000)
    hist_parser.add_argument("--max-days", type=int, default=1)

    sub.add_parser("list-symbols", help="Liệt kê symbol khả dụng trong MT5")

    backtest_ma_parser = sub.add_parser("backtest-ma", help="Backtest nâng cao MA crossover")
    backtest_ma_parser.add_argument("--db-url", required=True)
    backtest_ma_parser.add_argument("--symbol", required=True)
    backtest_ma_parser.add_argument("--start", required=True)
    backtest_ma_parser.add_argument("--end", required=True)
    backtest_ma_parser.add_argument("--preset", choices=list(strategy_presets.PRESETS.keys()), help="Chọn preset chiến lược")
    backtest_ma_parser.add_argument("--fast", type=int, default=21)
    backtest_ma_parser.add_argument("--slow", type=int, default=89)
    backtest_ma_parser.add_argument("--timeframe", default="5min")
    backtest_ma_parser.add_argument("--ma-type", default="ema", choices=["sma", "ema"])
    backtest_ma_parser.add_argument("--trend", type=int, default=200)
    backtest_ma_parser.add_argument(
        "--risk-pct",
        type=float,
        default=0.02,
        help="Tỷ lệ rủi ro mỗi lệnh (dạng thập phân, ví dụ 0.02 = 2%)",
    )
    backtest_ma_parser.add_argument("--capital", type=float, default=10000.0)
    backtest_ma_parser.add_argument("--trail-trigger-atr", type=float, default=1.8)
    backtest_ma_parser.add_argument("--trail-atr-mult", type=float, default=1.1)
    backtest_ma_parser.add_argument("--spread-atr-max", type=float, default=0.2)
    backtest_ma_parser.add_argument("--reverse-exit", action="store_true")
    backtest_ma_parser.add_argument("--market-state-window", type=int, default=20)
    backtest_ma_parser.add_argument("--sl-atr", type=float, default=2.0)
    backtest_ma_parser.add_argument("--tp-atr", type=float, default=3.0)
    backtest_ma_parser.add_argument("--volume", type=float, default=0.01)
    backtest_ma_parser.add_argument("--contract-size", type=float, default=100.0)
    backtest_ma_parser.add_argument("--sl-pips", type=float, default=None)
    backtest_ma_parser.add_argument("--tp-pips", type=float, default=None)
    backtest_ma_parser.add_argument("--pip-size", type=float, default=0.01)
    backtest_ma_parser.add_argument("--size-from-risk", action="store_true")
    backtest_ma_parser.add_argument("--momentum-type", choices=["macd", "pct", "hybrid"], default="hybrid")
    backtest_ma_parser.add_argument("--momentum-window", type=int, default=14)
    backtest_ma_parser.add_argument("--momentum-threshold", type=float, default=0.1)
    backtest_ma_parser.add_argument("--macd-fast", type=int, default=12)
    backtest_ma_parser.add_argument("--macd-slow", type=int, default=26)
    backtest_ma_parser.add_argument("--macd-signal", type=int, default=9)
    backtest_ma_parser.add_argument("--macd-threshold", type=float, default=0.0)
    backtest_ma_parser.add_argument("--range-lookback", type=int, default=40)
    backtest_ma_parser.add_argument("--range-min-atr", type=float, default=0.8)
    backtest_ma_parser.add_argument("--range-min-points", type=float, default=0.5)
    backtest_ma_parser.add_argument("--breakout-buffer-atr", type=float, default=0.5)
    backtest_ma_parser.add_argument("--breakout-confirmation-bars", type=int, default=2)
    backtest_ma_parser.add_argument("--atr-baseline-window", type=int, default=14)
    backtest_ma_parser.add_argument("--atr-multiplier-min", type=float, default=0.8)
    backtest_ma_parser.add_argument("--atr-multiplier-max", type=float, default=4.0)
    backtest_ma_parser.add_argument("--trading-hours", default=None)
    backtest_ma_parser.add_argument("--adx-window", type=int, default=14)
    backtest_ma_parser.add_argument("--adx-threshold", type=float, default=25.0)
    backtest_ma_parser.add_argument("--rsi-threshold-long", type=float, default=60.0)
    backtest_ma_parser.add_argument("--rsi-threshold-short", type=float, default=40.0)
    backtest_ma_parser.add_argument("--max-daily-loss", type=float, default=None,
                                help="Giới hạn lỗ tuyệt đối mỗi ngày (USD)")
    backtest_ma_parser.add_argument("--max-loss-streak", type=int, default=None)
    backtest_ma_parser.add_argument("--max-losses-per-session", type=int, default=None)
    backtest_ma_parser.add_argument("--cooldown-minutes", type=int, default=None,
                                help="Số phút tạm dừng trade sau khi vi phạm guard")
    backtest_ma_parser.add_argument("--allow-buy", type=int, choices=[0, 1], default=1,
                                help="1: cho phép BUY, 0: chặn BUY")
    backtest_ma_parser.add_argument("--allow-sell", type=int, choices=[0, 1], default=1,
                                 help="1: cho phép SELL, 0: chặn SELL")
    backtest_ma_parser.add_argument("--order-retry-times", type=int, default=0)
    backtest_ma_parser.add_argument("--order-retry-delay-ms", type=int, default=0)
    backtest_ma_parser.add_argument("--safety-entry-atr-mult", type=float, default=0.1)
    backtest_ma_parser.add_argument("--spread-samples", type=int, default=5)
    backtest_ma_parser.add_argument("--spread-sample-delay-ms", type=int, default=8)
    backtest_ma_parser.add_argument("--allowed-deviation-points", type=int, default=300)
    backtest_ma_parser.add_argument("--volatility-spike-atr-mult", type=float, default=0.8)
    backtest_ma_parser.add_argument("--spike-delay-ms", type=int, default=50)
    backtest_ma_parser.add_argument("--skip-reset-window", type=int, choices=[0, 1], default=1)
    backtest_ma_parser.add_argument("--latency-min-ms", type=int, default=200)
    backtest_ma_parser.add_argument("--latency-max-ms", type=int, default=400)
    backtest_ma_parser.add_argument("--slippage-usd", type=float, default=0.05)
    backtest_ma_parser.add_argument("--order-reject-prob", type=float, default=0.03)
    backtest_ma_parser.add_argument("--base-spread-points", type=int, default=50)
    backtest_ma_parser.add_argument("--spread-spike-chance", type=float, default=0.02)
    backtest_ma_parser.add_argument("--spread-spike-min-points", type=int, default=80)
    backtest_ma_parser.add_argument("--spread-spike-max-points", type=int, default=300)
    backtest_ma_parser.add_argument("--slip-per-atr-ratio", type=float, default=0.2)
    backtest_ma_parser.add_argument("--requote-prob", type=float, default=0.01)
    backtest_ma_parser.add_argument("--offquotes-prob", type=float, default=0.005)
    backtest_ma_parser.add_argument("--timeout-prob", type=float, default=0.005)
    backtest_ma_parser.add_argument("--stop-hunt-chance", type=float, default=0.015)
    backtest_ma_parser.add_argument("--stop-hunt-min-atr-ratio", type=float, default=0.2)
    backtest_ma_parser.add_argument("--stop-hunt-max-atr-ratio", type=float, default=1.0)
    backtest_ma_parser.add_argument("--missing-tick-chance", type=float, default=0.005)

    live_parser = sub.add_parser("run-live-ma", help="Chạy chiến lược MA Crossover realtime")
    live_parser.add_argument("--db-url", required=True)
    live_parser.add_argument("--symbol", default=None)
    live_parser.add_argument("--preset", choices=list(strategy_presets.PRESETS.keys()), help="Chọn preset chiến lược")
    live_parser.add_argument("--fast", type=int, default=21)
    live_parser.add_argument("--slow", type=int, default=89)
    live_parser.add_argument("--ma-type", default="ema", choices=["sma", "ema"])
    live_parser.add_argument("--timeframe", default="1min")
    live_parser.add_argument("--trend", type=int, default=200)
    live_parser.add_argument("--spread-atr-max", type=float, default=0.2)
    live_parser.add_argument("--reverse-exit", action="store_true")
    live_parser.add_argument("--market-state-window", type=int, default=20)
    live_parser.add_argument("--volume", type=float, default=0.10)
    live_parser.add_argument("--capital", type=float, default=10000.0)
    live_parser.add_argument(
        "--risk-pct",
        type=float,
        default=0.02,
        help="Tỷ lệ rủi ro mỗi lệnh (dạng thập phân, ví dụ 0.02 = 2%)",
    )
    live_parser.add_argument("--contract-size", type=float, default=100.0)
    live_parser.add_argument("--size-from-risk", action="store_true")
    live_parser.add_argument("--ensure-history-hours", type=float, default=0.0,
                             help="Tự fetch dữ liệu lịch sử n giờ gần nhất nếu DB thiếu")
    live_parser.add_argument("--history-batch", type=int, default=2000)
    live_parser.add_argument("--history-max-days", type=int, default=1)
    # luôn ghi tick realtime, không cần option
    live_parser.add_argument("--sl-atr", type=float, default=2.0)
    live_parser.add_argument("--tp-atr", type=float, default=3.0)
    live_parser.add_argument("--trail-trigger-atr", type=float, default=1.8)
    live_parser.add_argument("--trail-atr-mult", type=float, default=1.1)
    live_parser.add_argument("--sl-pips", type=float, default=None)
    live_parser.add_argument("--tp-pips", type=float, default=None)
    live_parser.add_argument("--pip-size", type=float, default=0.01)
    live_parser.add_argument("--momentum-type", choices=["macd", "pct", "hybrid"], default="hybrid")
    live_parser.add_argument("--momentum-window", type=int, default=14)
    live_parser.add_argument("--momentum-threshold", type=float, default=0.1)
    live_parser.add_argument("--macd-fast", type=int, default=12)
    live_parser.add_argument("--macd-slow", type=int, default=26)
    live_parser.add_argument("--macd-signal", type=int, default=9)
    live_parser.add_argument("--macd-threshold", type=float, default=0.0)
    live_parser.add_argument("--range-lookback", type=int, default=40)
    live_parser.add_argument("--range-min-atr", type=float, default=0.8)
    live_parser.add_argument("--range-min-points", type=float, default=0.5)
    live_parser.add_argument("--breakout-buffer-atr", type=float, default=0.5)
    live_parser.add_argument("--breakout-confirmation-bars", type=int, default=2)
    live_parser.add_argument("--atr-baseline-window", type=int, default=14)
    live_parser.add_argument("--atr-multiplier-min", type=float, default=0.8)
    live_parser.add_argument("--atr-multiplier-max", type=float, default=4.0)
    live_parser.add_argument("--trading-hours", default=None)
    live_parser.add_argument("--adx-window", type=int, default=14)
    live_parser.add_argument("--adx-threshold", type=float, default=25.0)
    live_parser.add_argument("--rsi-threshold-long", type=float, default=60.0)
    live_parser.add_argument("--rsi-threshold-short", type=float, default=40.0)
    live_parser.add_argument("--max-daily-loss", type=float, default=None)
    live_parser.add_argument("--max-loss-streak", type=int, default=None)
    live_parser.add_argument("--max-losses-per-session", type=int, default=None)
    live_parser.add_argument("--cooldown-minutes", type=int, default=None)
    live_parser.add_argument("--max-holding-minutes", type=int, default=None,
                             help="Tự đóng lệnh sau số phút nắm giữ nếu được đặt")
    live_parser.add_argument("--allow-buy", type=int, choices=[0, 1], default=1)
    live_parser.add_argument("--allow-sell", type=int, choices=[0, 1], default=1)
    live_parser.add_argument("--poll", type=float, default=1.0)
    live_parser.add_argument("--live", action="store_true", help="Bật gửi lệnh MT5 thật")

    args = parser.parse_args()

    if args.command == "ingest-live":
        asyncio.run(ingest.ingest_live_ticks(db_url=args.db_url, symbol=args.symbol, interval=args.interval))
    elif args.command == "fetch-history":
        start = _parse_dt(args.start)
        end = _parse_dt(args.end)
        if start is None or end is None:
            raise ValueError("--start và --end phải theo định dạng ISO8601 (YYYY-MM-DD)")
        asyncio.run(
            history.fetch_history(
                symbol=args.symbol,
                start=start,
                end=end,
                db_url=args.db_url,
                batch=args.batch,
                max_days=args.max_days,
            )
        )
    elif args.command == "list-symbols":
        history.list_available_symbols()
    elif args.command == "backtest-ma":
        asyncio.run(
            backtest_ma.run_backtest(
                db_url=args.db_url,
                symbol=args.symbol,
                preset=args.preset,
                start_str=args.start,
                end_str=args.end,
                fast=args.fast,
                slow=args.slow,
                timeframe=args.timeframe,
                ma_type=args.ma_type,
                trend=args.trend,
                risk_pct=args.risk_pct,
                capital=args.capital,
                trail_trigger_atr=args.trail_trigger_atr,
                trail_atr_mult=args.trail_atr_mult,
                spread_atr_max=args.spread_atr_max,
                reverse_exit=args.reverse_exit,
                market_state_window=args.market_state_window,
                sl_atr=args.sl_atr,
                tp_atr=args.tp_atr,
                volume=args.volume,
                contract_size=args.contract_size,
                sl_pips=args.sl_pips,
                tp_pips=args.tp_pips,
                pip_size=args.pip_size,
                momentum_type=args.momentum_type,
                momentum_window=args.momentum_window,
                momentum_threshold=args.momentum_threshold,
                macd_fast=args.macd_fast,
                macd_slow=args.macd_slow,
                macd_signal=args.macd_signal,
                macd_threshold=args.macd_threshold,
                size_from_risk=args.size_from_risk,
                range_lookback=args.range_lookback,
                range_min_atr=args.range_min_atr,
                range_min_points=args.range_min_points,
                breakout_buffer_atr=args.breakout_buffer_atr,
                breakout_confirmation_bars=args.breakout_confirmation_bars,
                atr_baseline_window=args.atr_baseline_window,
                atr_multiplier_min=args.atr_multiplier_min,
                atr_multiplier_max=args.atr_multiplier_max,
                trading_hours=args.trading_hours,
                adx_window=args.adx_window,
                adx_threshold=args.adx_threshold,
                rsi_threshold_long=args.rsi_threshold_long,
                rsi_threshold_short=args.rsi_threshold_short,
                max_daily_loss=args.max_daily_loss,
                max_loss_streak=args.max_loss_streak,
                max_losses_per_session=args.max_losses_per_session,
                cooldown_minutes=args.cooldown_minutes,
                allow_buy=bool(args.allow_buy),
                allow_sell=bool(args.allow_sell),
                order_retry_times=args.order_retry_times,
                order_retry_delay_ms=args.order_retry_delay_ms,
                safety_entry_atr_mult=args.safety_entry_atr_mult,
                spread_samples=args.spread_samples,
                spread_sample_delay_ms=args.spread_sample_delay_ms,
                allowed_deviation_points=args.allowed_deviation_points,
                volatility_spike_atr_mult=args.volatility_spike_atr_mult,
                spike_delay_ms=args.spike_delay_ms,
                skip_reset_window=bool(args.skip_reset_window),
                latency_min_ms=args.latency_min_ms,
                latency_max_ms=args.latency_max_ms,
                slippage_usd=args.slippage_usd,
                order_reject_prob=args.order_reject_prob,
                base_spread_points=args.base_spread_points,
                spread_spike_chance=args.spread_spike_chance,
                spread_spike_min_points=args.spread_spike_min_points,
                spread_spike_max_points=args.spread_spike_max_points,
                slip_per_atr_ratio=args.slip_per_atr_ratio,
                requote_prob=args.requote_prob,
                offquotes_prob=args.offquotes_prob,
                timeout_prob=args.timeout_prob,
                stop_hunt_chance=args.stop_hunt_chance,
                stop_hunt_min_atr_ratio=args.stop_hunt_min_atr_ratio,
                stop_hunt_max_atr_ratio=args.stop_hunt_max_atr_ratio,
                missing_tick_chance=args.missing_tick_chance,
            )
        )
    elif args.command == "run-live-ma":
        asyncio.run(
            live_ma.run_live_strategy(
                db_url=args.db_url,
                symbol=args.symbol,
                preset=args.preset,
                fast=args.fast,
                slow=args.slow,
                ma_type=args.ma_type,
                timeframe=args.timeframe,
                trend=args.trend,
                spread_atr_max=args.spread_atr_max,
                reverse_exit=args.reverse_exit,
                market_state_window=args.market_state_window,
                volume=args.volume,
                capital=args.capital,
                risk_pct=args.risk_pct,
                contract_size=args.contract_size,
                size_from_risk=args.size_from_risk,
                sl_atr=args.sl_atr,
                tp_atr=args.tp_atr,
                sl_pips=args.sl_pips,
                tp_pips=args.tp_pips,
                pip_size=args.pip_size,
                momentum_type=args.momentum_type,
                momentum_window=args.momentum_window,
                momentum_threshold=args.momentum_threshold,
                macd_fast=args.macd_fast,
                macd_slow=args.macd_slow,
                macd_signal=args.macd_signal,
                macd_threshold=args.macd_threshold,
                range_lookback=args.range_lookback,
                range_min_atr=args.range_min_atr,
                range_min_points=args.range_min_points,
                breakout_buffer_atr=args.breakout_buffer_atr,
                breakout_confirmation_bars=args.breakout_confirmation_bars,
                atr_baseline_window=args.atr_baseline_window,
                atr_multiplier_min=args.atr_multiplier_min,
                atr_multiplier_max=args.atr_multiplier_max,
                trading_hours=args.trading_hours,
                adx_window=args.adx_window,
                adx_threshold=args.adx_threshold,
                rsi_threshold_long=args.rsi_threshold_long,
                rsi_threshold_short=args.rsi_threshold_short,
                max_daily_loss=args.max_daily_loss,
                max_loss_streak=args.max_loss_streak,
                max_losses_per_session=args.max_losses_per_session,
                cooldown_minutes=args.cooldown_minutes,
                max_holding_minutes=args.max_holding_minutes,
                allow_buy=bool(args.allow_buy),
                allow_sell=bool(args.allow_sell),
                poll=args.poll,
                live=args.live,
                ensure_history_hours=args.ensure_history_hours,
                history_batch=args.history_batch,
                history_max_days=args.history_max_days,
                ingest_live_db=args.ingest_live_db,
            )
        )
    else:  # pragma: no cover - fallback
        parser.print_help()


if __name__ == "__main__":  # pragma: no cover
    main()
