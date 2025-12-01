"""CLI hợp nhất cho Donchian breakout.

Chạy: ``python -m app.cli <command> [options]``
"""

from __future__ import annotations

import argparse
import asyncio
from datetime import datetime, timezone
from typing import Optional

from app.commands import backtest, history, live
from app.config import DEFAULT_DONCHIAN_PARAMS


def _parse_dt(value: Optional[str]) -> Optional[datetime]:
    if not value:
        return None
    cleaned = value.strip()
    if cleaned.endswith("Z"):
        cleaned = cleaned[:-1] + "+00:00"
    dt = datetime.fromisoformat(cleaned)
    return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)


def _cfg(key: str):
    return DEFAULT_DONCHIAN_PARAMS.get(key)


def _cfg_str(key: str) -> Optional[str]:
    val = DEFAULT_DONCHIAN_PARAMS.get(key)
    if isinstance(val, list):
        return ",".join(str(v) for v in val)
    return val


def main() -> None:
    parser = argparse.ArgumentParser(description="Donchian breakout toolkit")
    sub = parser.add_subparsers(dest="command", required=True)

    hist_parser = sub.add_parser("fetch-history", help="Lấy ticks lịch sử từ MT5 vào DB")
    hist_parser.add_argument("--symbol", default=_cfg("symbol"))
    hist_parser.add_argument("--start", default=_cfg("start"), help="YYYY-MM-DD")
    hist_parser.add_argument("--end", default=_cfg("end"), help="YYYY-MM-DD")
    hist_parser.add_argument("--db-url", default=_cfg("db_url"))
    hist_parser.add_argument("--batch", type=int, default=_cfg("history_batch") or 2000)
    hist_parser.add_argument("--max-days", type=int, default=_cfg("history_max_days") or 1)

    sub.add_parser("list-symbols", help="Liệt kê symbol khả dụng trong MT5")

    backtest_parser = sub.add_parser("backtest", help="Backtest Donchian breakout")
    backtest_parser.add_argument("--db-url", default=_cfg("db_url"))
    backtest_parser.add_argument("--symbol", default=_cfg("symbol"))
    backtest_parser.add_argument("--start", default=_cfg("start"))
    backtest_parser.add_argument("--end", default=_cfg("end"))
    backtest_parser.add_argument("--timeframe", default=_cfg("timeframe") or "1H")
    backtest_parser.add_argument("--donchian-period", type=int, default=_cfg("donchian_period") or 20)
    backtest_parser.add_argument("--ema-trend-period", type=int, default=_cfg("ema_trend_period") or 200)
    backtest_parser.add_argument("--atr-period", type=int, default=_cfg("atr_period") or 14)
    backtest_parser.add_argument("--entry-buffer-points", type=float, default=_cfg("entry_buffer_points") or 0.0)
    backtest_parser.add_argument("--capital", type=float, default=_cfg("capital") or 100.0)
    backtest_parser.add_argument("--risk-pct", type=float, default=_cfg("risk_pct") or 0.0)
    backtest_parser.add_argument("--size-from-risk", action="store_true", default=bool(_cfg("size_from_risk")))
    backtest_parser.add_argument("--contract-size", type=float, default=_cfg("contract_size") or 100.0)
    backtest_parser.add_argument("--pip-size", type=float, default=_cfg("pip_size") or 0.1)
    backtest_parser.add_argument("--volume", type=float, default=_cfg("volume") or 0.01)
    backtest_parser.add_argument("--min-volume", type=float, default=_cfg("min_volume") or 0.01)
    backtest_parser.add_argument("--volume-step", type=float, default=_cfg("volume_step") or 0.01)
    backtest_parser.add_argument("--max-positions", type=int, default=_cfg("max_positions") or 1)
    backtest_parser.add_argument("--sl-atr", type=float, default=_cfg("sl_atr") or 2.0)
    backtest_parser.add_argument("--tp-atr", type=float, default=_cfg("tp_atr") or 5.0)
    backtest_parser.add_argument("--trail-trigger-atr", type=float, default=_cfg("trail_trigger_atr") or 2.0)
    backtest_parser.add_argument("--trail-atr-mult", type=float, default=_cfg("trail_atr_mult") or 2.2)
    backtest_parser.add_argument("--breakeven-atr", type=float, default=_cfg("breakeven_atr") or 1.5)
    backtest_parser.add_argument("--breakeven-after-rr", type=float, default=_cfg("breakeven_after_rr"))
    backtest_parser.add_argument("--exit-on-opposite", action="store_true", default=bool(_cfg("exit_on_opposite")))
    backtest_parser.add_argument("--partial-close", action="store_true", default=bool(_cfg("partial_close")))
    backtest_parser.add_argument("--partial-close-atr", type=float, default=_cfg("partial_close_atr") or 2.8)
    backtest_parser.add_argument("--trading-hours", default=_cfg_str("trading_hours"))
    backtest_parser.add_argument("--min-atr-multiplier", type=float, default=_cfg("min_atr_multiplier") or 0.8)
    backtest_parser.add_argument("--max-atr-multiplier", type=float, default=_cfg("max_atr_multiplier") or 3.0)
    backtest_parser.add_argument("--max-spread-points", type=float, default=_cfg("max_spread_points") or 50.0)
    backtest_parser.add_argument("--allowed-deviation-points", type=float, default=_cfg("allowed_deviation_points") or 30.0)
    backtest_parser.add_argument("--slippage-points", type=float, default=_cfg("slippage_points") or 0.0)
    weekend_group = backtest_parser.add_mutually_exclusive_group()
    weekend_group.add_argument("--skip-weekend", dest="skip_weekend", action="store_true", default=bool(_cfg("skip_weekend")))
    weekend_group.add_argument("--no-skip-weekend", dest="skip_weekend", action="store_false")
    backtest_parser.add_argument("--max-daily-loss", type=float, default=_cfg("max_daily_loss"),
                                 help="Giới hạn lỗ tuyệt đối mỗi ngày (USD)")
    backtest_parser.add_argument("--max-loss-streak", type=int, default=_cfg("max_loss_streak"))
    backtest_parser.add_argument("--max-losses-per-session", type=int, default=_cfg("max_losses_per_session"))
    backtest_parser.add_argument("--cooldown-minutes", type=int, default=_cfg("cooldown_minutes"),
                                 help="Số phút tạm dừng trade sau khi vi phạm guard")
    backtest_parser.add_argument("--session-cooldown-minutes", type=int, default=_cfg("session_cooldown_minutes") or 0)

    live_parser = sub.add_parser("live", help="Chạy chiến lược Donchian realtime")
    live_parser.add_argument("--db-url", default=_cfg("db_url"))
    live_parser.add_argument("--symbol", default=_cfg("symbol"))
    live_parser.add_argument("--timeframe", default=_cfg("timeframe") or "1H")
    live_parser.add_argument("--donchian-period", type=int, default=_cfg("donchian_period") or 20)
    live_parser.add_argument("--ema-trend-period", type=int, default=_cfg("ema_trend_period") or 200)
    live_parser.add_argument("--atr-period", type=int, default=_cfg("atr_period") or 14)
    live_parser.add_argument("--entry-buffer-points", type=float, default=_cfg("entry_buffer_points") or 0.0)
    live_parser.add_argument("--capital", type=float, default=_cfg("capital") or 100.0)
    live_parser.add_argument("--risk-pct", type=float, default=_cfg("risk_pct") or 0.0)
    live_parser.add_argument("--size-from-risk", action="store_true", default=bool(_cfg("size_from_risk")))
    live_parser.add_argument("--contract-size", type=float, default=_cfg("contract_size") or 100.0)
    live_parser.add_argument("--pip-size", type=float, default=_cfg("pip_size") or 0.1)
    live_parser.add_argument("--volume", type=float, default=_cfg("volume") or 0.10)
    live_parser.add_argument("--min-volume", type=float, default=_cfg("min_volume") or 0.01)
    live_parser.add_argument("--volume-step", type=float, default=_cfg("volume_step") or 0.01)
    live_parser.add_argument("--max-positions", type=int, default=_cfg("max_positions") or 1)
    live_parser.add_argument("--sl-atr", type=float, default=_cfg("sl_atr") or 2.0)
    live_parser.add_argument("--tp-atr", type=float, default=_cfg("tp_atr") or 3.0)
    live_parser.add_argument("--trail-trigger-atr", type=float, default=_cfg("trail_trigger_atr") or 2.0)
    live_parser.add_argument("--trail-atr-mult", type=float, default=_cfg("trail_atr_mult") or 2.2)
    live_parser.add_argument("--breakeven-atr", type=float, default=_cfg("breakeven_atr") or 1.5)
    live_parser.add_argument("--breakeven-after-rr", type=float, default=_cfg("breakeven_after_rr"))
    live_parser.add_argument("--exit-on-opposite", action="store_true", default=bool(_cfg("exit_on_opposite")))
    live_parser.add_argument("--partial-close", action="store_true", default=bool(_cfg("partial_close")))
    live_parser.add_argument("--partial-close-atr", type=float, default=_cfg("partial_close_atr") or 2.8)
    live_parser.add_argument("--trading-hours", default=_cfg_str("trading_hours"))
    live_parser.add_argument("--min-atr-multiplier", type=float, default=_cfg("min_atr_multiplier") or 0.8)
    live_parser.add_argument("--max-atr-multiplier", type=float, default=_cfg("max_atr_multiplier") or 3.0)
    live_parser.add_argument("--max-spread-points", type=float, default=_cfg("max_spread_points") or 50.0)
    live_parser.add_argument("--allowed-deviation-points", type=float, default=_cfg("allowed_deviation_points") or 30.0)
    live_parser.add_argument("--slippage-points", type=float, default=_cfg("slippage_points") or 0.0)
    weekend_live = live_parser.add_mutually_exclusive_group()
    weekend_live.add_argument("--skip-weekend", dest="skip_weekend", action="store_true", default=bool(_cfg("skip_weekend")))
    weekend_live.add_argument("--no-skip-weekend", dest="skip_weekend", action="store_false")
    live_parser.add_argument("--max-daily-loss", type=float, default=_cfg("max_daily_loss"))
    live_parser.add_argument("--max-loss-streak", type=int, default=_cfg("max_loss_streak"))
    live_parser.add_argument("--max-losses-per-session", type=int, default=_cfg("max_losses_per_session"))
    live_parser.add_argument("--cooldown-minutes", type=int, default=_cfg("cooldown_minutes"))
    live_parser.add_argument("--session-cooldown-minutes", type=int, default=_cfg("session_cooldown_minutes") or 0)
    live_parser.add_argument("--poll", type=float, default=_cfg("poll") or 1.0)
    live_parser.add_argument("--order-retry-times", type=int, default=_cfg("order_retry_times") or 1)
    live_parser.add_argument("--order-retry-delay-ms", type=int, default=_cfg("order_retry_delay_ms") or 0)
    live_parser.add_argument("--magic-number", type=int, default=_cfg("magic_number") or 20251230)
    live_parser.add_argument("--live", action="store_true", default=bool(_cfg("live")), help="Bật gửi lệnh MT5 thật")
    live_parser.add_argument("--ensure-history-hours", type=float, default=_cfg("ensure_history_hours") or 0.0)
    live_parser.add_argument("--history-batch", type=int, default=_cfg("history_batch") or 2000)
    live_parser.add_argument("--history-max-days", type=int, default=_cfg("history_max_days") or 30)
    live_parser.add_argument("--ingest-live-db", action="store_true", default=False, help="Lưu ticks vào DB khi chạy live")

    args = parser.parse_args()

    if args.command == "fetch-history":
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
    elif args.command == "backtest":
        asyncio.run(
            backtest.run_backtest(
                db_url=args.db_url,
                symbol=args.symbol,
                start_str=args.start,
                end_str=args.end,
                timeframe=args.timeframe,
                donchian_period=args.donchian_period,
                ema_trend_period=args.ema_trend_period,
                atr_period=args.atr_period,
                entry_buffer_points=args.entry_buffer_points,
                capital=args.capital,
                risk_pct=args.risk_pct,
                size_from_risk=args.size_from_risk,
                contract_size=args.contract_size,
                pip_size=args.pip_size,
                volume=args.volume,
                min_volume=args.min_volume,
                volume_step=args.volume_step,
                max_positions=args.max_positions,
                sl_atr=args.sl_atr,
                tp_atr=args.tp_atr,
                trail_trigger_atr=args.trail_trigger_atr,
                trail_atr_mult=args.trail_atr_mult,
                breakeven_atr=args.breakeven_atr,
                breakeven_after_rr=args.breakeven_after_rr,
                exit_on_opposite=args.exit_on_opposite,
                partial_close=args.partial_close,
                partial_close_atr=args.partial_close_atr,
                trading_hours=args.trading_hours,
                min_atr_multiplier=args.min_atr_multiplier,
                max_atr_multiplier=args.max_atr_multiplier,
                max_spread_points=args.max_spread_points,
                allowed_deviation_points=args.allowed_deviation_points,
                slippage_points=args.slippage_points,
                skip_weekend=args.skip_weekend,
                max_daily_loss=args.max_daily_loss,
                max_loss_streak=args.max_loss_streak,
                max_losses_per_session=args.max_losses_per_session,
                cooldown_minutes=args.cooldown_minutes,
                session_cooldown_minutes=args.session_cooldown_minutes,
            )
        )
    elif args.command == "live":
        asyncio.run(
            live.run_live_strategy(
                db_url=args.db_url,
                symbol=args.symbol,
                timeframe=args.timeframe,
                donchian_period=args.donchian_period,
                ema_trend_period=args.ema_trend_period,
                atr_period=args.atr_period,
                entry_buffer_points=args.entry_buffer_points,
                capital=args.capital,
                risk_pct=args.risk_pct,
                size_from_risk=args.size_from_risk,
                contract_size=args.contract_size,
                pip_size=args.pip_size,
                volume=args.volume,
                min_volume=args.min_volume,
                volume_step=args.volume_step,
                max_positions=args.max_positions,
                sl_atr=args.sl_atr,
                tp_atr=args.tp_atr,
                trail_trigger_atr=args.trail_trigger_atr,
                trail_atr_mult=args.trail_atr_mult,
                breakeven_atr=args.breakeven_atr,
                breakeven_after_rr=args.breakeven_after_rr,
                exit_on_opposite=args.exit_on_opposite,
                partial_close=args.partial_close,
                partial_close_atr=args.partial_close_atr,
                trading_hours=args.trading_hours,
                min_atr_multiplier=args.min_atr_multiplier,
                max_atr_multiplier=args.max_atr_multiplier,
                max_spread_points=args.max_spread_points,
                allowed_deviation_points=args.allowed_deviation_points,
                slippage_points=args.slippage_points,
                skip_weekend=args.skip_weekend,
                max_daily_loss=args.max_daily_loss,
                max_loss_streak=args.max_loss_streak,
                max_losses_per_session=args.max_losses_per_session,
                cooldown_minutes=args.cooldown_minutes,
                session_cooldown_minutes=args.session_cooldown_minutes,
                poll=args.poll,
                live=args.live,
                order_retry_times=args.order_retry_times,
                order_retry_delay_ms=args.order_retry_delay_ms,
                magic_number=args.magic_number,
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
