"""Migration helper để thêm cột mới cho bảng trades và tính vntime.

Chạy script này với biến môi trường DATABASE_URL đã cấu hình giống ứng dụng:
    python scripts/migrate_trades_table.py

Hỗ trợ PostgreSQL và SQLite (3.35+ để DROP COLUMN). Nếu không drop được meta,
script sẽ báo để bạn tự xử lý.
"""

import asyncio
import os
import sys
from pathlib import Path
from datetime import datetime, timezone, timedelta
from typing import List, Set

from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError


# Đảm bảo thêm project root vào sys.path khi chạy trực tiếp
CURRENT_DIR = Path(__file__).resolve().parent
ROOT_DIR = CURRENT_DIR.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from app.storage import (
    Storage,
    flatten_backtest_config,
    compute_config_hash,
    SAVED_BACKTEST_CONFIG_FIELDS,
)


NEW_COLUMNS = {
    "event_type": "VARCHAR(32)",
    "symbol": "VARCHAR(64)",
    "open_price": "FLOAT",
    "close_price": "FLOAT",
    "volume": "FLOAT",
    "stop_loss": "FLOAT",
    "take_profit": "FLOAT",
    "pnl_points": "FLOAT",
    "pnl_value": "FLOAT",
    "openid": "INTEGER",
    "vntime": None,  # set per dialect
}

BACKTEST_BT_COLUMNS = {
    "saved_config_id": "INTEGER",
}

SAVED_BT_COLUMNS = {
    "config_hash": "VARCHAR(128)",
    "last_run_at": None,  # datetime per dialect
    "symbol": "VARCHAR(64)",
    "fast": "INTEGER",
    "slow": "INTEGER",
    "ma_type": "VARCHAR(16)",
    "timeframe": "VARCHAR(16)",
    "trend": "INTEGER",
    "risk_pct": "FLOAT",
    "capital": "FLOAT",
    "trail_trigger_atr": "FLOAT",
    "trail_atr_mult": "FLOAT",
    "spread_atr_max": "FLOAT",
    "reverse_exit": "BOOLEAN",
    "market_state_window": "INTEGER",
    "sl_atr": "FLOAT",
    "tp_atr": "FLOAT",
    "volume": "FLOAT",
    "contract_size": "FLOAT",
    "sl_pips": "FLOAT",
    "tp_pips": "FLOAT",
    "pip_size": "FLOAT",
    "size_from_risk": "BOOLEAN",
    "momentum_type": "VARCHAR(32)",
    "momentum_window": "INTEGER",
    "momentum_threshold": "FLOAT",
    "macd_fast": "INTEGER",
    "macd_slow": "INTEGER",
    "macd_signal": "INTEGER",
    "macd_threshold": "FLOAT",
    "range_lookback": "INTEGER",
    "range_min_atr": "FLOAT",
    "range_min_points": "FLOAT",
    "breakout_buffer_atr": "FLOAT",
    "breakout_confirmation_bars": "INTEGER",
    "atr_baseline_window": "INTEGER",
    "atr_multiplier_min": "FLOAT",
    "atr_multiplier_max": "FLOAT",
    "trading_hours": "VARCHAR(255)",
    "adx_window": "INTEGER",
    "adx_threshold": "FLOAT",
    "rsi_threshold_long": "FLOAT",
    "rsi_threshold_short": "FLOAT",
    "max_daily_loss": "FLOAT",
    "max_loss_streak": "INTEGER",
    "max_losses_per_session": "INTEGER",
    "cooldown_minutes": "INTEGER",
    "max_holding_minutes": "INTEGER",
    "allow_buy": "BOOLEAN",
    "allow_sell": "BOOLEAN",
    "ensure_history_hours": "INTEGER",
    "poll": "INTEGER",
    "live": "BOOLEAN",
    "ingest_live_db": "BOOLEAN",
    "history_batch": "INTEGER",
    "history_max_days": "INTEGER",
    "order_retry_times": "INTEGER",
    "order_retry_delay_ms": "INTEGER",
    "safety_entry_atr_mult": "FLOAT",
    "spread_samples": "INTEGER",
    "spread_sample_delay_ms": "INTEGER",
    "allowed_deviation_points": "INTEGER",
    "volatility_spike_atr_mult": "FLOAT",
    "spike_delay_ms": "INTEGER",
    "skip_reset_window": "BOOLEAN",
    "latency_min_ms": "INTEGER",
    "latency_max_ms": "INTEGER",
    "slippage_usd": "FLOAT",
    "order_reject_prob": "FLOAT",
    "base_spread_points": "INTEGER",
    "spread_spike_chance": "FLOAT",
    "spread_spike_min_points": "INTEGER",
    "spread_spike_max_points": "INTEGER",
    "slip_per_atr_ratio": "FLOAT",
    "requote_prob": "FLOAT",
    "offquotes_prob": "FLOAT",
    "timeout_prob": "FLOAT",
    "stop_hunt_chance": "FLOAT",
    "stop_hunt_min_atr_ratio": "FLOAT",
    "stop_hunt_max_atr_ratio": "FLOAT",
    "missing_tick_chance": "FLOAT",
    "min_volume_multiplier": "FLOAT",
    "slippage_pips": "FLOAT",
}


def _ensure_async_url(url: str) -> str:
    if not url:
        return url
    lowered = url.lower()
    if lowered.startswith("postgres://"):
        return "postgresql+asyncpg://" + url.split("://", 1)[1]
    if lowered.startswith("postgresql://"):
        return "postgresql+asyncpg://" + url.split("://", 1)[1]
    if "+psycopg2" in lowered:
        return url.replace("+psycopg2", "+asyncpg")
    return url


async def _get_existing_columns(conn, dialect: str, table: str) -> Set[str]:
    if dialect == "sqlite":
        result = await conn.execute(text(f"PRAGMA table_info({table})"))
        rows = result.fetchall()
        return {row[1] for row in rows}
    result = await conn.execute(
        text(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_name = :table
            AND table_schema = current_schema()
            """
        ),
        {"table": table},
    )
    rows = result.fetchall()
    return {row[0] for row in rows}


async def _add_missing_columns(conn, dialect: str, existing: Set[str]) -> None:
    dt_type = "TIMESTAMP WITH TIME ZONE" if dialect == "postgresql" else "DATETIME"
    for name, type_sql in NEW_COLUMNS.items():
        if name in existing:
            continue
        column_type = type_sql or dt_type
        ddl = f"ALTER TABLE trades ADD COLUMN {name} {column_type}"
        print(f"Adding column {name} ({column_type}) ...")
        await conn.execute(text(ddl))


async def _drop_meta(conn, dialect: str, existing: Set[str]) -> None:
    if "meta" not in existing:
        return
    try:
        print("Dropping column meta ...")
        await conn.execute(text("ALTER TABLE trades DROP COLUMN meta"))
    except SQLAlchemyError as exc:
        print("Không thể DROP COLUMN meta tự động, vui lòng drop thủ công nếu cần. Lỗi:", exc)


async def _backfill_vntime(conn, existing: Set[str]) -> None:
    if "vntime" not in existing or "time_msc" not in existing:
        return
    result = await conn.execute(text("SELECT id, time_msc FROM trades WHERE vntime IS NULL"))
    rows = result.fetchall()
    if not rows:
        return
    vn_tz = timezone(timedelta(hours=7))
    updates: List[dict] = []
    for row in rows:
        time_msc = row.time_msc
        if time_msc is None:
            continue
        ts_utc = datetime.fromtimestamp(int(time_msc) / 1000, tz=timezone.utc)
        updates.append({"id": row.id, "vntime": ts_utc.astimezone(vn_tz)})
    if updates:
        print(f"Backfilling vntime cho {len(updates)} dòng ...")
        await conn.execute(text("UPDATE trades SET vntime = :vntime WHERE id = :id"), updates)


async def _add_saved_bt_columns(conn, dialect: str, existing: Set[str]) -> None:
    dt_type = "TIMESTAMP WITH TIME ZONE" if dialect == "postgresql" else "DATETIME"
    for name, type_sql in SAVED_BT_COLUMNS.items():
        if name in existing:
            continue
        column_type = type_sql or dt_type
        ddl = f"ALTER TABLE saved_backtests ADD COLUMN {name} {column_type}"
        print(f"Adding saved_backtests column {name} ({column_type}) ...")
        await conn.execute(text(ddl))


async def _add_backtest_trades_columns(conn, dialect: str, existing: Set[str]) -> None:
    for name, type_sql in BACKTEST_BT_COLUMNS.items():
        if name in existing:
            continue
        column_type = type_sql
        ddl = f"ALTER TABLE backtest_trades ADD COLUMN {name} {column_type}"
        print(f"Adding backtest_trades column {name} ({column_type}) ...")
        await conn.execute(text(ddl))


async def _backfill_saved_configs(conn, dialect: str, existing: Set[str]) -> None:
    if "config" not in existing:
        print("Bỏ qua backfill saved_backtests vì không còn cột config")
        return
    required_cols = set(SAVED_BACKTEST_CONFIG_FIELDS) | {"config_hash", "last_run_at"}
    if not required_cols.issubset(existing):
        return
    result = await conn.execute(text("SELECT id, config, created_at FROM saved_backtests"))
    rows = result.fetchall()
    if not rows:
        return
    updates: List[dict] = []
    for row in rows:
        cfg = row.config or {}
        flat = flatten_backtest_config(cfg)
        cfg_hash = compute_config_hash(flat)
        payload = {k: flat.get(k) for k in SAVED_BACKTEST_CONFIG_FIELDS}
        payload.update(
            {
                "config_hash": cfg_hash,
                "last_run_at": row.created_at
                if isinstance(row.created_at, datetime)
                else datetime.now(timezone.utc),
                "id": row.id,
            }
        )
        updates.append(payload)
    if not updates:
        return
    ordered_cols = [c for c in SAVED_BACKTEST_CONFIG_FIELDS if c in required_cols and c != "config_hash"] + [
        "last_run_at"
    ]
    set_clause = ", ".join([f"{col} = :{col}" for col in ordered_cols] + ["config_hash = :config_hash"])
    sql = text(f"UPDATE saved_backtests SET {set_clause} WHERE id = :id")
    print(f"Backfilling saved_backtests cho {len(updates)} dòng ...")
    await conn.execute(sql, updates)


async def _drop_saved_json(conn, dialect: str, existing: Set[str]) -> None:
    for col in ("config", "summary"):
        if col not in existing:
            continue
        try:
            print(f"Dropping column {col} from saved_backtests ...")
            await conn.execute(text(f"ALTER TABLE saved_backtests DROP COLUMN {col}"))
        except SQLAlchemyError as exc:
            print(f"Không thể DROP COLUMN {col} tự động, vui lòng xử lý thủ công nếu cần. Lỗi: {exc}")


async def migrate() -> None:
    db_url = _ensure_async_url(os.getenv("DATABASE_URL"))
    if not db_url:
        raise RuntimeError("DATABASE_URL chưa được set")
    storage = Storage(db_url)
    await storage.ensure_initialized()
    dialect = storage.engine.dialect.name
    print(f"Dialect: {dialect}")

    async with storage.engine.begin() as conn:
        existing_trades = await _get_existing_columns(conn, dialect, "trades")
        print("Cột trades hiện có:", ", ".join(sorted(existing_trades)))

        await _add_missing_columns(conn, dialect, existing_trades)
        # Refresh columns after add
        existing_trades = await _get_existing_columns(conn, dialect, "trades")
        await _drop_meta(conn, dialect, existing_trades)
        await _backfill_vntime(conn, existing_trades)

        existing_saved = await _get_existing_columns(conn, dialect, "saved_backtests")
        print("Cột saved_backtests hiện có:", ", ".join(sorted(existing_saved)))
        await _add_saved_bt_columns(conn, dialect, existing_saved)
        existing_saved = await _get_existing_columns(conn, dialect, "saved_backtests")
        await _backfill_saved_configs(conn, dialect, existing_saved)
        await _drop_saved_json(conn, dialect, existing_saved)

        existing_bt = await _get_existing_columns(conn, dialect, "backtest_trades")
        print("Cột backtest_trades hiện có:", ", ".join(sorted(existing_bt)))
        await _add_backtest_trades_columns(conn, dialect, existing_bt)

    await storage.close()
    print("Done.")


if __name__ == "__main__":
    asyncio.run(migrate())
