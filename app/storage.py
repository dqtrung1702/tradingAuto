from typing import Optional, List, Dict, Any
import os
import json
import hashlib
from datetime import datetime, timezone, timedelta

from sqlalchemy import (
    MetaData,
    Table,
    Column,
    Integer,
    BigInteger,
    Float,
    String,
    JSON,
    UniqueConstraint,
    select,
    DateTime,
    delete,
    func,
    Boolean,
)
from sqlalchemy.ext.asyncio import create_async_engine, AsyncEngine
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.dialects.sqlite import insert as sqlite_insert

DATABASE_URL_ENV = "DATABASE_URL"

metadata = MetaData()

ticks_table = Table(
    "ticks",
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("symbol", String(64), nullable=False),
    Column("time_msc", BigInteger, nullable=False, index=True),
    Column("datetime", String(32), nullable=False, index=True),  # ISO format for timezone support
    Column("bid", Float),
    Column("ask", Float),
    Column("last", Float),
    UniqueConstraint("symbol", "time_msc", name="uq_ticks_symbol_time"),
)

trades_table = Table(
    "trades",
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("event_type", String(32), nullable=True),
    Column("symbol", String(64), nullable=True, index=True),
    Column("side", String(8)),
    Column("price", Float),
    Column("open_price", Float, nullable=True),
    Column("close_price", Float, nullable=True),
    Column("volume", Float, nullable=True),
    Column("stop_loss", Float, nullable=True),
    Column("take_profit", Float, nullable=True),
    Column("pnl_points", Float, nullable=True),
    Column("pnl_value", Float, nullable=True),
    Column("pnl", Float),
    Column("openid", Integer, nullable=True, index=True),
    Column("time_msc", BigInteger, index=True),
    Column("vntime", DateTime(timezone=True), nullable=True, index=True),
)

run_configs_table = Table(
    "run_configs",
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("started_at", DateTime(timezone=True), nullable=False, index=True),
    Column("config", JSON, nullable=False),
)

backtest_trades_table = Table(
    "backtest_trades",
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("saved_config_id", Integer, nullable=True, index=True),
    Column("run_id", String(64), nullable=False, index=True),
    Column("symbol", String(64), nullable=False, index=True),
    Column("preset", String(64), nullable=True),
    Column("side", String(8), nullable=False),
    Column("entry_time", DateTime(timezone=True), nullable=False),
    Column("exit_time", DateTime(timezone=True), nullable=False),
    Column("entry_price", Float, nullable=False),
    Column("exit_price", Float, nullable=False),
    Column("stop_loss", Float),
    Column("take_profit", Float),
    Column("volume", Float),
    Column("pnl", Float),
    Column("pct", Float),
    Column("usd_pnl", Float),
    Column("run_start", DateTime(timezone=True), nullable=False, index=True),
    Column("run_end", DateTime(timezone=True), nullable=False),
    Column("created_at", DateTime(timezone=True), nullable=False, index=True),
)

saved_backtests_table = Table(
    "saved_backtests",
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("created_at", DateTime(timezone=True), nullable=False, index=True),
    Column("last_run_at", DateTime(timezone=True), nullable=False, index=True),
    Column("note", String(255)),
    Column("config_hash", String(128), nullable=True, index=True),
    Column("symbol", String(64), nullable=True, index=True),
    Column("preset", String(64), nullable=True),
    Column("fast", Integer, nullable=True),
    Column("slow", Integer, nullable=True),
    Column("ma_type", String(16), nullable=True),
    Column("timeframe", String(16), nullable=True),
    Column("trend", Integer, nullable=True),
    Column("risk_pct", Float, nullable=True),
    Column("capital", Float, nullable=True),
    Column("trail_trigger_atr", Float, nullable=True),
    Column("trail_atr_mult", Float, nullable=True),
    Column("spread_atr_max", Float, nullable=True),
    Column("reverse_exit", Boolean, nullable=True),
    Column("market_state_window", Integer, nullable=True),
    Column("sl_atr", Float, nullable=True),
    Column("tp_atr", Float, nullable=True),
    Column("volume", Float, nullable=True),
    Column("contract_size", Float, nullable=True),
    Column("sl_pips", Float, nullable=True),
    Column("tp_pips", Float, nullable=True),
    Column("pip_size", Float, nullable=True),
    Column("size_from_risk", Boolean, nullable=True),
    Column("momentum_type", String(32), nullable=True),
    Column("momentum_window", Integer, nullable=True),
    Column("momentum_threshold", Float, nullable=True),
    Column("macd_fast", Integer, nullable=True),
    Column("macd_slow", Integer, nullable=True),
    Column("macd_signal", Integer, nullable=True),
    Column("macd_threshold", Float, nullable=True),
    Column("range_lookback", Integer, nullable=True),
    Column("range_min_atr", Float, nullable=True),
    Column("range_min_points", Float, nullable=True),
    Column("breakout_buffer_atr", Float, nullable=True),
    Column("breakout_confirmation_bars", Integer, nullable=True),
    Column("atr_baseline_window", Integer, nullable=True),
    Column("atr_multiplier_min", Float, nullable=True),
    Column("atr_multiplier_max", Float, nullable=True),
    Column("trading_hours", String(255), nullable=True),
    Column("adx_window", Integer, nullable=True),
    Column("adx_threshold", Float, nullable=True),
    Column("rsi_threshold_long", Float, nullable=True),
    Column("rsi_threshold_short", Float, nullable=True),
    Column("max_daily_loss", Float, nullable=True),
    Column("max_loss_streak", Integer, nullable=True),
    Column("max_losses_per_session", Integer, nullable=True),
    Column("cooldown_minutes", Integer, nullable=True),
    Column("allow_buy", Boolean, nullable=True),
    Column("allow_sell", Boolean, nullable=True),
    Column("max_holding_minutes", Integer, nullable=True),
    Column("ensure_history_hours", Integer, nullable=True),
    Column("poll", Integer, nullable=True),
    Column("live", Boolean, nullable=True),
    Column("ingest_live_db", Boolean, nullable=True),
    Column("history_batch", Integer, nullable=True),
    Column("history_max_days", Integer, nullable=True),
    UniqueConstraint("config_hash", name="uq_saved_backtests_config_hash"),
)

# Danh sách cột cấu hình backtest cần lưu phẳng (không JSON)
SAVED_BACKTEST_CONFIG_FIELDS: List[str] = [
    "symbol",
    "preset",
    "fast",
    "slow",
    "ma_type",
    "timeframe",
    "trend",
    "risk_pct",
    "capital",
    "trail_trigger_atr",
    "trail_atr_mult",
    "spread_atr_max",
    "reverse_exit",
    "market_state_window",
    "sl_atr",
    "tp_atr",
    "volume",
    "contract_size",
    "sl_pips",
    "tp_pips",
    "pip_size",
    "size_from_risk",
    "momentum_type",
    "momentum_window",
    "momentum_threshold",
    "macd_fast",
    "macd_slow",
    "macd_signal",
    "macd_threshold",
    "range_lookback",
    "range_min_atr",
    "range_min_points",
    "breakout_buffer_atr",
    "breakout_confirmation_bars",
    "atr_baseline_window",
    "atr_multiplier_min",
    "atr_multiplier_max",
    "trading_hours",
    "adx_window",
    "adx_threshold",
    "rsi_threshold_long",
    "rsi_threshold_short",
    "max_daily_loss",
    "max_loss_streak",
    "max_losses_per_session",
    "cooldown_minutes",
    "max_holding_minutes",
    "allow_buy",
    "allow_sell",
    "ensure_history_hours",
    "poll",
    "live",
    "ingest_live_db",
    "history_batch",
    "history_max_days",
]


def flatten_backtest_config(config: Dict[str, Any]) -> Dict[str, Any]:
    """Tuỳ biến config dict thành dict phẳng theo danh sách cột."""

    def _coerce(value: Any) -> Any:
        if isinstance(value, str):
            return value
        if isinstance(value, bool):
            return bool(value)
        if value is None:
            return None
        try:
            if isinstance(value, int):
                return int(value)
            if isinstance(value, float):
                return float(value)
            if isinstance(value, (list, tuple)):
                return ",".join(str(v) for v in value)
        except Exception:
            return value
        return value

    flattened: Dict[str, Any] = {}
    for field in SAVED_BACKTEST_CONFIG_FIELDS:
        # Hỗ trợ alias fast_ma/slow_ma/trend_ma nếu được cung cấp
        if field == "fast":
            flattened[field] = _coerce(config.get("fast", config.get("fast_ma")))
        elif field == "slow":
            flattened[field] = _coerce(config.get("slow", config.get("slow_ma")))
        elif field == "trend":
            flattened[field] = _coerce(config.get("trend", config.get("trend_ma")))
        elif field == "trading_hours":
            raw = config.get("trading_hours")
            if isinstance(raw, str):
                flattened[field] = raw
            elif isinstance(raw, (list, tuple)):
                flattened[field] = ",".join(str(v) for v in raw)
            else:
                flattened[field] = None
        else:
            flattened[field] = _coerce(config.get(field))
    return flattened


def compute_config_hash(flat_config: Dict[str, Any]) -> str:
    normalized = {k: flat_config.get(k) for k in sorted(SAVED_BACKTEST_CONFIG_FIELDS)}
    data = json.dumps(normalized, sort_keys=True, default=str, separators=(",", ":"))
    return hashlib.sha1(data.encode("utf-8")).hexdigest()


def _get_db_url(provided: Optional[str] = None) -> str:
    if provided:
        return provided
    env_val = os.getenv(DATABASE_URL_ENV)
    if not env_val:
        raise RuntimeError("DATABASE_URL not provided and env var DATABASE_URL is empty")
    return env_val


class Storage:
    """Async storage wrapper that manages a single shared engine and provides batch inserts.

    Usage:
        s = Storage(db_url)
        await s.init()
        await s.insert_ticks_batch([...])
        await s.insert_trade(...)
        await s.close()
    """

    def __init__(self, db_url: Optional[str] = None):
        self._db_url = _get_db_url(db_url) if db_url or os.getenv(DATABASE_URL_ENV) else None
        if not self._db_url:
            raise RuntimeError("DATABASE_URL must be provided via env or constructor")
        self._engine: Optional[AsyncEngine] = None

    def _create_engine(self) -> AsyncEngine:
        return create_async_engine(self._db_url, future=True)

    async def init(self) -> None:
        if self._engine is None:
            self._engine = self._create_engine()
        async with self._engine.begin() as conn:
            # create tables if not exist
            await conn.run_sync(metadata.create_all)

    async def ensure_initialized(self) -> None:
        if self._engine is None:
            await self.init()

    @property
    def engine(self) -> AsyncEngine:
        if not self._engine:
            raise RuntimeError("Storage not initialized")
        return self._engine

    async def close(self) -> None:
        if self._engine is not None:
            await self._engine.dispose()
            self._engine = None

    async def insert_ticks_batch(self, rows: List[Dict], batch_size: int = 1000, quiet: bool = False) -> None:
        """Insert ticks in batches. Each row is dict matching ticks_table columns."""
        if not self._engine:
            raise RuntimeError("Storage not initialized")

        if rows:
            for row in rows:
                _ensure_datetime(row)
        if rows and not quiet:
            print("\nVerifying first row before DB insert:")
            first_row = rows[0]
            for k, v in first_row.items():
                print(f"  {k}: {v} (type: {type(v)})")

        dialect_name = self._engine.dialect.name
        async with self._engine.begin() as conn:
            for i in range(0, len(rows), batch_size):
                batch = rows[i : i + batch_size]
                if not quiet:
                    print(f"\nInserting batch {i//batch_size + 1} ({len(batch)} rows)...")
                # Validate numeric values in batch
                for row in batch:
                    _ensure_datetime(row)
                    if not isinstance(row['time_msc'], int):
                        row['time_msc'] = int(row['time_msc'])
                    if row['bid'] is not None:
                        row['bid'] = float(row['bid'])
                    if row['ask'] is not None:
                        row['ask'] = float(row['ask'])
                    if row['last'] is not None:
                        row['last'] = float(row['last'])
                if dialect_name == "postgresql":
                    stmt = pg_insert(ticks_table).on_conflict_do_nothing(index_elements=["symbol", "time_msc"])
                    await conn.execute(stmt, batch)
                elif dialect_name == "sqlite":
                    stmt = sqlite_insert(ticks_table).on_conflict_do_nothing(index_elements=["symbol", "time_msc"])
                    await conn.execute(stmt, batch)
                else:
                    await conn.execute(ticks_table.insert(), batch)

    async def insert_trade(
        self,
        *,
        side: str,
        price: float,
        time_msc: int,
        pnl: Optional[float] = None,
        event_type: Optional[str] = None,
        symbol: Optional[str] = None,
        volume: Optional[float] = None,
        stop_loss: Optional[float] = None,
        take_profit: Optional[float] = None,
        open_price: Optional[float] = None,
        close_price: Optional[float] = None,
        pnl_points: Optional[float] = None,
        pnl_value: Optional[float] = None,
        openid: Optional[int] = None,
    ) -> Optional[int]:
        """Insert a trade event with explicit columns instead of JSON meta."""
        if not self._engine:
            raise RuntimeError("Storage not initialized")

        ts_utc = datetime.fromtimestamp(int(time_msc) / 1000, tz=timezone.utc)
        vn_tz = timezone(timedelta(hours=7))
        vn_dt = ts_utc.astimezone(vn_tz)

        record: Dict[str, Any] = {
            "event_type": event_type,
            "symbol": symbol,
            "side": side,
            "price": price,
            "open_price": open_price,
            "close_price": close_price,
            "volume": volume,
            "stop_loss": stop_loss,
            "take_profit": take_profit,
            "pnl_points": pnl_points,
            "pnl_value": pnl_value,
            "pnl": pnl,
            "openid": openid,
            "time_msc": int(time_msc),
            "vntime": vn_dt,
        }

        async with self._engine.begin() as conn:
            result = await conn.execute(trades_table.insert().values(**record))
            inserted_pk = None
            if hasattr(result, "inserted_primary_key") and result.inserted_primary_key:
                inserted_pk = result.inserted_primary_key[0]
        return int(inserted_pk) if inserted_pk is not None else None

    async def insert_run_config(self, started_at: datetime, config: Dict[str, Any]) -> None:
        if not self._engine:
            raise RuntimeError("Storage not initialized")
        ts = started_at.astimezone(timezone.utc)
        async with self._engine.begin() as conn:
            await conn.execute(run_configs_table.insert().values(started_at=ts, config=config))

    async def insert_backtest_trades(self, rows: List[Dict[str, Any]]) -> None:
        if not rows:
            return
        if not self._engine:
            raise RuntimeError("Storage not initialized")
        normalized: List[Dict[str, Any]] = []
        for row in rows:
            norm = dict(row)
            for key in ("entry_time", "exit_time", "run_start", "run_end", "created_at"):
                if key in norm and isinstance(norm[key], datetime):
                    if norm[key].tzinfo is None:
                        norm[key] = norm[key].replace(tzinfo=timezone.utc)
                    else:
                        norm[key] = norm[key].astimezone(timezone.utc)
            normalized.append(norm)
        async with self._engine.begin() as conn:
            await conn.execute(backtest_trades_table.insert(), normalized)

    async def insert_saved_backtest(self, config: Dict[str, Any], note: Optional[str] = None) -> int:
        if not self._engine:
            raise RuntimeError("Storage not initialized")

        flat_cfg = flatten_backtest_config(config)
        cfg_hash = compute_config_hash(flat_cfg)
        now = datetime.now(timezone.utc)

        if not flat_cfg.get("symbol"):
            raise ValueError("Config backtest thiếu 'symbol'")

        async with self._engine.begin() as conn:
            # Nếu đã có cấu hình cùng hash -> chỉ cập nhật last_run_at (và note nếu gửi mới)
            existing = await conn.execute(
                select(saved_backtests_table.c.id, saved_backtests_table.c.note).where(
                    saved_backtests_table.c.config_hash == cfg_hash
                )
            )
            row = existing.first()
            if row:
                update_values: Dict[str, Any] = {"last_run_at": now}
                if note is not None:
                    update_values["note"] = note
                await conn.execute(
                    saved_backtests_table.update()
                    .where(saved_backtests_table.c.id == row.id)
                    .values(**update_values)
                )
                return int(row.id)

            record: Dict[str, Any] = {
                **flat_cfg,
                "config_hash": cfg_hash,
                "created_at": now,
                "last_run_at": now,
                "note": note,
            }
            result = await conn.execute(saved_backtests_table.insert().values(**record))
            inserted_pk = None
            if hasattr(result, "inserted_primary_key") and result.inserted_primary_key:
                inserted_pk = result.inserted_primary_key[0]
        if inserted_pk is None:
            raise RuntimeError("Không thể xác định ID saved_backtests mới")
        return int(inserted_pk)

    async def delete_backtest_trades_in_range(
        self,
        symbol: str,
        start_dt: datetime,
        end_dt: datetime,
        preset: Optional[str] = None,
        saved_config_id: Optional[int] = None,
    ) -> int:
        if not self._engine:
            raise RuntimeError("Storage not initialized")

        def _normalize(dt: datetime) -> datetime:
            if dt.tzinfo is None:
                return dt.replace(tzinfo=timezone.utc)
            return dt.astimezone(timezone.utc)

        start_dt = _normalize(start_dt)
        end_dt = _normalize(end_dt)
        conditions = [
            backtest_trades_table.c.symbol == symbol,
            backtest_trades_table.c.entry_time >= start_dt,
            backtest_trades_table.c.entry_time < end_dt,
        ]
        if preset:
            conditions.append(backtest_trades_table.c.preset == preset)
        if saved_config_id is not None:
            conditions.append(backtest_trades_table.c.saved_config_id == saved_config_id)
        async with self._engine.begin() as conn:
            result = await conn.execute(delete(backtest_trades_table).where(*conditions))
            return result.rowcount if hasattr(result, "rowcount") else 0

    async def has_ticks_since(self, symbol: str, since_time_msc: int) -> bool:
        await self.ensure_initialized()
        stmt = (
            select(ticks_table.c.id)
            .where(ticks_table.c.symbol == symbol, ticks_table.c.time_msc >= since_time_msc)
            .limit(1)
        )
        async with self.engine.connect() as conn:
            result = await conn.execute(stmt)
            return result.first() is not None

    async def latest_tick_msc(self, symbol: str) -> Optional[int]:
        """Lấy timestamp (ms) của tick mới nhất trong DB cho symbol."""
        await self.ensure_initialized()
        stmt = select(func.max(ticks_table.c.time_msc)).where(ticks_table.c.symbol == symbol)
        async with self.engine.connect() as conn:
            result = await conn.execute(stmt)
            value = result.scalar()
        return int(value) if value is not None else None

    async def fetch_ticks_range(self, symbol: str, start_msc: int, end_msc: int) -> List[Dict]:
        await self.ensure_initialized()
        stmt = (
            select(
                ticks_table.c.symbol,
                ticks_table.c.time_msc,
                ticks_table.c.datetime,
                ticks_table.c.bid,
                ticks_table.c.ask,
                ticks_table.c.last,
            )
            .where(
                ticks_table.c.symbol == symbol,
                ticks_table.c.time_msc >= start_msc,
                ticks_table.c.time_msc <= end_msc,
            )
            .order_by(ticks_table.c.time_msc)
        )
        async with self.engine.connect() as conn:
            result = await conn.execute(stmt)
            rows = result.fetchall()
        return [dict(row._mapping) for row in rows]

    async def fetch_backtest_trades(
        self,
        symbol: str,
        start_dt: datetime,
        end_dt: datetime,
        saved_config_id: Optional[int] = None,
    ) -> List[Dict[str, Any]]:
        """Lấy các backtest_trades trong khoảng thời gian."""
        await self.ensure_initialized()

        def _normalize(dt: datetime) -> datetime:
            if dt.tzinfo is None:
                return dt.replace(tzinfo=timezone.utc)
            return dt.astimezone(timezone.utc)

        start_dt = _normalize(start_dt)
        end_dt = _normalize(end_dt)
        conditions = [
            backtest_trades_table.c.symbol == symbol,
            backtest_trades_table.c.exit_time >= start_dt,
            backtest_trades_table.c.exit_time <= end_dt,
        ]
        if saved_config_id:
            conditions.append(backtest_trades_table.c.saved_config_id == saved_config_id)
        stmt = (
            select(
                backtest_trades_table.c.run_id,
                backtest_trades_table.c.symbol,
                backtest_trades_table.c.preset,
                backtest_trades_table.c.saved_config_id,
                backtest_trades_table.c.side,
                backtest_trades_table.c.entry_time,
                backtest_trades_table.c.exit_time,
                backtest_trades_table.c.entry_price,
                backtest_trades_table.c.exit_price,
                backtest_trades_table.c.stop_loss,
                backtest_trades_table.c.take_profit,
                backtest_trades_table.c.volume,
                backtest_trades_table.c.pnl,
                backtest_trades_table.c.pct,
                backtest_trades_table.c.usd_pnl,
                backtest_trades_table.c.run_start,
                backtest_trades_table.c.run_end,
            )
            .where(*conditions)
            .order_by(backtest_trades_table.c.exit_time)
        )
        async with self.engine.connect() as conn:
            result = await conn.execute(stmt)
            rows = result.fetchall()
        return [dict(row._mapping) for row in rows]


def _ensure_datetime(row: Dict) -> None:
    if row.get('datetime'):
        return
    ts = row.get('time_msc')
    try:
        ts_int = int(ts) if ts is not None else None
    except (TypeError, ValueError):
        ts_int = None
    if ts_int is None or ts_int <= 0:
        row['datetime'] = datetime.now(timezone.utc).isoformat()
        return
    row['datetime'] = datetime.fromtimestamp(ts_int / 1000, tz=timezone.utc).isoformat()
