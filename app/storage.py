from typing import Optional, List, Dict, Any
import os
from datetime import datetime, timezone

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
    Column("side", String(8)),
    Column("price", Float),
    Column("time_msc", BigInteger),
    Column("pnl", Float),
    Column("meta", JSON, nullable=True),
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

    async def insert_trade(self, side: str, price: float, time_msc: int, pnl: Optional[float] = None, meta: Optional[dict] = None) -> None:
        if not self._engine:
            raise RuntimeError("Storage not initialized")
        async with self._engine.begin() as conn:
            await conn.execute(trades_table.insert().values(side=side, price=price, time_msc=time_msc, pnl=pnl, meta=meta))

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
