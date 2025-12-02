from typing import Optional, List, Dict, Any
import os
import logging
from datetime import datetime, timezone, timedelta

from sqlalchemy import (
    MetaData,
    Table,
    Column,
    Integer,
    BigInteger,
    Float,
    String,
    UniqueConstraint,
    select,
    func,
    cast,
    DateTime,
)
from sqlalchemy.ext.asyncio import create_async_engine, AsyncEngine
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.dialects.sqlite import insert as sqlite_insert

DATABASE_URL_ENV = "DATABASE_URL"

metadata = MetaData()
logger = logging.getLogger(__name__)

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


def _to_minutes(val: str) -> Optional[int]:
    """Convert HH:MM string to minutes; return None on error."""
    try:
        parts = val.split(":")
        h = int(parts[0])
        m = int(parts[1]) if len(parts) > 1 else 0
        return h * 60 + m
    except Exception:
        return None

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
            logger.info("Verifying first row before DB insert")
            first_row = rows[0]
            for k, v in first_row.items():
                logger.info("  %s: %s (type: %s)", k, v, type(v))

        dialect_name = self._engine.dialect.name
        async with self._engine.begin() as conn:
            for i in range(0, len(rows), batch_size):
                batch = rows[i : i + batch_size]
                if not quiet:
                    logger.info("Inserting batch %d (%d rows)...", i // batch_size + 1, len(batch))
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
        raise RuntimeError("Trade table removed")

    async def insert_run_config(self, started_at: datetime, config: Dict[str, Any]) -> None:
        raise RuntimeError("Run config table removed")

    async def insert_backtest_trades(self, rows: List[Dict[str, Any]]) -> None:
        raise RuntimeError("Backtest trades table removed")

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

    async def get_latest_time_msc(self, symbol: str) -> Optional[int]:
        """Lấy time_msc lớn nhất hiện có cho symbol (None nếu chưa có)."""
        await self.ensure_initialized()
        stmt = select(ticks_table.c.time_msc).where(ticks_table.c.symbol == symbol).order_by(ticks_table.c.time_msc.desc()).limit(1)
        async with self.engine.connect() as conn:
            result = await conn.execute(stmt)
            row = result.first()
        if row is None:
            return None
        return int(row[0])

    async def get_time_range_msc(self, symbol: str) -> Optional[tuple[int, int]]:
        """Trả về (min_time_msc, max_time_msc) nếu có dữ liệu; None nếu bảng rỗng cho symbol."""
        await self.ensure_initialized()
        stmt = (
            select(
                ticks_table.c.time_msc.min().label("min_msc"),
                ticks_table.c.time_msc.max().label("max_msc"),
            )
            .where(ticks_table.c.symbol == symbol)
        )
        async with self.engine.connect() as conn:
            result = await conn.execute(stmt)
            row = result.first()
        if not row or row[0] is None or row[1] is None:
            return None
        return int(row[0]), int(row[1])

    async def get_missing_hour_ranges(
        self,
        symbol: str,
        start_dt: datetime,
        end_dt: datetime,
        min_ticks_per_hour: int = 1,
        skip_weekend: bool = True,
        closed_sessions: Optional[List[str]] = None,
    ) -> List[tuple[datetime, datetime]]:
        """Tìm các khoảng giờ trống dữ liệu ticks.

        Trả về danh sách (range_start, range_end) theo UTC, liên tiếp các giờ thiếu được gộp chung.
        """
        await self.ensure_initialized()
        start_dt = start_dt.astimezone(timezone.utc)
        end_dt = end_dt.astimezone(timezone.utc)
        dt_col = cast(ticks_table.c.datetime, DateTime(timezone=True))
        hour_expr = func.date_trunc("hour", dt_col).label("hour_start")
        hour_counts = (
            select(
                hour_expr,
                func.count().label("cnt"),
            )
            .where(
                ticks_table.c.symbol == symbol,
                dt_col >= start_dt,
                dt_col <= end_dt,
            )
            .group_by(hour_expr)
            .subquery("hour_counts")
        )
        stmt = select(hour_counts.c.hour_start, hour_counts.c.cnt)
        hours_with_data: dict[str, int] = {}
        async with self.engine.connect() as conn:
            result = await conn.execute(stmt)
            for row in result.fetchall():
                hour_start = row[0]
                cnt = row[1]
                if isinstance(hour_start, datetime):
                    key = hour_start.replace(tzinfo=None).isoformat()
                else:
                    key = str(hour_start)
                hours_with_data[key] = cnt

        closed_sessions = closed_sessions or []
        missing_ranges: List[tuple[datetime, datetime]] = []
        cur = start_dt.replace(minute=0, second=0, microsecond=0)
        end_floor = end_dt.replace(minute=0, second=0, microsecond=0)
        range_start = None
        while cur <= end_floor:
            # Bỏ qua giờ cuối tuần hoặc khung giờ đóng cửa
            if skip_weekend and cur.weekday() >= 5:
                if range_start is not None:
                    missing_ranges.append((range_start, cur))
                    range_start = None
                cur += timedelta(hours=1)
                continue
            if closed_sessions:
                cur_min = cur.hour * 60 + cur.minute
                closed = False
                for session in closed_sessions:
                    try:
                        start_str, end_str = session.split("-", 1)
                        start_min = _to_minutes(start_str.strip())
                        end_min = _to_minutes(end_str.strip())
                    except ValueError:
                        continue
                    if start_min is None or end_min is None:
                        continue
                    if end_min < start_min:
                        if cur_min >= start_min or cur_min <= end_min:
                            closed = True
                            break
                    else:
                        if start_min <= cur_min <= end_min:
                            closed = True
                            break
                if closed:
                    if range_start is not None:
                        missing_ranges.append((range_start, cur))
                        range_start = None
                    cur += timedelta(hours=1)
                    continue
            key = cur.replace(tzinfo=None).isoformat()
            has_data = hours_with_data.get(key, 0) >= min_ticks_per_hour
            if not has_data and range_start is None:
                range_start = cur
            if has_data and range_start is not None:
                missing_ranges.append((range_start, cur))
                range_start = None
            cur += timedelta(hours=1)
        if range_start is not None:
            missing_ranges.append((range_start, end_floor + timedelta(hours=1)))
        return missing_ranges


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
