"""Các lệnh làm việc với dữ liệu lịch sử từ MetaTrader5."""

from __future__ import annotations

import asyncio
from datetime import datetime, timezone, timedelta
from time import sleep
from typing import List
from functools import partial
import logging

try:  # MetaTrader5 là dependency tuỳ chọn
    import MetaTrader5 as mt5  # type: ignore
except Exception:  # pragma: no cover - MT5 không khả dụng
    mt5 = None  # type: ignore

from app.storage import Storage
from app.config import DEFAULT_DONCHIAN_PARAMS

logger = logging.getLogger(__name__)


def parse_mt5_tick(raw_tick) -> dict:
    fields = ["time", "bid", "ask", "last", "volume", "time_msc", "flags", "volume_real"]
    values = dict(zip(fields, raw_tick))

    time_msc = int(values["time_msc"]) if values.get("time_msc") else int(values["time"] * 1000)
    bid = float(values["bid"])
    ask = float(values["ask"])
    last = float(values["last"]) if values.get("last") else None

    if bid <= 0 or ask <= 0:
        raise ValueError(f"Invalid bid/ask prices: {bid}/{ask}")

    return {"time_msc": time_msc, "bid": bid, "ask": ask, "last": last}


def list_available_symbols() -> None:
    if mt5 is None:
        raise RuntimeError("MetaTrader5 không khả dụng. Hãy cài đặt thư viện MetaTrader5 trên Windows.")
    if not mt5.initialize():
        raise RuntimeError("MT5 initialize failed")
    try:
        symbols = mt5.symbols_get()
        if symbols is None:
            logger.error("Không lấy được danh sách symbols")
            return
        logger.info("Có %d symbols:", len(symbols))
        for sym in symbols:
            logger.info(
                "- %s: visible=%s, select=%s, trade_mode=%s, bid=%s, ask=%s",
                sym.name,
                sym.visible,
                sym.select,
                sym.trade_mode,
                sym.bid,
                sym.ask,
            )
    finally:
        mt5.shutdown()


def fetch_ticks_mt5(
    symbol: str,
    start: datetime,
    end: datetime,
    *,
    max_days_per_call: int = None,
    max_retries: int = None,
    retry_delay: float = None,
) -> List[dict]:
    max_days_per_call = max_days_per_call or 1
    max_retries = max_retries or 3
    retry_delay = retry_delay or 1.0
    if mt5 is None:
        raise RuntimeError("MetaTrader5 không khả dụng.")
    if not mt5.initialize():
        raise RuntimeError("MT5 initialize failed")

    try:
        if not mt5.symbol_select(symbol, True):
            err = mt5.last_error()
            raise RuntimeError(f"Không thể chọn symbol {symbol}: {err}")

        now = datetime.now(timezone.utc)
        if start > now or end > now:
            logger.warning(
                "Thời gian yêu cầu (%s -> %s) vượt quá thời điểm hiện tại (%s). Điều chỉnh lấy 7 ngày gần nhất.",
                start,
                end,
                now,
            )
            end = now
            start = end - timedelta(days=7)

        all_ticks: List[dict] = []
        window_start = start
        while window_start < end:
            window_end = min(window_start + timedelta(days=max_days_per_call or 1), end)
            utc_from = int(window_start.replace(tzinfo=timezone.utc).timestamp())
            utc_to = int(window_end.replace(tzinfo=timezone.utc).timestamp())

            logger.info("Đang lấy ticks từ %s đến %s...", window_start, window_end)

            raw = None
            last_error = None
            for attempt in range(max_retries):
                raw = mt5.copy_ticks_range(symbol, utc_from, utc_to, mt5.COPY_TICKS_ALL)
                if raw is not None and len(raw) > 0:
                    break
                last_error = mt5.last_error()
                if attempt < max_retries - 1:
                    logger.warning(
                        "Lần thử %d/%d thất bại: %s. Thử lại sau %ss...",
                        attempt + 1,
                        max_retries,
                        last_error,
                        retry_delay,
                    )
                    sleep(retry_delay)

            if raw is None or len(raw) == 0:
                logger.error("Không lấy được dữ liệu cho %s -> %s: %s", window_start, window_end, last_error)
            else:
                window_ticks: List[dict] = []
                for item in raw:
                    try:
                        window_ticks.append(parse_mt5_tick(item))
                    except (ValueError, TypeError):
                        continue
                logger.info("Parsed %d ticks", len(window_ticks))
                all_ticks.extend(window_ticks)

            window_start = window_end

        if not all_ticks:
            logger.info("Thử lấy 1 giờ dữ liệu gần nhất...")
            end = now
            start = end - timedelta(hours=1)
            utc_from = int(start.replace(tzinfo=timezone.utc).timestamp())
            utc_to = int(end.replace(tzinfo=timezone.utc).timestamp())
            raw = mt5.copy_ticks_range(symbol, utc_from, utc_to, mt5.COPY_TICKS_ALL)
            if raw is not None:
                for item in raw:
                    try:
                        all_ticks.append(parse_mt5_tick(item))
                    except (ValueError, TypeError):
                        continue
        return all_ticks
    finally:
        mt5.shutdown()


async def fetch_history(
    *,
    symbol: str,
    start: datetime = None,
    end: datetime = None,
    db_url: str = None,
    batch: int = None,
    max_days: int = None,
    resume: bool = False,
) -> None:
    cfg = DEFAULT_DONCHIAN_PARAMS
    max_days = max_days if max_days is not None else int(cfg.get("history_max_days", 1))
    batch = batch if batch is not None else int(cfg.get("history_batch", 2000))
    if start is None:
        start = _parse_iso_utc(str(cfg.get("start")))
    if end is None:
        end = _parse_iso_utc(str(cfg.get("end")))
    if db_url is None:
        db_url = cfg.get("db_url")
    storage = Storage(db_url=db_url)
    await storage.init()

    fetch_start = start
    if resume:
        latest_msc = await storage.get_latest_time_msc(symbol)
        start_msc = int(start.replace(tzinfo=timezone.utc).timestamp() * 1000)
        if latest_msc is not None and latest_msc >= start_msc:
            overlap_minutes = 60  # nạp chồng 1h để tránh hụt dữ liệu sát mép
            fetch_start = datetime.fromtimestamp(latest_msc / 1000, tz=timezone.utc) - timedelta(minutes=overlap_minutes)
            logger.info(
                "Đã có dữ liệu tới %s, fetch tiếp từ %s",
                datetime.fromtimestamp(latest_msc/1000, tz=timezone.utc).isoformat(),
                fetch_start.isoformat(),
            )
    else:
        logger.info("Fetch từ %s tới %s (không dùng resume)", fetch_start.isoformat(), end.isoformat())

    logger.info("Fetching ticks from MT5...")
    loop = asyncio.get_running_loop()
    fetch_fn = partial(fetch_ticks_mt5, symbol, fetch_start, end, max_days_per_call=max_days)
    ticks = await loop.run_in_executor(None, fetch_fn)
    logger.info("Fetched %d ticks total", len(ticks))

    rows = []
    for tick in ticks:
        # tick có thể là dict (parse_mt5_tick) hoặc mt5.Tick tuỳ môi trường
        if isinstance(tick, dict):
            time_msc = int(tick.get("time_msc") or tick.get("time") * 1000)
            bid = tick.get("bid")
            ask = tick.get("ask")
            last = tick.get("last")
        else:
            time_msc = int(getattr(tick, "time_msc", 0) or getattr(tick, "time", 0) * 1000)
            bid = getattr(tick, "bid", None)
            ask = getattr(tick, "ask", None)
            last = getattr(tick, "last", None)

        dt_utc = datetime.fromtimestamp(time_msc / 1000.0, tz=timezone.utc)
        rows.append(
            {
                "symbol": symbol,
                "time_msc": time_msc,
                "datetime": dt_utc.isoformat(),
                "bid": float(bid) if bid is not None else None,
                "ask": float(ask) if ask is not None else None,
                "last": float(last) if last is not None else None,
            }
        )

    try:
        logger.info("Inserting %d ticks into database...", len(rows))
        await storage.insert_ticks_batch(rows, batch_size=batch)
        logger.info("Insert completed")
    finally:
        await storage.close()


def _parse_iso_utc(value: str) -> datetime:
    cleaned = value.strip()
    if cleaned.endswith("Z"):
        cleaned = cleaned[:-1] + "+00:00"
    dt = datetime.fromisoformat(cleaned)
    return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
