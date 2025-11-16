"""Lệnh ingest realtime ticks từ MT5 vào cơ sở dữ liệu."""

import asyncio
from datetime import timezone, timedelta

from app.config import get_settings
from app.quote_service import QuoteService
from app.storage import Storage


async def ingest_live_ticks(db_url: str, symbol: str, interval: float) -> None:
    """Ingest realtime ticks mỗi ``interval`` giây."""

    settings = get_settings().model_copy(update={"quote_symbol": symbol})
    storage = Storage(db_url)
    await storage.init()
    quote_service = QuoteService(settings)

    print(f"Bắt đầu ingest realtime cho {symbol} mỗi {interval}s. Nhấn Ctrl+C để dừng.")
    try:
        while True:
            quote = await quote_service.fetch_quote()
            if quote and quote.price is not None:
                utc7 = timezone(timedelta(hours=7))
                dt_vn = quote.updated_at.astimezone(utc7)
                row = {
                    "symbol": symbol,
                    "time_msc": int(quote.updated_at.timestamp() * 1000),
                    "datetime": dt_vn.isoformat(),
                    "bid": float(quote.bid) if quote.bid is not None else None,
                    "ask": float(quote.ask) if quote.ask is not None else None,
                    "last": float(quote.price),
                }
                await storage.insert_ticks_batch([row], batch_size=1)
            await asyncio.sleep(interval)
    except KeyboardInterrupt:  # pragma: no cover - tương tác người dùng
        print("Dừng ingest...")
    finally:
        await quote_service.aclose()
        await storage.close()
