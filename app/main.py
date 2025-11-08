import logging
from typing import Optional

from fastapi import FastAPI, HTTPException, WebSocket, WebSocketDisconnect

from .config import get_settings
from .models import HealthStatus, Quote
from .quote_service import QuoteService, QuoteCache, QuotePoller, WebSocketManager


logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(name)s | %(message)s")
logger = logging.getLogger(__name__)

app = FastAPI(
    title="Dịch vụ quote realtime XAUUSDc",
    version="0.2.0",
    description="Dịch vụ phát giá XAUUSDc thời gian thực từ MetaTrader5 cục bộ qua REST và WebSocket.",
)

settings = get_settings()
cache = QuoteCache()
ws_manager = WebSocketManager()

quote_provider: Optional[QuoteService] = None
quote_poller: Optional[QuotePoller] = None


@app.on_event("startup")
async def _startup() -> None:
    global quote_provider, quote_poller

    try:
        quote_provider = QuoteService(settings)

        quote_poller = QuotePoller(
            provider=quote_provider,
            cache=cache,
            ws_manager=ws_manager,
            interval_seconds=settings.poll_interval_seconds,
        )

        await quote_poller.start()
        logger.info("Quote service started. Poll interval: %ss", settings.poll_interval_seconds)
    except Exception:
        # Ghi đầy đủ stacktrace để chẩn đoán lỗi khởi tạo MT5 hoặc chọn symbol
        logger.exception("Không thể khởi tạo provider hoặc khởi động poller trong lúc startup")
        # Re-raise để uvicorn hiển thị lỗi và tránh ứng dụng ở trạng thái bán khởi tạo
        raise


@app.on_event("shutdown")
async def _shutdown() -> None:
    if quote_poller:
        await quote_poller.stop()
    if quote_provider:
        await quote_provider.aclose()
    logger.info("Quote service stopped.")


@app.get("/quotes/xauusdc", response_model=Quote, summary="Lấy quote XAU/USD mới nhất")
async def get_latest_quote() -> Quote:
    quote = await cache.get()
    if not quote:
        raise HTTPException(status_code=503, detail="Chưa lấy được dữ liệu từ upstream.")
    return quote


@app.get("/healthz", response_model=HealthStatus, summary="Kiểm tra trạng thái")
async def health_check() -> HealthStatus:
    quote = await cache.get()
    return HealthStatus(status="ok" if quote else "initializing", last_quote_timestamp=quote.updated_at if quote else None)


@app.websocket("/ws/xauusdc")
async def xauusdc_stream(websocket: WebSocket) -> None:
    await ws_manager.connect(websocket)
    try:
        latest = await cache.get()
        if latest:
            await websocket.send_json(latest.model_dump())
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        logger.info("WebSocket client disconnected.")
    finally:
        await ws_manager.disconnect(websocket)


if __name__ == "__main__":
    import uvicorn

    uvicorn.run("app.main:app", host="0.0.0.0", port=8000, reload=True)
