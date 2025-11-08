import asyncio
import logging
from contextlib import suppress
from datetime import datetime, timezone
from typing import Optional, Set

from fastapi import WebSocket

try:  # Thư viện MetaTrader5 chỉ khả dụng trên Windows.
    import MetaTrader5 as mt5  # type: ignore
except ImportError:  # pragma: no cover - handled at runtime when MT5 mode is requested
    mt5 = None  # type: ignore

from .config import Settings
from .models import Quote

logger = logging.getLogger(__name__)


def _to_float(value: Optional[float]) -> Optional[float]:
    try:
        return float(value) if value is not None else None
    except (TypeError, ValueError):
        return None


class QuoteService:
    """Lấy quote trực tiếp từ MetaTrader5 terminal đang chạy trên máy."""

    def __init__(self, settings: Settings):
        if mt5 is None:  # pragma: no cover - depends on optional dependency
            raise RuntimeError(
                "MetaTrader5 library chưa được cài đặt hoặc không khả dụng trên hệ điều hành này."
            )
        self._settings = settings
        self._initialized = False
        self._symbol_selected = False

    async def fetch_quote(self) -> Quote:
        return await asyncio.to_thread(self._fetch_sync)

    def _fetch_sync(self) -> Quote:
        self._ensure_initialized()
        self._ensure_symbol_selected()

        symbol = self._settings.quote_symbol
        tick = mt5.symbol_info_tick(symbol)
        if tick is None:
            code, message = mt5.last_error()
            raise RuntimeError(f"Không lấy được tick cho {symbol}: {code} {message}")

        info = mt5.symbol_info(symbol)
        currency = getattr(info, "currency_profit", None) if info else None
        price = _to_float(getattr(tick, "last", None)) or _to_float(getattr(tick, "ask", None))
        if not price:
            price = _to_float(getattr(tick, "bid", None))

        timestamp = getattr(tick, "time_msc", None) or getattr(tick, "time", None)
        updated_at = (
            datetime.fromtimestamp(timestamp / 1000, tz=timezone.utc)
            if timestamp and getattr(tick, "time_msc", None)
            else datetime.fromtimestamp(timestamp, tz=timezone.utc)
            if timestamp
            else datetime.now(timezone.utc)
        )

        change_percent: Optional[float] = None
        session_close = _to_float(getattr(info, "session_price_close", None)) if info else None
        if price and session_close and session_close != 0:
            change_percent = ((price - session_close) / session_close) * 100

        return Quote(
            symbol=symbol,
            price=price,
            bid=_to_float(getattr(tick, "bid", None)),
            ask=_to_float(getattr(tick, "ask", None)),
            change_percent=change_percent,
            currency=currency or "USD",
            source=self._settings.source_name or "MetaTrader5",
            updated_at=updated_at,
        )

    def _ensure_initialized(self) -> None:
        if self._initialized:
            return

        kwargs = {}
        if self._settings.mt5_login:
            kwargs["login"] = self._settings.mt5_login
        if self._settings.mt5_password:
            kwargs["password"] = self._settings.mt5_password
        if self._settings.mt5_server:
            kwargs["server"] = self._settings.mt5_server
        if self._settings.mt5_terminal_path:
            kwargs["path"] = self._settings.mt5_terminal_path

        if not mt5.initialize(**kwargs):
            code, message = mt5.last_error()
            raise RuntimeError(f"MT5 initialize thất bại: {code} {message}")
        self._initialized = True
        logger.info("Kết nối MT5 thành công.")

    def _ensure_symbol_selected(self) -> None:
        if self._symbol_selected:
            return

        symbol = self._settings.quote_symbol
        if not mt5.symbol_select(symbol, True):
            code, message = mt5.last_error()
            # Cố gắng cung cấp thông tin gợi ý: liệt kê các symbol tương tự có trong terminal
            suggestions = []
            try:
                all_syms = mt5.symbols_get()
                if all_syms:
                    needle = symbol.lower()
                    for s in all_syms:
                        name = getattr(s, 'name', None) or getattr(s, 'path', None) or str(s)
                        if name and needle in name.lower():
                            suggestions.append(name)
            except Exception:
                # Nếu việc liệt kê symbol thất bại, bỏ qua phần gợi ý
                suggestions = []

            suggestion_msg = f" Các symbol tương tự: {', '.join(suggestions)}" if suggestions else ""
            raise RuntimeError(f"Không thể chọn symbol {symbol}: {code} {message}.{suggestion_msg}")
        self._symbol_selected = True

    async def aclose(self) -> None:
        if not self._initialized:
            return
        await asyncio.to_thread(self._shutdown_sync)

    def _shutdown_sync(self) -> None:
        mt5.shutdown()
        self._initialized = False
        self._symbol_selected = False


class QuoteCache:
    """Bộ đệm nhớ trong lưu quote mới nhất."""

    def __init__(self) -> None:
        self._quote: Optional[Quote] = None
        self._lock = asyncio.Lock()

    async def set(self, quote: Quote) -> None:
        async with self._lock:
            self._quote = quote

    async def get(self) -> Optional[Quote]:
        async with self._lock:
            return self._quote


class WebSocketManager:
    """Quản lý kết nối WebSocket đang hoạt động và phát payload tới client."""

    def __init__(self) -> None:
        self._connections: Set[WebSocket] = set()
        self._lock = asyncio.Lock()

    async def connect(self, websocket: WebSocket) -> None:
        await websocket.accept()
        async with self._lock:
            self._connections.add(websocket)

    async def disconnect(self, websocket: WebSocket) -> None:
        async with self._lock:
            self._connections.discard(websocket)

    async def broadcast(self, quote: Quote) -> None:
        async with self._lock:
            connections = list(self._connections)

        if not connections:
            return

        payload = quote.model_dump()
        for ws in connections:
            try:
                await ws.send_json(payload)
            except Exception as exc:  # pragma: no cover - network errors depend on clients
                logger.warning("Gửi WebSocket thất bại: %s", exc)
                await self.disconnect(ws)


class QuotePoller:
    """Tác vụ nền định kỳ cập nhật cache và đẩy thông tin tới client."""

    def __init__(
        self,
        provider: QuoteService,
        cache: QuoteCache,
        ws_manager: WebSocketManager,
        interval_seconds: float,
    ) -> None:
        self._provider = provider
        self._cache = cache
        self._ws_manager = ws_manager
        self._interval_seconds = interval_seconds
        self._task: Optional[asyncio.Task] = None

    async def start(self) -> None:
        if self._task is None:
            self._task = asyncio.create_task(self._run(), name="quote-poller")

    async def stop(self) -> None:
        if self._task:
            self._task.cancel()
            with suppress(asyncio.CancelledError):
                await self._task
            self._task = None

    async def _run(self) -> None:
        while True:
            try:
                quote = await self._provider.fetch_quote()
                await self._cache.set(quote)
                await self._ws_manager.broadcast(quote)
            except Exception:
                # Log full exception including stacktrace to ease debugging of MT5 or network issues
                logger.exception("Không thể cập nhật quote")
            await asyncio.sleep(self._interval_seconds)
