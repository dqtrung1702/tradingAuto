import asyncio
import logging
from datetime import datetime, timezone
from typing import Optional

try:  # Thư viện MetaTrader5 chỉ khả dụng trên Windows
    import MetaTrader5 as mt5  # type: ignore
except ImportError:  # pragma: no cover - optional dependency
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
        if mt5 is None:  # pragma: no cover - phụ thuộc tuỳ chọn
            raise RuntimeError(
                "MetaTrader5 library chưa được cài đặt hoặc không khả dụng trên hệ điều hành này."
            )
        self._settings = settings
        self._initialized = False
        self._symbol_selected = False

    async def fetch_quote(self) -> Quote:
        return await asyncio.to_thread(self._fetch_sync)

    def _fetch_sync(self) -> Quote:
        symbol = self._settings.quote_symbol
        attempt = 0
        while True:
            attempt += 1
            self._ensure_initialized()
            self._ensure_symbol_selected()

            tick = mt5.symbol_info_tick(symbol)
            if tick is None:
                code, message = mt5.last_error()
                if code == -10004 and attempt == 1:
                    logger.warning("MT5 mất kết nối IPC, thử khởi tạo lại...")
                    self._reset_connection()
                    continue
                raise RuntimeError(f"Không lấy được tick cho {symbol}: {code} {message}")
            break

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
            suggestions = []
            try:
                all_syms = mt5.symbols_get()
                if all_syms:
                    needle = symbol.lower()
                    for sym in all_syms:
                        name = getattr(sym, "name", None) or getattr(sym, "path", None) or str(sym)
                        if name and needle in name.lower():
                            suggestions.append(name)
            except Exception:  # pragma: no cover - phụ thuộc môi trường MT5
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

    def _reset_connection(self) -> None:
        try:
            mt5.shutdown()
        except Exception:
            pass
        self._initialized = False
        self._symbol_selected = False
