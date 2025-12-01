"""Chạy chiến lược MA Crossover live qua QuoteService."""

import asyncio
from datetime import datetime, timezone, timedelta
from typing import Any, Awaitable, Callable, Dict, Optional

from app.commands import history as history_cmd
from app.config import get_settings
from app.models import Quote
from app.quote_service import QuoteService
from app.storage import Storage
from app.Breakout_Strategy import MAConfig, MACrossoverStrategy


async def run_live_strategy(
    *,
    db_url: str,
    symbol: Optional[str],
    timeframe: str,
    donchian_period: int,
    ema_trend_period: int,
    atr_period: int,
    entry_buffer_points: float,
    breakeven_after_rr: Optional[float],
    exit_on_opposite: bool,
    capital: float,
    risk_pct: float,
    size_from_risk: bool,
    contract_size: float,
    pip_size: float,
    volume: float,
    min_volume: float,
    volume_step: float,
    max_positions: int,
    sl_atr: float,
    tp_atr: float,
    trail_trigger_atr: float,
    trail_atr_mult: float,
    breakeven_atr: float,
    partial_close: bool,
    partial_close_atr: float,
    trading_hours: Optional[str],
    min_atr_multiplier: float,
    max_atr_multiplier: float,
    max_spread_points: float,
    allowed_deviation_points: float,
    slippage_points: float,
    skip_weekend: bool,
    max_daily_loss: Optional[float],
    max_loss_streak: Optional[int],
    max_losses_per_session: Optional[int],
    cooldown_minutes: Optional[int],
    session_cooldown_minutes: int,
    poll: float,
    live: bool,
    order_retry_times: int,
    order_retry_delay_ms: int,
    magic_number: int,
    ensure_history_hours: float,
    history_batch: int,
    history_max_days: int,
    ingest_live_db: bool,
    event_handler: Optional[Callable[[Dict[str, Any]], Awaitable[None] | None]] = None,
    quote_service: Optional[QuoteService] = None,
) -> None:
    base_settings = get_settings()
    storage = Storage(db_url)
    await storage.init()
    resolved_symbol = symbol or base_settings.quote_symbol
    if ensure_history_hours > 0:
        await _ensure_history_data(
            storage,
            resolved_symbol,
            ensure_history_hours,
            db_url,
            history_batch,
            history_max_days,
        )

    symbol_settings = base_settings.model_copy(update={"quote_symbol": resolved_symbol})
    owns_quote_service = False
    if quote_service:
        current_symbol = getattr(getattr(quote_service, "_settings", None), "quote_symbol", None)
        if current_symbol == resolved_symbol:
            local_quote_service = quote_service
        else:
            local_quote_service = QuoteService(symbol_settings)
            owns_quote_service = True
    else:
        local_quote_service = QuoteService(symbol_settings)
        owns_quote_service = True

    cfg = MAConfig(
        symbol=resolved_symbol,
        timeframe=timeframe,
        donchian_period=donchian_period,
        ema_trend_period=ema_trend_period,
        atr_period=atr_period,
        entry_buffer_points=entry_buffer_points,
        breakeven_after_rr=breakeven_after_rr,
        exit_on_opposite=exit_on_opposite,
        paper_mode=not live,
    )
    cfg.volume = volume
    cfg.min_volume = min_volume
    cfg.volume_step = volume_step
    cfg.max_positions = max_positions
    cfg.live = live
    cfg.capital = capital
    cfg.risk_pct = risk_pct
    cfg.contract_size = contract_size
    cfg.size_from_risk = size_from_risk
    cfg.pip_size = pip_size
    cfg.sl_atr = sl_atr
    cfg.tp_atr = tp_atr
    cfg.trail_trigger_atr = trail_trigger_atr
    cfg.trail_atr_mult = trail_atr_mult
    cfg.breakeven_atr = breakeven_atr
    cfg.partial_close = partial_close
    cfg.partial_close_atr = partial_close_atr
    cfg.min_atr_multiplier = min_atr_multiplier
    cfg.max_atr_multiplier = max_atr_multiplier
    cfg.max_spread_points = max_spread_points
    cfg.allowed_deviation_points = allowed_deviation_points
    cfg.slippage_points = slippage_points
    cfg.trading_hours = [h.strip() for h in trading_hours.split(',')] if trading_hours else None
    cfg.skip_weekend = skip_weekend
    cfg.max_daily_loss = max_daily_loss
    cfg.max_loss_streak = max_loss_streak
    cfg.max_losses_per_session = max_losses_per_session
    cfg.cooldown_minutes = cooldown_minutes
    cfg.session_cooldown_minutes = session_cooldown_minutes
    cfg.order_retry_times = order_retry_times
    cfg.order_retry_delay_ms = order_retry_delay_ms
    cfg.magic_number = magic_number

    strategy = MACrossoverStrategy(cfg, local_quote_service, storage, event_handler=event_handler)
    strategy._running = True  # noqa: SLF001 - giữ nguyên hành vi script gốc

    print('Bắt đầu chạy live. Nhấn Ctrl+C để dừng...')
    try:
        while True:
            quote = await local_quote_service.fetch_quote()
            bid = quote.bid if quote.bid is not None else (quote.price or 0.0)
            ask = quote.ask if quote.ask is not None else (quote.price or bid)
            await strategy._on_quote(cfg.symbol, float(bid), float(ask))

            if event_handler:
                await _dispatch_event(
                    event_handler,
                    {
                        "type": "quote",
                        "timestamp": quote.updated_at,
                        "symbol": cfg.symbol,
                        "bid": float(bid),
                        "ask": float(ask),
                        "price": float(quote.price) if quote.price is not None else None,
                    },
                )

            if ingest_live_db and quote.price is not None:
                await _ingest_tick(storage, cfg.symbol, quote)

            await asyncio.sleep(float(poll))
    except KeyboardInterrupt:  # pragma: no cover - tương tác người dùng
        print('Đang dừng...')
    except asyncio.CancelledError:
        raise
    finally:
        strategy._running = False
        if owns_quote_service:
            await local_quote_service.aclose()
        await storage.close()


async def _ensure_history_data(
    storage: Storage,
    symbol: str,
    hours: float,
    db_url: str,
    batch: int,
    max_days: int,
) -> None:
    end = datetime.now(timezone.utc)
    start = end - timedelta(hours=hours)
    since_msc = int(start.timestamp() * 1000)
    has_data = await storage.has_ticks_since(symbol, since_msc)
    if has_data:
        return

    print(
        f"Không đủ dữ liệu lịch sử cho {symbol} (cần từ {start.isoformat()}), tự động fetch từ MT5..."
    )
    await history_cmd.fetch_history(
        symbol=symbol,
        start=start,
        end=end,
        db_url=db_url,
        batch=batch,
        max_days=max_days,
    )


async def _ingest_tick(storage: Storage, symbol: str, quote: Quote) -> None:
    dt_utc = quote.updated_at.astimezone(timezone.utc)
    row = {
        "symbol": symbol,
        "time_msc": int(dt_utc.timestamp() * 1000),
        "datetime": dt_utc.isoformat(),
        "bid": float(quote.bid) if quote.bid is not None else None,
        "ask": float(quote.ask) if quote.ask is not None else None,
        "last": float(quote.price) if quote.price is not None else None,
    }
    await storage.insert_ticks_batch([row], batch_size=1, quiet=True)


async def _dispatch_event(
    handler: Callable[[Dict[str, Any]], Awaitable[None] | None],
    payload: Dict[str, Any],
) -> None:
    try:
        result = handler(payload)
        if asyncio.iscoroutine(result):
            await result
    except Exception as exc:  # pragma: no cover
        print("Event handler error:", exc)
