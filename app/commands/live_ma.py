"""Chạy chiến lược MA Crossover live qua QuoteService."""

import asyncio
from datetime import datetime, timezone, timedelta
from typing import Any, Awaitable, Callable, Dict, Optional

from app.commands import history as history_cmd
from app.constants import DEFAULT_US_TRADING_HOURS
from app.config import get_settings
from app.models import Quote
from app.quote_service import QuoteService
from app.storage import Storage
from app.strategy_presets import resolve_preset
from app.strategies.ma_crossover import MAConfig, MACrossoverStrategy


async def run_live_strategy(
    *,
    db_url: str,
    symbol: Optional[str],
    preset: Optional[str] = None,
    fast: int,
    slow: int,
    ma_type: str,
    timeframe: str,
    trend: int,
    spread_atr_max: float,
    reverse_exit: bool,
    market_state_window: int,
    volume: float,
    capital: float,
    risk_pct: float,
    contract_size: float,
    size_from_risk: bool,
    sl_atr: float,
    tp_atr: float,
    sl_pips: Optional[float],
    tp_pips: Optional[float],
    pip_size: float,
    momentum_window: int,
    momentum_threshold: float,
    momentum_type: str,
    macd_fast: int,
    macd_slow: int,
    macd_signal: int,
    macd_threshold: float,
    rsi_threshold_long: float,
    rsi_threshold_short: float,
    range_lookback: int,
    range_min_atr: float,
    range_min_points: float,
    breakout_buffer_atr: float,
    breakout_confirmation_bars: int,
    atr_baseline_window: int,
    atr_multiplier_min: float,
    atr_multiplier_max: float,
    trading_hours: Optional[str],
    adx_window: int,
    adx_threshold: float,
    poll: float,
    live: bool,
    ensure_history_hours: float,
    history_batch: int,
    history_max_days: int,
    ingest_live_db: bool,
    trail_trigger_atr: float,
    trail_atr_mult: float,
    max_daily_loss: Optional[float],
    max_loss_streak: Optional[int],
    max_losses_per_session: Optional[int],
    cooldown_minutes: Optional[int],
    event_handler: Optional[Callable[[Dict[str, Any]], Awaitable[None] | None]] = None,
    quote_service: Optional[QuoteService] = None,
) -> None:
    settings = get_settings()
    storage = Storage(db_url)
    await storage.init()
    resolved_symbol = symbol or settings.quote_symbol
    preset_cfg = resolve_preset(preset)

    if ensure_history_hours > 0:
        await _ensure_history_data(
            storage,
            resolved_symbol,
            ensure_history_hours,
            db_url,
            history_batch,
            history_max_days,
        )

    local_quote_service = quote_service or QuoteService(settings)
    owns_quote_service = quote_service is None

    cfg = MAConfig(
        symbol=resolved_symbol,
        fast_ma=preset_cfg.fast_ma if preset_cfg else fast,
        slow_ma=preset_cfg.slow_ma if preset_cfg else slow,
        ma_type=preset_cfg.ma_type if preset_cfg else ma_type,
        timeframe=preset_cfg.timeframe if preset_cfg else timeframe,
        paper_mode=not live,
    )
    cfg.trend_ma = trend
    cfg.spread_atr_max = preset_cfg.spread_atr_max if preset_cfg else spread_atr_max
    cfg.reverse_exit = reverse_exit
    cfg.market_state_window = market_state_window
    cfg.volume = volume
    cfg.capital = capital
    cfg.risk_pct = risk_pct
    cfg.contract_size = contract_size
    cfg.size_from_risk = size_from_risk
    cfg.sl_atr = preset_cfg.sl_atr if preset_cfg else sl_atr
    cfg.tp_atr = preset_cfg.tp_atr if preset_cfg else tp_atr
    cfg.trail_trigger_atr = trail_trigger_atr
    cfg.trail_atr_mult = trail_atr_mult
    cfg.momentum_type = preset_cfg.momentum_type if preset_cfg else momentum_type
    cfg.momentum_window = preset_cfg.momentum_window if preset_cfg else momentum_window
    cfg.momentum_threshold = preset_cfg.momentum_threshold if preset_cfg else momentum_threshold
    cfg.macd_fast = preset_cfg.macd_fast if preset_cfg else macd_fast
    cfg.macd_slow = preset_cfg.macd_slow if preset_cfg else macd_slow
    cfg.macd_signal = preset_cfg.macd_signal if preset_cfg else macd_signal
    cfg.macd_threshold = preset_cfg.macd_threshold if preset_cfg else macd_threshold
    cfg.range_lookback = preset_cfg.range_lookback if preset_cfg else range_lookback
    cfg.range_min_atr = preset_cfg.range_min_atr if preset_cfg else range_min_atr
    cfg.range_min_points = preset_cfg.range_min_points if preset_cfg else range_min_points
    cfg.breakout_buffer_atr = preset_cfg.breakout_buffer_atr if preset_cfg else breakout_buffer_atr
    cfg.breakout_confirmation_bars = (
        preset_cfg.breakout_confirmation_bars if preset_cfg else breakout_confirmation_bars
    )
    cfg.atr_baseline_window = preset_cfg.atr_baseline_window if preset_cfg else atr_baseline_window
    cfg.atr_multiplier_min = preset_cfg.atr_multiplier_min if preset_cfg else atr_multiplier_min
    cfg.atr_multiplier_max = preset_cfg.atr_multiplier_max if preset_cfg else atr_multiplier_max
    if trading_hours:
        cfg.trading_hours = [h.strip() for h in trading_hours.split(',')]
    elif preset_cfg and preset_cfg.trading_hours:
        cfg.trading_hours = [h.strip() for h in preset_cfg.trading_hours.split(',')]
    else:
        cfg.trading_hours = [h.strip() for h in DEFAULT_US_TRADING_HOURS.split(',')]
    cfg.adx_window = preset_cfg.adx_window if preset_cfg else adx_window
    cfg.adx_threshold = preset_cfg.adx_threshold if preset_cfg else adx_threshold
    cfg.rsi_threshold_long = (
        preset_cfg.rsi_threshold_long if (preset_cfg and preset_cfg.rsi_threshold_long is not None) else rsi_threshold_long
    )
    cfg.rsi_threshold_short = (
        preset_cfg.rsi_threshold_short if (preset_cfg and preset_cfg.rsi_threshold_short is not None) else rsi_threshold_short
    )
    cfg.max_daily_loss = (
        preset_cfg.max_daily_loss if (preset_cfg and preset_cfg.max_daily_loss is not None) else max_daily_loss
    )
    cfg.max_consecutive_losses = (
        preset_cfg.max_consecutive_losses
        if (preset_cfg and preset_cfg.max_consecutive_losses is not None)
        else max_loss_streak
    )
    cfg.max_losses_per_session = (
        preset_cfg.max_losses_per_session
        if (preset_cfg and preset_cfg.max_losses_per_session is not None)
        else max_losses_per_session
    )
    cfg.cooldown_minutes = (
        preset_cfg.cooldown_minutes if (preset_cfg and preset_cfg.cooldown_minutes is not None) else cooldown_minutes
    )

    if sl_pips is not None and tp_pips is not None:
        setattr(cfg, 'sl_pips', float(sl_pips))
        setattr(cfg, 'tp_pips', float(tp_pips))
        setattr(cfg, 'pip_size', float(pip_size))

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
    utc7 = timezone(timedelta(hours=7))
    dt_vn = quote.updated_at.astimezone(utc7)
    row = {
        "symbol": symbol,
        "time_msc": int(quote.updated_at.timestamp() * 1000),
        "datetime": dt_vn.isoformat(),
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
