"""Chạy chiến lược MA Crossover live qua QuoteService."""

import asyncio
from datetime import datetime, timezone, timedelta
from typing import Any, Awaitable, Callable, Dict, Optional

from app.commands import history as history_cmd
from app.config import get_settings
from app.models import Quote
from app.quote_service import QuoteService
from app.storage import Storage
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
    ingest_live_db: bool = True,
    trail_trigger_atr: float,
    trail_atr_mult: float,
    max_daily_loss: Optional[float],
    max_loss_streak: Optional[int],
    max_losses_per_session: Optional[int],
    cooldown_minutes: Optional[int],
    max_holding_minutes: Optional[int] = None,
    allow_buy: bool = True,
    allow_sell: bool = True,
    event_handler: Optional[Callable[[Dict[str, Any]], Awaitable[None] | None]] = None,
    quote_service: Optional[QuoteService] = None,
) -> None:
    base_settings = get_settings()
    storage = Storage(db_url)
    await storage.init()
    resolved_symbol = symbol or base_settings.quote_symbol
    # preset tạm vô hiệu hoá, luôn dùng cấu hình truyền vào
    preset_cfg = None

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
        fast_ma=fast,
        slow_ma=slow,
        ma_type=ma_type,
        timeframe=timeframe,
        paper_mode=not live,
    )
    cfg.trend_ma = trend
    cfg.spread_atr_max = spread_atr_max
    cfg.reverse_exit = reverse_exit
    cfg.market_state_window = market_state_window
    cfg.volume = volume
    cfg.capital = capital
    cfg.risk_pct = risk_pct
    cfg.contract_size = contract_size
    cfg.size_from_risk = size_from_risk
    cfg.sl_atr = sl_atr
    cfg.tp_atr = tp_atr
    cfg.trail_trigger_atr = trail_trigger_atr
    cfg.trail_atr_mult = trail_atr_mult
    cfg.momentum_type = momentum_type
    cfg.momentum_window = momentum_window
    cfg.momentum_threshold = momentum_threshold
    cfg.macd_fast = macd_fast
    cfg.macd_slow = macd_slow
    cfg.macd_signal = macd_signal
    cfg.macd_threshold = macd_threshold
    cfg.range_lookback = range_lookback
    cfg.range_min_atr = range_min_atr
    cfg.range_min_points = range_min_points
    cfg.breakout_buffer_atr = breakout_buffer_atr
    cfg.breakout_confirmation_bars = (
        breakout_confirmation_bars
    )
    cfg.atr_baseline_window = atr_baseline_window
    cfg.atr_multiplier_min = atr_multiplier_min
    cfg.atr_multiplier_max = atr_multiplier_max
    cfg.trading_hours = [h.strip() for h in trading_hours.split(',')] if trading_hours else None
    cfg.adx_window = adx_window
    cfg.adx_threshold = adx_threshold
    cfg.rsi_threshold_long = rsi_threshold_long
    cfg.rsi_threshold_short = rsi_threshold_short
    cfg.max_daily_loss = max_daily_loss
    cfg.max_consecutive_losses = max_loss_streak
    cfg.max_losses_per_session = max_losses_per_session
    cfg.cooldown_minutes = cooldown_minutes
    cfg.max_holding_minutes = (
        preset_cfg.max_holding_minutes
        if (preset_cfg and preset_cfg.max_holding_minutes is not None)
        else max_holding_minutes
    )
    cfg.allow_buy = allow_buy
    cfg.allow_sell = allow_sell

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
    last_msc = await storage.latest_tick_msc(symbol)

    # Nếu DB trống hoặc dữ liệu quá cũ, fetch toàn bộ đoạn lookback
    if last_msc is None or last_msc < since_msc:
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
        return

    # Nếu có dữ liệu nhưng bị gap do disconnect, fetch phần thiếu từ tick cuối đến hiện tại
    end_msc = int(end.timestamp() * 1000)
    if last_msc < end_msc - 500:  # chênh 0.5s để tránh fetch trùng
        gap_start = datetime.fromtimestamp((last_msc + 1) / 1000, tz=timezone.utc)
        print(
            f"Phát hiện thiếu lịch sử cho {symbol} từ {gap_start.isoformat()} tới {end.isoformat()}, đang fetch bù..."
        )
        await history_cmd.fetch_history(
            symbol=symbol,
            start=gap_start,
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
