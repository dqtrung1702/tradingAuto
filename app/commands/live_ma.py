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
    fast: int,
    slow: int,
    ma_type: str,
    timeframe: str,
    trend: int,
    spread_atr_max: float,
    reverse_exit: bool,
    market_state_window: int,
    volume: float = 0.1,
    capital: float = 100.0,
    risk_pct: float,
    contract_size: float,
    size_from_risk: bool = True,
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
    order_retry_times: Optional[int] = 3,
    order_retry_delay_ms: Optional[int] = 300,
    safety_entry_atr_mult: float = 0.1,
    spread_samples: int = 5,
    spread_sample_delay_ms: int = 8,
    allowed_deviation_points: int = 300,
    volatility_spike_atr_mult: float = 0.8,
    spike_delay_ms: int = 50,
    skip_reset_window: bool = True,
    latency_min_ms: int = 200,
    latency_max_ms: int = 400,
    slippage_usd: float = 0.05,
    order_reject_prob: float = 0.03,
    base_spread_points: int = 50,
    spread_spike_chance: float = 0.02,
    spread_spike_min_points: int = 80,
    spread_spike_max_points: int = 300,
    slip_per_atr_ratio: float = 0.2,
    requote_prob: float = 0.01,
    offquotes_prob: float = 0.005,
    timeout_prob: float = 0.005,
    stop_hunt_chance: float = 0.015,
    stop_hunt_min_atr_ratio: float = 0.2,
    stop_hunt_max_atr_ratio: float = 1.0,
    missing_tick_chance: float = 0.005,
    min_volume_multiplier: float = 1.1,
    slippage_pips: Optional[float] = 3.0,
    allow_buy: bool = True,
    allow_sell: bool = True,
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
    cfg.size_from_risk = True
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
    cfg.max_holding_minutes = max_holding_minutes
    cfg.allow_buy = allow_buy
    cfg.allow_sell = allow_sell
    cfg.order_retry_times = 3 if order_retry_times is None else int(order_retry_times)        # ← THÊM
    cfg.order_retry_delay_ms = 300 if order_retry_delay_ms is None else int(order_retry_delay_ms)  # ← THÊM
    cfg.safety_entry_atr_mult = safety_entry_atr_mult
    cfg.spread_samples = max(1, int(spread_samples or 1))
    cfg.spread_sample_delay_ms = max(0, int(spread_sample_delay_ms or 0))
    cfg.allowed_deviation_points = max(0, int(allowed_deviation_points or 0))
    cfg.volatility_spike_atr_mult = max(0.0, float(volatility_spike_atr_mult or 0.0))
    cfg.spike_delay_ms = max(0, int(spike_delay_ms or 0))
    cfg.skip_reset_window = bool(skip_reset_window)
    cfg.latency_min_ms = max(0, int(latency_min_ms or 0))
    cfg.latency_max_ms = max(cfg.latency_min_ms, int(latency_max_ms or cfg.latency_min_ms))
    cfg.slippage_usd = max(0.0, float(slippage_usd or 0.0))
    cfg.order_reject_prob = max(0.0, float(order_reject_prob or 0.0))
    cfg.base_spread_points = max(0, int(base_spread_points or 0))
    cfg.spread_spike_chance = max(0.0, float(spread_spike_chance or 0.0))
    cfg.spread_spike_min_points = max(0, int(spread_spike_min_points or 0))
    cfg.spread_spike_max_points = max(cfg.spread_spike_min_points, int(spread_spike_max_points or cfg.spread_spike_min_points))
    cfg.slip_per_atr_ratio = max(0.0, float(slip_per_atr_ratio or 0.0))
    cfg.requote_prob = max(0.0, float(requote_prob or 0.0))
    cfg.offquotes_prob = max(0.0, float(offquotes_prob or 0.0))
    cfg.timeout_prob = max(0.0, float(timeout_prob or 0.0))
    cfg.stop_hunt_chance = max(0.0, float(stop_hunt_chance or 0.0))
    cfg.stop_hunt_min_atr_ratio = max(0.0, float(stop_hunt_min_atr_ratio or 0.0))
    cfg.stop_hunt_max_atr_ratio = max(cfg.stop_hunt_min_atr_ratio, float(stop_hunt_max_atr_ratio or cfg.stop_hunt_min_atr_ratio))
    cfg.missing_tick_chance = max(0.0, float(missing_tick_chance or 0.0))
    cfg.min_volume_multiplier = min_volume_multiplier
    cfg.slippage_pips = slippage_pips
    cfg.max_holding_minutes = max_holding_minutes or 180  # nếu None thì mặc định 3 tiếng

    if sl_pips is not None and tp_pips is not None:
        setattr(cfg, 'sl_pips', float(sl_pips))
        setattr(cfg, 'tp_pips', float(tp_pips))
        setattr(cfg, 'pip_size', float(pip_size))

    strategy = MACrossoverStrategy(cfg, local_quote_service, storage, event_handler=event_handler)
    strategy._running = True  # noqa: SLF001 - giữ nguyên hành vi script gốc
    # Warmup dữ liệu chỉ báo từ DB trước khi nhận quote realtime
    async def _warmup_with_fetch() -> None:
        def _required_hours() -> float:
            try:
                lookback = strategy._calculate_lookback_duration()
                minutes = strategy._timeframe_minutes() * strategy._calculate_required_bars()
                return max(1.0, lookback.total_seconds() / 3600.0, minutes / 60.0)
            except Exception:
                return 24.0

        async def _do_fetch(hours: float) -> None:
            end = datetime.now(timezone.utc)
            start = end - timedelta(hours=hours)
            await history_cmd.fetch_history(
                symbol=resolved_symbol,
                start=start,
                end=end,
                db_url=db_url,
                batch=history_batch,
                max_days=history_max_days,
            )

        try:
            await strategy._update_data()
            if strategy._df is None or strategy._df.empty:
                raise RuntimeError("Empty DF after warmup")
        except Exception:
            # Nếu thiếu dữ liệu (thường sau cuối tuần), fetch bù rồi thử lại
            try:
                target_hours = max(_required_hours(), ensure_history_hours or 24, 24)
                attempts = 0
                while attempts < 4:
                    await _do_fetch(target_hours)
                    await strategy._update_data()
                    if strategy._df is not None and len(strategy._df) >= strategy._calculate_required_bars():
                        break
                    target_hours *= 2
                    attempts += 1
                if strategy._df is None or len(strategy._df) < strategy._calculate_required_bars():
                    raise RuntimeError("Empty/insufficient DF after extended fetch")
            except Exception as exc_inner:  # pragma: no cover - best effort
                print(f"⚠️ Warmup dữ liệu thất bại sau fetch: {exc_inner}")

        # Log số bar để debug thiếu dữ liệu
        try:
            df_len = len(strategy._df) if strategy._df is not None else 0
            print(
                f"[warmup] Loaded {df_len} bars for {resolved_symbol} "
                f"(timeframe={timeframe}, ensure_history_hours={ensure_history_hours})"
            )
        except Exception:
            pass

    await _warmup_with_fetch()

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
