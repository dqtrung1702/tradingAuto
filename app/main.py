import csv
import io
import logging
from datetime import datetime, timezone, timedelta
from typing import Optional

from fastapi import FastAPI, HTTPException, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse, StreamingResponse
from pydantic import BaseModel

from .config import get_settings
from .models import HealthStatus, Quote
from .quote_service import QuoteService, QuoteCache, QuotePoller, WebSocketManager
from .live_manager import LiveStrategyManager, LiveStartRequest
from .strategy_presets import PRESETS
from .commands.backtest_ma import run_backtest as run_backtest_ma
from .commands import history as history_cmd
from .storage import Storage


class BacktestRequest(BaseModel):
    db_url: str
    symbol: str
    start: str
    end: str
    preset: Optional[str] = None
    fast: int = 8
    slow: int = 21
    ma_type: str = "ema"
    timeframe: str = "5min"
    trend: int = 200
    risk_pct: float = 1.0
    capital: float = 10000.0
    trail_trigger_atr: float = 1.8
    trail_atr_mult: float = 1.1
    spread_atr_max: float = 0.08
    reverse_exit: bool = False
    market_state_window: int = 40
    sl_atr: float = 1.5
    tp_atr: float = 2.5
    volume: float = 0.1
    contract_size: float = 100.0
    sl_pips: Optional[float] = None
    tp_pips: Optional[float] = None
    pip_size: float = 0.01
    size_from_risk: bool = False
    momentum_type: str = "hybrid"
    momentum_window: int = 14
    momentum_threshold: float = 0.07
    macd_fast: int = 12
    macd_slow: int = 26
    macd_signal: int = 9
    macd_threshold: float = 0.0002
    range_lookback: int = 40
    range_min_atr: float = 0.8
    range_min_points: float = 1.0
    breakout_buffer_atr: float = 0.3
    breakout_confirmation_bars: int = 1
    atr_baseline_window: int = 14
    atr_multiplier_min: float = 1.1
    atr_multiplier_max: float = 3.2
    trading_hours: Optional[str] = None
    adx_window: int = 14
    adx_threshold: float = 25.0
    rsi_threshold_long: float = 60.0
    rsi_threshold_short: float = 40.0
    max_daily_loss: Optional[float] = None
    max_loss_streak: Optional[int] = None
    max_losses_per_session: Optional[int] = None
    cooldown_minutes: Optional[int] = None


class FetchHistoryRequest(BaseModel):
    db_url: str
    symbol: str
    start: str
    end: str
    batch: int = 2000
    max_days: int = 1


class DownloadHistoryRequest(BaseModel):
    db_url: str
    symbol: str
    hours: float = 24.0


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
live_manager = LiveStrategyManager(settings.quote_symbol)

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
    await live_manager.stop()
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


@app.get("/dashboard", response_class=HTMLResponse)
async def dashboard() -> HTMLResponse:
    return HTMLResponse(DASHBOARD_HTML)


@app.get("/api/live/status")
async def get_live_status() -> dict:
    status = await live_manager.get_status()
    return status.model_dump()


@app.post("/api/live/start")
async def start_live_strategy(payload: LiveStartRequest) -> dict:
    try:
        await live_manager.start(payload, quote_provider)
    except RuntimeError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return {"status": "started"}


@app.post("/api/live/stop")
async def stop_live_strategy() -> dict:
    await live_manager.stop()
    return {"status": "stopped"}


@app.get("/api/live/presets")
async def list_presets() -> list[dict]:
    return [
        {
            "id": key,
            "name": preset.name,
            "description": preset.description,
            "fast_ma": preset.fast_ma,
            "slow_ma": preset.slow_ma,
            "ma_type": preset.ma_type,
            "timeframe": preset.timeframe,
            "spread_atr_max": preset.spread_atr_max,
            "momentum_type": preset.momentum_type,
            "momentum_window": preset.momentum_window,
            "momentum_threshold": preset.momentum_threshold,
            "macd_fast": preset.macd_fast,
            "macd_slow": preset.macd_slow,
            "macd_signal": preset.macd_signal,
            "macd_threshold": preset.macd_threshold,
            "sl_atr": preset.sl_atr,
            "tp_atr": preset.tp_atr,
            "range_lookback": preset.range_lookback,
            "range_min_atr": preset.range_min_atr,
            "range_min_points": preset.range_min_points,
            "breakout_buffer_atr": preset.breakout_buffer_atr,
            "breakout_confirmation_bars": preset.breakout_confirmation_bars,
            "atr_baseline_window": preset.atr_baseline_window,
            "atr_multiplier_min": preset.atr_multiplier_min,
            "atr_multiplier_max": preset.atr_multiplier_max,
            "adx_window": getattr(preset, "adx_window", 14),
            "adx_threshold": getattr(preset, "adx_threshold", 0.0),
            "rsi_threshold_long": getattr(preset, "rsi_threshold_long", 60.0),
            "rsi_threshold_short": getattr(preset, "rsi_threshold_short", 40.0),
            "trail_trigger_atr": getattr(preset, "trail_trigger_atr", 0.0),
            "trail_atr_mult": getattr(preset, "trail_atr_mult", 0.0),
            "max_daily_loss": getattr(preset, "max_daily_loss", None),
            "max_loss_streak": getattr(preset, "max_consecutive_losses", None),
            "max_losses_per_session": getattr(preset, "max_losses_per_session", None),
            "cooldown_minutes": getattr(preset, "cooldown_minutes", None),
        }
        for key, preset in PRESETS.items()
    ]


@app.post("/api/backtest/run")
async def run_backtest_endpoint(payload: BacktestRequest) -> dict:
    try:
        summary = await run_backtest_ma(
            db_url=payload.db_url,
            symbol=payload.symbol,
            preset=payload.preset,
            start_str=payload.start,
            end_str=payload.end,
            fast=payload.fast,
            slow=payload.slow,
            timeframe=payload.timeframe,
            ma_type=payload.ma_type,
            trend=payload.trend,
            risk_pct=payload.risk_pct,
            capital=payload.capital,
            trail_trigger_atr=payload.trail_trigger_atr,
            trail_atr_mult=payload.trail_atr_mult,
            spread_atr_max=payload.spread_atr_max,
            reverse_exit=payload.reverse_exit,
            market_state_window=payload.market_state_window,
            sl_atr=payload.sl_atr,
            tp_atr=payload.tp_atr,
            volume=payload.volume,
            contract_size=payload.contract_size,
            sl_pips=payload.sl_pips,
            tp_pips=payload.tp_pips,
            pip_size=payload.pip_size,
            size_from_risk=payload.size_from_risk,
            momentum_type=payload.momentum_type,
            momentum_window=payload.momentum_window,
            momentum_threshold=payload.momentum_threshold,
            macd_fast=payload.macd_fast,
            macd_slow=payload.macd_slow,
            macd_signal=payload.macd_signal,
            macd_threshold=payload.macd_threshold,
            range_lookback=payload.range_lookback,
            range_min_atr=payload.range_min_atr,
            range_min_points=payload.range_min_points,
            breakout_buffer_atr=payload.breakout_buffer_atr,
            breakout_confirmation_bars=payload.breakout_confirmation_bars,
            atr_baseline_window=payload.atr_baseline_window,
            atr_multiplier_min=payload.atr_multiplier_min,
            atr_multiplier_max=payload.atr_multiplier_max,
            trading_hours=payload.trading_hours,
            adx_window=payload.adx_window,
            adx_threshold=payload.adx_threshold,
            rsi_threshold_long=payload.rsi_threshold_long,
            rsi_threshold_short=payload.rsi_threshold_short,
            max_daily_loss=payload.max_daily_loss,
            max_loss_streak=payload.max_loss_streak,
            max_losses_per_session=payload.max_losses_per_session,
            cooldown_minutes=payload.cooldown_minutes,
            return_summary=True,
        )
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    if summary is None:
        summary = {}
    summary["cli_command"] = _build_backtest_cli(payload)
    return summary


@app.post("/api/history/fetch")
async def fetch_history_endpoint(payload: FetchHistoryRequest) -> dict:
    try:
        start = _parse_iso_dt(payload.start)
        end = _parse_iso_dt(payload.end)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=f"Thời gian không hợp lệ: {exc}") from exc

    try:
        await history_cmd.fetch_history(
            symbol=payload.symbol,
            start=start,
            end=end,
            db_url=payload.db_url,
            batch=payload.batch,
            max_days=payload.max_days,
        )
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return {
        "status": "fetched",
        "symbol": payload.symbol,
        "start": start.isoformat(),
        "end": end.isoformat(),
        "batch": payload.batch,
        "max_days": payload.max_days,
    }


@app.post("/api/history/download")
async def download_history_csv(payload: DownloadHistoryRequest) -> StreamingResponse:
    hours = max(0.1, payload.hours)
    end_dt = datetime.now(timezone.utc)
    start_dt = end_dt - timedelta(hours=hours)
    start_msc = int(start_dt.timestamp() * 1000)
    end_msc = int(end_dt.timestamp() * 1000)
    storage = Storage(payload.db_url)
    await storage.init()
    try:
        rows = await storage.fetch_ticks_range(payload.symbol, start_msc, end_msc)
    finally:
        await storage.close()

    buffer = io.StringIO()
    fieldnames = ["symbol", "time_msc", "datetime", "bid", "ask", "last"]
    writer = csv.DictWriter(buffer, fieldnames=fieldnames)
    writer.writeheader()
    for row in rows:
        writer.writerow(
            {
                "symbol": row.get("symbol"),
                "time_msc": row.get("time_msc"),
                "datetime": row.get("datetime"),
                "bid": row.get("bid"),
                "ask": row.get("ask"),
                "last": row.get("last"),
            }
        )
    filename = f"ticks_{payload.symbol}_{start_dt.strftime('%Y%m%dT%H%M')}_{end_dt.strftime('%Y%m%dT%H%M')}.csv"
    return StreamingResponse(
        iter([buffer.getvalue()]),
        media_type="text/csv",
        headers={"Content-Disposition": f"attachment; filename={filename}"},
    )


def _build_backtest_cli(payload: BacktestRequest) -> str:
    parts = ["python", "-m", "app.cli", "backtest-ma"]
    data = payload.model_dump()
    bool_fields = {"reverse_exit", "size_from_risk"}
    for key, value in data.items():
        flag = f"--{key.replace('_', '-')}"
        if value is None:
            continue
        if key in bool_fields:
            if value:
                parts.append(flag)
            continue
        parts.extend([flag, str(value)])
    return " ".join(parts)


def _parse_iso_dt(value: str) -> datetime:
    dt = datetime.fromisoformat(value)
    return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)


DASHBOARD_HTML = """
<!doctype html>
<html lang=\"vi\">
  <head>
    <meta charset=\"utf-8\" />
    <title>Breakout Strategy Control</title>
    <style>
      body { font-family: Arial, sans-serif; margin: 20px; background-color: #0f172a; color: #f8fafc; }
      h1 { margin-bottom: 0.5rem; }
      .note { margin-top: 0; margin-bottom: 1rem; color: #cbd5f5; }
      section { border: 1px solid #1e293b; padding: 1rem; margin-bottom: 1rem; border-radius: 8px; background-color: #1e293b; }
      label { display: block; margin-top: 0.5rem; font-size: 0.9rem; }
      input, select { width: 100%; padding: 0.4rem; border-radius: 4px; border: none; margin-top: 0.2rem; }
      button { margin-top: 0.8rem; padding: 0.6rem 1.2rem; border: none; border-radius: 6px; cursor: pointer; background-color: #38bdf8; color: #0f172a; font-weight: bold; }
      #status-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 1rem; }
      .event-list { max-height: 250px; overflow-y: auto; background-color: #0f172a; padding: 0.5rem; border-radius: 4px; font-size: 0.85rem; }
      .event-item { border-bottom: 1px solid #1e293b; padding: 0.4rem 0; }
      .event-item:last-child { border-bottom: none; }
      .error { color: #f87171; }
      .success { color: #4ade80; }
      .flex { display: flex; gap: 1rem; flex-wrap: wrap; }
      .flex > div { flex: 1 1 200px; }
      input[type=checkbox] { width: auto; display: inline-block; }
      .modal { position: fixed; inset: 0; background: rgba(15, 23, 42, 0.85); display: none; align-items: center; justify-content: center; z-index: 1000; }
      .modal.show { display: flex; }
      .modal-content { background: #1e293b; border-radius: 10px; padding: 1.5rem; width: min(800px, 90%); box-shadow: 0 15px 40px rgba(0, 0, 0, 0.45); }
      .modal textarea { width: 100%; min-height: 260px; border: none; border-radius: 6px; padding: 0.7rem; font-family: monospace; font-size: 0.95rem; background: #0f172a; color: #f8fafc; }
      .modal-actions { justify-content: flex-end; margin-top: 0.8rem; }
      .btn-secondary { background-color: #475569; color: #f8fafc; }
      #start-form { counter-reset: param; }
      #start-form label { position: relative; padding-left: 1.4rem; }
      #start-form label.no-counter { padding-left: 0; }
      #start-form label:not(.no-counter)::before {
        counter-increment: param;
        content: counter(param) ".";
        position: absolute;
        left: 0;
        top: 0;
        color: #94a3b8;
      }
    </style>
  </head>
  <body>
    <h1>Breakout Strategy Control</h1>
    <p class="note">
      Nhập vùng tích luỹ, tham số breakout và nhấn <strong>Start</strong> để chạy bot live hoặc dùng khung Backtest/FETCH history phía dưới.
      Mọi tham số sẽ được gửi tới chiến lược breakout (ATR filter, EMA trend, time filter, momentum...).
    </p>
    <section>
      <h2 style=\"margin-top:0\">Cấu hình breakout</h2>
      <p class=\"note\" style=\"margin-bottom:0.5rem;\">Chọn preset hoặc tuỳ chỉnh range, buffer, ATR filter và phiên giao dịch. Các giá trị này áp dụng cho cả Live/Backtest/Fetch.</p>
      <form id=\"start-form\">
        <div class=\"flex\">
          <div>
            <label>DB URL*</label>
            <input name=\"db_url\" placeholder=\"postgresql+asyncpg://user:pass@host/db\" required />
          </div>
          <div>
            <label>Symbol</label>
            <input name=\"symbol\" placeholder=\"XAUUSDc\" value=\"__DEFAULT_SYMBOL__\" />
          </div>
          <div>
            <label>Preset</label>
            <select name=\"preset\" id=\"preset-select\"></select>
          </div>
        </div>
        <div class=\"flex\" style=\"margin-top:1rem;\">
          <div style=\"flex:1 1 360px;\">
            <label>Paste config (key = value)</label>
            <button type=\"button\" id=\"bulk-config-open-btn\" style=\"width:100%;\">Mở popup dán cấu hình</button>
            <p class=\"note\" style=\"margin-top:0.4rem;\">Dán danh sách tham số dạng <code>key = value</code> hoặc JSON đầy đủ trong popup để auto điền toàn bộ form.</p>
            <p id=\"bulk-config-status\" class=\"note\" style=\"margin-top:0.2rem;\">Chưa áp dụng cấu hình nào.</p>
          </div>
        </div>

        <section style="margin-top:1rem;">
          <h3>I. Timeframe &amp; General</h3>
          <div class="flex">
            <div>
              <label>Fast / Slow EMA</label>
              <input name="fast" type="number" value="8" />
              <input name="slow" type="number" value="21" />
            </div>
            <div>
              <label>MA type</label>
              <select name="ma_type">
                <option value="ema" selected>EMA</option>
                <option value="sma">SMA</option>
              </select>
            </div>
            <div>
              <label>Timeframe</label>
              <input name="timeframe" value="5min" />
            </div>
            <div>
              <label>Volume (lot)</label>
              <input name="volume" type="number" step="0.01" value="0.1" />
            </div>
            <div>
              <label>Capital</label>
              <input name="capital" type="number" value="10000" />
            </div>
            <div>
              <label>Risk %</label>
              <input name="risk_pct" type="number" step="0.1" value="1.0" />
            </div>
          </div>
          <label class="no-counter" style="display:block; margin-top:0.5rem;"><input type="checkbox" name="size_from_risk" checked /> Size theo % risk</label>
        </section>

        <section>
          <h3>II. Trend Filter</h3>
          <div class="flex">
            <div>
              <label>EMA trend (chu kỳ)</label>
              <input name="trend" type="number" value="200" />
            </div>
            <div>
              <label>Market state window</label>
              <input name="market_state_window" type="number" value="40" />
            </div>
            <div>
              <label>Trading hours</label>
              <input name="trading_hours" placeholder="14:00-16:00,20:00-23:00" />
            </div>
            <div>
              <label>ADX window</label>
              <input name="adx_window" type="number" value="14" />
            </div>
            <div>
              <label>ADX threshold</label>
              <input name="adx_threshold" type="number" step="0.1" value="25" />
            </div>
          </div>
        </section>

        <section>
          <h3>III. Volatility &amp; Runtime</h3>
          <div class="flex">
            <div>
              <label>ATR baseline window</label>
              <input name="atr_baseline_window" type="number" value="14" />
            </div>
            <div>
              <label>ATR multiplier min</label>
              <input name="atr_multiplier_min" type="number" step="0.1" value="1.1" />
            </div>
            <div>
              <label>ATR multiplier max</label>
              <input name="atr_multiplier_max" type="number" step="0.1" value="3.2" />
            </div>
            <div>
              <label>Spread ATR max</label>
              <input name="spread_atr_max" type="number" step="0.01" value="0.08" />
            </div>
            <div>
              <label>Ensure history (hours)</label>
              <input name="ensure_history_hours" type="number" value="6" />
            </div>
            <div>
              <label>Poll (s)</label>
              <input name="poll" type="number" step="0.1" value="1.0" />
            </div>
          </div>
        </section>

        <section>
          <h3>IV. Breakout Conditions</h3>
          <div class="flex">
            <div>
              <label>Range lookback (bars)</label>
              <input name="range_lookback" type="number" value="40" />
            </div>
            <div>
              <label>Range min ATR (x)</label>
              <input name="range_min_atr" type="number" step="0.1" value="0.8" />
            </div>
            <div>
              <label>Range min points (USD)</label>
              <input name="range_min_points" type="number" step="0.1" value="1.0" />
            </div>
            <div>
              <label>Breakout buffer (ATR)</label>
              <input name="breakout_buffer_atr" type="number" step="0.01" value="0.3" />
            </div>
            <div>
              <label>Confirm bars</label>
              <input name="breakout_confirmation_bars" type="number" value="1" />
            </div>
          </div>
        </section>

        <section>
          <h3>V. Momentum Confirmation</h3>
          <div class="flex">
            <div>
              <label>Momentum type</label>
              <select name="momentum_type">
                <option value="hybrid" selected>Hybrid RSI + MACD</option>
                <option value="macd">MACD</option>
                <option value="pct">% Change</option>
              </select>
            </div>
            <div>
              <label>Momentum window</label>
              <input name="momentum_window" type="number" value="14" />
            </div>
            <div>
              <label>Momentum threshold (%)</label>
              <input name="momentum_threshold" type="number" step="any" value="0.07" />
            </div>
            <div>
              <label>MACD fast / slow / signal</label>
              <input name="macd_fast" type="number" value="12" />
              <input name="macd_slow" type="number" value="26" />
              <input name="macd_signal" type="number" value="9" />
            </div>
            <div>
              <label>MACD threshold</label>
              <input name="macd_threshold" type="number" step="any" value="0.0002" />
            </div>
            <div>
              <label>RSI long / short</label>
              <input name="rsi_threshold_long" type="number" step="any" value="60" />
              <input name="rsi_threshold_short" type="number" step="any" value="40" />
            </div>
          </div>
        </section>

        <section>
          <h3>VI. Trade Management</h3>
          <div class="flex">
            <div>
              <label>SL ATR</label>
              <input name="sl_atr" type="number" step="any" value="1.5" />
            </div>
            <div>
              <label>TP ATR</label>
              <input name="tp_atr" type="number" step="any" value="2.5" />
            </div>
            <div>
              <label>Trail trigger ATR</label>
              <input name="trail_trigger_atr" type="number" step="any" value="1.8" />
            </div>
            <div>
              <label>Trail ATR multiplier</label>
              <input name="trail_atr_mult" type="number" step="any" value="1.1" />
            </div>
            <div>
              <label>Contract size</label>
              <input name="contract_size" type="number" value="100" />
            </div>
            <div>
              <label>Pip size</label>
              <input name="pip_size" type="number" step="any" value="0.01" />
            </div>
            <div>
              <label>SL (pips)</label>
              <input name="sl_pips" type="number" step="any" placeholder="Nếu muốn cố định" />
            </div>
            <div>
              <label>TP (pips)</label>
              <input name="tp_pips" type="number" step="any" placeholder="Nếu muốn cố định" />
            </div>
          </div>
        </section>

        <section>
          <h3>VII. Risk Guard</h3>
          <div class="flex">
            <div>
              <label>Max daily loss (USD)</label>
              <input name="max_daily_loss" type="number" step="any" value="300" />
            </div>
            <div>
              <label>Max loss streak</label>
              <input name="max_loss_streak" type="number" min="0" value="3" />
            </div>
            <div>
              <label>Max losses / session</label>
              <input name="max_losses_per_session" type="number" min="0" value="2" />
            </div>
            <div>
              <label>Cooldown (phút)</label>
              <input name="cooldown_minutes" type="number" min="0" value="60" />
            </div>
          </div>
          <label class="no-counter" style="display:block; margin-top:0.5rem;"><input type="checkbox" name="ingest_live_db" /> Ghi tick realtime vào DB</label>
          <label class="no-counter" style="display:block;"><input type="checkbox" name="live" /> Gửi lệnh MT5 thật (live)</label>
        </section>

        <section>
          <h3>VIII. Backtest breakout</h3>
          <p class="note" style="margin-bottom:0.4rem;">Sử dụng toàn bộ tham số cấu hình phía trên. Nếu bỏ trống thời gian sẽ auto lấy 24h gần nhất.</p>
          <div class="flex">
            <div>
              <label>Backtest start</label>
              <input name="backtest_start" type="datetime-local" />
            </div>
            <div>
              <label>Backtest end</label>
              <input name="backtest_end" type="datetime-local" />
            </div>
          </div>
          <button type="button" id="backtest-btn">Chạy backtest</button>
          <div id="backtest-result" class="event-list" style="margin-top:0.5rem;">Chưa chạy backtest breakout</div>
        </section>

        <section>
          <h3>IX. Lịch sử &amp; CSV</h3>
          <p class="note" style="margin-bottom:0.4rem;">Fetch tick về DB (copy_ticks_range) hoặc tải CSV theo số ngày đã nhập.</p>
          <div class="flex">
            <div>
              <label>History start</label>
              <input name="history_start" type="datetime-local" />
            </div>
            <div>
              <label>History end</label>
              <input name="history_end" type="datetime-local" />
            </div>
            <div>
              <label>Batch size</label>
              <input name="history_batch" type="number" value="2000" />
            </div>
            <div>
              <label>Max days</label>
              <input name="history_max_days" type="number" value="5" />
            </div>
          </div>
          <div class="flex">
            <button type="button" id="fetch-history-btn" style="flex:1;">Fetch history</button>
            <button type="button" id="download-history-btn" style="flex:1;">Tải CSV</button>
          </div>
          <div id="history-result" class="event-list" style="margin-top:0.5rem;">Chưa fetch history breakout</div>
        </section>

        <div class="flex" style="gap:0.5rem;">
          <button type="submit" style="flex:1;">Start</button>
          <button type="button" id="stop-btn" style="flex:1;">Stop</button>
        </div>
      </form>

      <div id="message"></div>
      <div id="strategy-summary" class="event-list" style="margin-top:0.5rem;">Chưa có cấu hình breakout</div>
    </section>

    <div id="bulk-config-modal" class="modal" aria-hidden="true">
      <div class="modal-content">
        <h3 style="margin-top:0;">Dán cấu hình</h3>
        <p class="note" style="margin-top:0; margin-bottom:0.8rem;">
          Export CSV ticks, gửi cho AI phân tích rồi yêu cầu trả lại bộ tham số tối ưu theo các tiêu chí trong dashboard:
          timeframe &amp; fast/slow EMA/MA type, trend EMA + market_state_window, khung giờ giao dịch từng phiên, ADX window/threshold,
          ATR baseline &amp; multiplier/spread guard, breakout range/buffer/confirmation, momentum MACD+RSI (window, threshold),
          SL/TP ATR, trailing trigger/multiplier, risk_pct/capital/size_from_risk, cùng các guard về max_daily_loss/loss_streak/session/cooldown.
        </p>
        <textarea id="bulk-config-input" placeholder="fast = 8&#10;slow = 21&#10;timeframe = 5min&#10;..."></textarea>
        <div class="flex modal-actions" style="gap:0.5rem;">
          <button type="button" id="bulk-config-apply-btn">Áp dụng</button>
          <button type="button" id="bulk-config-close-btn" class="btn-secondary">Đóng</button>
        </div>
      </div>
    </div>

    <section>
      <h2>Trạng thái breakout</h2>
      <p class=\"note\" style=\"margin-bottom:0.5rem;\">Theo dõi quote, tín hiệu gần nhất và lý do chờ breakout (range/ATR/momentum). Tổng hợp cấu hình nằm ở khối dưới.</p>
      <div id=\"status-grid\">
        <div>Trạng thái: <span id=\"st-running\">-</span></div>
        <div>Symbol: <span id=\"st-symbol\">-</span></div>
        <div>Last quote: <span id=\"st-quote\">-</span></div>
        <div>PNL tích luỹ: <span id=\"st-pnl\">-</span></div>
        <div>Vị thế: <span id=\"st-position\">-</span></div>
        <div style=\"grid-column:1 / -1\">Đang chờ: <span id=\"st-waiting\">-</span></div>
      </div>
      <div class=\"flex\">
        <div>
          <h3>Tín hiệu mới nhất</h3>
          <div class=\"event-list\" id=\"signal-block\">-</div>
        </div>
        <div>
          <h3>Chi tiết vị thế</h3>
          <div class=\"event-list\" id=\"position-detail\">-</div>
        </div>
      </div>
    </section>

    <script>
      const form = document.getElementById('start-form');
      const stopBtn = document.getElementById('stop-btn');
      const messageEl = document.getElementById('message');
      const signalBlock = document.getElementById('signal-block');
      const positionDetail = document.getElementById('position-detail');
      const presetSelect = document.getElementById('preset-select');
      const summaryBox = document.getElementById('strategy-summary');
      const backtestBtn = document.getElementById('backtest-btn');
      const backtestResult = document.getElementById('backtest-result');
      const fetchHistoryBtn = document.getElementById('fetch-history-btn');
      const downloadHistoryBtn = document.getElementById('download-history-btn');
      const historyResult = document.getElementById('history-result');
      const bulkConfigInput = document.getElementById('bulk-config-input');
      const bulkConfigApplyBtn = document.getElementById('bulk-config-apply-btn');
      const bulkConfigStatus = document.getElementById('bulk-config-status');
      const bulkConfigModal = document.getElementById('bulk-config-modal');
      const bulkConfigOpenBtn = document.getElementById('bulk-config-open-btn');
      const bulkConfigCloseBtn = document.getElementById('bulk-config-close-btn');
      let presetCache = [];
      const DEFAULT_FETCH_TRADING_DAYS = 2;

      const fieldHelp = {
        db_url: 'Chuỗi kết nối CSDL async (Postgres/SQLite). Ví dụ: postgresql+asyncpg://user:pass@host/db',
        symbol: 'Mã giao dịch trong MT5, ví dụ XAUUSDc',
        preset: 'Chọn bộ tham số tối ưu sẵn theo market regime (tự điền fast/slow, momentum...)',
        fast: 'Chu kỳ MA nhanh (số bar) được dùng làm trigger',
        slow: 'Chu kỳ MA chậm (số bar) để so sánh với MA nhanh',
        ma_type: 'Loại trung bình động: EMA phản ứng nhanh hơn, SMA mượt hơn',
        timeframe: 'Khung thời gian để resample tick (ví dụ 1min, 5min, 15min)',
        volume: 'Khối lượng mặc định (lot). Nếu bật size theo % risk sẽ bị ghi đè',
        capital: 'Vốn quy đổi USD dùng để tính khối lượng khi bật size-from-risk',
        risk_pct: '% vốn rủi ro mỗi lệnh (dùng với size-from-risk)',
        ensure_history_hours: 'Số giờ dữ liệu tối thiểu cần có trong DB trước khi chạy. Thiếu sẽ tự fetch MT5',
        poll: 'Chu kỳ lấy quote từ MT5 (giây). Giá trị nhỏ => phản ứng nhanh hơn',
        spread_atr_max: 'Ngưỡng spread tối đa (tính theo ATR). Spread cao hơn sẽ bỏ qua tín hiệu',
        trend: 'Chu kỳ EMA trend giúp xác nhận hướng chính (ví dụ EMA200)',
        market_state_window: 'Số bar để đánh giá vùng sideway/trạng thái thị trường',
        trading_hours: 'Giới hạn khung giờ trade, ví dụ 14:00-16:00,20:00-23:00 (giờ VN)',
        adx_window: 'Số nến dùng để tính ADX nhằm đo sức mạnh xu hướng',
        adx_threshold: 'ADX phải lớn hơn hoặc bằng ngưỡng này mới kích hoạt tín hiệu (0 = bỏ qua)',
        momentum_type: 'Chọn bộ lọc xung lực: MACD, %Change hoặc Hybrid (MACD + RSI)',
        momentum_window: 'Số bar nhìn lại khi dùng %change hoặc RSI',
        momentum_threshold: 'Ngưỡng % thay đổi tối thiểu để xác nhận breakout (%change/hybrid)',
        rsi_threshold_long: 'RSI tối thiểu cho lệnh Long (ví dụ >60)',
        rsi_threshold_short: 'RSI tối đa cho lệnh Short (ví dụ <40)',
        range_lookback: 'Số bar dùng để đo vùng tích luỹ gần nhất',
        range_min_atr: 'Độ cao range tối thiểu tính theo ATR (range >= ATR * hệ số)',
        range_min_points: 'Độ cao range tối thiểu tuyệt đối (USD)',
        breakout_buffer_atr: 'Khoảng đệm cộng thêm vào S/R trước khi xác nhận breakout',
        breakout_confirmation_bars: 'Số nến đóng ngoài vùng cần có để xác nhận',
        atr_baseline_window: 'Số bar để tính ATR trung bình làm baseline so sánh',
        atr_multiplier_min: 'ATR phải lớn hơn baseline * hệ số này',
        atr_multiplier_max: 'ATR phải nhỏ hơn baseline * hệ số này',
        trail_trigger_atr: 'ATR tại đó bật trailing stop',
        trail_atr_mult: 'Khoảng dịch SL mỗi lần trail = ATR * hệ số này',
        max_daily_loss: 'Giới hạn lỗ tuyệt đối (USD) trong 1 ngày, vượt ngưỡng sẽ ngưng mở lệnh',
        max_loss_streak: 'Số lệnh thua liên tục tối đa trước khi bot tạm dừng',
        max_losses_per_session: 'Số lệnh thua tối đa trong một phiên giao dịch',
        cooldown_minutes: 'Thời gian nghỉ sau khi chạm guard trước khi trade lại',
        macd_fast: 'Chu kỳ EMA nhanh trong MACD (mặc định 12)',
        macd_slow: 'Chu kỳ EMA chậm trong MACD (mặc định 26)',
        macd_signal: 'Chu kỳ đường tín hiệu MACD (mặc định 9)',
        macd_threshold: 'Ngưỡng histogram MACD ( >0 mới xác nhận mua, <0 xác nhận bán )',
        contract_size: 'Hệ số quy đổi PnL (ví dụ XAUUSD ~100 oz/lot)',
        sl_atr: 'Hệ số ATR dùng để đặt Stop Loss',
        tp_atr: 'Hệ số ATR dùng để đặt Take Profit',
        size_from_risk: 'Nếu bật, bot sẽ tính volume dựa trên capital/risk_pct/contract_size',
        ingest_live_db: 'Bật để lưu mọi quote khi chạy live vào DB phục vụ backtest sau',
        live: 'Bật để gửi lệnh thật tới MT5. Nếu tắt sẽ chạy chế độ paper',
        backtest_start: 'Thời điểm bắt đầu backtest (ISO 8601)',
        backtest_end: 'Thời điểm kết thúc backtest (ISO 8601)',
        history_start: 'Thời điểm bắt đầu fetch lịch sử từ MT5 (ISO 8601). Để trống sẽ tự lùi 2 ngày giao dịch gần nhất',
        history_end: 'Thời điểm kết thúc fetch lịch sử (ISO 8601). Để trống sẽ dùng thời gian hiện tại',
        history_batch: 'Số dòng ghi mỗi batch insert vào DB',
        history_max_days: 'Số ngày tối đa cho mỗi lần gọi MT5 copy_ticks_range và độ dài CSV tải xuống',
      };

      form.addEventListener('submit', async (e) => {
        e.preventDefault();
        const payload = buildPayload(new FormData(form));
        try {
          const res = await fetch('/api/live/start', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(payload),
          });
          const data = await res.json();
          if (!res.ok) {
            throw new Error(data.detail || 'Không thể start');
          }
          messageEl.textContent = 'Đã start breakout live';
          messageEl.className = 'success';
        } catch (err) {
          messageEl.textContent = err.message;
          messageEl.className = 'error';
        }
        refreshStatus();
      });

      stopBtn.addEventListener('click', async () => {
        try {
          await fetch('/api/live/stop', { method: 'POST' });
          messageEl.textContent = 'Đã stop breakout';
          messageEl.className = 'success';
        } catch (err) {
          messageEl.textContent = err.message;
          messageEl.className = 'error';
        }
        refreshStatus();
      });

      if (backtestBtn) {
        backtestBtn.addEventListener('click', runBacktest);
      }
      if (fetchHistoryBtn) {
        fetchHistoryBtn.addEventListener('click', runFetchHistory);
      }
      if (downloadHistoryBtn) {
        downloadHistoryBtn.addEventListener('click', downloadHistoryCsv);
      }
      if (bulkConfigOpenBtn) {
        bulkConfigOpenBtn.addEventListener('click', () => openBulkConfigModal());
      }
      if (bulkConfigCloseBtn) {
        bulkConfigCloseBtn.addEventListener('click', () => closeBulkConfigModal());
      }
      if (bulkConfigModal) {
        bulkConfigModal.addEventListener('click', (event) => {
          if (event.target === bulkConfigModal) {
            closeBulkConfigModal();
          }
        });
      }
      if (bulkConfigApplyBtn) {
        bulkConfigApplyBtn.addEventListener('click', () => applyBulkConfig(bulkConfigInput?.value || ''));
      }

      function buildPayload(formData) {
        const payload = {};
        for (const [key, value] of formData.entries()) {
          if (value === '' && key !== 'symbol') continue;
          if (
            [
              'fast',
              'slow',
              'trend',
              'market_state_window',
              'history_batch',
              'history_max_days',
              'momentum_window',
              'macd_fast',
              'macd_slow',
              'macd_signal',
              'range_lookback',
              'breakout_confirmation_bars',
              'atr_baseline_window',
              'adx_window',
              'max_loss_streak',
              'max_losses_per_session',
              'cooldown_minutes',
            ].includes(key)
          ) {
            payload[key] = parseInt(value, 10);
          } else if (
            [
              'volume',
              'capital',
              'risk_pct',
              'spread_atr_max',
              'sl_atr',
              'tp_atr',
              'trail_trigger_atr',
              'trail_atr_mult',
              'poll',
              'ensure_history_hours',
              'pip_size',
              'contract_size',
              'momentum_threshold',
              'macd_threshold',
              'rsi_threshold_long',
              'rsi_threshold_short',
              'range_min_atr',
              'range_min_points',
              'breakout_buffer_atr',
              'atr_multiplier_min',
              'atr_multiplier_max',
              'max_daily_loss',
              'adx_threshold',
            ].includes(key)
          ) {
            payload[key] = parseFloat(value);
          } else if (key === 'size_from_risk' || key === 'ingest_live_db' || key === 'live') {
            payload[key] = form[key].checked;
          } else {
            payload[key] = value;
          }
        }
        return payload;
      }

      function applySuggestionToForm(suggestion = {}) {
        Object.entries(suggestion).forEach(([key, value]) => {
          const control = form.elements[key];
          if (!control) return;
          if (typeof RadioNodeList !== 'undefined' && control instanceof RadioNodeList) {
            control.value = value ?? '';
            return;
          }
          if (control.type === 'checkbox') {
            control.checked = Boolean(value);
          } else {
            control.value = value ?? '';
          }
        });
      }

      async function populatePresets() {
        const res = await fetch('/api/live/presets');
        presetCache = await res.json();
        presetSelect.innerHTML = '';
        const customOpt = document.createElement('option');
        customOpt.value = '';
        customOpt.textContent = 'Custom';
        presetSelect.appendChild(customOpt);
        presetCache.forEach((preset) => {
          const opt = document.createElement('option');
          opt.value = preset.id;
          opt.textContent = `${preset.name} (${preset.description})`;
          presetSelect.appendChild(opt);
        });
          presetSelect.addEventListener('change', () => {
            const preset = presetCache.find((p) => p.id === presetSelect.value);
            if (!preset) return;
            form.fast.value = preset.fast_ma;
            form.slow.value = preset.slow_ma;
            form.ma_type.value = preset.ma_type;
            form.timeframe.value = preset.timeframe;
            form.spread_atr_max.value = preset.spread_atr_max;
            form.momentum_type.value = preset.momentum_type;
            form.momentum_window.value = preset.momentum_window;
            form.momentum_threshold.value = preset.momentum_threshold;
            form.macd_fast.value = preset.macd_fast;
            form.macd_slow.value = preset.macd_slow;
            form.macd_signal.value = preset.macd_signal;
            form.macd_threshold.value = preset.macd_threshold;
            form.sl_atr.value = preset.sl_atr;
            form.tp_atr.value = preset.tp_atr;
            form.range_lookback.value = preset.range_lookback;
            form.range_min_atr.value = preset.range_min_atr;
            form.range_min_points.value = preset.range_min_points;
            form.breakout_buffer_atr.value = preset.breakout_buffer_atr;
            form.breakout_confirmation_bars.value = preset.breakout_confirmation_bars;
            form.atr_baseline_window.value = preset.atr_baseline_window;
            form.atr_multiplier_min.value = preset.atr_multiplier_min;
            form.atr_multiplier_max.value = preset.atr_multiplier_max;
            form.trading_hours.value = preset.trading_hours || '';
          });
        }

      async function refreshStatus() {
        const res = await fetch('/api/live/status');
        const data = await res.json();
        document.getElementById('st-running').textContent = data.running ? 'Đang chạy' : 'Đang dừng';
        document.getElementById('st-symbol').textContent = data.config?.symbol || '-';
        document.getElementById('st-quote').textContent = data.last_quote ? data.last_quote.toFixed(3) : '-';
        document.getElementById('st-pnl').textContent = data.cumulative_pnl?.toFixed(2) || '-';
        if (data.current_position) {
          const pos = data.current_position;
          document.getElementById('st-position').textContent = `${pos.side} @ ${pos.price}`;
        } else {
          document.getElementById('st-position').textContent = 'No position';
        }
        document.getElementById('st-waiting').textContent = formatWaitingReason(data.last_signal, data.waiting_reason);
        signalBlock.innerHTML = formatSignal(data.last_signal);
        positionDetail.innerHTML = formatPositionDetail(data.current_position);
        summaryBox.innerHTML = formatConfigSummary(data.config, data.cli_command);
      }

      async function runBacktest() {
        let start = form.backtest_start.value;
        let end = form.backtest_end.value;
        if (!start || !end) {
          const now = new Date();
          if (!end) {
            end = new Date(now).toISOString().slice(0, 16);
            form.backtest_end.value = end;
          }
          if (!start) {
            const endDate = new Date(end);
            const startDate = new Date(endDate.getTime() - 24 * 60 * 60 * 1000);
            start = startDate.toISOString().slice(0, 16);
            form.backtest_start.value = start;
          }
        }
        const payload = buildPayload(new FormData(form));
        payload.start = start;
        payload.end = end;
        payload.db_url = form.db_url.value;
        payload.symbol = form.symbol.value || 'XAUUSDc';
        backtestResult.textContent = 'Đang chạy backtest breakout...';
        try {
          const res = await fetch('/api/backtest/run', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(payload),
          });
          const data = await res.json();
          if (!res.ok) {
            throw new Error(data.detail || 'Backtest thất bại');
          }
          backtestResult.innerHTML = formatBacktestResult(data);
        } catch (err) {
          backtestResult.textContent = err.message;
        }
      }

      async function runFetchHistory() {
        const dbUrl = form.db_url.value.trim();
        if (!dbUrl) {
          setHistoryMessage('Vui lòng nhập DB URL trước', 'error');
          return;
        }
        let end = form.history_end.value;
        let start = form.history_start.value;
        const now = new Date();
        if (!end) {
          end = new Date(now).toISOString().slice(0, 16);
          form.history_end.value = end;
        }
        if (!start) {
          const endDate = new Date(end);
          const startDate = subtractTradingDays(endDate, DEFAULT_FETCH_TRADING_DAYS);
          start = startDate.toISOString().slice(0, 16);
          form.history_start.value = start;
        }
        const batch = parseInt(form.history_batch.value || '2000', 10);
        const maxDays = parseInt(form.history_max_days.value || '5', 10);
        if (!Number.isFinite(batch) || batch <= 0) {
          setHistoryMessage('Batch size phải > 0', 'error');
          return;
        }
        if (!Number.isFinite(maxDays) || maxDays <= 0) {
          setHistoryMessage('Max days phải > 0', 'error');
          return;
        }
        const payload = {
          db_url: dbUrl,
          symbol: form.symbol.value || 'XAUUSDc',
          start,
          end,
          batch,
          max_days: maxDays,
        };
        setHistoryMessage('Đang fetch dữ liệu từ MT5...', null);
        try {
          const res = await fetch('/api/history/fetch', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(payload),
          });
          const data = await res.json();
          if (!res.ok) {
            throw new Error(data.detail || 'Fetch history thất bại');
          }
          setHistoryMessage(
            `Đã fetch ${data.symbol} (${formatTimestamp(data.start)} -> ${formatTimestamp(data.end)})`,
            'success',
          );
        } catch (err) {
          setHistoryMessage(err.message || 'Fetch history thất bại', 'error');
        }
      }

      async function downloadHistoryCsv() {
        const dbUrl = form.db_url.value.trim();
        if (!dbUrl) {
          setHistoryMessage('Vui lòng nhập DB URL trước', 'error');
          return;
        }
        const symbol = form.symbol.value || '__DEFAULT_SYMBOL__';
        const daysRaw = parseFloat(form.history_max_days.value || '1');
        const tradingDays = Number.isFinite(daysRaw) && daysRaw > 0 ? daysRaw : 1;
        const rawHours = tradingDays * 24;
        const weekendDays = estimateWeekendDays(tradingDays);
        const weekendHours = weekendDays * 24;
        const totalHours = rawHours + weekendHours;
        const hours = Math.max(totalHours, 1);
        const effectiveDays = hours / 24;
        const payload = {
          db_url: dbUrl,
          symbol,
          hours,
        };
        setHistoryMessage(
          `Đang chuẩn bị CSV ~${effectiveDays.toFixed(2)} ngày (cộng ${weekendDays} ngày nghỉ)`,
          null,
        );
        try {
          const res = await fetch('/api/history/download', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(payload),
          });
          if (!res.ok) {
            let detail = 'Không thể tải CSV';
            try {
              const data = await res.json();
              detail = data.detail || detail;
            } catch (e) {
              // ignore
            }
            throw new Error(detail);
          }
          const blob = await res.blob();
          const url = window.URL.createObjectURL(blob);
          const a = document.createElement('a');
          const ts = new Date().toISOString().replace(/[:.]/g, '-');
          a.href = url;
          a.download = `ticks_${symbol}_last24h_${ts}.csv`;
          document.body.appendChild(a);
          a.click();
          a.remove();
          window.URL.revokeObjectURL(url);
          setHistoryMessage(
            `Đã tải CSV ~${effectiveDays.toFixed(2)} ngày (bao gồm ${weekendDays} ngày nghỉ)`,
            'success',
          );
        } catch (err) {
          setHistoryMessage(err.message || 'Không thể tải CSV', 'error');
        }
      }

      function formatSignal(signal) {
        if (!signal) return 'Chưa có tín hiệu';
        const ts = formatTimestamp(signal.timestamp);
        const side = signal.side ? String(signal.side).toUpperCase() : '';
        if (signal.type === 'position_open') {
          return `${ts}<br />OPEN ${side} @ ${formatNum(signal.price)}<br />Vol: ${signal.volume} | SL: ${formatNum(signal.stop_loss)} | TP: ${formatNum(signal.take_profit)}`;
        }
        if (signal.type === 'position_close') {
          return `${ts}<br />CLOSE ${side} @ ${formatNum(signal.close_price)}<br />PnL: ${formatNum(signal.pnl_value, 2)} (${formatNum(signal.pnl_points)})`;
        }
        if (signal.type === 'error') {
          return `${ts}<br />ERROR ${signal.message}`;
        }
        return `${ts}<br />${signal.type || 'event'}`;
      }

      function formatWaitingReason(signal, fallback) {
        if (signal && signal.type === 'status') {
          const extra = signal.extra || {};
          const info = Object.entries(extra)
            .map(([k, v]) => `${k}: ${v}`)
            .join(' | ');
          return info ? `${signal.reason} (${info})` : signal.reason;
        }
        return fallback || 'Đang chờ tín hiệu';
      }

      function formatPositionDetail(pos) {
        if (!pos) return 'Không có vị thế';
        const lines = [
          `Side: <strong>${String(pos.side).toUpperCase()}</strong>`,
          `Entry: ${formatNum(pos.price)}`,
          `Volume: ${pos.volume}`,
          `SL: ${formatNum(pos.stop_loss)} | TP: ${formatNum(pos.take_profit)}`,
          `Opened: ${formatTimestamp(pos.timestamp)}`,
        ];
        return lines.join('<br />');
      }

      function formatConfigSummary(cfg, cliCmd) {
        if (!cfg) return 'Chưa có cấu hình';
        const items = [
          `Preset: ${cfg.preset || 'Custom'}`,
          `MA: ${cfg.fast_ma}/${cfg.slow_ma} (${cfg.ma_type?.toUpperCase()})`,
          `Timeframe: ${cfg.timeframe}`,
          `Trend EMA: ${cfg.trend_ma || '-'} | ADX ≥ ${cfg.adx_threshold || 0}`,
          `Momentum: ${cfg.momentum_type?.toUpperCase()} (MACD ${cfg.macd_fast || '-'} / ${cfg.macd_slow || '-'} / ${cfg.macd_signal || '-'}, RSI ${cfg.rsi_threshold_long ?? '-'} / ${cfg.rsi_threshold_short ?? '-'})`,
          `Breakout: lookback ${cfg.range_lookback || '-'} | buffer ATR ${cfg.breakout_buffer_atr || '-'}`,
          `ATR filter: baseline ${cfg.atr_baseline_window || '-'} | min ${cfg.atr_multiplier_min || '-'} | max ${cfg.atr_multiplier_max || '-'}`,
          cfg.trading_hours
            ? `Sessions: ${
                Array.isArray(cfg.trading_hours) ? cfg.trading_hours.join(', ') : cfg.trading_hours
              }`
            : '',
          `Risk: capital ${cfg.capital || '-'} | risk% ${cfg.risk_pct || '-'}`,
          `ATR SL/TP: ${cfg.sl_atr || '-'} / ${cfg.tp_atr || '-'}`,
          `Trailing: trigger ${cfg.trail_trigger_atr || '-'} ATR | mult ${cfg.trail_atr_mult || '-'}`,
          cfg.max_daily_loss
            ? `Risk guard: daily ${cfg.max_daily_loss} | streak ${cfg.max_consecutive_losses ?? '-'} | session ${cfg.max_losses_per_session ?? '-'}`.trim()
            : '',
          `Spread ATR max: ${cfg.spread_atr_max || '-'}`,
          cliCmd ? `CLI: <code>${cliCmd}</code>` : '',
        ];
        return items.filter(Boolean).join('<br />');
      }

      function formatBacktestResult(data) {
        if (!data || !data.total_trades) {
          return 'Không có kết quả backtest';
        }
        return [
          `Số lệnh: ${data.total_trades} | Thắng: ${data.wins} | Thua: ${data.losses}`,
          `PnL (price units): ${formatNum(data.total_pnl, 6)} | Avg: ${formatNum(data.avg_pnl, 6)}`,
          `PnL (USD): ${formatNum(data.total_usd_pnl, 2)} | Avg: ${formatNum(data.avg_usd_pnl, 2)}`,
          data.csv_path ? `CSV: ${data.csv_path}` : '',
          data.cli_command ? `CLI: <code>${data.cli_command}</code>` : '',
        ].filter(Boolean).join('<br />');
      }

      function setHistoryMessage(text, status) {
        historyResult.textContent = text;
        historyResult.classList.remove('error', 'success');
        if (status) {
          historyResult.classList.add(status);
        }
      }

      function formatTimestamp(value) {
        if (!value) return '-';
        try {
          return new Date(value).toLocaleString();
        } catch (err) {
          return String(value);
        }
      }

      function formatNum(val, digits = 3) {
        const num = Number(val);
        return Number.isFinite(num) ? num.toFixed(digits) : '-';
      }

      function openBulkConfigModal() {
        if (!bulkConfigModal) return;
        bulkConfigModal.classList.add('show');
        setTimeout(() => bulkConfigInput?.focus(), 50);
      }

      function closeBulkConfigModal() {
        bulkConfigModal?.classList.remove('show');
      }

      function estimateWeekendDays(tradingDays) {
        const need = Math.max(0, Math.ceil(tradingDays));
        if (!need) return 0;
        let remaining = need;
        let weekendCount = 0;
        const cursor = new Date();
        while (remaining > 0) {
          const dow = cursor.getUTCDay();
          if (dow === 0 || dow === 6) {
            weekendCount += 1;
          } else {
            remaining -= 1;
          }
          cursor.setDate(cursor.getDate() - 1);
        }
        return weekendCount;
      }

      function subtractTradingDays(endDate, tradingDays) {
        if (!(endDate instanceof Date) || Number.isNaN(endDate)) {
          return new Date();
        }
        const cursor = new Date(endDate);
        let remaining = Math.max(1, Math.ceil(tradingDays));
        while (remaining > 0) {
          cursor.setDate(cursor.getDate() - 1);
          const dow = cursor.getUTCDay();
          if (dow !== 0 && dow !== 6) {
            remaining -= 1;
          }
        }
        return cursor;
      }

      function applyBulkConfig(rawText) {
        const content = (rawText || '').trim();
        if (!content) {
          setBulkConfigStatus('Vui lòng dán danh sách tham số theo định dạng key = value', 'error');
          return;
        }
        try {
          const parsed = parseBulkConfigText(content);
          const keys = Object.keys(parsed);
          if (!keys.length) {
            throw new Error('Không tìm thấy tham số hợp lệ trong nội dung đã dán');
          }
          applySuggestionToForm(parsed);
          setBulkConfigStatus(`Đã áp dụng ${keys.length} tham số: ${keys.slice(0, 5).join(', ')}${keys.length > 5 ? '…' : ''}`, 'success');
          closeBulkConfigModal();
        } catch (err) {
          setBulkConfigStatus(err.message || 'Không thể phân tích danh sách tham số', 'error');
        }
      }

      function parseBulkConfigText(rawText) {
        const result = {};
        const listFields = new Set(['trading_hours']);
        if (rawText.trim().startsWith('{')) {
          let data;
          try {
            data = JSON.parse(rawText);
          } catch (err) {
            throw new Error('JSON không hợp lệ, vui lòng kiểm tra lại cú pháp');
          }
          Object.entries(data || {}).forEach(([key, value]) => {
            result[key] = coerceBulkValue(key, value, listFields);
          });
          return result;
        }
        rawText.split(/\\n+/).forEach((line, idx) => {
          const stripped = line.replace(/#.*/, '').replace(/\/\/.*/, '').trim();
          if (!stripped) return;
          const match = stripped.match(/^([^=:]+)\s*[:=]\s*(.+)$/);
          if (!match) {
            throw new Error(`Không nhận diện được dòng ${idx + 1}: "${line.trim()}"`);
          }
          const key = match[1].trim();
          let value = match[2].trim();
          if (
            (value.startsWith('"') && value.endsWith('"')) ||
            (value.startsWith("'") && value.endsWith("'"))
          ) {
            value = value.slice(1, -1);
          }
          result[key] = coerceBulkValue(key, value, listFields);
        });
        return result;
      }

      function coerceBulkValue(key, value, listFields) {
        if (value === null || value === undefined) return '';
        if (typeof value === 'number' || typeof value === 'boolean') {
          return value;
        }
        let text = String(value).trim();
        if (!text) return '';
        const lower = text.toLowerCase();
        if (['true', 'yes', 'on', '1'].includes(lower)) return true;
        if (['false', 'no', 'off', '0'].includes(lower)) return false;
        if (/^\d+(\.\d+)?%$/.test(text)) {
          return parseFloat(text.replace('%', ''));
        }
        const numVal = Number(text);
        if (!Number.isNaN(numVal)) {
          return numVal;
        }
        if (listFields.has(key)) {
          return text
            .split(',')
            .map((token) => token.trim())
            .filter(Boolean)
            .join(',');
        }
        return text;
      }

      function setBulkConfigStatus(message, type) {
        if (!bulkConfigStatus) return;
        bulkConfigStatus.className = type ? `note ${type}` : 'note';
        bulkConfigStatus.textContent = message;
      }

      function applyFieldHelp() {
        Object.entries(fieldHelp).forEach(([name, text]) => {
          const el = form.elements[name];
          if (!el) return;
          if (el instanceof RadioNodeList) {
            el.forEach((child) => {
              if (child && child.title !== undefined) child.title = text;
            });
          } else if (el instanceof HTMLElement) {
            el.title = text;
          }
        });
      }

      populatePresets();
      applyFieldHelp();
      refreshStatus();
      setInterval(refreshStatus, 5000);
    </script>
  </body>
</html>
""".replace("__DEFAULT_SYMBOL__", settings.quote_symbol)





BREAKOUT_DASHBOARD_HTML = """
<!doctype html>
<html lang="vi">
  <head>
    <meta charset="utf-8" />
    <title>Breakout Strategy Planner</title>
    <style>
      :root {
        color-scheme: dark;
      }
      body { font-family: 'Inter', Arial, sans-serif; margin: 24px; background-color: #020617; color: #e2e8f0; }
      h1, h2, h3 { margin-bottom: 0.4rem; }
      section { border: 1px solid #1d273b; padding: 1.25rem; border-radius: 10px; background: #0f172a; margin-bottom: 1.5rem; }
      .grid { display: grid; gap: 1rem; grid-template-columns: repeat(auto-fit, minmax(220px, 1fr)); }
      .flex { display: flex; flex-wrap: wrap; gap: 1rem; }
      label { font-size: 0.85rem; opacity: 0.8; }
      input, select { width: 100%; padding: 0.45rem 0.55rem; background: #020617; color: #f8fafc; border: 1px solid #1e293b; border-radius: 6px; margin-top: 0.25rem; }
      button { padding: 0.65rem 1.5rem; border-radius: 999px; border: none; background: linear-gradient(120deg, #34d399, #38bdf8); color: #041223; font-weight: 600; cursor: pointer; }
      button.secondary { background: #1e293b; color: #e2e8f0; }
      .note { font-size: 0.85rem; opacity: 0.9; }
      .list { margin: 0.4rem 0 0 1rem; padding: 0; }
      .list li { margin-bottom: 0.25rem; }
      .card { background: #020617; border-radius: 10px; padding: 0.75rem; border: 1px solid #1e293b; }
      .badge { display: inline-block; padding: 0.15rem 0.5rem; font-size: 0.75rem; border-radius: 999px; background: #1e40af; }
      #plan-output, #quote-box { background: #020617; border: 1px dashed #334155; padding: 0.75rem; border-radius: 8px; font-size: 0.9rem; min-height: 100px; }
      table { width: 100%; border-collapse: collapse; margin-top: 0.5rem; font-size: 0.9rem; }
      th, td { padding: 0.35rem 0.45rem; border-bottom: 1px solid #1e293b; text-align: left; }
      code { background: #1e293b; padding: 0.2rem 0.35rem; border-radius: 4px; }
    </style>
  </head>
  <body>
    <h1>Breakout Strategy Planner</h1>
    <p class="note">
      Chiến lược Breakout tập trung vào việc bắt đầu xu hướng mới ngay khi giá phá vỡ vùng tích lũy quan trọng. Trang này giúp bạn lập kế hoạch giao dịch
      (entry/stop/take-profit, sizing) dựa trên vùng hỗ trợ/kháng cự vừa quan sát được và theo dõi quote XAUUSDc thời gian thực.
    </p>

    <section>
      <h2>1. Tư duy chiến lược</h2>
      <div class="grid">
        <div class="card">
          <span class="badge">Bước 1</span>
          <h3>Xác định phạm vi</h3>
          <p>Chọn khung thời gian chính, quan sát vùng tích lũy gần nhất (support/resistance), đo độ cao range để ước lượng biên độ breakout.</p>
        </div>
        <div class="card">
          <span class="badge">Bước 2</span>
          <h3>Xác nhận tín hiệu</h3>
          <p>Dùng khối lượng hoặc biến động gia tốc (ATR, % thay đổi) để tránh phá vỡ giả. Cần ít nhất <strong>n</strong> nến đóng cửa ngoài vùng.</p>
        </div>
        <div class="card">
          <span class="badge">Bước 3</span>
          <h3>Quản trị rủi ro</h3>
          <p>Đặt stop-loss ở mép đối diện range hoặc dưới hỗ trợ cũ/kháng cự cũ. Chốt lời dựa trên RR cố định hoặc vùng cản kế tiếp.</p>
        </div>
        <div class="card">
          <span class="badge">Bước 4</span>
          <h3>Theo dõi</h3>
          <p>Cập nhật quote realtime, ghi chú lý do vào lệnh và yếu tố vô hiệu (invalidations) để cải thiện kỷ luật chiến lược.</p>
        </div>
      </div>
    </section>

    <section>
      <h2>2. Lập kế hoạch Breakout</h2>
      <form id="breakout-form">
        <div class="grid">
          <div>
            <label>Symbol</label>
            <input name="symbol" value="__DEFAULT_SYMBOL__" />
          </div>
          <div>
            <label>Timeframe chính</label>
            <input name="timeframe" value="15min" />
          </div>
          <div>
            <label>Hướng giao dịch</label>
            <select name="direction">
              <option value="bullish">Breakout tăng (mua)</option>
              <option value="bearish">Breakdown giảm (bán)</option>
            </select>
          </div>
          <div>
            <label>Số nến quan sát (lookback)</label>
            <input type="number" name="lookback" value="40" min="5" />
          </div>
        </div>
        <div class="grid" style="margin-top: 1rem;">
          <div>
            <label>Kháng cự gần nhất</label>
            <input type="number" step="any" name="resistance" placeholder="Ví dụ 2435.0" />
          </div>
          <div>
            <label>Hỗ trợ gần nhất</label>
            <input type="number" step="any" name="support" placeholder="Ví dụ 2422.0" />
          </div>
          <div>
            <label>Biên đệm breakout (%)</label>
            <input type="number" step="any" name="buffer_pct" value="0.15" />
          </div>
          <div>
            <label>Xác nhận khối lượng (hệ số)</label>
            <input type="number" step="any" name="volume_mult" value="1.5" />
          </div>
        </div>
        <div class="grid" style="margin-top: 1rem;">
          <div>
            <label>Số nến đóng cửa xác nhận</label>
            <input type="number" name="confirmation" value="2" min="1" />
          </div>
          <div>
            <label>Khoảng cách Stop (USD)</label>
            <input type="number" step="any" name="stop_offset" value="1.5" />
          </div>
          <div>
            <label>Tỷ lệ RR mục tiêu</label>
            <input type="number" step="any" name="rr" value="2.0" />
          </div>
          <div>
            <label>Risk % / Trade</label>
            <input type="number" step="any" name="risk_pct" value="1.0" />
          </div>
        </div>
        <div class="grid" style="margin-top: 1rem;">
          <div>
            <label>Capital (USD)</label>
            <input type="number" name="capital" value="10000" />
          </div>
          <div>
            <label>Contract size (oz/lot)</label>
            <input type="number" name="contract" value="100" />
          </div>
          <div>
            <label>Trailing kích hoạt (ATR)</label>
            <input type="number" step="any" name="trail_atr" value="1.0" />
          </div>
          <div>
            <label>Ghi chú</label>
            <input name="notes" placeholder="Tin tức, vùng invalidate..." />
          </div>
        </div>
        <div style="margin-top: 1rem;" class="flex">
          <button type="submit">Tạo kế hoạch</button>
          <button type="button" class="secondary" id="clear-btn">Xóa</button>
        </div>
      </form>
      <div style="margin-top: 1rem;">
        <h3>Kế hoạch giao dịch</h3>
        <div id="plan-output">Nhập vùng giá + tham số để tính toán entry/stop/take-profit.</div>
      </div>
    </section>

    <section>
      <h2>3. Quote realtime & checklist</h2>
      <div class="grid">
        <div>
          <h3>Quote XAUUSDc</h3>
          <div id="quote-box">Đang tải...</div>
        </div>
        <div>
          <h3>Checklist Breakout</h3>
          <table>
            <tbody>
              <tr><td>Range rõ ràng?</td><td id="chk-range">-</td></tr>
              <tr><td>Khối lượng tăng ≥ hệ số?</td><td id="chk-volume">-</td></tr>
              <tr><td>Nến đóng ngoài vùng ≥ yêu cầu?</td><td id="chk-close">-</td></tr>
              <tr><td>Tin tức lớn?</td><td id="chk-news">Theo dõi lịch</td></tr>
              <tr><td>Stop đặt đúng logic?</td><td id="chk-stop">-</td></tr>
            </tbody>
          </table>
        </div>
        <div>
          <h3>Ghi chú</h3>
          <div id="note-box" class="card" style="min-height: 100px;">Thêm ghi chú chiến lược ở mục trên để hiển thị tại đây.</div>
        </div>
      </div>
    </section>

    <section>
      <h2>4. Hướng dẫn triển khai thực tế</h2>
      <ul class="list">
        <li>Sử dụng form trên để xác định tham số nhanh trước khi triển khai chiến lược thật (CLI hoặc dashboard MA hiện tại).</li>
        <li>Khi cần tự động hóa: triển khai <code>breakout_strategy</code> riêng (logic: quét range, xác nhận breakout, gửi lệnh) và tái sử dụng API / storage sẵn có.</li>
        <li>Nên backtest bằng <code>app.cli</code> với dữ liệu tick hiện hữu để điều chỉnh buffer, RR và xác nhận khối lượng phù hợp với từng phiên.</li>
      </ul>
    </section>

    <script>
      const form = document.getElementById('breakout-form');
      const planOutput = document.getElementById('plan-output');
      const quoteBox = document.getElementById('quote-box');
      const clearBtn = document.getElementById('clear-btn');
      const checklist = {
        range: document.getElementById('chk-range'),
        volume: document.getElementById('chk-volume'),
        close: document.getElementById('chk-close'),
        stop: document.getElementById('chk-stop'),
      };
      const noteBox = document.getElementById('note-box');

      form.addEventListener('submit', (e) => {
        e.preventDefault();
        const data = Object.fromEntries(new FormData(form).entries());
        const parsed = parseConfig(data);
        if (!parsed) return;
        const plan = buildPlan(parsed);
        renderPlan(plan);
      });

      clearBtn.addEventListener('click', () => {
        form.reset();
        planOutput.textContent = 'Nhập vùng giá + tham số để tính toán entry/stop/take-profit.';
        Object.values(checklist).forEach((el) => (el.textContent = '-'));
        noteBox.textContent = 'Thêm ghi chú chiến lược ở mục trên để hiển thị tại đây.';
      });

      function parseConfig(data) {
        const required = ['resistance', 'support'];
        for (const key of required) {
          if (!data[key]) {
            planOutput.textContent = 'Vui lòng nhập đầy đủ hỗ trợ/kháng cự.';
            return null;
          }
        }
        const config = {
          symbol: data.symbol || '__DEFAULT_SYMBOL__',
          timeframe: data.timeframe || '15min',
          direction: data.direction,
          resistance: parseFloat(data.resistance),
          support: parseFloat(data.support),
          bufferPct: parseFloat(data.buffer_pct) / 100,
          volumeMult: parseFloat(data.volume_mult),
          confirmation: parseInt(data.confirmation, 10),
          stopOffset: parseFloat(data.stop_offset),
          rr: parseFloat(data.rr),
          riskPct: parseFloat(data.risk_pct),
          capital: parseFloat(data.capital),
          contract: parseFloat(data.contract),
          lookback: parseInt(data.lookback, 10),
          trailAtr: parseFloat(data.trail_atr),
          notes: data.notes || '',
        };
        if (!Number.isFinite(config.bufferPct) || config.bufferPct < 0) config.bufferPct = 0;
        return config;
      }

      function buildPlan(cfg) {
        const rangeHeight = Math.max(cfg.resistance - cfg.support, 0);
        const buffer = rangeHeight * cfg.bufferPct;
        const directionFactor = cfg.direction === 'bearish' ? -1 : 1;
        const breakoutLevel = cfg.direction === 'bearish' ? cfg.support : cfg.resistance;
        const entry = breakoutLevel + directionFactor * buffer;
        const stop = cfg.direction === 'bearish'
          ? cfg.resistance + cfg.stopOffset
          : cfg.support - cfg.stopOffset;
        const stopDistance = Math.max(Math.abs(entry - stop), 0.01);
        const tpDistance = rangeHeight * cfg.rr;
        const takeProfit = cfg.direction === 'bearish' ? entry - tpDistance : entry + tpDistance;
        const riskCapital = cfg.capital * cfg.riskPct / 100;
        const pipValue = cfg.contract;
        const perLotRisk = stopDistance * pipValue;
        const volume = perLotRisk > 0 ? Math.min(riskCapital / perLotRisk, 10) : 0;
        return {
          cfg,
          rangeHeight,
          entry,
          stop,
          takeProfit,
          volume: Number(volume.toFixed(2)),
          stopDistance,
          tpDistance,
        };
      }

      function renderPlan(plan) {
        const {
          cfg,
          rangeHeight,
          entry,
          stop,
          takeProfit,
          volume,
          stopDistance,
        } = plan;
        const rr = plan.tpDistance / stopDistance;
        const summary = [
          `<strong>Symbol:</strong> ${cfg.symbol} (${cfg.timeframe})`,
          `<strong>Hướng:</strong> ${cfg.direction === 'bullish' ? 'Breakout tăng' : 'Breakdown giảm'}`,
          `<strong>Range Height:</strong> ${rangeHeight.toFixed(2)} USD`,
          `<strong>Entry gợi ý:</strong> ${entry.toFixed(2)}`,
          `<strong>Stop-loss:</strong> ${stop.toFixed(2)} (khoảng ${stopDistance.toFixed(2)} USD)`,
          `<strong>Take-profit:</strong> ${takeProfit.toFixed(2)} (RR ≈ ${rr.toFixed(2)})`,
          `<strong>Khối lượng đề xuất:</strong> ${volume} lot (risk ${cfg.riskPct}% trên capital ${cfg.capital})`,
          `<strong>Trailing kích hoạt khi ATR ≥</strong> ${cfg.trailAtr}`,
        ];
        planOutput.innerHTML = summary.join('<br />');
        checklist.range.textContent = cfg.lookback >= 30 ? 'OK - range rõ' : 'Xem xét lại';
        checklist.volume.textContent = `${cfg.volumeMult}x`;
        checklist.close.textContent = `>= ${cfg.confirmation} nến`;
        checklist.stop.textContent = stopDistance > 0 ? 'Đã tính' : 'Chưa hợp lệ';
        noteBox.textContent = cfg.notes || 'Chưa có ghi chú.';
      }

      async function refreshQuote() {
        try {
          const res = await fetch('/quotes/xauusdc');
          if (!res.ok) throw new Error('HTTP ' + res.status);
          const data = await res.json();
          quoteBox.innerHTML = [
            `Giá: <strong>${Number(data.price || data.bid || 0).toFixed(2)}</strong>`,
            `Bid / Ask: ${Number(data.bid || 0).toFixed(2)} / ${Number(data.ask || 0).toFixed(2)}`,
            `Cập nhật: ${data.updated_at ? new Date(data.updated_at).toLocaleString() : '-'}`,
          ].join('<br />');
        } catch (err) {
          quoteBox.textContent = 'Không thể tải quote: ' + err.message;
        }
      }
      refreshQuote();
      setInterval(refreshQuote, 5000);
    </script>
  </body>
</html>
""".replace("__DEFAULT_SYMBOL__", settings.quote_symbol)


if __name__ == "__main__":
    import uvicorn

    uvicorn.run("app.main:app", host="0.0.0.0", port=8000, reload=True)
