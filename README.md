# tradingAuto CLI

Bộ công cụ dòng lệnh để làm việc với dữ liệu MT5, chạy chiến lược breakout MA và backtest nhanh. Không còn API/Dashboard – mọi thứ chạy qua CLI.

## Cài đặt nhanh
```bash
cd tradingAuto
py -m venv .venv
.\.venv\Scripts\Activate.ps1 | .venv\Scripts\activate.bat | source .venv/Scripts/activate
pip install -r requirements.txt
```

Chạy lệnh bằng:
```bash
python -m app.cli <command> [tùy chọn]
```

## Các lệnh chính
### fetch-history
Tải tick lịch sử từ MT5 và ghi vào DB (đã chống duplicate bằng `symbol+time_msc`).
```bash
python -m app.cli fetch-history \
  --symbol XAUUSD \
  --start 2025-01-01 \
  --end 2025-02-01 \
  --db-url postgresql+asyncpg://user:pass@localhost:5432/mt5 \
  --batch 2000 \
  --max-days 1
```

### list-symbols
Liệt kê symbol đang khả dụng trong terminal MT5:
```bash
python -m app.cli list-symbols
```

### backtest-ma (Breakout backtest)
Backtest chiến lược breakout (range detection, ATR filter, EMA trend, giờ giao dịch...).
```bash
python -m app.cli backtest-ma \
  --db-url postgresql+asyncpg://user:pass@localhost:5432/mt5 \
  --symbol XAUUSD \
  --start 2025-11-05 --end 2025-11-08 \
  --fast 21 --slow 89 --timeframe 1min \
  --ma-type ema --trend 200 --spread-atr-max 0.2 \
  --reverse-exit --market-state-window 20 \
  --risk-pct 1.0 --capital 10000 --volume 0.01 \
  --sl-atr 2.0 --tp-atr 3.0 \
  --momentum-window 14 --momentum-threshold 0.1 \
  --range-lookback 40 --range-min-atr 0.9 --range-min-points 0.6 \
  --breakout-buffer-atr 0.6 --breakout-confirmation-bars 2 \
  --atr-baseline-window 14 --atr-multiplier-min 0.8 --atr-multiplier-max 3.5 \
  --size-from-risk
```
- Lưu toàn bộ lệnh vào bảng `backtest_trades` trong DB cùng `run_id`.
- Có thể kết hợp thêm `--sl-pips/--tp-pips/--pip-size` hoặc `--momentum-type pct`.

### run-live-ma (Breakout live)
Chạy chiến lược breakout realtime (paper mặc định, thêm `--live` để gửi lệnh MT5 thật).
```bash
python -m app.cli run-live-ma \
  --db-url postgresql+asyncpg://user:pass@localhost:5432/mt5 \
  --symbol XAUUSD \
  --fast 21 --slow 89 --timeframe 1min \
  --ma-type ema --trend 200 \
  --spread-atr-max 0.2 --reverse-exit --market-state-window 20 \
  --volume 0.10 --capital 10000 --risk-pct 1.0 \
  --contract-size 100 --size-from-risk \
  --sl-atr 2.0 --tp-atr 3.0 \
  --momentum-window 14 --momentum-threshold 0.1 \
  --range-lookback 40 --range-min-atr 0.9 --range-min-points 0.6 \
  --breakout-buffer-atr 0.6 --breakout-confirmation-bars 2 \
  --atr-baseline-window 14 --atr-multiplier-min 0.8 --atr-multiplier-max 3.5 \
  --ensure-history-hours 24 --history-batch 2000 --history-max-days 5 \
  --ingest-live-db --poll 1.0 [--live]
```
- `--size-from-risk` tính volume dựa trên capital/risk_pct/contract_size/stop-distance.
- `--range-*`, `--breakout-*`, `--atr-*` tinh chỉnh breakout detection & ATR filter.
- `--trading-hours` giới hạn phiên giao dịch; nếu bỏ trống bot chạy 24/5.

### resample_ticks_to_bars
Chuyển tick sang bảng OHLC (`bars_5m`) phục vụ tối ưu breakout nhanh hơn.
```bash
python resample_ticks_to_bars.py \
  --db-url postgresql://user:pass@localhost:5432/mt5 \
  --tick-table ticks \
  --bars-table bars_5m \
  --symbol XAUUSD \
  --start 2025-11-01T00:00:00 --end 2025-11-19T00:00:00
```

Mỗi lệnh hỗ trợ `-h` để xem đầy đủ tham số: `python -m app.cli <command> -h`.

## Biến môi trường MetaTrader5
| Tên | Mặc định | Diễn giải |
| --- | --- | --- |
| `QUOTE_SYMBOL` | `XAUUSD` | Mã symbol chính xác trong MT5. |
| `SOURCE_NAME` | `MetaTrader5` | Ghi đè tên nguồn dữ liệu. |
| `MT5_LOGIN` | `None` | Login ID để initialize terminal (tuỳ chọn). |
| `MT5_PASSWORD` | `None` | Password tương ứng. |
| `MT5_SERVER` | `None` | Server name của broker. |
| `MT5_TERMINAL_PATH` | `None` | Đường dẫn tuỳ chỉnh tới `terminal64.exe`. |

> Thư viện `MetaTrader5` yêu cầu chạy trên cùng máy với terminal MT5 và hiện chỉ hỗ trợ Windows.

## Dọn file tạm
Các lệnh tải lịch sử có thể sinh file CSV như `ticks_*last24h*.csv`. Xoá nhanh:
```bash
rm -f ticks_*last24h*.csv
find . -maxdepth 1 -type f -name 'backtest_*.csv' -delete
```

Luôn kiểm tra `git status` trước khi commit để tránh file rác.
