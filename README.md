# XAUUSDc Realtime Quote Service

Dịch vụ FastAPI nhỏ này kết nối tới terminal MetaTrader5 đang chạy trên máy, lấy dữ liệu giao dịch XAU/USD rồi phát lại qua REST và WebSocket theo thời gian thực.

## Tính năng
- Kết nối trực tiếp tới MetaTrader5 để lấy tick và đồng bộ theo chu kỳ `5s` (có thể cấu hình).
- REST endpoint `GET /quotes/xauusdc` trả về giá/bid/ask/biến động cùng mốc thời gian cập nhật.
- WebSocket endpoint `GET /ws/xauusdc` phát quote mới ngay khi có dữ liệu.
- Health check `GET /healthz`.

## Cách chạy
```bash
cd tradingAuto
py -m venv .venv
.\.venv\Scripts\Activate.ps1 | .venv\Scripts\activate.bat | source .venv/Scripts/activate
pip install -r requirements.txt
uvicorn app.main:app --reload
```

Sau khi chạy, truy cập `http://127.0.0.1:8000/docs` để thử nhanh các endpoint.

## CLI hợp nhất
Toàn bộ script trong thư mục `scripts/` đã được gom vào CLI duy nhất:

```bash
python -m app.cli <command> [tùy chọn]
```

> Chiến lược chính là **Breakout Strategy** dựa trên EMA 21/89, ATR filter và buffer breakout. Mọi thông số (range lookback, buffer ATR, momentum, trading hours, ATR baseline...) đều có thể cấu hình trực tiếp qua CLI hoặc Dashboard.

### `ingest-live`
Ingest tick realtime từ MT5 vào DB (nếu bạn vẫn muốn ghi liên tục):

```bash
python -m app.cli ingest-live \
  --db-url postgresql+asyncpg://user:pass@localhost:5432/mt5 \
  --symbol XAUUSDc \
  --interval 1.0
```
- `--db-url`: chuỗi kết nối async
- `--symbol`: mã trong MT5
- `--interval`: chu kỳ poll (giây)

### `fetch-history`
Tải dữ liệu tick lịch sử từ MT5 và ghi vào DB (đã chống duplicate bằng `symbol+time_msc`).

```bash
python -m app.cli fetch-history \
  --symbol XAUUSDc \
  --start 2025-01-01 \
  --end 2025-02-01 \
  --db-url postgresql+asyncpg://user:pass@localhost:5432/mt5 \
  --batch 2000 \
  --max-days 1
```
- `--start/--end`: ISO 8601 UTC
- `--batch`: số dòng mỗi lần insert
- `--max-days`: số ngày tối đa cho một lần gọi `copy_ticks_range`

### `list-symbols`
Liệt kê các symbol hiện có trong terminal MT5 đang mở:

```bash
python -m app.cli list-symbols
```

### `backtest-ma` (Breakout backtest)
Backtest chiến lược breakout đầy đủ (range detection, ATR filter, EMA confirmation, giờ giao dịch).

```bash
python -m app.cli backtest-ma \
  --db-url postgresql+asyncpg://user:pass@localhost:5432/mt5 \
  --symbol XAUUSDc \
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
- Xuất CSV `backtest_<symbol>_<start>_<end>.csv` với danh sách lệnh breakout mô phỏng.
- Có thể kết hợp thêm `--sl-pips/--tp-pips/--pip-size` hoặc `--momentum-type pct`.
- `--trading-hours` mặc định dùng phiên Mỹ: `19:30-23:30,01:00-02:30`.

### `run-live-ma` (Breakout live)
Chạy chiến lược breakout realtime (paper mặc định, thêm `--live` để gửi lệnh MT5 thật).

```bash
python -m app.cli run-live-ma \
  --db-url postgresql+asyncpg://user:pass@localhost:5432/mt5 \
  --symbol XAUUSDc \
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
- `--size-from-risk` tự tính volume dựa trên capital/risk_pct/contract_size/stop-distance.
- `--range-*`, `--breakout-*`, `--atr-*` tinh chỉnh độ rộng range, buffer và ATR baseline dùng để xác nhận breakout.
- `--trading-hours` đảm bảo bot chỉ trade trong khung giờ breakout (mặc định phiên Mỹ `19:30-23:30,01:00-02:30`).
- Có thể chuyển `--momentum-type pct` nếu muốn xác nhận bằng %change thay vì MACD.
- Khi bật `--live`, đảm bảo MT5 terminal đang chạy và tài khoản có quyền giao dịch.

> Mỗi lệnh đều hỗ trợ `-h` để xem danh sách tham số chi tiết: `python -m app.cli <command> -h`.

## Dashboard breakout
- Chạy FastAPI (`uvicorn app.main:app --reload`) và truy cập `http://localhost:8000/dashboard`.
- Form chính cho phép nhập toàn bộ tham số breakout (preset hoặc custom), bấm **Start** để chạy live bot, **Backtest breakout** để backtest hoặc **Fetch history** (MT5) để bổ sung dữ liệu.
- Khối “Trạng thái breakout” hiển thị quote, vị thế, tín hiệu cuối và lý do đang chờ (ví dụ “Range quá hẹp”, “MACD chưa xác nhận”).

### Cấu hình MetaTrader5
1. Cài MetaTrader5 (terminal64) và đăng nhập tài khoản có quyền xem symbol XAUUSDc.
2. Đảm bảo terminal đang chạy trên cùng máy với dịch vụ hoặc cấu hình biến môi trường để thư viện `MetaTrader5` tự khởi tạo:
   ```bash
   export QUOTE_SYMBOL=XAUUSDc         # Symbol đúng trong MT5 của bạn
   export MT5_LOGIN=<account_id>       # Tuỳ chọn
   export MT5_PASSWORD=<password>      # Tuỳ chọn
   export MT5_SERVER=<server_name>     # Tuỳ chọn
   export MT5_TERMINAL_PATH="/path/to/terminal64.exe"  # Nếu cần chỉ định đường dẫn cụ thể
   ```
3. Khởi động dịch vụ. Poller sẽ lấy dữ liệu với tần suất `POLL_INTERVAL_SECONDS`.

## Biến môi trường
| Tên | Mặc định | Diễn giải |
| --- | --- | --- |
| `QUOTE_SYMBOL` | `XAUUSDc` | Mã symbol chính xác trong MT5. |
| `POLL_INTERVAL_SECONDS` | `5` | Khoảng thời gian giữa mỗi lần gọi tick. |
| `SOURCE_NAME` | `MetaTrader5` | Ghi đè tên nguồn trong response. |
| `MT5_LOGIN` | `None` | Login ID để initialize terminal (tuỳ chọn). |
| `MT5_PASSWORD` | `None` | Password tương ứng. |
| `MT5_SERVER` | `None` | Server name của broker. |
| `MT5_TERMINAL_PATH` | `None` | Đường dẫn tuỳ chỉnh tới `terminal64.exe`. |

> **Lưu ý MT5:** Thư viện `MetaTrader5` yêu cầu chạy trên cùng máy với terminal MT5 và hiện chỉ hỗ trợ Windows. Nếu deploy dịch vụ trên Linux/macos, hãy triển khai phần lấy dữ liệu trên một máy Windows rồi truyền dữ liệu về dịch vụ này (qua WebSocket, MQ hoặc HTTP nội bộ).

## Dọn các file rác / dữ liệu tạm
Các lệnh backtest và tải lịch sử sẽ sinh ra nhiều file CSV như `backtest_XAUUSDc_*.csv`, `ticks_*last24h*.csv`. Chúng không cần commit và có thể xoá bằng:

```bash
# Xoá toàn bộ backtest CSV + file tick tạm ở thư mục gốc
rm -f backtest_XAUUSDc_*.csv ticks_*last24h*.csv
```

Nếu muốn dọn sâu hơn (VD các file tạm nằm trong thư mục con), có thể dùng `find`:

```bash
find . -maxdepth 1 -type f -name 'backtest_*.csv' -delete
find . -maxdepth 1 -type f -name 'ticks_*last24h*.csv' -delete
```

Trước khi commit, hãy chạy `git status` để đảm bảo không còn file rác xuất hiện ở trạng thái `untracked`.
