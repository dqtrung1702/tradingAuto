# XAUUSD Realtime Quote Service

Dịch vụ FastAPI nhỏ này kết nối tới terminal MetaTrader5 đang chạy trên máy, lấy dữ liệu giao dịch XAU/USD rồi phát lại qua REST và WebSocket theo thời gian thực.

## Tính năng
- Kết nối trực tiếp tới MetaTrader5 để lấy tick và đồng bộ theo chu kỳ `5s` (có thể cấu hình).
- REST endpoint `GET /quotes/xauusd` trả về giá/bid/ask/biến động cùng mốc thời gian cập nhật.
- WebSocket endpoint `GET /ws/xauusd` phát quote mới ngay khi có dữ liệu.
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
  --symbol XAUUSD \
  --interval 1.0
```
- `--db-url`: chuỗi kết nối async
- `--symbol`: mã trong MT5
- `--interval`: chu kỳ poll (giây)

### `fetch-history`
Tải dữ liệu tick lịch sử từ MT5 và ghi vào DB (đã chống duplicate bằng `symbol+time_msc`).

```bash
python -m app.cli fetch-history \
  --symbol XAUUSD \
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

### `summarize_backtest_trades.py`
Tổng hợp nhanh kết quả backtest đã lưu trong bảng `backtest_trades`:

```bash
python summarize_backtest_trades.py \
  --db-url postgresql+asyncpg://user:pass@localhost:5432/mt5 \
  --top 5

python summarize_backtest_trades.py \
  --db-url postgresql+asyncpg://user:pass@localhost:5432/mt5 \
  --run-id bt_XAUUSD_ab12cd34
```
- Nếu không truyền `--run-id`, script hiển thị `top` run mới nhất (mặc định 5).
- Script tự động chấp nhận chuỗi kết nối async (`+asyncpg`) và chuẩn hoá về driver sync để đọc dữ liệu.

### `backtest-ma` (Breakout backtest)
Backtest chiến lược breakout đầy đủ (range detection, ATR filter, EMA confirmation, giờ giao dịch).

```bash
python -m app.cli backtest-ma \
  --db-url postgresql+asyncpg://user:pass@localhost:5432/mt5 \
  --symbol XAUUSD \
  --start 2025-11-05 --end 2025-11-08 \
  --fast 21 --slow 89 --timeframe 1min \
  --ma-type ema --trend 200 --spread-atr-max 0.2 \
  --reverse-exit --market-state-window 20 \
  --risk-pct 0.02 --capital 10000 --volume 0.01 \
  --sl-atr 2.0 --tp-atr 3.0 \
  --momentum-window 14 --momentum-threshold 0.1 \
  --range-lookback 40 --range-min-atr 0.9 --range-min-points 0.6 \
  --breakout-buffer-atr 0.6 --breakout-confirmation-bars 2 \
  --atr-baseline-window 14 --atr-multiplier-min 0.8 --atr-multiplier-max 3.5 \
  --size-from-risk
```
- Lưu toàn bộ lệnh breakout (entry/exit/SL/TP và PnL) vào bảng `backtest_trades` trong DB cùng `run_id` để tra cứu lại.
- Có thể kết hợp thêm `--sl-pips/--tp-pips/--pip-size` hoặc `--momentum-type pct`.
- `--trading-hours` tùy chọn; nếu bỏ trống backtest sẽ xét toàn bộ phiên (24h) trừ khi preset có cấu hình riêng.
- `--risk-pct` nhập dạng số thập phân (ví dụ 0.02 = 2% vốn mỗi lệnh) nếu bật `--size-from-risk`.
- Khi chọn khoảng nhiều ngày, script sẽ tự chia từng ngày, xoá kết quả cũ trong `backtest_trades` của ngày đó rồi chạy lại để tránh dữ liệu lặp.

### `resample_ticks_to_bars`
Chuyển dữ liệu tick sang bảng OHLC (`bars_5m`) phục vụ tối ưu breakout nhanh hơn.

```bash
python resample_ticks_to_bars.py \
  --db-url postgresql://trader:admin@localhost:5432/mt5 \
  --tick-table ticks \
  --bars-table bars_5m \
  --symbol XAUUSD \
  --start 2025-08-01T00:00:00 --end 2025-11-19T00:00:00
```
- Script sẽ đọc tick theo symbol + khoảng thời gian, resample về chu kỳ mặc định `5min`, xoá bar trùng và ghi vào bảng `bars_5m`.
- Sau khi đã có `bars_5m`, các script tối ưu (`optimize_wrap.py`, `optimize_breakout_params_v2.py`) sẽ chỉ query bảng này thay vì đọc nguyên bảng tick.

### `run-live-ma` (Breakout live)
Chạy chiến lược breakout realtime (paper mặc định, thêm `--live` để gửi lệnh MT5 thật).

```bash
python -m app.cli run-live-ma \
  --db-url postgresql+asyncpg://user:pass@localhost:5432/mt5 \
  --symbol XAUUSD \
  --fast 21 --slow 89 --timeframe 1min \
  --ma-type ema --trend 200 \
  --spread-atr-max 0.2 --reverse-exit --market-state-window 20 \
  --volume 0.10 --capital 10000 --risk-pct 0.02 \
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
- `--trading-hours` đảm bảo bot chỉ trade trong khung giờ breakout; nếu không truyền (và preset không định nghĩa) bot sẽ chạy 24h.
- Có thể chuyển `--momentum-type pct` nếu muốn xác nhận bằng %change thay vì MACD.
- Khi bật `--live`, đảm bảo MT5 terminal đang chạy và tài khoản có quyền giao dịch.
- `--risk-pct` nhập dạng số thập phân (ví dụ 0.02 = 2% vốn) khi kết hợp với `--size-from-risk`.
- Sau khi backtest trên dashboard, có thể bấm **Lưu cấu hình** để ghi toàn bộ tham số + summary kết quả vào bảng `saved_backtests` trong DB để tra cứu về sau.

> Mỗi lệnh đều hỗ trợ `-h` để xem danh sách tham số chi tiết: `python -m app.cli <command> -h`.

## Dashboard breakout
- Chạy FastAPI (`uvicorn app.main:app --reload`) và truy cập `http://localhost:8000/dashboard`.
- Form chính cho phép nhập toàn bộ tham số breakout (preset hoặc custom), bấm **Start** để chạy live bot, **Backtest breakout** để backtest hoặc **Fetch history** (MT5) để bổ sung dữ liệu.
- Khối “Trạng thái breakout” hiển thị quote, vị thế, tín hiệu cuối và lý do đang chờ (ví dụ “Range quá hẹp”, “MACD chưa xác nhận”).

### Cấu hình MetaTrader5
1. Cài MetaTrader5 (terminal64) và đăng nhập tài khoản có quyền xem symbol XAUUSD.
2. Đảm bảo terminal đang chạy trên cùng máy với dịch vụ hoặc cấu hình biến môi trường để thư viện `MetaTrader5` tự khởi tạo:
   ```bash
   export QUOTE_SYMBOL=XAUUSD         # Symbol đúng trong MT5 của bạn
   export MT5_LOGIN=<account_id>       # Tuỳ chọn
   export MT5_PASSWORD=<password>      # Tuỳ chọn
   export MT5_SERVER=<server_name>     # Tuỳ chọn
   export MT5_TERMINAL_PATH="/path/to/terminal64.exe"  # Nếu cần chỉ định đường dẫn cụ thể
   ```
3. Khởi động dịch vụ. Poller sẽ lấy dữ liệu với tần suất `POLL_INTERVAL_SECONDS`.

## Biến môi trường
| Tên | Mặc định | Diễn giải |
| --- | --- | --- |
| `QUOTE_SYMBOL` | `XAUUSD` | Mã symbol chính xác trong MT5. |
| `POLL_INTERVAL_SECONDS` | `5` | Khoảng thời gian giữa mỗi lần gọi tick. |
| `SOURCE_NAME` | `MetaTrader5` | Ghi đè tên nguồn trong response. |
| `MT5_LOGIN` | `None` | Login ID để initialize terminal (tuỳ chọn). |
| `MT5_PASSWORD` | `None` | Password tương ứng. |
| `MT5_SERVER` | `None` | Server name của broker. |
| `MT5_TERMINAL_PATH` | `None` | Đường dẫn tuỳ chỉnh tới `terminal64.exe`. |

> **Lưu ý MT5:** Thư viện `MetaTrader5` yêu cầu chạy trên cùng máy với terminal MT5 và hiện chỉ hỗ trợ Windows. Nếu deploy dịch vụ trên Linux/macos, hãy triển khai phần lấy dữ liệu trên một máy Windows rồi truyền dữ liệu về dịch vụ này (qua WebSocket, MQ hoặc HTTP nội bộ).

## Dọn các file rác / dữ liệu tạm
Các lệnh tải lịch sử sẽ sinh ra file CSV như `ticks_*last24h*.csv`. Chúng không cần commit và có thể xoá bằng:

```bash
# Xoá toàn bộ file tick tạm ở thư mục gốc
rm -f ticks_*last24h*.csv
```

Nếu muốn dọn sâu hơn (VD các file tạm nằm trong thư mục con), có thể dùng `find` (các file backtest CSV cũ - nếu có - vẫn xoá an toàn):

```bash
find . -maxdepth 1 -type f -name 'backtest_*.csv' -delete
find . -maxdepth 1 -type f -name 'ticks_*last24h*.csv' -delete
```

Trước khi commit, hãy chạy `git status` để đảm bảo không còn file rác xuất hiện ở trạng thái `untracked`.
