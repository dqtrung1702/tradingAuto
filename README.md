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
