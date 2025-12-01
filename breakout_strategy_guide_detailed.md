**Breakout Strategy (Chiến lược Giao dịch Đột phá)** là một trong những chiến lược **cổ điển, mạnh mẽ và được sử dụng rộng rãi nhất** trên thế giới từ sàn giao dịch sàn truyền thống đến quỹ đầu tư định lượng.

### Định nghĩa đơn giản nhất
> **Breakout = Mua khi giá phá vỡ lên trên vùng kháng cự**  
> **Breakout = Bán khi giá phá vỡ xuống dưới vùng hỗ trợ**

Bạn chờ giá tích lũy (đi ngang) trong một vùng → khi giá bật mạnh ra khỏi vùng đó với volume + momentum lớn → bạn nhảy vào theo chiều phá vỡ → kỳ vọng giá sẽ chạy rất xa.

### Các dạng Breakout phổ biến nhất

| Loại Breakout                       | Cách xác định vùng | Ví dụ thực tế | Tỷ lệ R:R thường thấy |
|-------------------------------------|-------------------|---------------|----------------------|
| 1. Range Breakout (phổ biến nhất)   | Đỉnh/đáy N cây nến gần nhất (Donchian Channel) | Giá XAUUSD đi ngang 20 cây 1H → phá lên đỉnh 20 cây → BUY | 1:3 ~ 1:6 |
| 2. Opening Range Breakout (ORB)     | Cao/thấp của 15–60 phút đầu phiên (London/NY) | 14h–15h VN là phiên London mở → phá cao 14h–15h → BUY | 1:4 trở lên |
| 3. Volatility Breakout (Bollinger Band Squeeze, ATR) | Bollinger Band co hẹp + ATR thấp → đột ngột mở rộng | Giá nén trong 2–3 ngày → bùng nổ | 1:5+ |
| 4. News Breakout | Tin Non-farm, lãi suất → giá chạy 100–300 pip trong 1–2 phút | NFP ra tốt → USDJPY phá đỉnh 20 cây 1M → BUY | 1:8+ (nhưng rất rủi ro) |
| 5. Asian Session Breakout | Cao/thấp của phiên Á (0h–7h UTC) → phá vỡ lúc London mở | Phá cao phiên Á lúc 14h VN → BUY | 1:3 ~ 1:5 |
| 6. Previous Day High/Low Breakout | Đỉnh/đáy ngày hôm trước | Giá phá đỉnh ngày hôm qua → BUY | 1:3 trở lên |

### Ví dụ thực tế cực mạnh (bạn có thể code ngay hôm nay)

Chiến lược “20-period Donchian Channel Breakout” – được rất nhiều quỹ CTA dùng:

```python
# Mua khi giá đóng cửa > cao nhất 20 cây trước đó
if close[-1] > highest(high[-21:-1], 20):  # phá đỉnh 20 cây
    BUY

# Bán khi giá đóng cửa < thấp nhất 20 cây trước đó
if close[-1] < lowest(low[-21:-1], 20):   # phá đáy 20 cây
    SELL
```

Thêm ATR trailing stop → chiến lược này trên XAUUSD, NAS100, dầu thô… thường cho **Profit Factor 2.0–4.0** và **winrate 40–50%** nhưng **R:R cực cao**.

### Ưu điểm & Nhược điểm

| Ưu điểm | Nhược điểm |
|---------|-----------|
| Bắt được các sóng trending cực mạnh (50–200 pip XAUUSD, 500–2000 điểm NAS100) | Dễ bị fakeout (phá vỡ giả) trong thị trường đi ngang |
| Logic cực đơn giản, dễ code | Cần lọc tín hiệu tốt (volume, ATR, time filter…) |
| Phù hợp H1, H4, D1 | Không hợp với thị trường sideway dài |
| Dễ kết hợp trailing stop, pyramid | |

### Cách biến Breakout Strategy thành “máy in tiền” (pro tips)

1. Chỉ trade theo xu hướng lớn (200 EMA) → chỉ mua khi giá trên EMA200, chỉ bán khi dưới
2. Chỉ trade phiên London + New York (7h–21h UTC)
3. Chỉ vào lệnh khi ATR hiện tại > 1.2 × ATR trung bình 20 cây → tránh sideway
4. Dùng volume hoặc tick volume tăng đột biến để xác nhận
5. Trailing stop bằng ATR hoặc Chandelier Exit
6. Không trade ngược xu hướng tuần (ví dụ vàng đang trong downtrend tuần → chỉ SELL breakout)

### Donchian 20 + EMA200

tradingAuto/
├─ app/
│  ├─ Breakout_Strategy.py      # Chiến lược Donchian 20 + EMA200: guard ATR/spread, 
|  |                              skip weekend, trailing/breakeven/partial-close, 
|  |                              MT5 order send, risk cooldown.
│  ├─ cli.py                    # CLI gốc: subcommand fetch-history / 
|  |                              list-symbols / backtest / live, 
|  |                              nhận tham số Donchian và gọi tương ứng.
│  ├─ config.py                 # DEFAULT_DONCHIAN_PARAMS và 
|  |                              helper pick/apply_defaults; 
|  |                              Settings cho MT5/quote.
│  ├─ indicators.py             # Lấy ticks từ DB, resample OHLC, 
|  |                              tính MA (SMA/EMA) và ATR, trả về DataFrame.
│  ├─ models.py                 # Kiểu dữ liệu Quote, TradeResult, v.v. 
|  |                              (dùng chung nếu cần).
│  ├─ quote_service.py          # Service lấy quote MT5/polling, 
|  |                              publish callback subscribe/unsubscribe.
│  ├─ storage.py                # Async storage, chỉ còn bảng ticks; 
|  |                              insert_ticks_batch, fetch ticks range, has_ticks_since.
│  ├─ __init__.py               # Khởi tạo gói.
│  └─ commands/
│     ├─ backtest.py            # Chạy backtest Donchian từ DB ticks, tính tín hiệu, 
|     |                           mô phỏng SL/TP, sizing rủi ro, in summary 
|     |                           (bỏ qua lưu nếu table thiếu).
│     ├─ live.py                # Chạy live Donchian: bảo đảm history tối thiểu, 
|     |                           stream quote, gọi strategy, tùy chọn ingest ticks.
│     └─ history.py             # Lấy tick MT5 theo thời gian, lưu vào DB; 
|                                 list-symbols; dùng default start/end/batch từ config.
├─ breakout_strategy_guide_detailed.md # Tài liệu mô tả chiến lược breakout.
├─ note.txt                     # Ghi chú tuỳ chỉnh.
├─ resample_ticks_to_bars.py    # Script tiện ích resample ticks sang bars (ngoài app).
├─ optimize_breakout_params_v2.py / optimize_wrap.py # Script tối ưu tham số (chưa gắn CLI).
└─ requirements.txt             # Danh sách dependency.
