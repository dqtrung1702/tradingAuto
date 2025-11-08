from datetime import datetime
from typing import Optional

from pydantic import BaseModel, Field


class Quote(BaseModel):
    """Payload quote đã chuẩn hoá trả về client."""

    symbol: str = Field(description="Mã ticker, ví dụ XAUUSDc=X")
    price: Optional[float] = Field(default=None, description="Giá giao dịch gần nhất")
    bid: Optional[float] = Field(default=None, description="Giá bid")
    ask: Optional[float] = Field(default=None, description="Giá ask")
    change_percent: Optional[float] = Field(default=None, description="Phần trăm thay đổi so với đóng phiên trước")
    currency: Optional[str] = Field(default=None, description="Đơn vị tiền tệ của quote")
    source: str = Field(description="Nguồn dữ liệu upstream")
    updated_at: datetime = Field(description="Timestamp UTC của quote từ upstream")


class HealthStatus(BaseModel):
    status: str = Field(description="Trạng thái đơn giản của service")
    last_quote_timestamp: Optional[datetime] = Field(
        default=None, description="Thời điểm cache được cập nhật lần cuối"
    )
