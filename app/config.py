from functools import lru_cache
from typing import Optional

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Cấu hình ở mức ứng dụng."""

    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    quote_symbol: str = Field(default="XAUUSD", description="Tên symbol như đã đăng ký trong MT5.")
    poll_interval_seconds: float = Field(default=5.0)
    source_name: Optional[str] = Field(default=None, description="Ghi đè tên nguồn dữ liệu upstream.")

    # Tuỳ chọn liên quan MetaTrader5
    mt5_login: Optional[int] = Field(default=None, description="Tài khoản đăng nhập MT5 (tuỳ chọn).")
    mt5_password: Optional[str] = Field(default=None, description="Mật khẩu tài khoản MT5 (tuỳ chọn).")
    mt5_server: Optional[str] = Field(default=None, description="Tên server MT5, ví dụ MetaQuotes-Demo.")
    mt5_terminal_path: Optional[str] = Field(default=None, description="Đường dẫn tuỳ chỉnh tới terminal64.exe nếu cần.")


@lru_cache
def get_settings() -> Settings:
    """Trả về instance settings đã cache."""
    return Settings()
