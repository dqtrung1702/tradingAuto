from functools import lru_cache
from typing import Any, Dict, Optional

try:  # pydantic là tuỳ chọn để CLI vẫn chạy nếu thiếu dependency
    from pydantic import Field  # type: ignore
    from pydantic_settings import BaseSettings, SettingsConfigDict  # type: ignore

    _HAS_PYDANTIC = True
except Exception:  # pragma: no cover - optional
    _HAS_PYDANTIC = False
    Field = None  # type: ignore
    BaseSettings = object  # type: ignore
    SettingsConfigDict = dict  # type: ignore


if _HAS_PYDANTIC:
    class Settings(BaseSettings):
        """Cấu hình ở mức ứng dụng."""

        model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

        quote_symbol: str = Field(default="XAUUSD", description="Tên symbol trong MT5.")
        poll_interval_seconds: float = Field(default=5.0)
        source_name: Optional[str] = Field(default=None, description="Ghi đè tên nguồn dữ liệu upstream.")

        # Tuỳ chọn liên quan MetaTrader5
        mt5_login: Optional[int] = Field(default=None, description="Tài khoản đăng nhập MT5 (tuỳ chọn).")
        mt5_password: Optional[str] = Field(default=None, description="Mật khẩu tài khoản MT5 (tuỳ chọn).")
        mt5_server: Optional[str] = Field(default=None, description="Tên server MT5, ví dụ MetaQuotes-Demo.")
        mt5_terminal_path: Optional[str] = Field(default=None, description="Đường dẫn tuỳ chỉnh tới terminal64.exe nếu cần.")
else:
    class Settings:
        """Fallback đơn giản nếu thiếu pydantic_settings (CLI vẫn chạy)."""

        def __init__(
            self,
            quote_symbol: str = "XAUUSD",
            poll_interval_seconds: float = 5.0,
            source_name: Optional[str] = None,
            mt5_login: Optional[int] = None,
            mt5_password: Optional[str] = None,
            mt5_server: Optional[str] = None,
            mt5_terminal_path: Optional[str] = None,
        ) -> None:
            self.quote_symbol = quote_symbol
            self.poll_interval_seconds = poll_interval_seconds
            self.source_name = source_name
            self.mt5_login = mt5_login
            self.mt5_password = mt5_password
            self.mt5_server = mt5_server
            self.mt5_terminal_path = mt5_terminal_path


@lru_cache
def get_settings() -> Settings:
    """Trả về instance settings đã cache."""
    return Settings()


DEFAULT_DONCHIAN_PARAMS: Dict[str, Any] = {
    # Phổ quát / CLI
    "db_url": "postgresql+asyncpg://trader:admin@localhost:5432/mt5",
    "start": "2025-12-01T00:00:00+00:00",
    # Chiến lược Donchian/EMA
    "symbol": "XAUUSD",
    "timeframe": "1H",
    "donchian_period": 20,
    "ema_trend_period": 200,
    "atr_period": 14,
    "entry_buffer_points": 0.0,
    "exit_on_opposite": True,
    "breakeven_after_rr": None,
    # Vốn & khối lượng
    "capital": 100.0,
    "risk_pct": 0.015,
    "size_from_risk": True,
    "contract_size": 100.0,
    "pip_size": 0.1,
    "min_volume": 0.01,
    "volume_step": 0.01,
    "max_positions": 1,
    # Quản lý lệnh
    "sl_atr": 2.0,
    "tp_atr": 5.0,
    "trail_trigger_atr": 2.0,
    "trail_atr_mult": 2.2,
    "breakeven_atr": 1.5,
    "partial_close": True,
    "partial_close_atr": 2.8,
    # Lọc phiên / biến động
    "trading_hours": ["00:00-23:59"],
    "min_atr_multiplier": 0.8,
    "max_atr_multiplier": 3.0,
    "max_spread_points": 50,
    "allowed_deviation_points": 30,
    "slippage_points": 0.0,
    "spread_samples": 10,
    "spread_sample_delay_ms": 100,
    # Risk control
    "max_daily_loss": 0.06,
    "cooldown_minutes": 60,
    "max_loss_streak": 3,
    "session_cooldown_minutes": 0,
    # Hạ tầng / vận hành
    "order_retry_times": 5,
    "order_retry_delay_ms": 500,
    "magic_number": 20251230,
    "poll": 1.0,
    "ensure_history_hours": 240,
    "history_batch": 2000,
    "history_max_days": 30,
    # Logging
    "log_level": "DEBUG",
    "log_file": "logs/app.log",
    "log_to_console": False,
    "ignore_gaps": True,
    "closed_sessions": ["23:00-02:00"],
    "dry_run": False,
    "live": False,
}


def pick(key: str, *candidates: Any, defaults: Dict[str, Any] = DEFAULT_DONCHIAN_PARAMS) -> Any:
    """Chọn giá trị đầu tiên khác None, nếu không có thì lấy default theo key."""
    for val in candidates:
        if val is not None:
            return val
    return defaults.get(key)


def apply_defaults(overrides: Dict[str, Any], defaults: Dict[str, Any] = DEFAULT_DONCHIAN_PARAMS) -> Dict[str, Any]:
    """Trộn overrides lên default map, bỏ qua các giá trị None."""
    merged = dict(defaults)
    for key, val in overrides.items():
        if val is not None:
            merged[key] = val
    return merged
