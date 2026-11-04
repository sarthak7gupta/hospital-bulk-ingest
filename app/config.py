"""Application configuration settings."""

from functools import lru_cache
from typing import Literal, Self

from pydantic import Field, HttpUrl, field_validator, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

LogLevel = Literal["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]


class Settings(BaseSettings):
    """Configuration settings loaded from environment or defaults."""

    directory_api_base_url: str = Field(
        default="https://hospital-directory.onrender.com",
        description="Base URL for target Hospital Directory API",
    )
    max_csv_rows: int = Field(default=20, ge=1, le=1000)
    max_concurrent_requests: int = Field(default=10, ge=1)
    request_timeout_seconds: float = Field(default=15.0, gt=0)
    max_attempts: int = Field(default=3, ge=1)
    max_retries: int = Field(default=3, ge=1, description="Alias for max_attempts")
    max_file_size_bytes: int = Field(default=2 * 1024 * 1024, ge=1024)
    max_stored_batches: int = Field(default=1000, ge=1)
    batch_ttl_seconds: float = Field(default=86400.0, gt=0)
    stale_batch_timeout_seconds: float = Field(default=300.0, gt=0)
    shutdown_drain_timeout_seconds: float = Field(default=10.0, gt=0)
    max_retry_after_seconds: float = Field(default=30.0, ge=0)
    connection_pool_limit: int = Field(default=100, ge=1)
    connection_pool_limit_per_host: int = Field(default=25, ge=1)
    max_websocket_subscribers_per_batch: int = Field(default=10, ge=1)
    max_websocket_total_subscribers: int = Field(default=100, ge=1)
    cors_allowed_origins: list[str] = Field(default_factory=list)
    api_key: str | None = None
    rate_limit_enabled: bool = False
    rate_limit_requests_per_minute: int = Field(default=60, ge=1)
    log_level: LogLevel = "INFO"
    log_json: bool = False

    @model_validator(mode="after")
    def sync_retries_and_attempts(self) -> Self:
        if self.max_retries != 3 and self.max_attempts == 3:
            object.__setattr__(self, "max_attempts", self.max_retries)
        elif self.max_attempts != 3 and self.max_retries == 3:
            object.__setattr__(self, "max_retries", self.max_attempts)
        return self

    @field_validator("directory_api_base_url")
    @classmethod
    def validate_directory_base_url(cls, v: str) -> str:
        return str(HttpUrl(v)).rstrip("/")

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
        populate_by_name=True,
    )


@lru_cache
def get_settings() -> Settings:
    """Return cached settings instance."""
    return Settings()
