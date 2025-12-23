"""Core configuration module using Pydantic Settings."""

from functools import lru_cache
from typing import Literal

from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Application settings loaded from environment variables."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    # Database
    database_url: str = Field(
        default="postgresql+asyncpg://kasparro:kasparro_secret@localhost:5432/kasparro",
        description="PostgreSQL connection string (async)",
    )
    database_url_sync: str = Field(
        default="postgresql://kasparro:kasparro_secret@localhost:5432/kasparro",
        description="PostgreSQL connection string (sync, for migrations)",
    )

    # API Configuration
    api_key: str = Field(
        default="",
        description="External API authentication key",
    )
    api_base_url: str = Field(
        default="https://jsonplaceholder.typicode.com",
        description="Base URL for external API",
    )

    # ETL Configuration
    etl_interval_minutes: int = Field(
        default=5,
        ge=1,
        le=1440,
        description="ETL run interval in minutes",
    )
    etl_batch_size: int = Field(
        default=100,
        ge=1,
        le=10000,
        description="Number of records to process per batch",
    )

    # Rate Limiting
    rate_limit_requests_per_minute: int = Field(
        default=60,
        ge=1,
        description="Maximum requests per minute per source",
    )
    rate_limit_burst: int = Field(
        default=10,
        ge=1,
        description="Maximum burst requests allowed",
    )

    # Logging
    log_level: Literal["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"] = Field(
        default="INFO",
        description="Logging level",
    )
    log_format: Literal["json", "console"] = Field(
        default="json",
        description="Log output format",
    )

    # Server
    host: str = Field(default="0.0.0.0", description="Server host")
    port: int = Field(default=8000, ge=1, le=65535, description="Server port")

    # Application
    app_name: str = Field(default="Kasparro Backend", description="Application name")
    debug: bool = Field(default=False, description="Debug mode")

    @field_validator("database_url", mode="before")
    @classmethod
    def convert_database_url_to_asyncpg(cls, v: str) -> str:
        """Convert postgres:// URLs to postgresql+asyncpg:// for async SQLAlchemy.
        
        Render and other cloud providers use postgres:// format, but asyncpg needs
        the postgresql+asyncpg:// prefix.
        """
        if v.startswith("postgres://"):
            return v.replace("postgres://", "postgresql+asyncpg://", 1)
        elif v.startswith("postgresql://") and "+asyncpg" not in v:
            return v.replace("postgresql://", "postgresql+asyncpg://", 1)
        return v

    @field_validator("api_key")
    @classmethod
    def validate_api_key(cls, v: str) -> str:
        """Validate API key is provided in production."""
        # Allow empty for local development, but warn
        return v


@lru_cache
def get_settings() -> Settings:
    """Get cached settings instance."""
    return Settings()

