"""Pydantic schemas for ETL data validation."""

from datetime import datetime
from typing import Any, Optional

from pydantic import BaseModel, Field, field_validator


class BaseETLSchema(BaseModel):
    """Base schema for ETL data validation."""

    class Config:
        extra = "allow"  # Allow extra fields for raw data


# =============================================================================
# CoinPaprika API Schemas
# =============================================================================


class CoinPaprikaSchema(BaseETLSchema):
    """Schema for CoinPaprika ticker data."""

    id: str = Field(..., description="Coin ID (e.g., btc-bitcoin)")
    name: str = Field(..., max_length=255)
    symbol: str = Field(..., max_length=20)
    rank: int = Field(..., ge=0)
    circulating_supply: Optional[float] = None
    total_supply: Optional[float] = None
    max_supply: Optional[float] = None
    beta_value: Optional[float] = None
    first_data_at: Optional[datetime] = None
    last_updated: Optional[datetime] = None
    quotes: Optional[dict[str, Any]] = Field(default_factory=dict)

    @field_validator("last_updated", "first_data_at", mode="before")
    @classmethod
    def parse_datetime(cls, v: Any) -> Optional[datetime]:
        if v is None:
            return None
        if isinstance(v, datetime):
            return v
        try:
            return datetime.fromisoformat(str(v).replace("Z", "+00:00"))
        except ValueError:
            return None


# =============================================================================
# CoinGecko API Schemas
# =============================================================================


class CoinGeckoSchema(BaseETLSchema):
    """Schema for CoinGecko market data."""

    id: str = Field(..., description="Coin ID (e.g., bitcoin)")
    symbol: str = Field(..., max_length=20)
    name: str = Field(..., max_length=255)
    image: Optional[str] = None
    current_price: Optional[float] = None
    market_cap: Optional[float] = None
    market_cap_rank: Optional[int] = None
    fully_diluted_valuation: Optional[float] = None
    total_volume: Optional[float] = None
    high_24h: Optional[float] = None
    low_24h: Optional[float] = None
    price_change_24h: Optional[float] = None
    price_change_percentage_24h: Optional[float] = None
    price_change_percentage_7d_in_currency: Optional[float] = None
    market_cap_change_24h: Optional[float] = None
    market_cap_change_percentage_24h: Optional[float] = None
    circulating_supply: Optional[float] = None
    total_supply: Optional[float] = None
    max_supply: Optional[float] = None
    ath: Optional[float] = None
    ath_change_percentage: Optional[float] = None
    ath_date: Optional[datetime] = None
    atl: Optional[float] = None
    atl_change_percentage: Optional[float] = None
    atl_date: Optional[datetime] = None
    last_updated: Optional[datetime] = None

    @field_validator("last_updated", "ath_date", "atl_date", mode="before")
    @classmethod
    def parse_datetime(cls, v: Any) -> Optional[datetime]:
        if v is None:
            return None
        if isinstance(v, datetime):
            return v
        try:
            return datetime.fromisoformat(str(v).replace("Z", "+00:00"))
        except ValueError:
            return None


# =============================================================================
# CSV Source Schemas
# =============================================================================


class CsvProductSchema(BaseETLSchema):
    """Schema for CSV product data."""

    product_id: str = Field(..., alias="product_id")
    name: str = Field(..., max_length=500)
    category: str = Field(..., max_length=100)
    price: float = Field(..., ge=0)
    description: Optional[str] = None
    stock_quantity: int = Field(default=0, ge=0)
    created_at: Optional[datetime] = None

    @field_validator("price", mode="before")
    @classmethod
    def parse_price(cls, v: Any) -> float:
        if isinstance(v, str):
            # Remove currency symbols and parse
            v = v.replace("$", "").replace(",", "").strip()
        return float(v) if v else 0.0

    @field_validator("stock_quantity", mode="before")
    @classmethod
    def parse_quantity(cls, v: Any) -> int:
        if v is None or v == "":
            return 0
        return int(float(v))


# =============================================================================
# RSS Source Schemas
# =============================================================================


class RssEntrySchema(BaseETLSchema):
    """Schema for RSS feed entry data."""

    entry_id: str = Field(..., alias="id")
    title: str = Field(..., max_length=500)
    link: str = Field(..., max_length=512)
    summary: Optional[str] = None
    content: Optional[str] = None
    author: Optional[str] = None
    published: Optional[datetime] = None
    updated: Optional[datetime] = None
    tags: list[str] = Field(default_factory=list)

    @field_validator("published", "updated", mode="before")
    @classmethod
    def parse_datetime(cls, v: Any) -> Optional[datetime]:
        if v is None:
            return None
        if isinstance(v, datetime):
            return v
        # Try to parse various date formats
        try:
            from email.utils import parsedate_to_datetime
            return parsedate_to_datetime(str(v))
        except (TypeError, ValueError):
            pass
        try:
            return datetime.fromisoformat(str(v).replace("Z", "+00:00"))
        except ValueError:
            return None


# =============================================================================
# Unified Schema (Output)
# =============================================================================


class UnifiedDataInput(BaseModel):
    """Schema for creating unified data records.
    
    The canonical_id field enables identity unification - the same cryptocurrency
    from different sources (CoinPaprika, CoinGecko) will share the same canonical_id,
    allowing them to be merged into a single unified record.
    """

    source: str
    source_id: str
    canonical_id: str  # Normalized identifier for cross-source matching (e.g., "btc")
    symbol: Optional[str] = None  # Original symbol from source
    title: Optional[str] = None
    content: Optional[str] = None
    author: Optional[str] = None
    category: Optional[str] = None
    url: Optional[str] = None
    external_created_at: Optional[datetime] = None
    extra_data: Optional[dict[str, Any]] = None
    checksum: str



# =============================================================================
# Legacy API Schema (kept for backward compatibility)
# =============================================================================


class ApiPostSchema(BaseETLSchema):
    """Schema for API post data (legacy - JSONPlaceholder)."""

    id: int = Field(..., alias="id")
    user_id: int = Field(..., alias="userId")
    title: str = Field(..., max_length=500)
    body: str

    @field_validator("title", "body", mode="before")
    @classmethod
    def clean_string(cls, v: Any) -> str:
        if v is None:
            return ""
        return str(v).strip()

