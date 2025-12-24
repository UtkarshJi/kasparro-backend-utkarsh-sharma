"""CoinGecko API data source connector."""

from datetime import datetime
from typing import Any, Optional

import httpx
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

from core.config import get_settings
from core.logging import get_logger
from ingestion.sources.base import BaseSource, FetchResult, SourceConfig
from schemas.etl import CoinGeckoSchema, UnifiedDataInput
from services.identity_resolver import get_identity_resolver

logger = get_logger(__name__)
settings = get_settings()


class CoinGeckoSource(BaseSource[CoinGeckoSchema]):
    """Data source for CoinGecko API (cryptocurrency data)."""

    BASE_URL = "https://api.coingecko.com/api/v3"

    def __init__(self, config: Optional[SourceConfig] = None):
        if config is None:
            config = SourceConfig(
                name="coingecko",
                enabled=True,
                batch_size=100,  # CoinGecko allows up to 250 per page
                rate_limit_per_minute=30,  # Conservative for free tier
            )
        super().__init__(config)
        self.identity_resolver = get_identity_resolver()

    @property
    def source_type(self) -> str:
        return "coingecko"

    def _get_headers(self) -> dict[str, str]:
        """Get request headers."""
        return {
            "Accept": "application/json",
        }

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=30),
        retry=retry_if_exception_type((httpx.HTTPError, httpx.TimeoutException)),
    )
    async def _make_request(
        self, endpoint: str, params: Optional[dict[str, Any]] = None
    ) -> list[dict[str, Any]]:
        """Make HTTP request with retry logic."""
        async with httpx.AsyncClient(timeout=30.0) as client:
            url = f"{self.BASE_URL}/{endpoint}"
            self.logger.info("coingecko_request", url=url, params=params)

            response = await client.get(
                url,
                params=params,
                headers=self._get_headers(),
            )
            response.raise_for_status()

            data = response.json()
            self.logger.info(
                "coingecko_response",
                status_code=response.status_code,
                record_count=len(data) if isinstance(data, list) else 1,
            )
            return data if isinstance(data, list) else [data]

    async def fetch(
        self, checkpoint: Optional[str] = None, batch_size: Optional[int] = None
    ) -> FetchResult:
        """Fetch cryptocurrency market data from CoinGecko."""
        batch_size = batch_size or self.config.batch_size

        # Parse checkpoint to get page number
        page = 1
        if checkpoint:
            try:
                page = int(checkpoint)
            except ValueError:
                page = 1

        try:
            # Fetch coins markets data (includes price, market cap, volume)
            params = {
                "vs_currency": "usd",
                "order": "market_cap_desc",
                "per_page": batch_size,
                "page": page,
                "sparkline": "false",
                "price_change_percentage": "24h,7d",
            }

            records = await self._make_request("coins/markets", params)

            # CoinGecko returns empty list when no more pages
            has_more = len(records) == batch_size

            # Checkpoint is next page number
            checkpoint_value = str(page + 1) if has_more else None

            return FetchResult(
                records=records,
                total_fetched=len(records),
                has_more=has_more,
                checkpoint_value=checkpoint_value,
                metadata={
                    "source": "coingecko",
                    "endpoint": "coins/markets",
                    "page": page,
                    "per_page": batch_size,
                },
            )
        except Exception as e:
            self.logger.error("coingecko_fetch_failed", error=str(e))
            raise

    def validate(self, record: dict[str, Any]) -> CoinGeckoSchema:
        """Validate CoinGecko record against schema."""
        return CoinGeckoSchema.model_validate(record)

    def transform(self, validated_record: CoinGeckoSchema) -> UnifiedDataInput:
        """Transform CoinGecko record to unified schema with identity resolution."""
        # Generate canonical ID for identity unification
        canonical_id = self.identity_resolver.get_canonical_id(
            source=self.source_type,
            source_id=validated_record.id,
            symbol=validated_record.symbol,
            name=validated_record.name,
        )
        
        return UnifiedDataInput(
            source=self.source_type,
            source_id=validated_record.id,
            canonical_id=canonical_id,  # Enables cross-source unification
            symbol=validated_record.symbol.lower(),
            title=validated_record.name,
            content=f"{validated_record.symbol.upper()} - Rank #{validated_record.market_cap_rank or 'N/A'}",
            author=None,
            category="cryptocurrency",
            url=f"https://www.coingecko.com/en/coins/{validated_record.id}",
            external_created_at=validated_record.last_updated,
            extra_data={
                "symbol": validated_record.symbol,
                "rank": validated_record.market_cap_rank,
                "price_usd": validated_record.current_price,
                "volume_24h_usd": validated_record.total_volume,
                "market_cap_usd": validated_record.market_cap,
                "percent_change_24h": validated_record.price_change_percentage_24h,
                "percent_change_7d": validated_record.price_change_percentage_7d_in_currency,
                "circulating_supply": validated_record.circulating_supply,
                "total_supply": validated_record.total_supply,
                "max_supply": validated_record.max_supply,
                "ath": validated_record.ath,
                "ath_date": validated_record.ath_date.isoformat() if validated_record.ath_date else None,
                "image": validated_record.image,
                "_source": "coingecko",  # Track data source
            },
            checksum=self.compute_checksum(validated_record.model_dump()),
        )

    def get_checkpoint_value(self, record: dict[str, Any]) -> str:
        """Extract coin ID as checkpoint value."""
        return record.get("id", "")

