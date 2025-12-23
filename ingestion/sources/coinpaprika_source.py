"""CoinPaprika API data source connector."""

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
from schemas.etl import CoinPaprikaSchema, UnifiedDataInput

logger = get_logger(__name__)
settings = get_settings()


class CoinPaprikaSource(BaseSource[CoinPaprikaSchema]):
    """Data source for CoinPaprika API (cryptocurrency data)."""

    BASE_URL = "https://api.coinpaprika.com/v1"

    def __init__(self, config: Optional[SourceConfig] = None):
        if config is None:
            config = SourceConfig(
                name="coinpaprika",
                enabled=True,
                batch_size=settings.etl_batch_size,
                rate_limit_per_minute=30,  # Conservative for free tier
            )
        super().__init__(config)
        self.api_key = settings.api_key

    @property
    def source_type(self) -> str:
        return "coinpaprika"

    def _get_headers(self) -> dict[str, str]:
        """Get request headers with optional authentication."""
        headers = {
            "Accept": "application/json",
        }
        # CoinPaprika uses API key in header if provided
        if self.api_key:
            headers["Authorization"] = self.api_key
        return headers

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=1, max=10),
        retry=retry_if_exception_type((httpx.HTTPError, httpx.TimeoutException)),
    )
    async def _make_request(
        self, endpoint: str, params: Optional[dict[str, Any]] = None
    ) -> list[dict[str, Any]]:
        """Make HTTP request with retry logic."""
        async with httpx.AsyncClient(timeout=30.0) as client:
            url = f"{self.BASE_URL}/{endpoint}"
            self.logger.info("coinpaprika_request", url=url, params=params)

            response = await client.get(
                url,
                params=params,
                headers=self._get_headers(),
            )
            response.raise_for_status()

            data = response.json()
            self.logger.info(
                "coinpaprika_response",
                status_code=response.status_code,
                record_count=len(data) if isinstance(data, list) else 1,
            )
            return data if isinstance(data, list) else [data]

    async def fetch(
        self, checkpoint: Optional[str] = None, batch_size: Optional[int] = None
    ) -> FetchResult:
        """Fetch cryptocurrency tickers from CoinPaprika."""
        batch_size = batch_size or self.config.batch_size

        # Parse checkpoint to get offset
        offset = 0
        if checkpoint:
            try:
                offset = int(checkpoint)
            except ValueError:
                offset = 0

        try:
            # Fetch tickers (prices for all coins)
            records = await self._make_request("tickers")

            # Apply pagination (CoinPaprika returns all at once)
            total_records = len(records)
            end_offset = offset + batch_size
            paginated_records = records[offset:end_offset]

            has_more = end_offset < total_records
            checkpoint_value = str(end_offset) if has_more else None

            return FetchResult(
                records=paginated_records,
                total_fetched=len(paginated_records),
                has_more=has_more,
                checkpoint_value=checkpoint_value,
                metadata={
                    "source": "coinpaprika",
                    "endpoint": "tickers",
                    "total_available": total_records,
                    "offset": offset,
                },
            )
        except Exception as e:
            self.logger.error("coinpaprika_fetch_failed", error=str(e))
            raise

    def validate(self, record: dict[str, Any]) -> CoinPaprikaSchema:
        """Validate CoinPaprika record against schema."""
        return CoinPaprikaSchema.model_validate(record)

    def transform(self, validated_record: CoinPaprikaSchema) -> UnifiedDataInput:
        """Transform CoinPaprika record to unified schema."""
        # Extract price info from quotes
        usd_quote = validated_record.quotes.get("USD", {}) if validated_record.quotes else {}
        
        return UnifiedDataInput(
            source=self.source_type,
            source_id=validated_record.id,
            title=validated_record.name,
            content=f"{validated_record.symbol} - Rank #{validated_record.rank}",
            author=None,
            category="cryptocurrency",
            url=f"https://coinpaprika.com/coin/{validated_record.id}/",
            external_created_at=validated_record.last_updated,
            extra_data={
                "symbol": validated_record.symbol,
                "rank": validated_record.rank,
                "price_usd": usd_quote.get("price"),
                "volume_24h_usd": usd_quote.get("volume_24h"),
                "market_cap_usd": usd_quote.get("market_cap"),
                "percent_change_24h": usd_quote.get("percent_change_24h"),
                "percent_change_7d": usd_quote.get("percent_change_7d"),
                "circulating_supply": validated_record.circulating_supply,
                "total_supply": validated_record.total_supply,
                "max_supply": validated_record.max_supply,
            },
            checksum=self.compute_checksum(validated_record.model_dump()),
        )

    def get_checkpoint_value(self, record: dict[str, Any]) -> str:
        """Extract coin ID as checkpoint value."""
        return record.get("id", "")
