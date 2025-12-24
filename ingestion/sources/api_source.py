"""API data source connector."""

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
from schemas.etl import ApiPostSchema, UnifiedDataInput

logger = get_logger(__name__)
settings = get_settings()


class ApiSource(BaseSource[ApiPostSchema]):
    """Data source for API ingestion (JSONPlaceholder posts)."""

    def __init__(self, config: Optional[SourceConfig] = None):
        if config is None:
            config = SourceConfig(
                name="api",
                enabled=True,
                batch_size=settings.etl_batch_size,
                rate_limit_per_minute=settings.rate_limit_requests_per_minute,
            )
        super().__init__(config)
        self.base_url = settings.api_base_url
        self.api_key = settings.api_key

    @property
    def source_type(self) -> str:
        return "api"

    def _get_headers(self) -> dict[str, str]:
        """Get request headers with authentication."""
        headers = {
            "Content-Type": "application/json",
            "Accept": "application/json",
        }
        if self.api_key:
            headers["Authorization"] = f"Bearer {self.api_key}"
            headers["X-API-Key"] = self.api_key
        return headers

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=1, max=10),
        retry=retry_if_exception_type((httpx.HTTPError, httpx.TimeoutException)),
    )
    async def _make_request(
        self, endpoint: str, params: Optional[dict[str, Any]] = None
    ) -> list[dict[str, Any]]:
        """Make authenticated HTTP request with retry logic."""
        async with httpx.AsyncClient(timeout=30.0) as client:
            url = f"{self.base_url}/{endpoint}"
            self.logger.info("api_request", url=url, params=params)

            response = await client.get(
                url,
                params=params,
                headers=self._get_headers(),
            )
            response.raise_for_status()

            data = response.json()
            self.logger.info(
                "api_response",
                status_code=response.status_code,
                record_count=len(data) if isinstance(data, list) else 1,
            )
            return data if isinstance(data, list) else [data]

    async def fetch(
        self, checkpoint: Optional[str] = None, batch_size: Optional[int] = None
    ) -> FetchResult:
        """Fetch posts from JSONPlaceholder API with incremental support."""
        batch_size = batch_size or self.config.batch_size

        # Parse checkpoint to get last processed ID
        start_id = 1
        if checkpoint:
            try:
                start_id = int(checkpoint) + 1
            except ValueError:
                start_id = 1

        # JSONPlaceholder has 100 posts total, IDs 1-100
        # Fetch in batches using ID range
        params = {
            "_start": start_id - 1,  # 0-indexed
            "_limit": batch_size,
        }

        try:
            records = await self._make_request("posts", params)

            # Determine if there are more records
            has_more = len(records) == batch_size and start_id + batch_size <= 100

            # Get the last ID for checkpoint
            checkpoint_value = None
            if records:
                last_record = records[-1]
                checkpoint_value = str(last_record.get("id", start_id + len(records) - 1))

            return FetchResult(
                records=records,
                total_fetched=len(records),
                has_more=has_more,
                checkpoint_value=checkpoint_value,
                metadata={
                    "source": "jsonplaceholder",
                    "endpoint": "posts",
                    "start_id": start_id,
                },
            )
        except Exception as e:
            self.logger.error("api_fetch_failed", error=str(e))
            raise

    def validate(self, record: dict[str, Any]) -> ApiPostSchema:
        """Validate API record against schema."""
        return ApiPostSchema.model_validate(record)

    def transform(self, validated_record: ApiPostSchema) -> UnifiedDataInput:
        """Transform API record to unified schema with identity resolution."""
        # For legacy API posts, use source-prefixed ID as canonical ID
        # (no cross-source matching for JSONPlaceholder posts)
        canonical_id = f"api_post_{validated_record.id}"
        
        return UnifiedDataInput(
            source=self.source_type,
            source_id=str(validated_record.id),
            canonical_id=canonical_id,
            symbol=None,  # Not applicable for posts
            title=validated_record.title,
            content=validated_record.body,
            author=f"user_{validated_record.user_id}",
            category="posts",
            url=f"{self.base_url}/posts/{validated_record.id}",
            external_created_at=None,  # JSONPlaceholder doesn't provide timestamps
            extra_data={
                "user_id": validated_record.user_id,
                "original_id": validated_record.id,
            },
            checksum=self.compute_checksum(validated_record.model_dump()),
        )

    def get_checkpoint_value(self, record: dict[str, Any]) -> str:
        """Extract post ID as checkpoint value."""
        return str(record.get("id", ""))
