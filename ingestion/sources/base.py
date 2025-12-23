"""Abstract base class for data sources."""

import hashlib
import json
from abc import ABC, abstractmethod
from datetime import datetime
from typing import Any, Generic, Optional, TypeVar

from pydantic import BaseModel

from core.logging import get_logger
from schemas.etl import UnifiedDataInput

T = TypeVar("T", bound=BaseModel)

logger = get_logger(__name__)


class SourceConfig(BaseModel):
    """Configuration for a data source."""

    name: str
    enabled: bool = True
    batch_size: int = 100
    rate_limit_per_minute: int = 60


class FetchResult(BaseModel):
    """Result of a fetch operation."""

    records: list[dict[str, Any]]
    total_fetched: int
    has_more: bool
    checkpoint_value: Optional[str] = None
    metadata: dict[str, Any] = {}


class BaseSource(ABC, Generic[T]):
    """Abstract base class for all data sources."""

    def __init__(self, config: SourceConfig):
        self.config = config
        self.logger = get_logger(f"source.{config.name}")

    @property
    @abstractmethod
    def source_type(self) -> str:
        """Return the source type identifier."""
        pass

    @abstractmethod
    async def fetch(
        self, checkpoint: Optional[str] = None, batch_size: Optional[int] = None
    ) -> FetchResult:
        """
        Fetch records from the source.

        Args:
            checkpoint: Last processed checkpoint for incremental ingestion
            batch_size: Number of records to fetch

        Returns:
            FetchResult with records and metadata
        """
        pass

    @abstractmethod
    def validate(self, record: dict[str, Any]) -> T:
        """
        Validate a raw record against the source schema.

        Args:
            record: Raw record data

        Returns:
            Validated Pydantic model instance
        """
        pass

    @abstractmethod
    def transform(self, validated_record: T) -> UnifiedDataInput:
        """
        Transform a validated record into the unified schema.

        Args:
            validated_record: Validated source-specific record

        Returns:
            UnifiedDataInput for storage
        """
        pass

    def compute_checksum(self, data: dict[str, Any]) -> str:
        """Compute SHA-256 checksum for deduplication."""
        # Sort keys for consistent hashing
        json_str = json.dumps(data, sort_keys=True, default=str)
        return hashlib.sha256(json_str.encode()).hexdigest()

    async def process_batch(
        self, records: list[dict[str, Any]]
    ) -> tuple[list[UnifiedDataInput], list[dict[str, Any]]]:
        """
        Process a batch of records through validation and transformation.

        Returns:
            Tuple of (successful_records, failed_records)
        """
        successful = []
        failed = []

        for record in records:
            try:
                validated = self.validate(record)
                transformed = self.transform(validated)
                successful.append(transformed)
            except Exception as e:
                self.logger.warning(
                    "record_processing_failed",
                    error=str(e),
                    record_preview=str(record)[:200],
                )
                failed.append({"record": record, "error": str(e)})

        return successful, failed

    def get_checkpoint_value(self, record: dict[str, Any]) -> str:
        """Extract checkpoint value from a record (override in subclass)."""
        return str(datetime.utcnow().isoformat())
