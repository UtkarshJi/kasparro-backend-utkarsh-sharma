"""RSS data source connector."""

from typing import Any, Optional

import feedparser
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

from core.config import get_settings
from core.logging import get_logger
from ingestion.sources.base import BaseSource, FetchResult, SourceConfig
from schemas.etl import RssEntrySchema, UnifiedDataInput

logger = get_logger(__name__)
settings = get_settings()

# Default RSS feeds (tech news)
DEFAULT_RSS_FEEDS = [
    "https://hnrss.org/frontpage",  # Hacker News
]


class RssSource(BaseSource[RssEntrySchema]):
    """Data source for RSS feed ingestion."""

    def __init__(
        self,
        feed_urls: Optional[list[str]] = None,
        config: Optional[SourceConfig] = None,
    ):
        if config is None:
            config = SourceConfig(
                name="rss",
                enabled=True,
                batch_size=settings.etl_batch_size,
                rate_limit_per_minute=30,
            )
        super().__init__(config)
        self.feed_urls = feed_urls or DEFAULT_RSS_FEEDS

    @property
    def source_type(self) -> str:
        return "rss"

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=1, max=10),
        retry=retry_if_exception_type(Exception),
    )
    def _fetch_feed(self, url: str) -> list[dict[str, Any]]:
        """Fetch and parse an RSS feed."""
        self.logger.info("rss_fetch", url=url)

        feed = feedparser.parse(url)

        if feed.bozo and feed.bozo_exception:
            self.logger.warning(
                "rss_parse_warning",
                url=url,
                error=str(feed.bozo_exception),
            )

        entries = []
        for entry in feed.entries:
            entry_data = {
                "id": entry.get("id") or entry.get("link", ""),
                "title": entry.get("title", ""),
                "link": entry.get("link", ""),
                "summary": entry.get("summary", ""),
                "content": self._extract_content(entry),
                "author": entry.get("author", ""),
                "published": entry.get("published", ""),
                "updated": entry.get("updated", ""),
                "tags": [tag.get("term", "") for tag in entry.get("tags", [])],
                "_feed_url": url,
                "_feed_title": feed.feed.get("title", ""),
            }
            entries.append(entry_data)

        self.logger.info("rss_fetched", url=url, entry_count=len(entries))
        return entries

    def _extract_content(self, entry: Any) -> str:
        """Extract full content from entry."""
        if hasattr(entry, "content") and entry.content:
            return entry.content[0].get("value", "")
        return entry.get("summary", "")

    async def fetch(
        self, checkpoint: Optional[str] = None, batch_size: Optional[int] = None
    ) -> FetchResult:
        """Fetch entries from RSS feeds with incremental support."""
        batch_size = batch_size or self.config.batch_size

        # Parse checkpoint to get seen entry IDs
        seen_ids: set[str] = set()
        if checkpoint:
            try:
                seen_ids = set(checkpoint.split(","))
            except ValueError:
                seen_ids = set()

        all_entries = []
        for url in self.feed_urls:
            try:
                entries = self._fetch_feed(url)
                all_entries.extend(entries)
            except Exception as e:
                self.logger.error("rss_fetch_failed", url=url, error=str(e))

        # Filter out already seen entries (incremental)
        new_entries = [e for e in all_entries if e.get("id") not in seen_ids]

        # Apply batch size limit
        records = new_entries[:batch_size]
        has_more = len(new_entries) > batch_size

        # Update checkpoint with new IDs
        new_ids = {e.get("id") for e in records if e.get("id")}
        all_seen_ids = seen_ids | new_ids

        # Limit checkpoint size (keep last 1000 IDs)
        checkpoint_ids = list(all_seen_ids)[-1000:]
        checkpoint_value = ",".join(checkpoint_ids) if checkpoint_ids else None

        return FetchResult(
            records=records,
            total_fetched=len(records),
            has_more=has_more,
            checkpoint_value=checkpoint_value,
            metadata={
                "feeds_processed": len(self.feed_urls),
                "total_entries": len(all_entries),
                "new_entries": len(new_entries),
            },
        )

    def validate(self, record: dict[str, Any]) -> RssEntrySchema:
        """Validate RSS entry against schema."""
        # Remove internal fields before validation
        clean_record = {k: v for k, v in record.items() if not k.startswith("_")}
        return RssEntrySchema.model_validate(clean_record)

    def transform(self, validated_record: RssEntrySchema) -> UnifiedDataInput:
        """Transform RSS entry to unified schema with identity resolution."""
        # Combine summary and content
        content = validated_record.content or validated_record.summary
        
        # For RSS entries, use entry_id as canonical ID
        # Hash long URLs to keep canonical_id manageable
        import hashlib
        entry_hash = hashlib.md5(validated_record.entry_id.encode()).hexdigest()[:12]
        canonical_id = f"rss_{entry_hash}"

        return UnifiedDataInput(
            source=self.source_type,
            source_id=validated_record.entry_id,
            canonical_id=canonical_id,
            symbol=None,  # Not applicable for news
            title=validated_record.title,
            content=content,
            author=validated_record.author,
            category="news" if validated_record.tags else None,
            url=validated_record.link,
            external_created_at=validated_record.published or validated_record.updated,
            extra_data={
                "tags": validated_record.tags,
                "updated_at": validated_record.updated.isoformat() if validated_record.updated else None,
            },
            checksum=self.compute_checksum(validated_record.model_dump()),
        )

    def get_checkpoint_value(self, record: dict[str, Any]) -> str:
        """Extract entry ID as checkpoint value."""
        return record.get("id", "")
