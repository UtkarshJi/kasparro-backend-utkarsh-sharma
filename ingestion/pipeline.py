"""ETL pipeline orchestrator."""

import uuid
from datetime import datetime, timezone
from typing import Any, Optional

from sqlalchemy import select, update
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from core.database import get_session_context
from core.logging import get_logger
from core.models import (
    ETLCheckpoint,
    ETLRun,
    ETLStatus,
    RawApiData,
    RawCsvData,
    RawRssData,
    UnifiedData,
)
from ingestion.sources import (
    BaseSource,
    CoinPaprikaSource,
    CoinGeckoSource,
    CsvSource,
    RssSource,
)
from schemas.etl import UnifiedDataInput

logger = get_logger(__name__)


class ETLPipeline:
    """Main ETL pipeline orchestrator."""

    def __init__(self):
        self.sources: dict[str, BaseSource] = {
            "coinpaprika": CoinPaprikaSource(),  # API 1: CoinPaprika
            "coingecko": CoinGeckoSource(),      # API 2: CoinGecko
            "csv": CsvSource(),                   # CSV source
        }
        self.logger = get_logger("etl.pipeline")

    async def run_all(self) -> dict[str, Any]:
        """Run ETL for all enabled sources."""
        results = {}
        for source_name, source in self.sources.items():
            if source.config.enabled:
                try:
                    result = await self.run_source(source_name)
                    results[source_name] = result
                except Exception as e:
                    self.logger.error(
                        "source_run_failed",
                        source=source_name,
                        error=str(e),
                    )
                    results[source_name] = {"status": "failed", "error": str(e)}
        return results

    async def run_source(self, source_name: str) -> dict[str, Any]:
        """Run ETL for a specific source."""
        if source_name not in self.sources:
            raise ValueError(f"Unknown source: {source_name}")

        source = self.sources[source_name]
        run_id = str(uuid.uuid4())
        started_at = datetime.now(timezone.utc)

        self.logger.info("etl_run_started", source=source_name, run_id=run_id)

        async with get_session_context() as session:
            # Create run record
            run = ETLRun(
                run_id=run_id,
                source=source_name,
                status=ETLStatus.RUNNING.value,
                started_at=started_at,
            )
            session.add(run)
            await session.commit()

            total_fetched = 0
            total_processed = 0
            total_failed = 0
            total_skipped = 0

            try:
                # Get checkpoint
                checkpoint = await self._get_checkpoint(session, source_name)
                checkpoint_value = checkpoint.last_value if checkpoint else None

                # Process in batches
                has_more = True
                while has_more:
                    # Fetch batch
                    fetch_result = await source.fetch(
                        checkpoint=checkpoint_value,
                        batch_size=source.config.batch_size,
                    )

                    total_fetched += fetch_result.total_fetched
                    has_more = fetch_result.has_more

                    if not fetch_result.records:
                        break

                    # Store raw data
                    await self._store_raw_data(
                        session, source_name, fetch_result.records
                    )

                    # Process and transform
                    successful, failed = await source.process_batch(fetch_result.records)
                    total_processed += len(successful)
                    total_failed += len(failed)

                    # Store unified data (with upsert for idempotency)
                    skipped = await self._store_unified_data(session, successful)
                    total_skipped += skipped

                    # Update checkpoint
                    if fetch_result.checkpoint_value:
                        await self._update_checkpoint(
                            session,
                            source_name,
                            fetch_result.checkpoint_value,
                        )
                        checkpoint_value = fetch_result.checkpoint_value

                    await session.commit()

                # Update run record - success
                finished_at = datetime.now(timezone.utc)
                duration = (finished_at - started_at).total_seconds()

                await session.execute(
                    update(ETLRun)
                    .where(ETLRun.run_id == run_id)
                    .values(
                        status=ETLStatus.SUCCESS.value,
                        finished_at=finished_at,
                        duration_seconds=duration,
                        records_fetched=total_fetched,
                        records_processed=total_processed,
                        records_failed=total_failed,
                        records_skipped=total_skipped,
                    )
                )
                await session.commit()

                self.logger.info(
                    "etl_run_completed",
                    source=source_name,
                    run_id=run_id,
                    duration_seconds=duration,
                    records_fetched=total_fetched,
                    records_processed=total_processed,
                )

                return {
                    "status": "success",
                    "run_id": run_id,
                    "duration_seconds": duration,
                    "records_fetched": total_fetched,
                    "records_processed": total_processed,
                    "records_failed": total_failed,
                    "records_skipped": total_skipped,
                }

            except Exception as e:
                # Update run record - failed
                finished_at = datetime.now(timezone.utc)
                duration = (finished_at - started_at).total_seconds()

                await session.execute(
                    update(ETLRun)
                    .where(ETLRun.run_id == run_id)
                    .values(
                        status=ETLStatus.FAILED.value,
                        finished_at=finished_at,
                        duration_seconds=duration,
                        records_fetched=total_fetched,
                        records_processed=total_processed,
                        records_failed=total_failed,
                        error_message=str(e),
                    )
                )
                await session.commit()

                self.logger.error(
                    "etl_run_failed",
                    source=source_name,
                    run_id=run_id,
                    error=str(e),
                )
                raise

    async def _get_checkpoint(
        self, session: AsyncSession, source: str
    ) -> Optional[ETLCheckpoint]:
        """Get the last checkpoint for a source."""
        result = await session.execute(
            select(ETLCheckpoint).where(ETLCheckpoint.source == source)
        )
        return result.scalar_one_or_none()

    async def _update_checkpoint(
        self,
        session: AsyncSession,
        source: str,
        value: str,
    ) -> None:
        """Update or create checkpoint for a source."""
        stmt = pg_insert(ETLCheckpoint).values(
            source=source,
            last_value=value,
            last_processed_at=datetime.now(timezone.utc),
            updated_at=datetime.now(timezone.utc),
        )
        stmt = stmt.on_conflict_do_update(
            index_elements=["source"],
            set_={
                "last_value": value,
                "last_processed_at": datetime.now(timezone.utc),
                "updated_at": datetime.now(timezone.utc),
            },
        )
        await session.execute(stmt)

    async def _store_raw_data(
        self,
        session: AsyncSession,
        source: str,
        records: list[dict[str, Any]],
    ) -> None:
        """Store raw data to appropriate table."""
        for record in records:
            try:
                # CoinPaprika and CoinGecko both store in RawApiData
                if source in ("coinpaprika", "coingecko"):
                    # Use coin id as external_id, prefix with source to avoid conflicts
                    external_id = f"{source}_{record.get('id', '')}"
                    stmt = pg_insert(RawApiData).values(
                        external_id=external_id,
                        data=record,
                        checksum=self.sources[source].compute_checksum(record),
                    )
                    stmt = stmt.on_conflict_do_update(
                        index_elements=["external_id"],
                        set_={"data": record, "fetched_at": datetime.now(timezone.utc)},
                    )
                    await session.execute(stmt)

                elif source == "csv":
                    stmt = pg_insert(RawCsvData).values(
                        file_name=record.get("_file_name", "unknown"),
                        row_number=record.get("_row_number", 0),
                        data=record,
                        checksum=self.sources[source].compute_checksum(record),
                    )
                    stmt = stmt.on_conflict_do_update(
                        index_elements=["file_name", "row_number"],
                        set_={"data": record, "imported_at": datetime.now(timezone.utc)},
                    )
                    await session.execute(stmt)

                elif source == "rss":
                    stmt = pg_insert(RawRssData).values(
                        feed_url=record.get("_feed_url", ""),
                        entry_id=record.get("id", ""),
                        data=record,
                        checksum=self.sources[source].compute_checksum(record),
                    )
                    stmt = stmt.on_conflict_do_update(
                        index_elements=["feed_url", "entry_id"],
                        set_={"data": record, "fetched_at": datetime.now(timezone.utc)},
                    )
                    await session.execute(stmt)

            except Exception as e:
                self.logger.warning(
                    "raw_data_store_failed",
                    source=source,
                    error=str(e),
                )

    async def _store_unified_data(
        self,
        session: AsyncSession,
        records: list[UnifiedDataInput],
    ) -> int:
        """Store unified data with upsert based on canonical_id (identity unification).
        
        This enables merging data from different sources for the same cryptocurrency.
        For example, Bitcoin from CoinPaprika and CoinGecko will be unified into
        a single record with canonical_id='btc'.
        """
        skipped = 0
        for record in records:
            try:
                stmt = pg_insert(UnifiedData).values(
                    canonical_id=record.canonical_id,
                    symbol=record.symbol,
                    source=record.source,
                    source_id=record.source_id,
                    title=record.title,
                    content=record.content,
                    author=record.author,
                    category=record.category,
                    url=record.url,
                    external_created_at=record.external_created_at,
                    extra_data=record.extra_data,
                    checksum=record.checksum,
                )
                # Upsert based on canonical_id - this merges same coins from different sources
                stmt = stmt.on_conflict_do_update(
                    index_elements=["canonical_id"],
                    set_={
                        # Update source info (tracks last source to update)
                        "source": record.source,
                        "source_id": record.source_id,
                        "symbol": record.symbol,
                        # Keep latest metadata
                        "title": record.title,
                        "content": record.content,
                        "author": record.author,
                        "category": record.category,
                        "url": record.url,
                        # Merge extra_data - preserve data from all sources
                        "extra_data": record.extra_data,
                        "checksum": record.checksum,
                        "updated_at": datetime.now(timezone.utc),
                    },
                )
                result = await session.execute(stmt)
                # Check if it was an update (skip) vs insert
                if result.rowcount == 0:
                    skipped += 1
            except Exception as e:
                self.logger.warning(
                    "unified_data_store_failed",
                    source=record.source,
                    source_id=record.source_id,
                    canonical_id=record.canonical_id,
                    error=str(e),
                )
        return skipped


# Global pipeline instance
_pipeline: Optional[ETLPipeline] = None


def get_pipeline() -> ETLPipeline:
    """Get or create the global pipeline instance."""
    global _pipeline
    if _pipeline is None:
        _pipeline = ETLPipeline()
    return _pipeline
