"""Database models for ETL and data storage."""

from datetime import datetime
from enum import Enum
from typing import Any, Optional

from sqlalchemy import (
    JSON,
    Boolean,
    DateTime,
    Float,
    Index,
    Integer,
    String,
    Text,
    func,
)
from sqlalchemy.orm import Mapped, mapped_column

from core.database import Base


class SourceType(str, Enum):
    """Data source types."""

    API = "api"
    CSV = "csv"
    RSS = "rss"


class ETLStatus(str, Enum):
    """ETL run status."""

    RUNNING = "running"
    SUCCESS = "success"
    FAILED = "failed"
    PARTIAL = "partial"


# =============================================================================
# Raw Data Tables
# =============================================================================


class RawApiData(Base):
    """Raw data from API source."""

    __tablename__ = "raw_api_data"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    external_id: Mapped[str] = mapped_column(String(255), unique=True, index=True)
    data: Mapped[dict[str, Any]] = mapped_column(JSON, nullable=False)
    fetched_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )
    checksum: Mapped[str] = mapped_column(String(64), nullable=True)

    __table_args__ = (Index("ix_raw_api_fetched_at", "fetched_at"),)


class RawCsvData(Base):
    """Raw data from CSV source."""

    __tablename__ = "raw_csv_data"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    file_name: Mapped[str] = mapped_column(String(255), index=True)
    row_number: Mapped[int] = mapped_column(Integer)
    data: Mapped[dict[str, Any]] = mapped_column(JSON, nullable=False)
    imported_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )
    checksum: Mapped[str] = mapped_column(String(64), nullable=True)

    __table_args__ = (
        Index("ix_raw_csv_file_row", "file_name", "row_number", unique=True),
    )


class RawRssData(Base):
    """Raw data from RSS source."""

    __tablename__ = "raw_rss_data"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    feed_url: Mapped[str] = mapped_column(String(512), index=True)
    entry_id: Mapped[str] = mapped_column(String(512), index=True)
    data: Mapped[dict[str, Any]] = mapped_column(JSON, nullable=False)
    published_at: Mapped[Optional[datetime]] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    fetched_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )
    checksum: Mapped[str] = mapped_column(String(64), nullable=True)

    __table_args__ = (
        Index("ix_raw_rss_feed_entry", "feed_url", "entry_id", unique=True),
    )


# =============================================================================
# Unified/Normalized Data Table
# =============================================================================


class UnifiedData(Base):
    """Normalized data from all sources."""

    __tablename__ = "unified_data"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)

    # Source tracking
    source: Mapped[str] = mapped_column(String(50), index=True)
    source_id: Mapped[str] = mapped_column(String(255), index=True)

    # Common fields (normalized schema)
    title: Mapped[Optional[str]] = mapped_column(String(500), nullable=True)
    content: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    author: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    category: Mapped[Optional[str]] = mapped_column(String(100), nullable=True)
    url: Mapped[Optional[str]] = mapped_column(String(512), nullable=True)

    # Metadata
    external_created_at: Mapped[Optional[datetime]] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    ingested_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now()
    )

    # Additional data as JSON
    extra_data: Mapped[Optional[dict[str, Any]]] = mapped_column(JSON, nullable=True)

    # Deduplication
    checksum: Mapped[str] = mapped_column(String(64), index=True)

    __table_args__ = (
        Index("ix_unified_source_id", "source", "source_id", unique=True),
        Index("ix_unified_ingested_at", "ingested_at"),
    )


# =============================================================================
# ETL Metadata Tables
# =============================================================================


class ETLCheckpoint(Base):
    """Checkpoint for incremental ingestion."""

    __tablename__ = "etl_checkpoints"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    source: Mapped[str] = mapped_column(String(50), unique=True, index=True)
    last_processed_id: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    last_processed_at: Mapped[Optional[datetime]] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    last_value: Mapped[Optional[str]] = mapped_column(
        Text, nullable=True
    )  # JSON string for flexible checkpointing
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now()
    )


class ETLRun(Base):
    """ETL run history and metadata."""

    __tablename__ = "etl_runs"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    run_id: Mapped[str] = mapped_column(String(36), unique=True, index=True)
    source: Mapped[str] = mapped_column(String(50), index=True)
    status: Mapped[str] = mapped_column(String(20), index=True)

    # Timing
    started_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )
    finished_at: Mapped[Optional[datetime]] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    duration_seconds: Mapped[Optional[float]] = mapped_column(Float, nullable=True)

    # Statistics
    records_fetched: Mapped[int] = mapped_column(Integer, default=0)
    records_processed: Mapped[int] = mapped_column(Integer, default=0)
    records_failed: Mapped[int] = mapped_column(Integer, default=0)
    records_skipped: Mapped[int] = mapped_column(Integer, default=0)

    # Error tracking
    error_message: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    error_details: Mapped[Optional[dict[str, Any]]] = mapped_column(JSON, nullable=True)

    # Run metadata (renamed from 'metadata' to avoid SQLAlchemy conflict)
    run_metadata: Mapped[Optional[dict[str, Any]]] = mapped_column(JSON, nullable=True)

    __table_args__ = (Index("ix_etl_runs_started_at", "started_at"),)


class SchemaRegistry(Base):
    """Schema version tracking for drift detection."""

    __tablename__ = "schema_registry"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    source: Mapped[str] = mapped_column(String(50), index=True)
    schema_version: Mapped[int] = mapped_column(Integer, default=1)
    schema_definition: Mapped[dict[str, Any]] = mapped_column(JSON, nullable=False)
    detected_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )
    is_active: Mapped[bool] = mapped_column(Boolean, default=True)
    confidence_score: Mapped[Optional[float]] = mapped_column(Float, nullable=True)

    __table_args__ = (Index("ix_schema_source_version", "source", "schema_version"),)
