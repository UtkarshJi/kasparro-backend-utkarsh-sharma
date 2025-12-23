"""Pydantic schemas for API requests and responses."""

from datetime import datetime
from typing import Any, Generic, Optional, TypeVar

from pydantic import BaseModel, ConfigDict, Field

T = TypeVar("T")


# =============================================================================
# Base Schemas
# =============================================================================


class BaseSchema(BaseModel):
    """Base schema with common configuration."""

    model_config = ConfigDict(
        from_attributes=True,
        populate_by_name=True,
        str_strip_whitespace=True,
    )


# =============================================================================
# Data Schemas
# =============================================================================


class UnifiedDataSchema(BaseSchema):
    """Unified data record schema."""

    id: int
    source: str
    source_id: str
    title: Optional[str] = None
    content: Optional[str] = None
    author: Optional[str] = None
    category: Optional[str] = None
    url: Optional[str] = None
    external_created_at: Optional[datetime] = None
    ingested_at: datetime
    updated_at: datetime
    extra_data: Optional[dict[str, Any]] = None


class DataFilterParams(BaseModel):
    """Query parameters for filtering data."""

    source: Optional[str] = Field(None, description="Filter by source (api/csv/rss)")
    category: Optional[str] = Field(None, description="Filter by category")
    author: Optional[str] = Field(None, description="Filter by author")
    search: Optional[str] = Field(None, description="Search in title and content")
    start_date: Optional[datetime] = Field(None, description="Filter by ingested_at >= start_date")
    end_date: Optional[datetime] = Field(None, description="Filter by ingested_at <= end_date")


class PaginationParams(BaseModel):
    """Pagination parameters."""

    limit: int = Field(default=20, ge=1, le=100, description="Number of records to return")
    offset: int = Field(default=0, ge=0, description="Number of records to skip")


# =============================================================================
# Response Schemas
# =============================================================================


class ResponseMetadata(BaseModel):
    """Metadata included in all responses."""

    request_id: str = Field(..., description="Unique request identifier")
    api_latency_ms: float = Field(..., description="API processing time in milliseconds")
    timestamp: datetime = Field(default_factory=datetime.utcnow)


class PaginatedResponse(BaseModel, Generic[T]):
    """Paginated response wrapper."""

    data: list[T]
    total: int
    limit: int
    offset: int
    has_more: bool
    metadata: ResponseMetadata


class DataResponse(PaginatedResponse[UnifiedDataSchema]):
    """Response for GET /data endpoint."""

    pass


# =============================================================================
# Health Schemas
# =============================================================================


class ETLStatusInfo(BaseModel):
    """ETL status information."""

    last_run_at: Optional[datetime] = None
    last_run_status: Optional[str] = None
    last_run_duration_seconds: Optional[float] = None
    records_processed: Optional[int] = None


class HealthResponse(BaseModel):
    """Response for GET /health endpoint."""

    status: str = Field(..., description="Overall health status")
    db_connected: bool = Field(..., description="Database connectivity status")
    db_latency_ms: Optional[float] = Field(None, description="Database latency")
    etl: ETLStatusInfo
    metadata: ResponseMetadata


# =============================================================================
# Stats Schemas
# =============================================================================


class SourceStats(BaseModel):
    """Statistics for a single source."""

    source: str
    total_records: int
    last_run_at: Optional[datetime] = None
    last_run_status: Optional[str] = None
    last_run_duration_seconds: Optional[float] = None
    last_records_processed: Optional[int] = None


class ETLSummary(BaseModel):
    """ETL summary statistics."""

    total_runs: int
    successful_runs: int
    failed_runs: int
    total_records_processed: int
    average_duration_seconds: Optional[float] = None
    last_success_at: Optional[datetime] = None
    last_failure_at: Optional[datetime] = None


class StatsResponse(BaseModel):
    """Response for GET /stats endpoint."""

    summary: ETLSummary
    by_source: list[SourceStats]
    metadata: ResponseMetadata


# =============================================================================
# Run Schemas
# =============================================================================


class ETLRunSchema(BaseSchema):
    """ETL run record schema."""

    id: int
    run_id: str
    source: str
    status: str
    started_at: datetime
    finished_at: Optional[datetime] = None
    duration_seconds: Optional[float] = None
    records_fetched: int
    records_processed: int
    records_failed: int
    records_skipped: int
    error_message: Optional[str] = None
    run_metadata: Optional[dict[str, Any]] = None


class RunsResponse(BaseModel):
    """Response for GET /runs endpoint."""

    runs: list[ETLRunSchema]
    total: int
    metadata: ResponseMetadata


class RunComparisonResult(BaseModel):
    """Result of comparing two ETL runs."""

    run_1: ETLRunSchema
    run_2: ETLRunSchema
    duration_diff_seconds: Optional[float] = None
    records_diff: int
    anomalies: list[str]


class CompareRunsResponse(BaseModel):
    """Response for GET /compare-runs endpoint."""

    comparison: RunComparisonResult
    metadata: ResponseMetadata


# =============================================================================
# Metrics Schemas
# =============================================================================


class MetricValue(BaseModel):
    """Single metric value."""

    name: str
    value: float
    labels: dict[str, str] = Field(default_factory=dict)


class MetricsResponse(BaseModel):
    """Prometheus-format metrics response."""

    # Raw text format for Prometheus
    pass
