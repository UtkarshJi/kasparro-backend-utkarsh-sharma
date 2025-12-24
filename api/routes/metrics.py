"""Metrics API routes (Prometheus format)."""

from fastapi import APIRouter, Response
from prometheus_client import CONTENT_TYPE_LATEST, Counter, Gauge, Histogram, generate_latest
from sqlalchemy import func, select

from api.dependencies import DbSession
from core.models import ETLRun, UnifiedData

router = APIRouter(tags=["Metrics"])

# Define Prometheus metrics
ETL_RUNS_TOTAL = Counter(
    "kasparro_etl_runs_total",
    "Total number of ETL runs",
    ["source", "status"],
)

ETL_RECORDS_PROCESSED = Counter(
    "kasparro_etl_records_processed_total",
    "Total records processed by ETL",
    ["source"],
)

ETL_DURATION_SECONDS = Histogram(
    "kasparro_etl_duration_seconds",
    "ETL run duration in seconds",
    ["source"],
    buckets=[0.5, 1, 2, 5, 10, 30, 60, 120, 300],
)

DATA_RECORDS_TOTAL = Gauge(
    "kasparro_data_records_total",
    "Total records in unified data",
    ["source"],
)

DB_CONNECTED = Gauge(
    "kasparro_db_connected",
    "Database connection status (1=connected, 0=disconnected)",
)

LAST_ETL_RUN_TIMESTAMP = Gauge(
    "kasparro_last_etl_run_timestamp",
    "Timestamp of last ETL run",
    ["source"],
)


async def update_metrics(session: DbSession) -> None:
    """Update Prometheus metrics from database state."""
    try:
        # Update record counts per source
        result = await session.execute(
            select(UnifiedData.source, func.count(UnifiedData.id))
            .group_by(UnifiedData.source)
        )
        for source, count in result.all():
            DATA_RECORDS_TOTAL.labels(source=source).set(count)

        # Update last run timestamps
        for source in ["api", "csv", "rss"]:
            result = await session.execute(
                select(ETLRun.started_at)
                .where(ETLRun.source == source)
                .order_by(ETLRun.started_at.desc())
                .limit(1)
            )
            last_run = result.scalar_one_or_none()
            if last_run:
                LAST_ETL_RUN_TIMESTAMP.labels(source=source).set(
                    last_run.timestamp()
                )

        # Database is connected
        DB_CONNECTED.set(1)

    except Exception:
        DB_CONNECTED.set(0)


@router.get("/metrics", response_class=Response)
async def get_metrics(session: DbSession) -> Response:
    """
    Prometheus metrics endpoint.

    Returns metrics in Prometheus text format.
    """
    await update_metrics(session)

    return Response(
        content=generate_latest(),
        media_type=CONTENT_TYPE_LATEST,
    )
