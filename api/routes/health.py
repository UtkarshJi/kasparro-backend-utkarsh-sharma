"""Health check API routes."""

import time

from fastapi import APIRouter

from api.dependencies import DbSession, get_request_metadata
from core.database import check_db_connection
from core.models import ETLRun
from schemas.responses import ETLStatusInfo, HealthResponse
from sqlalchemy import select

router = APIRouter(tags=["Health"])


@router.get("/health", response_model=HealthResponse)
async def health_check(session: DbSession) -> HealthResponse:
    """
    Health check endpoint.

    Reports database connectivity and ETL last-run status.
    """
    start_time = time.time()

    # Check database connection
    db_connected = await check_db_connection()

    # Measure DB latency
    db_start = time.time()
    try:
        from sqlalchemy import text
        await session.execute(text("SELECT 1"))
        db_latency = (time.time() - db_start) * 1000
    except Exception:
        db_latency = None

    # Get last ETL run status
    etl_info = ETLStatusInfo()
    try:
        result = await session.execute(
            select(ETLRun)
            .order_by(ETLRun.started_at.desc())
            .limit(1)
        )
        last_run = result.scalar_one_or_none()
        if last_run:
            etl_info = ETLStatusInfo(
                last_run_at=last_run.started_at,
                last_run_status=last_run.status,
                last_run_duration_seconds=last_run.duration_seconds,
                records_processed=last_run.records_processed,
            )
    except Exception:
        pass

    # Determine overall status
    status = "healthy" if db_connected else "unhealthy"

    return HealthResponse(
        status=status,
        db_connected=db_connected,
        db_latency_ms=round(db_latency, 2) if db_latency else None,
        etl=etl_info,
        metadata=get_request_metadata(start_time),
    )
