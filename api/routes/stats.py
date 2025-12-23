"""Stats API routes."""

import time
from typing import Annotated

from fastapi import APIRouter, Query
from sqlalchemy import case, distinct, func, select

from api.dependencies import DbSession, get_request_metadata
from core.models import ETLRun, ETLStatus, UnifiedData
from schemas.responses import ETLSummary, SourceStats, StatsResponse

router = APIRouter(tags=["Stats"])


@router.get("/stats", response_model=StatsResponse)
async def get_stats(
    session: DbSession,
    days: Annotated[int, Query(ge=1, le=90, description="Days to look back")] = 7,
) -> StatsResponse:
    """
    Get ETL summary statistics.

    Returns aggregate stats across all sources and per-source breakdowns.
    """
    start_time = time.time()

    # Get overall run statistics
    run_stats = await session.execute(
        select(
            func.count(ETLRun.id).label("total_runs"),
            func.sum(
                case(
                    (ETLRun.status == ETLStatus.SUCCESS.value, 1),
                    else_=0,
                )
            ).label("successful_runs"),
            func.sum(
                case(
                    (ETLRun.status == ETLStatus.FAILED.value, 1),
                    else_=0,
                )
            ).label("failed_runs"),
            func.sum(ETLRun.records_processed).label("total_records"),
            func.avg(ETLRun.duration_seconds).label("avg_duration"),
        )
    )
    stats_row = run_stats.one()

    # Get last success and failure timestamps
    last_success = await session.execute(
        select(ETLRun.finished_at)
        .where(ETLRun.status == ETLStatus.SUCCESS.value)
        .order_by(ETLRun.finished_at.desc())
        .limit(1)
    )
    last_success_at = last_success.scalar_one_or_none()

    last_failure = await session.execute(
        select(ETLRun.finished_at)
        .where(ETLRun.status == ETLStatus.FAILED.value)
        .order_by(ETLRun.finished_at.desc())
        .limit(1)
    )
    last_failure_at = last_failure.scalar_one_or_none()

    summary = ETLSummary(
        total_runs=stats_row.total_runs or 0,
        successful_runs=stats_row.successful_runs or 0,
        failed_runs=stats_row.failed_runs or 0,
        total_records_processed=stats_row.total_records or 0,
        average_duration_seconds=(
            round(stats_row.avg_duration, 2) if stats_row.avg_duration else None
        ),
        last_success_at=last_success_at,
        last_failure_at=last_failure_at,
    )

    # Get per-source statistics
    sources = await session.execute(select(distinct(UnifiedData.source)))
    source_names = [row[0] for row in sources.all()]

    by_source = []
    for source_name in source_names:
        # Count records
        record_count = await session.execute(
            select(func.count(UnifiedData.id)).where(UnifiedData.source == source_name)
        )
        total_records = record_count.scalar() or 0

        # Get last run for this source
        last_run_result = await session.execute(
            select(ETLRun)
            .where(ETLRun.source == source_name)
            .order_by(ETLRun.started_at.desc())
            .limit(1)
        )
        last_run = last_run_result.scalar_one_or_none()

        by_source.append(
            SourceStats(
                source=source_name,
                total_records=total_records,
                last_run_at=last_run.started_at if last_run else None,
                last_run_status=last_run.status if last_run else None,
                last_run_duration_seconds=last_run.duration_seconds if last_run else None,
                last_records_processed=last_run.records_processed if last_run else None,
            )
        )

    return StatsResponse(
        summary=summary,
        by_source=by_source,
        metadata=get_request_metadata(start_time),
    )
