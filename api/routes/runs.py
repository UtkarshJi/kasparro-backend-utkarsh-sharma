"""Runs API routes."""

import time
from typing import Annotated

from fastapi import APIRouter, HTTPException, Query
from sqlalchemy import select

from api.dependencies import DbSession, get_request_metadata
from core.models import ETLRun
from schemas.responses import (
    CompareRunsResponse,
    ETLRunSchema,
    RunComparisonResult,
    RunsResponse,
)

router = APIRouter(tags=["Runs"])


@router.get("/runs", response_model=RunsResponse)
async def get_runs(
    session: DbSession,
    limit: Annotated[int, Query(ge=1, le=100)] = 20,
    source: Annotated[str | None, Query(description="Filter by source")] = None,
) -> RunsResponse:
    """
    Get ETL run history.

    Returns list of recent ETL runs with metadata.
    """
    start_time = time.time()

    query = select(ETLRun).order_by(ETLRun.started_at.desc())

    if source:
        query = query.where(ETLRun.source == source)

    query = query.limit(limit)

    result = await session.execute(query)
    runs = result.scalars().all()

    return RunsResponse(
        runs=[ETLRunSchema.model_validate(r) for r in runs],
        total=len(runs),
        metadata=get_request_metadata(start_time),
    )


@router.get("/compare-runs", response_model=CompareRunsResponse)
async def compare_runs(
    session: DbSession,
    run_id_1: Annotated[str, Query(description="First run ID to compare")],
    run_id_2: Annotated[str, Query(description="Second run ID to compare")],
) -> CompareRunsResponse:
    """
    Compare two ETL runs.

    Identifies differences and potential anomalies between runs.
    """
    start_time = time.time()

    # Fetch both runs
    result1 = await session.execute(
        select(ETLRun).where(ETLRun.run_id == run_id_1)
    )
    run1 = result1.scalar_one_or_none()

    result2 = await session.execute(
        select(ETLRun).where(ETLRun.run_id == run_id_2)
    )
    run2 = result2.scalar_one_or_none()

    if not run1:
        raise HTTPException(status_code=404, detail=f"Run not found: {run_id_1}")
    if not run2:
        raise HTTPException(status_code=404, detail=f"Run not found: {run_id_2}")

    # Calculate differences
    duration_diff = None
    if run1.duration_seconds and run2.duration_seconds:
        duration_diff = round(run1.duration_seconds - run2.duration_seconds, 2)

    records_diff = run1.records_processed - run2.records_processed

    # Detect anomalies
    anomalies = []

    # Large duration difference (>50%)
    if duration_diff and run2.duration_seconds:
        pct_diff = abs(duration_diff) / run2.duration_seconds * 100
        if pct_diff > 50:
            anomalies.append(f"Duration difference: {pct_diff:.1f}%")

    # Significant record count difference
    if run2.records_processed and records_diff:
        pct_diff = abs(records_diff) / run2.records_processed * 100
        if pct_diff > 20:
            anomalies.append(f"Record count difference: {pct_diff:.1f}%")

    # Status change
    if run1.status != run2.status:
        anomalies.append(f"Status changed: {run2.status} -> {run1.status}")

    # High failure rate in one run
    if run1.records_failed > run1.records_processed * 0.1:
        anomalies.append(f"High failure rate in run 1: {run1.records_failed} failed")
    if run2.records_failed > run2.records_processed * 0.1:
        anomalies.append(f"High failure rate in run 2: {run2.records_failed} failed")

    return CompareRunsResponse(
        comparison=RunComparisonResult(
            run_1=ETLRunSchema.model_validate(run1),
            run_2=ETLRunSchema.model_validate(run2),
            duration_diff_seconds=duration_diff,
            records_diff=records_diff,
            anomalies=anomalies,
        ),
        metadata=get_request_metadata(start_time),
    )
