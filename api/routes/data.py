"""Data API routes."""

import time

from fastapi import APIRouter
from sqlalchemy import func, or_, select

from api.dependencies import DataFilters, DbSession, Pagination, get_request_metadata
from core.models import UnifiedData
from schemas.responses import DataResponse, UnifiedDataSchema

router = APIRouter(tags=["Data"])


@router.get("/data", response_model=DataResponse)
async def get_data(
    session: DbSession,
    pagination: Pagination,
    filters: DataFilters,
) -> DataResponse:
    """
    Get normalized data with pagination and filtering.

    Supports filtering by source, category, author, and text search.
    """
    start_time = time.time()

    # Build query
    query = select(UnifiedData)

    # Apply filters
    if filters.source:
        query = query.where(UnifiedData.source == filters.source)
    if filters.category:
        query = query.where(UnifiedData.category == filters.category)
    if filters.author:
        query = query.where(UnifiedData.author.ilike(f"%{filters.author}%"))
    if filters.search:
        search_term = f"%{filters.search}%"
        query = query.where(
            or_(
                UnifiedData.title.ilike(search_term),
                UnifiedData.content.ilike(search_term),
            )
        )
    if filters.start_date:
        query = query.where(UnifiedData.ingested_at >= filters.start_date)
    if filters.end_date:
        query = query.where(UnifiedData.ingested_at <= filters.end_date)

    # Get total count
    count_query = select(func.count()).select_from(query.subquery())
    total_result = await session.execute(count_query)
    total = total_result.scalar() or 0

    # Apply pagination
    query = query.order_by(UnifiedData.ingested_at.desc())
    query = query.offset(pagination.offset).limit(pagination.limit)

    # Execute query
    result = await session.execute(query)
    records = result.scalars().all()

    # Convert to response schema
    data = [UnifiedDataSchema.model_validate(r) for r in records]

    return DataResponse(
        data=data,
        total=total,
        limit=pagination.limit,
        offset=pagination.offset,
        has_more=pagination.offset + len(data) < total,
        metadata=get_request_metadata(start_time),
    )
