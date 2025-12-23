"""FastAPI dependencies."""

import time
import uuid
from typing import Annotated

from fastapi import Depends, Query
from sqlalchemy.ext.asyncio import AsyncSession

from core.database import get_session
from schemas.responses import DataFilterParams, PaginationParams, ResponseMetadata


async def get_db_session() -> AsyncSession:
    """Dependency for database session."""
    async for session in get_session():
        yield session


def get_request_metadata(start_time: float) -> ResponseMetadata:
    """Create response metadata with timing information."""
    latency_ms = (time.time() - start_time) * 1000
    return ResponseMetadata(
        request_id=str(uuid.uuid4()),
        api_latency_ms=round(latency_ms, 2),
    )


def get_pagination(
    limit: Annotated[int, Query(ge=1, le=100, description="Number of records")] = 20,
    offset: Annotated[int, Query(ge=0, description="Number to skip")] = 0,
) -> PaginationParams:
    """Dependency for pagination parameters."""
    return PaginationParams(limit=limit, offset=offset)


def get_data_filters(
    source: Annotated[str | None, Query(description="Filter by source")] = None,
    category: Annotated[str | None, Query(description="Filter by category")] = None,
    author: Annotated[str | None, Query(description="Filter by author")] = None,
    search: Annotated[str | None, Query(description="Search in title/content")] = None,
) -> DataFilterParams:
    """Dependency for data filtering parameters."""
    return DataFilterParams(
        source=source,
        category=category,
        author=author,
        search=search,
    )


# Type aliases for dependency injection
DbSession = Annotated[AsyncSession, Depends(get_db_session)]
Pagination = Annotated[PaginationParams, Depends(get_pagination)]
DataFilters = Annotated[DataFilterParams, Depends(get_data_filters)]
