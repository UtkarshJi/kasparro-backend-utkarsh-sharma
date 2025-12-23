"""ETL control API routes."""

import time
from typing import Annotated

from fastapi import APIRouter, BackgroundTasks, Query

from api.dependencies import get_request_metadata
from ingestion.pipeline import get_pipeline
from schemas.responses import ResponseMetadata

router = APIRouter(tags=["ETL"])


class ETLTriggerResponse:
    """Response for ETL trigger endpoint."""

    def __init__(
        self,
        message: str,
        source: str | None,
        metadata: ResponseMetadata,
    ):
        self.message = message
        self.source = source
        self.metadata = metadata


@router.post("/etl/trigger")
async def trigger_etl(
    background_tasks: BackgroundTasks,
    source: Annotated[str | None, Query(description="Specific source to run")] = None,
) -> dict:
    """
    Trigger ETL run manually.

    Can run all sources or a specific one.
    """
    start_time = time.time()
    pipeline = get_pipeline()

    if source:
        if source not in pipeline.sources:
            return {
                "message": f"Unknown source: {source}",
                "source": source,
                "metadata": get_request_metadata(start_time).model_dump(),
            }
        background_tasks.add_task(pipeline.run_source, source)
        message = f"ETL triggered for source: {source}"
    else:
        background_tasks.add_task(pipeline.run_all)
        message = "ETL triggered for all sources"

    return {
        "message": message,
        "source": source,
        "metadata": get_request_metadata(start_time).model_dump(),
    }
