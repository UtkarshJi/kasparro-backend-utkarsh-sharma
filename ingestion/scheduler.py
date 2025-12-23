"""ETL scheduler using APScheduler."""

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.interval import IntervalTrigger

from core.config import get_settings
from core.logging import get_logger
from ingestion.pipeline import get_pipeline

logger = get_logger(__name__)
settings = get_settings()

# Global scheduler instance
_scheduler: AsyncIOScheduler | None = None


async def run_scheduled_etl() -> None:
    """Scheduled ETL job function."""
    logger.info("scheduled_etl_started")
    try:
        pipeline = get_pipeline()
        results = await pipeline.run_all()
        logger.info("scheduled_etl_completed", results=results)
    except Exception as e:
        logger.error("scheduled_etl_failed", error=str(e))


def get_scheduler() -> AsyncIOScheduler:
    """Get or create the scheduler instance."""
    global _scheduler
    if _scheduler is None:
        _scheduler = AsyncIOScheduler()
    return _scheduler


def start_scheduler() -> None:
    """Start the ETL scheduler."""
    scheduler = get_scheduler()

    if scheduler.running:
        logger.info("scheduler_already_running")
        return

    # Add ETL job
    scheduler.add_job(
        run_scheduled_etl,
        trigger=IntervalTrigger(minutes=settings.etl_interval_minutes),
        id="etl_job",
        name="ETL Pipeline Job",
        replace_existing=True,
    )

    scheduler.start()
    logger.info(
        "scheduler_started",
        interval_minutes=settings.etl_interval_minutes,
    )


def stop_scheduler() -> None:
    """Stop the ETL scheduler."""
    global _scheduler
    if _scheduler and _scheduler.running:
        _scheduler.shutdown(wait=False)
        logger.info("scheduler_stopped")
    _scheduler = None
