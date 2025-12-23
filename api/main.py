"""FastAPI main application."""

from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from api.routes import (
    data_router,
    etl_router,
    health_router,
    metrics_router,
    runs_router,
    stats_router,
)
from core.config import get_settings
from core.database import close_db, init_db
from core.logging import get_logger, setup_logging
from ingestion.scheduler import start_scheduler, stop_scheduler
from ingestion.pipeline import get_pipeline

settings = get_settings()
logger = get_logger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan handler."""
    # Startup
    setup_logging()
    logger.info("application_starting", app_name=settings.app_name)

    # Initialize database
    await init_db()
    logger.info("database_initialized")

    # Start ETL scheduler
    start_scheduler()
    logger.info("etl_scheduler_started")

    # Run initial ETL on startup
    logger.info("initial_etl_starting")
    try:
        pipeline = get_pipeline()
        await pipeline.run_all()
        logger.info("initial_etl_completed")
    except Exception as e:
        logger.error("initial_etl_failed", error=str(e))

    yield

    # Shutdown
    logger.info("application_shutting_down")
    stop_scheduler()
    await close_db()
    logger.info("application_stopped")


# Create FastAPI app
app = FastAPI(
    title="Kasparro Backend API",
    description="ETL and Data API for Kasparro",
    version="1.0.0",
    lifespan=lifespan,
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Include routers
app.include_router(health_router)
app.include_router(data_router)
app.include_router(stats_router)
app.include_router(runs_router)
app.include_router(metrics_router)
app.include_router(etl_router, prefix="/api")


@app.get("/")
async def root():
    """Root endpoint."""
    return {
        "name": "Kasparro Backend API",
        "version": "1.0.0",
        "docs": "/docs",
    }
