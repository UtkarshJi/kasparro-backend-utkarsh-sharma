"""API routes module exports."""

from api.routes.data import router as data_router
from api.routes.etl import router as etl_router
from api.routes.health import router as health_router
from api.routes.metrics import router as metrics_router
from api.routes.runs import router as runs_router
from api.routes.stats import router as stats_router

__all__ = [
    "data_router",
    "health_router",
    "stats_router",
    "runs_router",
    "metrics_router",
    "etl_router",
]
