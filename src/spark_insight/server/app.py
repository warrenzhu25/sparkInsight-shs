from __future__ import annotations

import os
from pathlib import Path

from fastapi import FastAPI

from spark_insight import __version__
from spark_insight.core.service import ApplicationService
from spark_insight.server.routes import ai, insight, shs


def create_app(log_dir: str | None = None, cache_dir: str | None = None) -> FastAPI:
    resolved_log_dir = log_dir or os.getenv("SPARK_INSIGHT_LOG_DIR", "./examples/eventlogs")
    resolved_cache_dir = cache_dir or os.getenv("SPARK_INSIGHT_CACHE_DIR", "./data/cache")
    app = FastAPI(title="Spark Insight", version=__version__)
    app.state.app_service = ApplicationService(Path(resolved_log_dir), Path(resolved_cache_dir))
    app.include_router(shs.router)
    app.include_router(ai.router)
    app.include_router(insight.router)
    return app
