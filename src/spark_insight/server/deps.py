from __future__ import annotations

from functools import lru_cache
from pathlib import Path

from spark_insight.core.service import ApplicationService


@lru_cache(maxsize=1)
def get_app_service(
    log_dir: str = "./examples/eventlogs",
    cache_dir: str = "./data/cache",
) -> ApplicationService:
    return ApplicationService(Path(log_dir), Path(cache_dir))
