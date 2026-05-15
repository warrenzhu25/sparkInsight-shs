from __future__ import annotations

import json
from hashlib import sha256
from pathlib import Path

from spark_insight.core.models import (
    ApplicationInfo,
    EnvironmentInfo,
    ExecutorSummary,
    JobData,
    ParsedApplication,
    StageData,
    TaskData,
)
from spark_insight.core.parser import EventLogParser
from spark_insight.core.utils import model_to_dict


class CacheManager:
    """Disk cache for parsed applications."""

    def __init__(self, cache_dir: str | Path = "./data/cache") -> None:
        self.cache_dir = Path(cache_dir).expanduser()
        self.cache_dir.mkdir(parents=True, exist_ok=True)

    def get_cache_path(self, eventlog_path: str | Path) -> Path:
        path = Path(eventlog_path)
        stat = path.stat()
        cache_input = f"{path.resolve()}:{stat.st_mtime_ns}:{stat.st_size}"
        key = sha256(cache_input.encode()).hexdigest()[:16]
        return self.cache_dir / key

    def is_cached(self, eventlog_path: str | Path) -> bool:
        return (self.get_cache_path(eventlog_path) / "parsed.json").exists()

    def get_or_parse(self, eventlog_path: str | Path) -> ParsedApplication:
        if self.is_cached(eventlog_path):
            return self.load_cached(eventlog_path)
        parsed = EventLogParser().parse(eventlog_path)
        self.cache_parsed(eventlog_path, parsed)
        return parsed

    def cache_parsed(self, eventlog_path: str | Path, parsed: ParsedApplication) -> None:
        cache_path = self.get_cache_path(eventlog_path)
        cache_path.mkdir(parents=True, exist_ok=True)
        payload = {
            "app_info": model_to_dict(parsed.app_info),
            "jobs": [model_to_dict(item) for item in parsed.jobs],
            "stages": [model_to_dict(item) for item in parsed.stages],
            "tasks": _dump_tasks(parsed),
            "executors": [model_to_dict(item) for item in parsed.executors],
            "environment": model_to_dict(parsed.environment),
        }
        with (cache_path / "parsed.json").open("w", encoding="utf-8") as handle:
            json.dump(payload, handle)

    def load_cached(self, eventlog_path: str | Path) -> ParsedApplication:
        with (self.get_cache_path(eventlog_path) / "parsed.json").open(encoding="utf-8") as handle:
            payload = json.load(handle)
        return ParsedApplication(
            app_info=ApplicationInfo(**payload["app_info"]),
            jobs=[JobData(**item) for item in payload["jobs"]],
            stages=[StageData(**item) for item in payload["stages"]],
            tasks={
                key: [TaskData(**task) for task in values]
                for key, values in payload.get("tasks", {}).items()
            },
            executors=[ExecutorSummary(**item) for item in payload["executors"]],
            environment=EnvironmentInfo(**payload["environment"]),
        )


def _dump_tasks(parsed: ParsedApplication) -> dict[str, list[dict]]:
    return {
        key: [model_to_dict(item) for item in values]
        for key, values in parsed.tasks.items()
    }
