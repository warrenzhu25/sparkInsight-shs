from __future__ import annotations

import shutil
import uuid
from pathlib import Path

from fastapi import HTTPException, UploadFile

from spark_insight.core.cache import CacheManager
from spark_insight.core.models import (
    ApplicationInfo,
    EnvironmentInfo,
    ExecutorSummary,
    JobData,
    ParsedApplication,
    StageData,
    TaskData,
    TaskMetricDistributions,
)
from spark_insight.core.query import QueryEngine
from spark_insight.core.utils import parse_spark_time


class ApplicationService:
    """Application index, parsing cache, and SHS-compatible data access."""

    def __init__(self, log_dir: str | Path, cache_dir: str | Path = "./data/cache") -> None:
        self.log_dir = Path(log_dir).expanduser()
        self.log_dir.mkdir(parents=True, exist_ok=True)
        self.cache = CacheManager(cache_dir)
        self._app_index: dict[str, Path] = {}

    def refresh_index(self) -> None:
        index: dict[str, Path] = {}
        for path in sorted(self.log_dir.glob("**/*")):
            if not path.is_file() or not self._is_eventlog(path):
                continue
            try:
                parsed = self.cache.get_or_parse(path)
            except Exception:
                continue
            index[parsed.app_info.id] = path
        self._app_index = index

    def list_applications(
        self,
        status: str | None = None,
        min_date: str | None = None,
        max_date: str | None = None,
        limit: int = 100,
    ) -> list[ApplicationInfo]:
        self.refresh_index()
        apps: list[ApplicationInfo] = []
        for path in self._app_index.values():
            parsed = self.cache.get_or_parse(path)
            app = parsed.app_info
            if not self._matches_application_filters(app, status, min_date, max_date):
                continue
            apps.append(app)
            if len(apps) >= limit:
                break
        return apps

    def get_application(self, app_id: str) -> ApplicationInfo:
        return self._load(app_id).app_info

    def get_jobs(self, app_id: str, status: str | None = None) -> list[JobData]:
        jobs = self._load(app_id).jobs
        if status:
            jobs = [job for job in jobs if job.status == status.upper()]
        return jobs

    def get_job(self, app_id: str, job_id: int) -> JobData:
        for job in self.get_jobs(app_id):
            if job.jobId == job_id:
                return job
        raise HTTPException(404, f"Job {job_id} not found")

    def get_stages(self, app_id: str, status: str | None = None) -> list[StageData]:
        stages = self._load(app_id).stages
        if status:
            stages = [stage for stage in stages if stage.status == status.upper()]
        return stages

    def get_stage(self, app_id: str, stage_id: int, attempt_id: int) -> StageData:
        for stage in self.get_stages(app_id):
            if stage.stageId == stage_id and stage.attemptId == attempt_id:
                return stage
        raise HTTPException(404, f"Stage {stage_id}.{attempt_id} not found")

    def get_tasks(
        self,
        app_id: str,
        stage_id: int,
        attempt_id: int,
        offset: int = 0,
        length: int = 100,
        sort_by: str = "taskId",
    ) -> list[TaskData]:
        tasks = self._load(app_id).tasks.get(f"{stage_id}:{attempt_id}", [])
        tasks = self._sort_tasks(tasks, sort_by)
        return tasks[offset : offset + length]

    def get_task_summary(
        self,
        app_id: str,
        stage_id: int,
        attempt_id: int,
        quantiles: list[float] | None = None,
    ) -> TaskMetricDistributions:
        tasks = self._load(app_id).tasks.get(f"{stage_id}:{attempt_id}", [])
        if not tasks:
            self.get_stage(app_id, stage_id, attempt_id)
        requested_quantiles = quantiles or [0.05, 0.25, 0.5, 0.75, 0.95]
        return TaskMetricDistributions(
            quantiles=requested_quantiles,
            duration=_quantile_values(tasks, "duration", requested_quantiles),
            executorRunTime=_quantile_values(tasks, "executorRunTime", requested_quantiles),
            executorCpuTime=_quantile_values(tasks, "executorCpuTime", requested_quantiles),
            jvmGcTime=_quantile_values(tasks, "jvmGcTime", requested_quantiles),
            inputBytes=_quantile_values(tasks, "inputBytes", requested_quantiles),
            shuffleReadBytes=_quantile_values(tasks, "shuffleReadBytes", requested_quantiles),
            shuffleWriteBytes=_quantile_values(tasks, "shuffleWriteBytes", requested_quantiles),
            memoryBytesSpilled=_quantile_values(tasks, "memoryBytesSpilled", requested_quantiles),
            diskBytesSpilled=_quantile_values(tasks, "diskBytesSpilled", requested_quantiles),
        )

    def get_executors(self, app_id: str) -> list[ExecutorSummary]:
        return self._load(app_id).executors

    def get_environment(self, app_id: str) -> EnvironmentInfo:
        return self._load(app_id).environment

    def get_query_engine(self, app_id: str) -> QueryEngine:
        return QueryEngine(self._load(app_id))

    async def upload_eventlog(self, file: UploadFile) -> ApplicationInfo:
        suffix = Path(file.filename or "eventlog").suffix
        destination = self.log_dir / f"upload-{uuid.uuid4().hex[:12]}{suffix}"
        with destination.open("wb") as handle:
            shutil.copyfileobj(file.file, handle)
        try:
            parsed = self.cache.get_or_parse(destination)
        except Exception as exc:
            destination.unlink(missing_ok=True)
            raise HTTPException(400, f"Failed to parse event log: {exc}") from exc
        self._app_index[parsed.app_info.id] = destination
        return parsed.app_info

    def _load(self, app_id: str) -> ParsedApplication:
        if app_id not in self._app_index:
            self.refresh_index()
        path = self._app_index.get(app_id)
        if not path:
            raise HTTPException(404, f"Application {app_id} not found")
        return self.cache.get_or_parse(path)

    @staticmethod
    def _is_eventlog(path: Path) -> bool:
        name = path.name.lower()
        return (
            name.endswith(".json")
            or name.endswith(".json.gz")
            or name.startswith("app-")
            or name.startswith("application_")
            or "eventlog" in name
        )

    @staticmethod
    def _sort_tasks(tasks: list[TaskData], sort_by: str) -> list[TaskData]:
        descending = sort_by.startswith("-")
        field = sort_by[1:] if descending else sort_by
        if not tasks or not hasattr(tasks[0], field):
            return tasks
        return sorted(tasks, key=lambda task: getattr(task, field), reverse=descending)

    @staticmethod
    def _matches_application_filters(
        app: ApplicationInfo,
        status: str | None,
        min_date: str | None,
        max_date: str | None,
    ) -> bool:
        attempt = app.attempts[0] if app.attempts else None
        if status and attempt:
            completed = attempt.completed
            if status.lower() == "completed" and not completed:
                return False
            if status.lower() == "running" and completed:
                return False
        start_time = parse_spark_time(attempt.startTime if attempt else None)
        min_time = parse_spark_time(min_date)
        max_time = parse_spark_time(max_date)
        if min_time and start_time and start_time < min_time:
            return False
        if max_time and start_time and start_time > max_time:
            return False
        return True


def _quantile_values(tasks: list[TaskData], field: str, quantiles: list[float]) -> list[float]:
    values = sorted(float(getattr(task, field)) for task in tasks)
    if not values:
        return [0.0 for _ in quantiles]
    return [_nearest_rank(values, quantile) for quantile in quantiles]


def _nearest_rank(values: list[float], quantile: float) -> float:
    bounded = max(0.0, min(1.0, quantile))
    index = round((len(values) - 1) * bounded)
    return values[index]
