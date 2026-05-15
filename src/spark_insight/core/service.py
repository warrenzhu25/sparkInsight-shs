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
)
from spark_insight.core.query import QueryEngine


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
        limit: int = 100,
    ) -> list[ApplicationInfo]:
        self.refresh_index()
        apps: list[ApplicationInfo] = []
        for path in list(self._app_index.values())[:limit]:
            parsed = self.cache.get_or_parse(path)
            app = parsed.app_info
            if status:
                completed = bool(app.attempts and app.attempts[0].completed)
                if status.lower() == "completed" and not completed:
                    continue
                if status.lower() == "running" and completed:
                    continue
            apps.append(app)
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
        if tasks and hasattr(tasks[0], sort_by):
            tasks = sorted(tasks, key=lambda task: getattr(task, sort_by))
        return tasks[offset : offset + length]

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
