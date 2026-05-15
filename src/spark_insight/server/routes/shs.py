from __future__ import annotations

from fastapi import APIRouter, Request

from spark_insight import __version__
from spark_insight.core.models import (
    ApplicationInfo,
    EnvironmentInfo,
    ExecutorSummary,
    JobData,
    StageData,
    TaskData,
)
from spark_insight.core.service import ApplicationService

router = APIRouter(prefix="/api/v1", tags=["Spark History Server"])


def _service(request: Request) -> ApplicationService:
    return request.app.state.app_service


@router.get("/version")
async def get_version() -> dict[str, str]:
    return {"spark-insight": __version__}


@router.get("/applications", response_model=list[ApplicationInfo])
async def list_applications(
    request: Request,
    status: str | None = None,
    minDate: str | None = None,
    maxDate: str | None = None,
    limit: int = 100,
) -> list[ApplicationInfo]:
    del minDate, maxDate
    return _service(request).list_applications(status=status, limit=limit)


@router.get("/applications/{app_id}", response_model=ApplicationInfo)
async def get_application(request: Request, app_id: str) -> ApplicationInfo:
    return _service(request).get_application(app_id)


@router.get("/applications/{app_id}/jobs", response_model=list[JobData])
async def list_jobs(request: Request, app_id: str, status: str | None = None) -> list[JobData]:
    return _service(request).get_jobs(app_id, status=status)


@router.get("/applications/{app_id}/jobs/{job_id}", response_model=JobData)
async def get_job(request: Request, app_id: str, job_id: int) -> JobData:
    return _service(request).get_job(app_id, job_id)


@router.get("/applications/{app_id}/stages", response_model=list[StageData])
async def list_stages(request: Request, app_id: str, status: str | None = None) -> list[StageData]:
    return _service(request).get_stages(app_id, status=status)


@router.get("/applications/{app_id}/stages/{stage_id}/{attempt_id}", response_model=StageData)
async def get_stage(request: Request, app_id: str, stage_id: int, attempt_id: int) -> StageData:
    return _service(request).get_stage(app_id, stage_id, attempt_id)


@router.get(
    "/applications/{app_id}/stages/{stage_id}/{attempt_id}/taskList",
    response_model=list[TaskData],
)
async def get_task_list(
    request: Request,
    app_id: str,
    stage_id: int,
    attempt_id: int,
    offset: int = 0,
    length: int = 100,
    sortBy: str = "taskId",
) -> list[TaskData]:
    return _service(request).get_tasks(app_id, stage_id, attempt_id, offset, length, sortBy)


@router.get("/applications/{app_id}/executors", response_model=list[ExecutorSummary])
async def list_executors(request: Request, app_id: str) -> list[ExecutorSummary]:
    return _service(request).get_executors(app_id)


@router.get("/applications/{app_id}/allexecutors", response_model=list[ExecutorSummary])
async def list_all_executors(request: Request, app_id: str) -> list[ExecutorSummary]:
    return _service(request).get_executors(app_id)


@router.get("/applications/{app_id}/environment", response_model=EnvironmentInfo)
async def get_environment(request: Request, app_id: str) -> EnvironmentInfo:
    return _service(request).get_environment(app_id)


@router.get("/applications/{app_id}/storage/rdd")
async def get_rdd_storage(request: Request, app_id: str) -> list[dict]:
    _service(request).get_application(app_id)
    return []


@router.get("/applications/{app_id}/logs")
async def get_logs(request: Request, app_id: str) -> dict[str, list]:
    _service(request).get_application(app_id)
    return {"logs": []}
