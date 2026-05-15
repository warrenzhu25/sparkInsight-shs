from __future__ import annotations

from fastapi import APIRouter, Request
from fastapi.responses import PlainTextResponse

from spark_insight.core.service import ApplicationService
from spark_insight.core.utils import format_bytes

router = APIRouter(prefix="/api/ai", tags=["AI optimized"])


def _service(request: Request) -> ApplicationService:
    return request.app.state.app_service


@router.get("/{app_id}/summary", response_class=PlainTextResponse)
async def get_summary(request: Request, app_id: str) -> PlainTextResponse:
    service = _service(request)
    app = service.get_application(app_id)
    attempt = app.attempts[0] if app.attempts else None
    stats = service.get_query_engine(app_id).get_summary_stats()
    return PlainTextResponse(
        "\n".join(
            [
                f"# {app.name}",
                "",
                "| Metric | Value |",
                "|--------|-------|",
                f"| App ID | `{app.id}` |",
                f"| Duration | {(attempt.duration if attempt else 0) // 1000}s |",
                f"| Status | {'Completed' if attempt and attempt.completed else 'Running'} |",
                f"| Jobs | {stats.get('jobs', 0)} ({stats.get('failed_jobs', 0)} failed) |",
                f"| Stages | {stats.get('stages', 0)} |",
                f"| Tasks | {stats.get('tasks', 0)} ({stats.get('failed_tasks', 0)} failed) |",
                f"| Input | {format_bytes(stats.get('input_bytes', 0))} |",
                f"| Shuffle | {format_bytes(stats.get('shuffle_bytes', 0))} |",
            ]
        )
    )


@router.get("/{app_id}/problems", response_class=PlainTextResponse)
async def get_problems(request: Request, app_id: str) -> PlainTextResponse:
    engine = _service(request).get_query_engine(app_id)
    sections: list[str] = []
    failures = engine.get_failed_tasks_summary()
    if failures:
        sections.append("## Failed Tasks")
        for failure in failures[:5]:
            sections.append(f"- {failure['count']}x: {str(failure['error'])[:200]}")
    skew = [row for row in engine.get_stage_skew_analysis() if row.get("skewRatio", 0) > 5]
    if skew:
        sections.append("\n## Data Skew")
        for row in skew[:5]:
            sections.append(f"- Stage {row['stageId']}: {row['skewRatio']:.1f}x skew")
    if not sections:
        return PlainTextResponse("# No Problems\n\nNo significant issues detected.")
    return PlainTextResponse("# Problems\n\n" + "\n".join(sections))


@router.get("/{app_id}/stages", response_class=PlainTextResponse)
async def get_stages(request: Request, app_id: str) -> PlainTextResponse:
    stages = _service(request).get_stages(app_id)
    lines = [
        "# Stages",
        "",
        "| ID | Status | Tasks | Input | Shuffle | Name |",
        "|---:|--------|------:|------:|--------:|------|",
    ]
    for stage in stages[:50]:
        shuffle = stage.shuffleReadBytes + stage.shuffleWriteBytes
        lines.append(
            f"| {stage.stageId}.{stage.attemptId} | {stage.status} | {stage.numTasks} | "
            f"{format_bytes(stage.inputBytes)} | {format_bytes(shuffle)} | {stage.name[:40]} |"
        )
    return PlainTextResponse("\n".join(lines))


@router.get("/{app_id}/executors", response_class=PlainTextResponse)
async def get_executors(request: Request, app_id: str) -> PlainTextResponse:
    executors = _service(request).get_executors(app_id)
    lines = [
        "# Executors",
        "",
        "| ID | Active | Tasks | Failed | GC Time | Host |",
        "|----|--------|------:|-------:|--------:|------|",
    ]
    for executor in executors[:50]:
        lines.append(
            f"| {executor.id} | {executor.isActive} | {executor.totalTasks} | "
            f"{executor.failedTasks} | {executor.totalGCTime // 1000}s | {executor.hostPort} |"
        )
    return PlainTextResponse("\n".join(lines))


@router.get("/{app_id}/config", response_class=PlainTextResponse)
async def get_config(request: Request, app_id: str) -> PlainTextResponse:
    env = _service(request).get_environment(app_id)
    lines = ["# Spark Config", "", "| Key | Value |", "|-----|-------|"]
    for key, value in env.sparkProperties[:100]:
        lines.append(f"| `{key}` | `{value}` |")
    return PlainTextResponse("\n".join(lines))
