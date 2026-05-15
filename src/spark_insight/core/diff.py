from __future__ import annotations

from spark_insight.core.models import DiffResult, StageData
from spark_insight.core.service import ApplicationService


class DiffEngine:
    def __init__(self, app_service: ApplicationService) -> None:
        self.app_service = app_service

    def compare(self, app_id_1: str, app_id_2: str) -> DiffResult:
        app_1 = self.app_service.get_application(app_id_1)
        app_2 = self.app_service.get_application(app_id_2)
        jobs_1 = self.app_service.get_jobs(app_id_1)
        jobs_2 = self.app_service.get_jobs(app_id_2)
        stages_1 = self.app_service.get_stages(app_id_1)
        stages_2 = self.app_service.get_stages(app_id_2)
        tasks_1 = sum(stage.numTasks for stage in stages_1)
        tasks_2 = sum(stage.numTasks for stage in stages_2)
        duration_1 = app_1.attempts[0].duration if app_1.attempts else 0
        duration_2 = app_2.attempts[0].duration if app_2.attempts else 0
        duration_diff = duration_2 - duration_1
        threshold = max(duration_1 * 0.1, 1000)
        if duration_diff > threshold:
            performance_change = "degraded"
        elif duration_diff < -threshold:
            performance_change = "improved"
        else:
            performance_change = "similar"
        return DiffResult(
            app_id_1=app_id_1,
            app_id_2=app_id_2,
            duration_diff=duration_diff,
            job_count_diff=len(jobs_2) - len(jobs_1),
            stage_count_diff=len(stages_2) - len(stages_1),
            task_count_diff=tasks_2 - tasks_1,
            performance_change=performance_change,
            key_differences=_key_differences(duration_diff, stages_1, stages_2),
            stage_diffs=_stage_diffs(stages_1, stages_2),
        )


def _stage_diffs(stages_1: list[StageData], stages_2: list[StageData]) -> list[dict]:
    by_name_1 = {stage.name or f"{stage.stageId}.{stage.attemptId}": stage for stage in stages_1}
    by_name_2 = {stage.name or f"{stage.stageId}.{stage.attemptId}": stage for stage in stages_2}
    diffs = []
    for name in sorted(set(by_name_1) | set(by_name_2)):
        stage_1 = by_name_1.get(name)
        stage_2 = by_name_2.get(name)
        duration_1 = stage_1.executorRunTime if stage_1 else 0
        duration_2 = stage_2.executorRunTime if stage_2 else 0
        shuffle_1 = _shuffle_bytes(stage_1)
        shuffle_2 = _shuffle_bytes(stage_2)
        diffs.append(
            {
                "stage_name": name,
                "duration_1": duration_1,
                "duration_2": duration_2,
                "duration_diff": duration_2 - duration_1,
                "shuffle_1": shuffle_1,
                "shuffle_2": shuffle_2,
                "shuffle_diff": shuffle_2 - shuffle_1,
                "task_count_1": stage_1.numTasks if stage_1 else 0,
                "task_count_2": stage_2.numTasks if stage_2 else 0,
            }
        )
    return sorted(diffs, key=lambda item: abs(item["duration_diff"]), reverse=True)


def _key_differences(
    duration_diff: int,
    stages_1: list[StageData],
    stages_2: list[StageData],
) -> list[str]:
    differences: list[str] = []
    if duration_diff:
        direction = "slower" if duration_diff > 0 else "faster"
        differences.append(f"Application 2 is {abs(duration_diff) / 1000:.1f}s {direction}.")
    for stage in _stage_diffs(stages_1, stages_2)[:3]:
        if stage["duration_diff"]:
            direction = "higher" if stage["duration_diff"] > 0 else "lower"
            differences.append(
                f"{stage['stage_name']} executor runtime is "
                f"{abs(stage['duration_diff'])}ms {direction}."
            )
    return differences


def _shuffle_bytes(stage: StageData | None) -> int:
    if not stage:
        return 0
    return stage.shuffleReadBytes + stage.shuffleWriteBytes
