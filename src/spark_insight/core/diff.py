from __future__ import annotations

from spark_insight.core.models import DiffResult
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
            key_differences=[],
        )
