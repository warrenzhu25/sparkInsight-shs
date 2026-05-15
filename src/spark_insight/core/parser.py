from __future__ import annotations

import gzip
import json
from collections import defaultdict
from collections.abc import Iterable
from pathlib import Path
from typing import Any

from spark_insight.core.models import (
    ApplicationAttemptInfo,
    ApplicationInfo,
    EnvironmentInfo,
    ExecutorSummary,
    JobData,
    ParsedApplication,
    StageData,
    TaskData,
)
from spark_insight.core.utils import format_spark_time


class EventLogParser:
    """Streaming parser for completed Spark event logs."""

    def parse(self, path: str | Path) -> ParsedApplication:
        state = _ParseState()
        for event in self._iter_events(Path(path)):
            state.apply(event)
        return state.to_parsed_application()

    def _iter_events(self, path: Path) -> Iterable[dict[str, Any]]:
        opener = gzip.open if path.suffix == ".gz" else open
        with opener(path, "rt", encoding="utf-8") as handle:
            for line in handle:
                line = line.strip()
                if not line:
                    continue
                yield json.loads(line)


class _ParseState:
    def __init__(self) -> None:
        self.app_id = ""
        self.app_name = "unknown"
        self.spark_user = ""
        self.spark_version = ""
        self.start_ms = 0
        self.end_ms = 0
        self.jobs: dict[int, JobData] = {}
        self.stages: dict[tuple[int, int], StageData] = {}
        self.stage_to_jobs: dict[int, list[int]] = defaultdict(list)
        self.tasks: dict[str, list[TaskData]] = defaultdict(list)
        self.executors: dict[str, ExecutorSummary] = {}
        self.environment = EnvironmentInfo()

    def apply(self, event: dict[str, Any]) -> None:
        event_name = event.get("Event")
        if event_name == "SparkListenerApplicationStart":
            self._application_start(event)
        elif event_name == "SparkListenerApplicationEnd":
            self.end_ms = int(event.get("Timestamp") or 0)
        elif event_name == "SparkListenerEnvironmentUpdate":
            self._environment(event)
        elif event_name == "SparkListenerJobStart":
            self._job_start(event)
        elif event_name == "SparkListenerJobEnd":
            self._job_end(event)
        elif event_name == "SparkListenerStageSubmitted":
            self._stage_submitted(event)
        elif event_name == "SparkListenerStageCompleted":
            self._stage_completed(event)
        elif event_name == "SparkListenerTaskEnd":
            self._task_end(event)
        elif event_name == "SparkListenerExecutorAdded":
            self._executor_added(event)
        elif event_name == "SparkListenerExecutorRemoved":
            self._executor_removed(event)

    def to_parsed_application(self) -> ParsedApplication:
        self._finalize_jobs()
        attempt = ApplicationAttemptInfo(
            startTime=format_spark_time(self.start_ms),
            endTime=format_spark_time(self.end_ms),
            lastUpdated=format_spark_time(self.end_ms or self.start_ms),
            duration=max((self.end_ms or self.start_ms) - self.start_ms, 0),
            sparkUser=self.spark_user,
            completed=bool(self.end_ms),
            appSparkVersion=self.spark_version,
        )
        return ParsedApplication(
            app_info=ApplicationInfo(
                id=self.app_id or "unknown",
                name=self.app_name,
                attempts=[attempt],
            ),
            jobs=sorted(self.jobs.values(), key=lambda item: item.jobId),
            stages=sorted(self.stages.values(), key=lambda item: (item.stageId, item.attemptId)),
            tasks=dict(self.tasks),
            executors=sorted(self.executors.values(), key=lambda item: item.id),
            environment=self.environment,
        )

    def _application_start(self, event: dict[str, Any]) -> None:
        self.app_id = event.get("App ID") or self.app_id
        self.app_name = event.get("App Name") or self.app_name
        self.spark_user = event.get("User") or self.spark_user
        self.spark_version = event.get("Spark Version") or self.spark_version
        self.start_ms = int(event.get("Timestamp") or self.start_ms or 0)

    def _environment(self, event: dict[str, Any]) -> None:
        props = event.get("Spark Properties") or {}
        system = event.get("System Properties") or {}
        classpath = event.get("Classpath Entries") or {}
        self.environment = EnvironmentInfo(
            sparkProperties=_pairs(props),
            systemProperties=_pairs(system),
            classpathEntries=_pairs(classpath),
        )
        if not self.spark_version:
            self.spark_version = str(props.get("spark.app.initial.jar.version", ""))

    def _job_start(self, event: dict[str, Any]) -> None:
        job_id = int(event.get("Job ID") or 0)
        stage_ids = [int(stage_id) for stage_id in event.get("Stage IDs", [])]
        properties = event.get("Properties") or {}
        job = self.jobs.get(job_id) or JobData(jobId=job_id)
        job.name = str(
            properties.get("spark.job.description")
            or properties.get("callSite.short")
            or ""
        )
        job.submissionTime = format_spark_time(int(event.get("Submission Time") or 0))
        job.stageIds = stage_ids
        job.status = "RUNNING"
        self.jobs[job_id] = job
        for stage_id in stage_ids:
            if job_id not in self.stage_to_jobs[stage_id]:
                self.stage_to_jobs[stage_id].append(job_id)

    def _job_end(self, event: dict[str, Any]) -> None:
        job_id = int(event.get("Job ID") or 0)
        job = self.jobs.get(job_id) or JobData(jobId=job_id)
        job.completionTime = format_spark_time(int(event.get("Completion Time") or 0))
        result = event.get("Job Result") or {}
        job.status = "SUCCEEDED" if result.get("Result") == "JobSucceeded" else "FAILED"
        self.jobs[job_id] = job

    def _stage_submitted(self, event: dict[str, Any]) -> None:
        info = event.get("Stage Info") or {}
        stage = self._stage_from_info(info)
        stage.status = "ACTIVE"
        properties = event.get("Properties") or {}
        stage.schedulingPool = str(properties.get("spark.scheduler.pool") or "")
        self.stages[(stage.stageId, stage.attemptId)] = stage

    def _stage_completed(self, event: dict[str, Any]) -> None:
        info = event.get("Stage Info") or {}
        stage = self._stage_from_info(info)
        stage.status = "FAILED" if info.get("Failure Reason") else "COMPLETE"
        existing = self.stages.get((stage.stageId, stage.attemptId))
        if existing and existing.schedulingPool:
            stage.schedulingPool = existing.schedulingPool
        self.stages[(stage.stageId, stage.attemptId)] = stage

    def _stage_from_info(self, info: dict[str, Any]) -> StageData:
        stage_id = int(info.get("Stage ID") or 0)
        attempt_id = int(info.get("Stage Attempt ID") or 0)
        accum = _accumulables(info.get("Accumulables") or [])
        return StageData(
            stageId=stage_id,
            attemptId=attempt_id,
            numTasks=int(info.get("Number of Tasks") or 0),
            numCompleteTasks=int(info.get("Number of Completed Tasks") or 0),
            numFailedTasks=int(info.get("Number of Failed Tasks") or 0),
            name=str(info.get("Stage Name") or ""),
            details=str(info.get("Details") or ""),
            executorRunTime=accum.get("internal.metrics.executorRunTime", 0),
            executorCpuTime=accum.get("internal.metrics.executorCpuTime", 0),
            inputBytes=accum.get("internal.metrics.input.bytesRead", 0),
            inputRecords=accum.get("internal.metrics.input.recordsRead", 0),
            outputBytes=accum.get("internal.metrics.output.bytesWritten", 0),
            outputRecords=accum.get("internal.metrics.output.recordsWritten", 0),
            shuffleReadBytes=accum.get("internal.metrics.shuffle.read.remoteBytesRead", 0)
            + accum.get("internal.metrics.shuffle.read.localBytesRead", 0),
            shuffleReadRecords=accum.get("internal.metrics.shuffle.read.recordsRead", 0),
            shuffleWriteBytes=accum.get("internal.metrics.shuffle.write.bytesWritten", 0),
            shuffleWriteRecords=accum.get("internal.metrics.shuffle.write.recordsWritten", 0),
            memoryBytesSpilled=accum.get("internal.metrics.memoryBytesSpilled", 0),
            diskBytesSpilled=accum.get("internal.metrics.diskBytesSpilled", 0),
        )

    def _task_end(self, event: dict[str, Any]) -> None:
        stage_id = int(event.get("Stage ID") or 0)
        attempt_id = int(event.get("Stage Attempt ID") or 0)
        info = event.get("Task Info") or {}
        metrics = event.get("Task Metrics") or {}
        input_metrics = metrics.get("Input Metrics") or {}
        output_metrics = metrics.get("Output Metrics") or {}
        shuffle_read = metrics.get("Shuffle Read Metrics") or {}
        shuffle_write = metrics.get("Shuffle Write Metrics") or {}
        task_status = str(event.get("Task End Reason", {}).get("Reason") or "Success")
        failed = bool(info.get("Failed")) or task_status != "Success"
        error_message = _task_error(event.get("Task End Reason") or {})
        task = TaskData(
            taskId=int(info.get("Task ID") or 0),
            index=int(info.get("Index") or 0),
            attempt=int(info.get("Attempt") or 0),
            launchTime=format_spark_time(int(info.get("Launch Time") or 0)),
            duration=max(int(info.get("Finish Time") or 0) - int(info.get("Launch Time") or 0), 0),
            executorRunTime=int(metrics.get("Executor Run Time") or 0),
            executorCpuTime=int(metrics.get("Executor CPU Time") or 0),
            jvmGcTime=int(metrics.get("JVM GC Time") or 0),
            inputBytes=int(input_metrics.get("Bytes Read") or 0),
            inputRecords=int(input_metrics.get("Records Read") or 0),
            outputBytes=int(output_metrics.get("Bytes Written") or 0),
            outputRecords=int(output_metrics.get("Records Written") or 0),
            shuffleReadBytes=int(shuffle_read.get("Remote Bytes Read") or 0)
            + int(shuffle_read.get("Local Bytes Read") or 0),
            shuffleReadRecords=int(shuffle_read.get("Records Read") or 0),
            shuffleWriteBytes=int(shuffle_write.get("Shuffle Bytes Written") or 0),
            shuffleWriteRecords=int(shuffle_write.get("Shuffle Records Written") or 0),
            memoryBytesSpilled=int(metrics.get("Memory Bytes Spilled") or 0),
            diskBytesSpilled=int(metrics.get("Disk Bytes Spilled") or 0),
            executorId=str(info.get("Executor ID") or ""),
            host=str(info.get("Host") or ""),
            status="FAILED" if failed else "SUCCESS",
            taskLocality=str(info.get("Locality") or ""),
            speculative=bool(info.get("Speculative")),
            errorMessage=error_message,
        )
        self.tasks[_stage_key(stage_id, attempt_id)].append(task)
        self._update_stage_from_task(stage_id, attempt_id, task, metrics)
        self._update_executor_from_task(task, metrics)

    def _update_stage_from_task(
        self, stage_id: int, attempt_id: int, task: TaskData, metrics: dict[str, Any]
    ) -> None:
        stage = self.stages.get((stage_id, attempt_id)) or StageData(
            stageId=stage_id,
            attemptId=attempt_id,
        )
        stage.numTasks = max(stage.numTasks, len(self.tasks[_stage_key(stage_id, attempt_id)]))
        if task.status == "FAILED":
            stage.numFailedTasks += 1
            if stage.status not in {"FAILED", "COMPLETE"}:
                stage.status = "FAILED"
        else:
            stage.numCompleteTasks += 1
            if stage.status not in {"FAILED", "COMPLETE"}:
                stage.status = "ACTIVE"
        stage.executorRunTime += int(metrics.get("Executor Run Time") or 0)
        stage.executorCpuTime += int(metrics.get("Executor CPU Time") or 0)
        input_metrics = metrics.get("Input Metrics") or {}
        output_metrics = metrics.get("Output Metrics") or {}
        shuffle_read = metrics.get("Shuffle Read Metrics") or {}
        shuffle_write = metrics.get("Shuffle Write Metrics") or {}
        stage.inputBytes += int(input_metrics.get("Bytes Read") or 0)
        stage.inputRecords += int(input_metrics.get("Records Read") or 0)
        stage.outputBytes += int(output_metrics.get("Bytes Written") or 0)
        stage.outputRecords += int(output_metrics.get("Records Written") or 0)
        stage.shuffleReadBytes += int(shuffle_read.get("Remote Bytes Read") or 0) + int(
            shuffle_read.get("Local Bytes Read") or 0
        )
        stage.shuffleReadRecords += int(shuffle_read.get("Records Read") or 0)
        stage.shuffleWriteBytes += int(shuffle_write.get("Shuffle Bytes Written") or 0)
        stage.shuffleWriteRecords += int(shuffle_write.get("Shuffle Records Written") or 0)
        stage.memoryBytesSpilled += int(metrics.get("Memory Bytes Spilled") or 0)
        stage.diskBytesSpilled += int(metrics.get("Disk Bytes Spilled") or 0)
        self.stages[(stage_id, attempt_id)] = stage

    def _executor_added(self, event: dict[str, Any]) -> None:
        executor_id = str(event.get("Executor ID") or "")
        info = event.get("Executor Info") or {}
        self.executors[executor_id] = ExecutorSummary(
            id=executor_id,
            hostPort=str(info.get("Host") or info.get("Host Port") or ""),
            isActive=True,
            totalCores=int(info.get("Total Cores") or 0),
            maxTasks=int(info.get("Total Cores") or 0),
            addTime=format_spark_time(int(event.get("Timestamp") or 0)),
        )

    def _executor_removed(self, event: dict[str, Any]) -> None:
        executor_id = str(event.get("Executor ID") or "")
        executor = self.executors.get(executor_id) or ExecutorSummary(id=executor_id)
        executor.isActive = False
        executor.removeTime = format_spark_time(int(event.get("Timestamp") or 0))
        executor.removeReason = str(event.get("Removed Reason") or "")
        self.executors[executor_id] = executor

    def _update_executor_from_task(self, task: TaskData, metrics: dict[str, Any]) -> None:
        executor = self.executors.get(task.executorId) or ExecutorSummary(
            id=task.executorId,
            hostPort=task.host,
        )
        executor.totalTasks += 1
        executor.totalDuration += task.duration
        executor.totalGCTime += int(metrics.get("JVM GC Time") or 0)
        if task.status == "FAILED":
            executor.failedTasks += 1
        else:
            executor.completedTasks += 1
        input_metrics = metrics.get("Input Metrics") or {}
        shuffle_read = metrics.get("Shuffle Read Metrics") or {}
        shuffle_write = metrics.get("Shuffle Write Metrics") or {}
        executor.totalInputBytes += int(input_metrics.get("Bytes Read") or 0)
        executor.totalShuffleRead += int(shuffle_read.get("Remote Bytes Read") or 0) + int(
            shuffle_read.get("Local Bytes Read") or 0
        )
        executor.totalShuffleWrite += int(shuffle_write.get("Shuffle Bytes Written") or 0)
        executor.maxMemory = max(executor.maxMemory, int(metrics.get("Peak Execution Memory") or 0))
        self.executors[task.executorId] = executor

    def _finalize_jobs(self) -> None:
        for job in self.jobs.values():
            stages = [self.stages[key] for key in self.stages if key[0] in set(job.stageIds)]
            job.numTasks = sum(stage.numTasks for stage in stages)
            job.numCompletedTasks = sum(stage.numCompleteTasks for stage in stages)
            job.numFailedTasks = sum(stage.numFailedTasks for stage in stages)
            job.numActiveStages = sum(stage.status == "ACTIVE" for stage in stages)
            job.numCompletedStages = sum(stage.status == "COMPLETE" for stage in stages)
            job.numFailedStages = sum(stage.status == "FAILED" for stage in stages)


def _stage_key(stage_id: int, attempt_id: int) -> str:
    return f"{stage_id}:{attempt_id}"


def _pairs(values: dict[str, Any]) -> list[tuple[str, str]]:
    return [(str(key), str(value)) for key, value in values.items()]


def _accumulables(values: list[dict[str, Any]]) -> dict[str, int]:
    result: dict[str, int] = {}
    for item in values:
        name = item.get("Name")
        if not name:
            continue
        value = item.get("Value") if item.get("Value") is not None else item.get("Update")
        try:
            result[str(name)] = int(value or 0)
        except (TypeError, ValueError):
            result[str(name)] = 0
    return result


def _task_error(reason: dict[str, Any]) -> str | None:
    if not reason:
        return None
    if reason.get("Reason") == "Success":
        return None
    return str(reason.get("Description") or reason.get("Class Name") or reason.get("Reason") or "")
