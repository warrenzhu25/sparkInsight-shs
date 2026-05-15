from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Literal

from pydantic import BaseModel, Field


class ApplicationAttemptInfo(BaseModel):
    attemptId: str | None = None
    startTime: str = ""
    endTime: str = ""
    lastUpdated: str = ""
    duration: int = 0
    sparkUser: str = ""
    completed: bool = False
    appSparkVersion: str = ""


class ApplicationInfo(BaseModel):
    id: str
    name: str
    attempts: list[ApplicationAttemptInfo] = Field(default_factory=list)


class JobData(BaseModel):
    jobId: int
    name: str = ""
    submissionTime: str | None = None
    completionTime: str | None = None
    stageIds: list[int] = Field(default_factory=list)
    status: str = "RUNNING"
    numTasks: int = 0
    numActiveTasks: int = 0
    numCompletedTasks: int = 0
    numSkippedTasks: int = 0
    numFailedTasks: int = 0
    numKilledTasks: int = 0
    numActiveStages: int = 0
    numCompletedStages: int = 0
    numSkippedStages: int = 0
    numFailedStages: int = 0


class StageData(BaseModel):
    status: str = "PENDING"
    stageId: int
    attemptId: int = 0
    numTasks: int = 0
    numActiveTasks: int = 0
    numCompleteTasks: int = 0
    numFailedTasks: int = 0
    numKilledTasks: int = 0
    executorRunTime: int = 0
    executorCpuTime: int = 0
    inputBytes: int = 0
    inputRecords: int = 0
    outputBytes: int = 0
    outputRecords: int = 0
    shuffleReadBytes: int = 0
    shuffleReadRecords: int = 0
    shuffleWriteBytes: int = 0
    shuffleWriteRecords: int = 0
    memoryBytesSpilled: int = 0
    diskBytesSpilled: int = 0
    name: str = ""
    details: str = ""
    schedulingPool: str = ""


class TaskData(BaseModel):
    taskId: int
    index: int = 0
    attempt: int = 0
    launchTime: str = ""
    duration: int = 0
    executorId: str = ""
    host: str = ""
    status: str = "SUCCESS"
    taskLocality: str = ""
    speculative: bool = False
    errorMessage: str | None = None


class ExecutorSummary(BaseModel):
    id: str
    hostPort: str = ""
    isActive: bool = True
    rddBlocks: int = 0
    memoryUsed: int = 0
    diskUsed: int = 0
    totalCores: int = 0
    maxTasks: int = 0
    activeTasks: int = 0
    failedTasks: int = 0
    completedTasks: int = 0
    totalTasks: int = 0
    totalDuration: int = 0
    totalGCTime: int = 0
    totalInputBytes: int = 0
    totalShuffleRead: int = 0
    totalShuffleWrite: int = 0
    maxMemory: int = 0
    addTime: str = ""
    removeTime: str | None = None
    removeReason: str | None = None


class EnvironmentInfo(BaseModel):
    runtime: dict[str, Any] = Field(default_factory=dict)
    sparkProperties: list[tuple[str, str]] = Field(default_factory=list)
    systemProperties: list[tuple[str, str]] = Field(default_factory=list)
    classpathEntries: list[tuple[str, str]] = Field(default_factory=list)


class AnalysisResult(BaseModel):
    summary: str
    visualization: Literal["table", "bar_chart", "line_chart", "pie_chart", "none"] = "none"
    data: list[dict[str, Any]] = Field(default_factory=list)
    x_column: str | None = None
    y_column: str | None = None


class AnalyzeRequest(BaseModel):
    app_id: str
    question: str


class DiffRequest(BaseModel):
    app_id_1: str
    app_id_2: str


class DiffAnalyzeRequest(DiffRequest):
    question: str


class DiffResult(BaseModel):
    app_id_1: str
    app_id_2: str
    duration_diff: int
    job_count_diff: int
    stage_count_diff: int
    task_count_diff: int
    performance_change: str
    key_differences: list[str] = Field(default_factory=list)


@dataclass
class ParsedApplication:
    app_info: ApplicationInfo
    jobs: list[JobData]
    stages: list[StageData]
    tasks: dict[str, list[TaskData]]
    executors: list[ExecutorSummary]
    environment: EnvironmentInfo

