from __future__ import annotations

from typing import Any

import duckdb

from spark_insight.core.models import ParsedApplication
from spark_insight.core.utils import model_to_dict


class QueryEngine:
    """DuckDB query engine over a parsed application."""

    def __init__(self, parsed: ParsedApplication) -> None:
        self.parsed = parsed
        self.conn = duckdb.connect()
        self._load()

    def query(self, sql: str) -> list[dict[str, Any]]:
        columns = [item[0] for item in self.conn.execute(sql).description or []]
        rows = self.conn.fetchall()
        return [dict(zip(columns, row, strict=False)) for row in rows]

    def get_failed_tasks_summary(self) -> list[dict[str, Any]]:
        return self.query(
            """
            SELECT COALESCE(errorMessage, 'Unknown') AS error,
                   COUNT(*) AS count,
                   AVG(duration) AS avg_duration_ms,
                   MAX(duration) AS max_duration_ms
            FROM tasks
            WHERE status = 'FAILED'
            GROUP BY error
            ORDER BY count DESC
            LIMIT 20
            """
        )

    def get_stage_skew_analysis(self) -> list[dict[str, Any]]:
        return self.query(
            """
            SELECT stageId,
                   attemptId,
                   COUNT(*) AS numTasks,
                   MIN(duration) AS minDuration,
                   MAX(duration) AS maxDuration,
                   AVG(duration) AS avgDuration,
                   CASE
                       WHEN AVG(duration) = 0 THEN 0
                       ELSE MAX(duration) / AVG(duration)
                   END AS skewRatio
            FROM tasks
            GROUP BY stageId, attemptId
            HAVING COUNT(*) > 1
            ORDER BY skewRatio DESC
            """
        )

    def get_executor_stats(self) -> list[dict[str, Any]]:
        return self.query("SELECT * FROM executors ORDER BY totalTasks DESC, id")

    def get_summary_stats(self) -> dict[str, Any]:
        rows = self.query(
            """
            SELECT
                (SELECT COUNT(*) FROM jobs) AS jobs,
                (SELECT COUNT(*) FROM jobs WHERE status = 'FAILED') AS failed_jobs,
                (SELECT COUNT(*) FROM stages) AS stages,
                (SELECT COUNT(*) FROM tasks) AS tasks,
                (SELECT COUNT(*) FROM tasks WHERE status = 'FAILED') AS failed_tasks,
                (SELECT COALESCE(SUM(inputBytes), 0) FROM stages) AS input_bytes,
                (
                    SELECT COALESCE(SUM(shuffleReadBytes + shuffleWriteBytes), 0)
                    FROM stages
                ) AS shuffle_bytes
            """
        )
        return rows[0] if rows else {}

    def _load(self) -> None:
        jobs = [model_to_dict(item) for item in self.parsed.jobs]
        stages = [model_to_dict(item) for item in self.parsed.stages]
        executors = [model_to_dict(item) for item in self.parsed.executors]
        tasks = []
        for key, values in self.parsed.tasks.items():
            stage_id, attempt_id = key.split(":", 1)
            for task in values:
                row = model_to_dict(task)
                row["stageId"] = int(stage_id)
                row["attemptId"] = int(attempt_id)
                tasks.append(row)

        self._empty_jobs()
        self._empty_stages()
        self._empty_executors()
        self._empty_tasks()
        self._insert("jobs", _JOB_COLUMNS, jobs)
        self._insert("stages", _STAGE_COLUMNS, stages)
        self._insert("executors", _EXECUTOR_COLUMNS, executors)
        self._insert("tasks", _TASK_COLUMNS, tasks)

    def _insert(self, table: str, columns: list[str], rows: list[dict[str, Any]]) -> None:
        if not rows:
            return
        placeholders = ", ".join(["?"] * len(columns))
        column_sql = ", ".join(columns)
        values = [[row.get(column) for column in columns] for row in rows]
        self.conn.executemany(f"INSERT INTO {table} ({column_sql}) VALUES ({placeholders})", values)

    def _empty_jobs(self) -> None:
        self.conn.execute(
            """
            CREATE TABLE jobs(
                jobId INTEGER, name VARCHAR, submissionTime VARCHAR, completionTime VARCHAR,
                stageIds INTEGER[], status VARCHAR, numTasks INTEGER, numActiveTasks INTEGER,
                numCompletedTasks INTEGER, numSkippedTasks INTEGER, numFailedTasks INTEGER,
                numKilledTasks INTEGER, numActiveStages INTEGER, numCompletedStages INTEGER,
                numSkippedStages INTEGER, numFailedStages INTEGER
            )
            """
        )

    def _empty_stages(self) -> None:
        self.conn.execute(
            """
            CREATE TABLE stages(
                stageId INTEGER, attemptId INTEGER, status VARCHAR, name VARCHAR,
                numTasks INTEGER, numActiveTasks INTEGER, numCompleteTasks INTEGER,
                numFailedTasks INTEGER, numKilledTasks INTEGER, executorRunTime BIGINT,
                executorCpuTime BIGINT, inputBytes BIGINT, inputRecords BIGINT,
                outputBytes BIGINT, outputRecords BIGINT, shuffleReadBytes BIGINT,
                shuffleReadRecords BIGINT, shuffleWriteBytes BIGINT, shuffleWriteRecords BIGINT,
                memoryBytesSpilled BIGINT, diskBytesSpilled BIGINT, details VARCHAR,
                schedulingPool VARCHAR
            )
            """
        )

    def _empty_executors(self) -> None:
        self.conn.execute(
            """
            CREATE TABLE executors(
                id VARCHAR, hostPort VARCHAR, isActive BOOLEAN, rddBlocks INTEGER,
                memoryUsed BIGINT, diskUsed BIGINT, totalCores INTEGER, maxTasks INTEGER,
                activeTasks INTEGER, failedTasks INTEGER, completedTasks INTEGER,
                totalTasks INTEGER, totalDuration BIGINT, totalGCTime BIGINT,
                totalInputBytes BIGINT, totalShuffleRead BIGINT, totalShuffleWrite BIGINT,
                maxMemory BIGINT, addTime VARCHAR, removeTime VARCHAR, removeReason VARCHAR
            )
            """
        )

    def _empty_tasks(self) -> None:
        self.conn.execute(
            """
            CREATE TABLE tasks(
                taskId BIGINT, stageId INTEGER, attemptId INTEGER, duration BIGINT,
                executorRunTime BIGINT, executorCpuTime BIGINT, jvmGcTime BIGINT,
                inputBytes BIGINT, inputRecords BIGINT, outputBytes BIGINT,
                outputRecords BIGINT, shuffleReadBytes BIGINT, shuffleReadRecords BIGINT,
                shuffleWriteBytes BIGINT, shuffleWriteRecords BIGINT,
                memoryBytesSpilled BIGINT, diskBytesSpilled BIGINT,
                index INTEGER, attempt INTEGER, launchTime VARCHAR, executorId VARCHAR,
                host VARCHAR, status VARCHAR, taskLocality VARCHAR, speculative BOOLEAN,
                errorMessage VARCHAR
            )
            """
        )


_JOB_COLUMNS = [
    "jobId",
    "name",
    "submissionTime",
    "completionTime",
    "stageIds",
    "status",
    "numTasks",
    "numActiveTasks",
    "numCompletedTasks",
    "numSkippedTasks",
    "numFailedTasks",
    "numKilledTasks",
    "numActiveStages",
    "numCompletedStages",
    "numSkippedStages",
    "numFailedStages",
]

_STAGE_COLUMNS = [
    "stageId",
    "attemptId",
    "status",
    "name",
    "numTasks",
    "numActiveTasks",
    "numCompleteTasks",
    "numFailedTasks",
    "numKilledTasks",
    "executorRunTime",
    "executorCpuTime",
    "inputBytes",
    "inputRecords",
    "outputBytes",
    "outputRecords",
    "shuffleReadBytes",
    "shuffleReadRecords",
    "shuffleWriteBytes",
    "shuffleWriteRecords",
    "memoryBytesSpilled",
    "diskBytesSpilled",
    "details",
    "schedulingPool",
]

_EXECUTOR_COLUMNS = [
    "id",
    "hostPort",
    "isActive",
    "rddBlocks",
    "memoryUsed",
    "diskUsed",
    "totalCores",
    "maxTasks",
    "activeTasks",
    "failedTasks",
    "completedTasks",
    "totalTasks",
    "totalDuration",
    "totalGCTime",
    "totalInputBytes",
    "totalShuffleRead",
    "totalShuffleWrite",
    "maxMemory",
    "addTime",
    "removeTime",
    "removeReason",
]

_TASK_COLUMNS = [
    "taskId",
    "stageId",
    "attemptId",
    "duration",
    "executorRunTime",
    "executorCpuTime",
    "jvmGcTime",
    "inputBytes",
    "inputRecords",
    "outputBytes",
    "outputRecords",
    "shuffleReadBytes",
    "shuffleReadRecords",
    "shuffleWriteBytes",
    "shuffleWriteRecords",
    "memoryBytesSpilled",
    "diskBytesSpilled",
    "index",
    "attempt",
    "launchTime",
    "executorId",
    "host",
    "status",
    "taskLocality",
    "speculative",
    "errorMessage",
]
