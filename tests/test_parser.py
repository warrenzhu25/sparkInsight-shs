from pathlib import Path

from spark_insight.core.parser import EventLogParser


def test_parser_extracts_application_jobs_stages_tasks_and_executors():
    parsed = EventLogParser().parse(Path("examples/eventlogs/local-0001.json"))

    assert parsed.app_info.id == "local-0001"
    assert parsed.app_info.name == "Example Spark App"
    assert parsed.app_info.attempts[0].completed is True
    assert parsed.jobs[0].status == "SUCCEEDED"
    assert parsed.jobs[0].numTasks == 2
    assert parsed.stages[0].status == "COMPLETE"
    assert parsed.stages[0].inputBytes == 3072
    assert len(parsed.tasks["0:0"]) == 2
    assert parsed.executors[0].totalTasks == 2
