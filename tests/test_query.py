from pathlib import Path

from spark_insight.core.parser import EventLogParser
from spark_insight.core.query import QueryEngine


def test_query_engine_summary_stats():
    parsed = EventLogParser().parse(Path("examples/eventlogs/local-0001.json"))
    stats = QueryEngine(parsed).get_summary_stats()

    assert stats["jobs"] == 1
    assert stats["stages"] == 1
    assert stats["tasks"] == 2
    assert stats["input_bytes"] == 3072


def test_query_engine_uses_task_metric_columns():
    parsed = EventLogParser().parse(Path("examples/eventlogs/local-0001.json"))
    rows = QueryEngine(parsed).query(
        "SELECT SUM(inputBytes) AS input_bytes, SUM(shuffleWriteBytes) AS shuffle_write FROM tasks"
    )

    assert rows[0]["input_bytes"] == 3072
    assert rows[0]["shuffle_write"] == 1536
