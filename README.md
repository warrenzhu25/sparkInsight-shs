# Spark Insight

Spark Insight is a Python-native Spark History Server compatible service. The v1
foundation focuses on correctness for completed local event logs:

- SHS-compatible REST endpoints under `/api/v1`
- Streaming JSON/gzip event log parsing
- Parsed cache on disk
- DuckDB-backed query helpers
- Thin CLI client that talks to the REST service

The design also includes UI, LLM, MCP, S3, and HDFS support. Those are optional
extras and extension points in this initial scaffold.

## Install

```bash
python3 -m pip install -e ".[dev]"
```

## Run

```bash
spark-insight serve --log-dir examples/eventlogs --port 18080
```

Then open:

- API docs: `http://localhost:18080/docs`
- SHS applications: `http://localhost:18080/api/v1/applications`
- Health/version: `http://localhost:18080/api/v1/version`

## CLI

```bash
spark-insight apps
spark-insight jobs local-0001
spark-insight stages local-0001
spark-insight executors local-0001
```

Use `--server` or `SPARK_INSIGHT_SERVER` to point the CLI at a remote service.

## Development

```bash
python3 -m pytest
ruff check src tests
ruff format src tests
```

The repository CI runs the same checks across Python 3.10, 3.11, and 3.12
using `uv`.
