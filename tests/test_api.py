from fastapi.testclient import TestClient

from spark_insight.server.app import create_app


def test_shs_application_endpoints(tmp_path):
    app = create_app(log_dir="examples/eventlogs", cache_dir=str(tmp_path))
    client = TestClient(app)

    apps = client.get("/api/v1/applications").json()
    assert apps[0]["id"] == "local-0001"

    jobs = client.get("/api/v1/applications/local-0001/jobs").json()
    assert jobs[0]["status"] == "SUCCEEDED"

    stages = client.get("/api/v1/applications/local-0001/stages").json()
    assert stages[0]["numTasks"] == 2

    summary = client.get("/api/ai/local-0001/summary")
    assert summary.status_code == 200
    assert "Example Spark App" in summary.text


def test_shs_filters_task_list_and_compact_problem_endpoint(tmp_path):
    app = create_app(log_dir="examples/eventlogs", cache_dir=str(tmp_path))
    client = TestClient(app)

    completed = client.get("/api/v1/applications", params={"status": "completed"}).json()
    running = client.get("/api/v1/applications", params={"status": "running"}).json()
    tasks = client.get(
        "/api/v1/applications/local-0001/stages/0/0/taskList",
        params={"offset": 1, "length": 1},
    ).json()
    problems = client.get("/api/ai/local-0001/problems")

    assert len(completed) == 1
    assert running == []
    assert len(tasks) == 1
    assert tasks[0]["taskId"] == 2
    assert problems.status_code == 200
    assert "# No Problems" in problems.text


def test_task_list_sorting_and_task_summary(tmp_path):
    app = create_app(log_dir="examples/eventlogs", cache_dir=str(tmp_path))
    client = TestClient(app)

    tasks = client.get(
        "/api/v1/applications/local-0001/stages/0/0/taskList",
        params={"sortBy": "-duration"},
    ).json()
    summary = client.get(
        "/api/v1/applications/local-0001/stages/0/0/taskSummary",
        params={"quantiles": "0,0.5,1"},
    ).json()

    assert [task["taskId"] for task in tasks] == [2, 1]
    assert summary["quantiles"] == [0, 0.5, 1]
    assert summary["duration"] == [1000, 1000, 1200]
    assert summary["inputBytes"] == [1024, 1024, 2048]
