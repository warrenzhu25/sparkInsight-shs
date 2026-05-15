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
