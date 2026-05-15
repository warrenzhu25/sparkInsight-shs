from __future__ import annotations

import os

import click
import httpx
from rich.console import Console
from rich.markdown import Markdown
from rich.table import Table

from spark_insight.core.utils import format_bytes

DEFAULT_SERVER = "http://localhost:18080"
console = Console()


@click.group()
@click.option("--server", "-s", default=None, help="Spark Insight server URL.")
@click.pass_context
def cli(ctx: click.Context, server: str | None) -> None:
    """Spark Insight CLI."""
    ctx.ensure_object(dict)
    ctx.obj["server"] = server or os.getenv("SPARK_INSIGHT_SERVER", DEFAULT_SERVER)


@cli.command()
@click.option("--host", default="0.0.0.0", show_default=True)
@click.option("--port", default=18080, show_default=True)
@click.option("--log-dir", default=None, type=click.Path(file_okay=False))
@click.option("--cache-dir", default=None, type=click.Path(file_okay=False))
def serve(host: str, port: int, log_dir: str | None, cache_dir: str | None) -> None:
    """Start the REST API service."""
    import uvicorn

    from spark_insight.server.app import create_app

    app = create_app(log_dir=log_dir, cache_dir=cache_dir)
    console.print(f"REST API: http://{host}:{port}/api/v1/")
    console.print(f"Docs:     http://{host}:{port}/docs")
    uvicorn.run(app, host=host, port=port)


@cli.command()
@click.option("--status", type=click.Choice(["completed", "running"]))
@click.option("--limit", default=20, show_default=True)
@click.pass_context
def apps(ctx: click.Context, status: str | None, limit: int) -> None:
    """List applications."""
    data = _client(ctx).get(
        "/api/v1/applications",
        params={"status": status, "limit": limit},
    ).json()
    table = Table(title="Applications")
    table.add_column("App ID")
    table.add_column("Name")
    table.add_column("Status")
    table.add_column("Duration")
    table.add_column("Start")
    for app in data:
        attempt = app.get("attempts", [{}])[0]
        table.add_row(
            app["id"],
            app["name"],
            "completed" if attempt.get("completed") else "running",
            f"{attempt.get('duration', 0) / 1000:.1f}s",
            attempt.get("startTime", "")[:19],
        )
    console.print(table)


@cli.command()
@click.argument("app_id")
@click.pass_context
def jobs(ctx: click.Context, app_id: str) -> None:
    """List jobs for an application."""
    data = _client(ctx).get(f"/api/v1/applications/{app_id}/jobs").json()
    table = Table(title=f"Jobs for {app_id}")
    table.add_column("ID")
    table.add_column("Status")
    table.add_column("Tasks")
    table.add_column("Stages")
    table.add_column("Name")
    for job in data:
        table.add_row(
            str(job["jobId"]),
            job["status"],
            f"{job['numCompletedTasks']}/{job['numTasks']}",
            f"{job['numCompletedStages']}/{len(job['stageIds'])}",
            job["name"][:50],
        )
    console.print(table)


@cli.command()
@click.argument("app_id")
@click.pass_context
def stages(ctx: click.Context, app_id: str) -> None:
    """List stages for an application."""
    data = _client(ctx).get(f"/api/v1/applications/{app_id}/stages").json()
    table = Table(title=f"Stages for {app_id}")
    table.add_column("ID")
    table.add_column("Status")
    table.add_column("Tasks")
    table.add_column("Input")
    table.add_column("Shuffle")
    table.add_column("Name")
    for stage in data:
        table.add_row(
            f"{stage['stageId']}.{stage['attemptId']}",
            stage["status"],
            f"{stage['numCompleteTasks']}/{stage['numTasks']}",
            format_bytes(stage["inputBytes"]),
            format_bytes(stage["shuffleReadBytes"] + stage["shuffleWriteBytes"]),
            stage["name"][:50],
        )
    console.print(table)


@cli.command()
@click.argument("app_id")
@click.pass_context
def executors(ctx: click.Context, app_id: str) -> None:
    """List executors for an application."""
    data = _client(ctx).get(f"/api/v1/applications/{app_id}/executors").json()
    table = Table(title=f"Executors for {app_id}")
    table.add_column("ID")
    table.add_column("Active")
    table.add_column("Cores")
    table.add_column("Tasks")
    table.add_column("Failed")
    table.add_column("Host")
    for executor in data:
        table.add_row(
            executor["id"],
            str(executor["isActive"]),
            str(executor["totalCores"]),
            str(executor["totalTasks"]),
            str(executor["failedTasks"]),
            executor["hostPort"],
        )
    console.print(table)


@cli.command()
@click.argument("app_id")
@click.pass_context
def summary(ctx: click.Context, app_id: str) -> None:
    """Show compact markdown summary."""
    console.print(Markdown(_client(ctx).get(f"/api/ai/{app_id}/summary").text))


@cli.command()
@click.argument("app_id")
@click.pass_context
def problems(ctx: click.Context, app_id: str) -> None:
    """Show compact problem analysis."""
    console.print(Markdown(_client(ctx).get(f"/api/ai/{app_id}/problems").text))


@cli.command()
@click.argument("eventlog", type=click.Path(exists=True, dir_okay=False))
@click.pass_context
def upload(ctx: click.Context, eventlog: str) -> None:
    """Upload an event log to the service."""
    with open(eventlog, "rb") as handle:
        response = _client(ctx).post("/api/insight/upload", files={"file": handle})
    response.raise_for_status()
    app = response.json()
    console.print(f"Uploaded {app['id']}: {app['name']}")


def _client(ctx: click.Context) -> httpx.Client:
    client = httpx.Client(base_url=ctx.obj["server"], timeout=60)
    original_request = client.request

    def request_with_status(*args, **kwargs):
        response = original_request(*args, **kwargs)
        response.raise_for_status()
        return response

    client.request = request_with_status  # type: ignore[method-assign]
    return client
