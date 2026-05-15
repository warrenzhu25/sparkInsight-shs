from __future__ import annotations

from typing import Annotated

from fastapi import APIRouter, File, Request, UploadFile

from spark_insight.core.diff import DiffEngine
from spark_insight.core.models import (
    AnalysisResult,
    AnalyzeRequest,
    DiffAnalyzeRequest,
    DiffRequest,
    DiffResult,
)
from spark_insight.core.service import ApplicationService

router = APIRouter(prefix="/api/insight", tags=["Spark Insight"])


def _service(request: Request) -> ApplicationService:
    return request.app.state.app_service


@router.post("/upload")
async def upload_eventlog(request: Request, file: Annotated[UploadFile, File()]):
    return await _service(request).upload_eventlog(file)


@router.post("/diff", response_model=DiffResult)
async def diff_applications(request: Request, payload: DiffRequest) -> DiffResult:
    return DiffEngine(_service(request)).compare(payload.app_id_1, payload.app_id_2)


@router.post("/analyze", response_model=AnalysisResult)
async def analyze_application(request: Request, payload: AnalyzeRequest) -> AnalysisResult:
    _service(request).get_application(payload.app_id)
    return AnalysisResult(
        summary=(
            "LLM analysis is not enabled in the core install. Install the llm extra and configure "
            "a provider to answer natural language questions."
        ),
        visualization="none",
    )


@router.post("/diff/analyze", response_model=AnalysisResult)
async def analyze_diff(request: Request, payload: DiffAnalyzeRequest) -> AnalysisResult:
    DiffEngine(_service(request)).compare(payload.app_id_1, payload.app_id_2)
    return AnalysisResult(
        summary=(
            "LLM diff analysis is not enabled in the core install. The non-LLM diff endpoint is "
            "available at /api/insight/diff."
        ),
        visualization="none",
    )
