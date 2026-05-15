from __future__ import annotations

from spark_insight.core.models import AnalysisResult


class LLMService:
    def analyze_unconfigured(self) -> AnalysisResult:
        return AnalysisResult(
            summary="LLM support is optional. Install spark-insight[llm] and configure a provider.",
            visualization="none",
        )
