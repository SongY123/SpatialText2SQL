"""LLM client implementations for quality-control self-consistency judging."""

from __future__ import annotations

from typing import Any

from src.synthesis.llm import (
    LLMClient,
    LLMGenerationResponse,
    MockLLMClient,
    OllamaLLMClient,
    OpenAICompatibleLLMClient,
    build_llm_client,
)


QualityControlLLMResponse = LLMGenerationResponse
QualityControlLLMClient = LLMClient


class OpenAICompatibleQualityControlLLM(OpenAICompatibleLLMClient):
    pass


class OllamaQualityControlLLM(OllamaLLMClient):
    pass


class MockQualityControlLLM(MockLLMClient):
    pass


def build_quality_control_llm(
    *,
    config: Any | None = None,
    provider: str | None = None,
    model: str | None = None,
    base_url: str | None = None,
    api_key_env: str | None = None,
    temperature: float | None = None,
    max_tokens: int | None = None,
    timeout: int | None = None,
    max_retries: int | None = None,
    trust_env: bool | None = None,
) -> QualityControlLLMClient:
    if config is not None:
        provider = getattr(config, "provider", provider)
        model = getattr(config, "model", model)
        base_url = getattr(config, "base_url", base_url)
        api_key_env = getattr(config, "api_key_env", api_key_env)
        temperature = getattr(config, "temperature", temperature)
        max_tokens = getattr(config, "max_tokens", max_tokens)
        timeout = getattr(config, "timeout", timeout)
        max_retries = getattr(config, "max_retries", max_retries)
        trust_env = getattr(config, "trust_env", trust_env)
    return build_llm_client(
        provider=str(provider or ""),
        model=str(model or ""),
        base_url=str(base_url or ""),
        api_key_env=str(api_key_env or ""),
        temperature=float(temperature if temperature is not None else 0.0),
        max_tokens=int(max_tokens if max_tokens is not None else 0),
        timeout=int(timeout if timeout is not None else 0),
        max_retries=int(max_retries if max_retries is not None else 0),
        trust_env=bool(True if trust_env is None else trust_env),
        openai_client_cls=OpenAICompatibleQualityControlLLM,
        ollama_client_cls=OllamaQualityControlLLM,
        mock_client_cls=MockQualityControlLLM,
        run_label="Quality-control judgment",
        missing_dependency_label="quality control",
    )
