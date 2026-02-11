from __future__ import annotations

from typing import Optional

from agentscope.formatter import (
    DashScopeChatFormatter,
    OllamaChatFormatter,
    OpenAIChatFormatter,
)
from agentscope.model import DashScopeChatModel, OllamaChatModel, OpenAIChatModel

from src.agent.spatial_multi_agent_system import SpatialText2SQLMultiAgentSystem
from src.agent.tools import SpatialText2SQLToolRegistry


def build_spatial_text2sql_system(
    model,
    formatter,
    jdbc_url: Optional[str],
    preprocess_config_path: str = "config/preprocess.yml",
    google_api_key: Optional[str] = None,
    google_cse_id: Optional[str] = None,
    max_rounds: int = 3,
) -> SpatialText2SQLMultiAgentSystem:
    registry = SpatialText2SQLToolRegistry.from_preprocess_config(
        preprocess_config_path=preprocess_config_path,
        jdbc_url=jdbc_url,
        google_api_key=google_api_key,
        google_cse_id=google_cse_id,
    )
    return SpatialText2SQLMultiAgentSystem(
        model=model,
        formatter=formatter,
        tool_registry=registry,
        max_rounds=max_rounds,
    )


def build_openai_system(
    model_name: str,
    jdbc_url: Optional[str],
    preprocess_config_path: str = "config/preprocess.yml",
    api_key: Optional[str] = None,
    google_api_key: Optional[str] = None,
    google_cse_id: Optional[str] = None,
    max_rounds: int = 3,
) -> SpatialText2SQLMultiAgentSystem:
    model = OpenAIChatModel(model_name=model_name, api_key=api_key, stream=True)
    formatter = OpenAIChatFormatter()
    return build_spatial_text2sql_system(
        model=model,
        formatter=formatter,
        jdbc_url=jdbc_url,
        preprocess_config_path=preprocess_config_path,
        google_api_key=google_api_key,
        google_cse_id=google_cse_id,
        max_rounds=max_rounds,
    )


def build_dashscope_system(
    model_name: str,
    api_key: str,
    jdbc_url: Optional[str],
    preprocess_config_path: str = "config/preprocess.yml",
    google_api_key: Optional[str] = None,
    google_cse_id: Optional[str] = None,
    max_rounds: int = 3,
) -> SpatialText2SQLMultiAgentSystem:
    model = DashScopeChatModel(model_name=model_name, api_key=api_key, stream=True)
    formatter = DashScopeChatFormatter()
    return build_spatial_text2sql_system(
        model=model,
        formatter=formatter,
        jdbc_url=jdbc_url,
        preprocess_config_path=preprocess_config_path,
        google_api_key=google_api_key,
        google_cse_id=google_cse_id,
        max_rounds=max_rounds,
    )


def build_ollama_system(
    model_name: str,
    jdbc_url: Optional[str],
    preprocess_config_path: str = "config/preprocess.yml",
    base_url: Optional[str] = None,
    google_api_key: Optional[str] = None,
    google_cse_id: Optional[str] = None,
    max_rounds: int = 3,
) -> SpatialText2SQLMultiAgentSystem:
    model_kwargs = {"base_url": base_url} if base_url else {}
    model = OllamaChatModel(model_name=model_name, **model_kwargs)
    formatter = OllamaChatFormatter()
    return build_spatial_text2sql_system(
        model=model,
        formatter=formatter,
        jdbc_url=jdbc_url,
        preprocess_config_path=preprocess_config_path,
        google_api_key=google_api_key,
        google_cse_id=google_cse_id,
        max_rounds=max_rounds,
    )
