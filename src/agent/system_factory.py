from __future__ import annotations

from typing import Optional

from agentscope.formatter import (
    DashScopeChatFormatter,
    OllamaChatFormatter,
    OpenAIChatFormatter,
)
from agentscope.model import DashScopeChatModel, OllamaChatModel, OpenAIChatModel

from agent.spatial_multi_agent_system import SpatialText2SQLMultiAgentSystem
from agent.tools import SpatialText2SQLToolRegistry


def build_spatial_text2sql_system(
    model,
    formatter,
    jdbc_url: Optional[str],
    config_path: str = "src/web/resources/config.yaml",
    max_rounds: int = 3,
) -> SpatialText2SQLMultiAgentSystem:
    registry = SpatialText2SQLToolRegistry.from_agent_config(
        config_path=config_path,
        jdbc_url=jdbc_url,
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
    config_path: str = "src/web/resources/config.yaml",
    api_key: Optional[str] = None,
    max_rounds: int = 3,
) -> SpatialText2SQLMultiAgentSystem:
    model = OpenAIChatModel(model_name=model_name, api_key=api_key, stream=True)
    formatter = OpenAIChatFormatter()
    return build_spatial_text2sql_system(
        model=model,
        formatter=formatter,
        jdbc_url=jdbc_url,
        config_path=config_path,
        max_rounds=max_rounds,
    )


def build_dashscope_system(
    model_name: str,
    api_key: str,
    jdbc_url: Optional[str],
    config_path: str = "src/web/resources/config.yaml",
    max_rounds: int = 3,
) -> SpatialText2SQLMultiAgentSystem:
    model = DashScopeChatModel(model_name=model_name, api_key=api_key, stream=True)
    formatter = DashScopeChatFormatter()
    return build_spatial_text2sql_system(
        model=model,
        formatter=formatter,
        jdbc_url=jdbc_url,
        config_path=config_path,
        max_rounds=max_rounds,
    )


def build_ollama_system(
    model_name: str,
    jdbc_url: Optional[str],
    config_path: str = "src/web/resources/config.yaml",
    base_url: Optional[str] = None,
    max_rounds: int = 3,
) -> SpatialText2SQLMultiAgentSystem:
    model_kwargs = {"base_url": base_url} if base_url else {}
    model = OllamaChatModel(model_name=model_name, **model_kwargs)
    formatter = OllamaChatFormatter()
    return build_spatial_text2sql_system(
        model=model,
        formatter=formatter,
        jdbc_url=jdbc_url,
        config_path=config_path,
        max_rounds=max_rounds,
    )
