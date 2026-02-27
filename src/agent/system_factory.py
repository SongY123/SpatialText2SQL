from __future__ import annotations

import inspect
from typing import Any, Dict, Optional

from agentscope.formatter import (
    DashScopeChatFormatter,
    OllamaChatFormatter,
    OpenAIChatFormatter,
)
from agentscope.model import DashScopeChatModel, OllamaChatModel, OpenAIChatModel

from agent.spatial_multi_agent_system import SpatialText2SQLMultiAgentSystem
from agent.tools import SpatialText2SQLToolRegistry


def _normalize_model_kwargs(model_kwargs: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    if not isinstance(model_kwargs, dict):
        return {}
    return dict(model_kwargs)


def _drop_empty_kwargs(kwargs: Dict[str, Any]) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    for k, v in dict(kwargs).items():
        if v is None:
            continue
        if isinstance(v, str) and v.strip() == "":
            continue
        out[k] = v
    return out


def _build_model_instance(model_cls, kwargs: Dict[str, Any]):
    clean_kwargs = _drop_empty_kwargs(kwargs)
    try:
        return model_cls(**clean_kwargs)
    except TypeError:
        # Backward/forward compatibility: drop unsupported kwargs based on signature.
        sig = inspect.signature(model_cls.__init__)
        supported = {name for name in sig.parameters.keys() if name != "self"}
        filtered_kwargs = {k: v for k, v in clean_kwargs.items() if k in supported}
        return model_cls(**filtered_kwargs)


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
    api_base: Optional[str] = None,
    stream: bool = True,
    model_kwargs: Optional[Dict[str, Any]] = None,
    max_rounds: int = 3,
) -> SpatialText2SQLMultiAgentSystem:
    extra_kwargs = _normalize_model_kwargs(model_kwargs)
    if api_base and "base_url" not in extra_kwargs:
        extra_kwargs["base_url"] = api_base
    model = _build_model_instance(
        OpenAIChatModel,
        {
            "model_name": model_name,
            "api_key": api_key,
            "stream": bool(stream),
            **extra_kwargs,
        },
    )
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
    base_url: Optional[str] = None,
    stream: bool = True,
    model_kwargs: Optional[Dict[str, Any]] = None,
    max_rounds: int = 3,
) -> SpatialText2SQLMultiAgentSystem:
    extra_kwargs = _normalize_model_kwargs(model_kwargs)
    if base_url and "base_url" not in extra_kwargs:
        extra_kwargs["base_url"] = base_url
    model = _build_model_instance(
        DashScopeChatModel,
        {
            "model_name": model_name,
            "api_key": api_key,
            "stream": bool(stream),
            **extra_kwargs,
        },
    )
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
    model_kwargs: Optional[Dict[str, Any]] = None,
    max_rounds: int = 3,
) -> SpatialText2SQLMultiAgentSystem:
    extra_kwargs = _normalize_model_kwargs(model_kwargs)
    if base_url and "base_url" not in extra_kwargs:
        extra_kwargs["base_url"] = base_url
    model = _build_model_instance(
        OllamaChatModel,
        {
            "model_name": model_name,
            **extra_kwargs,
        },
    )
    formatter = OllamaChatFormatter()
    return build_spatial_text2sql_system(
        model=model,
        formatter=formatter,
        jdbc_url=jdbc_url,
        config_path=config_path,
        max_rounds=max_rounds,
    )


def build_gemini_system(
    model_name: str,
    jdbc_url: Optional[str],
    config_path: str = "src/web/resources/config.yaml",
    api_key: Optional[str] = None,
    api_base: Optional[str] = None,
    stream: bool = True,
    model_kwargs: Optional[Dict[str, Any]] = None,
    max_rounds: int = 3,
) -> SpatialText2SQLMultiAgentSystem:
    try:
        from agentscope.formatter import GeminiChatFormatter
        from agentscope.model import GeminiChatModel
    except Exception as exc:  # pragma: no cover - runtime dependency branch
        raise RuntimeError(
            "Gemini client is unavailable in current agentscope environment."
        ) from exc

    extra_kwargs = _normalize_model_kwargs(model_kwargs)
    if api_base and "base_url" not in extra_kwargs:
        extra_kwargs["base_url"] = api_base

    model = _build_model_instance(
        GeminiChatModel,
        {
            "model_name": model_name,
            "api_key": api_key,
            "stream": bool(stream),
            **extra_kwargs,
        },
    )
    formatter = GeminiChatFormatter()
    return build_spatial_text2sql_system(
        model=model,
        formatter=formatter,
        jdbc_url=jdbc_url,
        config_path=config_path,
        max_rounds=max_rounds,
    )
