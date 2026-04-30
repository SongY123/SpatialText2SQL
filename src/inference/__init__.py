"""模型加载与推理模块。"""

from .base import BaseModelLoader

try:
    from .model_inference import ModelInference, ModelLoaderFactory, build_model_run_name
except ModuleNotFoundError:  # pragma: no cover - 允许按需安装推理依赖
    ModelInference = None  # type: ignore[assignment]
    ModelLoaderFactory = None  # type: ignore[assignment]
    build_model_run_name = None  # type: ignore[assignment]

try:
    from .loaders import QwenModelLoader, VllmOpenAILoader
except ModuleNotFoundError:  # pragma: no cover - 允许按需安装推理依赖
    QwenModelLoader = None  # type: ignore[assignment]
    VllmOpenAILoader = None  # type: ignore[assignment]

__all__ = [
    "BaseModelLoader",
    "ModelInference",
    "ModelLoaderFactory",
    "build_model_run_name",
    "QwenModelLoader",
    "VllmOpenAILoader",
]
