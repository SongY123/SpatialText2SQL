"""数据集加载与预处理模块。"""

from .base import BaseDataLoader

try:
    from .processing import DataLoaderFactory, DataPreprocessor
except ModuleNotFoundError:  # pragma: no cover - 允许按需安装数据处理依赖
    DataLoaderFactory = None  # type: ignore[assignment]
    DataPreprocessor = None  # type: ignore[assignment]

try:
    from .loaders import FloodSQLLoader, SpatialQALoader, SpatialSQLLoader
except ModuleNotFoundError:  # pragma: no cover - 允许按需安装数据处理依赖
    FloodSQLLoader = None  # type: ignore[assignment]
    SpatialQALoader = None  # type: ignore[assignment]
    SpatialSQLLoader = None  # type: ignore[assignment]

__all__ = [
    "BaseDataLoader",
    "DataLoaderFactory",
    "DataPreprocessor",
    "FloodSQLLoader",
    "SpatialQALoader",
    "SpatialSQLLoader",
]
