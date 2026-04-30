"""具体数据集加载器实现。"""

try:
    from .spatial_qa_loader import SpatialQALoader
except ModuleNotFoundError:  # pragma: no cover - 允许按需安装 Excel 依赖
    SpatialQALoader = None  # type: ignore[assignment]

from .floodsql_loader import FloodSQLLoader
from .spatial_sql_loader import SpatialSQLLoader

__all__ = ["SpatialQALoader", "SpatialSQLLoader", "FloodSQLLoader"]
