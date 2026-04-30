"""SQL 与数据库相关工具。"""

try:
    from .floodsql_migration import run_floodsql_migration
except ModuleNotFoundError:  # pragma: no cover - 允许在轻量环境按需导入
    run_floodsql_migration = None  # type: ignore[assignment]
try:
    from .schema_extractor import SchemaExtractor
except ModuleNotFoundError:  # pragma: no cover - 允许在轻量环境按需导入
    SchemaExtractor = None  # type: ignore[assignment]
from .sql_dialect_adapter import (
    add_table_prefix,
    classify_spatialsql_failure,
    convert_batch_and_collect_unconverted,
    convert_duckdb_to_postgis,
    convert_spatialite_to_postgis,
)

__all__ = [
    "run_floodsql_migration",
    "SchemaExtractor",
    "add_table_prefix",
    "classify_spatialsql_failure",
    "convert_batch_and_collect_unconverted",
    "convert_duckdb_to_postgis",
    "convert_spatialite_to_postgis",
]
