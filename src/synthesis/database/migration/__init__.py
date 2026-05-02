"""Migration utilities for synthesized spatial databases."""

from .config import DEFAULT_INPUT_PATH, DEFAULT_MIGRATE_CONFIG_PATH, MigrationRuntimeConfig, load_migration_config
from .core import (
    ColumnMigrationSpec,
    PostGISConnectionSettings,
    PostGISSynthesizedDatabaseMigrator,
    build_feature_row,
    canonical_type_to_postgres_type,
    load_geojson_features,
    normalize_postgres_identifier,
    parse_srid,
    prepare_column_specs,
)

__all__ = [
    "DEFAULT_INPUT_PATH",
    "DEFAULT_MIGRATE_CONFIG_PATH",
    "MigrationRuntimeConfig",
    "ColumnMigrationSpec",
    "PostGISConnectionSettings",
    "PostGISSynthesizedDatabaseMigrator",
    "build_feature_row",
    "canonical_type_to_postgres_type",
    "load_geojson_features",
    "load_migration_config",
    "normalize_postgres_identifier",
    "parse_srid",
    "prepare_column_specs",
]
