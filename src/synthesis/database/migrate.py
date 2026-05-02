"""Backward-compatible wrapper for synthesized database migration."""

from .migration.core import (
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
    "ColumnMigrationSpec",
    "PostGISConnectionSettings",
    "PostGISSynthesizedDatabaseMigrator",
    "build_feature_row",
    "canonical_type_to_postgres_type",
    "load_geojson_features",
    "normalize_postgres_identifier",
    "parse_srid",
    "prepare_column_specs",
]
