"""Relation-aware spatial database synthesis exports."""

from __future__ import annotations

from importlib import import_module
from typing import Any

__all__ = [
    "DEFAULT_EMBEDDING_MODEL",
    "EmbeddingProvider",
    "MockEmbeddingProvider",
    "SentenceTransformerEmbeddingProvider",
    "RelationGraphBuilder",
    "RelationAwareDatabaseSampler",
    "CanonicalSpatialTable",
    "SynthesizedSpatialDatabase",
    "DEFAULT_INPUT_PATH",
    "DEFAULT_MIGRATE_CONFIG_PATH",
    "MigrationRuntimeConfig",
    "PostGISConnectionSettings",
    "PostGISSynthesizedDatabaseMigrator",
    "SpatialDatabaseSynthesizer",
    "build_table_text",
    "build_feature_row",
    "canonical_type_to_postgres_type",
    "load_canonical_tables",
    "load_geojson_features",
    "load_migration_config",
    "load_synthesized_databases",
    "normalize_postgres_identifier",
    "parse_srid",
    "prepare_column_specs",
    "write_synthesized_databases",
]

_EXPORT_MAP = {
    "DEFAULT_EMBEDDING_MODEL": (".embeddings", "DEFAULT_EMBEDDING_MODEL"),
    "EmbeddingProvider": (".embeddings", "EmbeddingProvider"),
    "MockEmbeddingProvider": (".embeddings", "MockEmbeddingProvider"),
    "SentenceTransformerEmbeddingProvider": (".embeddings", "SentenceTransformerEmbeddingProvider"),
    "RelationGraphBuilder": (".graph", "RelationGraphBuilder"),
    "RelationAwareDatabaseSampler": (".sampler", "RelationAwareDatabaseSampler"),
    "CanonicalSpatialTable": (".models", "CanonicalSpatialTable"),
    "SynthesizedSpatialDatabase": (".models", "SynthesizedSpatialDatabase"),
    "DEFAULT_INPUT_PATH": (".migration", "DEFAULT_INPUT_PATH"),
    "DEFAULT_MIGRATE_CONFIG_PATH": (".migration", "DEFAULT_MIGRATE_CONFIG_PATH"),
    "MigrationRuntimeConfig": (".migration", "MigrationRuntimeConfig"),
    "PostGISConnectionSettings": (".migration", "PostGISConnectionSettings"),
    "PostGISSynthesizedDatabaseMigrator": (".migration", "PostGISSynthesizedDatabaseMigrator"),
    "SpatialDatabaseSynthesizer": (".synthesizer", "SpatialDatabaseSynthesizer"),
    "build_table_text": (".text", "build_table_text"),
    "build_feature_row": (".migration", "build_feature_row"),
    "canonical_type_to_postgres_type": (".migration", "canonical_type_to_postgres_type"),
    "load_canonical_tables": (".io", "load_canonical_tables"),
    "load_geojson_features": (".migration", "load_geojson_features"),
    "load_migration_config": (".migration", "load_migration_config"),
    "load_synthesized_databases": (".io", "load_synthesized_databases"),
    "normalize_postgres_identifier": (".migration", "normalize_postgres_identifier"),
    "parse_srid": (".migration", "parse_srid"),
    "prepare_column_specs": (".migration", "prepare_column_specs"),
    "write_synthesized_databases": (".io", "write_synthesized_databases"),
}


def __getattr__(name: str) -> Any:
    if name not in _EXPORT_MAP:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    module_name, attr_name = _EXPORT_MAP[name]
    module = import_module(module_name, __name__)
    return getattr(module, attr_name)
