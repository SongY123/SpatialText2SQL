"""Relation-aware spatial database synthesis."""

from .embeddings import (
    DEFAULT_EMBEDDING_MODEL,
    EmbeddingProvider,
    MockEmbeddingProvider,
    SentenceTransformerEmbeddingProvider,
)
from .graph import RelationGraphBuilder
from .io import load_canonical_tables, load_synthesized_databases, write_synthesized_databases
from .migration import (
    DEFAULT_INPUT_PATH,
    DEFAULT_MIGRATE_CONFIG_PATH,
    MigrationRuntimeConfig,
    PostGISConnectionSettings,
    PostGISSynthesizedDatabaseMigrator,
    build_feature_row,
    canonical_type_to_postgres_type,
    load_geojson_features,
    load_migration_config,
    normalize_postgres_identifier,
    parse_srid,
    prepare_column_specs,
)
from .models import CanonicalSpatialTable, SynthesizedSpatialDatabase
from .sampler import RelationAwareDatabaseSampler
from .synthesizer import SpatialDatabaseSynthesizer
from .text import build_table_text

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
