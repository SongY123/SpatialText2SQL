from __future__ import annotations

from pathlib import Path
from typing import Any

from .clustering import (
    TableProfile,
    build_profiles,
    build_scenario_clusters,
    cluster_table_membership,
    load_scenario_clusters,
    profile_to_record,
    run_clustering_pipeline,
    write_csv_catalog,
    write_json,
)
from .database import render_database_blueprints, run_database_blueprint_pipeline
from .text2sql import build_benchmark_samples, build_query_templates


def run_pipeline(raw_dir: Path, artifacts_dir: Path) -> dict[str, Any]:
    """Legacy compatibility wrapper.

    The default project flow is now stage-based:
    1. clustering
    2. database build / ETL
    3. text2sql generation (deferred)

    This wrapper keeps the old import path alive by running only the clustering stage.
    """

    return run_clustering_pipeline(raw_dir, artifacts_dir)


__all__ = [
    "TableProfile",
    "build_benchmark_samples",
    "build_profiles",
    "build_query_templates",
    "build_scenario_clusters",
    "cluster_table_membership",
    "load_scenario_clusters",
    "profile_to_record",
    "render_database_blueprints",
    "run_clustering_pipeline",
    "run_database_blueprint_pipeline",
    "run_pipeline",
    "write_csv_catalog",
    "write_json",
]
