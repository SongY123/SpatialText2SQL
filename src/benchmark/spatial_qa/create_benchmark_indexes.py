#!/usr/bin/env python3
"""Create or inspect Spatial QA benchmark indexes on PostgreSQL."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import yaml

REPO_ROOT = Path(__file__).resolve().parents[3]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from src.sql.spatial_qa_benchmark_setup import (
    apply_spatial_qa_benchmark_setup,
    inspect_spatial_qa_benchmark_setup,
)


def _load_db_config(config_path: Path, db_key: str) -> dict:
    with config_path.open("r", encoding="utf-8") as f:
        config = yaml.safe_load(f)

    databases = config.get("databases", {})
    if db_key in databases:
        return databases[db_key]
    if db_key == "default" and "database" in config:
        return config["database"]
    raise SystemExit(f"Database configuration key was not found: {db_key}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Spatial QA benchmark index setup")
    parser.add_argument(
        "--config",
        default=str(REPO_ROOT / "config" / "db_config.yaml"),
        help="Path to db_config.yaml",
    )
    parser.add_argument(
        "--db-key",
        default="postgres",
        help="Database key in db_config.yaml (default: postgres)",
    )
    parser.add_argument(
        "--check-only",
        action="store_true",
        help="Only inspect benchmark index status without creating indexes",
    )
    parser.add_argument(
        "--no-concurrently",
        action="store_true",
        help="Use CREATE INDEX instead of CREATE INDEX CONCURRENTLY",
    )
    parser.add_argument(
        "--skip-analyze",
        action="store_true",
        help="Skip ANALYZE after index creation",
    )
    args = parser.parse_args()

    db_config = _load_db_config(Path(args.config), args.db_key)
    if args.check_only:
        payload = inspect_spatial_qa_benchmark_setup(db_config)
    else:
        payload = apply_spatial_qa_benchmark_setup(
            db_config,
            concurrently=not args.no_concurrently,
            analyze=not args.skip_analyze,
        )

    print(json.dumps(payload, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
