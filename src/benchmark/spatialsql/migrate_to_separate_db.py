#!/usr/bin/env python3
"""Run the iterative SpatialSQL migration workflow."""
from __future__ import annotations

import argparse
import sys
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[3]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from src.sql.spatialsql_migration_framework import run_iterative_spatialsql_migration


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Run the iterative SpatialSQL SQLite/SpatiaLite -> PostgreSQL migration framework",
    )
    parser.add_argument(
        "--db-config",
        default=str(REPO_ROOT / "config" / "db_config.yaml"),
        help="Path to db_config.yaml",
    )
    parser.add_argument(
        "--dataset-config",
        default=str(REPO_ROOT / "config" / "dataset_config.yaml"),
        help="Path to dataset_config.yaml",
    )
    parser.add_argument(
        "--model-config",
        default=str(REPO_ROOT / "config" / "model_config.yaml"),
        help="Path to model_config.yaml",
    )
    parser.add_argument(
        "--eval-config",
        default=str(REPO_ROOT / "config" / "eval_config.yaml"),
        help="Path to eval_config.yaml",
    )
    parser.add_argument(
        "--report-dir",
        default=str(REPO_ROOT / "scripts" / "benchmark" / "spatialsql"),
        help="Directory for generated reports",
    )
    parser.add_argument(
        "--repair-model",
        default=None,
        help="Optional model name for LLM repair attempts",
    )
    parser.add_argument(
        "--repair-backend",
        default=None,
        help="Optional backend name for LLM repair attempts",
    )
    parser.add_argument(
        "--repair-limit",
        type=int,
        default=50,
        help="Maximum number of LLM repair attempts in one run",
    )
    args = parser.parse_args()

    summary = run_iterative_spatialsql_migration(
        project_root=REPO_ROOT,
        db_config_path=Path(args.db_config),
        dataset_config_path=Path(args.dataset_config),
        model_config_path=Path(args.model_config) if args.model_config else None,
        eval_config_path=Path(args.eval_config) if args.eval_config else None,
        report_dir=Path(args.report_dir),
        repair_model=args.repair_model,
        repair_backend=args.repair_backend,
        repair_limit=max(0, args.repair_limit),
    )

    print("=" * 70)
    print("SpatialSQL iterative migration completed")
    print("=" * 70)
    for key, value in summary.items():
        print(f"{key}: {value}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
