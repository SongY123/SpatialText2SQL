#!/usr/bin/env python3
"""FloodSQL parquet -> PostGIS migration entrypoint."""
from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

import yaml


REPO_ROOT = Path(__file__).resolve().parents[3]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

os.chdir(REPO_ROOT)

from src.sql.floodsql_migration import run_floodsql_migration


def _load_yaml(path: Path) -> dict:
    with open(path, "r", encoding="utf-8") as f:
        data = yaml.safe_load(f)
    return data if isinstance(data, dict) else {}


def _resolve_benchmark_root(raw_path: str | Path) -> Path:
    candidate = Path(raw_path).expanduser()
    candidates = []
    if candidate.is_absolute():
        candidates.append(candidate.resolve())
    else:
        candidates.append((REPO_ROOT / candidate).resolve())
        candidates.append(candidate.resolve())
    candidates.append((REPO_ROOT / "FloodSQL-Bench").resolve())
    candidates.append((REPO_ROOT.parent / "FloodSQL-Bench").resolve())

    deduped = []
    seen = set()
    for item in candidates:
        key = str(item)
        if key in seen:
            continue
        seen.add(key)
        deduped.append(item)

    for item in deduped:
        if (item / "data" / "metadata_parquet.json").exists():
            return item
        if item.name == "data" and (item / "metadata_parquet.json").exists():
            return item.parent
    return deduped[0]


def _resolve_data_paths(dataset_config_path: Path) -> tuple[Path, Path]:
    dataset_cfg = _load_yaml(dataset_config_path)
    floodsql_cfg = dataset_cfg.get("datasets", {}).get("floodsql_pg", {})
    configured_root = floodsql_cfg.get("data_path", "../FloodSQL-Bench")
    benchmark_root = _resolve_benchmark_root(configured_root)
    data_root = benchmark_root / "data"
    metadata_path = data_root / "metadata_parquet.json"
    return data_root, metadata_path


def _resolve_db_config(db_config_path: Path) -> dict:
    db_cfg = _load_yaml(db_config_path)
    return db_cfg.get("databases", {}).get("floodsql", db_cfg.get("database", {}))


def main() -> int:
    parser = argparse.ArgumentParser(description="FloodSQL parquet -> PostGIS migration")
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
    parser.add_argument("--data-root", default=None, help="FloodSQL parquet data directory")
    parser.add_argument("--metadata", default=None, help="Path to metadata_parquet.json")
    parser.add_argument("--report", default=str(REPO_ROOT / "scripts" / "benchmark" / "floodsql" / "migration_report.json"))
    parser.add_argument("--checkpoint", default=None, help="Checkpoint JSON path")
    parser.add_argument("--host", default=None)
    parser.add_argument("--port", type=int, default=None)
    parser.add_argument("--database", default=None)
    parser.add_argument("--maintenance-db", default="postgres")
    parser.add_argument("--user", default=None)
    parser.add_argument("--password", default=None)
    parser.add_argument("--batch-size", type=int, default=5000)
    parser.add_argument("--resume", action="store_true", help="Resume from checkpoint")
    parser.add_argument(
        "--validate-only",
        action="store_true",
        help="Only validate row counts and geometry validity without reimporting",
    )
    parser.add_argument(
        "--truncate",
        action="store_true",
        help="Deprecated no-op; fresh runs already recreate target tables",
    )
    args = parser.parse_args()

    dataset_config_path = Path(args.dataset_config).expanduser().resolve()
    db_config_path = Path(args.db_config).expanduser().resolve()
    default_data_root, default_metadata = _resolve_data_paths(dataset_config_path)
    db_defaults = _resolve_db_config(db_config_path)

    data_root = Path(args.data_root).expanduser().resolve() if args.data_root else default_data_root
    metadata_path = Path(args.metadata).expanduser().resolve() if args.metadata else default_metadata
    host = args.host or db_defaults.get("host", "127.0.0.1")
    port = args.port or int(db_defaults.get("port", 5432))
    database = args.database or db_defaults.get("database", "floodsql")
    user = args.user or db_defaults.get("user", "postgres")
    password = args.password or db_defaults.get("password", "")

    if args.truncate:
        print("Note: `--truncate` is deprecated; fresh runs already recreate target tables unless `--resume` is enabled.", flush=True)

    report = run_floodsql_migration(
        data_root=data_root,
        metadata_path=metadata_path,
        report_path=args.report,
        checkpoint_path=args.checkpoint,
        host=host,
        port=port,
        database=database,
        maintenance_db=args.maintenance_db,
        user=user,
        password=password,
        batch_size=args.batch_size,
        resume=args.resume,
        validate_only=args.validate_only,
    )
    summary = report.get("summary", {})
    print(f"FloodSQL migration complete: {Path(args.report).expanduser().resolve()}")
    print(
        "Summary: "
        f"total={summary.get('tables_total', len(report.get('tables', {})))} "
        f"imported={summary.get('tables_imported', 0)} "
        f"validated={summary.get('tables_validated', 0)} "
        f"checkpoint_skipped={summary.get('tables_skipped_from_checkpoint', 0)}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
