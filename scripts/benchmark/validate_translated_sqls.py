#!/usr/bin/env python3
"""Validate normalized benchmark SQL on PostgreSQL, plus SpatialSQL source-vs-target consistency."""

from __future__ import annotations

import argparse
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
import json
import os
import sqlite3
import sys
import threading
import time
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any, Dict, Iterable, Optional, Tuple

import psycopg2
import yaml

try:
    import duckdb  # type: ignore
except ModuleNotFoundError:  # pragma: no cover - optional runtime dependency
    duckdb = None


REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

os.chdir(REPO_ROOT)

from src.datasets.db_routing import (  # noqa: E402
    apply_search_path,
    extract_embedded_db_config,
    resolve_database_key,
    resolve_db_settings,
    resolve_schema_name,
)
from src.datasets.loaders.canonical_json_loader import CanonicalJSONLoader  # noqa: E402
from src.datasets.loaders.floodsql_loader import _resolve_benchmark_root  # noqa: E402
from src.datasets.names import canonicalize_dataset_name, canonicalize_dataset_names  # noqa: E402
DEFAULT_DATASETS = ["spatialqueryqa", "spatialsql", "floodsql"]
SPATIALITE_EXTENSION_CANDIDATES = [
    "mod_spatialite",
    "mod_spatialite.so",
    "mod_spatialite.dylib",
    "libspatialite",
    "libspatialite.so",
    "libspatialite.dylib",
    "/opt/homebrew/lib/mod_spatialite.dylib",
    "/opt/homebrew/lib/libspatialite.dylib",
    "/usr/local/lib/mod_spatialite.dylib",
    "/usr/local/lib/libspatialite.dylib",
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Execute translated SQL from normalized benchmark inputs on PostgreSQL/PostGIS.",
    )
    parser.add_argument(
        "--config",
        default=str(REPO_ROOT / "config" / "dataset_config.yaml"),
        help="Path to dataset_config.yaml",
    )
    parser.add_argument(
        "--datasets",
        nargs="+",
        default=None,
        help="Datasets to validate: spatialqueryqa spatialsql floodsql or all",
    )
    parser.add_argument(
        "--limit-per-dataset",
        type=int,
        default=0,
        help="Only validate the first N samples of each dataset; 0 means all samples",
    )
    parser.add_argument(
        "--timeout-ms",
        type=int,
        default=60000,
        help="Per-statement timeout in milliseconds",
    )
    parser.add_argument(
        "--connect-timeout",
        type=int,
        default=10,
        help="PostgreSQL connect timeout in seconds",
    )
    parser.add_argument(
        "--fail-fast",
        action="store_true",
        help="Stop immediately on the first execution failure",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=8,
        help="Number of validation worker threads; defaults to 8",
    )
    parser.add_argument(
        "--report",
        default=str(REPO_ROOT / "scripts" / "benchmark" / "translated_sql_validation_report.json"),
        help="JSON report output path",
    )
    return parser.parse_args()


def load_yaml(path: Path) -> Dict[str, Any]:
    with open(path, "r", encoding="utf-8") as handle:
        payload = yaml.safe_load(handle) or {}
    return payload if isinstance(payload, dict) else {}


def resolve_dataset_names(dataset_config: Dict[str, Any], requested: list[str] | None) -> list[str]:
    available = dataset_config.get("datasets", {})
    if not requested:
        return [name for name in DEFAULT_DATASETS if name in available]

    normalized = canonicalize_dataset_names(requested)
    if "all" in normalized:
        return [name for name in DEFAULT_DATASETS if name in available]
    return [name for name in normalized if name in available]


def load_dataset_items(dataset_config: Dict[str, Any], dataset_name: str) -> list[dict]:
    dataset_info = (dataset_config.get("datasets") or {}).get(dataset_name) or {}
    data_path = str(dataset_info.get("data_path") or "").strip()
    if not data_path:
        return []

    target_path = Path(data_path)
    if not target_path.is_absolute():
        target_path = REPO_ROOT / target_path
    if not target_path.exists():
        return []

    loader = CanonicalJSONLoader(dataset_info)
    return loader.extract_questions_and_sqls(loader.load_raw_data(str(target_path)))


def normalize_json_value(value: Any) -> Any:
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, (list, tuple)):
        return [normalize_json_value(item) for item in value]
    return value


def fetch_normalized_rows(cursor) -> tuple[list[tuple[Any, ...]], list[Any]]:
    if cursor.description is None:
        return [], []

    rows = normalize_result_rows(cursor.fetchall())
    full_rows = [normalize_json_value(list(row)) for row in rows]
    return rows, full_rows


class ConnectionManager:
    def __init__(self, *, timeout_ms: int, connect_timeout: int) -> None:
        self.timeout_ms = timeout_ms
        self.connect_timeout = connect_timeout
        self._connections: dict[str, Any] = {}

    def get(self, db_key: str, db_config: Dict[str, Any]):
        connection = self._connections.get(db_key)
        if connection is not None and not getattr(connection, "closed", False):
            return connection

        connection = psycopg2.connect(
            host=db_config["host"],
            port=db_config["port"],
            database=db_config["database"],
            user=db_config["user"],
            password=db_config["password"],
            connect_timeout=self.connect_timeout,
            options=f"-c statement_timeout={self.timeout_ms}",
        )
        connection.autocommit = True
        cursor = connection.cursor()
        try:
            apply_search_path(cursor, db_config)
        finally:
            cursor.close()
        self._connections[db_key] = connection
        return connection

    def close_all(self) -> None:
        for connection in self._connections.values():
            try:
                connection.close()
            except Exception:
                pass


class SQLiteConnectionManager:
    def __init__(self, *, timeout_ms: int) -> None:
        self.timeout_ms = timeout_ms
        self._connections: dict[str, sqlite3.Connection] = {}
        self._extension_loaded: dict[str, bool] = {}

    def get(self, sqlite_path: Path) -> tuple[sqlite3.Connection, bool]:
        key = str(sqlite_path.resolve())
        connection = self._connections.get(key)
        if connection is not None:
            return connection, self._extension_loaded.get(key, False)

        connection = sqlite3.connect(key)
        loaded = self._load_spatialite_extension(connection)
        self._connections[key] = connection
        self._extension_loaded[key] = loaded
        return connection, loaded

    def execute(self, sqlite_path: Path, sql_text: str) -> Dict[str, Any]:
        connection, extension_loaded = self.get(sqlite_path)
        cursor = connection.cursor()
        started = time.perf_counter()
        deadline = started + (self.timeout_ms / 1000.0)

        def _progress() -> int:
            return 1 if time.perf_counter() >= deadline else 0

        try:
            connection.set_progress_handler(_progress, 10_000)
            cursor.execute(sql_text)
            rows = cursor.fetchall() if cursor.description is not None else []
            normalized_rows = normalize_result_rows(rows)
            return {
                "status": "ok",
                "rows": normalized_rows,
                "row_count": len(normalized_rows),
                "result_preview": [normalize_json_value(list(row)) for row in normalized_rows],
                "extension_loaded": extension_loaded,
            }
        except Exception as exc:
            message = str(exc)
            status = "timeout" if _is_sqlite_timeout_error(message, deadline) else "error"
            return {
                "status": status,
                "error": message,
                "extension_loaded": extension_loaded,
            }
        finally:
            connection.set_progress_handler(None, 0)
            cursor.close()

    def close_all(self) -> None:
        for connection in self._connections.values():
            try:
                connection.close()
            except Exception:
                pass

    @staticmethod
    def _load_spatialite_extension(connection: sqlite3.Connection) -> bool:
        try:
            connection.enable_load_extension(True)
        except Exception:
            return False

        for candidate in SPATIALITE_EXTENSION_CANDIDATES:
            try:
                connection.load_extension(candidate)
                return True
            except Exception:
                continue
        return False


class DuckDBFloodExecutor:
    def __init__(self, benchmark_root: Path, *, timeout_ms: int) -> None:
        if duckdb is None:
            raise RuntimeError("duckdb is not installed")

        self.timeout_ms = timeout_ms
        self.benchmark_root = benchmark_root
        self.data_root = (benchmark_root / "data").resolve()
        self.metadata_path = (self.data_root / "metadata_parquet.json").resolve()
        self.metadata = load_json_file(self.metadata_path)
        self.conn = duckdb.connect(database=":memory:")
        self.spatial_extension_loaded = self._load_spatial_extension()
        self._register_tables()

    def _load_spatial_extension(self) -> bool:
        last_error: Optional[Exception] = None
        for statement in ("LOAD spatial", "INSTALL spatial", "LOAD spatial"):
            try:
                self.conn.execute(statement)
            except Exception as exc:  # pragma: no cover - runtime dependency
                last_error = exc
                continue
            else:
                if statement == "LOAD spatial":
                    return True
        if last_error is not None:
            return False
        return False

    def _register_tables(self) -> None:
        for table_name, info in sorted(self.metadata.items()):
            if not isinstance(info, dict) or str(table_name).startswith("_"):
                continue
            parquet_name = str(info.get("file") or "").strip()
            if not parquet_name:
                continue
            parquet_path = (self.data_root / parquet_name).resolve()
            parquet_sql = str(parquet_path).replace("\\", "\\\\").replace("'", "''")
            self.conn.execute(
                f"CREATE OR REPLACE VIEW {table_name} AS SELECT * FROM read_parquet('{parquet_sql}')"
            )

    def execute(self, sql_text: str) -> Dict[str, Any]:
        started = time.perf_counter()
        deadline = started + (self.timeout_ms / 1000.0)
        timer = threading.Timer(self.timeout_ms / 1000.0, self.conn.interrupt)
        timer.daemon = True

        try:
            timer.start()
            self.conn.execute(sql_text)
            rows = self.conn.fetchall()
            return {
                "status": "ok",
                "rows": rows,
                "row_count": len(rows),
                "result_preview": [normalize_json_value(list(row)) for row in rows],
                "spatial_extension_loaded": self.spatial_extension_loaded,
            }
        except Exception as exc:
            message = str(exc)
            status = "timeout" if _is_duckdb_timeout_error(message, deadline) else "error"
            return {
                "status": status,
                "error": message,
                "spatial_extension_loaded": self.spatial_extension_loaded,
            }
        finally:
            timer.cancel()

    def close(self) -> None:
        try:
            self.conn.close()
        except Exception:
            pass


def validate_dataset(
    *,
    dataset_name: str,
    items: list[dict],
    dataset_config: Dict[str, Any],
    embedded_db_config: Dict[str, Any],
    timeout_ms: int,
    connect_timeout: int,
    fail_fast: bool,
    workers: int,
    report_path: Path,
    run_generated_at: str,
    config_path: Path,
) -> Dict[str, Any]:
    summary = {
        "dataset": dataset_name,
        "total": len(items),
        "success": 0,
        "failed": 0,
        "consistency_failed": 0,
        "details": [],
    }
    worker_count = max(1, int(workers))
    thread_state = threading.local()
    resource_lock = threading.Lock()
    pg_managers: list[ConnectionManager] = []
    sqlite_managers: list[SQLiteConnectionManager] = []
    duckdb_executors: list[DuckDBFloodExecutor] = []
    floodsql_benchmark_root = None

    def get_pg_manager() -> ConnectionManager:
        manager = getattr(thread_state, "pg_manager", None)
        if manager is None:
            manager = ConnectionManager(timeout_ms=timeout_ms, connect_timeout=connect_timeout)
            thread_state.pg_manager = manager
            with resource_lock:
                pg_managers.append(manager)
        return manager

    def get_sqlite_manager() -> SQLiteConnectionManager:
        manager = getattr(thread_state, "sqlite_manager", None)
        if manager is None:
            manager = SQLiteConnectionManager(timeout_ms=timeout_ms)
            thread_state.sqlite_manager = manager
            with resource_lock:
                sqlite_managers.append(manager)
        return manager

    def get_duckdb_executor() -> DuckDBFloodExecutor:
        executor = getattr(thread_state, "duckdb_executor", None)
        if executor is None:
            if floodsql_benchmark_root is None:
                raise RuntimeError("FloodSQL benchmark root is unavailable")
            executor = DuckDBFloodExecutor(
                floodsql_benchmark_root,
                timeout_ms=timeout_ms,
            )
            thread_state.duckdb_executor = executor
            with resource_lock:
                duckdb_executors.append(executor)
        return executor

    def validate_one(index: int, item: dict) -> tuple[dict[str, Any], bool]:
        metadata = item.get("metadata", {}) if isinstance(item.get("metadata"), dict) else {}
        db_key = (
            resolve_database_key(
                dataset_config,
                dataset_name,
                metadata,
                allow_fallback_mapping=True,
            )
            or dataset_name
        )
        db_settings = resolve_db_settings(
            embedded_db_config,
            dataset_config,
            dataset_name,
            metadata,
            allow_fallback_mapping=True,
        )
        if not db_settings:
            raise RuntimeError(f"Missing database routing for dataset={dataset_name}, sample={item.get('id')}")

        sql_text = str(item.get("sql") or item.get("gold_sql") or "").strip()
        detail = {
            "id": item.get("id"),
            "level": item.get("level") or metadata.get("level"),
            "domain": metadata.get("domain"),
            "family": metadata.get("family"),
            "database_key": db_key,
            "database": db_settings.get("database"),
            "schema": resolve_schema_name(db_settings),
            "status": "unknown",
            "row_count": None,
            "elapsed_ms": None,
            "sql": sql_text,
            "result_preview": [],
            "error": None,
            "source_sql": None,
            "source_extension_loaded": None,
            "source_spatial_extension_loaded": None,
            "source_row_count": None,
            "source_result_preview": [],
            "consistency": None,
            "consistency_details": None,
        }
        if dataset_name == "floodsql":
            detail["source_sql"] = str(
                item.get("source_sql") or item.get("original_sql") or item.get("gold_sql") or ""
            ).strip()

        start = time.perf_counter()
        consistency_failed = False
        try:
            conn = get_pg_manager().get(db_key, db_settings)
            cursor = conn.cursor()
            try:
                cursor.execute(sql_text)
                target_rows, preview = fetch_normalized_rows(cursor)
            finally:
                cursor.close()
            detail["status"] = "ok"
            detail["row_count"] = len(target_rows)
            detail["result_preview"] = preview

            if dataset_name == "spatialsql":
                source_sql = select_primary_spatialsql_source_sql(
                    str(item.get("original_sql") or item.get("source_sql") or "").strip()
                )
                detail["source_sql"] = source_sql
                sqlite_path = resolve_spatialsql_sqlite_path(dataset_config, item, metadata)
                if not source_sql:
                    raise RuntimeError("Missing SpatialSQL original_sql")
                if sqlite_path is None or not sqlite_path.exists():
                    raise RuntimeError(
                        f"Missing SpatialSQL source sqlite path for sample={item.get('id')} domain={metadata.get('domain') or item.get('level')}"
                    )

                source_exec = get_sqlite_manager().execute(sqlite_path, source_sql)
                detail["source_extension_loaded"] = source_exec.get("extension_loaded")
                if source_exec["status"] != "ok":
                    detail["status"] = "failed"
                    detail["error"] = f"source_sql_{source_exec['status']}: {source_exec.get('error')}"
                    return detail, False

                detail["source_row_count"] = source_exec["row_count"]
                detail["source_result_preview"] = source_exec["result_preview"]
                consistency, consistency_details = compare_sql_results(
                    source_exec["rows"],
                    target_rows,
                )
                detail["consistency"] = consistency
                detail["consistency_details"] = consistency_details or None
                if consistency not in {"exact_match", "format_difference", "approximate_match"}:
                    detail["status"] = "failed"
                    detail["error"] = "source_target_result_mismatch"
                    consistency_failed = True
                    return detail, consistency_failed

            if dataset_name == "floodsql":
                source_sql = str(detail.get("source_sql") or "").strip()
                detail["source_sql"] = source_sql
                if not source_sql:
                    raise RuntimeError("Missing FloodSQL source SQL")

                source_exec = get_duckdb_executor().execute(source_sql)
                detail["source_spatial_extension_loaded"] = source_exec.get("spatial_extension_loaded")
                if source_exec["status"] != "ok":
                    detail["status"] = "failed"
                    detail["error"] = f"source_sql_{source_exec['status']}: {source_exec.get('error')}"
                    return detail, False

                detail["source_row_count"] = source_exec["row_count"]
                detail["source_result_preview"] = source_exec["result_preview"]
                consistency, consistency_details = compare_sql_results(
                    source_exec["rows"],
                    target_rows,
                )
                detail["consistency"] = consistency
                detail["consistency_details"] = consistency_details or None
                if consistency not in {"exact_match", "format_difference", "approximate_match"}:
                    detail["status"] = "failed"
                    detail["error"] = "source_target_result_mismatch"
                    consistency_failed = True
                    return detail, consistency_failed
        except Exception as exc:
            detail["status"] = "failed"
            detail["error"] = str(exc)
        finally:
            detail["elapsed_ms"] = round((time.perf_counter() - start) * 1000.0, 2)
        return detail, consistency_failed

    try:
        if dataset_name == "floodsql":
            floodsql_benchmark_root = resolve_floodsql_benchmark_root(dataset_config)
            print(
                f"  preparing FloodSQL DuckDB executors for {worker_count} worker(s)...",
                flush=True,
            )
        pending = {}
        processed = 0
        legacy_notice_printed = False
        with ThreadPoolExecutor(max_workers=worker_count) as executor:
            for index, item in enumerate(items, start=1):
                future = executor.submit(validate_one, index, item)
                pending[future] = index

            while pending:
                done, _ = wait(set(pending.keys()), return_when=FIRST_COMPLETED)
                for future in done:
                    pending.pop(future, None)
                    detail, consistency_failed = future.result()
                    summary["details"].append(detail)
                    processed += 1

                    if detail["status"] == "failed":
                        summary["failed"] += 1
                        if consistency_failed:
                            summary["consistency_failed"] += 1
                        record = {
                            "generated_at": run_generated_at,
                            "config_path": str(config_path),
                            "dataset": dataset_name,
                            "detail": detail,
                        }
                        legacy_path = append_report_entry(report_path, record)
                        if legacy_path is not None and not legacy_notice_printed:
                            print(f"\narchived legacy report to {legacy_path}", flush=True)
                            legacy_notice_printed = True
                        print("X", end="", flush=True)
                        if fail_fast:
                            for pending_future in pending:
                                pending_future.cancel()
                            pending.clear()
                            print()
                            return summary
                    else:
                        summary["success"] += 1
                        print(".", end="", flush=True)

                    if processed % 50 == 0:
                        print(f"  [{dataset_name} {processed}/{len(items)}]")
        print()
        return summary
    finally:
        for manager in pg_managers:
            manager.close_all()
        for manager in sqlite_managers:
            manager.close_all()
        for executor in duckdb_executors:
            executor.close()


def resolve_spatialsql_sqlite_path(
    dataset_config: Dict[str, Any],
    item: Dict[str, Any],
    metadata: Dict[str, Any],
) -> Optional[Path]:
    dataset_info = (dataset_config.get("datasets") or {}).get("spatialsql") or {}
    raw_root = Path(str(dataset_info.get("raw_data_path") or "").strip())
    if not raw_root.is_absolute():
        raw_root = REPO_ROOT / raw_root

    domain = str(metadata.get("domain") or item.get("level") or "").strip()
    dataset_version = str(metadata.get("dataset_version") or "").strip()
    partitions = dataset_info.get("source_partitions") or {}
    for partition in partitions.values():
        if not isinstance(partition, dict):
            continue
        partition_domain = str(partition.get("domain") or partition.get("level") or "").strip()
        if partition_domain != domain:
            continue
        raw_path = str(partition.get("raw_path") or "").strip()
        if raw_path:
            domain_dir = raw_root / raw_path
            return domain_dir / f"{domain}.sqlite"
        if not dataset_version:
            dataset_version = str(partition.get("dataset_version") or "").strip()
            if dataset_version:
                break

    if not domain:
        return None
    version = dataset_version or "dataset2"
    return raw_root / version / domain / f"{domain}.sqlite"


def select_primary_spatialsql_source_sql(sql_text: str) -> str:
    if not sql_text:
        return ""
    parts = [part.strip() for part in sql_text.split("%%%") if part.strip()]
    return parts[0] if parts else sql_text.strip()


def resolve_floodsql_benchmark_root(dataset_config: Dict[str, Any]) -> Path:
    dataset_info = (dataset_config.get("datasets") or {}).get("floodsql") or {}
    raw_data_path = str(dataset_info.get("raw_data_path") or "").strip()
    return _resolve_benchmark_root(raw_data_path or "data/benchmark/FloodSQL-Bench")


def load_json_file(path: Path) -> Dict[str, Any]:
    with open(path, "r", encoding="utf-8") as handle:
        payload = json.load(handle)
    return payload if isinstance(payload, dict) else {}


def normalize_result_rows(rows: Iterable[Any]) -> list[tuple[Any, ...]]:
    normalized: list[tuple[Any, ...]] = []
    for row in rows:
        normalized.append(tuple(_normalize_scalar(value) for value in row))
    return normalized


def compare_sql_results(
    source_rows: Iterable[tuple[Any, ...]],
    target_rows: Iterable[tuple[Any, ...]],
) -> Tuple[str, Dict[str, Any]]:
    normalized_source = normalize_result_rows(source_rows)
    normalized_target = normalize_result_rows(target_rows)
    if normalized_source == normalized_target:
        return "exact_match", {}
    if sorted(normalized_source, key=repr) == sorted(normalized_target, key=repr):
        return "format_difference", {
            "source_count": len(normalized_source),
            "target_count": len(normalized_target),
        }
    approx_match, approx_details = _rows_match_with_numeric_tolerance(
        normalized_source,
        normalized_target,
        abs_tolerance=1e-1,
    )
    if approx_match:
        return "approximate_match", approx_details
    source_counter = _build_row_counter(normalized_source)
    target_counter = _build_row_counter(normalized_target)
    return "semantic_mismatch", {
        "source_count": len(normalized_source),
        "target_count": len(normalized_target),
        "only_in_source": _expand_row_counter_difference(source_counter, target_counter),
        "only_in_target": _expand_row_counter_difference(target_counter, source_counter),
    }


def _build_row_counter(rows: Iterable[tuple[Any, ...]]) -> dict[str, list[list[Any]]]:
    counter: dict[str, list[list[Any]]] = {}
    for row in rows:
        normalized_row = [normalize_json_value(value) for value in row]
        key = json.dumps(
            normalized_row,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            default=str,
        )
        counter.setdefault(key, []).append(normalized_row)
    return counter


def _expand_row_counter_difference(
    left_counter: dict[str, list[list[Any]]],
    right_counter: dict[str, list[list[Any]]],
) -> list[list[Any]]:
    diff_rows: list[list[Any]] = []
    for key in sorted(left_counter):
        remaining = len(left_counter[key]) - len(right_counter.get(key, []))
        if remaining > 0:
            diff_rows.extend(left_counter[key][:remaining])
    return diff_rows


def _rows_match_with_numeric_tolerance(
    source_rows: list[tuple[Any, ...]],
    target_rows: list[tuple[Any, ...]],
    *,
    abs_tolerance: float,
) -> tuple[bool, Dict[str, Any]]:
    if len(source_rows) != len(target_rows):
        return False, {}

    sorted_source = sorted(source_rows, key=_row_sort_key)
    sorted_target = sorted(target_rows, key=_row_sort_key)
    max_abs_diff = 0.0

    for left_row, right_row in zip(sorted_source, sorted_target):
        if len(left_row) != len(right_row):
            return False, {}
        for left_value, right_value in zip(left_row, right_row):
            if _is_numeric_scalar(left_value) and _is_numeric_scalar(right_value):
                diff = abs(float(left_value) - float(right_value))
                if diff > abs_tolerance:
                    return False, {}
                max_abs_diff = max(max_abs_diff, diff)
            elif left_value != right_value:
                return False, {}

    if max_abs_diff == 0.0:
        return False, {}

    return True, {
        "source_count": len(source_rows),
        "target_count": len(target_rows),
        "numeric_abs_tolerance": abs_tolerance,
        "max_numeric_abs_diff": max_abs_diff,
    }


def _row_sort_key(row: tuple[Any, ...]) -> tuple[Any, ...]:
    non_numeric_tokens = tuple("" if _is_numeric_scalar(value) else repr(value) for value in row)
    numeric_tokens = tuple(float(value) if _is_numeric_scalar(value) else 0.0 for value in row)
    return (len(row), non_numeric_tokens, numeric_tokens)


def _normalize_scalar(value: Any) -> Any:
    if isinstance(value, Decimal):
        return round(float(value), 6)
    if isinstance(value, float):
        return round(value, 6)
    if isinstance(value, bytes):
        return value.hex()
    if isinstance(value, str):
        return value.strip()
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    return value


def _is_numeric_scalar(value: Any) -> bool:
    return isinstance(value, (int, float, Decimal)) and not isinstance(value, bool)


def _is_sqlite_timeout_error(message: str, deadline: float) -> bool:
    lowered = message.lower()
    return (
        "interrupted" in lowered
        or "query interrupted" in lowered
        or ("timeout" in lowered and time.perf_counter() >= deadline)
    )


def _is_duckdb_timeout_error(message: str, deadline: float) -> bool:
    lowered = message.lower()
    return (
        "interrupted" in lowered
        or "timeout" in lowered
        or ("cancelled" in lowered and time.perf_counter() >= deadline)
    )


def _archive_legacy_report(path: Path) -> Optional[Path]:
    if not path.exists() or path.stat().st_size == 0:
        return None
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    legacy_path = path.with_name(f"{path.stem}.legacy_{timestamp}{path.suffix}")
    path.rename(legacy_path)
    return legacy_path


def append_report_entry(path: Path, payload: Dict[str, Any]) -> Optional[Path]:
    path.parent.mkdir(parents=True, exist_ok=True)
    entry = json.dumps(payload, ensure_ascii=False, indent=2)

    if not path.exists() or path.stat().st_size == 0:
        with open(path, "w", encoding="utf-8") as handle:
            handle.write("[\n")
            handle.write(entry)
            handle.write("\n]\n")
        return None

    with open(path, "r+b") as handle:
        handle.seek(0, os.SEEK_END)
        pos = handle.tell() - 1
        while pos >= 0:
            handle.seek(pos)
            ch = handle.read(1)
            if ch not in b" \t\r\n":
                break
            pos -= 1

        if pos < 0 or ch != b"]":
            legacy_path = _archive_legacy_report(path)
            with open(path, "w", encoding="utf-8") as rewrite:
                rewrite.write("[\n")
                rewrite.write(entry)
                rewrite.write("\n]\n")
            return legacy_path

        prev = pos - 1
        while prev >= 0:
            handle.seek(prev)
            prev_ch = handle.read(1)
            if prev_ch not in b" \t\r\n":
                break
            prev -= 1

        handle.seek(pos)
        handle.truncate()
        if prev >= 0 and prev_ch == b"[":
            handle.write(f"\n{entry}\n]\n".encode("utf-8"))
        else:
            handle.write(f",\n{entry}\n]\n".encode("utf-8"))
    return None


def main() -> int:
    args = parse_args()
    config_path = Path(args.config).expanduser().resolve()
    report_path = Path(args.report).expanduser().resolve()
    dataset_config = load_yaml(config_path)
    embedded_db_config = extract_embedded_db_config(dataset_config)
    dataset_names = resolve_dataset_names(dataset_config, args.datasets)
    if not dataset_names:
        print("No datasets selected.", file=sys.stderr)
        return 1

    run_generated_at = datetime.now().isoformat(timespec="seconds")
    summary = {
        "datasets": dataset_names,
        "total": 0,
        "success": 0,
        "failed": 0,
    }

    for dataset_name in dataset_names:
        dataset_name = canonicalize_dataset_name(dataset_name)
        items = load_dataset_items(dataset_config, dataset_name)
        if args.limit_per_dataset > 0:
            items = items[: args.limit_per_dataset]

        print(f"\n=== Validating {dataset_name} ({len(items)} samples) ===")
        dataset_report = validate_dataset(
            dataset_name=dataset_name,
            items=items,
            dataset_config=dataset_config,
            embedded_db_config=embedded_db_config,
            timeout_ms=max(1, int(args.timeout_ms)),
            connect_timeout=max(1, int(args.connect_timeout)),
            fail_fast=bool(args.fail_fast),
            workers=max(1, int(args.workers)),
            report_path=report_path,
            run_generated_at=run_generated_at,
            config_path=config_path,
        )
        summary["total"] += dataset_report["total"]
        summary["success"] += dataset_report["success"]
        summary["failed"] += dataset_report["failed"]

        print(
            f"{dataset_name}: total={dataset_report['total']} "
            f"success={dataset_report['success']} failed={dataset_report['failed']} "
            f"consistency_failed={dataset_report.get('consistency_failed', 0)}"
        )

        if args.fail_fast and dataset_report["failed"] > 0:
            break

    print("\n=== Summary ===")
    print(
        f"total={summary['total']} "
        f"success={summary['success']} "
        f"failed={summary['failed']}"
    )
    print(f"report={report_path}")
    return 0 if summary["failed"] == 0 else 2


if __name__ == "__main__":
    raise SystemExit(main())
