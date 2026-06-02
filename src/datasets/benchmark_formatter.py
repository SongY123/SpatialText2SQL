"""Build unified benchmark JSON inputs with materialized PostgreSQL results."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import re
from typing import Any, Callable, Dict, List, Optional

import openpyxl
import psycopg2
import yaml

from src.datasets.db_routing import apply_search_path, extract_embedded_db_config, resolve_db_settings
from src.datasets.loaders.spatialsql_loader import _parse_qa_txt_block, _split_sql_candidates
from src.sql.sql_dialect_adapter import convert_duckdb_to_postgis, convert_spatialite_to_postgis
from src.utils.execution_results import normalize_result_rows


REPO_ROOT = Path(__file__).resolve().parents[2]


def _load_dataset_config(config_path: Path) -> Dict[str, Any]:
    with open(config_path, "r", encoding="utf-8") as handle:
        return yaml.safe_load(handle) or {}


def _load_eval_config(config_path: Path) -> Dict[str, Any]:
    with open(config_path, "r", encoding="utf-8") as handle:
        return yaml.safe_load(handle) or {}


def _write_json(output_path: Path, rows: List[Dict[str, Any]]) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "w", encoding="utf-8") as handle:
        json.dump(rows, handle, ensure_ascii=False, indent=2)


def _load_jsonl(path: Path) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    with open(path, "r", encoding="utf-8") as handle:
        for line_number, line in enumerate(handle, start=1):
            stripped = line.strip()
            if not stripped:
                continue
            payload = json.loads(stripped)
            if not isinstance(payload, dict):
                raise ValueError(f"Expected JSON object on line {line_number} of {path}")
            rows.append(payload)
    return rows


class _JsonArrayAppendWriter:
    def __init__(self, output_path: Path):
        self.output_path = output_path
        self.count = 0
        self._initialize()

    def _initialize(self) -> None:
        self.output_path.parent.mkdir(parents=True, exist_ok=True)
        with open(self.output_path, "w", encoding="utf-8") as handle:
            handle.write("[\n]\n")

    def append(self, row: Dict[str, Any]) -> None:
        payload = json.dumps(row, ensure_ascii=False, indent=2)
        with open(self.output_path, "r+", encoding="utf-8") as handle:
            handle.seek(0, 2)
            end_pos = handle.tell()
            if end_pos < 3:
                handle.seek(0)
                handle.write("[\n]\n")
                handle.seek(0, 2)
                end_pos = handle.tell()
            handle.seek(max(0, end_pos - 2))
            if self.count == 0:
                handle.write(payload)
                handle.write("\n]\n")
            else:
                handle.write(",\n")
                handle.write(payload)
                handle.write("\n]\n")
            handle.truncate()
        self.count += 1


def _progress_interval(total: int) -> int:
    if total <= 0:
        return 1
    return max(1, min(50, total // 20 or 1))


def _log_dataset(message: str) -> None:
    print(message, flush=True)


def _resolve_repo_path(path_text: str) -> Path:
    target = Path(path_text).expanduser()
    if target.is_absolute():
        return target
    return (REPO_ROOT / target).resolve()


def build_spatialqueryqa_rows(
    dataset_config: Dict[str, Any],
    embedded_db_config: Dict[str, Any],
    eval_config: Dict[str, Any],
    *,
    workers: int = 1,
    on_row: Optional[Callable[[Dict[str, Any]], None]] = None,
) -> List[Dict[str, Any]]:
    dataset_cfg = dataset_config["datasets"]["spatialqueryqa"]
    input_root = _resolve_repo_path(str(dataset_cfg["raw_data_path"]))
    columns = dataset_cfg.get("columns", {})
    question_col = columns.get("question", "Question")
    sql_col = columns.get("sql", "Reviewed Query")
    partitions = dataset_cfg.get("source_partitions", {})
    execution_timeout_sec = _resolve_result_materialization_timeout_seconds(dataset_cfg, eval_config)
    tasks: List[Dict[str, Any]] = []

    _log_dataset("[spatialqueryqa] scanning raw benchmark files...")

    for partition_key, partition in partitions.items():
        level = str(partition.get("level") or "").strip()
        xlsx_path = input_root / str(partition.get("raw_path") or "")
        db_settings = resolve_db_settings(
            embedded_db_config,
            dataset_config,
            "spatialqueryqa",
            dict(partition),
            allow_fallback_mapping=True,
        )
        if not db_settings:
            raise ValueError(f"Missing database settings for SpatialQueryQA partition: {partition_key}")
        connect_timeout = int(
            dataset_cfg.get("result_materialization", {}).get("connect_timeout")
            or (db_settings.get("timeout") or {}).get("connection_timeout")
            or 10
        )
        workbook = openpyxl.load_workbook(xlsx_path, read_only=True)
        sheet = workbook.active
        headers = [cell.value for cell in sheet[1]]
        try:
            for row_index, values in enumerate(sheet.iter_rows(min_row=2, values_only=True), start=2):
                record = dict(zip(headers, values))
                question = str(record.get(question_col) or "").strip()
                sql = str(record.get(sql_col) or "").strip()
                if not question or not sql:
                    continue
                tasks.append(
                    {
                        "partition_key": partition_key,
                        "level": level,
                        "question": question,
                        "sql": sql,
                        "db_settings": db_settings,
                        "connect_timeout": connect_timeout,
                        "sample_label": f"{partition_key}:row{row_index}",
                        "error_context": f"{xlsx_path}:{row_index}",
                    }
                )
        finally:
            workbook.close()

    _log_dataset(
        f"[spatialqueryqa] collected {len(tasks)} executable samples, materializing results sequentially..."
    )
    materialized = _materialize_results_serially(
        tasks,
        timeout_seconds=execution_timeout_sec,
        dataset_label="spatialqueryqa",
        timeout_policy="keep_if_explain_ok",
    )

    rows: List[Dict[str, Any]] = []
    partition_sequences: Dict[str, int] = {}
    kept_timeouts = 0
    for task in materialized:
        status = task["execution_status"]
        if status == "timeout":
            if task.get("explain_status") != "ok":
                raise RuntimeError(
                    f"SpatialQueryQA SQL execution timed out and EXPLAIN failed at "
                    f"{task['error_context']}: {task.get('explain_payload')}"
                )
            kept_timeouts += 1
        elif status != "ok":
            raise RuntimeError(
                f"SpatialQueryQA SQL execution failed at {task['error_context']}: {task['execution_payload']}"
            )

        partition_key = task["partition_key"]
        partition_sequences[partition_key] = partition_sequences.get(partition_key, 0) + 1
        row = {
            "id": f"spatialqueryqa_{partition_key}_{partition_sequences[partition_key]:05d}",
            "question": task["question"],
            "sql": task["sql"],
            "level": task["level"],
        }
        if status == "ok":
            row["results"] = task["execution_payload"]
        else:
            row["result_materialization_status"] = "timeout"
            row["result_materialization_error"] = task["execution_payload"]
            row["explain_result"] = task.get("explain_payload")
        rows.append(row)
        if on_row is not None:
            on_row(rows[-1])

    _log_dataset(
        f"[spatialqueryqa] materialized {len(rows)} samples"
        + (f", kept {kept_timeouts} timeout sample(s) after EXPLAIN" if kept_timeouts else "")
    )
    return rows

def build_spatialsql_rows(
    dataset_config: Dict[str, Any],
    embedded_db_config: Dict[str, Any],
    eval_config: Dict[str, Any],
    *,
    workers: int = 1,
    on_row: Optional[Callable[[Dict[str, Any]], None]] = None,
) -> List[Dict[str, Any]]:
    dataset_cfg = dataset_config["datasets"]["spatialsql"]
    input_root = _resolve_repo_path(str(dataset_cfg["raw_data_path"]))
    partitions = dataset_cfg.get("source_partitions", {})
    tasks: List[Dict[str, Any]] = []
    execution_timeout_sec = _resolve_result_materialization_timeout_seconds(dataset_cfg, eval_config)

    _log_dataset("[spatialsql] scanning raw benchmark files...")

    for partition_key, partition in partitions.items():
        domain = str(partition.get("domain") or partition.get("level") or "").strip()
        domain_dir = input_root / str(partition.get("raw_path") or "")
        db_settings = resolve_db_settings(
            embedded_db_config,
            dataset_config,
            "spatialsql",
            dict(partition),
            allow_fallback_mapping=True,
        )
        if not db_settings:
            raise ValueError(f"Missing database settings for SpatialSQL partition: {partition_key}")
        connect_timeout = _resolve_connect_timeout_seconds(dataset_cfg, db_settings)
        sequence = 0
        for txt_path in sorted(domain_dir.glob("QA-*.txt")):
            content = txt_path.read_text(encoding="utf-8", errors="replace")
            for block in re.split(r"\n\s*\n", content):
                record = _parse_qa_txt_block(block)
                if not record:
                    continue

                question = str(record.get("question") or "").strip()
                original_sql = str(record.get("SQL") or "").strip()
                sql_candidates = _split_sql_candidates(original_sql)
                source_sql = sql_candidates[0] if sql_candidates else ""
                if not question or not original_sql or not source_sql:
                    continue

                converted_primary, _issues = convert_spatialite_to_postgis(
                    source_sql,
                    table_prefix=None,
                )

                sequence += 1
                row = {
                    key: value
                    for key, value in record.items()
                    if not str(key).startswith("_") and key != "SQL"
                }
                row["id"] = row.get("id") or f"spatialsql_{partition_key}_{sequence:05d}"
                row["label"] = [
                    item
                    for item in str(record.get("label") or "").split()
                    if item
                ]
                row["original_sql"] = original_sql
                row["sql"] = converted_primary
                row["level"] = domain
                tasks.append(
                    {
                        "row": row,
                        "sql": converted_primary,
                        "db_settings": db_settings,
                        "connect_timeout": connect_timeout,
                        "sample_label": f"{partition_key}:{record.get('id') or sequence}",
                        "error_context": f"{txt_path}:{record.get('id')}",
                    }
                )

    _log_dataset(
        f"[spatialsql] collected {len(tasks)} executable samples, materializing results sequentially..."
    )
    materialized = _materialize_results_serially(
        tasks,
        timeout_seconds=execution_timeout_sec,
        dataset_label="spatialsql",
        timeout_policy="error",
    )
    rows: List[Dict[str, Any]] = []
    for task in materialized:
        if task["execution_status"] != "ok":
            raise RuntimeError(
                f"SpatialSQL SQL execution failed at {task['error_context']}: {task['execution_payload']}"
            )
        row = dict(task["row"])
        row["results"] = task["execution_payload"]
        rows.append(row)
        if on_row is not None:
            on_row(row)

    _log_dataset(f"[spatialsql] materialized {len(rows)} samples")
    return rows


def build_floodsql_rows(
    dataset_config: Dict[str, Any],
    embedded_db_config: Dict[str, Any],
    eval_config: Dict[str, Any],
    *,
    workers: int = 1,
    on_row: Optional[Callable[[Dict[str, Any]], None]] = None,
) -> List[Dict[str, Any]]:
    dataset_cfg = dataset_config["datasets"]["floodsql"]
    input_root = _resolve_repo_path(str(dataset_cfg["raw_data_path"]))
    partitions = dataset_cfg.get("source_partitions", {})
    tasks: List[Dict[str, Any]] = []
    execution_timeout_sec = _resolve_result_materialization_timeout_seconds(dataset_cfg, eval_config)

    updated_path = _resolve_floodsql_updated_benchmark_path(dataset_cfg, input_root)
    _log_dataset(f"[floodsql] scanning updated benchmark file: {updated_path}")

    db_settings = resolve_db_settings(
        embedded_db_config,
        dataset_config,
        "floodsql",
        {},
        allow_fallback_mapping=True,
    )
    if not db_settings:
        raise ValueError("Missing database settings for FloodSQL")
    connect_timeout = _resolve_connect_timeout_seconds(dataset_cfg, db_settings)
    metadata_by_id = _build_floodsql_metadata_by_id(input_root, partitions)
    updated_records = _load_jsonl(updated_path)

    for record in updated_records:
        source_id = str(record.get("id") or "").strip()
        question = str(record.get("question") or "").strip()
        source_sql = str(record.get("sql") or "").strip()
        if not source_id or not question or not source_sql:
            continue
        metadata_row = metadata_by_id.get(source_id, {})
        converted_sql, _issues = convert_duckdb_to_postgis(source_sql)
        row = {
            "id": source_id,
            "question": question,
            "level": record.get("level") or metadata_row.get("level") or source_id.split("_", 1)[0],
            "category": record.get("category") or metadata_row.get("category"),
            "output_type": record.get("output_type") or metadata_row.get("output_type"),
            "expected_columns": record.get("expected_columns") or metadata_row.get("expected_columns") or [],
            "source_sql": source_sql,
            "sql": converted_sql,
        }
        tasks.append(
            {
                "row": row,
                "sql": converted_sql,
                "db_settings": db_settings,
                "connect_timeout": connect_timeout,
                "sample_label": source_id,
                "error_context": f"{updated_path}:{source_id}",
            }
        )

    _log_dataset(
        f"[floodsql] collected {len(tasks)} executable samples, materializing results sequentially..."
    )
    materialized = _materialize_results_serially(
        tasks,
        timeout_seconds=execution_timeout_sec,
        dataset_label="floodsql",
        timeout_policy="error",
    )
    rows: List[Dict[str, Any]] = []
    for task in materialized:
        if task["execution_status"] != "ok":
            raise RuntimeError(
                f"FloodSQL SQL execution failed at {task['error_context']}: {task['execution_payload']}"
            )
        row = dict(task["row"])
        row["results"] = task["execution_payload"]
        rows.append(row)
        if on_row is not None:
            on_row(row)

    _log_dataset(f"[floodsql] materialized {len(rows)} samples")
    return rows


def _resolve_floodsql_updated_benchmark_path(dataset_cfg: Dict[str, Any], input_root: Path) -> Path:
    configured = str(dataset_cfg.get("updated_benchmark_path") or "").strip()
    candidates: List[Path] = []
    if configured:
        configured_path = Path(configured)
        candidates.append(configured_path if configured_path.is_absolute() else input_root / configured_path)
    candidates.extend(
        [
            input_root / "benchmark" / "bechmark_updated.jsonl",
            input_root / "benchmark" / "benchmark_updated.jsonl",
        ]
    )
    for candidate in candidates:
        if candidate.exists():
            return candidate
    raise FileNotFoundError(
        "Could not find FloodSQL updated benchmark JSONL. Checked: "
        + ", ".join(str(candidate) for candidate in candidates)
    )


def _build_floodsql_metadata_by_id(input_root: Path, partitions: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
    metadata_by_id: Dict[str, Dict[str, Any]] = {}
    partition_values = [
        partition for partition in partitions.values() if isinstance(partition, dict)
    ]
    partition_values.sort(
        key=lambda partition: (
            "updated" in str(partition.get("raw_path") or ""),
            str(partition.get("raw_path") or ""),
        )
    )

    for partition in partition_values:
        raw_path = str(partition.get("raw_path") or "").strip()
        if not raw_path:
            continue
        family_dir = input_root / raw_path
        for json_path in sorted(family_dir.glob("*.json")):
            payload = json.loads(json_path.read_text(encoding="utf-8"))
            if not isinstance(payload, list):
                continue
            for record in payload:
                if not isinstance(record, dict):
                    continue
                source_id = str(record.get("id") or "").strip()
                if not source_id:
                    continue
                metadata_by_id[source_id] = {
                    "level": record.get("level") or partition.get("level"),
                    "category": record.get("category"),
                    "output_type": record.get("output_type"),
                    "expected_columns": record.get("expected_columns") or [],
                }
    return metadata_by_id


def build_all(
    config_path: Path,
    eval_config_path: Path,
    *,
    workers: int = 1,
    floodsql_updated_benchmark_path: Optional[str] = None,
) -> Dict[str, int]:
    config = _load_dataset_config(config_path)
    eval_config = _load_eval_config(eval_config_path)
    datasets = config.get("datasets", {})
    if floodsql_updated_benchmark_path:
        floodsql_cfg = datasets.setdefault("floodsql", {})
        floodsql_cfg["updated_benchmark_path"] = floodsql_updated_benchmark_path
    embedded_db_config = extract_embedded_db_config(config)

    spatialqueryqa_cfg = datasets["spatialqueryqa"]
    spatialsql_cfg = datasets["spatialsql"]
    floodsql_cfg = datasets["floodsql"]

    outputs = {
        "spatialqueryqa": Path(spatialqueryqa_cfg["data_path"]),
        "spatialsql": Path(spatialsql_cfg["data_path"]),
        "floodsql": Path(floodsql_cfg["data_path"]),
    }

    del workers
    _log_dataset("[formatter] starting benchmark normalization in single-thread mode")

    spatialqueryqa_writer = _JsonArrayAppendWriter(outputs["spatialqueryqa"])
    spatialsql_writer = _JsonArrayAppendWriter(outputs["spatialsql"])
    floodsql_writer = _JsonArrayAppendWriter(outputs["floodsql"])

    spatialqueryqa_rows = build_spatialqueryqa_rows(
        config,
        embedded_db_config,
        eval_config,
        workers=1,
        on_row=spatialqueryqa_writer.append,
    )
    _log_dataset(f"[formatter] wrote {len(spatialqueryqa_rows)} rows -> {outputs['spatialqueryqa']}")
    spatialsql_rows = build_spatialsql_rows(
        config,
        embedded_db_config,
        eval_config,
        workers=1,
        on_row=spatialsql_writer.append,
    )
    _log_dataset(f"[formatter] wrote {len(spatialsql_rows)} rows -> {outputs['spatialsql']}")
    floodsql_rows = build_floodsql_rows(
        config,
        embedded_db_config,
        eval_config,
        workers=1,
        on_row=floodsql_writer.append,
    )
    _log_dataset(f"[formatter] wrote {len(floodsql_rows)} rows -> {outputs['floodsql']}")

    return {
        "spatialqueryqa": len(spatialqueryqa_rows),
        "spatialsql": len(spatialsql_rows),
        "floodsql": len(floodsql_rows),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Build unified benchmark JSON inputs.")
    parser.add_argument(
        "--config",
        default="config/dataset_config.yaml",
        help="Path to dataset_config.yaml",
    )
    parser.add_argument(
        "--eval-config",
        default="config/eval_config.yaml",
        help="Path to eval_config.yaml",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=1,
        help="Reserved for compatibility; formatter now always runs in single-thread mode",
    )
    parser.add_argument(
        "--floodsql-updated-benchmark",
        default=None,
        help=(
            "FloodSQL updated benchmark JSONL path. Relative paths are resolved under "
            "the FloodSQL-Bench root."
        ),
    )
    args = parser.parse_args()
    counts = build_all(
        Path(args.config),
        Path(args.eval_config),
        workers=1,
        floodsql_updated_benchmark_path=args.floodsql_updated_benchmark,
    )
    print(json.dumps(counts, ensure_ascii=False, indent=2))


def _resolve_result_materialization_timeout_seconds(
    dataset_cfg: Dict[str, Any],
    eval_config: Dict[str, Any],
) -> int:
    materialization_cfg = dataset_cfg.get("result_materialization", {})
    explicit_timeout = materialization_cfg.get("timeout_seconds")
    if explicit_timeout is not None:
        return max(1, int(explicit_timeout))

    eval_timeout = int((eval_config.get("evaluation") or {}).get("timeout", 60))
    timeout_buffer_seconds = int(materialization_cfg.get("timeout_buffer_seconds", 10))
    return max(1, eval_timeout - timeout_buffer_seconds)


def _resolve_connect_timeout_seconds(dataset_cfg: Dict[str, Any], db_settings: Dict[str, Any]) -> int:
    return int(
        dataset_cfg.get("result_materialization", {}).get("connect_timeout")
        or (db_settings.get("timeout") or {}).get("connection_timeout")
        or 10
    )


def _materialize_results_serially(
    tasks: List[Dict[str, Any]],
    *,
    timeout_seconds: int,
    dataset_label: str,
    timeout_policy: str,
) -> List[Dict[str, Any]]:
    if not tasks:
        return []

    interval = _progress_interval(len(tasks))
    connections: Dict[str, Any] = {}

    def get_connection_key(task: Dict[str, Any]) -> str:
        db_settings = task["db_settings"]
        return json.dumps(
            {
                "host": db_settings["host"],
                "port": db_settings["port"],
                "database": db_settings["database"],
                "user": db_settings["user"],
                "schema": db_settings.get("schema"),
                "search_path": db_settings.get("search_path"),
                "timeout_seconds": timeout_seconds,
                "connect_timeout": int(task["connect_timeout"]),
            },
            sort_keys=True,
            ensure_ascii=False,
        )

    results: List[Dict[str, Any]] = []
    try:
        for index, task in enumerate(tasks, start=1):
            connection_key = get_connection_key(task)
            connection = connections.get(connection_key)
            if connection is None:
                connection = _connect_postgres(
                    task["db_settings"],
                    timeout_seconds=timeout_seconds,
                    connect_timeout=int(task["connect_timeout"]),
                )
                connections[connection_key] = connection
            status, payload = _execute_query_for_results(
                connection,
                task["sql"],
                sample_label=task["sample_label"],
            )
            enriched = dict(task)
            enriched["execution_status"] = status
            enriched["execution_payload"] = payload
            if status == "timeout" and timeout_policy == "keep_if_explain_ok":
                _rollback_quietly(connection)
                explain_status, explain_payload = _execute_explain_for_sql(
                    connection,
                    task["sql"],
                    sample_label=task["sample_label"],
                )
                enriched["explain_status"] = explain_status
                enriched["explain_payload"] = explain_payload
            results.append(enriched)
            if index == 1 or index % interval == 0 or index == len(tasks):
                _log_dataset(
                    f"[{dataset_label}] materialized {index}/{len(tasks)} result set(s)"
                )
    finally:
        for connection in connections.values():
            try:
                connection.close()
            except Exception:
                pass

    if timeout_policy not in {"skip", "error", "keep_if_explain_ok"}:
        raise ValueError(f"Unsupported timeout policy: {timeout_policy}")
    return results


def _connect_postgres(
    db_settings: Dict[str, Any],
    *,
    timeout_seconds: int,
    connect_timeout: int,
):
    connection = psycopg2.connect(
        host=db_settings["host"],
        port=db_settings["port"],
        database=db_settings["database"],
        user=db_settings["user"],
        password=db_settings["password"],
        connect_timeout=connect_timeout,
        options=f"-c statement_timeout={int(timeout_seconds * 1000)}",
    )
    connection.autocommit = True
    cursor = connection.cursor()
    try:
        apply_search_path(cursor, db_settings)
    finally:
        cursor.close()
    return connection


def _execute_query_for_results(connection, sql: str, *, sample_label: str) -> tuple[str, Any]:
    cursor = connection.cursor()
    try:
        cursor.execute(sql)
        rows = cursor.fetchall() if cursor.description is not None else []
        return "ok", normalize_result_rows(rows)
    except Exception as exc:
        error_message = str(exc)
        lowered = error_message.lower()
        if (
            "statement timeout" in lowered
            or "canceling statement due to statement timeout" in lowered
            or "interruptedexception" in lowered
            or "interrupted!" in lowered
        ):
            return "timeout", error_message
        return "error", error_message
    finally:
        cursor.close()


def _execute_explain_for_sql(connection, sql: str, *, sample_label: str) -> tuple[str, Any]:
    cursor = connection.cursor()
    try:
        cursor.execute(_build_explain_sql(sql))
        rows = cursor.fetchall() if cursor.description is not None else []
        return "ok", normalize_result_rows(rows)
    except Exception as exc:
        error_message = str(exc)
        lowered = error_message.lower()
        if (
            "statement timeout" in lowered
            or "canceling statement due to statement timeout" in lowered
            or "interruptedexception" in lowered
            or "interrupted!" in lowered
        ):
            return "timeout", error_message
        return "error", error_message
    finally:
        cursor.close()


def _build_explain_sql(sql: str) -> str:
    cleaned = str(sql or "").strip().rstrip(";")
    return f"EXPLAIN {cleaned}"


def _rollback_quietly(connection) -> None:
    rollback = getattr(connection, "rollback", None)
    if callable(rollback):
        try:
            rollback()
        except Exception:
            pass


if __name__ == "__main__":
    main()
