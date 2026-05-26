"""Build unified benchmark JSON inputs for inference."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import re
from typing import Any, Dict, List

import openpyxl
import psycopg2
import yaml

from src.datasets.db_routing import apply_search_path, extract_embedded_db_config, resolve_db_settings
from src.datasets.loaders.spatialsql_loader import _parse_qa_txt_block, _split_sql_candidates
from src.sql.sql_dialect_adapter import convert_duckdb_to_postgis, convert_spatialite_to_postgis
from src.utils.execution_results import normalize_result_rows


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


def build_spatialqueryqa_rows(
    dataset_config: Dict[str, Any],
    embedded_db_config: Dict[str, Any],
    eval_config: Dict[str, Any],
) -> List[Dict[str, Any]]:
    dataset_cfg = dataset_config["datasets"]["spatialqueryqa"]
    input_root = Path(dataset_cfg["raw_data_path"])
    columns = dataset_cfg.get("columns", {})
    question_col = columns.get("question", "Question")
    sql_col = columns.get("sql", "Reviewed Query")
    reviewer_effort_col = columns.get("reviewer_effort", "Reviewer Effort")
    partitions = dataset_cfg.get("source_partitions", {})
    execution_timeout_sec = _resolve_spatialqueryqa_timeout_seconds(dataset_cfg, eval_config)
    rows: List[Dict[str, Any]] = []

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
        sequence = 0
        connection = _connect_postgres(
            db_settings,
            timeout_seconds=execution_timeout_sec,
            connect_timeout=connect_timeout,
        )
        try:
            for row_index, values in enumerate(sheet.iter_rows(min_row=2, values_only=True), start=2):
                record = dict(zip(headers, values))
                question = str(record.get(question_col) or "").strip()
                sql = str(record.get(sql_col) or "").strip()
                reviewer_effort = str(record.get(reviewer_effort_col) or "").strip()
                if reviewer_effort == "❌":
                    continue
                if not question or not sql:
                    continue

                execution_status, execution_results = _execute_query_for_results(
                    connection,
                    sql,
                    sample_label=f"{partition_key}:row{row_index}",
                )
                if execution_status == "timeout":
                    continue
                if execution_status != "ok":
                    raise RuntimeError(
                        f"SpatialQueryQA SQL execution failed at {xlsx_path}:{row_index}: {execution_results}"
                    )

                sequence += 1
                rows.append(
                    {
                        "id": f"spatialqueryqa_{partition_key}_{sequence:05d}",
                        "question": question,
                        "sql": sql,
                        "level": level,
                        "results": execution_results,
                    }
                )
        finally:
            workbook.close()
            connection.close()

    return rows

def build_spatialsql_rows(dataset_cfg: Dict[str, Any]) -> List[Dict[str, Any]]:
    input_root = Path(dataset_cfg["raw_data_path"])
    partitions = dataset_cfg.get("source_partitions", {})
    rows: List[Dict[str, Any]] = []

    for partition_key, partition in partitions.items():
        domain = str(partition.get("domain") or partition.get("level") or "").strip()
        domain_dir = input_root / str(partition.get("raw_path") or "")
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
                rows.append(row)

    return rows


def build_floodsql_rows(dataset_cfg: Dict[str, Any]) -> List[Dict[str, Any]]:
    input_root = Path(dataset_cfg["raw_data_path"])
    partitions = dataset_cfg.get("source_partitions", {})
    rows: List[Dict[str, Any]] = []

    for partition in partitions.values():
        family = str(partition.get("family") or "").strip()
        level = str(partition.get("level") or "").strip()
        family_dir = input_root / str(partition.get("raw_path") or "")
        if not family or not family_dir.exists():
            continue
        for json_path in sorted(family_dir.glob("*.json")):
            question_rows = json.loads(json_path.read_text(encoding="utf-8"))
            if not isinstance(question_rows, list):
                continue
            for record in question_rows:
                if not isinstance(record, dict):
                    continue
                question = str(record.get("question") or "").strip()
                source_sql = str(record.get("sql") or "").strip()
                if not question or not source_sql:
                    continue
                converted_sql, _issues = convert_duckdb_to_postgis(source_sql)
                row = dict(record)
                row["source_sql"] = row.pop("sql")
                row["sql"] = converted_sql
                if level:
                    row.setdefault("level", level)
                rows.append(row)

    return rows


def build_all(config_path: Path, eval_config_path: Path) -> Dict[str, int]:
    config = _load_dataset_config(config_path)
    eval_config = _load_eval_config(eval_config_path)
    datasets = config.get("datasets", {})
    embedded_db_config = extract_embedded_db_config(config)

    spatialqueryqa_cfg = datasets["spatialqueryqa"]
    spatialsql_cfg = datasets["spatialsql"]
    floodsql_cfg = datasets["floodsql"]

    outputs = {
        "spatialqueryqa": Path(spatialqueryqa_cfg["data_path"]),
        "spatialsql": Path(spatialsql_cfg["data_path"]),
        "floodsql": Path(floodsql_cfg["data_path"]),
    }

    spatialqueryqa_rows = build_spatialqueryqa_rows(config, embedded_db_config, eval_config)
    spatialsql_rows = build_spatialsql_rows(spatialsql_cfg)
    floodsql_rows = build_floodsql_rows(floodsql_cfg)

    _write_json(outputs["spatialqueryqa"], spatialqueryqa_rows)
    _write_json(outputs["spatialsql"], spatialsql_rows)
    _write_json(outputs["floodsql"], floodsql_rows)

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
    args = parser.parse_args()
    counts = build_all(Path(args.config), Path(args.eval_config))
    print(json.dumps(counts, ensure_ascii=False, indent=2))


def _resolve_spatialqueryqa_timeout_seconds(
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


if __name__ == "__main__":
    main()
