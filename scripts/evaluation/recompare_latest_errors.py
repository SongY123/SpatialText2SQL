#!/usr/bin/env python3
"""Recompare latest benchmark prediction results and refresh errors.json files."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any, Iterable

import psycopg2
import yaml

from src.datasets.db_routing import apply_search_path, resolve_db_settings
from src.utils.execution_results import compare_result_rows, normalize_result_rows


DEFAULT_DATASETS = ("spatialqueryqa", "spatialsql", "floodsql")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Read latest evaluation.json files, recompare current "
            "result_mismatch diff_details with current normalization/tolerance "
            "rules, and rewrite latest errors.json in the original compact "
            "pipeline format."
        )
    )
    parser.add_argument("--results-root", default="results/tasks")
    parser.add_argument("--dataset-config", default="config/dataset_config.yaml")
    parser.add_argument("--eval-config", default="config/eval_config.yaml")
    parser.add_argument("--backend", default="vllm")
    parser.add_argument("--model", default="qwen2.5-coder-7b")
    parser.add_argument("--config", default="finetune_alpaca")
    parser.add_argument("--datasets", nargs="+", default=list(DEFAULT_DATASETS))
    parser.add_argument(
        "--execute-sql",
        action="store_true",
        help=(
            "Re-execute predicted SQL for result_mismatch samples instead of "
            "using existing diff_details. This may be slow and requires DB access."
        ),
    )
    parser.add_argument(
        "--reexecute-execution-errors",
        action="store_true",
        help="Also retry samples whose previous error_type was execution_error.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print summaries without writing errors.json.",
    )
    return parser.parse_args()


def load_yaml(path: str | Path) -> dict[str, Any]:
    with open(path, "r", encoding="utf-8") as handle:
        data = yaml.safe_load(handle) or {}
    if not isinstance(data, dict):
        raise ValueError(f"YAML root must be a mapping: {path}")
    return data


def latest_dir(
    *,
    results_root: Path,
    dataset: str,
    backend: str,
    model: str,
    config: str,
) -> Path:
    return results_root / dataset / backend / model / config / "latest"


def execute_sql(
    sql: str,
    *,
    db_config: dict[str, Any],
    timeout_seconds: int,
) -> dict[str, Any]:
    info = {
        "status": None,
        "result": None,
        "result_count": None,
        "error": None,
        "error_type": None,
    }
    conn = None
    cursor = None
    connect_timeout = int(
        db_config.get("connect_timeout")
        or (db_config.get("timeout") or {}).get("connection_timeout")
        or 10
    )
    try:
        conn = psycopg2.connect(
            host=db_config["host"],
            port=db_config["port"],
            database=db_config["database"],
            user=db_config["user"],
            password=db_config["password"],
            connect_timeout=connect_timeout,
            options=f"-c statement_timeout={int(timeout_seconds) * 1000}",
        )
        cursor = conn.cursor()
        apply_search_path(cursor, db_config)
        cursor.execute(sql)
        rows = cursor.fetchall() if cursor.description is not None else []
        result = normalize_result_rows(rows)
        info["status"] = "ok"
        info["result"] = result
        info["result_count"] = len(result)
        return info
    except Exception as exc:
        info["error"] = str(exc)
        info["error_type"] = type(exc).__name__
        if "statement timeout" in str(exc).lower():
            info["status"] = "timeout"
        elif isinstance(
            exc,
            (
                getattr(psycopg2, "OperationalError", Exception),
                getattr(psycopg2, "InterfaceError", Exception),
            ),
        ):
            info["status"] = "connection_error"
        else:
            info["status"] = "execution_error"
        return info
    finally:
        if cursor is not None:
            try:
                cursor.close()
            except Exception:
                pass
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass


def should_recompare(detail: dict[str, Any], *, reexecute_execution_errors: bool) -> bool:
    if int(detail.get("correct", 0) or 0) == 1:
        return False
    if detail.get("error_type") == "result_mismatch":
        return True
    return reexecute_execution_errors and detail.get("error_type") == "execution_error"


def base_error_entry(detail: dict[str, Any]) -> dict[str, Any]:
    return {
        "id": detail.get("id"),
        "question": detail.get("question"),
        "gold_sql": detail.get("gold_sql"),
        "predicted_sql": detail.get("predicted_sql"),
    }


def recompare_detail(
    detail: dict[str, Any],
    *,
    db_config_full: dict[str, Any],
    dataset_config: dict[str, Any],
    dataset_name: str,
    timeout_seconds: int,
) -> tuple[bool, dict[str, Any] | None]:
    expected_rows = normalize_result_rows(detail.get("results"))
    predicted_sql = str(detail.get("predicted_sql") or "").strip()
    if not predicted_sql:
        return False, base_error_entry(detail)

    active_db_config = resolve_db_settings(
        db_config_full,
        dataset_config,
        dataset_name,
        detail.get("metadata") or {},
        allow_fallback_mapping=True,
    )
    execution = execute_sql(
        predicted_sql,
        db_config=active_db_config,
        timeout_seconds=timeout_seconds,
    )
    if execution["status"] != "ok":
        return False, base_error_entry(detail)

    matched, diff_details = compare_result_rows(
        execution["result"],
        expected_rows,
        left_name="predicted",
        right_name="gold",
    )
    if matched:
        return True, None

    return False, base_error_entry(detail)


def recompare_existing_diff(detail: dict[str, Any]) -> tuple[bool, dict[str, Any] | None]:
    diff_details = detail.get("diff_details") or {}
    only_in_predicted = diff_details.get("only_in_predicted")
    only_in_gold = diff_details.get("only_in_gold")
    if only_in_predicted is None or only_in_gold is None:
        return False, base_error_entry(detail)

    matched, _diff_details = compare_result_rows(
        only_in_predicted,
        only_in_gold,
        left_name="predicted",
        right_name="gold",
    )
    if matched:
        return True, None
    return False, base_error_entry(detail)


def refresh_dataset_errors(
    *,
    dataset: str,
    dataset_dir: Path,
    db_config_full: dict[str, Any],
    dataset_config: dict[str, Any],
    timeout_seconds: int,
    reexecute_execution_errors: bool,
    execute_sql_mode: bool,
    dry_run: bool,
) -> dict[str, Any]:
    evaluation_path = dataset_dir / "evaluation.json"
    errors_path = dataset_dir / "errors.json"
    if not evaluation_path.exists():
        raise FileNotFoundError(f"Missing evaluation file: {evaluation_path}")

    evaluation = json.loads(evaluation_path.read_text(encoding="utf-8"))
    details = evaluation.get("details") or []
    if not isinstance(details, list):
        raise ValueError(f"evaluation.details must be a list: {evaluation_path}")

    new_errors: list[dict[str, Any]] = []
    corrected_ids: list[str] = []
    recomputed = 0
    retained_without_recompute = 0
    recomputed_still_wrong = 0

    for detail in details:
        if not isinstance(detail, dict):
            continue
        if int(detail.get("correct", 0) or 0) == 1:
            continue
        if should_recompare(
            detail,
            reexecute_execution_errors=reexecute_execution_errors,
        ):
            recomputed += 1
            if execute_sql_mode:
                matched, entry = recompare_detail(
                    detail,
                    db_config_full=db_config_full,
                    dataset_config=dataset_config,
                    dataset_name=dataset,
                    timeout_seconds=timeout_seconds,
                )
            else:
                matched, entry = recompare_existing_diff(detail)
            if matched:
                corrected_ids.append(str(detail.get("id")))
                continue
            recomputed_still_wrong += 1
            if entry is not None:
                new_errors.append(entry)
            continue

        retained_without_recompute += 1
        new_errors.append(base_error_entry(detail))

    if not dry_run:
        errors_path.write_text(
            json.dumps(new_errors, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )

    return {
        "dataset": dataset,
        "evaluation": str(evaluation_path),
        "errors": str(errors_path),
        "original_wrong": sum(
            1
            for detail in details
            if isinstance(detail, dict) and int(detail.get("correct", 0) or 0) != 1
        ),
        "new_errors": len(new_errors),
        "corrected_by_recompare": len(corrected_ids),
        "corrected_ids": corrected_ids,
        "recomputed": recomputed,
        "recomputed_still_wrong": recomputed_still_wrong,
        "retained_without_recompute": retained_without_recompute,
    }


def print_summary(summaries: Iterable[dict[str, Any]]) -> None:
    for summary in summaries:
        print(
            "{dataset}: errors {original_wrong} -> {new_errors}; "
            "corrected={corrected_by_recompare}; recomputed={recomputed}; "
            "retained={retained_without_recompute}".format(**summary)
        )
        if summary["corrected_ids"]:
            print("  corrected_ids: " + ", ".join(summary["corrected_ids"]))


def main() -> int:
    args = parse_args()
    dataset_config = load_yaml(args.dataset_config)
    eval_config = load_yaml(args.eval_config)
    timeout_seconds = int((eval_config.get("evaluation") or {}).get("timeout") or 60)
    db_config_full = {
        "databases": dataset_config.get("databases", {}),
        "database": dataset_config.get("database", {}),
    }

    summaries = []
    for dataset in args.datasets:
        dataset_dir = latest_dir(
            results_root=Path(args.results_root),
            dataset=dataset,
            backend=args.backend,
            model=args.model,
            config=args.config,
        )
        summaries.append(
            refresh_dataset_errors(
                dataset=dataset,
                dataset_dir=dataset_dir,
                db_config_full=db_config_full,
                dataset_config=dataset_config,
                timeout_seconds=timeout_seconds,
                reexecute_execution_errors=args.reexecute_execution_errors,
                execute_sql_mode=args.execute_sql,
                dry_run=args.dry_run,
            )
        )

    print_summary(summaries)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
