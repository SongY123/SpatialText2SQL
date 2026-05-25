#!/usr/bin/env python3
"""Analyze synthesized SQL JSONL output for duplicate writes and count anomalies."""

from __future__ import annotations

import argparse
import json
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Analyze synthesized SQL JSONL output for duplicate rows or suspicious count growth.",
    )
    parser.add_argument(
        "--input",
        default="data/processed/synthesized_sql_queries.jsonl",
        help="Path to synthesized SQL JSONL output.",
    )
    parser.add_argument(
        "--expected-per-database",
        type=int,
        default=140,
        help="Expected upper bound per database for a single run. Default: 140.",
    )
    parser.add_argument(
        "--top-k",
        type=int,
        default=20,
        help="How many duplicate/count examples to print. Default: 20.",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Print machine-readable JSON instead of text summary.",
    )
    return parser


def normalize_sql(sql_text: str) -> str:
    return " ".join((sql_text or "").split()).strip()


def load_rows(path: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    with path.open("r", encoding="utf-8") as handle:
        for line_number, line in enumerate(handle, start=1):
            stripped = line.strip()
            if not stripped:
                continue
            try:
                payload = json.loads(stripped)
            except json.JSONDecodeError as exc:
                raise ValueError(f"Invalid JSON on line {line_number}: {exc}") from exc
            if not isinstance(payload, dict):
                raise ValueError(f"Expected JSON object on line {line_number}, got {type(payload).__name__}.")
            rows.append(payload)
    return rows


def analyze(rows: list[dict[str, Any]], expected_per_database: int, top_k: int) -> dict[str, Any]:
    sql_id_counter: Counter[str] = Counter()
    db_counter: Counter[str] = Counter()
    db_sql_counter: Counter[tuple[str, str]] = Counter()
    db_norm_sql_counter: Counter[tuple[str, str]] = Counter()
    db_difficulty_counter: dict[str, Counter[str]] = defaultdict(Counter)
    db_sql_id_examples: dict[str, list[str]] = defaultdict(list)

    missing_sql_id = 0
    missing_database_id = 0
    missing_sql = 0

    for row in rows:
        sql_id = str(row.get("sql_id") or "").strip()
        database_id = str(row.get("database_id") or "").strip()
        sql_text = str(row.get("sql") or "").strip()
        difficulty = str(row.get("difficulty_level") or "").strip()

        if not sql_id:
            missing_sql_id += 1
        else:
            sql_id_counter[sql_id] += 1

        if not database_id:
            missing_database_id += 1
        else:
            db_counter[database_id] += 1
            if sql_id:
                db_sql_id_examples[database_id].append(sql_id)

        if not sql_text:
            missing_sql += 1
        elif database_id:
            db_sql_counter[(database_id, sql_text)] += 1
            db_norm_sql_counter[(database_id, normalize_sql(sql_text))] += 1

        if database_id and difficulty:
            db_difficulty_counter[database_id][difficulty] += 1

    duplicate_sql_ids = [(sql_id, count) for sql_id, count in sql_id_counter.items() if count > 1]
    duplicate_db_sql = [
        {"database_id": database_id, "count": count, "sql": sql_text}
        for (database_id, sql_text), count in db_sql_counter.items()
        if count > 1
    ]
    duplicate_db_normalized_sql = [
        {"database_id": database_id, "count": count, "normalized_sql": sql_text}
        for (database_id, sql_text), count in db_norm_sql_counter.items()
        if count > 1
    ]

    over_expected = [
        {
            "database_id": database_id,
            "row_count": count,
            "difficulty_counts": dict(db_difficulty_counter.get(database_id, {})),
        }
        for database_id, count in db_counter.items()
        if count > expected_per_database
    ]
    over_expected.sort(key=lambda item: (-item["row_count"], item["database_id"]))

    summary = {
        "total_rows": len(rows),
        "unique_sql_ids": len(sql_id_counter),
        "unique_databases": len(db_counter),
        "missing_sql_id_rows": missing_sql_id,
        "missing_database_id_rows": missing_database_id,
        "missing_sql_rows": missing_sql,
        "duplicate_sql_id_rows": sum(count - 1 for _, count in duplicate_sql_ids),
        "duplicate_sql_id_entries": len(duplicate_sql_ids),
        "duplicate_db_sql_rows": sum(item["count"] - 1 for item in duplicate_db_sql),
        "duplicate_db_sql_entries": len(duplicate_db_sql),
        "duplicate_db_normalized_sql_rows": sum(item["count"] - 1 for item in duplicate_db_normalized_sql),
        "duplicate_db_normalized_sql_entries": len(duplicate_db_normalized_sql),
        "databases_over_expected": len(over_expected),
        "expected_per_database": expected_per_database,
        "max_rows_in_one_database": max(db_counter.values()) if db_counter else 0,
    }

    sql_id_examples = sorted(duplicate_sql_ids, key=lambda item: (-item[1], item[0]))[:top_k]
    db_sql_examples = sorted(
        duplicate_db_sql,
        key=lambda item: (-item["count"], item["database_id"], item["sql"]),
    )[:top_k]
    db_norm_sql_examples = sorted(
        duplicate_db_normalized_sql,
        key=lambda item: (-item["count"], item["database_id"], item["normalized_sql"]),
    )[:top_k]
    over_expected_examples = over_expected[:top_k]
    top_database_counts = [
        {
            "database_id": database_id,
            "row_count": count,
            "difficulty_counts": dict(db_difficulty_counter.get(database_id, {})),
        }
        for database_id, count in db_counter.most_common(top_k)
    ]

    return {
        "summary": summary,
        "duplicate_sql_id_examples": sql_id_examples,
        "duplicate_db_sql_examples": db_sql_examples,
        "duplicate_db_normalized_sql_examples": db_norm_sql_examples,
        "databases_over_expected_examples": over_expected_examples,
        "top_database_counts": top_database_counts,
    }


def print_text_report(report: dict[str, Any]) -> None:
    summary = report["summary"]
    print("== Summary ==")
    for key in (
        "total_rows",
        "unique_sql_ids",
        "unique_databases",
        "missing_sql_id_rows",
        "missing_database_id_rows",
        "missing_sql_rows",
        "duplicate_sql_id_rows",
        "duplicate_sql_id_entries",
        "duplicate_db_sql_rows",
        "duplicate_db_sql_entries",
        "duplicate_db_normalized_sql_rows",
        "duplicate_db_normalized_sql_entries",
        "databases_over_expected",
        "expected_per_database",
        "max_rows_in_one_database",
    ):
        print(f"{key}: {summary[key]}")

    print("\n== Top Database Counts ==")
    for item in report["top_database_counts"]:
        print(json.dumps(item, ensure_ascii=False))

    print("\n== Duplicate sql_id Examples ==")
    if report["duplicate_sql_id_examples"]:
        for item in report["duplicate_sql_id_examples"]:
            print(json.dumps({"sql_id": item[0], "count": item[1]}, ensure_ascii=False))
    else:
        print("none")

    print("\n== Duplicate (database_id, sql) Examples ==")
    if report["duplicate_db_sql_examples"]:
        for item in report["duplicate_db_sql_examples"]:
            print(json.dumps(item, ensure_ascii=False))
    else:
        print("none")

    print("\n== Duplicate (database_id, normalized_sql) Examples ==")
    if report["duplicate_db_normalized_sql_examples"]:
        for item in report["duplicate_db_normalized_sql_examples"]:
            print(json.dumps(item, ensure_ascii=False))
    else:
        print("none")

    print("\n== Databases Over Expected Count ==")
    if report["databases_over_expected_examples"]:
        for item in report["databases_over_expected_examples"]:
            print(json.dumps(item, ensure_ascii=False))
    else:
        print("none")

    print("\n== Diagnosis ==")
    if summary["duplicate_sql_id_rows"] > 0:
        print("- 检测到重复 sql_id。这个更像是重复写入或重复运行追加。")
    if summary["duplicate_db_sql_rows"] > 0:
        print("- 检测到同一个 database_id 下完全相同的 SQL 重复出现。可能是重复写入，也可能是多次运行追加。")
    if summary["duplicate_db_normalized_sql_rows"] > 0 and summary["duplicate_db_sql_rows"] == 0:
        print("- 检测到同一个 database_id 下规范化后相同的 SQL。可能只是格式差异，也可能是重复生成。")
    if summary["databases_over_expected"] > 0:
        print("- 有数据库的样本数超过了单次理论上限。若本次只运行过一次，这很可疑。")
    if (
        summary["duplicate_sql_id_rows"] == 0
        and summary["duplicate_db_sql_rows"] == 0
        and summary["databases_over_expected"] == 0
    ):
        print("- 没发现明显的重复写入证据。总量异常更可能来自输入库数量比预期多，或历史结果被追加。")


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()

    input_path = Path(args.input)
    if not input_path.is_file():
        raise FileNotFoundError(f"Input JSONL file not found: {input_path}")

    rows = load_rows(input_path)
    report = analyze(
        rows,
        expected_per_database=args.expected_per_database,
        top_k=args.top_k,
    )

    if args.json:
        print(json.dumps(report, ensure_ascii=False, indent=2))
    else:
        print_text_report(report)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
