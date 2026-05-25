#!/usr/bin/env python3
"""Deduplicate synthesized SQL rows per database and resequence sql_id values."""

from __future__ import annotations

import argparse
import json
from collections import defaultdict
from pathlib import Path
from typing import Any


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Keep up to N unique SQL rows per database_id from a synthesized SQL JSONL file, "
            "dropping exact duplicate SQL rows even when the final count stays below N, "
            "then rewrite sql_id values in per-database sequence order."
        )
    )
    parser.add_argument(
        "--input",
        default="data/processed/synthesized_sql_queries.jsonl",
        help="Path to the input synthesized SQL JSONL file.",
    )
    parser.add_argument(
        "--output",
        default="data/processed/synthesized_sql_queries_deduped.jsonl",
        help="Path to the deduplicated output JSONL file.",
    )
    parser.add_argument(
        "--per-database",
        type=int,
        default=140,
        help=(
            "Maximum number of unique SQL rows to keep per database_id. "
            "Exact duplicate SQL rows are always dropped and are never used to backfill up to this limit. "
            "Default: 140."
        ),
    )
    return parser


def load_rows(path: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    with path.open("r", encoding="utf-8") as handle:
        for line_number, line in enumerate(handle, start=1):
            stripped = line.strip()
            if not stripped:
                continue
            payload = json.loads(stripped)
            if not isinstance(payload, dict):
                raise ValueError(f"Expected JSON object on line {line_number}, got {type(payload).__name__}.")
            rows.append(payload)
    return rows


def normalize_sql(sql_text: str) -> str:
    return " ".join((sql_text or "").split()).strip()


def deduplicate_rows(rows: list[dict[str, Any]], per_database: int) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    grouped_rows: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        database_id = str(row.get("database_id") or "").strip()
        if not database_id:
            raise ValueError("Encountered row without database_id.")
        grouped_rows[database_id].append(row)

    deduped_rows: list[dict[str, Any]] = []
    stats: list[dict[str, Any]] = []

    for database_id in sorted(grouped_rows):
        kept_for_db: list[dict[str, Any]] = []
        seen_sql_keys: set[str] = set()
        duplicate_rows = 0
        overflow_rows = 0

        for row in grouped_rows[database_id]:
            sql_text_raw = str(row.get("sql") or "")
            if not sql_text_raw.strip():
                continue
            sql_key = normalize_sql(sql_text_raw)
            if sql_key in seen_sql_keys:
                duplicate_rows += 1
                continue
            if len(kept_for_db) >= per_database:
                overflow_rows += 1
                continue
            seen_sql_keys.add(sql_key)
            row_copy = dict(row)
            row_copy["sql_id"] = f"{database_id}_{len(kept_for_db) + 1:04d}"
            kept_for_db.append(row_copy)

        deduped_rows.extend(kept_for_db)
        stats.append(
            {
                "database_id": database_id,
                "input_rows": len(grouped_rows[database_id]),
                "kept_rows": len(kept_for_db),
                "duplicate_rows_dropped": duplicate_rows,
                "overflow_rows_dropped": overflow_rows,
            }
        )

    return deduped_rows, stats


def write_rows(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row, ensure_ascii=False))
            handle.write("\n")


def main() -> int:
    args = build_parser().parse_args()
    input_path = Path(args.input)
    output_path = Path(args.output)

    if not input_path.is_file():
        raise FileNotFoundError(f"Input JSONL file not found: {input_path}")
    if args.per_database <= 0:
        raise ValueError("--per-database must be a positive integer.")

    rows = load_rows(input_path)
    deduped_rows, stats = deduplicate_rows(rows, per_database=args.per_database)
    write_rows(output_path, deduped_rows)

    print(f"input_rows={len(rows)}")
    print(f"output_rows={len(deduped_rows)}")
    print(f"output_path={output_path}")
    print("== Per Database ==")
    for item in stats:
        print(json.dumps(item, ensure_ascii=False))

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
