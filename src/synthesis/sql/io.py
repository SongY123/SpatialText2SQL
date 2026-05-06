"""I/O helpers for SQL synthesis."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Mapping

from src.synthesis.database.io import load_synthesized_databases
from src.synthesis.database.models import SynthesizedSpatialDatabase

from .models import SynthesizedSQLQuery


def load_input_databases(input_path: str) -> list[SynthesizedSpatialDatabase]:
    return load_synthesized_databases(input_path)


def initialize_sql_output(output_path: str) -> None:
    path = Path(output_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("", encoding="utf-8")


def ensure_sql_output(output_path: str) -> None:
    path = Path(output_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.touch(exist_ok=True)


def load_existing_sql_id_offsets(output_path: str) -> dict[str, int]:
    path = Path(output_path)
    if not path.is_file():
        return {}
    offsets: dict[str, int] = {}
    with path.open("r", encoding="utf-8") as handle:
        for line_number, line in enumerate(handle, start=1):
            stripped = line.strip()
            if not stripped:
                continue
            try:
                payload = json.loads(stripped)
            except json.JSONDecodeError as exc:
                raise ValueError(f"Invalid JSON on line {line_number} of {path}: {exc}") from exc
            if not isinstance(payload, Mapping):
                raise ValueError(f"Invalid SQL synthesis row on line {line_number} of {path}: expected object.")
            sql_id = str(payload.get("sql_id") or "").strip()
            if not sql_id:
                raise ValueError(f"Missing sql_id on line {line_number} of {path}.")
            database_id = str(payload.get("database_id") or "").strip()
            if not database_id:
                database_id, separator, suffix = sql_id.rpartition("_")
                if not separator:
                    raise ValueError(
                        f"Missing database_id and unparseable sql_id on line {line_number} of {path}: {sql_id}"
                    )
            else:
                _, separator, suffix = sql_id.rpartition("_")
            if not separator or not suffix.isdigit():
                raise ValueError(f"Invalid sql_id format on line {line_number} of {path}: {sql_id}")
            offsets[database_id] = max(offsets.get(database_id, 0), int(suffix))
    return offsets


def append_sql_query(output_path: str, row: SynthesizedSQLQuery) -> None:
    path = Path(output_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(row.to_dict(), ensure_ascii=False))
        handle.write("\n")


def append_sql_queries(output_path: str, rows: list[SynthesizedSQLQuery]) -> None:
    if not rows:
        return
    path = Path(output_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row.to_dict(), ensure_ascii=False))
            handle.write("\n")


def write_sql_queries(output_path: str, rows: list[SynthesizedSQLQuery]) -> None:
    initialize_sql_output(output_path)
    append_sql_queries(output_path, rows)
