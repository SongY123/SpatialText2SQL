"""I/O helpers for SQL synthesis."""

from __future__ import annotations

import json
from pathlib import Path

from src.synthesis.database.io import load_synthesized_databases
from src.synthesis.database.models import SynthesizedSpatialDatabase

from .models import SynthesizedSQLQuery


def load_input_databases(input_path: str) -> list[SynthesizedSpatialDatabase]:
    return load_synthesized_databases(input_path)


def write_sql_queries(output_path: str, rows: list[SynthesizedSQLQuery]) -> None:
    path = Path(output_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row.to_dict(), ensure_ascii=False))
            handle.write("\n")
