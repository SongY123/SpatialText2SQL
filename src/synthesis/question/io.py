"""I/O helpers for diversity-aware question generation."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Mapping

from src.synthesis.database.io import load_synthesized_databases
from src.synthesis.database.models import SynthesizedSpatialDatabase

from .models import QuestionGenerationContext, SQLQuestionSource, SynthesizedQuestion


def load_sql_question_sources(input_path: str) -> list[SQLQuestionSource]:
    path = Path(input_path)
    if not path.is_file():
        raise FileNotFoundError(f"SQL input JSONL file not found: {path}")
    rows: list[SQLQuestionSource] = []
    with path.open("r", encoding="utf-8") as handle:
        for line_number, line in enumerate(handle, start=1):
            stripped = line.strip()
            if not stripped:
                continue
            try:
                payload = json.loads(stripped)
            except json.JSONDecodeError as exc:
                raise ValueError(f"Invalid JSON on line {line_number} of {path}: {exc}") from exc
            try:
                rows.append(SQLQuestionSource.from_dict(payload))
            except ValueError as exc:
                raise ValueError(f"Invalid SQL query row on line {line_number} of {path}: {exc}") from exc
    return rows


def load_question_generation_contexts(
    database_context_path: str,
) -> dict[str, QuestionGenerationContext]:
    databases: list[SynthesizedSpatialDatabase] = load_synthesized_databases(database_context_path)
    return {
        database.database_id: QuestionGenerationContext.from_database(database)
        for database in databases
    }


def build_question_generation_contexts_from_sql_sources(
    sql_sources: list[SQLQuestionSource],
) -> dict[str, QuestionGenerationContext]:
    contexts: dict[str, QuestionGenerationContext] = {}
    for row in sql_sources:
        if row.database_id in contexts:
            continue
        context = QuestionGenerationContext.from_sql_metadata(
            row.metadata,
            database_id=row.database_id,
            city=row.city,
        )
        if context is not None:
            contexts[row.database_id] = context
    return contexts


def initialize_question_output(output_path: str) -> None:
    path = Path(output_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("", encoding="utf-8")


def ensure_question_output(output_path: str) -> None:
    path = Path(output_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.touch(exist_ok=True)


def load_existing_question_id_offsets(output_path: str) -> dict[str, int]:
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
                raise ValueError(f"Invalid question synthesis row on line {line_number} of {path}: expected object.")
            question_id = str(payload.get("question_id") or "").strip()
            if not question_id:
                raise ValueError(f"Missing question_id on line {line_number} of {path}.")
            database_id = str(payload.get("database_id") or "").strip()
            if not database_id:
                database_id, separator, suffix = question_id.rpartition("_")
                if not separator:
                    raise ValueError(
                        f"Missing database_id and unparseable question_id on line {line_number} of {path}: {question_id}"
                    )
            else:
                _, separator, suffix = question_id.rpartition("_")
            if not separator or not suffix.isdigit():
                raise ValueError(f"Invalid question_id format on line {line_number} of {path}: {question_id}")
            offsets[database_id] = max(offsets.get(database_id, 0), int(suffix))
    return offsets


def append_synthesized_question(output_path: str, row: SynthesizedQuestion) -> None:
    path = Path(output_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(row.to_dict(), ensure_ascii=False))
        handle.write("\n")


def append_synthesized_questions(output_path: str, rows: list[SynthesizedQuestion]) -> None:
    if not rows:
        return
    path = Path(output_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row.to_dict(), ensure_ascii=False))
            handle.write("\n")


def write_synthesized_questions(output_path: str, rows: list[SynthesizedQuestion]) -> None:
    initialize_question_output(output_path)
    append_synthesized_questions(output_path, rows)
