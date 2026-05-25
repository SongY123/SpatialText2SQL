"""I/O helpers for diversity-aware question generation."""

from __future__ import annotations

import json
import os
from pathlib import Path
import tempfile
from typing import Mapping

import fcntl

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


def _locked_file_merge_jsonl(
    output_path: str,
    *,
    new_rows: list[dict],
    sort_key,
) -> None:
    path = Path(output_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    lock_path = path.with_name(f"{path.name}.lock")
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    with lock_path.open("a+", encoding="utf-8") as lock_handle:
        fcntl.flock(lock_handle.fileno(), fcntl.LOCK_EX)
        try:
            existing_rows: list[dict] = []
            if path.is_file():
                with path.open("r", encoding="utf-8") as handle:
                    for line in handle:
                        stripped = line.strip()
                        if not stripped:
                            continue
                        existing_rows.append(json.loads(stripped))
            merged_rows = existing_rows + list(new_rows)
            merged_rows.sort(key=sort_key)
            fd, temp_path = tempfile.mkstemp(prefix=f"{path.name}.", suffix=".tmp", dir=str(path.parent))
            os.close(fd)
            temp_file = Path(temp_path)
            try:
                with temp_file.open("w", encoding="utf-8") as handle:
                    for row in merged_rows:
                        handle.write(json.dumps(row, ensure_ascii=False))
                        handle.write("\n")
                temp_file.replace(path)
            finally:
                if temp_file.exists():
                    temp_file.unlink()
        finally:
            fcntl.flock(lock_handle.fileno(), fcntl.LOCK_UN)


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


def merge_synthesized_questions_with_lock(output_path: str, rows: list[SynthesizedQuestion]) -> None:
    if not rows:
        return
    _locked_file_merge_jsonl(
        output_path,
        new_rows=[row.to_dict() for row in rows],
        sort_key=lambda item: str(item.get("question_id") or ""),
    )
