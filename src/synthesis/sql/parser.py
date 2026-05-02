"""Robust parsing for LLM SQL synthesis responses."""

from __future__ import annotations

import json
import re
from typing import Any, Mapping

from src.synthesis.database.utils import stable_jsonify, to_text

from .models import SQLGenerationCandidate


CODE_BLOCK_PATTERN = re.compile(r"```(?:json)?\s*(\{.*?\})\s*```", re.I | re.S)


def _extract_json_object(text: str) -> str:
    code_match = CODE_BLOCK_PATTERN.search(text)
    if code_match:
        return code_match.group(1).strip()
    start = text.find("{")
    if start == -1:
        raise ValueError("No JSON object found in model response.")
    depth = 0
    in_string = False
    escape = False
    for index in range(start, len(text)):
        char = text[index]
        if in_string:
            if escape:
                escape = False
            elif char == "\\":
                escape = True
            elif char == '"':
                in_string = False
            continue
        if char == '"':
            in_string = True
        elif char == "{":
            depth += 1
        elif char == "}":
            depth -= 1
            if depth == 0:
                return text[start : index + 1]
    raise ValueError("Unterminated JSON object in model response.")


def _to_string_list(value: Any) -> list[str]:
    normalized = stable_jsonify(value)
    if normalized in (None, ""):
        return []
    if isinstance(normalized, str):
        return [normalized]
    if isinstance(normalized, list):
        return [to_text(item) for item in normalized if to_text(item)]
    return [to_text(normalized)] if to_text(normalized) else []


def parse_sql_generation_response(raw_text: str, raw_response: Any = None) -> SQLGenerationCandidate:
    text = to_text(raw_text)
    try:
        json_text = _extract_json_object(text)
        payload = json.loads(json_text)
    except Exception as exc:
        return SQLGenerationCandidate(
            sql="",
            raw_response_text=text,
            raw_response=raw_response,
            parse_error=str(exc),
        )

    if not isinstance(payload, Mapping):
        return SQLGenerationCandidate(
            sql="",
            raw_response_text=text,
            raw_response=raw_response,
            parse_error="Model response JSON is not an object.",
        )

    sql = to_text(payload.get("sql"))
    if not sql:
        return SQLGenerationCandidate(
            sql="",
            raw_response_text=text,
            raw_response=raw_response,
            parse_error="Missing 'sql' field in model response.",
        )

    return SQLGenerationCandidate(
        sql=sql,
        used_tables=_to_string_list(payload.get("used_tables")),
        used_columns=_to_string_list(payload.get("used_columns")),
        used_spatial_functions=_to_string_list(payload.get("used_spatial_functions")),
        reasoning_summary=to_text(payload.get("reasoning_summary")),
        raw_response_text=text,
        raw_response=raw_response if raw_response is not None else stable_jsonify(payload),
    )
