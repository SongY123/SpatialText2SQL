"""Lightweight helpers for fine-tuning data preparation."""

from __future__ import annotations

import json
from typing import Any, Mapping, Sequence


def to_text(value: Any) -> str:
    return str(value or "").strip()


def _maybe_parse_json_string(value: str) -> Any:
    stripped = value.strip()
    if not stripped:
        return ""
    if stripped[0] not in "[{":
        return stripped
    try:
        return json.loads(stripped)
    except json.JSONDecodeError:
        return stripped


def stable_jsonify(value: Any) -> Any:
    if isinstance(value, (str, bytes)):
        if isinstance(value, bytes):
            value = value.decode("utf-8", errors="replace")
        parsed = _maybe_parse_json_string(value)
        if parsed is value:
            return value.strip()
        if isinstance(parsed, str):
            return parsed
        return stable_jsonify(parsed)
    if value is None or isinstance(value, (bool, int, float)):
        return value
    if hasattr(value, "item") and callable(getattr(value, "item")):
        try:
            return stable_jsonify(value.item())
        except Exception:
            pass
    if isinstance(value, Mapping):
        return {
            str(key): stable_jsonify(val)
            for key, val in sorted(value.items(), key=lambda item: str(item[0]))
        }
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        return [stable_jsonify(item) for item in value]
    if isinstance(value, set):
        return [stable_jsonify(item) for item in sorted(value, key=lambda item: str(item))]
    return str(value)
