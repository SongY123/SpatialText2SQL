"""Helpers for stable SQL execution-result serialization and comparison."""

from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal
import json
from typing import Any, Iterable


def normalize_execution_value(value: Any) -> Any:
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, memoryview):
        return value.tobytes().hex()
    if isinstance(value, bytes):
        return value.hex()
    if isinstance(value, tuple):
        return [normalize_execution_value(item) for item in value]
    if isinstance(value, list):
        return [normalize_execution_value(item) for item in value]
    if isinstance(value, dict):
        return {
            str(key): normalize_execution_value(item)
            for key, item in sorted(value.items(), key=lambda pair: str(pair[0]))
        }
    if isinstance(value, (set, frozenset)):
        normalized_items = [normalize_execution_value(item) for item in value]
        return sorted(normalized_items, key=_stable_json_key)
    if hasattr(value, "item"):
        try:
            scalar = value.item()
        except Exception:
            scalar = None
        else:
            if scalar is not value:
                return normalize_execution_value(scalar)
    return value


def normalize_result_rows(rows: Iterable[Any] | None) -> list[Any]:
    if rows is None:
        return []

    normalized_rows: list[Any] = []
    for row in rows:
        normalized_rows.append(
            normalize_execution_value(list(row) if isinstance(row, tuple) else row)
        )
    return normalized_rows


def compare_result_rows(
    left_rows: Iterable[Any] | None,
    right_rows: Iterable[Any] | None,
    *,
    left_name: str,
    right_name: str,
) -> tuple[bool, dict[str, Any] | None]:
    left_normalized = normalize_result_rows(left_rows)
    right_normalized = normalize_result_rows(right_rows)

    if left_normalized == right_normalized:
        return True, None

    left_exact_counts = _build_row_counter(left_normalized)
    right_exact_counts = _build_row_counter(right_normalized)
    if left_exact_counts == right_exact_counts:
        return True, None

    only_in_left = _expand_counter_difference(left_exact_counts, right_exact_counts)
    only_in_right = _expand_counter_difference(right_exact_counts, left_exact_counts)
    return False, {
        f"only_in_{left_name}": only_in_left or None,
        f"only_in_{right_name}": only_in_right or None,
    }


def _build_row_counter(rows: Iterable[Any]) -> dict[str, list[Any]]:
    counter: dict[str, list[Any]] = {}
    for row in rows:
        key = _stable_json_key(row)
        bucket = counter.setdefault(key, [])
        bucket.append(row)
    return counter


def _expand_counter_difference(
    left_counter: dict[str, list[Any]],
    right_counter: dict[str, list[Any]],
) -> list[Any]:
    diff_rows: list[Any] = []
    for key in sorted(left_counter):
        remaining = len(left_counter[key]) - len(right_counter.get(key, []))
        if remaining > 0:
            diff_rows.extend(left_counter[key][:remaining])
    return diff_rows




def _stable_json_key(value: Any) -> str:
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    )
