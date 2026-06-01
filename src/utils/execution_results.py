"""Helpers for stable SQL execution-result serialization and comparison."""

from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal
import json
import math
import re
from typing import Any, Iterable


NUMERIC_ABS_TOLERANCE = 1e-6

_WKT_TYPE_RE = re.compile(
    r"\b(POINT|LINESTRING|POLYGON|MULTIPOINT|MULTILINESTRING|MULTIPOLYGON|"
    r"GEOMETRYCOLLECTION|CIRCULARSTRING|COMPOUNDCURVE|CURVEPOLYGON|"
    r"MULTICURVE|MULTISURFACE|POLYHEDRALSURFACE|TIN|TRIANGLE)"
    r"(?:\s+(ZM|Z|M))?\s*\(",
    re.IGNORECASE,
)
_WKT_START_RE = re.compile(
    r"^\s*(?:SRID=\d+;\s*)?"
    r"(?:POINT|LINESTRING|POLYGON|MULTIPOINT|MULTILINESTRING|MULTIPOLYGON|"
    r"GEOMETRYCOLLECTION|CIRCULARSTRING|COMPOUNDCURVE|CURVEPOLYGON|"
    r"MULTICURVE|MULTISURFACE|POLYHEDRALSURFACE|TIN|TRIANGLE)"
    r"(?:\s+(?:ZM|Z|M))?\s*(?:\(|EMPTY\b)",
    re.IGNORECASE,
)
_HEX_RE = re.compile(r"^[0-9A-Fa-f]+$")
_WKB_BASE_GEOMETRY_TYPES = {1, 2, 3, 4, 5, 6, 7}


def normalize_execution_value(value: Any) -> Any:
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, str):
        return _normalize_geometry_text(value)
    if isinstance(value, memoryview):
        return _normalize_geometry_text(value.tobytes().hex())
    if isinstance(value, bytes):
        return _normalize_geometry_text(value.hex())
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


def _normalize_geometry_text(value: str) -> str:
    wkt_from_hex = _wkb_hex_to_wkt(value)
    if wkt_from_hex is not None:
        return _normalize_wkt_text(wkt_from_hex)
    return _normalize_wkt_text(value)


def _normalize_wkt_text(value: str) -> str:
    if not _WKT_START_RE.match(value):
        return value

    stripped = value.strip()

    def replace_type(match: re.Match[str]) -> str:
        geometry_type = match.group(1).upper()
        dimension = match.group(2)
        if dimension:
            return f"{geometry_type} {dimension.upper()}("
        return f"{geometry_type}("

    normalized = _WKT_TYPE_RE.sub(replace_type, stripped)
    normalized = re.sub(r"\s*,\s*", ",", normalized)
    normalized = re.sub(r"\(\s+", "(", normalized)
    normalized = re.sub(r"\s+\)", ")", normalized)
    normalized = re.sub(r"[ \t]+", " ", normalized)
    return normalized


def _wkb_hex_to_wkt(value: str) -> str | None:
    stripped = value.strip()
    if not _looks_like_wkb_hex(stripped):
        return None

    try:
        from shapely import to_wkt, wkb
    except Exception:
        return None

    try:
        geometry = wkb.loads(stripped, hex=True)
        return to_wkt(geometry, trim=True, rounding_precision=-1)
    except Exception:
        return None


def _looks_like_wkb_hex(value: str) -> bool:
    if len(value) < 18 or len(value) % 2 != 0 or not _HEX_RE.match(value):
        return False

    byte_order = value[:2]
    if byte_order not in {"00", "01"}:
        return False

    type_bytes = bytes.fromhex(value[2:10])
    endian = "little" if byte_order == "01" else "big"
    type_word = int.from_bytes(type_bytes, endian)
    type_without_ewkb_flags = type_word & 0x0FFFFFFF
    base_type = type_without_ewkb_flags % 1000
    return base_type in _WKB_BASE_GEOMETRY_TYPES


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
    numeric_abs_tol: float = NUMERIC_ABS_TOLERANCE,
) -> tuple[bool, dict[str, Any] | None]:
    left_normalized = normalize_result_rows(left_rows)
    right_normalized = normalize_result_rows(right_rows)

    if left_normalized == right_normalized:
        return True, None

    left_exact_counts = _build_row_counter(left_normalized)
    right_exact_counts = _build_row_counter(right_normalized)
    if left_exact_counts == right_exact_counts:
        return True, None

    only_in_left, only_in_right = _unmatched_rows_by_tolerance(
        left_normalized,
        right_normalized,
        numeric_abs_tol=numeric_abs_tol,
    )
    if not only_in_left and not only_in_right:
        return True, None

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


def _unmatched_rows_by_tolerance(
    left_rows: list[Any],
    right_rows: list[Any],
    *,
    numeric_abs_tol: float,
) -> tuple[list[Any], list[Any]]:
    right_by_shape: dict[str, list[int]] = {}
    right_matched = [False] * len(right_rows)
    for index, row in enumerate(right_rows):
        right_by_shape.setdefault(_tolerant_shape_key(row), []).append(index)

    only_in_left: list[Any] = []
    for left_row in left_rows:
        shape_key = _tolerant_shape_key(left_row)
        match_index = None
        for right_index in right_by_shape.get(shape_key, []):
            if right_matched[right_index]:
                continue
            if _values_match_with_tolerance(
                left_row,
                right_rows[right_index],
                numeric_abs_tol=numeric_abs_tol,
            ):
                match_index = right_index
                break
        if match_index is None:
            only_in_left.append(left_row)
        else:
            right_matched[match_index] = True

    only_in_right = [
        row for index, row in enumerate(right_rows) if not right_matched[index]
    ]
    return only_in_left, only_in_right


def _values_match_with_tolerance(
    left: Any,
    right: Any,
    *,
    numeric_abs_tol: float,
) -> bool:
    if _is_numeric_scalar(left) and _is_numeric_scalar(right):
        return math.isclose(
            float(left),
            float(right),
            rel_tol=0.0,
            abs_tol=numeric_abs_tol,
        )
    if isinstance(left, list) and isinstance(right, list):
        if len(left) != len(right):
            return False
        return all(
            _values_match_with_tolerance(
                left_item,
                right_item,
                numeric_abs_tol=numeric_abs_tol,
            )
            for left_item, right_item in zip(left, right)
        )
    if isinstance(left, dict) and isinstance(right, dict):
        if left.keys() != right.keys():
            return False
        return all(
            _values_match_with_tolerance(
                left[key],
                right[key],
                numeric_abs_tol=numeric_abs_tol,
            )
            for key in left
        )
    return left == right


def _tolerant_shape_key(value: Any) -> str:
    return _stable_json_key(_tolerant_shape_value(value))


def _tolerant_shape_value(value: Any) -> Any:
    if _is_numeric_scalar(value):
        return "<number>"
    if isinstance(value, list):
        return [_tolerant_shape_value(item) for item in value]
    if isinstance(value, dict):
        return {
            key: _tolerant_shape_value(item)
            for key, item in sorted(value.items(), key=lambda pair: str(pair[0]))
        }
    return value


def _is_numeric_scalar(value: Any) -> bool:
    return isinstance(value, (int, float)) and not isinstance(value, bool)



def _stable_json_key(value: Any) -> str:
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    )
