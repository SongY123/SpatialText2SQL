"""Utility helpers for spatial database synthesis."""

from __future__ import annotations

import json
from typing import Any, Iterable, Mapping, Sequence


def to_text(value: Any) -> str:
    """Convert a value into a stripped string."""
    return str(value or "").strip()


def is_missing(value: Any) -> bool:
    if value is None:
        return True
    if isinstance(value, str):
        stripped = value.strip()
        return stripped == "" or stripped.lower() in {"null", "none", "nan", "n/a"}
    return False


def unique_preserve_order(values: Iterable[Any]) -> list[Any]:
    seen: set[str] = set()
    ordered: list[Any] = []
    for value in values:
        marker = stable_json_dumps(value)
        if marker in seen:
            continue
        seen.add(marker)
        ordered.append(value)
    return ordered


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
    """Normalize a JSON-like structure into a stable, serializable form."""
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


def stable_json_dumps(value: Any) -> str:
    return json.dumps(stable_jsonify(value), ensure_ascii=False, sort_keys=True)


def normalize_schema(value: Any) -> list[dict[str, Any]]:
    value = stable_jsonify(value)
    if value in (None, ""):
        return []
    if isinstance(value, list):
        normalized: list[dict[str, Any]] = []
        for item in value:
            if isinstance(item, Mapping):
                normalized.append(
                    {
                        str(key): stable_jsonify(val)
                        for key, val in sorted(item.items(), key=lambda it: str(it[0]))
                        if str(key) != "nullable"
                    }
                )
            else:
                normalized.append({"name": to_text(item)})
        return normalized
    if isinstance(value, Mapping):
        if "columns" in value and isinstance(value["columns"], list):
            return normalize_schema(value["columns"])
        return [{"name": str(key), "value": stable_jsonify(val)} for key, val in sorted(value.items(), key=lambda item: str(item[0]))]
    return [{"name": to_text(value)}]


def normalize_representative_values(value: Any) -> dict[str, Any]:
    value = stable_jsonify(value)
    if value in (None, ""):
        return {}
    if isinstance(value, Mapping):
        return {str(key): stable_jsonify(val) for key, val in sorted(value.items(), key=lambda item: str(item[0]))}
    if isinstance(value, list):
        return {"values": [stable_jsonify(item) for item in value]}
    return {"values": [value]}


def normalize_themes(value: Any) -> list[str]:
    value = stable_jsonify(value)
    if value in (None, ""):
        return []
    if isinstance(value, str):
        if "," in value:
            return unique_preserve_order([part.strip() for part in value.split(",") if part.strip()])
        return [value]
    if isinstance(value, Mapping):
        truthy_keys = [str(key) for key, val in sorted(value.items(), key=lambda item: str(item[0])) if bool(val)]
        if truthy_keys:
            return truthy_keys
        return [str(key) for key in sorted(value.keys(), key=lambda key: str(key))]
    if isinstance(value, list):
        flattened: list[str] = []
        for item in value:
            if isinstance(item, Mapping):
                if "name" in item:
                    flattened.append(to_text(item.get("name")))
                else:
                    flattened.extend(str(key) for key in sorted(item.keys(), key=lambda key: str(key)))
            elif not is_missing(item):
                flattened.append(to_text(item))
        return [item for item in unique_preserve_order(flattened) if item]
    return [to_text(value)] if to_text(value) else []


def normalize_spatial_fields(value: Any) -> list[dict[str, Any]]:
    value = stable_jsonify(value)
    if value in (None, ""):
        return []
    if isinstance(value, Mapping):
        value = [value]
    if not isinstance(value, list):
        return [{"canonical_name": to_text(value), "crs": None}]
    normalized: list[dict[str, Any]] = []
    for item in value:
        if isinstance(item, Mapping):
            canonical_name = to_text(
                item.get("canonical_name")
                or item.get("field_name")
                or item.get("name")
            )
            if not canonical_name:
                continue
            normalized.append(
                {
                    "canonical_name": canonical_name,
                    "crs": None if is_missing(item.get("crs")) else to_text(item.get("crs")),
                }
            )
        elif not is_missing(item):
            normalized.append({"canonical_name": to_text(item), "crs": None})
    return normalized
