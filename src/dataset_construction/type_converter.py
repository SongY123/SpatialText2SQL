"""Shared raw-type normalization for dataset canonicalization and PostGIS synthesis."""

from __future__ import annotations

from dataclasses import dataclass
import re
from typing import Any


CANONICAL_TYPES = {
    "integer",
    "double",
    "text",
    "date",
    "time",
    "timestamp",
    "boolean",
    "spatial",
    "unk",
}


@dataclass(frozen=True)
class RawTypeRule:
    canonical_type: str
    aliases: tuple[str, ...]
    prefer_value_refinement: bool = False


RAW_TYPE_RULES: tuple[RawTypeRule, ...] = (
    RawTypeRule("boolean", ("bool", "boolean", "checkbox")),
    RawTypeRule("timestamp", ("timestamp", "datetime", "date_time", "esrifieldtypedate")),
    RawTypeRule("date", ("calendar_date", "date")),
    RawTypeRule("time", ("time",)),
    RawTypeRule("integer", ("int", "integer", "smallint", "bigint", "long", "short", "oid", "esrifieldtypeinteger", "esrifieldtypesmallinteger", "esrifieldtypeoid")),
    RawTypeRule("double", ("esrifieldtypedouble", "float", "decimal", "numeric", "real")),
    RawTypeRule("double", ("number",), prefer_value_refinement=True),
    RawTypeRule("spatial", ("geometry", "geography", "point", "multipoint", "line", "linestring", "polyline", "multiline", "multilinestring", "polygon", "multipolygon", "geojson", "location", "esrigeometrypoint", "esrigeometrypolygon", "esrigeometrypolyline")),
    RawTypeRule("text", ("text", "string", "varchar", "char", "esrifieldtypestring"), prefer_value_refinement=True),
    RawTypeRule("text", ("url", "photo", "esrifieldtypeglobalid", "esrifieldtypeblob")),
)


def normalize_raw_column_type(raw_type: Any) -> str:
    normalized = re.sub(r"[^a-z0-9_ ]+", "", str(raw_type or "").strip().lower())
    return re.sub(r"[ _]+", "_", normalized).strip("_")


def raw_column_type_rule(raw_type: Any) -> RawTypeRule | None:
    normalized = normalize_raw_column_type(raw_type)
    if not normalized:
        return None
    for rule in RAW_TYPE_RULES:
        if normalized in rule.aliases:
            return rule
    for rule in RAW_TYPE_RULES:
        if any(len(alias) >= 5 and alias in normalized for alias in rule.aliases):
            return rule
    return None


def raw_column_type_to_canonical(raw_type: Any) -> str:
    rule = raw_column_type_rule(raw_type)
    if rule is None:
        return "unk"
    return rule.canonical_type


def raw_column_type_prefers_value_refinement(raw_type: Any) -> bool:
    rule = raw_column_type_rule(raw_type)
    return bool(rule.prefer_value_refinement) if rule is not None else False


def canonical_type_to_postgres_type(canonical_type: Any, srid: int | None = None) -> str:
    canonical = str(canonical_type or "").strip().lower() or "text"
    if canonical == "integer":
        return "BIGINT"
    if canonical == "double":
        return "DOUBLE PRECISION"
    if canonical == "date":
        return "DATE"
    if canonical == "time":
        return "TIME"
    if canonical == "timestamp":
        return "TIMESTAMP"
    if canonical == "boolean":
        return "BOOLEAN"
    if canonical == "spatial":
        if srid is not None:
            return f"geometry(GEOMETRY,{srid})"
        return "geometry"
    return "TEXT"
