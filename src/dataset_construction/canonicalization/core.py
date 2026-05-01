"""Canonicalize raw spatial tables into a unified representation."""
from __future__ import annotations

from copy import deepcopy
from dataclasses import dataclass, field
from datetime import date, datetime, time
import json
from pathlib import Path
import re
from typing import Any, Iterable, Mapping, Optional, Sequence


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

SQL_RESERVED_WORDS = {
    "all",
    "and",
    "any",
    "as",
    "asc",
    "between",
    "by",
    "case",
    "check",
    "column",
    "create",
    "date",
    "default",
    "delete",
    "desc",
    "distinct",
    "drop",
    "else",
    "end",
    "exists",
    "false",
    "from",
    "full",
    "group",
    "having",
    "in",
    "index",
    "inner",
    "insert",
    "interval",
    "into",
    "is",
    "join",
    "key",
    "left",
    "like",
    "limit",
    "not",
    "null",
    "offset",
    "on",
    "or",
    "order",
    "outer",
    "primary",
    "references",
    "right",
    "select",
    "set",
    "table",
    "then",
    "timestamp",
    "to",
    "true",
    "union",
    "unique",
    "update",
    "user",
    "using",
    "values",
    "view",
    "when",
    "where",
}

LONGITUDE_HINTS = {
    "lon",
    "long",
    "lng",
    "longitude",
    "point_x",
    "xcoord",
    "x_coordinate",
    "x_coord",
}

LATITUDE_HINTS = {
    "lat",
    "latitude",
    "point_y",
    "ycoord",
    "y_coordinate",
    "y_coord",
}

GEOMETRY_NAME_HINTS = (
    "geom",
    "geometry",
    "the_geom",
    "shape",
    "wkt",
    "location",
    "coordinate",
    "coordinates",
    "point",
    "polygon",
    "line",
)

GEOMETRY_TYPE_HINTS = (
    "geometry",
    "geography",
    "point",
    "multipoint",
    "linestring",
    "multilinestring",
    "polygon",
    "multipolygon",
    "geojson",
)

WKT_VALUE_PATTERN = re.compile(
    r"^\s*(?:SRID=\d+;\s*)?"
    r"(?:POINT|MULTIPOINT|LINESTRING|MULTILINESTRING|POLYGON|MULTIPOLYGON|GEOMETRYCOLLECTION)"
    r"(?:\s+Z?M?)?\s*(?:EMPTY|\(.*\))\s*$",
    re.IGNORECASE,
)
ISO_DATE_PATTERN = re.compile(r"^\d{4}-\d{2}-\d{2}$")
ISO_TIME_PATTERN = re.compile(r"^\d{2}:\d{2}:\d{2}(?:\.\d+)?$")
ISO_TIMESTAMP_PATTERN = re.compile(
    r"^\d{4}-\d{2}-\d{2}[T\s]\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:\d{2})?$"
)
NUMERIC_PATTERN = re.compile(r"^[+-]?(?:\d+\.?\d*|\.\d+)$")
INTEGER_PATTERN = re.compile(r"^[+-]?\d+$")
PAIR_STRING_PATTERN = re.compile(
    r"^\s*[\[(]?\s*([+-]?(?:\d+\.?\d*|\.\d+))\s*[,; ]\s*([+-]?(?:\d+\.?\d*|\.\d+))\s*[\])]?\s*$"
)
POINT_WKT_PATTERN = re.compile(
    r"^\s*(?:SRID=\d+;\s*)?POINT(?:\s+Z?M?)?\s*\(\s*([+-]?(?:\d+\.?\d*|\.\d+))\s+([+-]?(?:\d+\.?\d*|\.\d+))",
    re.IGNORECASE,
)

UN_GGIM_THEMES: tuple[str, ...] = (
    "Global Geodetic Reference Framework",
    "Addresses",
    "Buildings and Settlements",
    "Elevation and Depth",
    "Functional Areas",
    "Geographical Names",
    "Geology and Soils",
    "Land Cover and Land Use",
    "Land Parcels",
    "Orthoimagery",
    "Physical Infrastructure",
    "Population Distribution",
    "Transport Networks",
    "Water",
)

THEME_KEYWORDS: dict[str, tuple[str, ...]] = {
    "Global Geodetic Reference Framework": (
        "geodetic",
        "benchmark",
        "datum",
        "survey control",
        "reference frame",
    ),
    "Addresses": (
        "address",
        "street address",
        "house number",
        "postal",
        "zip code",
    ),
    "Buildings and Settlements": (
        "building",
        "buildings",
        "housing",
        "residential",
        "settlement",
        "neighborhood",
        "neighbourhood",
        "block",
    ),
    "Elevation and Depth": (
        "elevation",
        "contour",
        "terrain",
        "slope",
        "depth",
        "bathymetry",
    ),
    "Functional Areas": (
        "park",
        "school",
        "hospital",
        "campus",
        "cemetery",
        "precinct",
        "district",
        "service area",
        "zoning",
        "zone",
        "facility",
    ),
    "Geographical Names": (
        "place name",
        "geographical name",
        "toponym",
        "street name",
        "landmark name",
    ),
    "Geology and Soils": (
        "geology",
        "geologic",
        "soil",
        "lithology",
        "sediment",
    ),
    "Land Cover and Land Use": (
        "land cover",
        "land use",
        "vegetation",
        "forest",
        "open space",
        "zoning",
    ),
    "Land Parcels": (
        "parcel",
        "cadastre",
        "cadastral",
        "lot",
        "property boundary",
        "assessment roll",
    ),
    "Orthoimagery": (
        "imagery",
        "orthophoto",
        "aerial",
        "satellite",
        "raster",
        "photo mosaic",
    ),
    "Physical Infrastructure": (
        "hydrant",
        "sewer",
        "utility",
        "pipe",
        "electric",
        "telecom",
        "substation",
        "manhole",
        "streetlight",
    ),
    "Population Distribution": (
        "population",
        "demographic",
        "census",
        "household",
        "race",
        "ethnicity",
        "income",
    ),
    "Transport Networks": (
        "road",
        "street",
        "transport",
        "transit",
        "rail",
        "bus",
        "route",
        "station",
        "highway",
        "bridge",
        "lane",
    ),
    "Water": (
        "water",
        "river",
        "stream",
        "lake",
        "shoreline",
        "coast",
        "harbor",
        "harbour",
        "flood",
        "drainage",
    ),
}

SUBJECT_PHRASES: tuple[tuple[tuple[str, ...], str], ...] = (
    (("hydrant", "sewer", "utility", "manhole", "substation"), "physical infrastructure assets"),
    (("crime", "incident", "accident", "complaint", "violation"), "events or incidents"),
    (("road", "street", "route", "transit", "rail", "station"), "transport network features"),
    (("parcel", "lot", "cadastral", "property"), "land parcel records"),
    (("building", "housing", "residential", "structure"), "buildings or settlement features"),
    (("park", "school", "hospital", "district", "zoning", "facility"), "functional areas or facilities"),
    (("population", "census", "demographic", "household"), "population or demographic patterns"),
    (("river", "water", "lake", "shoreline", "flood"), "water-related features"),
    (("contour", "elevation", "terrain", "depth"), "elevation or depth information"),
    (("address", "postal", "zip"), "address records"),
)

RAW_TYPE_HINTS: dict[str, tuple[str, ...]] = {
    "integer": ("int", "integer", "smallint", "bigint", "long", "short", "oid"),
    "double": ("double", "float", "number", "numeric", "decimal", "real"),
    "text": ("text", "string", "varchar", "char"),
    "date": ("date",),
    "time": ("time",),
    "timestamp": ("timestamp", "datetime", "date_time"),
    "boolean": ("bool", "boolean"),
    "spatial": GEOMETRY_TYPE_HINTS,
}


@dataclass
class CanonicalColumn:
    raw_name: str
    canonical_name: str
    type: str
    nullable: bool

    def to_dict(self) -> dict[str, Any]:
        return {
            "raw_name": self.raw_name,
            "canonical_name": self.canonical_name,
            "type": self.type,
            "nullable": self.nullable,
        }


@dataclass
class SpatialField:
    field_name: str
    crs: Optional[str]
    source_kind: str
    raw_columns: tuple[str, ...] = field(default_factory=tuple)
    match_count: int = 0
    valid_count: int = 0
    fingerprints: tuple[str, ...] = field(default_factory=tuple)

    def to_public_dict(self) -> dict[str, Any]:
        return {
            "field_name": self.field_name,
            "crs": self.crs,
        }


@dataclass
class CanonicalTable:
    table_name: str
    source_city: str
    raw_to_canonical_columns: dict[str, str]
    schema: list[CanonicalColumn]
    spatial_fields: list[SpatialField]
    semantic_summary: str
    themes: list[str]

    def to_dict(self) -> dict[str, Any]:
        return {
            "table_name": self.table_name,
            "source_city": self.source_city,
            "schema": [column.to_dict() for column in self.schema],
            "spatial_fields": [field.to_public_dict() for field in self.spatial_fields],
            "semantic_summary": self.semantic_summary,
            "themes": list(self.themes),
        }


def _is_missing(value: Any) -> bool:
    if value is None:
        return True
    if isinstance(value, str):
        stripped = value.strip()
        return stripped == "" or stripped.lower() in {"null", "none", "nan", "n/a"}
    return False


def _normalize_text(value: Any) -> str:
    return re.sub(r"\s+", " ", str(value or "")).strip()


def _normalize_crs_value(value: Any) -> Optional[str]:
    if value is None:
        return None
    if isinstance(value, str):
        stripped = value.strip()
        return stripped or None
    if isinstance(value, int):
        return f"EPSG:{value}"
    if isinstance(value, Mapping):
        properties = value.get("properties")
        if isinstance(properties, Mapping):
            for key in ("name", "code", "href"):
                if properties.get(key):
                    return _normalize_text(properties.get(key))
        for key in ("name", "code"):
            if value.get(key):
                return _normalize_text(value.get(key))
    return _normalize_text(value) or None


def _safe_mapping(value: Any) -> dict[str, Any]:
    if isinstance(value, Mapping):
        return dict(value)
    return {}


def _sample_rows(rows: Any, max_rows_for_inference: int) -> list[dict[str, Any]]:
    if not isinstance(rows, list):
        return []
    sampled: list[dict[str, Any]] = []
    for row in rows[:max_rows_for_inference]:
        if isinstance(row, Mapping):
            sampled.append(dict(row))
    return sampled


def _normalize_name_for_matching(name: str) -> str:
    return normalize_column_name(name, existing_names=None, column_index=1)


def normalize_table_name(name: Any) -> str:
    cleaned = re.sub(r"[^A-Za-z0-9 ]+", "", str(name or ""))
    cleaned = re.sub(r"\s+", " ", cleaned).strip().lower()
    return cleaned.replace(" ", "_")


def _geometryish_name(name: str) -> bool:
    normalized = _normalize_name_for_matching(name)
    return any(hint == normalized or hint in normalized for hint in GEOMETRY_NAME_HINTS)


def _geometryish_type(raw_type: Any) -> bool:
    normalized = _normalize_name_for_matching(_normalize_text(raw_type))
    return any(hint == normalized or hint in normalized for hint in GEOMETRY_TYPE_HINTS)


def _ordered_raw_columns(
    raw_schema: Any,
    rows: list[dict[str, Any]],
    geojson_field_name: Optional[str] = None,
) -> list[dict[str, str]]:
    columns: list[dict[str, str]] = []
    seen: set[str] = set()

    if isinstance(raw_schema, list):
        for column in raw_schema:
            if not isinstance(column, Mapping):
                continue
            raw_name = _normalize_text(column.get("name"))
            if not raw_name or raw_name in seen:
                continue
            seen.add(raw_name)
            columns.append(
                {
                    "name": raw_name,
                    "type": _normalize_text(column.get("type")),
                }
            )

    for row in rows:
        for raw_name in row.keys():
            key = _normalize_text(raw_name)
            if not key or key in seen:
                continue
            seen.add(key)
            columns.append({"name": key, "type": ""})

    if geojson_field_name:
        key = _normalize_text(geojson_field_name)
        if key and key not in seen:
            columns.append({"name": key, "type": "geometry"})

    return columns


def _build_property_alias_map(raw_schema: Any) -> dict[str, str]:
    alias_map: dict[str, str] = {}
    if not isinstance(raw_schema, list):
        return alias_map
    for column in raw_schema:
        if not isinstance(column, Mapping):
            continue
        raw_name = _normalize_text(column.get("name"))
        if not raw_name:
            continue
        alias_map[_normalize_name_for_matching(raw_name)] = raw_name
    return alias_map


def _extract_description_sentence(value: str, max_length: int = 180) -> str:
    normalized = _normalize_text(value)
    if not normalized:
        return ""
    first_sentence = re.split(r"(?<=[.!?])\s+", normalized, maxsplit=1)[0]
    if len(first_sentence) <= max_length:
        return first_sentence
    return first_sentence[: max_length - 1].rstrip() + "…"


def _format_number(value: float, decimals: int = 5) -> str:
    text = f"{float(value):.{decimals}f}"
    text = text.rstrip("0").rstrip(".")
    return text or "0"


def _to_float(value: Any) -> Optional[float]:
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        stripped = value.strip()
        if not stripped or not NUMERIC_PATTERN.match(stripped):
            return None
        try:
            return float(stripped)
        except ValueError:
            return None
    return None


def _to_int(value: Any) -> Optional[int]:
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, str):
        stripped = value.strip()
        if not INTEGER_PATTERN.match(stripped):
            return None
        try:
            return int(stripped)
        except ValueError:
            return None
    return None


def _parse_boolean(value: Any) -> Optional[bool]:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        stripped = value.strip().lower()
        if stripped in {"true", "false", "yes", "no", "y", "n"}:
            return stripped in {"true", "yes", "y"}
    return None


def _parse_temporal(value: Any) -> Optional[str]:
    if isinstance(value, datetime):
        return "timestamp"
    if isinstance(value, date) and not isinstance(value, datetime):
        return "date"
    if isinstance(value, time):
        return "time"
    if not isinstance(value, str):
        return None

    stripped = value.strip()
    if not stripped:
        return None
    if ISO_TIMESTAMP_PATTERN.match(stripped):
        try:
            datetime.fromisoformat(stripped.replace("Z", "+00:00"))
            return "timestamp"
        except ValueError:
            pass
    if ISO_DATE_PATTERN.match(stripped):
        try:
            date.fromisoformat(stripped)
            return "date"
        except ValueError:
            pass
    if ISO_TIME_PATTERN.match(stripped):
        try:
            time.fromisoformat(stripped)
            return "time"
        except ValueError:
            pass
    return None


def _classify_value(value: Any) -> str:
    if _is_missing(value):
        return "missing"
    if isinstance(value, (dict, list, tuple)) and not _extract_coordinate_pair(value):
        return "complex"
    temporal_kind = _parse_temporal(value)
    if temporal_kind:
        return temporal_kind
    if _parse_boolean(value) is not None:
        return "boolean"
    if _to_int(value) is not None:
        return "integer"
    if _to_float(value) is not None:
        return "double"
    if isinstance(value, str):
        return "text"
    return "unk"


def _map_raw_type(raw_type: Any) -> str:
    normalized = _normalize_name_for_matching(_normalize_text(raw_type))
    if not normalized:
        return "unk"
    for canonical_type, hints in RAW_TYPE_HINTS.items():
        if any(normalized == hint or hint in normalized for hint in hints):
            return canonical_type
    return "unk"


def normalize_column_name(
    name: Any,
    existing_names: Optional[set[str]] = None,
    column_index: int = 1,
) -> str:
    base = re.sub(r"[^a-z0-9]+", "_", str(name or "").strip().lower())
    base = re.sub(r"_+", "_", base).strip("_")
    if base and base[0].isdigit():
        base = f"col_{base}"
    if not base:
        base = f"col_{column_index}"
    if base in SQL_RESERVED_WORDS:
        base = f"{base}_col"

    if existing_names is None:
        return base

    candidate = base
    suffix = 2
    while candidate in existing_names:
        candidate = f"{base}_{suffix}"
        suffix += 1
    return candidate


def infer_column_type(
    raw_type: Any,
    values: Sequence[Any],
    *,
    is_spatial: bool = False,
) -> tuple[str, bool]:
    nullable = not values or any(_is_missing(value) for value in values)
    if is_spatial:
        return "spatial", nullable

    non_missing = [value for value in values if not _is_missing(value)]
    raw_type_guess = _map_raw_type(raw_type)

    if not non_missing:
        return (raw_type_guess if raw_type_guess != "spatial" else "unk"), True

    kinds = {_classify_value(value) for value in non_missing}
    kinds.discard("missing")

    if kinds == {"boolean"}:
        return "boolean", nullable
    if kinds == {"integer"}:
        return "integer", nullable
    if kinds <= {"integer", "double"}:
        return ("double" if "double" in kinds else "integer"), nullable
    if kinds == {"date"}:
        return "date", nullable
    if kinds == {"time"}:
        return "time", nullable
    if kinds <= {"date", "timestamp"}:
        return ("timestamp" if "timestamp" in kinds else "date"), nullable
    if kinds == {"timestamp"}:
        return "timestamp", nullable
    if kinds == {"text"}:
        return "text", nullable

    if "text" in kinds:
        return "text", nullable

    if raw_type_guess not in {"unk", "spatial"}:
        return raw_type_guess, nullable

    if "complex" in kinds or "unk" in kinds:
        return "unk", nullable

    return "unk", nullable


def _guess_geojson_field_name(raw_schema: Any, rows: list[dict[str, Any]]) -> str:
    if isinstance(raw_schema, list):
        for column in raw_schema:
            if not isinstance(column, Mapping):
                continue
            raw_name = _normalize_text(column.get("name"))
            if not raw_name:
                continue
            if _geometryish_type(column.get("type")):
                return raw_name
        for column in raw_schema:
            if not isinstance(column, Mapping):
                continue
            raw_name = _normalize_text(column.get("name"))
            if raw_name and _geometryish_name(raw_name):
                return raw_name
    for row in rows:
        geometry = row.get("geometry")
        if isinstance(geometry, Mapping) and geometry.get("type") and geometry.get("coordinates") is not None:
            return "geometry"
    return "geometry"


def _extract_coordinate_pair(value: Any) -> Optional[tuple[float, float]]:
    if isinstance(value, (list, tuple)) and len(value) >= 2:
        x = _to_float(value[0])
        y = _to_float(value[1])
        if x is not None and y is not None:
            return (x, y)

    if isinstance(value, Mapping):
        if {"x", "y"} <= set(value.keys()):
            x = _to_float(value.get("x"))
            y = _to_float(value.get("y"))
            if x is not None and y is not None:
                return (x, y)
        if {"lon", "lat"} <= set(value.keys()):
            x = _to_float(value.get("lon"))
            y = _to_float(value.get("lat"))
            if x is not None and y is not None:
                return (x, y)

    if isinstance(value, str):
        stripped = value.strip()
        if not stripped or WKT_VALUE_PATTERN.match(stripped):
            return None
        match = PAIR_STRING_PATTERN.match(stripped)
        if match:
            x = _to_float(match.group(1))
            y = _to_float(match.group(2))
            if x is not None and y is not None:
                return (x, y)
    return None


def _value_fingerprints(values: Iterable[str]) -> tuple[str, ...]:
    fingerprints: list[str] = []
    seen: set[str] = set()
    for value in values:
        if value and value not in seen:
            fingerprints.append(value)
            seen.add(value)
    return tuple(fingerprints)


def _geometry_fingerprint(geometry: Any) -> Optional[str]:
    if not isinstance(geometry, Mapping):
        return None
    geometry_type = _normalize_text(geometry.get("type")).lower()
    coords = geometry.get("coordinates")
    if geometry_type == "point" and isinstance(coords, (list, tuple)) and len(coords) >= 2:
        x = _to_float(coords[0])
        y = _to_float(coords[1])
        if x is not None and y is not None:
            return f"point:{_format_number(x)}:{_format_number(y)}"
    if coords is not None:
        pair = _extract_first_coordinate_pair(coords)
        if pair is not None:
            return f"{geometry_type}:{_format_number(pair[0])}:{_format_number(pair[1])}"
    return None


def _extract_first_coordinate_pair(value: Any) -> Optional[tuple[float, float]]:
    if isinstance(value, (list, tuple)):
        if len(value) >= 2 and all(not isinstance(item, (list, tuple, Mapping)) for item in value[:2]):
            x = _to_float(value[0])
            y = _to_float(value[1])
            if x is not None and y is not None:
                return (x, y)
        for item in value:
            pair = _extract_first_coordinate_pair(item)
            if pair is not None:
                return pair
    return None


def _wkt_fingerprint(value: Any) -> Optional[str]:
    if not isinstance(value, str):
        return None
    stripped = value.strip()
    if not WKT_VALUE_PATTERN.match(stripped):
        return None
    point_match = POINT_WKT_PATTERN.match(stripped)
    if point_match:
        x = _to_float(point_match.group(1))
        y = _to_float(point_match.group(2))
        if x is not None and y is not None:
            return f"point:{_format_number(x)}:{_format_number(y)}"
    normalized = re.sub(r"\s+", " ", stripped.upper())
    return normalized[:256]


def _coordinate_pair_fingerprint(pair: tuple[float, float]) -> str:
    return f"point:{_format_number(pair[0])}:{_format_number(pair[1])}"


def _coordinate_role(raw_name: str) -> Optional[str]:
    normalized = _normalize_name_for_matching(raw_name)
    tokens = [token for token in normalized.split("_") if token]
    if any(token in LONGITUDE_HINTS for token in tokens) or normalized in LONGITUDE_HINTS:
        return "lon"
    if any(token in LATITUDE_HINTS for token in tokens) or normalized in LATITUDE_HINTS:
        return "lat"
    return None


def _coordinate_base_key(raw_name: str) -> str:
    normalized = _normalize_name_for_matching(raw_name)
    tokens = [token for token in normalized.split("_") if token]
    filtered = [token for token in tokens if token not in LONGITUDE_HINTS and token not in LATITUDE_HINTS]
    return "_".join(filtered)


def detect_geojson_geometry(
    raw_table: Mapping[str, Any],
    raw_to_canonical_columns: Optional[Mapping[str, str]] = None,
    max_rows_for_inference: int = 100,
) -> list[SpatialField]:
    rows = _sample_rows(raw_table.get("rows"), max_rows_for_inference)
    geometries: list[Any] = []
    geojson_geometry = raw_table.get("geojson_geometry")

    if isinstance(geojson_geometry, list):
        geometries = geojson_geometry[:max_rows_for_inference]
    elif geojson_geometry is not None:
        geometries = [geojson_geometry]
    else:
        for row in rows:
            geometry = row.get("geometry")
            if isinstance(geometry, Mapping):
                geometries.append(geometry)

    valid_geometries = [
        geometry
        for geometry in geometries
        if isinstance(geometry, Mapping)
        and geometry.get("type")
        and geometry.get("coordinates") is not None
    ]
    if not valid_geometries:
        return []

    raw_name = _normalize_text(raw_table.get("geojson_geometry_field_name"))
    if not raw_name:
        raw_name = _guess_geojson_field_name(raw_table.get("raw_schema"), rows)

    fingerprints = _value_fingerprints(
        fingerprint
        for fingerprint in (_geometry_fingerprint(geometry) for geometry in valid_geometries)
        if fingerprint
    )
    field_name = raw_to_canonical_columns.get(raw_name, raw_name) if raw_to_canonical_columns else raw_name
    return [
        SpatialField(
            field_name=field_name,
            crs=None,
            source_kind="geojson",
            raw_columns=(raw_name,),
            match_count=len(valid_geometries),
            valid_count=len(valid_geometries),
            fingerprints=fingerprints,
        )
    ]


def detect_wkt_geometry(
    raw_table: Mapping[str, Any],
    raw_to_canonical_columns: Optional[Mapping[str, str]] = None,
    max_rows_for_inference: int = 100,
) -> list[SpatialField]:
    rows = _sample_rows(raw_table.get("rows"), max_rows_for_inference)
    raw_schema = raw_table.get("raw_schema")
    columns = _ordered_raw_columns(raw_schema, rows)
    detections: list[SpatialField] = []

    for column in columns:
        raw_name = column["name"]
        values = [row.get(raw_name) for row in rows]
        non_missing = [value for value in values if not _is_missing(value)]
        if not non_missing:
            continue
        wkt_matches = [value for value in non_missing if isinstance(value, str) and WKT_VALUE_PATTERN.match(value.strip())]
        if not wkt_matches:
            continue
        ratio = len(wkt_matches) / len(non_missing)
        if ratio < 0.8 and len(wkt_matches) != len(non_missing):
            continue
        field_name = raw_to_canonical_columns.get(raw_name, raw_name) if raw_to_canonical_columns else raw_name
        fingerprints = _value_fingerprints(
            fingerprint
            for fingerprint in (_wkt_fingerprint(value) for value in wkt_matches)
            if fingerprint
        )
        detections.append(
            SpatialField(
                field_name=field_name,
                crs=None,
                source_kind="wkt",
                raw_columns=(raw_name,),
                match_count=len(wkt_matches),
                valid_count=len(non_missing),
                fingerprints=fingerprints,
            )
        )

    return detections


def detect_coordinate_pairs(
    raw_table: Mapping[str, Any],
    raw_to_canonical_columns: Optional[Mapping[str, str]] = None,
    max_rows_for_inference: int = 100,
) -> list[SpatialField]:
    rows = _sample_rows(raw_table.get("rows"), max_rows_for_inference)
    raw_schema = raw_table.get("raw_schema")
    columns = _ordered_raw_columns(raw_schema, rows)

    lon_candidates: list[dict[str, Any]] = []
    lat_candidates: list[dict[str, Any]] = []

    for column in columns:
        raw_name = column["name"]
        role = _coordinate_role(raw_name)
        if role is None:
            continue
        values = [_to_float(row.get(raw_name)) for row in rows]
        valid = [value for value in values if value is not None]
        if not valid:
            continue
        if role == "lon" and not all(-180.0 <= value <= 180.0 for value in valid):
            continue
        if role == "lat" and not all(-90.0 <= value <= 90.0 for value in valid):
            continue
        candidate = {
            "raw_name": raw_name,
            "base_key": _coordinate_base_key(raw_name),
            "valid_count": len(valid),
        }
        if role == "lon":
            lon_candidates.append(candidate)
        else:
            lat_candidates.append(candidate)

    pairs: list[SpatialField] = []
    used_lon: set[str] = set()
    used_lat: set[str] = set()

    pair_candidates: list[tuple[int, int, dict[str, Any], dict[str, Any], list[tuple[float, float]]]] = []
    for lon_candidate in lon_candidates:
        for lat_candidate in lat_candidates:
            base_bonus = 1 if lon_candidate["base_key"] and lon_candidate["base_key"] == lat_candidate["base_key"] else 0
            overlap_pairs: list[tuple[float, float]] = []
            for row in rows:
                lon_value = _to_float(row.get(lon_candidate["raw_name"]))
                lat_value = _to_float(row.get(lat_candidate["raw_name"]))
                if lon_value is None or lat_value is None:
                    continue
                if -180.0 <= lon_value <= 180.0 and -90.0 <= lat_value <= 90.0:
                    overlap_pairs.append((lon_value, lat_value))
            if not overlap_pairs:
                continue
            pair_candidates.append(
                (
                    base_bonus,
                    len(overlap_pairs),
                    lon_candidate,
                    lat_candidate,
                    overlap_pairs,
                )
            )

    pair_candidates.sort(key=lambda item: (item[0], item[1]), reverse=True)

    for _base_bonus, overlap, lon_candidate, lat_candidate, overlap_pairs in pair_candidates:
        lon_name = lon_candidate["raw_name"]
        lat_name = lat_candidate["raw_name"]
        if lon_name in used_lon or lat_name in used_lat:
            continue
        used_lon.add(lon_name)
        used_lat.add(lat_name)
        base_key = lon_candidate["base_key"] or lat_candidate["base_key"]
        derived_name = f"{base_key}_geometry" if base_key else "geometry"
        field_name = normalize_column_name(derived_name, existing_names=None, column_index=1)
        fingerprints = _value_fingerprints(
            _coordinate_pair_fingerprint(pair) for pair in overlap_pairs[:max_rows_for_inference]
        )
        pairs.append(
            SpatialField(
                field_name=field_name,
                crs=None,
                source_kind="coordinate_pair",
                raw_columns=(lon_name, lat_name),
                match_count=overlap,
                valid_count=overlap,
                fingerprints=fingerprints,
            )
        )

    return pairs


def detect_single_coordinate_fields(
    raw_table: Mapping[str, Any],
    raw_to_canonical_columns: Optional[Mapping[str, str]] = None,
    max_rows_for_inference: int = 100,
    excluded_columns: Optional[set[str]] = None,
) -> list[SpatialField]:
    rows = _sample_rows(raw_table.get("rows"), max_rows_for_inference)
    raw_schema = raw_table.get("raw_schema")
    columns = _ordered_raw_columns(raw_schema, rows)
    excluded = excluded_columns or set()
    detections: list[SpatialField] = []

    for column in columns:
        raw_name = column["name"]
        if raw_name in excluded:
            continue
        values = [row.get(raw_name) for row in rows]
        non_missing = [value for value in values if not _is_missing(value)]
        if not non_missing:
            continue
        coordinate_pairs = [pair for pair in (_extract_coordinate_pair(value) for value in non_missing) if pair]
        if not coordinate_pairs:
            continue
        ratio = len(coordinate_pairs) / len(non_missing)
        if ratio < 0.8 and len(coordinate_pairs) != len(non_missing):
            continue
        field_name = raw_to_canonical_columns.get(raw_name, raw_name) if raw_to_canonical_columns else raw_name
        fingerprints = _value_fingerprints(
            _coordinate_pair_fingerprint(pair) for pair in coordinate_pairs[:max_rows_for_inference]
        )
        detections.append(
            SpatialField(
                field_name=field_name,
                crs=None,
                source_kind="single_coordinate",
                raw_columns=(raw_name,),
                match_count=len(coordinate_pairs),
                valid_count=len(non_missing),
                fingerprints=fingerprints,
            )
        )

    return detections


def infer_crs(raw_table: Mapping[str, Any], spatial_field: SpatialField) -> Optional[str]:
    explicit_crs = _normalize_crs_value(raw_table.get("crs"))
    if explicit_crs:
        return explicit_crs
    if spatial_field.source_kind == "coordinate_pair":
        return "EPSG:4326"
    return None


def _spatial_priority(spatial_field: SpatialField) -> tuple[int, int, int]:
    priority = {
        "geojson": 4,
        "wkt": 3,
        "coordinate_pair": 2,
        "single_coordinate": 1,
    }.get(spatial_field.source_kind, 0)
    return (priority, spatial_field.match_count, spatial_field.valid_count)


def _spatial_fields_equivalent(first: SpatialField, second: SpatialField) -> bool:
    if set(first.raw_columns) == set(second.raw_columns) and first.raw_columns:
        return True
    if set(first.raw_columns) & set(second.raw_columns) and (
        first.source_kind == second.source_kind
        or "geojson" in {first.source_kind, second.source_kind}
    ):
        return True

    first_fingerprints = set(first.fingerprints)
    second_fingerprints = set(second.fingerprints)
    if first_fingerprints and second_fingerprints:
        overlap = first_fingerprints & second_fingerprints
        smaller = min(len(first_fingerprints), len(second_fingerprints))
        if overlap and len(overlap) >= max(1, smaller // 2):
            return True
    return False


def deduplicate_spatial_fields(spatial_fields: Sequence[SpatialField]) -> list[SpatialField]:
    kept: list[SpatialField] = []

    for candidate in sorted(spatial_fields, key=_spatial_priority, reverse=True):
        duplicate_of: Optional[int] = None
        for index, existing in enumerate(kept):
            if _spatial_fields_equivalent(existing, candidate):
                duplicate_of = index
                break
        if duplicate_of is None:
            kept.append(candidate)
            continue

        existing = kept[duplicate_of]
        if _spatial_priority(candidate) > _spatial_priority(existing):
            kept[duplicate_of] = candidate

    existing_names: set[str] = set()
    deduped: list[SpatialField] = []
    for field_item in kept:
        unique_name = normalize_column_name(
            field_item.field_name,
            existing_names=existing_names,
            column_index=len(existing_names) + 1,
        )
        existing_names.add(unique_name)
        deduped.append(
            SpatialField(
                field_name=unique_name,
                crs=field_item.crs,
                source_kind=field_item.source_kind,
                raw_columns=field_item.raw_columns,
                match_count=field_item.match_count,
                valid_count=field_item.valid_count,
                fingerprints=field_item.fingerprints,
            )
        )
    return deduped


def detect_spatial_fields(
    raw_table: Mapping[str, Any],
    raw_to_canonical_columns: Optional[Mapping[str, str]] = None,
    max_rows_for_inference: int = 100,
) -> list[SpatialField]:
    spatial_fields: list[SpatialField] = []

    geojson_fields = detect_geojson_geometry(
        raw_table,
        raw_to_canonical_columns=raw_to_canonical_columns,
        max_rows_for_inference=max_rows_for_inference,
    )
    spatial_fields.extend(geojson_fields)

    wkt_fields = detect_wkt_geometry(
        raw_table,
        raw_to_canonical_columns=raw_to_canonical_columns,
        max_rows_for_inference=max_rows_for_inference,
    )
    spatial_fields.extend(wkt_fields)

    coordinate_pairs = detect_coordinate_pairs(
        raw_table,
        raw_to_canonical_columns=raw_to_canonical_columns,
        max_rows_for_inference=max_rows_for_inference,
    )
    spatial_fields.extend(coordinate_pairs)

    excluded_single_coordinate_columns = {
        raw_name
        for field_item in geojson_fields + wkt_fields + coordinate_pairs
        for raw_name in field_item.raw_columns
    }
    spatial_fields.extend(
        detect_single_coordinate_fields(
            raw_table,
            raw_to_canonical_columns=raw_to_canonical_columns,
            max_rows_for_inference=max_rows_for_inference,
            excluded_columns=excluded_single_coordinate_columns,
        )
    )

    with_crs = [
        SpatialField(
            field_name=field_item.field_name,
            crs=infer_crs(raw_table, field_item),
            source_kind=field_item.source_kind,
            raw_columns=field_item.raw_columns,
            match_count=field_item.match_count,
            valid_count=field_item.valid_count,
            fingerprints=field_item.fingerprints,
        )
        for field_item in spatial_fields
    ]
    return deduplicate_spatial_fields(with_crs)


def normalize_schema(
    raw_schema: Any,
    rows: Any,
    *,
    geojson_field_name: Optional[str] = None,
    geojson_geometry: Any = None,
    spatial_fields: Optional[Sequence[SpatialField]] = None,
    max_rows_for_inference: int = 100,
) -> tuple[dict[str, str], list[CanonicalColumn]]:
    sampled_rows = _sample_rows(rows, max_rows_for_inference)
    ordered_columns = _ordered_raw_columns(raw_schema, sampled_rows, geojson_field_name=geojson_field_name)

    raw_to_canonical: dict[str, str] = {}
    existing_names: set[str] = set()
    for index, column in enumerate(ordered_columns, start=1):
        canonical_name = normalize_column_name(
            column["name"],
            existing_names=existing_names,
            column_index=index,
        )
        existing_names.add(canonical_name)
        raw_to_canonical[column["name"]] = canonical_name

    complete_spatial_raw_columns = {
        raw_name
        for field_item in (spatial_fields or [])
        if field_item.source_kind in {"geojson", "wkt", "single_coordinate"}
        for raw_name in field_item.raw_columns
    }

    schema: list[CanonicalColumn] = []
    total_rows = len(sampled_rows)
    for column in ordered_columns:
        raw_name = column["name"]
        if raw_name == geojson_field_name:
            if isinstance(geojson_geometry, list):
                values = geojson_geometry[:total_rows] or geojson_geometry
            elif geojson_geometry is not None:
                values = [geojson_geometry]
            else:
                values = [row.get(raw_name) for row in sampled_rows]
        else:
            values = [row.get(raw_name) for row in sampled_rows]
        inferred_type, nullable = infer_column_type(
            column.get("type"),
            values,
            is_spatial=raw_name in complete_spatial_raw_columns,
        )
        schema.append(
            CanonicalColumn(
                raw_name=raw_name,
                canonical_name=raw_to_canonical[raw_name],
                type=inferred_type,
                nullable=nullable or total_rows == 0,
            )
        )

    return raw_to_canonical, schema


def _schema_values_for_column(
    raw_name: str,
    sampled_rows: list[dict[str, Any]],
    raw_table: Mapping[str, Any],
    geojson_field_name: Optional[str],
) -> list[Any]:
    if raw_name == geojson_field_name:
        geojson_geometry = raw_table.get("geojson_geometry")
        if isinstance(geojson_geometry, list):
            return geojson_geometry[: len(sampled_rows)] or geojson_geometry
        if geojson_geometry is not None:
            return [geojson_geometry]
    return [row.get(raw_name) for row in sampled_rows]


def _keywords_blob(
    table_name: str,
    source_description: str,
    schema: Sequence[CanonicalColumn],
    semantic_summary: str,
) -> str:
    schema_blob = " ".join(column.canonical_name for column in schema)
    return " ".join(
        part
        for part in (
            _normalize_text(table_name).lower(),
            _normalize_text(source_description).lower(),
            schema_blob.lower(),
            _normalize_text(semantic_summary).lower(),
        )
        if part
    )


def _infer_subject_phrase(table_name: str, source_description: str, schema: Sequence[CanonicalColumn]) -> str:
    text_blob = _keywords_blob(table_name, source_description, schema, "")
    for keywords, phrase in SUBJECT_PHRASES:
        if any(keyword in text_blob for keyword in keywords):
            return phrase
    return "spatially referenced records"


def generate_semantic_summary(
    table_name: str,
    source_city: str,
    source_description: str,
    schema: Sequence[CanonicalColumn],
    spatial_fields: Sequence[SpatialField],
) -> str:
    subject_phrase = _infer_subject_phrase(table_name, source_description, schema)
    description_sentence = _extract_description_sentence(source_description)
    spatial_clause = (
        f" It has {len(spatial_fields)} detected spatial field(s)."
        if spatial_fields
        else " No explicit spatial field was detected from the sampled rows."
    )
    summary = (
        f"{table_name} appears to describe {subject_phrase} in {source_city}. "
        f"The canonical schema contains {len(schema)} field(s).{spatial_clause}"
    )
    if description_sentence:
        summary += f" Source context: {description_sentence}"
    return summary.strip()


def assign_thematic_labels(
    table_name: str,
    source_description: str,
    schema: Sequence[CanonicalColumn],
    semantic_summary: str,
    spatial_fields: Sequence[SpatialField],
) -> list[str]:
    del spatial_fields  # Spatial presence is reflected through schema and summary.

    text_blob = _keywords_blob(table_name, source_description, schema, semantic_summary)
    labels: list[str] = []
    for theme in UN_GGIM_THEMES:
        keywords = THEME_KEYWORDS.get(theme, ())
        if any(keyword in text_blob for keyword in keywords):
            labels.append(theme)
    return labels


def _canonical_table_from_raw_table(
    raw_table: Mapping[str, Any],
    max_rows_for_inference: int = 100,
) -> CanonicalTable:
    table_name = _normalize_text(raw_table.get("table_name")) or "unknown_table"
    source_city = _normalize_text(raw_table.get("source_city")) or "unknown"
    source_description = _normalize_text(raw_table.get("source_description"))
    sampled_rows = _sample_rows(raw_table.get("rows"), max_rows_for_inference)
    geojson_field_name = _normalize_text(raw_table.get("geojson_geometry_field_name"))
    if not geojson_field_name and raw_table.get("geojson_geometry") is not None:
        geojson_field_name = _guess_geojson_field_name(raw_table.get("raw_schema"), sampled_rows)

    ordered_columns = _ordered_raw_columns(
        raw_table.get("raw_schema"),
        sampled_rows,
        geojson_field_name=geojson_field_name or None,
    )
    raw_to_canonical: dict[str, str] = {}
    existing_names: set[str] = set()
    for index, column in enumerate(ordered_columns, start=1):
        canonical_name = normalize_column_name(
            column["name"],
            existing_names=existing_names,
            column_index=index,
        )
        existing_names.add(canonical_name)
        raw_to_canonical[column["name"]] = canonical_name

    spatial_fields = detect_spatial_fields(
        raw_table,
        raw_to_canonical_columns=raw_to_canonical,
        max_rows_for_inference=max_rows_for_inference,
    )

    complete_spatial_raw_columns = {
        raw_name
        for field_item in spatial_fields
        if field_item.source_kind in {"geojson", "wkt", "single_coordinate"}
        for raw_name in field_item.raw_columns
    }

    schema: list[CanonicalColumn] = []
    for column in ordered_columns:
        raw_name = column["name"]
        values = _schema_values_for_column(raw_name, sampled_rows, raw_table, geojson_field_name or None)
        inferred_type, nullable = infer_column_type(
            column.get("type"),
            values,
            is_spatial=raw_name in complete_spatial_raw_columns,
        )
        if not values and not sampled_rows:
            nullable = True
        schema.append(
            CanonicalColumn(
                raw_name=raw_name,
                canonical_name=raw_to_canonical[raw_name],
                type=inferred_type,
                nullable=nullable,
            )
        )

    semantic_summary = generate_semantic_summary(
        table_name,
        source_city,
        source_description,
        schema,
        spatial_fields,
    )
    themes = assign_thematic_labels(
        table_name,
        source_description,
        schema,
        semantic_summary,
        spatial_fields,
    )
    return CanonicalTable(
        table_name=table_name,
        source_city=source_city,
        raw_to_canonical_columns=raw_to_canonical,
        schema=schema,
        spatial_fields=spatial_fields,
        semantic_summary=semantic_summary,
        themes=themes,
    )


def canonicalize_tables(
    raw_tables: list,
    max_rows_for_inference: int = 100,
) -> list:
    """Canonicalize a list of raw spatial table objects."""

    canonical_tables: list[dict[str, Any]] = []
    for raw_table in raw_tables or []:
        if not isinstance(raw_table, Mapping):
            continue
        canonical_tables.append(
            _canonical_table_from_raw_table(
                raw_table,
                max_rows_for_inference=max_rows_for_inference,
            ).to_dict()
        )
    return canonical_tables


def _load_rows_from_dataset_path(
    dataset_path: Path,
    raw_schema: Any,
    max_rows_for_inference: int,
) -> tuple[list[dict[str, Any]], Any, Any, str]:
    if not dataset_path.is_file():
        return [], None, None, dataset_path.suffix.lower().lstrip(".")

    try:
        payload = json.loads(dataset_path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError):
        return [], None, None, dataset_path.suffix.lower().lstrip(".")

    source_format = dataset_path.suffix.lower().lstrip(".")
    if isinstance(payload, Mapping) and isinstance(payload.get("features"), list):
        alias_map = _build_property_alias_map(raw_schema)
        rows: list[dict[str, Any]] = []
        geometries: list[Any] = []
        for feature in payload.get("features", [])[:max_rows_for_inference]:
            if not isinstance(feature, Mapping):
                continue
            properties = feature.get("properties")
            filtered_properties: dict[str, Any] = {}
            if isinstance(properties, Mapping):
                for key, value in properties.items():
                    matched_raw_name = alias_map.get(
                        _normalize_name_for_matching(_normalize_text(key))
                    )
                    if matched_raw_name:
                        filtered_properties[matched_raw_name] = value
            rows.append(filtered_properties)
            geometries.append(feature.get("geometry"))
        return rows, geometries, payload.get("crs"), "geojson"

    if isinstance(payload, list):
        rows = [dict(item) for item in payload[:max_rows_for_inference] if isinstance(item, Mapping)]
        return rows, None, None, source_format or "json"

    if isinstance(payload, Mapping):
        if isinstance(payload.get("rows"), list):
            rows = [
                dict(item)
                for item in payload.get("rows", [])[:max_rows_for_inference]
                if isinstance(item, Mapping)
            ]
            return rows, None, payload.get("crs"), source_format or "json"
        return [dict(payload)], None, payload.get("crs"), source_format or "json"

    return [], None, None, source_format or "json"


def _build_raw_table_from_dataset_record(
    city_metadata: Mapping[str, Any],
    dataset_record: Mapping[str, Any],
    max_rows_for_inference: int,
) -> dict[str, Any]:
    dataset_path = Path(
        _normalize_text(dataset_record.get("path") or dataset_record.get("geojson_path"))
    )
    raw_schema = dataset_record.get("columns")
    rows, geojson_geometry, crs, source_format = _load_rows_from_dataset_path(
        dataset_path,
        raw_schema,
        max_rows_for_inference=max_rows_for_inference,
    )

    source_city = (
        _normalize_text(city_metadata.get("City"))
        or _normalize_text(city_metadata.get("city_id"))
        or "unknown"
    )
    original_name = (
        _normalize_text(dataset_record.get("name"))
        or _normalize_text(dataset_record.get("id"))
        or dataset_path.stem
        or "unknown_table"
    )
    table_name = normalize_table_name(original_name) or normalize_table_name(
        _normalize_text(dataset_record.get("id")) or dataset_path.stem or "unknown_table"
    )
    geojson_field_name = None
    if geojson_geometry is not None:
        raw_schema_columns = raw_schema if isinstance(raw_schema, list) else []
        geometry_candidates = []
        for column in raw_schema_columns:
            if not isinstance(column, Mapping):
                continue
            candidate_name = _normalize_text(column.get("name"))
            if candidate_name in {"the_geom", "geometry"}:
                geometry_candidates.append(candidate_name)
        if geometry_candidates:
            geojson_field_name = geometry_candidates[0]
        else:
            geojson_geometry = None

    return {
        "table_name": table_name,
        "source_city": source_city,
        "source_description": _normalize_text(dataset_record.get("description")),
        "raw_schema": raw_schema if isinstance(raw_schema, list) else [],
        "rows": rows,
        "source_format": _normalize_text(dataset_record.get("source_format")) or source_format.upper(),
        "crs": crs,
        "geojson_geometry": geojson_geometry,
        "geojson_geometry_field_name": geojson_field_name,
    }


def canonicalize_metadata(
    metadata: Sequence[Mapping[str, Any]] | Mapping[str, Any],
    max_rows_for_inference: int = 100,
) -> list[dict[str, Any]]:
    """Canonicalize dataset records inside metadata while preserving original fields."""

    if isinstance(metadata, Mapping):
        metadata_items = [dict(metadata)]
    elif isinstance(metadata, Sequence):
        metadata_items = [dict(item) for item in metadata if isinstance(item, Mapping)]
    else:
        metadata_items = []

    canonicalized_metadata = deepcopy(metadata_items)
    for city_metadata in canonicalized_metadata:
        datasets = city_metadata.get("datasets")
        if not isinstance(datasets, list):
            continue
        for dataset in datasets:
            if not isinstance(dataset, dict):
                continue
            raw_table = _build_raw_table_from_dataset_record(
                city_metadata,
                dataset,
                max_rows_for_inference=max_rows_for_inference,
            )
            canonical_table = _canonical_table_from_raw_table(
                raw_table,
                max_rows_for_inference=max_rows_for_inference,
            )
            dataset["canonical_table"] = canonical_table.to_dict()
    return canonicalized_metadata


def canonicalize_metadata_file(
    metadata_path: str | Path,
    *,
    output_path: str | Path | None = None,
    max_rows_for_inference: int = 100,
) -> Path:
    """Read metadata.json, canonicalize its datasets, and write metadata_canonicalized.json."""

    metadata_path = Path(metadata_path)
    if not metadata_path.is_file():
        raise FileNotFoundError(f"Metadata file not found: {metadata_path}")

    payload = json.loads(metadata_path.read_text(encoding="utf-8"))
    canonicalized = canonicalize_metadata(
        payload,
        max_rows_for_inference=max_rows_for_inference,
    )

    if output_path is None:
        output_path = metadata_path.with_name("metadata_canonicalized.json")
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        json.dumps(canonicalized, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )
    return output_path
