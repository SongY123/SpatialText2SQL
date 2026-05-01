"""Aggregate metadata and data-shape statistics for downloaded GeoJSON datasets."""
from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any, Mapping

from .profiles import CityProfile


SPATIAL_NAME_HINTS = (
    "geom",
    "geometry",
    "geography",
    "point",
    "polygon",
    "line",
    "location",
    "latitude",
    "longitude",
    "point_x",
    "point_y",
    "x_coordinate",
    "y_coordinate",
    "xcoord",
    "ycoord",
    "x_coord",
    "y_coord",
)

SPATIAL_TYPE_HINTS = (
    "geometry",
    "location",
    "point",
    "multipoint",
    "line",
    "polyline",
    "multiline",
    "polygon",
    "multipolygon",
    "geography",
)

WKT_VALUE_PATTERN = re.compile(
    r"^\s*(?:SRID=\d+;\s*)?"
    r"(?:POINT|LINESTRING|POLYGON|MULTIPOINT|MULTILINESTRING|MULTIPOLYGON|GEOMETRYCOLLECTION)"
    r"(?:\s+Z?M?)?\s*(?:EMPTY|\(.*\))\s*$",
    re.IGNORECASE,
)


def _norm_field_name(name: str) -> str:
    return re.sub(r"[^a-z0-9]+", "_", name.strip().lower()).strip("_")



def _is_spatial_field(name: str) -> bool:
    normalized = _norm_field_name(name)
    return any(hint == normalized or hint in normalized for hint in SPATIAL_NAME_HINTS)


def _is_wkt_value(value: Any) -> bool:
    if not isinstance(value, str):
        return False
    return bool(WKT_VALUE_PATTERN.match(value))


def _normalize_columns(value: Any) -> list[dict[str, str]]:
    if not isinstance(value, list):
        return []

    columns: list[dict[str, str]] = []
    for column in value:
        if not isinstance(column, Mapping):
            continue
        name = str(column.get("name") or "").strip()
        if not name:
            continue
        columns.append(
            {
                "name": name,
                "description": str(column.get("description") or "").strip(),
                "type": str(column.get("type") or "").strip(),
            }
        )
    return columns


def _is_spatial_column(column_name: str, column_type: str) -> bool:
    normalized_type = _norm_field_name(column_type)
    if any(hint == normalized_type or hint in normalized_type for hint in SPATIAL_TYPE_HINTS):
        return True
    return _is_spatial_field(column_name)


def _column_stats(columns: list[dict[str, str]]) -> dict[str, int]:
    return {
        "field_count": len(columns),
        "spatial_field_count": sum(
            1 for column in columns if _is_spatial_column(column["name"], column["type"])
        ),
    }


def _inspect_geojson_file(path: Path) -> dict[str, Any]:
    """Read a GeoJSON file once and collect stats plus property-field names."""
    payload = json.loads(path.read_text(encoding="utf-8"))
    features = payload.get("features") or []
    if not isinstance(features, list):
        features = []

    property_fields: set[str] = set()
    spatial_fields_by_value: set[str] = set()
    geometry_present = False
    geometry_type = ""
    for feature in features:
        if not isinstance(feature, Mapping):
            continue
        props = feature.get("properties") or {}
        if isinstance(props, Mapping):
            for key, value in props.items():
                field_name = str(key)
                property_fields.add(field_name)
                if _is_wkt_value(value):
                    spatial_fields_by_value.add(field_name)
        geometry = feature.get("geometry")
        if isinstance(geometry, Mapping) and geometry.get("type"):
            geometry_present = True
            if not geometry_type:
                geometry_type = str(geometry.get("type") or "").strip().lower()

    # Count actual property fields directly, plus the GeoJSON geometry feature itself.
    field_count = len(property_fields) + int(geometry_present)

    # Count spatial fields: geometry (if present) + spatial properties.
    spatial_property_count = sum(
        1 for field in property_fields if _is_spatial_field(field) or field in spatial_fields_by_value
    )
    spatial_field_count = int(geometry_present) + spatial_property_count

    return {
        "row_count": len(features),
        "field_count": max(0, field_count),
        "spatial_field_count": max(0, spatial_field_count),
        "property_fields": sorted(property_fields),
        "geometry_present": geometry_present,
        "geometry_type": geometry_type,
    }


def analyze_geojson_file(path: Path) -> dict[str, int]:
    """Compute row, field, and spatial-field counts for one GeoJSON file."""
    inspected = _inspect_geojson_file(path)
    return {
        "row_count": int(inspected["row_count"]),
        "field_count": int(inspected["field_count"]),
        "spatial_field_count": int(inspected["spatial_field_count"]),
    }


def _filter_columns_by_geojson_properties(
    columns: list[dict[str, str]],
    property_fields: list[str],
    *,
    geometry_present: bool,
) -> list[dict[str, str]]:
    if not columns:
        return []
    normalized_property_fields = {_norm_field_name(field) for field in property_fields if str(field).strip()}
    filtered: list[dict[str, str]] = []
    for column in columns:
        normalized_name = _norm_field_name(column["name"])
        if normalized_name in {"the_geom", "geometry"} and geometry_present:
            filtered.append(column)
            continue
        if normalized_name in normalized_property_fields:
            filtered.append(column)
    return filtered


def _ensure_geometry_column(
    columns: list[dict[str, str]],
    *,
    geometry_present: bool,
    geometry_type: str,
) -> list[dict[str, str]]:
    if not geometry_present:
        return columns
    if any(_norm_field_name(column["name"]) in {"the_geom", "geometry"} for column in columns):
        return columns
    return [
        *columns,
        {"name": "the_geom", "description": "", "type": geometry_type or "geometry"},
    ]


def _avg(total: int, denominator: int) -> float:
    if denominator <= 0:
        return 0.0
    return round(total / denominator, 3)


def build_city_metadata(profile: CityProfile, crawl_result: Mapping[str, Any]) -> dict[str, Any]:
    """Build one city-level metadata object for the root metadata array."""
    datasets: list[dict[str, Any]] = []
    errors = list(crawl_result.get("errors") or [])

    total_fields = 0
    total_spatial_fields = 0
    total_rows = 0

    def _normalize_tags(value: Any) -> list[str]:
        """Coerce various tag shapes into a list of strings."""
        out: list[str] = []
        if not value:
            return out
        if isinstance(value, list):
            for v in value:
                if isinstance(v, str):
                    t = v.strip()
                    if t:
                        out.append(t)
                elif isinstance(v, Mapping):
                    # CKAN tag objects often have a 'name' field
                    name = v.get("name") or v.get("display_name") or v.get("id")
                    if isinstance(name, str) and name.strip():
                        out.append(name.strip())
        elif isinstance(value, str):
            # fall back to splitting simple comma/semicolon-separated strings
            for part in re.split(r"[,;|]", value):
                p = part.strip()
                if p:
                    out.append(p)
        return out


    for item in crawl_result.get("datasets") or []:
        if not isinstance(item, Mapping):
            continue
        path = Path(str(item.get("geojson_path") or item.get("path") or ""))
        if not path.is_file():
            errors.append(
                {
                    "id": item.get("id") or item.get("asset_id") or item.get("dataset_id"),
                    "name": item.get("name"),
                    "path": str(path),
                    "error": "downloaded_geojson_missing",
                }
            )
            continue

        try:
            inspected = _inspect_geojson_file(path)
        except (json.JSONDecodeError, UnicodeDecodeError, OSError) as exc:
            errors.append(
                {
                    "id": item.get("id") or item.get("asset_id") or item.get("dataset_id"),
                    "name": item.get("name"),
                    "path": str(path),
                    "error": "invalid_geojson",
                    "detail": str(exc),
                }
            )
            continue

        total_rows += int(inspected["row_count"])
        # Normalize description and tags when present in different crawler outputs.
        description = str(item.get("description") or item.get("notes") or "").strip()
        tags = _normalize_tags(item.get("tags") or item.get("classification") or [])
        raw_columns = _normalize_columns(item.get("columns"))
        columns = _filter_columns_by_geojson_properties(
            raw_columns,
            list(inspected["property_fields"]),
            geometry_present=bool(inspected["geometry_present"]),
        )
        columns = _ensure_geometry_column(
            columns,
            geometry_present=bool(inspected["geometry_present"]),
            geometry_type=str(inspected["geometry_type"] or ""),
        )
        if raw_columns:
            column_stats = _column_stats(columns)
            field_count = column_stats["field_count"]
            spatial_field_count = column_stats["spatial_field_count"]
        else:
            field_count = int(inspected["field_count"])
            spatial_field_count = int(inspected["spatial_field_count"])
        total_fields += field_count
        total_spatial_fields += spatial_field_count

        datasets.append(
            {
                "id": item.get("id") or item.get("asset_id") or item.get("dataset_id"),
                "name": item.get("name"),
                "description": description,
                "tags": tags,
                "tags_count": len(tags),
                "columns": columns,
                "path": str(path.resolve()),
                "source_link": item.get("source_link"),
                "download_url": item.get("download_url"),
                "row_count": int(inspected["row_count"]),
                "field_count": field_count,
                "spatial_field_count": spatial_field_count,
            }
        )

    table_count = len(datasets)
    return {
        "City": profile.label,
        "city_id": profile.city_id,
        "data_dir": str((Path(crawl_result.get("data_dir") or "")).resolve()) if crawl_result.get("data_dir") else "",
        "#Table": table_count,
        "#Field/Table": _avg(total_fields, table_count),
        "#Spatial Field/Table": _avg(total_spatial_fields, table_count),
        "#Row/Table": _avg(total_rows, table_count),
        "datasets": datasets,
        "errors": errors,
    }


def write_root_metadata(metadata_path: Path, city_metadata: list[dict[str, Any]]) -> None:
    """Write the single root metadata JSON array."""
    metadata_path.parent.mkdir(parents=True, exist_ok=True)
    metadata_path.write_text(json.dumps(city_metadata, indent=2, ensure_ascii=False), encoding="utf-8")


def build_column_type_metadata(root_metadata: list[Mapping[str, Any]]) -> list[dict[str, Any]]:
    """Collect the unique column-type list for each city from root metadata."""
    column_type_metadata: list[dict[str, Any]] = []
    for city in root_metadata:
        seen_types: dict[str, str] = {}
        for dataset in city.get("datasets") or []:
            if not isinstance(dataset, Mapping):
                continue
            for column in _normalize_columns(dataset.get("columns")):
                column_type = str(column.get("type") or "").strip()
                if not column_type:
                    continue
                normalized_type = column_type.lower()
                if normalized_type not in seen_types:
                    seen_types[normalized_type] = column_type
        column_type_metadata.append(
            {
                "City": city.get("City"),
                "city_id": city.get("city_id"),
                "column_types": sorted(seen_types.values(), key=str.lower),
            }
        )
    return column_type_metadata


def write_column_type_metadata(column_type_path: Path, root_metadata: list[Mapping[str, Any]]) -> None:
    """Write per-city column-type summaries beside root metadata."""
    column_type_path.parent.mkdir(parents=True, exist_ok=True)
    payload = build_column_type_metadata(root_metadata)
    column_type_path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")


def load_root_metadata(metadata_path: Path) -> list[dict[str, Any]]:
    """Load the root metadata JSON array if it exists."""
    if not metadata_path.is_file():
        return []

    try:
        payload = json.loads(metadata_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return []

    if not isinstance(payload, list):
        return []
    return [dict(item) for item in payload if isinstance(item, Mapping)]


def index_dataset_records(dataset_records: list[Mapping[str, Any]] | list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    """Index dataset records by their id-like identifiers for manifest-based skipping."""
    index: dict[str, dict[str, Any]] = {}
    for record in dataset_records:
        if not isinstance(record, Mapping):
            continue
        normalized_record = dict(record)
        for key in (record.get("id"), record.get("asset_id"), record.get("dataset_id")):
            if key is None:
                continue
            key_str = str(key).strip().lower()
            if key_str:
                index[key_str] = normalized_record
    return index
