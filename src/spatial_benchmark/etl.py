from __future__ import annotations

from collections import defaultdict
import csv
from dataclasses import dataclass
from datetime import datetime
import json
from pathlib import Path
import re
import subprocess
import sys
import time
from typing import Any

from .clustering import load_scenario_clusters
from .database import render_database_blueprints, validate_database_sources
from .scenario_specs import SCENARIO_DATABASE_SPECS, TableDef
from .taxonomy import CATEGORY_TAXONOMY


GEOMETRY_PREFIXES = ("POINT", "MULTIPOINT", "LINESTRING", "MULTILINESTRING", "POLYGON", "MULTIPOLYGON")
CSV_ENCODINGS = ("utf-8-sig", "utf-8", "latin-1")
BOROUGH_NAME_TO_ID = {
    "manhattan": 1,
    "mn": 1,
    "m": 1,
    "new york": 1,
    "bronx": 2,
    "bx": 2,
    "x": 2,
    "brooklyn": 3,
    "bk": 3,
    "b": 3,
    "queens": 4,
    "qn": 4,
    "q": 4,
    "staten island": 5,
    "si": 5,
    "r": 5,
}
BOROUGH_ID_TO_NAME = {
    1: "Manhattan",
    2: "Bronx",
    3: "Brooklyn",
    4: "Queens",
    5: "Staten Island",
}
BOROUGH_ID_TO_PREFIX = {
    1: "MN",
    2: "BX",
    3: "BK",
    4: "QN",
    5: "SI",
}
DATE_FORMATS = (
    "%Y-%m-%d",
    "%m/%d/%Y",
    "%m/%d/%y",
    "%m/%d/%Y %I:%M:%S %p",
    "%m/%d/%Y %I:%M:%S %p %z",
    "%m/%d/%Y %H:%M:%S",
    "%m/%d/%Y %H:%M",
    "%Y-%m-%d %H:%M:%S",
    "%Y-%m-%d %H:%M",
)
TIMESTAMP_FORMATS = (
    "%Y-%m-%d %H:%M:%S",
    "%Y-%m-%d %H:%M",
    "%m/%d/%Y %I:%M:%S %p",
    "%m/%d/%Y %I:%M:%S %p %z",
    "%m/%d/%Y %H:%M:%S",
    "%m/%d/%Y",
)
PED_COUNT_RE = re.compile(r"^(May|Sept)(\d{2})_(AM|PM|MD)$", re.I)
FIRST_COORD_RE = re.compile(r"\(\s*(-?\d+(?:\.\d+)?)\s+(-?\d+(?:\.\d+)?)")
LOCATION_PAIR_RE = re.compile(r"\(\s*(-?\d+(?:\.\d+)?)\s*,\s*(-?\d+(?:\.\d+)?)\s*\)")
NON_DIGIT_RE = re.compile(r"\D+")
MULTISPACE_RE = re.compile(r"\s+")
SCHEMA_DIMENSION_SOURCES = {
    "traffic_mobility": ("borough_boundaries_gthc-hcne.csv", "2020_community_district_tabulation_areas_cdtas_mapped_xn3r-zk6y.csv"),
    "emergency_response": ("borough_boundaries_gthc-hcne.csv",),
    "public_service_accessibility": ("borough_boundaries_gthc-hcne.csv", "health_center_districts_6ez8-za84.csv"),
    "environmental_resilience": ("borough_boundaries_gthc-hcne.csv",),
    "urban_planning_land_use": ("borough_boundaries_gthc-hcne.csv",),
    "housing_demographics": (
        "borough_boundaries_gthc-hcne.csv",
        "2020_census_tracts_mapped_63ge-mke6.csv",
        "2020_neighborhood_tabulation_areas_ntas_mapped_9nt8-h7nd.csv",
        "2020_public_use_microdata_areas_pumas_map_pikk-p9nv.csv",
    ),
    "parks_recreation_poi": ("borough_boundaries_gthc-hcne.csv",),
}


field_limit = sys.maxsize
while True:
    try:
        csv.field_size_limit(field_limit)
        break
    except OverflowError:
        field_limit = int(field_limit / 10)


@dataclass(frozen=True)
class LoadTableSpec:
    schema: str
    table: TableDef
    columns: tuple[str, ...]
    geom_type: str | None


@dataclass
class MaterializedTable:
    schema: str
    table_name: str
    path: Path
    row_count: int
    columns: list[str]
    geom_type: str | None


class TableWriter:
    def __init__(self, path: Path, fieldnames: list[str]) -> None:
        self.path = path
        self.fieldnames = fieldnames
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._handle = self.path.open("w", encoding="utf-8", newline="")
        self._writer = csv.DictWriter(self._handle, fieldnames=fieldnames)
        self._writer.writeheader()
        self.row_count = 0

    def write(self, row: dict[str, Any]) -> None:
        payload = {field: normalize_scalar(row.get(field, "")) for field in self.fieldnames}
        self._writer.writerow(payload)
        self.row_count += 1

    def close(self) -> None:
        self._handle.close()


def normalize_scalar(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, float) and value.is_integer():
        return str(int(value))
    return str(value).strip()


def clean_text(value: Any) -> str:
    text = normalize_scalar(value)
    if text.lower() in {"", "na", "n/a", "null", "none"}:
        return ""
    return text


def compact_spaces(value: str) -> str:
    return MULTISPACE_RE.sub(" ", clean_text(value)).strip()


def first_non_empty(row: dict[str, Any], *keys: str) -> str:
    for key in keys:
        if key in row:
            value = clean_text(row.get(key, ""))
            if value:
                return value
    return ""


def stage_columns(table: TableDef) -> tuple[str, ...]:
    columns: list[str] = []
    geom_type: str | None = None
    for column in table.columns:
        if column.name == "geom":
            geom_type = column.data_type
            continue
        if "SERIAL" in column.data_type.upper():
            continue
        columns.append(column.name)
    if geom_type:
        columns.extend(["geom_wkt", "geom_srid"])
    return tuple(columns)


def geom_target_type(table: TableDef) -> str | None:
    for column in table.columns:
        if column.name == "geom":
            return column.data_type
    return None


def build_load_specs() -> dict[tuple[str, str], LoadTableSpec]:
    specs: dict[tuple[str, str], LoadTableSpec] = {}
    for schema_spec in SCENARIO_DATABASE_SPECS:
        for table in schema_spec.tables:
            specs[(schema_spec.id, table.name)] = LoadTableSpec(
                schema=schema_spec.id,
                table=table,
                columns=stage_columns(table),
                geom_type=geom_target_type(table),
            )
    return specs


def iter_csv_rows(path: Path):
    last_error: Exception | None = None
    for encoding in CSV_ENCODINGS:
        try:
            with path.open("r", encoding=encoding, newline="") as handle:
                reader = csv.DictReader(handle)
                for row in reader:
                    if not row:
                        continue
                    yield {key: clean_text(value) for key, value in row.items() if key is not None}
            return
        except UnicodeDecodeError as exc:
            last_error = exc
    if last_error:
        raise last_error


def infer_srid(wkt: str) -> int:
    match = FIRST_COORD_RE.search(wkt)
    if not match:
        return 4326
    x = float(match.group(1))
    y = float(match.group(2))
    if abs(x) <= 180 and abs(y) <= 90:
        return 4326
    return 2263


def parse_date(value: str) -> str:
    raw = clean_text(value)
    if not raw:
        return ""
    if raw.endswith(".0") and raw.replace(".", "", 1).isdigit():
        raw = raw[:-2]
    for fmt in DATE_FORMATS:
        try:
            return datetime.strptime(raw, fmt).date().isoformat()
        except ValueError:
            continue
    return ""


def parse_timestamp(value: str) -> str:
    raw = clean_text(value)
    if not raw:
        return ""
    for fmt in TIMESTAMP_FORMATS:
        try:
            return datetime.strptime(raw, fmt).strftime("%Y-%m-%d %H:%M:%S")
        except ValueError:
            continue
    date_only = parse_date(raw)
    return f"{date_only} 00:00:00" if date_only else ""


def parse_int(value: str) -> str:
    raw = clean_text(value)
    if not raw:
        return ""
    try:
        return str(int(float(raw.replace(",", ""))))
    except ValueError:
        digits = NON_DIGIT_RE.sub("", raw)
        return digits


def duplicated_numeric_values(path: Path, field_name: str) -> set[str]:
    counts: dict[str, int] = defaultdict(int)
    duplicates: set[str] = set()
    for row in iter_csv_rows(path):
        value = parse_int(row.get(field_name, ""))
        if not value:
            continue
        counts[value] += 1
        if counts[value] > 1:
            duplicates.add(value)
    return duplicates


def building_primary_id(row: dict[str, str], duplicate_bins: set[str]) -> str:
    building_bin = parse_int(row.get("BIN", ""))
    if building_bin and building_bin not in duplicate_bins:
        return building_bin
    for field_name in ("DOITT_ID", "OBJECTID"):
        candidate = parse_int(row.get(field_name, ""))
        if candidate:
            return candidate
    return building_bin


def parse_float(value: str, precision: int = 6) -> str:
    raw = clean_text(value)
    if not raw:
        return ""
    try:
        number = float(raw.replace(",", ""))
    except ValueError:
        return ""
    return f"{number:.{precision}f}".rstrip("0").rstrip(".")


def normalize_borough_id(value: str) -> str:
    raw = clean_text(value).strip()
    if not raw:
        return ""
    if raw.isdigit():
        number = int(raw)
        if number in BOROUGH_ID_TO_NAME:
            return str(number)
    lowered = raw.lower()
    if lowered in BOROUGH_NAME_TO_ID:
        return str(BOROUGH_NAME_TO_ID[lowered])
    if len(raw) == 2 and raw.upper() in {"MN", "BX", "BK", "QN", "SI"}:
        for borough_id, prefix in BOROUGH_ID_TO_PREFIX.items():
            if prefix == raw.upper():
                return str(borough_id)
    return ""


def normalize_cdta(value: str, borough_id: str = "") -> str:
    raw = clean_text(value).upper()
    if not raw:
        return ""
    raw = NON_DIGIT_RE.sub("", raw) if raw.isdigit() else re.sub(r"[^A-Z0-9]", "", raw)
    if re.fullmatch(r"(MN|BX|BK|QN|SI)\d{2}", raw):
        return raw
    if re.fullmatch(r"\d{3}", raw):
        source_borough = int(raw[0])
        district = raw[1:]
        prefix = BOROUGH_ID_TO_PREFIX.get(source_borough)
        return f"{prefix}{district}" if prefix else ""
    if re.fullmatch(r"\d{2}", raw) and borough_id:
        prefix = BOROUGH_ID_TO_PREFIX.get(int(borough_id))
        return f"{prefix}{raw}" if prefix else ""
    return ""


def make_point_wkt(longitude: str, latitude: str) -> str:
    lon = parse_float(longitude, precision=10)
    lat = parse_float(latitude, precision=10)
    if not lon or not lat:
        return ""
    return f"POINT ({lon} {lat})"


def extract_location_pair(value: str) -> tuple[str, str]:
    raw = clean_text(value)
    match = LOCATION_PAIR_RE.search(raw)
    if not match:
        return "", ""
    lat = match.group(1)
    lon = match.group(2)
    return lon, lat


def extract_geometry(
    row: dict[str, str],
    *,
    wkt_keys: tuple[str, ...] = ("the_geom", "geometry", "Shape", "polygon", "multipolygon", "SHAPE", "Point", "Location", "Location Point"),
    lon_keys: tuple[str, ...] = ("Longitude", "LONGITUDE", "longitude", "lon"),
    lat_keys: tuple[str, ...] = ("Latitude", "LATITUDE", "latitude", "lat"),
) -> tuple[str, str]:
    for key in wkt_keys:
        value = clean_text(row.get(key, ""))
        upper = value.upper()
        if any(upper.startswith(prefix) for prefix in GEOMETRY_PREFIXES):
            return value, str(infer_srid(value))
    longitude = first_non_empty(row, *lon_keys)
    latitude = first_non_empty(row, *lat_keys)
    if longitude and latitude:
        return make_point_wkt(longitude, latitude), "4326"
    lon, lat = extract_location_pair(first_non_empty(row, "Location 1"))
    if lon and lat:
        return make_point_wkt(lon, lat), "4326"
    return "", ""


def join_name(*parts: str) -> str:
    tokens = [compact_spaces(part) for part in parts if compact_spaces(part)]
    return " | ".join(tokens)


def maybe_typed_label(prefix: str, value: str) -> str:
    suffix = compact_spaces(value)
    return f"{prefix}:{suffix}" if suffix else prefix


def fire_company_name(company_type: str, company_number: str) -> str:
    label = {
        "E": "Engine",
        "L": "Ladder",
        "B": "Battalion",
        "R": "Rescue",
        "S": "Squad",
        "T": "Tower Ladder",
    }.get(company_type.upper(), company_type.upper() or "Company")
    number = parse_int(company_number)
    return f"{label} {number}".strip()


def latest_pedestrian_count(row: dict[str, str]) -> tuple[str, str, str]:
    grouped: dict[tuple[int, int], list[int]] = defaultdict(list)
    for key, value in row.items():
        match = PED_COUNT_RE.match(key)
        if not match:
            continue
        amount = parse_int(value)
        if not amount:
            continue
        season, year_suffix, _period = match.groups()
        year = 2000 + int(year_suffix)
        month = 9 if season.lower().startswith("sept") else 5
        grouped[(year, month)].append(int(amount))
    if not grouped:
        return "", "", ""
    year, month = max(grouped)
    values = grouped[(year, month)]
    average = round(sum(values) / len(values))
    season_name = "september" if month == 9 else "may"
    return f"{year:04d}-{month:02d}-01", season_name, str(average)


def housing_completion_snapshot(row: dict[str, str]) -> tuple[str, str]:
    latest_year = ""
    latest_units = ""
    for year in range(2024, 2009, -1):
        key = f"comp{year}"
        units = parse_int(row.get(key, ""))
        if units and int(units) > 0:
            latest_year = str(year)
            latest_units = units
            break
    return latest_year, latest_units


def safe_path_string(path: Path) -> str:
    return path.resolve().as_posix().replace("'", "''")


# Default \\copy paths in postgis_load.sql: POSIX paths for files mounted in the container
# (e.g. -v /host/.../load_ready:/nyc-data/load_ready). Docker auto-load still rewrites this to /tmp/load_ready.
CONTAINER_LOAD_READY_URI_PREFIX = "/nyc-data/load_ready"


def copy_csv_path_for_sql(item: MaterializedTable, load_ready_uri_prefix: str | None) -> str:
    """Path embedded in postgis_load.sql \\copy FROM '...'.

    If ``load_ready_uri_prefix`` is set (non-empty), use ``{prefix}/{schema}/{csv_name}`` (forward slashes).
    Otherwise use the resolved host absolute path (for local ``psql -f`` on Windows/Linux).
    """
    if load_ready_uri_prefix is None or not str(load_ready_uri_prefix).strip():
        return safe_path_string(item.path)
    base = str(load_ready_uri_prefix).strip().rstrip("/")
    uri = f"{base}/{item.schema}/{item.path.name}".replace("\\", "/")
    return uri.replace("'", "''")


class EtlContext:
    def __init__(self, raw_dir: Path, etl_dir: Path) -> None:
        self.raw_dir = raw_dir
        self.etl_dir = etl_dir
        self.load_specs = build_load_specs()
        self.writers: dict[tuple[str, str], TableWriter] = {}
        self.manifest: list[MaterializedTable] = []
        self.housing_units_by_tract: dict[str, str] = {}
        self.seen_parcel_bbls: set[str] = set()

    def source_path(self, file_name: str) -> Path:
        return self.raw_dir / file_name

    def writer(self, schema: str, table_name: str) -> TableWriter:
        key = (schema, table_name)
        if key not in self.writers:
            spec = self.load_specs[key]
            path = self.etl_dir / "load_ready" / schema / f"{table_name}.csv"
            self.writers[key] = TableWriter(path, list(spec.columns))
        return self.writers[key]

    def emit(self, schema: str, table_name: str, row: dict[str, Any]) -> None:
        self.writer(schema, table_name).write(row)

    def close(self) -> None:
        self.manifest = []
        for (schema, table_name), writer in sorted(self.writers.items()):
            writer.close()
            spec = self.load_specs[(schema, table_name)]
            self.manifest.append(
                MaterializedTable(
                    schema=schema,
                    table_name=table_name,
                    path=writer.path,
                    row_count=writer.row_count,
                    columns=list(spec.columns),
                    geom_type=spec.geom_type,
                )
            )

    def materialize(self) -> list[MaterializedTable]:
        self.materialize_dimensions()
        self.materialize_traffic_mobility()
        self.materialize_emergency_response()
        self.materialize_public_service_accessibility()
        self.materialize_environmental_resilience()
        self.materialize_urban_planning_land_use()
        self.materialize_housing_demographics()
        self.materialize_parks_recreation_poi()
        self.close()
        return self.manifest

    def materialize_dimensions(self) -> None:
        borough_rows = list(iter_csv_rows(self.source_path("borough_boundaries_gthc-hcne.csv")))
        for row in borough_rows:
            borough_id = normalize_borough_id(row.get("BoroCode", ""))
            geom_wkt, geom_srid = extract_geometry(row)
            payload = {
                "borough_id": borough_id,
                "borough_name": compact_spaces(row.get("BoroName", "")),
                "geom_wkt": geom_wkt,
                "geom_srid": geom_srid,
            }
            for schema in SCHEMA_DIMENSION_SOURCES:
                self.emit(schema, "dim_borough", payload)

        for row in iter_csv_rows(self.source_path("2020_community_district_tabulation_areas_cdtas_mapped_xn3r-zk6y.csv")):
            borough_id = normalize_borough_id(row.get("BoroCode", ""))
            geom_wkt, geom_srid = extract_geometry(row)
            self.emit(
                "traffic_mobility",
                "dim_community_district",
                {
                    "cdta_code": compact_spaces(row.get("CDTA2020", "")),
                    "borough_id": borough_id,
                    "district_name": compact_spaces(row.get("CDTAName", "")),
                    "geom_wkt": geom_wkt,
                    "geom_srid": geom_srid,
                },
            )

    def materialize_traffic_mobility(self) -> None:
        for row in iter_csv_rows(self.source_path("new_york_city_bike_routes_map_mzxg-pwib.csv")):
            borough_id = normalize_borough_id(row.get("boro", ""))
            geom_wkt, geom_srid = extract_geometry(row)
            self.emit(
                "traffic_mobility",
                "traffic_corridor",
                {
                    "source_dataset": "new_york_city_bike_routes_map_mzxg-pwib.csv",
                    "corridor_class": maybe_typed_label("bike_route", row.get("facilitycl", "")),
                    "corridor_name": join_name(row.get("street", ""), row.get("fromstreet", ""), row.get("tostreet", "")),
                    "borough_id": borough_id,
                    "cdta_code": "",
                    "valid_from": parse_date(row.get("instdate", "")),
                    "valid_to": parse_date(row.get("ret_date", "")),
                    "geom_wkt": geom_wkt,
                    "geom_srid": geom_srid,
                },
            )

        for row in iter_csv_rows(self.source_path("new_york_city_truck_routes_map_jjja-shxy.csv")):
            borough_id = normalize_borough_id(first_non_empty(row, "BoroCode", "BoroName"))
            geom_wkt, geom_srid = extract_geometry(row)
            self.emit(
                "traffic_mobility",
                "traffic_corridor",
                {
                    "source_dataset": "new_york_city_truck_routes_map_jjja-shxy.csv",
                    "corridor_class": maybe_typed_label("truck_route", row.get("RouteType", "")),
                    "corridor_name": compact_spaces(row.get("Street", "")),
                    "borough_id": borough_id,
                    "cdta_code": "",
                    "valid_from": "",
                    "valid_to": "",
                    "geom_wkt": geom_wkt,
                    "geom_srid": geom_srid,
                },
            )

        for row in iter_csv_rows(self.source_path("bus_lanes_local_streets_map_ycrg-ses3.csv")):
            borough_id = normalize_borough_id(first_non_empty(row, "Boro"))
            geom_wkt, geom_srid = extract_geometry(row)
            self.emit(
                "traffic_mobility",
                "traffic_corridor",
                {
                    "source_dataset": "bus_lanes_local_streets_map_ycrg-ses3.csv",
                    "corridor_class": maybe_typed_label("bus_lane", first_non_empty(row, "Lane_Type", "Lane_Type1", "Facility")),
                    "corridor_name": compact_spaces(first_non_empty(row, "Street", "Facility")),
                    "borough_id": borough_id,
                    "cdta_code": "",
                    "valid_from": "",
                    "valid_to": "",
                    "geom_wkt": geom_wkt,
                    "geom_srid": geom_srid,
                },
            )

        for row in iter_csv_rows(self.source_path("protected_streets_block_map_wyih-3nzf.csv")):
            borough_id = normalize_borough_id(first_non_empty(row, "GeoBoroughCode"))
            geom_wkt, geom_srid = extract_geometry(row)
            self.emit(
                "traffic_mobility",
                "traffic_corridor",
                {
                    "source_dataset": "protected_streets_block_map_wyih-3nzf.csv",
                    "corridor_class": "protected_street",
                    "corridor_name": join_name(row.get("OnstreetName", ""), row.get("FromStreet", ""), row.get("ToStreetName", "")),
                    "borough_id": borough_id,
                    "cdta_code": "",
                    "valid_from": parse_date(row.get("DateProtectedFrom", "")),
                    "valid_to": parse_date(row.get("DateProtectedTo", "")),
                    "geom_wkt": geom_wkt,
                    "geom_srid": geom_srid,
                },
            )

        for row in iter_csv_rows(self.source_path("accessible_pedestrian_signal_locations_map_de3m-c5p4.csv")):
            borough_id = normalize_borough_id(first_non_empty(row, "BoroCode", "Borough"))
            geom_wkt, geom_srid = extract_geometry(row)
            self.emit(
                "traffic_mobility",
                "pedestrian_signal",
                {
                    "source_dataset": "accessible_pedestrian_signal_locations_map_de3m-c5p4.csv",
                    "location_name": compact_spaces(row.get("Location", "")),
                    "borough_id": borough_id,
                    "cdta_code": normalize_cdta(row.get("BoroCD", ""), borough_id),
                    "installed_at": parse_date(row.get("Date_Insta", "")),
                    "signal_type": "accessible_pedestrian_signal",
                    "geom_wkt": geom_wkt,
                    "geom_srid": geom_srid,
                },
            )

        for row in iter_csv_rows(self.source_path("walknyc_sign_locations_ns8x-qshd.csv")):
            borough_id = normalize_borough_id(first_non_empty(row, "BoroCode", "BoroName"))
            geom_wkt, geom_srid = extract_geometry(row)
            self.emit(
                "traffic_mobility",
                "pedestrian_signal",
                {
                    "source_dataset": "walknyc_sign_locations_ns8x-qshd.csv",
                    "location_name": join_name(row.get("Street", ""), row.get("XStreet1", ""), row.get("XStreet2", ""), row.get("Site_ID", "")),
                    "borough_id": borough_id,
                    "cdta_code": normalize_cdta(row.get("BoroCD", ""), borough_id),
                    "installed_at": parse_date(row.get("StartDate", "")),
                    "signal_type": maybe_typed_label("walknyc_sign", row.get("Type", "")),
                    "geom_wkt": geom_wkt,
                    "geom_srid": geom_srid,
                },
            )

        for row in iter_csv_rows(self.source_path("bi_annual_pedestrian_counts_map_cqsj-cfgu.csv")):
            geom_wkt, geom_srid = extract_geometry(row)
            count_date, time_period, avg_count = latest_pedestrian_count(row)
            self.emit(
                "traffic_mobility",
                "pedestrian_count_site",
                {
                    "source_dataset": "bi_annual_pedestrian_counts_map_cqsj-cfgu.csv",
                    "corridor_id": "",
                    "count_date": count_date,
                    "time_period": time_period,
                    "avg_daily_count": avg_count,
                    "geom_wkt": geom_wkt,
                    "geom_srid": geom_srid,
                },
            )

        for row in iter_csv_rows(self.source_path("parking_meters_locations_and_status_map_693u-uax6.csv")):
            borough_id = normalize_borough_id(row.get("Borough", ""))
            geom_wkt, geom_srid = extract_geometry(row)
            self.emit(
                "traffic_mobility",
                "curbside_asset",
                {
                    "source_dataset": "parking_meters_locations_and_status_map_693u-uax6.csv",
                    "asset_type": "parking_meter",
                    "status": compact_spaces(row.get("Status", "")),
                    "borough_id": borough_id,
                    "geom_wkt": geom_wkt,
                    "geom_srid": geom_srid,
                },
            )

    def materialize_emergency_response(self) -> None:
        for row in iter_csv_rows(self.source_path("fire_battalions_xzng-ft6f.csv")):
            geom_wkt, geom_srid = extract_geometry(row)
            battalion = parse_int(row.get("Fire Battalion", ""))
            self.emit(
                "emergency_response",
                "safety_service_area",
                {
                    "area_type": "fire_battalion",
                    "area_name": f"Fire Battalion {battalion}" if battalion else "Fire Battalion",
                    "borough_id": "",
                    "geom_wkt": geom_wkt,
                    "geom_srid": geom_srid,
                },
            )

        for row in iter_csv_rows(self.source_path("fire_companies_bst7-5464.csv")):
            geom_wkt, geom_srid = extract_geometry(row)
            self.emit(
                "emergency_response",
                "response_facility",
                {
                    "source_dataset": "fire_companies_bst7-5464.csv",
                    "facility_type": "fire_company",
                    "facility_name": fire_company_name(row.get("Fire Company Type", ""), row.get("Fire Company Number", "")),
                    "borough_id": "",
                    "service_area_id": "",
                    "commissioned_at": "",
                    "geom_wkt": geom_wkt,
                    "geom_srid": geom_srid,
                },
            )

        for row in iter_csv_rows(self.source_path("nycha_psa_police_service_areas_bvi6-r9nk.csv")):
            borough_id = normalize_borough_id(row.get("BOROUGH", ""))
            geom_wkt, geom_srid = extract_geometry(row)
            self.emit(
                "emergency_response",
                "response_facility",
                {
                    "source_dataset": "nycha_psa_police_service_areas_bvi6-r9nk.csv",
                    "facility_type": "police_service_area",
                    "facility_name": compact_spaces(row.get("PSA", "")),
                    "borough_id": borough_id,
                    "service_area_id": "",
                    "commissioned_at": "",
                    "geom_wkt": geom_wkt,
                    "geom_srid": geom_srid,
                },
            )

        for row in iter_csv_rows(self.source_path("hurricane_evacuation_centers_map_p5md-weyf.csv")):
            borough_id = normalize_borough_id(first_non_empty(row, "BOROCODE", "CITY"))
            geom_wkt, geom_srid = extract_geometry(row)
            self.emit(
                "emergency_response",
                "response_facility",
                {
                    "source_dataset": "hurricane_evacuation_centers_map_p5md-weyf.csv",
                    "facility_type": "evacuation_center",
                    "facility_name": compact_spaces(row.get("EC_Name", "")),
                    "borough_id": borough_id,
                    "service_area_id": "",
                    "commissioned_at": "",
                    "geom_wkt": geom_wkt,
                    "geom_srid": geom_srid,
                },
            )

        for row in iter_csv_rows(self.source_path("in_service_alarm_box_locations_map_v57i-gtxb.csv")):
            borough_id = normalize_borough_id(row.get("BOROUGH", ""))
            geom_wkt, geom_srid = extract_geometry(row, wkt_keys=("Location Point",))
            self.emit(
                "emergency_response",
                "alarm_asset",
                {
                    "source_dataset": "in_service_alarm_box_locations_map_v57i-gtxb.csv",
                    "asset_type": maybe_typed_label("alarm_box", row.get("BOX_TYPE", "")),
                    "borough_id": borough_id,
                    "last_inspected_at": "",
                    "geom_wkt": geom_wkt,
                    "geom_srid": geom_srid,
                },
            )

        for row in iter_csv_rows(self.source_path("nycdep_citywide_hydrants_5bgh-vtsn.csv")):
            borough_id = normalize_borough_id(row.get("BORO", ""))
            geom_wkt, geom_srid = extract_geometry(row)
            self.emit(
                "emergency_response",
                "alarm_asset",
                {
                    "source_dataset": "nycdep_citywide_hydrants_5bgh-vtsn.csv",
                    "asset_type": "hydrant",
                    "borough_id": borough_id,
                    "last_inspected_at": "",
                    "geom_wkt": geom_wkt,
                    "geom_srid": geom_srid,
                },
            )

        for row in iter_csv_rows(self.source_path("hurricane_inundation_by_evacuation_zone_map_uk9f-6y9n.csv")):
            geom_wkt, geom_srid = extract_geometry(row)
            self.emit(
                "emergency_response",
                "hazard_zone",
                {
                    "hazard_type": "hurricane_inundation",
                    "severity_label": maybe_typed_label("evacuation_zone", row.get("Evac_Zone", "")),
                    "effective_from": "",
                    "effective_to": "",
                    "geom_wkt": geom_wkt,
                    "geom_srid": geom_srid,
                },
            )

        for row in iter_csv_rows(self.source_path("sandy_inundation_zone_5xsi-dfpx.csv")):
            geom_wkt, geom_srid = extract_geometry(row)
            self.emit(
                "emergency_response",
                "hazard_zone",
                {
                    "hazard_type": "sandy_inundation",
                    "severity_label": compact_spaces(first_non_empty(row, "status", "verified")),
                    "effective_from": "",
                    "effective_to": "",
                    "geom_wkt": geom_wkt,
                    "geom_srid": geom_srid,
                },
            )

    def materialize_public_service_accessibility(self) -> None:
        for row in iter_csv_rows(self.source_path("school_zones_map_2024_2025_ruu9-egea.csv")):
            borough_id = normalize_borough_id(first_non_empty(row, "Boro", "Boro_Text"))
            geom_wkt, geom_srid = extract_geometry(row)
            self.emit(
                "public_service_accessibility",
                "service_zone",
                {
                    "zone_type": "school_zone",
                    "zone_name": compact_spaces(first_non_empty(row, "SCH_NAME", "DBN", "Label", "Remarks")),
                    "borough_id": borough_id,
                    "geom_wkt": geom_wkt,
                    "geom_srid": geom_srid,
                },
            )

        for row in iter_csv_rows(self.source_path("health_center_districts_6ez8-za84.csv")):
            borough_id = normalize_borough_id(first_non_empty(row, "Borough Code", "Borough Name"))
            geom_wkt, geom_srid = extract_geometry(row)
            district_number = parse_int(row.get("Health Center number", ""))
            self.emit(
                "public_service_accessibility",
                "service_zone",
                {
                    "zone_type": "health_center_district",
                    "zone_name": f"Health Center District {district_number}" if district_number else "Health Center District",
                    "borough_id": borough_id,
                    "geom_wkt": geom_wkt,
                    "geom_srid": geom_srid,
                },
            )

        for row in iter_csv_rows(self.source_path("library_feuq-due4.csv")):
            borough_id = normalize_borough_id(row.get("BOROCODE", ""))
            geom_wkt, geom_srid = extract_geometry(row)
            self.emit(
                "public_service_accessibility",
                "public_facility",
                {
                    "source_dataset": "library_feuq-due4.csv",
                    "facility_type": "library",
                    "facility_name": compact_spaces(row.get("NAME", "")),
                    "operator_name": compact_spaces(row.get("SYSTEM", "")),
                    "borough_id": borough_id,
                    "zone_id": "",
                    "opened_at": "",
                    "geom_wkt": geom_wkt,
                    "geom_srid": geom_srid,
                },
            )

        for row in iter_csv_rows(self.source_path("post_office_bdha-6eqy.csv")):
            geom_wkt, geom_srid = extract_geometry(row)
            self.emit(
                "public_service_accessibility",
                "public_facility",
                {
                    "source_dataset": "post_office_bdha-6eqy.csv",
                    "facility_type": "post_office",
                    "facility_name": compact_spaces(row.get("NAME", "")),
                    "operator_name": "USPS",
                    "borough_id": "",
                    "zone_id": "",
                    "opened_at": "",
                    "geom_wkt": geom_wkt,
                    "geom_srid": geom_srid,
                },
            )

        for row in iter_csv_rows(self.source_path("pharmaceutical_and_syringe_drop_off_locations_in_nyc_map_edk2-vkjh.csv")):
            borough_id = normalize_borough_id(row.get("Borough", ""))
            geom_wkt, geom_srid = extract_geometry(row)
            self.emit(
                "public_service_accessibility",
                "public_facility",
                {
                    "source_dataset": "pharmaceutical_and_syringe_drop_off_locations_in_nyc_map_edk2-vkjh.csv",
                    "facility_type": maybe_typed_label("dropoff_site", row.get("Site Type", "")),
                    "facility_name": compact_spaces(row.get("Site Name", "")),
                    "operator_name": compact_spaces(row.get("Site Type", "")),
                    "borough_id": borough_id,
                    "zone_id": "",
                    "opened_at": "",
                    "geom_wkt": geom_wkt,
                    "geom_srid": geom_srid,
                },
            )

        for row in iter_csv_rows(self.source_path("nyc_automated_external_defibrillator_aed_map_2er2-jqsx.csv")):
            borough_id = normalize_borough_id(row.get("Borough", ""))
            geom_wkt, geom_srid = extract_geometry(row, wkt_keys=("Location Point",))
            self.emit(
                "public_service_accessibility",
                "amenity_point",
                {
                    "source_dataset": "nyc_automated_external_defibrillator_aed_map_2er2-jqsx.csv",
                    "amenity_type": "aed",
                    "amenity_name": compact_spaces(row.get("Entity_Name", "")),
                    "service_status": compact_spaces(row.get("Location Type", "")),
                    "borough_id": borough_id,
                    "activated_at": parse_date(row.get("Last Updated", "")),
                    "geom_wkt": geom_wkt,
                    "geom_srid": geom_srid,
                },
            )

        for row in iter_csv_rows(self.source_path("linknyc_locations_shapefile_5mv6-f76y.csv")):
            borough_id = normalize_borough_id(row.get("Boro", ""))
            geom_wkt, geom_srid = extract_geometry(row)
            self.emit(
                "public_service_accessibility",
                "amenity_point",
                {
                    "source_dataset": "linknyc_locations_shapefile_5mv6-f76y.csv",
                    "amenity_type": "linknyc",
                    "amenity_name": compact_spaces(first_non_empty(row, "Site_ID", "Legacy_ID")),
                    "service_status": compact_spaces(row.get("Installati", "")),
                    "borough_id": borough_id,
                    "activated_at": parse_date(first_non_empty(row, "Activation", "Install_Co")),
                    "geom_wkt": geom_wkt,
                    "geom_srid": geom_srid,
                },
            )

        for row in iter_csv_rows(self.source_path("public_restrooms_operational_i7jb-7jku.csv")):
            geom_wkt, geom_srid = extract_geometry(row)
            self.emit(
                "public_service_accessibility",
                "amenity_point",
                {
                    "source_dataset": "public_restrooms_operational_i7jb-7jku.csv",
                    "amenity_type": "public_restroom",
                    "amenity_name": compact_spaces(row.get("Facility Name", "")),
                    "service_status": compact_spaces(row.get("Status", "")),
                    "borough_id": "",
                    "activated_at": "",
                    "geom_wkt": geom_wkt,
                    "geom_srid": geom_srid,
                },
            )

        for row in iter_csv_rows(self.source_path("nyc_parks_drinking_fountains_map_qnv7-p7a2.csv")):
            borough_id = normalize_borough_id(row.get("Borough", ""))
            geom_wkt, geom_srid = extract_geometry(row, wkt_keys=("Point",))
            self.emit(
                "public_service_accessibility",
                "amenity_point",
                {
                    "source_dataset": "nyc_parks_drinking_fountains_map_qnv7-p7a2.csv",
                    "amenity_type": "drinking_fountain",
                    "amenity_name": compact_spaces(row.get("PropName", "")),
                    "service_status": compact_spaces(row.get("FeatureStatus", "")),
                    "borough_id": borough_id,
                    "activated_at": "",
                    "geom_wkt": geom_wkt,
                    "geom_srid": geom_srid,
                },
            )

        for row in iter_csv_rows(self.source_path("seating_locations_map_esmy-s8q5.csv")):
            borough_id = normalize_borough_id(first_non_empty(row, "BoroCode", "BoroName"))
            geom_wkt, geom_srid = extract_geometry(row)
            self.emit(
                "public_service_accessibility",
                "amenity_point",
                {
                    "source_dataset": "seating_locations_map_esmy-s8q5.csv",
                    "amenity_type": "seating",
                    "amenity_name": compact_spaces(first_non_empty(row, "SiteID", "Nearest_Add")),
                    "service_status": compact_spaces(first_non_empty(row, "Category", "Asset_Subtype")),
                    "borough_id": borough_id,
                    "activated_at": parse_date(row.get("Installation Date", "")),
                    "geom_wkt": geom_wkt,
                    "geom_srid": geom_srid,
                },
            )

    def materialize_environmental_resilience(self) -> None:
        for row in iter_csv_rows(self.source_path("nyc_wetlands_map_p48c-iqtu.csv")):
            geom_wkt, geom_srid = extract_geometry(row, wkt_keys=("Shape",))
            self.emit(
                "environmental_resilience",
                "ecological_zone",
                {
                    "zone_type": "wetland",
                    "zone_name": compact_spaces(row.get("ClassName", "")),
                    "borough_id": "",
                    "risk_level": compact_spaces(row.get("VerificationStatus", "")),
                    "geom_wkt": geom_wkt,
                    "geom_srid": geom_srid,
                },
            )

        for row in iter_csv_rows(self.source_path("sea_level_rise_maps_2050s_100_year_floodplain_27ya-gqtm.csv")):
            geom_wkt, geom_srid = extract_geometry(row)
            zone_name = compact_spaces(first_non_empty(row, "FLD_ZONE"))
            self.emit(
                "environmental_resilience",
                "ecological_zone",
                {
                    "zone_type": "sea_level_rise_2050_100yr",
                    "zone_name": zone_name or "2050s 100-year floodplain",
                    "borough_id": "",
                    "risk_level": compact_spaces(first_non_empty(row, "GRIDCODE", "ABFE_0_2Pc")),
                    "geom_wkt": geom_wkt,
                    "geom_srid": geom_srid,
                },
            )

        for row in iter_csv_rows(self.source_path("new_york_city_s_flood_vulnerability_index_map_mrjc-v9pm.csv")):
            geom_wkt, geom_srid = extract_geometry(row)
            self.emit(
                "environmental_resilience",
                "ecological_zone",
                {
                    "zone_type": "flood_vulnerability",
                    "zone_name": compact_spaces(row.get("GEOID", "")),
                    "borough_id": "",
                    "risk_level": compact_spaces(row.get("FSHRI", "")),
                    "geom_wkt": geom_wkt,
                    "geom_srid": geom_srid,
                },
            )

        for row in iter_csv_rows(self.source_path("dep_green_infrastructure_map_all_layers_df32-vzax.csv")):
            borough_id = normalize_borough_id(row.get("Borough", ""))
            geom_wkt, geom_srid = extract_geometry(row)
            self.emit(
                "environmental_resilience",
                "green_infrastructure_site",
                {
                    "source_dataset": "dep_green_infrastructure_map_all_layers_df32-vzax.csv",
                    "practice_type": compact_spaces(first_non_empty(row, "Asset_Type", "GI_Feature", "Project_Na")) or "unspecified_gi_practice",
                    "status": compact_spaces(row.get("Status", "")),
                    "borough_id": borough_id,
                    "installed_at": parse_date(row.get("Constructed_Date", "")),
                    "geom_wkt": geom_wkt,
                    "geom_srid": geom_srid,
                },
            )

        for row in iter_csv_rows(self.source_path("2005_street_tree_census_urik-ndeg.csv")):
            borough_id = normalize_borough_id(first_non_empty(row, "borocode", "boroname"))
            geom_wkt, geom_srid = extract_geometry(row)
            census_year = parse_int(row.get("cen_year", ""))
            observed_at = f"{int(census_year):04d}-01-01 00:00:00" if census_year and int(census_year) >= 1900 else ""
            self.emit(
                "environmental_resilience",
                "environmental_asset",
                {
                    "source_dataset": "2005_street_tree_census_urik-ndeg.csv",
                    "asset_type": "street_tree",
                    "asset_name": compact_spaces(first_non_empty(row, "spc_common", "spc_latin")),
                    "borough_id": borough_id,
                    "observed_at": observed_at,
                    "condition_status": compact_spaces(row.get("status", "")),
                    "geom_wkt": geom_wkt,
                    "geom_srid": geom_srid,
                },
            )

        for row in iter_csv_rows(self.source_path("floodnet_sensor_deployment_metadata_kb2e-tjy3.csv")):
            borough_id = normalize_borough_id(row.get("Borough", ""))
            geom_wkt, geom_srid = extract_geometry(row, wkt_keys=("Sensor Location",))
            self.emit(
                "environmental_resilience",
                "environmental_asset",
                {
                    "source_dataset": "floodnet_sensor_deployment_metadata_kb2e-tjy3.csv",
                    "asset_type": "flood_sensor",
                    "asset_name": compact_spaces(row.get("Sensor Name", "")),
                    "borough_id": borough_id,
                    "observed_at": parse_timestamp(row.get("Date Installed", "")),
                    "condition_status": compact_spaces(first_non_empty(row, "Tidally Influenced", "Date Removed")),
                    "geom_wkt": geom_wkt,
                    "geom_srid": geom_srid,
                },
            )

        for row in iter_csv_rows(self.source_path("nycdep_citywide_catch_basins_2w2g-fk3i.csv")):
            geom_wkt, geom_srid = extract_geometry(row)
            self.emit(
                "environmental_resilience",
                "environmental_asset",
                {
                    "source_dataset": "nycdep_citywide_catch_basins_2w2g-fk3i.csv",
                    "asset_type": "catch_basin",
                    "asset_name": compact_spaces(row.get("UNITID", "")),
                    "borough_id": "",
                    "observed_at": "",
                    "condition_status": "",
                    "geom_wkt": geom_wkt,
                    "geom_srid": geom_srid,
                },
            )

        for row in iter_csv_rows(self.source_path("citywide_outfalls_8rjn-kpsh.csv")):
            geom_wkt, geom_srid = extract_geometry(row)
            self.emit(
                "environmental_resilience",
                "environmental_asset",
                {
                    "source_dataset": "citywide_outfalls_8rjn-kpsh.csv",
                    "asset_type": "outfall",
                    "asset_name": compact_spaces(row.get("UNITID", "")),
                    "borough_id": "",
                    "observed_at": "",
                    "condition_status": compact_spaces(first_non_empty(row, "OWNERSHIP", "TREATMENT_")),
                    "geom_wkt": geom_wkt,
                    "geom_srid": geom_srid,
                },
            )

    def materialize_urban_planning_land_use(self) -> None:
        duplicate_building_bins = duplicated_numeric_values(
            self.source_path("building_footprints_map_5zhs-2jue.csv"),
            "BIN",
        )

        for row in iter_csv_rows(self.source_path("zoning_map_index_section_jsdz-u4b8.csv")):
            geom_wkt, geom_srid = extract_geometry(row)
            section = compact_spaces(row.get("SECTION_", ""))
            self.emit(
                "urban_planning_land_use",
                "planning_area",
                {
                    "area_type": "zoning_section",
                    "area_name": f"Section {section}" if section else "Zoning Section",
                    "borough_id": "",
                    "designation_status": "active",
                    "effective_date": "",
                    "geom_wkt": geom_wkt,
                    "geom_srid": geom_srid,
                },
            )

        for row in iter_csv_rows(self.source_path("waterfront_access_plans_mtfi-jmfv.csv")):
            geom_wkt, geom_srid = extract_geometry(row)
            self.emit(
                "urban_planning_land_use",
                "planning_area",
                {
                    "area_type": "waterfront_access_plan",
                    "area_name": compact_spaces(row.get("Name", "")),
                    "borough_id": "",
                    "designation_status": "active",
                    "effective_date": "",
                    "geom_wkt": geom_wkt,
                    "geom_srid": geom_srid,
                },
            )

        for row in iter_csv_rows(self.source_path("tax_lot_polygon_i38t-6if2.csv")):
            bbl = parse_int(row.get("BBL", ""))
            if not bbl or bbl in self.seen_parcel_bbls:
                continue
            self.seen_parcel_bbls.add(bbl)
            borough_id = normalize_borough_id(row.get("BORO", ""))
            geom_wkt, geom_srid = extract_geometry(row)
            self.emit(
                "urban_planning_land_use",
                "parcel",
                {
                    "bbl": bbl,
                    "source_dataset": "tax_lot_polygon_i38t-6if2.csv",
                    "borough_id": borough_id,
                    "zoning_district": "",
                    "land_use_code": "",
                    "lot_area": "",
                    "geom_wkt": geom_wkt,
                    "geom_srid": geom_srid,
                },
            )

        for row in iter_csv_rows(self.source_path("building_footprints_map_5zhs-2jue.csv")):
            geom_wkt, geom_srid = extract_geometry(row)
            self.emit(
                "urban_planning_land_use",
                "building",
                {
                    "building_id": building_primary_id(row, duplicate_building_bins),
                    "source_dataset": "building_footprints_map_5zhs-2jue.csv",
                    "bbl": parse_int(first_non_empty(row, "Map Pluto BBL", "BASE_BBL")),
                    "building_class": compact_spaces(first_non_empty(row, "Feature Code", "LAST_STATUS_TYPE")),
                    "num_floors": "",
                    "year_built": parse_int(row.get("Construction Year", "")),
                    "geom_wkt": geom_wkt,
                    "geom_srid": geom_srid,
                },
            )

        for row in iter_csv_rows(self.source_path("individual_landmark_sites_map_buis-pvji.csv")):
            borough_id = normalize_borough_id(first_non_empty(row, "Borough"))
            geom_wkt, geom_srid = extract_geometry(row)
            self.emit(
                "urban_planning_land_use",
                "landmark_site",
                {
                    "source_dataset": "individual_landmark_sites_map_buis-pvji.csv",
                    "landmark_name": compact_spaces(row.get("LPC_NAME", "")),
                    "designation_date": parse_date(row.get("DesDate", "")),
                    "bbl": parse_int(row.get("BBL", "")),
                    "borough_id": borough_id,
                    "geom_wkt": geom_wkt,
                    "geom_srid": geom_srid,
                },
            )

        for row in iter_csv_rows(self.source_path("designated_and_calendared_buildings_and_sites_map_ncre-qhxs.csv")):
            borough_id = normalize_borough_id(first_non_empty(row, "BoroughID"))
            geom_wkt, geom_srid = extract_geometry(row)
            self.emit(
                "urban_planning_land_use",
                "landmark_site",
                {
                    "source_dataset": "designated_and_calendared_buildings_and_sites_map_ncre-qhxs.csv",
                    "landmark_name": compact_spaces(row.get("LM_NAME", "")),
                    "designation_date": parse_date(first_non_empty(row, "DESDATE", "CALDATE")),
                    "bbl": parse_int(row.get("BBL", "")),
                    "borough_id": borough_id,
                    "geom_wkt": geom_wkt,
                    "geom_srid": geom_srid,
                },
            )

        for row in iter_csv_rows(self.source_path("board_of_standards_and_appeals_bsa_decisions_map_yvxd-uipr.csv")):
            borough_id = normalize_borough_id(first_non_empty(row, "Borough Code", "Borough"))
            geom_wkt, geom_srid = extract_geometry(row, wkt_keys=())
            self.emit(
                "urban_planning_land_use",
                "capital_project",
                {
                    "source_dataset": "board_of_standards_and_appeals_bsa_decisions_map_yvxd-uipr.csv",
                    "project_name": compact_spaces(first_non_empty(row, "Project Description", "Application")),
                    "project_type": compact_spaces(row.get("Application", "")),
                    "status": compact_spaces(row.get("Status", "")),
                    "project_start_date": parse_date(row.get("Date", "")),
                    "project_end_date": "",
                    "borough_id": borough_id,
                    "geom_wkt": geom_wkt,
                    "geom_srid": geom_srid,
                },
            )

        for file_name in (
            "capital_projects_database_cpdb_projects_polygons_maps_9jkp-n57r.csv",
            "capital_projects_database_cpdb_projects_points_map_h2ic-zdws.csv",
        ):
            for row in iter_csv_rows(self.source_path(file_name)):
                geom_wkt, geom_srid = extract_geometry(row)
                self.emit(
                    "urban_planning_land_use",
                    "capital_project",
                    {
                        "source_dataset": file_name,
                        "project_name": compact_spaces(first_non_empty(row, "descript", "projectid", "maprojid")),
                        "project_type": compact_spaces(first_non_empty(row, "typecat", "magenname")),
                        "status": compact_spaces(first_non_empty(row, "ccpversion")),
                        "project_start_date": parse_date(row.get("mindate", "")),
                        "project_end_date": parse_date(row.get("maxdate", "")),
                        "borough_id": "",
                        "geom_wkt": geom_wkt,
                        "geom_srid": geom_srid,
                    },
                )

    def materialize_housing_demographics(self) -> None:
        for row in iter_csv_rows(self.source_path("housing_database_by_2020_census_tract_map_nahe-je7c.csv")):
            tract_id = compact_spaces(first_non_empty(row, "centract2020"))
            borough_id = normalize_borough_id(row.get("bct2020", "")[:1])
            geom_wkt, geom_srid = extract_geometry(row)
            latest_year, latest_units = housing_completion_snapshot(row)
            housing_units = parse_int(row.get("cenunits20", ""))
            if tract_id and housing_units:
                self.housing_units_by_tract[tract_id] = housing_units
            self.emit(
                "housing_demographics",
                "housing_project",
                {
                    "source_dataset": "housing_database_by_2020_census_tract_map_nahe-je7c.csv",
                    "project_name": f"Housing summary for tract {tract_id}" if tract_id else "Housing summary",
                    "housing_program": "tract_housing_summary",
                    "affordable_units": latest_units,
                    "total_units": housing_units,
                    "completion_date": f"{latest_year}-12-31" if latest_year else "",
                    "borough_id": borough_id,
                    "tract_id": tract_id,
                    "geom_wkt": geom_wkt,
                    "geom_srid": geom_srid,
                },
            )

        for row in iter_csv_rows(self.source_path("2020_census_tracts_mapped_63ge-mke6.csv")):
            borough_id = normalize_borough_id(row.get("BoroCode", ""))
            tract_id = compact_spaces(first_non_empty(row, "GEOID"))
            geom_wkt, geom_srid = extract_geometry(row)
            self.emit(
                "housing_demographics",
                "census_tract",
                {
                    "tract_id": tract_id,
                    "borough_id": borough_id,
                    "nta_code": compact_spaces(row.get("NTA2020", "")),
                    "cdta_code": compact_spaces(row.get("CDTA2020", "")),
                    "puma_code": "",
                    "population_total": "",
                    "housing_units": self.housing_units_by_tract.get(tract_id, ""),
                    "geom_wkt": geom_wkt,
                    "geom_srid": geom_srid,
                },
            )

        for row in iter_csv_rows(self.source_path("2020_neighborhood_tabulation_areas_ntas_mapped_9nt8-h7nd.csv")):
            borough_id = normalize_borough_id(row.get("BoroCode", ""))
            geom_wkt, geom_srid = extract_geometry(row)
            self.emit(
                "housing_demographics",
                "neighborhood_area",
                {
                    "area_type": "nta",
                    "area_name": compact_spaces(first_non_empty(row, "NTAName", "NTA2020")),
                    "borough_id": borough_id,
                    "geom_wkt": geom_wkt,
                    "geom_srid": geom_srid,
                },
            )

        for row in iter_csv_rows(self.source_path("2020_public_use_microdata_areas_pumas_map_pikk-p9nv.csv")):
            geom_wkt, geom_srid = extract_geometry(row)
            self.emit(
                "housing_demographics",
                "neighborhood_area",
                {
                    "area_type": "puma",
                    "area_name": compact_spaces(row.get("PUMA", "")),
                    "borough_id": "",
                    "geom_wkt": geom_wkt,
                    "geom_srid": geom_srid,
                },
            )

        for row in iter_csv_rows(self.source_path("map_of_nycha_community_engagement_partnership_zones_m3cb-f9jj.csv")):
            geom_wkt, geom_srid = extract_geometry(row)
            self.emit(
                "housing_demographics",
                "neighborhood_area",
                {
                    "area_type": "nycha_engagement_zone",
                    "area_name": compact_spaces(first_non_empty(row, "Zone_Name", "Zone")),
                    "borough_id": "",
                    "geom_wkt": geom_wkt,
                    "geom_srid": geom_srid,
                },
            )

        for row in iter_csv_rows(self.source_path("nycha_public_housing_developments_map_phvi-damg.csv")):
            borough_id = normalize_borough_id(row.get("BOROUGH", ""))
            geom_wkt, geom_srid = extract_geometry(row)
            self.emit(
                "housing_demographics",
                "public_housing_development",
                {
                    "source_dataset": "nycha_public_housing_developments_map_phvi-damg.csv",
                    "development_name": compact_spaces(row.get("DEVELOPMEN", "")),
                    "borough_id": borough_id,
                    "households": "",
                    "completion_date": "",
                    "geom_wkt": geom_wkt,
                    "geom_srid": geom_srid,
                },
            )

    def materialize_parks_recreation_poi(self) -> None:
        for row in iter_csv_rows(self.source_path("parks_properties_map_enfh-gkve.csv")):
            borough_id = normalize_borough_id(row.get("BOROUGH", ""))
            geom_wkt, geom_srid = extract_geometry(row, wkt_keys=("multipolygon",))
            self.emit(
                "parks_recreation_poi",
                "park_property",
                {
                    "source_dataset": "parks_properties_map_enfh-gkve.csv",
                    "park_name": compact_spaces(first_non_empty(row, "SIGNNAME", "NAME311", "LOCATION")),
                    "borough_id": borough_id,
                    "park_type": compact_spaces(first_non_empty(row, "TYPECATEGORY", "CLASS", "SUBCATEGORY")),
                    "area_acres": parse_float(row.get("ACRES", ""), precision=3),
                    "geom_wkt": geom_wkt,
                    "geom_srid": geom_srid,
                },
            )

        for file_name, facility_type, name_fields, borough_fields, geom_keys, date_field, type_field in (
            ("athletic_facilities_qnem-b8re.csv", "athletic_facility", ("LOCATION", "SYSTEM"), ("BOROUGH",), ("multipolygon",), "", "PRIMARY_SPORT"),
            ("play_areas_at6q-ktig.csv", "play_area", ("PARK_NAME",), ("BOROUGH",), ("the_geom",), "", ""),
            ("nyc_parks_pools_map_y5rm-wagw.csv", "pool", ("NAME",), ("BOROUGH",), ("polygon",), "", "POOLTYPE"),
            ("nyc_parks_ice_skating_rinks_xvww-awjk.csv", "ice_rink", ("NAME",), ("BOROUGH",), ("the_geom",), "", "COMMENTS"),
            ("beaches_ijwa-mn2v.csv", "beach", ("NAME",), ("BOROUGH",), ("multipolygon",), "", ""),
            ("golf_courses_uwmn-v7un.csv", "golf_course", ("NAME",), ("BOROUGH",), ("multipolygon",), "", ""),
        ):
            for row in iter_csv_rows(self.source_path(file_name)):
                borough_id = normalize_borough_id(first_non_empty(row, *borough_fields))
                geom_wkt, geom_srid = extract_geometry(row, wkt_keys=geom_keys)
                facility_name = compact_spaces(first_non_empty(row, *name_fields))
                self.emit(
                    "parks_recreation_poi",
                    "recreation_facility",
                    {
                        "source_dataset": file_name,
                        "facility_type": maybe_typed_label(facility_type, row.get(type_field, "")) if type_field else facility_type,
                        "facility_name": facility_name,
                        "park_id": "",
                        "borough_id": borough_id,
                        "opened_at": parse_date(row.get(date_field, "")) if date_field else "",
                        "geom_wkt": geom_wkt,
                        "geom_srid": geom_srid,
                    },
                )

        for row in iter_csv_rows(self.source_path("parks_trails_map_vjbm-hsyr.csv")):
            geom_wkt, geom_srid = extract_geometry(row, wkt_keys=("SHAPE",))
            self.emit(
                "parks_recreation_poi",
                "trail_segment",
                {
                    "source_dataset": "parks_trails_map_vjbm-hsyr.csv",
                    "trail_name": compact_spaces(first_non_empty(row, "Trail_Name", "Park_Name")),
                    "park_id": "",
                    "borough_id": "",
                    "surface_type": compact_spaces(row.get("Surface", "")),
                    "geom_wkt": geom_wkt,
                    "geom_srid": geom_srid,
                },
            )

        for row in iter_csv_rows(self.source_path("kayak_and_canoe_launch_k3sr-iysq.csv")):
            borough_id = normalize_borough_id(row.get("BOROUGH", ""))
            geom_wkt, geom_srid = extract_geometry(row)
            self.emit(
                "parks_recreation_poi",
                "water_access_site",
                {
                    "source_dataset": "kayak_and_canoe_launch_k3sr-iysq.csv",
                    "access_type": "kayak_launch",
                    "site_name": compact_spaces(first_non_empty(row, "NAME", "LOCATION", "SYSTEM")) or "Unnamed kayak launch",
                    "borough_id": borough_id,
                    "park_id": "",
                    "seasonal_open_date": "",
                    "geom_wkt": geom_wkt,
                    "geom_srid": geom_srid,
                },
            )

        for row in iter_csv_rows(self.source_path("nyc_saltwater_fishing_sites_map_mvte-j9h9.csv")):
            geom_wkt, geom_srid = extract_geometry(row)
            self.emit(
                "parks_recreation_poi",
                "water_access_site",
                {
                    "source_dataset": "nyc_saltwater_fishing_sites_map_mvte-j9h9.csv",
                    "access_type": "saltwater_fishing",
                    "site_name": compact_spaces(first_non_empty(row, "Site", "Ownership")) or "Unnamed saltwater fishing site",
                    "borough_id": "",
                    "park_id": "",
                    "seasonal_open_date": "",
                    "geom_wkt": geom_wkt,
                    "geom_srid": geom_srid,
                },
            )


OVERRIDE_EXPRESSIONS = {
    ("urban_planning_land_use", "building", "bbl"): (
        "CASE "
        "WHEN NULLIF(BTRIM(s.bbl), '') IS NOT NULL "
        " AND EXISTS (SELECT 1 FROM urban_planning_land_use.parcel p WHERE p.bbl = NULLIF(BTRIM(s.bbl), '')::BIGINT) "
        "THEN NULLIF(BTRIM(s.bbl), '')::BIGINT ELSE NULL END"
    ),
    ("urban_planning_land_use", "landmark_site", "bbl"): (
        "CASE "
        "WHEN NULLIF(BTRIM(s.bbl), '') IS NOT NULL "
        " AND EXISTS (SELECT 1 FROM urban_planning_land_use.parcel p WHERE p.bbl = NULLIF(BTRIM(s.bbl), '')::BIGINT) "
        "THEN NULLIF(BTRIM(s.bbl), '')::BIGINT ELSE NULL END"
    ),
    ("housing_demographics", "housing_project", "tract_id"): (
        "CASE "
        "WHEN NULLIF(BTRIM(s.tract_id), '') IS NOT NULL "
        " AND EXISTS (SELECT 1 FROM housing_demographics.census_tract t WHERE t.tract_id = NULLIF(BTRIM(s.tract_id), '')) "
        "THEN NULLIF(BTRIM(s.tract_id), '') ELSE NULL END"
    ),
}


VALIDATION_QUERIES = {
    "traffic_mobility": "SELECT COUNT(*) FROM traffic_mobility.pedestrian_signal p JOIN traffic_mobility.dim_community_district d ON ST_Within(p.geom, d.geom);",
    "emergency_response": "SELECT COUNT(*) FROM emergency_response.response_facility f JOIN emergency_response.safety_service_area a ON ST_Within(f.geom, a.geom);",
    "public_service_accessibility": "SELECT COUNT(*) FROM public_service_accessibility.public_facility f JOIN public_service_accessibility.service_zone z ON ST_Within(f.geom, z.geom);",
    "environmental_resilience": "SELECT COUNT(*) FROM environmental_resilience.environmental_asset a JOIN environmental_resilience.ecological_zone z ON ST_Intersects(a.geom, z.geom);",
    "urban_planning_land_use": "SELECT COUNT(*) FROM urban_planning_land_use.building b JOIN urban_planning_land_use.parcel p ON ST_Intersects(ST_PointOnSurface(b.geom), p.geom);",
    "housing_demographics": "SELECT COUNT(*) FROM housing_demographics.housing_project h JOIN housing_demographics.census_tract t ON ST_Intersects(h.geom, t.geom);",
    "parks_recreation_poi": "SELECT COUNT(*) FROM parks_recreation_poi.recreation_facility f JOIN parks_recreation_poi.park_property p ON ST_Within(f.geom, p.geom);",
}


def text_cast_expression(column_name: str, data_type: str, schema: str, table_name: str) -> str:
    override = OVERRIDE_EXPRESSIONS.get((schema, table_name, column_name))
    if override:
        return override
    source = f"NULLIF(BTRIM(s.{column_name}), '')"
    upper = data_type.upper()
    if "SMALLINT" in upper:
        return f"{source}::SMALLINT"
    if "BIGINT" in upper:
        return f"{source}::BIGINT"
    if "INTEGER" in upper:
        return f"{source}::INTEGER"
    if "DOUBLE PRECISION" in upper:
        return f"{source}::DOUBLE PRECISION"
    if "NUMERIC" in upper or "DECIMAL" in upper:
        return f"{source}::NUMERIC"
    if "DATE" in upper:
        return f"{source}::DATE"
    if "TIMESTAMP" in upper:
        return f"{source}::TIMESTAMP"
    return source


def geometry_expression(geom_type: str | None) -> str:
    if not geom_type:
        return ""
    if "POINT" in geom_type.upper() and "MULTIPOINT" not in geom_type.upper():
        return "etl_stage.to_point(s.geom_wkt, s.geom_srid)"
    if "MULTIPOLYGON" in geom_type.upper():
        return "etl_stage.to_multipolygon(s.geom_wkt, s.geom_srid)"
    if "MULTILINESTRING" in geom_type.upper():
        return "etl_stage.to_multilinestring(s.geom_wkt, s.geom_srid)"
    return "etl_stage.to_geom(s.geom_wkt, s.geom_srid)"


def staging_table_name(schema: str, table_name: str) -> str:
    return f"{schema}__{table_name}"


def render_loader_sql(manifest: list[MaterializedTable], *, load_ready_uri_prefix: str | None = CONTAINER_LOAD_READY_URI_PREFIX) -> str:
    _, ddl_sql = render_database_blueprints()
    manifest_by_key = {(item.schema, item.table_name): item for item in manifest}
    lines = ["\\set ON_ERROR_STOP on"]
    for cat in CATEGORY_TAXONOMY:
        lines.append(f"DROP SCHEMA IF EXISTS {cat.id} CASCADE;")
    lines.append("DROP SCHEMA IF EXISTS ggim CASCADE;")
    for schema_spec in SCENARIO_DATABASE_SPECS:
        lines.append(f"DROP SCHEMA IF EXISTS {schema_spec.id} CASCADE;")
    lines.append("DROP SCHEMA IF EXISTS etl_stage CASCADE;")
    lines.append(ddl_sql.rstrip())
    lines.extend(
        [
            "CREATE SCHEMA IF NOT EXISTS etl_stage;",
            "CREATE OR REPLACE FUNCTION etl_stage.to_geom(wkt TEXT, srid_text TEXT) RETURNS geometry AS $$",
            "DECLARE src_srid INTEGER;",
            "BEGIN",
            "  IF wkt IS NULL OR BTRIM(wkt) = '' THEN RETURN NULL; END IF;",
            "  src_srid := COALESCE(NULLIF(BTRIM(srid_text), '')::INTEGER, 4326);",
            "  IF src_srid = 4326 THEN",
            "    RETURN ST_MakeValid(ST_GeomFromText(wkt, 4326));",
            "  END IF;",
            "  RETURN ST_Transform(ST_MakeValid(ST_SetSRID(ST_GeomFromText(wkt), src_srid)), 4326);",
            "END;",
            "$$ LANGUAGE plpgsql IMMUTABLE;",
            "CREATE OR REPLACE FUNCTION etl_stage.to_point(wkt TEXT, srid_text TEXT) RETURNS geometry(Point, 4326) AS $$",
            "DECLARE g geometry;",
            "BEGIN",
            "  g := etl_stage.to_geom(wkt, srid_text);",
            "  IF g IS NULL THEN RETURN NULL; END IF;",
            "  RETURN ST_PointOnSurface(g)::geometry(Point, 4326);",
            "END;",
            "$$ LANGUAGE plpgsql IMMUTABLE;",
            "CREATE OR REPLACE FUNCTION etl_stage.to_multipolygon(wkt TEXT, srid_text TEXT) RETURNS geometry(MultiPolygon, 4326) AS $$",
            "DECLARE g geometry;",
            "BEGIN",
            "  g := etl_stage.to_geom(wkt, srid_text);",
            "  IF g IS NULL THEN RETURN NULL; END IF;",
            "  RETURN ST_Multi(ST_CollectionExtract(g, 3))::geometry(MultiPolygon, 4326);",
            "END;",
            "$$ LANGUAGE plpgsql IMMUTABLE;",
            "CREATE OR REPLACE FUNCTION etl_stage.to_multilinestring(wkt TEXT, srid_text TEXT) RETURNS geometry(MultiLineString, 4326) AS $$",
            "DECLARE g geometry;",
            "BEGIN",
            "  g := etl_stage.to_geom(wkt, srid_text);",
            "  IF g IS NULL THEN RETURN NULL; END IF;",
            "  RETURN ST_Multi(ST_CollectionExtract(g, 2))::geometry(MultiLineString, 4326);",
            "END;",
            "$$ LANGUAGE plpgsql IMMUTABLE;",
        ]
    )

    for schema_spec in SCENARIO_DATABASE_SPECS:
        reverse_tables = ", ".join(f"{schema_spec.id}.{table.name}" for table in reversed(schema_spec.tables))
        lines.append(f"TRUNCATE TABLE {reverse_tables} RESTART IDENTITY CASCADE;")

    for item in manifest:
        stage_name = staging_table_name(item.schema, item.table_name)
        column_defs = ", ".join(f"{column} TEXT" for column in item.columns)
        lines.append(f"DROP TABLE IF EXISTS etl_stage.{stage_name};")
        lines.append(f"CREATE TABLE etl_stage.{stage_name} ({column_defs});")
        copy_columns = ", ".join(item.columns)
        lines.append(
            f"\\copy etl_stage.{stage_name} ({copy_columns}) FROM '{copy_csv_path_for_sql(item, load_ready_uri_prefix)}' CSV HEADER"
        )

    for schema_spec in SCENARIO_DATABASE_SPECS:
        for table in schema_spec.tables:
            item = manifest_by_key[(schema_spec.id, table.name)]
            stage_name = staging_table_name(schema_spec.id, table.name)
            insert_columns: list[str] = []
            select_columns: list[str] = []
            for column in table.columns:
                if "SERIAL" in column.data_type.upper():
                    continue
                if column.name == "geom":
                    insert_columns.append("geom")
                    select_columns.append(geometry_expression(item.geom_type))
                    continue
                insert_columns.append(column.name)
                select_columns.append(text_cast_expression(column.name, column.data_type, schema_spec.id, table.name))
            where_clause = " WHERE NULLIF(BTRIM(s.geom_wkt), '') IS NOT NULL" if item.geom_type else ""
            lines.append(
                "INSERT INTO "
                f"{schema_spec.id}.{table.name} ({', '.join(insert_columns)}) "
                f"SELECT {', '.join(select_columns)} "
                f"FROM etl_stage.{stage_name} s{where_clause};"
            )

    lines.extend(post_load_enrichment_sql())
    return "\n".join(lines) + "\n"


def post_load_enrichment_sql() -> list[str]:
    return [
        "UPDATE traffic_mobility.traffic_corridor t SET borough_id = b.borough_id FROM traffic_mobility.dim_borough b WHERE t.borough_id IS NULL AND ST_Within(ST_Centroid(t.geom), b.geom);",
        "UPDATE traffic_mobility.traffic_corridor t SET cdta_code = d.cdta_code FROM traffic_mobility.dim_community_district d WHERE t.cdta_code IS NULL AND ST_Intersects(ST_Centroid(t.geom), d.geom);",
        "UPDATE traffic_mobility.pedestrian_signal p SET borough_id = b.borough_id FROM traffic_mobility.dim_borough b WHERE p.borough_id IS NULL AND ST_Within(p.geom, b.geom);",
        "UPDATE traffic_mobility.pedestrian_signal p SET cdta_code = d.cdta_code FROM traffic_mobility.dim_community_district d WHERE p.cdta_code IS NULL AND ST_Within(p.geom, d.geom);",
        "UPDATE traffic_mobility.pedestrian_count_site s SET corridor_id = (SELECT c.corridor_id FROM traffic_mobility.traffic_corridor c ORDER BY c.geom <-> s.geom LIMIT 1) WHERE s.corridor_id IS NULL;",
        "UPDATE traffic_mobility.curbside_asset a SET borough_id = b.borough_id FROM traffic_mobility.dim_borough b WHERE a.borough_id IS NULL AND ST_Within(a.geom, b.geom);",
        "UPDATE emergency_response.safety_service_area a SET borough_id = b.borough_id FROM emergency_response.dim_borough b WHERE a.borough_id IS NULL AND ST_Intersects(ST_Centroid(a.geom), b.geom);",
        "UPDATE emergency_response.response_facility f SET borough_id = b.borough_id FROM emergency_response.dim_borough b WHERE f.borough_id IS NULL AND ST_Within(f.geom, b.geom);",
        "UPDATE emergency_response.response_facility f SET service_area_id = a.service_area_id FROM emergency_response.safety_service_area a WHERE f.service_area_id IS NULL AND ST_Within(f.geom, a.geom);",
        "UPDATE emergency_response.alarm_asset a SET borough_id = b.borough_id FROM emergency_response.dim_borough b WHERE a.borough_id IS NULL AND ST_Within(a.geom, b.geom);",
        "UPDATE public_service_accessibility.service_zone z SET borough_id = b.borough_id FROM public_service_accessibility.dim_borough b WHERE z.borough_id IS NULL AND ST_Intersects(ST_Centroid(z.geom), b.geom);",
        "UPDATE public_service_accessibility.public_facility f SET borough_id = b.borough_id FROM public_service_accessibility.dim_borough b WHERE f.borough_id IS NULL AND ST_Within(f.geom, b.geom);",
        "UPDATE public_service_accessibility.public_facility f SET zone_id = (SELECT z.zone_id FROM public_service_accessibility.service_zone z WHERE ST_Within(f.geom, z.geom) ORDER BY ST_Area(z.geom) ASC LIMIT 1) WHERE f.zone_id IS NULL;",
        "UPDATE public_service_accessibility.amenity_point a SET borough_id = b.borough_id FROM public_service_accessibility.dim_borough b WHERE a.borough_id IS NULL AND ST_Within(a.geom, b.geom);",
        "UPDATE environmental_resilience.ecological_zone z SET borough_id = b.borough_id FROM environmental_resilience.dim_borough b WHERE z.borough_id IS NULL AND ST_Intersects(ST_Centroid(z.geom), b.geom);",
        "UPDATE environmental_resilience.green_infrastructure_site s SET borough_id = b.borough_id FROM environmental_resilience.dim_borough b WHERE s.borough_id IS NULL AND ST_Intersects(ST_Centroid(s.geom), b.geom);",
        "UPDATE environmental_resilience.environmental_asset a SET borough_id = b.borough_id FROM environmental_resilience.dim_borough b WHERE a.borough_id IS NULL AND ST_Intersects(ST_Centroid(a.geom), b.geom);",
        "UPDATE urban_planning_land_use.planning_area a SET borough_id = b.borough_id FROM urban_planning_land_use.dim_borough b WHERE a.borough_id IS NULL AND ST_Intersects(ST_Centroid(a.geom), b.geom);",
        "UPDATE urban_planning_land_use.parcel p SET borough_id = b.borough_id FROM urban_planning_land_use.dim_borough b WHERE p.borough_id IS NULL AND ST_Intersects(ST_Centroid(p.geom), b.geom);",
        "UPDATE urban_planning_land_use.parcel SET lot_area = COALESCE(lot_area, ROUND((ST_Area(geom::geography))::numeric, 2));",
        "UPDATE urban_planning_land_use.building b SET bbl = p.bbl FROM urban_planning_land_use.parcel p WHERE b.bbl IS NULL AND ST_Intersects(ST_PointOnSurface(b.geom), p.geom);",
        "UPDATE urban_planning_land_use.landmark_site l SET borough_id = b.borough_id FROM urban_planning_land_use.dim_borough b WHERE l.borough_id IS NULL AND ST_Intersects(ST_Centroid(l.geom), b.geom);",
        "UPDATE urban_planning_land_use.capital_project p SET borough_id = b.borough_id FROM urban_planning_land_use.dim_borough b WHERE p.borough_id IS NULL AND ST_Intersects(ST_Centroid(p.geom), b.geom);",
        "UPDATE housing_demographics.neighborhood_area a SET borough_id = b.borough_id FROM housing_demographics.dim_borough b WHERE a.borough_id IS NULL AND ST_Intersects(ST_Centroid(a.geom), b.geom);",
        "UPDATE housing_demographics.public_housing_development d SET borough_id = b.borough_id FROM housing_demographics.dim_borough b WHERE d.borough_id IS NULL AND ST_Intersects(ST_Centroid(d.geom), b.geom);",
        "UPDATE housing_demographics.housing_project h SET borough_id = COALESCE(h.borough_id, t.borough_id) FROM housing_demographics.census_tract t WHERE h.tract_id = t.tract_id;",
        "UPDATE housing_demographics.housing_project h SET tract_id = t.tract_id FROM housing_demographics.census_tract t WHERE h.tract_id IS NULL AND ST_Intersects(ST_Centroid(h.geom), t.geom);",
        "UPDATE housing_demographics.census_tract t SET puma_code = n.area_name FROM housing_demographics.neighborhood_area n WHERE n.area_type = 'puma' AND t.puma_code IS NULL AND ST_Intersects(ST_Centroid(t.geom), n.geom);",
        "UPDATE parks_recreation_poi.park_property p SET borough_id = b.borough_id FROM parks_recreation_poi.dim_borough b WHERE p.borough_id IS NULL AND ST_Intersects(ST_Centroid(p.geom), b.geom);",
        "UPDATE parks_recreation_poi.recreation_facility f SET borough_id = b.borough_id FROM parks_recreation_poi.dim_borough b WHERE f.borough_id IS NULL AND ST_Within(f.geom, b.geom);",
        "UPDATE parks_recreation_poi.recreation_facility f SET park_id = (SELECT p.park_id FROM parks_recreation_poi.park_property p WHERE ST_Contains(p.geom, f.geom) OR ST_DWithin(p.geom::geography, f.geom::geography, 250) ORDER BY p.geom <-> f.geom LIMIT 1) WHERE f.park_id IS NULL;",
        "UPDATE parks_recreation_poi.trail_segment t SET borough_id = b.borough_id FROM parks_recreation_poi.dim_borough b WHERE t.borough_id IS NULL AND ST_Intersects(ST_Centroid(t.geom), b.geom);",
        "UPDATE parks_recreation_poi.trail_segment t SET park_id = (SELECT p.park_id FROM parks_recreation_poi.park_property p WHERE ST_Intersects(p.geom, t.geom) ORDER BY ST_Area(ST_Intersection(p.geom, t.geom)) DESC NULLS LAST LIMIT 1) WHERE t.park_id IS NULL;",
        "UPDATE parks_recreation_poi.water_access_site s SET borough_id = b.borough_id FROM parks_recreation_poi.dim_borough b WHERE s.borough_id IS NULL AND ST_Within(s.geom, b.geom);",
        "UPDATE parks_recreation_poi.water_access_site s SET park_id = (SELECT p.park_id FROM parks_recreation_poi.park_property p WHERE ST_Contains(p.geom, s.geom) OR ST_DWithin(p.geom::geography, s.geom::geography, 400) ORDER BY p.geom <-> s.geom LIMIT 1) WHERE s.park_id IS NULL;",
        "UPDATE parks_recreation_poi.park_property SET area_acres = COALESCE(area_acres, ROUND((ST_Area(geom::geography) / 4046.8564224)::numeric, 3));",
    ]


def write_manifest(manifest: list[MaterializedTable], output_path: Path) -> None:
    payload = [
        {
            "schema": item.schema,
            "table_name": item.table_name,
            "path": str(item.path.resolve()),
            "row_count": item.row_count,
            "columns": item.columns,
            "geom_type": item.geom_type,
        }
        for item in manifest
    ]
    output_path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")


def write_summary(manifest: list[MaterializedTable], output_path: Path) -> None:
    lines = ["# ETL Materialization Summary", ""]
    for item in manifest:
        lines.append(f"- `{item.schema}.{item.table_name}`: {item.row_count} rows")
    output_path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def run_command(command: list[str], *, input_text: str | None = None, cwd: Path | None = None) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        command,
        input=input_text,
        text=True,
        capture_output=True,
        cwd=str(cwd) if cwd else None,
        check=False,
    )


def command_exists(name: str) -> bool:
    check = run_command(["where", name])
    return check.returncode == 0


def ensure_docker_container(container_name: str, db_name: str, db_user: str, db_password: str, db_port: int, docker_image: str) -> None:
    inspect = run_command(["docker", "inspect", container_name])
    if inspect.returncode != 0:
        create = run_command(
            [
                "docker",
                "run",
                "--name",
                container_name,
                "-e",
                f"POSTGRES_PASSWORD={db_password}",
                "-e",
                f"POSTGRES_DB={db_name}",
                "-p",
                f"{db_port}:5432",
                "-d",
                docker_image,
            ]
        )
        if create.returncode != 0:
            raise RuntimeError(create.stderr.strip() or create.stdout.strip())
    else:
        start = run_command(["docker", "start", container_name])
        if start.returncode not in {0, 1}:
            raise RuntimeError(start.stderr.strip() or start.stdout.strip())
    deadline = time.time() + 120
    while time.time() < deadline:
        ready = run_command(["docker", "exec", container_name, "pg_isready", "-U", db_user, "-d", db_name])
        if ready.returncode == 0:
            return
        time.sleep(2)
    raise RuntimeError("Timed out waiting for PostGIS container to become ready.")


def load_with_docker(
    sql_text: str,
    *,
    load_ready_dir: Path,
    load_ready_uri_prefix: str | None = None,
    container_name: str,
    db_name: str,
    db_user: str,
    db_password: str,
    db_port: int,
    docker_image: str,
) -> None:
    info = run_command(["docker", "info"])
    if info.returncode != 0:
        raise RuntimeError(info.stderr.strip() or "Docker daemon is not available.")
    ensure_docker_container(container_name, db_name, db_user, db_password, db_port, docker_image)
    cleanup = run_command(["docker", "exec", container_name, "sh", "-lc", "rm -rf /tmp/load_ready"])
    if cleanup.returncode != 0:
        raise RuntimeError(cleanup.stderr.strip() or cleanup.stdout.strip())
    copy = run_command(["docker", "cp", str(load_ready_dir), f"{container_name}:/tmp"])
    if copy.returncode != 0:
        raise RuntimeError(copy.stderr.strip() or copy.stdout.strip())
    prefix = (load_ready_uri_prefix or "").strip().rstrip("/") if load_ready_uri_prefix else ""
    if prefix and prefix in sql_text:
        docker_sql = sql_text.replace(prefix, "/tmp/load_ready")
    else:
        docker_sql = sql_text.replace(safe_path_string(load_ready_dir), "/tmp/load_ready")
    load = run_command(
        ["docker", "exec", "-i", container_name, "psql", "-v", "ON_ERROR_STOP=1", "-U", db_user, "-d", db_name],
        input_text=docker_sql,
    )
    if load.returncode != 0:
        raise RuntimeError(load.stderr.strip() or load.stdout.strip())


def load_with_psql(sql_path: Path, *, db_name: str, db_user: str, db_host: str, db_port: int) -> None:
    load = run_command(
        [
            "psql",
            "-v",
            "ON_ERROR_STOP=1",
            "-h",
            db_host,
            "-p",
            str(db_port),
            "-U",
            db_user,
            "-d",
            db_name,
            "-f",
            str(sql_path),
        ]
    )
    if load.returncode != 0:
        raise RuntimeError(load.stderr.strip() or load.stdout.strip())


def run_validation_queries(*, backend: str, container_name: str, db_name: str, db_user: str, db_host: str, db_port: int) -> dict[str, str]:
    results: dict[str, str] = {}
    for scenario_id, query in VALIDATION_QUERIES.items():
        if backend == "docker":
            result = run_command(
                ["docker", "exec", "-i", container_name, "psql", "-t", "-A", "-U", db_user, "-d", db_name, "-c", query]
            )
        else:
            result = run_command(
                ["psql", "-t", "-A", "-h", db_host, "-p", str(db_port), "-U", db_user, "-d", db_name, "-c", query]
            )
        if result.returncode != 0:
            raise RuntimeError(result.stderr.strip() or result.stdout.strip())
        results[scenario_id] = result.stdout.strip()
    return results


def run_etl(
    raw_dir: Path,
    artifacts_dir: Path,
    *,
    load: bool = False,
    load_backend: str = "auto",
    container_name: str = "spatial-postgis",
    db_name: str = "spatial_benchmark",
    db_user: str = "postgres",
    db_password: str = "postgres",
    db_host: str = "localhost",
    db_port: int = 5432,
    docker_image: str = "postgis/postgis:16-3.4",
    load_ready_uri_prefix: str | None = CONTAINER_LOAD_READY_URI_PREFIX,
) -> dict[str, Any]:
    etl_dir = artifacts_dir / "etl"
    etl_dir.mkdir(parents=True, exist_ok=True)
    cluster_validations = validate_database_sources(load_scenario_clusters(artifacts_dir))
    (etl_dir / "cluster_alignment.json").write_text(
        json.dumps(cluster_validations, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )
    context = EtlContext(raw_dir.resolve(), etl_dir.resolve())
    manifest = context.materialize()
    loader_sql = render_loader_sql(manifest, load_ready_uri_prefix=load_ready_uri_prefix)
    loader_sql_path = etl_dir / "postgis_load.sql"
    loader_sql_path.write_text(loader_sql, encoding="utf-8")
    write_manifest(manifest, etl_dir / "etl_manifest.json")
    write_summary(manifest, etl_dir / "etl_summary.md")

    backend_used = ""
    validation_results: dict[str, str] = {}
    if load:
        if load_backend == "auto":
            backend_used = "psql" if command_exists("psql") else "docker"
        else:
            backend_used = load_backend
        if backend_used == "psql":
            load_with_psql(loader_sql_path, db_name=db_name, db_user=db_user, db_host=db_host, db_port=db_port)
        else:
            load_with_docker(
                loader_sql,
                load_ready_dir=etl_dir / "load_ready",
                load_ready_uri_prefix=load_ready_uri_prefix,
                container_name=container_name,
                db_name=db_name,
                db_user=db_user,
                db_password=db_password,
                db_port=db_port,
                docker_image=docker_image,
            )
        validation_results = run_validation_queries(
            backend=backend_used,
            container_name=container_name,
            db_name=db_name,
            db_user=db_user,
            db_host=db_host,
            db_port=db_port,
        )
        (etl_dir / "validation_results.json").write_text(
            json.dumps(validation_results, indent=2, ensure_ascii=False),
            encoding="utf-8",
        )

    summary = {
        "etl_tables": len(manifest),
        "etl_rows": sum(item.row_count for item in manifest),
        "etl_dir": str(etl_dir.resolve()),
        "loader_sql": str(loader_sql_path.resolve()),
        "cluster_alignment": str((etl_dir / "cluster_alignment.json").resolve()),
        "loaded": load,
        "load_backend": backend_used,
    }
    if validation_results:
        summary["validation_results"] = validation_results
    return summary
