"""PostGIS migration core for synthesized spatial databases."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, time
import json
import logging
import re
from pathlib import Path
from typing import Any, Iterable, Mapping, Optional, Sequence

import psycopg2
from psycopg2 import sql
from psycopg2.extensions import connection as PGConnection

from ..models import CanonicalSpatialTable, SynthesizedSpatialDatabase
from ..utils import is_missing, stable_jsonify, to_text

LOGGER = logging.getLogger(__name__)

POSTGIS_EXTENSIONS = (
    "postgis",
    "postgis_topology",
    "postgis_raster",
)

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

WKT_PREFIXES = (
    "POINT",
    "MULTIPOINT",
    "LINESTRING",
    "MULTILINESTRING",
    "POLYGON",
    "MULTIPOLYGON",
    "GEOMETRYCOLLECTION",
)

PAIR_STRING_PATTERN = re.compile(
    r"^\s*[\[(]?\s*([+-]?(?:\d+\.?\d*|\.\d+))\s*[,; ]\s*([+-]?(?:\d+\.?\d*|\.\d+))\s*[\])]?\s*$"
)


def normalize_postgres_identifier(value: Any, prefix: str = "obj") -> str:
    cleaned = re.sub(r"[^a-z0-9_]+", "_", str(value or "").strip().lower())
    cleaned = re.sub(r"_+", "_", cleaned).strip("_")
    if not cleaned:
        cleaned = prefix
    if cleaned[0].isdigit():
        cleaned = f"{prefix}_{cleaned}"
    if len(cleaned) <= 63:
        return cleaned
    digest = hex(abs(hash(cleaned)) % (16**8))[2:].zfill(8)
    return f"{cleaned[:54].rstrip('_')}_{digest}"[:63]


def parse_srid(crs: Any) -> Optional[int]:
    text = to_text(crs)
    if not text:
        return None
    upper = text.upper()
    if "CRS84" in upper:
        return 4326
    for pattern in (r"EPSG[:/ ](\d+)", r"SRID[:= ](\d+)"):
        match = re.search(pattern, upper)
        if match:
            return int(match.group(1))
    if upper.isdigit():
        return int(upper)
    return None


def canonical_type_to_postgres_type(canonical_type: str, srid: int | None = None) -> str:
    canonical = to_text(canonical_type).lower() or "text"
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


@dataclass(frozen=True)
class PostGISConnectionSettings:
    host: str = "localhost"
    port: int = 5432
    user: str = "postgres"
    password: str = "123456"
    catalog: str = "syntheized"
    bootstrap_db: str = "postgres"


@dataclass
class ColumnMigrationSpec:
    raw_name: str
    canonical_name: str
    canonical_type: str
    description: str = ""
    srid: int | None = None
    derived: bool = False

    @property
    def postgres_type(self) -> str:
        return canonical_type_to_postgres_type(self.canonical_type, self.srid)

    @property
    def is_spatial(self) -> bool:
        return self.canonical_type == "spatial"


def _lookup_property(properties: Mapping[str, Any], candidates: Iterable[str]) -> Any:
    exact = {str(key): value for key, value in properties.items()}
    lowered = {str(key).lower(): value for key, value in properties.items()}
    for candidate in candidates:
        if candidate in exact:
            return exact[candidate]
    for candidate in candidates:
        lowered_candidate = candidate.lower()
        if lowered_candidate in lowered:
            return lowered[lowered_candidate]
    return None


def _is_geojson_geometry(value: Any) -> bool:
    return isinstance(value, Mapping) and "type" in value and (
        "coordinates" in value or "geometries" in value
    )


def _looks_like_wkt(value: Any) -> bool:
    return isinstance(value, str) and value.strip().upper().startswith(WKT_PREFIXES)


def _to_float(value: Any) -> Optional[float]:
    if is_missing(value):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _extract_coordinate_pair(value: Any) -> Optional[tuple[float, float]]:
    if isinstance(value, (list, tuple)) and len(value) >= 2:
        left = _to_float(value[0])
        right = _to_float(value[1])
        if left is not None and right is not None:
            return left, right
        return None
    if isinstance(value, str):
        match = PAIR_STRING_PATTERN.match(value)
        if match:
            return float(match.group(1)), float(match.group(2))
    return None


def _coerce_scalar_value(value: Any, canonical_type: str) -> Any:
    if is_missing(value):
        return None
    canonical = to_text(canonical_type).lower()
    if canonical == "integer":
        try:
            return int(float(value))
        except (TypeError, ValueError):
            return None
    if canonical == "double":
        try:
            return float(value)
        except (TypeError, ValueError):
            return None
    if canonical == "boolean":
        if isinstance(value, bool):
            return value
        lowered = to_text(value).lower()
        if lowered in {"true", "t", "1", "yes", "y"}:
            return True
        if lowered in {"false", "f", "0", "no", "n"}:
            return False
        return None
    if canonical == "date":
        if isinstance(value, date) and not isinstance(value, datetime):
            return value
        try:
            return date.fromisoformat(to_text(value)[:10])
        except ValueError:
            return None
    if canonical == "time":
        if isinstance(value, time):
            return value
        try:
            return time.fromisoformat(to_text(value))
        except ValueError:
            return None
    if canonical == "timestamp":
        if isinstance(value, datetime):
            return value
        try:
            return datetime.fromisoformat(to_text(value).replace("Z", "+00:00"))
        except ValueError:
            return None
    if isinstance(value, (Mapping, list)):
        return json.dumps(stable_jsonify(value), ensure_ascii=False)
    return value


def _coordinate_role(name: str) -> Optional[str]:
    normalized = normalize_postgres_identifier(name, prefix="col")
    if normalized in LONGITUDE_HINTS or any(normalized.endswith(f"_{hint}") for hint in LONGITUDE_HINTS):
        return "lon"
    if normalized in LATITUDE_HINTS or any(normalized.endswith(f"_{hint}") for hint in LATITUDE_HINTS):
        return "lat"
    return None


def _coordinate_base_key(name: str) -> str:
    normalized = normalize_postgres_identifier(name, prefix="col")
    for suffix in list(LONGITUDE_HINTS) + list(LATITUDE_HINTS):
        if normalized == suffix:
            return ""
        if normalized.endswith(f"_{suffix}"):
            return normalized[: -(len(suffix) + 1)]
    return normalized


def prepare_column_specs(table: CanonicalSpatialTable) -> list[ColumnMigrationSpec]:
    spatial_srid_by_name = {
        normalize_postgres_identifier(field.get("canonical_name"), prefix="geom"): parse_srid(field.get("crs"))
        for field in table.spatial_fields
        if to_text(field.get("canonical_name"))
    }
    specs: list[ColumnMigrationSpec] = []
    existing_names: set[str] = set()
    for column in table.normalized_schema:
        if not isinstance(column, Mapping):
            continue
        raw_name = to_text(column.get("name") or column.get("raw_name") or column.get("canonical_name"))
        canonical_name = normalize_postgres_identifier(
            column.get("canonical_name") or raw_name,
            prefix="col",
        )
        if canonical_name in existing_names:
            continue
        existing_names.add(canonical_name)
        canonical_type = to_text(column.get("canonical_type") or column.get("type") or "text").lower()
        specs.append(
            ColumnMigrationSpec(
                raw_name=raw_name or canonical_name,
                canonical_name=canonical_name,
                canonical_type=canonical_type,
                description=to_text(column.get("description")),
                srid=spatial_srid_by_name.get(canonical_name),
            )
        )
    for field in table.spatial_fields:
        canonical_name = normalize_postgres_identifier(field.get("canonical_name"), prefix="geom")
        if canonical_name in existing_names:
            continue
        existing_names.add(canonical_name)
        specs.append(
            ColumnMigrationSpec(
                raw_name=canonical_name,
                canonical_name=canonical_name,
                canonical_type="spatial",
                description="Derived spatial field.",
                srid=parse_srid(field.get("crs")),
                derived=True,
            )
        )
    return specs


def load_geojson_features(geojson_path: str | Path) -> list[dict[str, Any]]:
    path = Path(geojson_path)
    if not path.is_file():
        raise FileNotFoundError(f"GeoJSON file not found: {path}")
    payload = json.loads(path.read_text(encoding="utf-8"))
    if isinstance(payload, Mapping) and payload.get("type") == "FeatureCollection":
        return [dict(item) for item in payload.get("features", []) if isinstance(item, Mapping)]
    if isinstance(payload, Mapping) and payload.get("type") == "Feature":
        return [dict(payload)]
    if isinstance(payload, list):
        return [dict(item) for item in payload if isinstance(item, Mapping)]
    raise ValueError(f"Unsupported GeoJSON payload in {path}")


def _derive_geometry_from_feature(
    field_name: str,
    properties: Mapping[str, Any],
    specs: Sequence[ColumnMigrationSpec],
) -> Any:
    desired_base = field_name.removesuffix("_geometry")
    lon_candidates: list[tuple[str, float]] = []
    lat_candidates: list[tuple[str, float]] = []
    for spec in specs:
        role = _coordinate_role(spec.raw_name) or _coordinate_role(spec.canonical_name)
        if role is None:
            continue
        value = _lookup_property(properties, [spec.raw_name, spec.canonical_name])
        numeric = _to_float(value)
        if numeric is None:
            continue
        base_key = _coordinate_base_key(spec.raw_name or spec.canonical_name)
        if role == "lon" and -180.0 <= numeric <= 180.0:
            lon_candidates.append((base_key, numeric))
        if role == "lat" and -90.0 <= numeric <= 90.0:
            lat_candidates.append((base_key, numeric))
    for lon_base, lon_value in lon_candidates:
        for lat_base, lat_value in lat_candidates:
            if desired_base and lon_base == desired_base and lat_base == desired_base:
                return {"type": "Point", "coordinates": [lon_value, lat_value]}
    if lon_candidates and lat_candidates:
        return {"type": "Point", "coordinates": [lon_candidates[0][1], lat_candidates[0][1]]}
    return None


def build_feature_row(
    table: CanonicalSpatialTable,
    feature: Mapping[str, Any],
    specs: Sequence[ColumnMigrationSpec] | None = None,
) -> dict[str, Any]:
    specs = list(specs or prepare_column_specs(table))
    properties = feature.get("properties")
    if not isinstance(properties, Mapping):
        properties = {}
    geometry = feature.get("geometry")
    row: dict[str, Any] = {}
    for spec in specs:
        raw_value = _lookup_property(properties, [spec.raw_name, spec.canonical_name])
        if spec.is_spatial:
            value = raw_value
            if spec.derived:
                value = _derive_geometry_from_feature(spec.canonical_name, properties, specs)
            elif value is None and spec.raw_name in {"the_geom", "geometry"}:
                value = geometry
            row[spec.canonical_name] = value
        else:
            row[spec.canonical_name] = _coerce_scalar_value(raw_value, spec.canonical_type)
    return row


class PostGISSynthesizedDatabaseMigrator:
    """Create PostGIS schemas in a shared catalog and load synthesized tables."""

    def __init__(self, settings: PostGISConnectionSettings):
        self.settings = settings

    def _connect(self, database: str) -> PGConnection:
        return psycopg2.connect(
            host=self.settings.host,
            port=self.settings.port,
            user=self.settings.user,
            password=self.settings.password,
            dbname=database,
        )

    def _connect_autocommit(self, database: str) -> PGConnection:
        conn = self._connect(database)
        conn.autocommit = True
        return conn

    def _catalog_exists(self, catalog_name: str) -> bool:
        conn = self._connect_autocommit(self.settings.bootstrap_db)
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT 1 FROM pg_database WHERE datname = %s", (catalog_name,))
                return cur.fetchone() is not None
        finally:
            conn.close()

    def _ensure_catalog(self, catalog_name: str) -> None:
        if self._catalog_exists(catalog_name):
            LOGGER.info("Catalog already exists: %s", catalog_name)
            return
        conn = self._connect_autocommit(self.settings.bootstrap_db)
        try:
            with conn.cursor() as cur:
                cur.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(catalog_name)))
        finally:
            conn.close()
        LOGGER.info("Created catalog: %s", catalog_name)

    def _ensure_postgis_extensions(self, conn: PGConnection) -> None:
        with conn.cursor() as cur:
            for extension_name in POSTGIS_EXTENSIONS:
                cur.execute(
                    sql.SQL("CREATE EXTENSION IF NOT EXISTS {}").format(
                        sql.Identifier(extension_name)
                    )
                )

    def _comment_on_catalog(self, catalog_name: str, comment: str) -> None:
        conn = self._connect_autocommit(self.settings.bootstrap_db)
        try:
            with conn.cursor() as cur:
                cur.execute(
                    sql.SQL("COMMENT ON DATABASE {} IS %s").format(sql.Identifier(catalog_name)),
                    (comment,),
                )
        finally:
            conn.close()

    def _recreate_schema(self, conn: PGConnection, schema_name: str, comment: str) -> None:
        with conn.cursor() as cur:
            cur.execute(sql.SQL("DROP SCHEMA IF EXISTS {} CASCADE").format(sql.Identifier(schema_name)))
            cur.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(schema_name)))
            cur.execute(
                sql.SQL("COMMENT ON SCHEMA {} IS %s").format(sql.Identifier(schema_name)),
                (comment,),
            )

    def _build_column_comment(self, spec: ColumnMigrationSpec) -> str:
        parts = [
            f"canonical_type={spec.canonical_type}",
            f"source_column={spec.raw_name}",
        ]
        if spec.description:
            parts.insert(0, spec.description)
        if spec.srid is not None:
            parts.append(f"srid={spec.srid}")
        if spec.derived:
            parts.append("derived_spatial_field=true")
        return "; ".join(parts)

    def _build_table_comment(self, table: CanonicalSpatialTable) -> str:
        description = to_text(table.extra_metadata.get("description"))
        parts = [table.semantic_summary] if table.semantic_summary else []
        if description and description not in parts:
            parts.append(description)
        if table.themes:
            parts.append(f"themes={', '.join(table.themes)}")
        return " | ".join(part for part in parts if part)

    def _create_table(
        self,
        conn: PGConnection,
        schema_name: str,
        table_name: str,
        specs: Sequence[ColumnMigrationSpec],
        table_comment: str,
    ) -> None:
        definitions = [
            sql.SQL("{} {}").format(
                sql.Identifier(spec.canonical_name),
                sql.SQL(spec.postgres_type),
            )
            for spec in specs
        ]
        with conn.cursor() as cur:
            qualified_table = sql.SQL("{}.{}").format(
                sql.Identifier(schema_name),
                sql.Identifier(table_name),
            )
            cur.execute(sql.SQL("DROP TABLE IF EXISTS {} CASCADE").format(qualified_table))
            cur.execute(
                sql.SQL("CREATE TABLE {} ({})").format(
                    qualified_table,
                    sql.SQL(", ").join(definitions),
                )
            )
            cur.execute(
                sql.SQL("COMMENT ON TABLE {} IS %s").format(qualified_table),
                (table_comment,),
            )
            for spec in specs:
                cur.execute(
                    sql.SQL("COMMENT ON COLUMN {}.{} IS %s").format(
                        qualified_table,
                        sql.Identifier(spec.canonical_name),
                    ),
                    (self._build_column_comment(spec),),
                )

    def _spatial_expression(self, value: Any, srid: int | None) -> tuple[sql.SQL, list[Any]]:
        if is_missing(value):
            return sql.SQL("NULL"), []
        if _is_geojson_geometry(value):
            geometry_json = json.dumps(stable_jsonify(value), ensure_ascii=False)
            if srid is None:
                return sql.SQL("ST_GeomFromGeoJSON(%s)"), [geometry_json]
            return sql.SQL("ST_SetSRID(ST_GeomFromGeoJSON(%s), %s)"), [geometry_json, srid]
        if _looks_like_wkt(value):
            if srid is None:
                return sql.SQL("ST_GeomFromText(%s)"), [to_text(value)]
            return sql.SQL("ST_SetSRID(ST_GeomFromText(%s), %s)"), [to_text(value), srid]
        pair = _extract_coordinate_pair(value)
        if pair is not None:
            if srid is None:
                return sql.SQL("ST_MakePoint(%s, %s)"), [pair[0], pair[1]]
            return sql.SQL("ST_SetSRID(ST_MakePoint(%s, %s), %s)"), [pair[0], pair[1], srid]
        LOGGER.warning("Unsupported spatial value encountered; inserting NULL.")
        return sql.SQL("NULL"), []

    def _insert_features(
        self,
        conn: PGConnection,
        schema_name: str,
        table_name: str,
        table: CanonicalSpatialTable,
        specs: Sequence[ColumnMigrationSpec],
        features: Sequence[Mapping[str, Any]],
    ) -> None:
        if not features:
            return
        with conn.cursor() as cur:
            for feature in features:
                row = build_feature_row(table, feature, specs)
                columns_sql = [sql.Identifier(spec.canonical_name) for spec in specs]
                values_sql: list[sql.SQL] = []
                params: list[Any] = []
                for spec in specs:
                    value = row.get(spec.canonical_name)
                    if spec.is_spatial:
                        expression, expression_params = self._spatial_expression(value, spec.srid)
                        values_sql.append(expression)
                        params.extend(expression_params)
                    else:
                        values_sql.append(sql.SQL("%s"))
                        params.append(value)
                cur.execute(
                    sql.SQL("INSERT INTO {} ({}) VALUES ({})").format(
                        sql.SQL("{}.{}").format(
                            sql.Identifier(schema_name),
                            sql.Identifier(table_name),
                        ),
                        sql.SQL(", ").join(columns_sql),
                        sql.SQL(", ").join(values_sql),
                    ),
                    params,
                )

    def migrate_database(self, database: SynthesizedSpatialDatabase) -> str:
        catalog_name = normalize_postgres_identifier(self.settings.catalog, prefix="catalog")
        schema_name = normalize_postgres_identifier(database.database_id, prefix="schema")
        schema_comment = (
            f"Synthesized spatial database for city={database.city}; "
            f"database_id={database.database_id}; table_count={len(database.selected_tables)}"
        )
        self._ensure_catalog(catalog_name)
        self._comment_on_catalog(
            catalog_name,
            "Catalog storing synthesized spatial database schemas.",
        )
        conn = self._connect_autocommit(catalog_name)
        try:
            self._ensure_postgis_extensions(conn)
            self._recreate_schema(conn, schema_name, schema_comment)
            for table in database.selected_tables:
                specs = prepare_column_specs(table)
                table_name = normalize_postgres_identifier(table.table_name, prefix="table")
                self._create_table(conn, schema_name, table_name, specs, self._build_table_comment(table))
                source_path = table.extra_metadata.get("path") or table.extra_metadata.get("source_path")
                if not source_path:
                    raise FileNotFoundError(
                        f"Missing source path for table {table.table_id} in database {database.database_id}"
                    )
                features = load_geojson_features(source_path)
                self._insert_features(conn, schema_name, table_name, table, specs, features)
        finally:
            conn.close()
        location = f"{catalog_name}.{schema_name}"
        LOGGER.info(
            "Migrated synthesized database %s to PostGIS schema %s",
            database.database_id,
            location,
        )
        return location

    def migrate_databases(self, databases: Sequence[SynthesizedSpatialDatabase]) -> list[str]:
        migrated: list[str] = []
        for database in databases:
            migrated.append(self.migrate_database(database))
        return migrated
