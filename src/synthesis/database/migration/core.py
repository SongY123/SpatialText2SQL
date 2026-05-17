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
from psycopg2.extras import execute_values
from psycopg2.extensions import connection as PGConnection

from src.dataset_construction.type_converter import canonical_type_to_postgres_type

from ..models import CanonicalSpatialTable, SynthesizedSpatialDatabase
from ..utils import is_missing, stable_jsonify, to_text

LOGGER = logging.getLogger(__name__)
DEFAULT_INSERT_BATCH_SIZE = 10000
DEFAULT_SOURCE_ROW_LIMIT = 500000
DEFAULT_MIGRATION_MODE = "override"
APPEND_MIGRATION_MODE = "append"

POSTGIS_EXTENSIONS = (
    "postgis",
    "postgis_topology",
    "postgis_raster",
)
INVALID_GEOMETRY_ERROR_MARKERS = (
    "invalid geometry",
    "parse error",
)
ROWWISE_RETRY_SQLSTATE_PREFIXES = (
    "22",
    "23",
)
ROWWISE_RETRY_MESSAGE_MARKERS = (
    "invalid input syntax",
    "value too long",
    "numeric value out of range",
    "out of range",
    "violates",
    "malformed",
    "bad value",
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


def normalize_migration_mode(value: Any, default: str = DEFAULT_MIGRATION_MODE) -> str:
    text = to_text(value).lower()
    if not text:
        return default
    if text not in {DEFAULT_MIGRATION_MODE, APPEND_MIGRATION_MODE}:
        raise ValueError(
            f"Unsupported migration mode: {value!r}. Expected one of: "
            f"{DEFAULT_MIGRATION_MODE}, {APPEND_MIGRATION_MODE}."
        )
    return text


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


def _format_wkt_number(value: Any) -> str:
    numeric = float(value)
    if numeric.is_integer():
        return str(int(numeric))
    return format(numeric, ".15g")


def _format_wkt_position(value: Sequence[Any]) -> str:
    return " ".join(_format_wkt_number(item) for item in value)


def _format_wkt_positions(values: Sequence[Sequence[Any]]) -> str:
    return ", ".join(_format_wkt_position(item) for item in values)


def _format_wkt_rings(values: Sequence[Sequence[Sequence[Any]]]) -> str:
    return ", ".join(f"({_format_wkt_positions(ring)})" for ring in values)


def _geojson_to_wkt(value: Mapping[str, Any]) -> str:
    geometry_type = to_text(value.get("type")).upper()
    coordinates = value.get("coordinates")
    if geometry_type == "POINT" and isinstance(coordinates, Sequence):
        return f"POINT ({_format_wkt_position(coordinates)})"
    if geometry_type == "MULTIPOINT" and isinstance(coordinates, Sequence):
        parts = ", ".join(f"({_format_wkt_position(item)})" for item in coordinates)
        return f"MULTIPOINT ({parts})"
    if geometry_type == "LINESTRING" and isinstance(coordinates, Sequence):
        parts = _format_wkt_positions(coordinates)
        return f"LINESTRING ({parts})"
    if geometry_type == "MULTILINESTRING" and isinstance(coordinates, Sequence):
        parts = ", ".join(f"({_format_wkt_positions(line)})" for line in coordinates)
        return f"MULTILINESTRING ({parts})"
    if geometry_type == "POLYGON" and isinstance(coordinates, Sequence):
        parts = _format_wkt_rings(coordinates)
        return f"POLYGON ({parts})"
    if geometry_type == "MULTIPOLYGON" and isinstance(coordinates, Sequence):
        parts = ", ".join(f"({_format_wkt_rings(polygon)})" for polygon in coordinates)
        return f"MULTIPOLYGON ({parts})"
    if geometry_type == "GEOMETRYCOLLECTION":
        geometries = value.get("geometries") or []
        if isinstance(geometries, Sequence):
            return "GEOMETRYCOLLECTION ({})".format(
                ", ".join(_geojson_to_wkt(item) for item in geometries if isinstance(item, Mapping))
            )
    raise ValueError(f"Unsupported GeoJSON geometry type: {geometry_type or '<missing>'}")


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


def _normalize_spatial_to_wkt(value: Any) -> Optional[str]:
    if is_missing(value):
        return None
    if _is_geojson_geometry(value):
        try:
            return _geojson_to_wkt(value)
        except ValueError:
            LOGGER.warning("Unsupported GeoJSON geometry encountered; inserting NULL.")
            return None
    if _looks_like_wkt(value):
        return to_text(value)
    pair = _extract_coordinate_pair(value)
    if pair is not None:
        return f"POINT ({_format_wkt_number(pair[0])} {_format_wkt_number(pair[1])})"
    LOGGER.warning("Unsupported spatial value encountered; inserting NULL.")
    return None


def _error_message(exc: BaseException) -> str:
    parts = [str(exc)]
    pgerror = getattr(exc, "pgerror", None)
    if pgerror:
        parts.append(str(pgerror))
    return " | ".join(part for part in parts if part).strip()


def _error_code(exc: BaseException) -> str:
    return to_text(getattr(exc, "pgcode", ""))


def _is_invalid_geometry_error(exc: BaseException) -> bool:
    message = _error_message(exc).lower()
    return (
        "geometry" in message
        and any(marker in message for marker in INVALID_GEOMETRY_ERROR_MARKERS)
    )


def _is_rowwise_retryable_insert_error(exc: BaseException) -> bool:
    if _is_invalid_geometry_error(exc):
        return True
    code = _error_code(exc)
    if len(code) >= 2 and code[:2] in ROWWISE_RETRY_SQLSTATE_PREFIXES:
        return True
    message = _error_message(exc).lower()
    return any(marker in message for marker in ROWWISE_RETRY_MESSAGE_MARKERS)


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


def load_geojson_features(
    geojson_path: str | Path,
    source_row_limit: int = DEFAULT_SOURCE_ROW_LIMIT,
) -> list[dict[str, Any]]:
    path = Path(geojson_path)
    if not path.is_file():
        raise FileNotFoundError(f"GeoJSON file not found: {path}")
    payload = json.loads(path.read_text(encoding="utf-8"))
    if isinstance(payload, Mapping) and payload.get("type") == "FeatureCollection":
        features = [dict(item) for item in payload.get("features", []) if isinstance(item, Mapping)]
    elif isinstance(payload, Mapping) and payload.get("type") == "Feature":
        features = [dict(payload)]
    elif isinstance(payload, list):
        features = [dict(item) for item in payload if isinstance(item, Mapping)]
    else:
        raise ValueError(f"Unsupported GeoJSON payload in {path}")

    if int(source_row_limit) == -1:
        return features
    if int(source_row_limit) <= 0:
        raise ValueError(f"source_row_limit must be -1 or a positive integer, got: {source_row_limit!r}")
    if len(features) > int(source_row_limit):
        LOGGER.info(
            "Truncating features from %s to %s for %s",
            len(features),
            int(source_row_limit),
            path,
        )
        return features[: int(source_row_limit)]
    return features


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

    def __init__(
        self,
        settings: PostGISConnectionSettings,
        insert_batch_size: int = DEFAULT_INSERT_BATCH_SIZE,
        source_row_limit: int = DEFAULT_SOURCE_ROW_LIMIT,
        migration_mode: str = DEFAULT_MIGRATION_MODE,
    ):
        self.settings = settings
        if int(insert_batch_size) <= 0:
            raise ValueError(f"insert_batch_size must be positive, got: {insert_batch_size!r}")
        self.insert_batch_size = int(insert_batch_size)
        if int(source_row_limit) != -1 and int(source_row_limit) <= 0:
            raise ValueError(f"source_row_limit must be -1 or a positive integer, got: {source_row_limit!r}")
        self.source_row_limit = int(source_row_limit)
        self.migration_mode = normalize_migration_mode(migration_mode)

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
            LOGGER.warning("Catalog already exists: %s", catalog_name)
            return
        conn = self._connect_autocommit(self.settings.bootstrap_db)
        try:
            with conn.cursor() as cur:
                cur.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(catalog_name)))
        finally:
            conn.close()
        LOGGER.info("Created catalog: %s", catalog_name)

    def _prepare_catalog(self) -> str:
        catalog_name = normalize_postgres_identifier(self.settings.catalog, prefix="catalog")
        self._ensure_catalog(catalog_name)
        self._comment_on_catalog(
            catalog_name,
            "Catalog storing synthesized spatial database schemas.",
        )
        return catalog_name

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

    def _schema_exists(self, conn: PGConnection, schema_name: str) -> bool:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT 1 FROM information_schema.schemata WHERE schema_name = %s",
                (schema_name,),
            )
            return cur.fetchone() is not None

    def _ensure_schema(self, conn: PGConnection, schema_name: str, comment: str) -> None:
        with conn.cursor() as cur:
            cur.execute(sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(sql.Identifier(schema_name)))
            cur.execute(
                sql.SQL("COMMENT ON SCHEMA {} IS %s").format(sql.Identifier(schema_name)),
                (comment,),
            )

    def _list_tables(self, conn: PGConnection, schema_name: str) -> set[str]:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT table_name
                FROM information_schema.tables
                WHERE table_schema = %s AND table_type = 'BASE TABLE'
                """,
                (schema_name,),
            )
            return {
                to_text(row[0])
                for row in cur.fetchall()
                if row and to_text(row[0])
            }

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

    def _build_insert_template(self, specs: Sequence[ColumnMigrationSpec]) -> str:
        if not specs:
            return "()"
        parts: list[str] = []
        for spec in specs:
            if not spec.is_spatial:
                parts.append("%s")
                continue
            if spec.srid is None:
                parts.append("CASE WHEN %s IS NULL THEN NULL ELSE ST_GeomFromText(%s) END")
            else:
                parts.append(
                    f"CASE WHEN %s IS NULL THEN NULL ELSE ST_SetSRID(ST_GeomFromText(%s), {int(spec.srid)}) END"
                )
        return f"({', '.join(parts)})"

    def _build_insert_values(
        self,
        row: Mapping[str, Any],
        specs: Sequence[ColumnMigrationSpec],
    ) -> tuple[Any, ...]:
        values: list[Any] = []
        for spec in specs:
            value = row.get(spec.canonical_name)
            if spec.is_spatial:
                wkt_value = _normalize_spatial_to_wkt(value)
                values.extend([wkt_value, wkt_value])
            else:
                values.append(value)
        return tuple(values)

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
        qualified_table = sql.SQL("{}.{}").format(
            sql.Identifier(schema_name),
            sql.Identifier(table_name),
        )
        if specs:
            columns_sql = [sql.Identifier(spec.canonical_name) for spec in specs]
            insert_sql = sql.SQL("INSERT INTO {} ({}) VALUES %s").format(
                qualified_table,
                sql.SQL(", ").join(columns_sql),
            ).as_string(conn)
        else:
            insert_sql = sql.SQL("INSERT INTO {} VALUES %s").format(qualified_table).as_string(conn)
        template = self._build_insert_template(specs)
        rows = [
            self._build_insert_values(build_feature_row(table, feature, specs), specs)
            for feature in features
        ]
        page_size = min(self.insert_batch_size, len(rows))
        LOGGER.info(
            "Bulk inserting %s feature(s) into %s.%s with batch_size=%s",
            len(rows),
            schema_name,
            table_name,
            page_size,
        )
        skipped_rows = 0
        for batch_start in range(0, len(rows), page_size):
            batch_end = min(batch_start + page_size, len(rows))
            batch_rows = rows[batch_start:batch_end]
            try:
                self._execute_insert_batch(conn, insert_sql, batch_rows, template)
            except Exception as exc:
                self._safe_rollback(conn)
                if not _is_rowwise_retryable_insert_error(exc):
                    raise
                reason = (
                    "invalid geometry"
                    if _is_invalid_geometry_error(exc)
                    else "retryable insert error"
                )
                LOGGER.warning(
                    "%s detected while inserting batch rows %s-%s into %s.%s; retrying row-by-row. %s",
                    reason.capitalize(),
                    batch_start + 1,
                    batch_end,
                    schema_name,
                    table_name,
                    _error_message(exc),
                )
                for row_offset, row in enumerate(batch_rows, start=batch_start + 1):
                    try:
                        self._execute_insert_batch(conn, insert_sql, [row], template)
                    except Exception as row_exc:
                        self._safe_rollback(conn)
                        if not _is_rowwise_retryable_insert_error(row_exc):
                            raise
                        skipped_rows += 1
                        row_reason = (
                            "invalid geometry"
                            if _is_invalid_geometry_error(row_exc)
                            else "insert error"
                        )
                        LOGGER.warning(
                            "Skipping row %s for %s.%s due to %s: %s",
                            row_offset,
                            schema_name,
                            table_name,
                            row_reason,
                            _error_message(row_exc),
                        )
        if skipped_rows:
            LOGGER.warning(
                "Skipped %s row(s) while loading %s.%s due to retryable insert errors",
                skipped_rows,
                schema_name,
                table_name,
            )

    @staticmethod
    def _safe_rollback(conn: PGConnection) -> None:
        try:
            conn.rollback()
        except Exception:
            pass

    @staticmethod
    def _execute_insert_batch(
        conn: PGConnection,
        insert_sql: str,
        rows: Sequence[tuple[Any, ...]],
        template: str,
    ) -> None:
        if not rows:
            return
        with conn.cursor() as cur:
            execute_values(
                cur,
                insert_sql,
                rows,
                template=template,
                page_size=len(rows),
            )

    def migrate_database(self, database: SynthesizedSpatialDatabase) -> str:
        catalog_name = normalize_postgres_identifier(self.settings.catalog, prefix="catalog")
        schema_name = normalize_postgres_identifier(database.database_id, prefix="schema")
        schema_comment = (
            f"Synthesized spatial database for city={database.city}; "
            f"database_id={database.database_id}; table_count={len(database.selected_tables)}"
        )
        conn = self._connect_autocommit(catalog_name)
        try:
            self._ensure_postgis_extensions(conn)
            existing_tables: set[str] = set()
            schema_exists = self._schema_exists(conn, schema_name)
            if self.migration_mode == DEFAULT_MIGRATION_MODE:
                self._recreate_schema(conn, schema_name, schema_comment)
            elif schema_exists:
                self._ensure_schema(conn, schema_name, schema_comment)
                existing_tables = self._list_tables(conn, schema_name)
                LOGGER.info(
                    "Append mode detected existing schema %s with %s table(s); only missing tables will be created.",
                    schema_name,
                    len(existing_tables),
                )
            else:
                self._ensure_schema(conn, schema_name, schema_comment)
                LOGGER.info(
                    "Append mode will create new schema %s for synthesized database %s.",
                    schema_name,
                    database.database_id,
                )
            for table in database.selected_tables:
                specs = prepare_column_specs(table)
                table_name = normalize_postgres_identifier(table.table_name, prefix="table")
                if self.migration_mode == APPEND_MIGRATION_MODE and table_name in existing_tables:
                    LOGGER.info(
                        "Skipping existing table %s.%s in append mode.",
                        schema_name,
                        table_name,
                    )
                    continue
                self._create_table(conn, schema_name, table_name, specs, self._build_table_comment(table))
                existing_tables.add(table_name)
                source_path = table.extra_metadata.get("path") or table.extra_metadata.get("source_path")
                if not source_path:
                    raise FileNotFoundError(
                        f"Missing source path for table {table.table_id} in database {database.database_id}"
                    )
                features = load_geojson_features(source_path, source_row_limit=self.source_row_limit)
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
        if not databases:
            return []
        self._prepare_catalog()
        migrated: list[str] = []
        for database in databases:
            migrated.append(self.migrate_database(database))
        return migrated
