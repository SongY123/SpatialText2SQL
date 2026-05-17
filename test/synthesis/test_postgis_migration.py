import json
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from src.synthesis.database.io import load_synthesized_databases
from src.synthesis.database.migration import (
    PostGISConnectionSettings,
    PostGISSynthesizedDatabaseMigrator,
    build_feature_row,
    canonical_type_to_postgres_type,
    load_geojson_features,
    load_migration_config,
    normalize_postgres_identifier,
    parse_srid,
    prepare_column_specs,
)
from src.synthesis.database.migration.core import DEFAULT_INSERT_BATCH_SIZE, DEFAULT_SOURCE_ROW_LIMIT
from src.synthesis.database.models import CanonicalSpatialTable, SynthesizedSpatialDatabase


class PostGISMigrationTests(unittest.TestCase):
    def test_normalize_postgres_identifier(self):
        self.assertEqual(normalize_postgres_identifier("NYC Hydrants", prefix="db"), "nyc_hydrants")
        self.assertEqual(normalize_postgres_identifier("2026-table", prefix="db"), "db_2026_table")

    def test_parse_srid_and_type_mapping(self):
        self.assertEqual(parse_srid("EPSG:4326"), 4326)
        self.assertEqual(parse_srid("urn:ogc:def:crs:OGC:1.3:CRS84"), 4326)
        self.assertEqual(canonical_type_to_postgres_type("integer"), "BIGINT")
        self.assertEqual(canonical_type_to_postgres_type("spatial", 4326), "geometry(GEOMETRY,4326)")

    def test_prepare_specs_and_build_feature_row(self):
        table = CanonicalSpatialTable.from_dict(
            {
                "table_id": "t1",
                "city": "nyc",
                "table_name": "hydrants",
                "normalized_schema": [
                    {
                        "name": "BORO",
                        "canonical_name": "boro",
                        "canonical_type": "integer",
                    },
                    {
                        "name": "longitude",
                        "canonical_name": "longitude",
                        "canonical_type": "double",
                    },
                    {
                        "name": "latitude",
                        "canonical_name": "latitude",
                        "canonical_type": "double",
                    },
                ],
                "spatial_fields": [{"canonical_name": "geometry", "crs": "EPSG:4326"}],
            }
        )
        specs = prepare_column_specs(table)
        self.assertEqual(specs[-1].canonical_name, "geometry")
        self.assertTrue(specs[-1].derived)
        feature = {
            "type": "Feature",
            "properties": {"BORO": "1", "longitude": -73.9, "latitude": 40.7},
            "geometry": {"type": "Point", "coordinates": [-73.9, 40.7]},
        }
        row = build_feature_row(table, feature, specs)
        self.assertEqual(row["boro"], 1)
        self.assertEqual(row["geometry"]["type"], "Point")

    def test_load_synthesized_databases(self):
        database = SynthesizedSpatialDatabase.from_selected_tables(
            database_id="nyc_0001",
            city="nyc",
            selected_tables=[
                CanonicalSpatialTable.from_dict(
                    {
                        "table_id": "t1",
                        "city": "nyc",
                        "table_name": "hydrants",
                        "normalized_schema": [],
                        "spatial_fields": [],
                    }
                )
            ],
            sampling_trace=[],
            graph_stats={},
            synthesize_config={},
        )
        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "synthesized.jsonl"
            path.write_text(json.dumps(database.to_dict(), ensure_ascii=False) + "\n", encoding="utf-8")
            loaded = load_synthesized_databases(str(path))
        self.assertEqual(len(loaded), 1)
        self.assertEqual(loaded[0].database_id, "nyc_0001")
        self.assertEqual(loaded[0].selected_tables[0].table_name, "hydrants")

    def test_load_migration_config(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            config_dir = root / "config"
            config_dir.mkdir()
            data_dir = root / "data" / "processed"
            data_dir.mkdir(parents=True)
            config_path = config_dir / "migrate.yaml"
            config_path.write_text(
                "\n".join(
                    [
                        "input: data/processed/synthesized_spatial_databases.jsonl",
                        "cities: nyc,sf",
                        "insert_batch_size: 2000",
                        "source_row_limit: -1",
                        "migration_mode: append",
                        "database:",
                        "  host: db.local",
                        "  port: 6543",
                        "  user: admin",
                        '  password: "secret"',
                        "  catalog: syntheized",
                        "  bootstrap_db: postgres",
                        "logging:",
                        "  level: DEBUG",
                    ]
                ),
                encoding="utf-8",
            )
            loaded = load_migration_config(config_path)
        self.assertEqual(loaded.cities, "nyc,sf")
        self.assertEqual(loaded.connection.host, "db.local")
        self.assertEqual(loaded.connection.port, 6543)
        self.assertEqual(loaded.connection.catalog, "syntheized")
        self.assertEqual(loaded.connection.bootstrap_db, "postgres")
        self.assertEqual(loaded.insert_batch_size, 2000)
        self.assertEqual(loaded.source_row_limit, -1)
        self.assertEqual(loaded.migration_mode, "append")
        self.assertTrue(loaded.input_path.endswith("data/processed/synthesized_spatial_databases.jsonl"))

    def test_load_migration_config_uses_default_insert_batch_size(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            config_path = Path(tmpdir) / "migrate.yaml"
            config_path.write_text("{}", encoding="utf-8")
            loaded = load_migration_config(config_path)
        self.assertEqual(loaded.insert_batch_size, DEFAULT_INSERT_BATCH_SIZE)
        self.assertEqual(loaded.source_row_limit, DEFAULT_SOURCE_ROW_LIMIT)

    def test_load_migration_config_rejects_invalid_source_row_limit(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            config_path = Path(tmpdir) / "migrate.yaml"
            config_path.write_text("source_row_limit: 0", encoding="utf-8")
            with self.assertRaises(ValueError):
                load_migration_config(config_path)

    def test_load_migration_config_rejects_invalid_migration_mode(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            config_path = Path(tmpdir) / "migrate.yaml"
            config_path.write_text("migration_mode: resume", encoding="utf-8")
            with self.assertRaises(ValueError):
                load_migration_config(config_path)

    def test_load_geojson_features_applies_source_row_limit(self):
        payload = {
            "type": "FeatureCollection",
            "features": [
                {"type": "Feature", "properties": {"id": 1}, "geometry": None},
                {"type": "Feature", "properties": {"id": 2}, "geometry": None},
                {"type": "Feature", "properties": {"id": 3}, "geometry": None},
            ],
        }
        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "sample.geojson"
            path.write_text(json.dumps(payload), encoding="utf-8")
            limited = load_geojson_features(path, source_row_limit=2)
            unlimited = load_geojson_features(path, source_row_limit=-1)
        self.assertEqual(len(limited), 2)
        self.assertEqual(len(unlimited), 3)

    def test_ensure_catalog_uses_autocommit_connection(self):
        class FakeCursor:
            def __init__(self):
                self.executed = []

            def __enter__(self):
                return self

            def __exit__(self, exc_type, exc, tb):
                return False

            def execute(self, query, params=None):
                self.executed.append((query, params))

        class FakeConnection:
            def __init__(self):
                self.closed = False
                self.cursor_obj = FakeCursor()

            def cursor(self):
                return self.cursor_obj

            def close(self):
                self.closed = True

        migrator = PostGISSynthesizedDatabaseMigrator(PostGISConnectionSettings())
        fake_conn = FakeConnection()
        with mock.patch.object(migrator, "_catalog_exists", return_value=False):
            with mock.patch.object(migrator, "_connect_autocommit", return_value=fake_conn) as patched:
                migrator._ensure_catalog("syntheized")
        patched.assert_called_once_with("postgres")
        self.assertTrue(fake_conn.closed)
        self.assertEqual(len(fake_conn.cursor_obj.executed), 1)

    def test_ensure_catalog_warns_when_catalog_exists(self):
        migrator = PostGISSynthesizedDatabaseMigrator(PostGISConnectionSettings(catalog="synthesis"))
        with mock.patch.object(migrator, "_catalog_exists", return_value=True):
            with mock.patch.object(migrator, "_connect_autocommit") as connect_autocommit:
                with self.assertLogs("src.synthesis.database.migration.core", level="WARNING") as captured:
                    migrator._ensure_catalog("synthesis")
        connect_autocommit.assert_not_called()
        self.assertIn("Catalog already exists: synthesis", "\n".join(captured.output))

    def test_migrate_database_override_recreates_schema(self):
        class FakeConnection:
            def __init__(self):
                self.closed = False

            def close(self):
                self.closed = True

        table = CanonicalSpatialTable.from_dict(
            {
                "table_id": "t1",
                "city": "nyc",
                "table_name": "hydrants",
                "normalized_schema": [
                    {"name": "id", "canonical_name": "id", "canonical_type": "integer"},
                    {"name": "the_geom", "canonical_name": "the_geom", "canonical_type": "spatial"},
                ],
                "spatial_fields": [{"canonical_name": "the_geom", "crs": "EPSG:4326"}],
                "path": "/tmp/hydrants.geojson",
            }
        )
        database = SynthesizedSpatialDatabase.from_selected_tables(
            database_id="NYC Demo DB",
            city="nyc",
            selected_tables=[table],
            sampling_trace=[],
            graph_stats={},
            synthesize_config={},
        )

        migrator = PostGISSynthesizedDatabaseMigrator(PostGISConnectionSettings())
        fake_conn = FakeConnection()
        expected_schema = normalize_postgres_identifier(database.database_id, prefix="schema")
        with mock.patch.object(migrator, "_connect_autocommit", return_value=fake_conn) as connect_autocommit:
            with mock.patch.object(migrator, "_ensure_postgis_extensions") as ensure_extensions:
                with mock.patch.object(migrator, "_schema_exists", return_value=True) as schema_exists:
                    with mock.patch.object(migrator, "_ensure_schema") as ensure_schema:
                        with mock.patch.object(migrator, "_list_tables") as list_tables:
                            with mock.patch.object(migrator, "_recreate_schema") as recreate_schema:
                                with mock.patch.object(migrator, "_create_table") as create_table:
                                    with mock.patch.object(migrator, "_insert_features") as insert_features:
                                        with mock.patch(
                                            "src.synthesis.database.migration.core.load_geojson_features",
                                            return_value=[],
                                        ) as patched_load:
                                            location = migrator.migrate_database(database)

        connect_autocommit.assert_called_once_with("syntheized")
        ensure_extensions.assert_called_once_with(fake_conn)
        schema_exists.assert_called_once_with(fake_conn, expected_schema)
        recreate_schema.assert_called_once()
        ensure_schema.assert_not_called()
        list_tables.assert_not_called()
        self.assertEqual(recreate_schema.call_args.args[1], expected_schema)
        create_table.assert_called_once()
        self.assertEqual(create_table.call_args.args[1], expected_schema)
        patched_load.assert_called_once_with("/tmp/hydrants.geojson", source_row_limit=DEFAULT_SOURCE_ROW_LIMIT)
        insert_features.assert_called_once()
        self.assertEqual(insert_features.call_args.args[1], expected_schema)
        self.assertTrue(fake_conn.closed)
        self.assertEqual(location, f"syntheized.{expected_schema}")

    def test_migrate_database_append_skips_existing_tables(self):
        class FakeConnection:
            def __init__(self):
                self.closed = False

            def close(self):
                self.closed = True

        hydrants = CanonicalSpatialTable.from_dict(
            {
                "table_id": "t1",
                "city": "nyc",
                "table_name": "hydrants",
                "normalized_schema": [
                    {"name": "id", "canonical_name": "id", "canonical_type": "integer"},
                ],
                "spatial_fields": [],
                "path": "/tmp/hydrants.geojson",
            }
        )
        schools = CanonicalSpatialTable.from_dict(
            {
                "table_id": "t2",
                "city": "nyc",
                "table_name": "schools",
                "normalized_schema": [
                    {"name": "id", "canonical_name": "id", "canonical_type": "integer"},
                ],
                "spatial_fields": [],
                "path": "/tmp/schools.geojson",
            }
        )
        database = SynthesizedSpatialDatabase.from_selected_tables(
            database_id="NYC Demo DB",
            city="nyc",
            selected_tables=[hydrants, schools],
            sampling_trace=[],
            graph_stats={},
            synthesize_config={},
        )

        migrator = PostGISSynthesizedDatabaseMigrator(
            PostGISConnectionSettings(),
            migration_mode="append",
        )
        fake_conn = FakeConnection()
        expected_schema = normalize_postgres_identifier(database.database_id, prefix="schema")
        existing_table = normalize_postgres_identifier(hydrants.table_name, prefix="table")
        missing_table = normalize_postgres_identifier(schools.table_name, prefix="table")
        with mock.patch.object(migrator, "_connect_autocommit", return_value=fake_conn) as connect_autocommit:
            with mock.patch.object(migrator, "_ensure_postgis_extensions") as ensure_extensions:
                with mock.patch.object(migrator, "_schema_exists", return_value=True) as schema_exists:
                    with mock.patch.object(migrator, "_ensure_schema") as ensure_schema:
                        with mock.patch.object(migrator, "_list_tables", return_value={existing_table}) as list_tables:
                            with mock.patch.object(migrator, "_recreate_schema") as recreate_schema:
                                with mock.patch.object(migrator, "_create_table") as create_table:
                                    with mock.patch.object(migrator, "_insert_features") as insert_features:
                                        with mock.patch(
                                            "src.synthesis.database.migration.core.load_geojson_features",
                                            return_value=[],
                                        ) as patched_load:
                                            location = migrator.migrate_database(database)

        connect_autocommit.assert_called_once_with("syntheized")
        ensure_extensions.assert_called_once_with(fake_conn)
        schema_exists.assert_called_once_with(fake_conn, expected_schema)
        ensure_schema.assert_called_once()
        list_tables.assert_called_once_with(fake_conn, expected_schema)
        recreate_schema.assert_not_called()
        create_table.assert_called_once()
        self.assertEqual(create_table.call_args.args[2], missing_table)
        patched_load.assert_called_once_with("/tmp/schools.geojson", source_row_limit=DEFAULT_SOURCE_ROW_LIMIT)
        insert_features.assert_called_once()
        self.assertEqual(insert_features.call_args.args[2], missing_table)
        self.assertTrue(fake_conn.closed)
        self.assertEqual(location, f"syntheized.{expected_schema}")

    def test_migrate_database_append_drops_unexpected_tables(self):
        class FakeConnection:
            def __init__(self):
                self.closed = False

            def close(self):
                self.closed = True

        hydrants = CanonicalSpatialTable.from_dict(
            {
                "table_id": "t1",
                "city": "nyc",
                "table_name": "hydrants",
                "normalized_schema": [
                    {"name": "id", "canonical_name": "id", "canonical_type": "integer"},
                ],
                "spatial_fields": [],
                "path": "/tmp/hydrants.geojson",
            }
        )
        database = SynthesizedSpatialDatabase.from_selected_tables(
            database_id="NYC Demo DB",
            city="nyc",
            selected_tables=[hydrants],
            sampling_trace=[],
            graph_stats={},
            synthesize_config={},
        )

        migrator = PostGISSynthesizedDatabaseMigrator(
            PostGISConnectionSettings(),
            migration_mode="append",
        )
        fake_conn = FakeConnection()
        expected_schema = normalize_postgres_identifier(database.database_id, prefix="schema")
        existing_table = normalize_postgres_identifier(hydrants.table_name, prefix="table")
        unexpected_table = "legacy_table"
        with mock.patch.object(migrator, "_connect_autocommit", return_value=fake_conn):
            with mock.patch.object(migrator, "_ensure_postgis_extensions"):
                with mock.patch.object(migrator, "_schema_exists", return_value=True):
                    with mock.patch.object(migrator, "_ensure_schema"):
                        with mock.patch.object(
                            migrator,
                            "_list_tables",
                            return_value={existing_table, unexpected_table},
                        ):
                            with mock.patch.object(migrator, "_drop_tables") as drop_tables:
                                with mock.patch.object(migrator, "_recreate_schema") as recreate_schema:
                                    with mock.patch.object(migrator, "_create_table") as create_table:
                                        with mock.patch.object(migrator, "_insert_features") as insert_features:
                                            with mock.patch(
                                                "src.synthesis.database.migration.core.load_geojson_features",
                                                return_value=[],
                                            ):
                                                migrator.migrate_database(database)

        drop_tables.assert_called_once_with(fake_conn, expected_schema, [unexpected_table])
        recreate_schema.assert_not_called()
        create_table.assert_not_called()
        insert_features.assert_not_called()

    def test_migrate_database_append_creates_new_schema_without_listing_tables(self):
        class FakeConnection:
            def __init__(self):
                self.closed = False

            def close(self):
                self.closed = True

        table = CanonicalSpatialTable.from_dict(
            {
                "table_id": "t1",
                "city": "nyc",
                "table_name": "hydrants",
                "normalized_schema": [
                    {"name": "id", "canonical_name": "id", "canonical_type": "integer"},
                ],
                "spatial_fields": [],
                "path": "/tmp/hydrants.geojson",
            }
        )
        database = SynthesizedSpatialDatabase.from_selected_tables(
            database_id="NYC Demo DB",
            city="nyc",
            selected_tables=[table],
            sampling_trace=[],
            graph_stats={},
            synthesize_config={},
        )

        migrator = PostGISSynthesizedDatabaseMigrator(
            PostGISConnectionSettings(),
            migration_mode="append",
        )
        fake_conn = FakeConnection()
        expected_schema = normalize_postgres_identifier(database.database_id, prefix="schema")
        with mock.patch.object(migrator, "_connect_autocommit", return_value=fake_conn):
            with mock.patch.object(migrator, "_ensure_postgis_extensions"):
                with mock.patch.object(migrator, "_schema_exists", return_value=False) as schema_exists:
                    with mock.patch.object(migrator, "_ensure_schema") as ensure_schema:
                        with mock.patch.object(migrator, "_list_tables") as list_tables:
                            with mock.patch.object(migrator, "_recreate_schema") as recreate_schema:
                                with mock.patch.object(migrator, "_create_table") as create_table:
                                    with mock.patch.object(migrator, "_insert_features") as insert_features:
                                        with mock.patch(
                                            "src.synthesis.database.migration.core.load_geojson_features",
                                            return_value=[],
                                        ):
                                            migrator.migrate_database(database)

        schema_exists.assert_called_once_with(fake_conn, expected_schema)
        ensure_schema.assert_called_once()
        list_tables.assert_not_called()
        recreate_schema.assert_not_called()
        create_table.assert_called_once()
        insert_features.assert_called_once()

    def test_migrate_databases_prepares_catalog_once(self):
        database_one = SynthesizedSpatialDatabase.from_selected_tables(
            database_id="db1",
            city="nyc",
            selected_tables=[],
            sampling_trace=[],
            graph_stats={},
            synthesize_config={},
        )
        database_two = SynthesizedSpatialDatabase.from_selected_tables(
            database_id="db2",
            city="sf",
            selected_tables=[],
            sampling_trace=[],
            graph_stats={},
            synthesize_config={},
        )
        migrator = PostGISSynthesizedDatabaseMigrator(PostGISConnectionSettings())
        with mock.patch.object(migrator, "_prepare_catalog") as prepare_catalog:
            with mock.patch.object(migrator, "migrate_database", side_effect=["syntheized.db1", "syntheized.db2"]) as migrate_database:
                locations = migrator.migrate_databases([database_one, database_two])

        prepare_catalog.assert_called_once_with()
        self.assertEqual(migrate_database.call_count, 2)
        self.assertEqual(locations, ["syntheized.db1", "syntheized.db2"])

    def test_bulk_insert_value_builder_converts_spatial_value_to_wkt(self):
        table = CanonicalSpatialTable.from_dict(
            {
                "table_id": "t1",
                "city": "nyc",
                "table_name": "hydrants",
                "normalized_schema": [
                    {"name": "id", "canonical_name": "id", "canonical_type": "integer"},
                    {"name": "the_geom", "canonical_name": "the_geom", "canonical_type": "spatial"},
                ],
                "spatial_fields": [{"canonical_name": "the_geom", "crs": "EPSG:4326"}],
            }
        )
        specs = prepare_column_specs(table)
        migrator = PostGISSynthesizedDatabaseMigrator(PostGISConnectionSettings())
        values = migrator._build_insert_values(
            {
                "id": 1,
                "the_geom": {"type": "Point", "coordinates": [-73.9, 40.7]},
            },
            specs,
        )
        self.assertEqual(values[0], 1)
        self.assertEqual(values[1], "POINT (-73.9 40.7)")
        self.assertEqual(values[2], "POINT (-73.9 40.7)")

    def test_insert_features_skips_invalid_geometry_rows(self):
        class FakeCursor:
            def __enter__(self):
                return self

            def __exit__(self, exc_type, exc, tb):
                return False

        class FakeConnection:
            def __init__(self):
                self.rollback_count = 0

            def cursor(self):
                return FakeCursor()

            def rollback(self):
                self.rollback_count += 1

        class InvalidGeometryError(Exception):
            def __init__(self, message: str):
                super().__init__(message)
                self.pgerror = message

        table = CanonicalSpatialTable.from_dict(
            {
                "table_id": "t1",
                "city": "nyc",
                "table_name": "hydrants",
                "normalized_schema": [
                    {"name": "id", "canonical_name": "id", "canonical_type": "integer"},
                    {"name": "the_geom", "canonical_name": "the_geom", "canonical_type": "spatial"},
                ],
                "spatial_fields": [{"canonical_name": "the_geom", "crs": "EPSG:4326"}],
            }
        )
        specs = prepare_column_specs(table)
        features = [
            {"type": "Feature", "properties": {"id": 1}, "geometry": {"type": "Point", "coordinates": [-73.9, 40.7]}},
            {"type": "Feature", "properties": {"id": 2}, "geometry": "MULTIPOLYGON ()"},
            {"type": "Feature", "properties": {"id": 3}, "geometry": {"type": "Point", "coordinates": [-73.8, 40.8]}},
        ]
        migrator = PostGISSynthesizedDatabaseMigrator(PostGISConnectionSettings(), insert_batch_size=100)
        fake_conn = FakeConnection()
        inserted_rows: list[tuple[object, ...]] = []

        def fake_execute_values(cur, insert_sql, rows, template=None, page_size=None):
            del cur, insert_sql, template, page_size
            if any(any(value == "MULTIPOLYGON ()" for value in row) for row in rows):
                raise InvalidGeometryError(
                    'parse error - invalid geometry\nHINT:  "MULTIPOLYGON ()" <-- parse error at position 16 within geometry'
                )
            inserted_rows.extend(rows)

        with mock.patch("psycopg2.sql.Composed.as_string", return_value="INSERT INTO demo VALUES %s"):
            with mock.patch("src.synthesis.database.migration.core.execute_values", side_effect=fake_execute_values):
                migrator._insert_features(
                    fake_conn,
                    "demo_schema",
                    "hydrants",
                    table,
                    specs,
                    features,
                )

        self.assertEqual([row[0] for row in inserted_rows], [1, 3])
        self.assertEqual(fake_conn.rollback_count, 2)

    def test_insert_features_skips_retryable_data_error_rows(self):
        class FakeCursor:
            def __enter__(self):
                return self

            def __exit__(self, exc_type, exc, tb):
                return False

        class FakeConnection:
            def __init__(self):
                self.rollback_count = 0

            def cursor(self):
                return FakeCursor()

            def rollback(self):
                self.rollback_count += 1

        class RetryableInsertError(Exception):
            def __init__(self, message: str, pgcode: str = "22000"):
                super().__init__(message)
                self.pgerror = message
                self.pgcode = pgcode

        table = CanonicalSpatialTable.from_dict(
            {
                "table_id": "t1",
                "city": "nyc",
                "table_name": "labels",
                "normalized_schema": [
                    {"name": "name", "canonical_name": "name", "canonical_type": "text"},
                ],
                "spatial_fields": [],
            }
        )
        specs = prepare_column_specs(table)
        features = [
            {"type": "Feature", "properties": {"name": "alpha"}, "geometry": None},
            {"type": "Feature", "properties": {"name": "bad"}, "geometry": None},
            {"type": "Feature", "properties": {"name": "gamma"}, "geometry": None},
        ]
        migrator = PostGISSynthesizedDatabaseMigrator(PostGISConnectionSettings(), insert_batch_size=100)
        fake_conn = FakeConnection()
        inserted_rows: list[tuple[object, ...]] = []

        def fake_execute_values(cur, insert_sql, rows, template=None, page_size=None):
            del cur, insert_sql, template, page_size
            if any(any(value == "bad" for value in row) for row in rows):
                raise RetryableInsertError("invalid input syntax for type text")
            inserted_rows.extend(rows)

        with mock.patch("psycopg2.sql.Composed.as_string", return_value="INSERT INTO demo(name) VALUES %s"):
            with mock.patch("src.synthesis.database.migration.core.execute_values", side_effect=fake_execute_values):
                migrator._insert_features(
                    fake_conn,
                    "demo_schema",
                    "labels",
                    table,
                    specs,
                    features,
                )

        self.assertEqual([row[0] for row in inserted_rows], ["alpha", "gamma"])
        self.assertEqual(fake_conn.rollback_count, 2)

    def test_insert_features_supports_zero_column_tables(self):
        class FakeCursor:
            def __init__(self, executed: list[str]):
                self.executed = executed

            def __enter__(self):
                return self

            def __exit__(self, exc_type, exc, tb):
                return False

            def execute(self, query, params=None):
                del params
                self.executed.append(query)

        class FakeConnection:
            def __init__(self):
                self.executed: list[str] = []

            def cursor(self):
                return FakeCursor(self.executed)

            def rollback(self):
                raise AssertionError("rollback should not be called")

        table = CanonicalSpatialTable.from_dict(
            {
                "table_id": "t1",
                "city": "lacity",
                "table_name": "zoning",
                "normalized_schema": [],
                "spatial_fields": [],
            }
        )
        specs = prepare_column_specs(table)
        features = [
            {"type": "Feature", "properties": {}, "geometry": None},
            {"type": "Feature", "properties": {}, "geometry": None},
        ]
        migrator = PostGISSynthesizedDatabaseMigrator(PostGISConnectionSettings(), insert_batch_size=100)
        fake_conn = FakeConnection()

        with mock.patch(
            "psycopg2.sql.Composed.as_string",
            return_value='INSERT INTO "lacity_0001"."zoning" DEFAULT VALUES',
        ):
            with mock.patch("src.synthesis.database.migration.core.execute_values") as execute_values:
                migrator._insert_features(
                    fake_conn,
                    "lacity_0001",
                    "zoning",
                    table,
                    specs,
                    features,
                )

        execute_values.assert_not_called()
        self.assertEqual(
            fake_conn.executed,
            [
                'INSERT INTO "lacity_0001"."zoning" DEFAULT VALUES',
                'INSERT INTO "lacity_0001"."zoning" DEFAULT VALUES',
            ],
        )


if __name__ == "__main__":
    unittest.main()
