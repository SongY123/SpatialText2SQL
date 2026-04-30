import importlib.util
import io
import json
import os
import sys
import tempfile
import types
import unittest
from contextlib import redirect_stdout
from pathlib import Path
from unittest import mock


ROOT = Path(__file__).resolve().parents[1]
HAS_PYARROW = importlib.util.find_spec("pyarrow") is not None


def _ensure_package(name: str, path: Path) -> None:
    if name in sys.modules:
        return
    module = types.ModuleType(name)
    module.__path__ = [str(path)]
    sys.modules[name] = module


def _load_module(module_name: str, file_path: Path):
    spec = importlib.util.spec_from_file_location(module_name, file_path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


_ensure_package("src", ROOT / "src")
_ensure_package("src.sql", ROOT / "src" / "sql")

floodsql_migration = _load_module(
    "src.sql.floodsql_migration",
    ROOT / "src" / "sql" / "floodsql_migration.py",
)


def _build_metadata(file_suffix: str = ".parquet") -> dict:
    metadata = {}
    for table_name in floodsql_migration.EXPECTED_TABLES:
        metadata[table_name] = {"file": f"{table_name}{file_suffix}"}
    metadata["_global"] = {}
    return metadata


class FloodSQLMigrationUnitTests(unittest.TestCase):
    def test_discovers_flat_data_root(self):
        metadata = _build_metadata()
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            for table_name in floodsql_migration.EXPECTED_TABLES:
                (root / metadata[table_name]["file"]).write_bytes(b"")
            layout = floodsql_migration.discover_floodsql_data_layout(root, metadata)
            self.assertEqual(layout.parquet_root, root.resolve())
            self.assertEqual(layout.layout_name, "flat")

    def test_discovers_nested_data_root(self):
        metadata = _build_metadata()
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            nested = root / "data"
            nested.mkdir()
            for table_name in floodsql_migration.EXPECTED_TABLES:
                (nested / metadata[table_name]["file"]).write_bytes(b"")
            layout = floodsql_migration.discover_floodsql_data_layout(root, metadata)
            self.assertEqual(layout.parquet_root, nested.resolve())
            self.assertEqual(layout.layout_name, "nested")

    def test_maps_scalar_types_to_pg(self):
        self.assertEqual(floodsql_migration._map_scalar_type_name_to_pg("string"), "TEXT")
        self.assertEqual(floodsql_migration._map_scalar_type_name_to_pg("int64"), "BIGINT")
        self.assertEqual(
            floodsql_migration._map_scalar_type_name_to_pg("decimal", precision=10, scale=2),
            "DECIMAL(10,2)",
        )
        self.assertEqual(floodsql_migration._map_scalar_type_name_to_pg("binary"), "BYTEA")
        self.assertEqual(floodsql_migration._map_scalar_type_name_to_pg("list"), "JSONB")

    def test_classifies_table_strategies(self):
        strategy = floodsql_migration.determine_table_strategy(
            "county",
            ["GEOID", "geometry"],
        )
        self.assertEqual(strategy.name, "binary_geometry")

        strategy = floodsql_migration.determine_table_strategy(
            "hospitals",
            ["HOSPITAL_ID", "LON", "LAT", "geometry"],
        )
        self.assertEqual(strategy.name, "point")
        self.assertTrue(strategy.use_coordinate_fallback)
        self.assertEqual(strategy.source_geometry_column, "geometry")

        strategy = floodsql_migration.determine_table_strategy(
            "svi",
            ["GEOID", "RPL_THEME1"],
        )
        self.assertEqual(strategy.name, "attribute")

    def test_point_strategy_ignores_metadata_marked_placeholder_geometry(self):
        strategy = floodsql_migration.determine_table_strategy(
            "hospitals",
            ["HOSPITAL_ID", "LON", "LAT", "geometry"],
            {
                "schema": [
                    {
                        "column_name": "geometry",
                        "description": "Geometry not stored directly; construct via ST_Point(LON, LAT) (EPSG:4326).",
                    }
                ],
                "sample_rows": [{"geometry": "BLOB(0 bytes)"}],
            },
        )
        self.assertEqual(strategy.name, "point")
        self.assertIsNone(strategy.source_geometry_column)
        self.assertTrue(strategy.use_coordinate_fallback)

    def test_point_row_params_prefer_geometry_then_coordinates(self):
        plan = floodsql_migration.TablePlan(
            table_name="hospitals",
            parquet_path=Path("/tmp/hospitals.parquet"),
            strategy=floodsql_migration.TableStrategy(
                name="point",
                source_geometry_column="geometry",
                use_coordinate_fallback=True,
            ),
            columns=(
                floodsql_migration.ColumnPlan("HOSPITAL_ID", "TEXT", source_name="HOSPITAL_ID"),
                floodsql_migration.ColumnPlan("LON", "DOUBLE PRECISION", source_name="LON"),
                floodsql_migration.ColumnPlan("LAT", "DOUBLE PRECISION", source_name="LAT"),
                floodsql_migration.ColumnPlan("geometry", "geometry(Point,4326)"),
            ),
            read_columns=("HOSPITAL_ID", "LON", "LAT", "geometry"),
            total_rows=1,
            num_row_groups=1,
        )
        params = floodsql_migration._row_to_params(
            plan,
            {"HOSPITAL_ID": "h1", "LON": 1.5, "LAT": 2.5, "geometry": [1, 2, 3]},
        )
        self.assertEqual(params[:3], ("h1", 1.5, 2.5))
        self.assertEqual(params[3], b"\x01\x02\x03")
        self.assertEqual(params[4], b"\x01\x02\x03")
        self.assertEqual(params[5:], (1.5, 2.5, 1.5, 2.5))

    def test_point_row_params_fall_back_to_coordinates(self):
        plan = floodsql_migration.TablePlan(
            table_name="schools",
            parquet_path=Path("/tmp/schools.parquet"),
            strategy=floodsql_migration.TableStrategy(
                name="point",
                source_geometry_column=None,
                use_coordinate_fallback=True,
            ),
            columns=(
                floodsql_migration.ColumnPlan("SCHOOL_ID", "TEXT", source_name="SCHOOL_ID"),
                floodsql_migration.ColumnPlan("LON", "DOUBLE PRECISION", source_name="LON"),
                floodsql_migration.ColumnPlan("LAT", "DOUBLE PRECISION", source_name="LAT"),
                floodsql_migration.ColumnPlan("geometry", "geometry(Point,4326)"),
            ),
            read_columns=("SCHOOL_ID", "LON", "LAT"),
            total_rows=1,
            num_row_groups=1,
        )
        params = floodsql_migration._row_to_params(
            plan,
            {"SCHOOL_ID": "s1", "LON": 11.0, "LAT": 12.0},
        )
        self.assertEqual(params[:3], ("s1", 11.0, 12.0))
        self.assertEqual(params[3:5], (None, None))
        self.assertEqual(params[5:], (11.0, 12.0, 11.0, 12.0))

    def test_empty_geometry_blob_is_treated_as_null(self):
        self.assertIsNone(floodsql_migration._coerce_binary_geometry_value(b""))
        self.assertIsNone(floodsql_migration._coerce_binary_geometry_value(bytearray()))
        self.assertIsNone(floodsql_migration._coerce_binary_geometry_value([]))

    def test_arrow_field_to_column_plan_normalizes_target_name_to_lowercase(self):
        field = types.SimpleNamespace(name="GEOID", type=object())
        fake_patypes = types.SimpleNamespace(
            is_fixed_size_binary=lambda value: False,
            is_large_list=lambda value: False,
            is_map=lambda value: False,
            is_string=lambda value: True,
            is_large_string=lambda value: False,
            is_boolean=lambda value: False,
            is_int8=lambda value: False,
            is_int16=lambda value: False,
            is_int32=lambda value: False,
            is_int64=lambda value: False,
            is_uint8=lambda value: False,
            is_uint16=lambda value: False,
            is_uint32=lambda value: False,
            is_uint64=lambda value: False,
            is_float32=lambda value: False,
            is_float64=lambda value: False,
            is_decimal=lambda value: False,
            is_timestamp=lambda value: False,
            is_date32=lambda value: False,
            is_date64=lambda value: False,
            is_binary=lambda value: False,
            is_large_binary=lambda value: False,
            is_list=lambda value: False,
            is_struct=lambda value: False,
        )

        with mock.patch.object(
            floodsql_migration,
            "_load_pyarrow",
            return_value=(None, fake_patypes),
        ):
            plan = floodsql_migration._arrow_field_to_column_plan(field)

        self.assertEqual(plan.name, "geoid")
        self.assertEqual(plan.source_name, "GEOID")
        self.assertEqual(plan.pg_type, "TEXT")

    def test_arrow_field_to_column_plan_forces_identifier_codes_to_text(self):
        field = types.SimpleNamespace(name="FIPS", type=object())
        fake_patypes = types.SimpleNamespace(
            is_fixed_size_binary=lambda value: False,
            is_large_list=lambda value: False,
            is_map=lambda value: False,
            is_string=lambda value: False,
            is_large_string=lambda value: False,
            is_boolean=lambda value: False,
            is_int8=lambda value: False,
            is_int16=lambda value: False,
            is_int32=lambda value: False,
            is_int64=lambda value: True,
            is_uint8=lambda value: False,
            is_uint16=lambda value: False,
            is_uint32=lambda value: False,
            is_uint64=lambda value: False,
            is_float32=lambda value: False,
            is_float64=lambda value: False,
            is_decimal=lambda value: False,
            is_timestamp=lambda value: False,
            is_date32=lambda value: False,
            is_date64=lambda value: False,
            is_binary=lambda value: False,
            is_large_binary=lambda value: False,
            is_list=lambda value: False,
            is_struct=lambda value: False,
        )

        with mock.patch.object(
            floodsql_migration,
            "_load_pyarrow",
            return_value=(None, fake_patypes),
        ):
            plan = floodsql_migration._arrow_field_to_column_plan(field)

        self.assertEqual(plan.name, "fips")
        self.assertEqual(plan.pg_type, "TEXT")

    def test_row_to_params_stringifies_identifier_values(self):
        plan = floodsql_migration.TablePlan(
            table_name="svi",
            parquet_path=Path("/tmp/svi.parquet"),
            strategy=floodsql_migration.TableStrategy(name="attribute"),
            columns=(
                floodsql_migration.ColumnPlan("fips", "TEXT", source_name="FIPS"),
                floodsql_migration.ColumnPlan("score", "DOUBLE PRECISION", source_name="SCORE"),
            ),
            read_columns=("FIPS", "SCORE"),
            total_rows=1,
            num_row_groups=1,
        )

        params = floodsql_migration._row_to_params(
            plan,
            {"FIPS": 22051020519, "SCORE": 0.42},
        )

        self.assertEqual(params, ("22051020519", 0.42))

    def test_row_to_params_converts_nonfinite_numeric_values_to_null(self):
        plan = floodsql_migration.TablePlan(
            table_name="claims",
            parquet_path=Path("/tmp/claims.parquet"),
            strategy=floodsql_migration.TableStrategy(name="attribute"),
            columns=(
                floodsql_migration.ColumnPlan(
                    "amountpaidonbuildingclaim",
                    "DOUBLE PRECISION",
                    source_name="amountPaidOnBuildingClaim",
                ),
                floodsql_migration.ColumnPlan("countyfips", "TEXT", source_name="COUNTYFIPS"),
            ),
            read_columns=("amountPaidOnBuildingClaim", "COUNTYFIPS"),
            total_rows=1,
            num_row_groups=1,
        )

        params = floodsql_migration._row_to_params(
            plan,
            {"amountPaidOnBuildingClaim": float("nan"), "COUNTYFIPS": 22007},
        )

        self.assertEqual(params, (None, "22007"))

    def test_row_to_params_converts_nonfinite_numeric_text_to_null(self):
        plan = floodsql_migration.TablePlan(
            table_name="nri",
            parquet_path=Path("/tmp/nri.parquet"),
            strategy=floodsql_migration.TableStrategy(name="attribute"),
            columns=(
                floodsql_migration.ColumnPlan("cfld_risks", "DOUBLE PRECISION", source_name="CFLD_RISKS"),
            ),
            read_columns=("CFLD_RISKS",),
            total_rows=1,
            num_row_groups=1,
        )

        params = floodsql_migration._row_to_params(
            plan,
            {"CFLD_RISKS": "NaN"},
        )

        self.assertEqual(params, (None,))

    def test_checkpoint_progress_and_completion(self):
        plan = floodsql_migration.TablePlan(
            table_name="county",
            parquet_path=Path("/tmp/county.parquet"),
            strategy=floodsql_migration.TableStrategy(
                name="binary_geometry",
                source_geometry_column="geometry",
            ),
            columns=(
                floodsql_migration.ColumnPlan("GEOID", "TEXT", source_name="GEOID"),
                floodsql_migration.ColumnPlan("geometry", "geometry(Geometry,4326)"),
            ),
            read_columns=("GEOID", "geometry"),
            total_rows=2,
            num_row_groups=3,
        )
        with tempfile.TemporaryDirectory() as tmpdir:
            checkpoint_path = Path(tmpdir) / "checkpoint.json"
            checkpoint = floodsql_migration._load_checkpoint(checkpoint_path)
            floodsql_migration._mark_row_group_completed(checkpoint, checkpoint_path, plan, 1)
            stored = json.loads(checkpoint_path.read_text())
            self.assertEqual(
                stored["table_progress"]["county"]["last_completed_row_group"],
                1,
            )

            floodsql_migration._mark_table_completed(
                checkpoint,
                checkpoint_path,
                plan,
                {"source_row_count": 2, "target_row_count": 2},
            )
            stored = json.loads(checkpoint_path.read_text())
            self.assertEqual(stored["completed_tables"]["county"]["status"], "ok")
            self.assertNotIn("county", stored["table_progress"])

    def test_import_single_table_emits_progress_logs(self):
        plan = floodsql_migration.TablePlan(
            table_name="county",
            parquet_path=Path("/tmp/county.parquet"),
            strategy=floodsql_migration.TableStrategy(
                name="binary_geometry",
                source_geometry_column="geometry",
            ),
            columns=(
                floodsql_migration.ColumnPlan("GEOID", "TEXT", source_name="GEOID"),
                floodsql_migration.ColumnPlan("geometry", "geometry(Geometry,4326)"),
            ),
            read_columns=("GEOID", "geometry"),
            total_rows=2,
            num_row_groups=1,
        )

        class FakeCursor:
            def close(self):
                return None

        class FakeConn:
            def __init__(self):
                self.commits = 0
                self.rollbacks = 0

            def cursor(self):
                return FakeCursor()

            def commit(self):
                self.commits += 1

            def rollback(self):
                self.rollbacks += 1

        fake_conn = FakeConn()
        extras = types.SimpleNamespace(execute_values=mock.Mock())

        with tempfile.TemporaryDirectory() as tmpdir:
            checkpoint_path = Path(tmpdir) / "checkpoint.json"
            checkpoint = {"completed_tables": {}, "table_progress": {}}
            output = io.StringIO()
            with (
                mock.patch.object(floodsql_migration, "_recreate_table"),
                mock.patch.object(floodsql_migration, "_create_indexes"),
                mock.patch.object(
                    floodsql_migration,
                    "_collect_validation",
                    return_value={
                        "source_row_count": 2,
                        "target_row_count": 2,
                        "geometry_non_null_count": 2,
                        "invalid_geometry_count": 0,
                    },
                ),
                mock.patch.object(
                    floodsql_migration,
                    "_iter_table_batches",
                    return_value=iter([[("001", b"\x01\x02")], [("002", b"\x03\x04")]]),
                ),
                redirect_stdout(output),
            ):
                result = floodsql_migration._import_single_table(
                    fake_conn,
                    extras,
                    checkpoint,
                    checkpoint_path,
                    plan,
                    batch_size=2,
                    resume=False,
                )

        self.assertEqual(result["status"], "imported")
        self.assertEqual(extras.execute_values.call_count, 2)
        logs = output.getvalue()
        self.assertIn("[FloodSQL][county] start", logs)
        self.assertIn("[FloodSQL][county] row-group", logs)
        self.assertIn("[FloodSQL][county] done", logs)

    def test_import_single_table_wraps_batch_errors_with_context(self):
        plan = floodsql_migration.TablePlan(
            table_name="county",
            parquet_path=Path("/tmp/county.parquet"),
            strategy=floodsql_migration.TableStrategy(
                name="binary_geometry",
                source_geometry_column="geometry",
            ),
            columns=(
                floodsql_migration.ColumnPlan("GEOID", "TEXT", source_name="GEOID"),
                floodsql_migration.ColumnPlan("geometry", "geometry(Geometry,4326)"),
            ),
            read_columns=("GEOID", "geometry"),
            total_rows=1,
            num_row_groups=1,
        )

        class FakeCursor:
            def close(self):
                return None

        class FakeConn:
            def cursor(self):
                return FakeCursor()

            def commit(self):
                return None

            def rollback(self):
                return None

        extras = types.SimpleNamespace(execute_values=mock.Mock(side_effect=ValueError("boom")))

        with tempfile.TemporaryDirectory() as tmpdir:
            checkpoint_path = Path(tmpdir) / "checkpoint.json"
            checkpoint = {"completed_tables": {}, "table_progress": {}}
            with (
                mock.patch.object(floodsql_migration, "_recreate_table"),
                mock.patch.object(
                    floodsql_migration,
                    "_iter_table_batches",
                    return_value=iter([[("001", b"\x01\x02")]]),
                ),
            ):
                with self.assertRaises(RuntimeError) as ctx:
                    floodsql_migration._import_single_table(
                        FakeConn(),
                        extras,
                        checkpoint,
                        checkpoint_path,
                        plan,
                        batch_size=1,
                        resume=False,
                    )

        message = str(ctx.exception)
        self.assertIn("batch import failed", message)
        self.assertIn("row_group=1/1", message)
        self.assertIn("geometry_non_null_rows=1/1", message)


@unittest.skipUnless(HAS_PYARROW, "pyarrow is required for parquet fixture tests")
class FloodSQLMigrationParquetFixtureTests(unittest.TestCase):
    def setUp(self):
        import pyarrow as pa  # type: ignore
        import pyarrow.parquet as pq  # type: ignore

        self.pa = pa
        self.pq = pq

    def test_binary_geometry_plan_uses_parquet_schema(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            table = self.pa.table(
                {
                    "GEOID": self.pa.array(["001"]),
                    "geometry": self.pa.array([b"\x01\x02"], type=self.pa.binary()),
                }
            )
            path = root / "county.parquet"
            self.pq.write_table(table, path, row_group_size=1)
            plan = floodsql_migration._build_table_plan("county", {"file": "county.parquet"}, root)

            self.assertEqual(plan.strategy.name, "binary_geometry")
            self.assertEqual(plan.total_rows, 1)
            self.assertEqual(plan.num_row_groups, 1)
            self.assertEqual([column.name for column in plan.columns], ["geoid", "geometry"])
            first_batch = next(iter(floodsql_migration._iter_table_batches(plan, 0, batch_size=10)))
            self.assertEqual(first_batch[0][-1], b"\x01\x02")

    def test_coordinate_derived_point_plan(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            table = self.pa.table(
                {
                    "SCHOOL_ID": self.pa.array(["s1"]),
                    "LAT": self.pa.array([30.1]),
                    "LON": self.pa.array([-97.7]),
                }
            )
            path = root / "schools.parquet"
            self.pq.write_table(table, path, row_group_size=1)
            plan = floodsql_migration._build_table_plan("schools", {"file": "schools.parquet"}, root)

            self.assertEqual(plan.strategy.name, "point")
            self.assertIsNone(plan.strategy.source_geometry_column)
            batch = next(iter(floodsql_migration._iter_table_batches(plan, 0, batch_size=10)))
            self.assertEqual(batch[0][0:3], ("s1", 30.1, -97.7))
            self.assertEqual(batch[0][3:5], (None, None))

    def test_point_plan_skips_placeholder_geometry_column_when_metadata_marks_dynamic(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            table = self.pa.table(
                {
                    "HOSPITAL_ID": self.pa.array(["h1"]),
                    "LAT": self.pa.array([30.1]),
                    "LON": self.pa.array([-97.7]),
                    "geometry": self.pa.array([b""], type=self.pa.binary()),
                }
            )
            path = root / "hospitals.parquet"
            self.pq.write_table(table, path, row_group_size=1)
            plan = floodsql_migration._build_table_plan(
                "hospitals",
                {
                    "file": "hospitals.parquet",
                    "schema": [
                        {
                            "column_name": "geometry",
                            "description": "Geometry not stored directly; construct via ST_Point(LON, LAT) (EPSG:4326).",
                        }
                    ],
                    "sample_rows": [{"geometry": "BLOB(0 bytes)"}],
                },
                root,
            )

            self.assertEqual(plan.strategy.name, "point")
            self.assertIsNone(plan.strategy.source_geometry_column)
            self.assertEqual(plan.read_columns, ("HOSPITAL_ID", "LAT", "LON"))
            self.assertEqual([column.name for column in plan.columns], ["hospital_id", "lat", "lon", "geometry"])

    def test_attribute_only_plan(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            table = self.pa.table(
                {
                    "GEOID": self.pa.array(["001"]),
                    "RPL_THEME1": self.pa.array([0.12]),
                }
            )
            path = root / "svi.parquet"
            self.pq.write_table(table, path, row_group_size=1)
            plan = floodsql_migration._build_table_plan("svi", {"file": "svi.parquet"}, root)

            self.assertEqual(plan.strategy.name, "attribute")
            self.assertFalse(plan.has_geometry)
            self.assertEqual([column.name for column in plan.columns], ["geoid", "rpl_theme1"])


@unittest.skipUnless(
    HAS_PYARROW and os.environ.get("RUN_FLOODSQL_MIGRATION_SMOKE_TEST") == "1",
    "Set RUN_FLOODSQL_MIGRATION_SMOKE_TEST=1 and install pyarrow to enable",
)
class FloodSQLMigrationPostGisSmokeTests(unittest.TestCase):
    POINT_WKB = bytes.fromhex("010100000000000000000000000000000000000000")

    def setUp(self):
        import pyarrow as pa  # type: ignore
        import pyarrow.parquet as pq  # type: ignore

        self.pa = pa
        self.pq = pq

    def _write_table(self, root: Path, file_name: str, payload: dict) -> None:
        table = self.pa.table({key: self.pa.array(values) for key, values in payload.items()})
        self.pq.write_table(table, root / file_name, row_group_size=1)

    def test_optional_real_postgis_import(self):
        import psycopg2  # type: ignore

        host = os.environ.get("FLOODSQL_SMOKE_HOST", "10.132.80.204")
        port = int(os.environ.get("FLOODSQL_SMOKE_PORT", "5432"))
        database = os.environ.get("FLOODSQL_SMOKE_DB", "floodsql_migration_smoke")
        user = os.environ.get("FLOODSQL_SMOKE_USER", "postgres")
        password = os.environ.get("FLOODSQL_SMOKE_PASSWORD", "123456")

        metadata = _build_metadata()
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            metadata_path = root / "metadata_parquet.json"
            metadata_path.write_text(json.dumps(metadata), encoding="utf-8")

            geom_tables = {"county", "floodplain", "census_tracts", "zcta", "claims"}
            for table_name in floodsql_migration.EXPECTED_TABLES:
                file_name = metadata[table_name]["file"]
                if table_name == "county":
                    self._write_table(
                        root,
                        file_name,
                        {
                            "GEOID": ["001"],
                            "STATEFP": ["48"],
                            "COUNTYFP": ["001"],
                            "geometry": [self.POINT_WKB],
                        },
                    )
                elif table_name == "floodplain":
                    self._write_table(
                        root,
                        file_name,
                        {
                            "GFID": ["f1"],
                            "STATEFP": ["48"],
                            "FLD_ZONE": ["AE"],
                            "geometry": [self.POINT_WKB],
                        },
                    )
                elif table_name == "census_tracts":
                    self._write_table(
                        root,
                        file_name,
                        {
                            "GEOID": ["001"],
                            "STATEFP": ["48"],
                            "COUNTYFP": ["001"],
                            "geometry": [self.POINT_WKB],
                        },
                    )
                elif table_name in {"zcta", "claims"}:
                    self._write_table(
                        root,
                        file_name,
                        {
                            "GEOID": ["001"],
                            "STATEFP": ["48"],
                            "geometry": [self.POINT_WKB],
                        },
                    )
                elif table_name == "hospitals":
                    self._write_table(
                        root,
                        file_name,
                        {
                            "HOSPITAL_ID": ["h1"],
                            "COUNTYFIPS": ["48001"],
                            "ZIP": ["78701"],
                            "STATEFP": ["48"],
                            "UNIQUE_ID": ["48hospitalh1"],
                            "LAT": [30.0],
                            "LON": [-97.0],
                            "geometry": [b""],
                        },
                    )
                elif table_name == "schools":
                    self._write_table(
                        root,
                        file_name,
                        {
                            "SCHOOL_ID": ["s1"],
                            "ZIP": ["78701"],
                            "STATEFP": ["48"],
                            "UNIQUE_ID": ["48schools1"],
                            "LAT": [30.0],
                            "LON": [-97.0],
                        },
                    )
                elif table_name == "cre":
                    self._write_table(
                        root,
                        file_name,
                        {"GEOID": ["001"], "STATE": ["48"], "COUNTY": ["001"], "TRACT": ["000100"]},
                    )
                elif table_name == "nri":
                    self._write_table(root, file_name, {"GEOID": ["001"], "STATE": ["48"]})
                elif table_name == "svi":
                    self._write_table(root, file_name, {"GEOID": ["001"], "STATE": ["48"]})
                else:
                    self._write_table(root, file_name, {"GEOID": ["001"], "STATE": ["48"]})

            floodsql_migration.run_floodsql_migration(
                data_root=root,
                metadata_path=metadata_path,
                report_path=root / "migration_report.json",
                checkpoint_path=root / "migration_checkpoint.json",
                host=host,
                port=port,
                database=database,
                maintenance_db="postgres",
                user=user,
                password=password,
                batch_size=1,
            )

            conn = psycopg2.connect(
                host=host,
                port=port,
                database=database,
                user=user,
                password=password,
            )
            cur = conn.cursor()
            try:
                cur.execute("SELECT COUNT(*) FROM county")
                self.assertEqual(cur.fetchone()[0], 1)
                cur.execute("SELECT COUNT(*) FROM county WHERE geometry IS NOT NULL")
                self.assertEqual(cur.fetchone()[0], 1)
                cur.execute(
                    "SELECT COUNT(*) FROM pg_indexes "
                    "WHERE tablename = 'county' AND indexdef ILIKE '%USING gist (geometry)%'"
                )
                self.assertEqual(cur.fetchone()[0], 1)
            finally:
                cur.close()
                conn.close()


if __name__ == "__main__":
    unittest.main()
