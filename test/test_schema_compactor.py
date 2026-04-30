import importlib.util
import sys
import tempfile
import types
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


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
_ensure_package("src.prompting", ROOT / "src" / "prompting")

schema_compactor_module = _load_module(
    "src.prompting.schema_compactor",
    ROOT / "src" / "prompting" / "schema_compactor.py",
)


class SchemaCompactorTests(unittest.TestCase):
    def test_compacts_spatialsql_schema_using_catalog_and_preserves_case_sensitive_tables(self):
        compactor = schema_compactor_module.SchemaCompactor(project_root=ROOT)
        schema = (ROOT / "data" / "schemas" / "spatial_sql_schema.txt").read_text(
            encoding="utf-8"
        )

        compacted = compactor.compact_schema(
            schema=schema,
            dataset_name="spatialsql_pg",
            metadata={"split": "dataset1_tourism"},
        )

        self.assertIn("- dataset1_tourism_GDPs(", compacted)
        self.assertIn("- dataset1_tourism_scenicSpots(", compacted)
        self.assertNotIn("dataset1_tourism_gdps", compacted)
        self.assertNotIn("dataset1_tourism_geometry_columns_field_infos", compacted)
        self.assertNotIn("dataset1_tourism_sql_statements_log", compacted)

    def test_falls_back_to_prefix_filter_when_catalog_files_are_missing(self):
        schema = """
CREATE TABLE dataset9_demo_cities (
    name character varying,
    shape bytea NOT NULL
);

CREATE TABLE dataset9_demo_geometry_columns_field_infos (
    ogc_fid integer NOT NULL,
    f_table_name character varying NOT NULL
);

CREATE TABLE dataset10_demo_cities (
    name character varying,
    shape bytea NOT NULL
);
""".strip()
        with tempfile.TemporaryDirectory() as tmpdir:
            compactor = schema_compactor_module.SchemaCompactor(project_root=tmpdir)
            compacted = compactor.compact_schema(
                schema=schema,
                dataset_name="spatialsql_pg",
                metadata={"split": "dataset9_demo"},
            )

        self.assertIn("- dataset9_demo_cities(name text, shape geometry)", compacted)
        self.assertNotIn("dataset9_demo_geometry_columns_field_infos", compacted)
        self.assertNotIn("dataset10_demo_cities", compacted)

    def test_extracts_geometry_columns_from_compact_schema(self):
        compactor = schema_compactor_module.SchemaCompactor(project_root=ROOT)
        compact_schema = "\n".join(
            [
                "- dataset1_ada_cities(name text, shape geometry)",
                "- dataset1_ada_airports(name text, location geometry)",
            ]
        )

        geometry_columns = compactor.extract_geometry_columns(compact_schema)

        self.assertEqual(
            geometry_columns,
            [
                "dataset1_ada_cities.shape",
                "dataset1_ada_airports.location",
            ],
        )

    def test_spatialsql_compaction_keeps_schema_pure_without_semantic_hints(self):
        compactor = schema_compactor_module.SchemaCompactor(project_root=ROOT)
        schema = (ROOT / "data" / "schemas" / "spatial_sql_schema.txt").read_text(
            encoding="utf-8"
        )

        compacted = compactor.compact_schema(
            schema=schema,
            dataset_name="spatialsql_pg",
            metadata={"split": "dataset1_ada"},
        )

        self.assertNotIn("semantic hints:", compacted)
        self.assertNotIn("value hints:", compacted)
        self.assertNotIn("records the area of the lake", compacted)
        self.assertNotIn("'洞庭湖'", compacted)
        self.assertIn("- dataset1_ada_lakes(", compacted)
        self.assertIn("- dataset1_ada_provinces(", compacted)

    def test_generic_compaction_excludes_only_irrelevant_tables_for_spatial_qa(self):
        compactor = schema_compactor_module.SchemaCompactor(project_root=ROOT)
        schema = (ROOT / "data" / "schemas" / "postgres_schema.txt").read_text(
            encoding="utf-8"
        )

        compacted = compactor.compact_schema(
            schema=schema,
            question="Which roads are in the block group with geoid '421010336004'?",
            dataset_name="spatial_qa",
            metadata={"level": 2},
        )

        self.assertIn("- roads(", compacted)
        self.assertIn("- blockgroups(", compacted)
        self.assertIn("- poi(", compacted)
        self.assertIn("- ne_time_zones(", compacted)
        self.assertNotIn("- spatial_ref_sys(", compacted)

    def test_generic_compaction_keeps_full_table_columns_for_spatial_qa(self):
        compactor = schema_compactor_module.SchemaCompactor(project_root=ROOT)
        schema = (ROOT / "data" / "schemas" / "postgres_schema.txt").read_text(
            encoding="utf-8"
        )

        compacted = compactor.compact_schema(
            schema=schema,
            question="Show the WGS 84 geometry for the state of 'Nevada'.",
            dataset_name="spatial_qa",
            metadata={"level": 1},
        )

        self.assertIn("- states(", compacted)
        self.assertIn("name text", compacted)
        self.assertIn("geom geometry", compacted)
        self.assertIn("awater double", compacted)

    def test_generic_compaction_keeps_all_business_tables_for_floodsql(self):
        compactor = schema_compactor_module.SchemaCompactor(project_root=ROOT)
        schema = """
CREATE TABLE claims (
    geoid character varying,
    statefp character varying,
    amountPaidOnBuildingClaim double precision,
    geometry USER-DEFINED
);

CREATE TABLE county (
    geoid character varying,
    name character varying,
    statefp character varying,
    geometry USER-DEFINED
);

CREATE TABLE hospitals (
    countyfips character varying,
    name character varying,
    geometry USER-DEFINED
);

CREATE TABLE zcta (
    geoid character varying,
    statefp character varying,
    geometry USER-DEFINED
);

CREATE TABLE floodplain (
    gfid character varying,
    fld_zone character varying,
    geometry USER-DEFINED
);
""".strip()

        compacted = compactor.compact_schema(
            schema=schema,
            question="How many claims are there in Harris County?",
            dataset_name="floodsql_pg",
            metadata={"level": "L0"},
        )

        self.assertIn("- claims(", compacted)
        self.assertIn("- county(", compacted)
        self.assertIn("- hospitals(", compacted)
        self.assertIn("- zcta(", compacted)


if __name__ == "__main__":
    unittest.main()
