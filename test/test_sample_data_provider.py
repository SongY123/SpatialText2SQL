import importlib.util
import sys
import types
import unittest
import warnings
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
_ensure_package(
    "src.prompting.prompt_enhancements",
    ROOT / "src" / "prompting" / "prompt_enhancements",
)
_ensure_package(
    "src.prompting.prompt_enhancements.spatialsql_pg",
    ROOT / "src" / "prompting" / "prompt_enhancements" / "spatialsql_pg",
)

sample_provider_module = _load_module(
    "src.prompting.sample_data_provider",
    ROOT / "src" / "prompting" / "sample_data_provider.py",
)
prompt_builder_module = _load_module(
    "src.prompting.prompt_builder",
    ROOT / "src" / "prompting" / "prompt_builder.py",
)


class FakeCursor:
    def __init__(self, connection):
        self.connection = connection
        self.rows = []

    def execute(self, query, params=None):
        params = params or ()
        compact_query = " ".join(query.split())
        self.connection.calls.append((compact_query, tuple(params)))

        if "FROM information_schema.columns" in compact_query:
            requested_table = params[0]
            self.rows = self.connection.resolve_schema_rows(requested_table)
            return

        for table_name, should_raise in self.connection.raise_on_sample.items():
            if f'FROM "{table_name}"' in compact_query and should_raise:
                raise RuntimeError(f"boom for {table_name}")

        for table_name, rows in self.connection.sample_rows.items():
            if f'FROM "{table_name}"' in compact_query:
                self.rows = rows
                return

        self.rows = []

    def fetchall(self):
        return list(self.rows)

    def close(self):
        return None


class FakeConnection:
    def __init__(self, schema_rows=None, sample_rows=None, raise_on_sample=None):
        self.schema_rows = schema_rows or {}
        self.sample_rows = sample_rows or {}
        self.raise_on_sample = raise_on_sample or {}
        self.calls = []
        self.rollback_count = 0
        self.closed = False

    def cursor(self):
        return FakeCursor(self)

    def rollback(self):
        self.rollback_count += 1

    def resolve_schema_rows(self, requested_table: str):
        if requested_table in self.schema_rows:
            return self.schema_rows[requested_table]
        requested_lower = requested_table.lower()
        for table_name, rows in self.schema_rows.items():
            if table_name.lower() == requested_lower:
                return rows
        return []


class FakeSampleDataProvider(sample_provider_module.PostgresSampleDataProvider):
    def __init__(self, connection):
        super().__init__(
            project_root=ROOT,
            db_config={
                "databases": {
                    "postgres": {
                        "host": "localhost",
                        "port": 5432,
                        "database": "postgres",
                        "user": "postgres",
                        "password": "postgres",
                    }
                }
            },
        )
        self.connection = connection

    def _get_connection(self, db_key: str):
        del db_key
        return self.connection


class EmptySampleDataProvider:
    def build_sample_data(self, dataset_name: str, metadata: dict | None, compact_schema: str) -> str:
        del dataset_name, metadata, compact_schema
        return ""


class PostgresSampleDataProviderTests(unittest.TestCase):
    def test_builds_sample_rows_and_sanitizes_geometry_binary_null_and_long_text(self):
        long_text = "x" * 120
        connection = FakeConnection(
            schema_rows={
                "demo_table": [
                    ("demo_table", "name", "text", "text", 1),
                    ("demo_table", "geom", "USER-DEFINED", "geometry", 2),
                    ("demo_table", "payload", "bytea", "bytea", 3),
                    ("demo_table", "note", "text", "text", 4),
                ]
            },
            sample_rows={
                "demo_table": [
                    ("Alpha", long_text),
                    ("Beta", None),
                ]
            },
        )
        provider = FakeSampleDataProvider(connection)

        rendered = provider.build_sample_data(
            dataset_name="spatial_qa",
            metadata={},
            compact_schema="- demo_table(name text, geom geometry, payload bytea, note text)",
        )

        self.assertIn("- demo_table", rendered)
        self.assertIn('"geom": "<geometry>"', rendered)
        self.assertIn('"payload": "<binary>"', rendered)
        self.assertIn('"note": null', rendered)
        self.assertIn("...", rendered)
        self.assertNotIn(long_text, rendered)
        self.assertIn(
            (
                'SELECT "name", "note" FROM "demo_table" ORDER BY "name" NULLS LAST LIMIT %s',
                (5,),
            ),
            connection.calls,
        )

    def test_queries_only_tables_visible_in_compact_schema(self):
        connection = FakeConnection(
            schema_rows={
                "visible_table": [
                    ("visible_table", "id", "integer", "int4", 1),
                ],
                "hidden_table": [
                    ("hidden_table", "id", "integer", "int4", 1),
                ],
            },
            sample_rows={
                "visible_table": [(1,)],
                "hidden_table": [(2,)],
            },
        )
        provider = FakeSampleDataProvider(connection)

        rendered = provider.build_sample_data(
            dataset_name="spatial_qa",
            metadata={},
            compact_schema="- visible_table(id integer)",
        )

        self.assertIn("- visible_table", rendered)
        self.assertNotIn("hidden_table", rendered)
        joined_calls = "\n".join(query for query, _params in connection.calls)
        self.assertIn("visible_table", joined_calls)
        self.assertNotIn("hidden_table", joined_calls)

    def test_resolves_table_names_case_insensitively(self):
        connection = FakeConnection(
            schema_rows={
                "scenicspots": [
                    ("scenicspots", "name", "text", "text", 1),
                ]
            },
            sample_rows={"scenicspots": [("West Lake",)]},
        )
        provider = FakeSampleDataProvider(connection)

        rendered = provider.build_sample_data(
            dataset_name="spatial_qa",
            metadata={},
            compact_schema="- ScenicSpots(name text)",
        )

        self.assertIn("West Lake", rendered)
        self.assertTrue(
            any('FROM "scenicspots"' in query for query, _params in connection.calls)
        )

    def test_skips_failed_tables_without_breaking_prompt_building(self):
        connection = FakeConnection(
            schema_rows={
                "broken_table": [
                    ("broken_table", "name", "text", "text", 1),
                ]
            },
            sample_rows={"broken_table": [("ignored",)]},
            raise_on_sample={"broken_table": True},
        )
        provider = FakeSampleDataProvider(connection)

        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            rendered = provider.build_sample_data(
                dataset_name="spatial_qa",
                metadata={},
                compact_schema="- broken_table(name text)",
            )

        self.assertEqual(rendered, "")
        self.assertGreaterEqual(len(caught), 1)
        self.assertGreaterEqual(connection.rollback_count, 1)

    def test_prompt_builder_removes_empty_sample_data_section(self):
        builder = prompt_builder_module.PromptBuilder(
            {"sample_data_provider": EmptySampleDataProvider()}
        )

        prompt = builder.build_prompt(
            question="Find all POIs.",
            schema="table pois(id integer, name text, geom geometry)",
            config_type="base",
            dataset_name="spatial_qa",
            metadata={},
        )

        self.assertNotIn("## Sample Data", prompt)


if __name__ == "__main__":
    unittest.main()
