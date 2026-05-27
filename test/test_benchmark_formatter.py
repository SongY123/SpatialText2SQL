import importlib.util
import json
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
_ensure_package("src.datasets", ROOT / "src" / "datasets")

benchmark_formatter = _load_module(
    "src.datasets.benchmark_formatter",
    ROOT / "src" / "datasets" / "benchmark_formatter.py",
)


class _FakeConnection:
    def close(self):
        return None


class BenchmarkFormatterTests(unittest.TestCase):
    def test_json_array_append_writer_keeps_valid_json(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            output_path = Path(tmpdir) / "rows.json"
            writer = benchmark_formatter._JsonArrayAppendWriter(output_path)
            writer.append({"id": "row1", "results": [[1]]})
            writer.append({"id": "row2", "results": [[2]]})

            payload = json.loads(output_path.read_text(encoding="utf-8"))

        self.assertEqual(
            payload,
            [
                {"id": "row1", "results": [[1]]},
                {"id": "row2", "results": [[2]]},
            ],
        )

    def test_build_spatialsql_rows_materializes_results(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            qa_dir = root / "SpatialSQL" / "dataset2" / "edu"
            qa_dir.mkdir(parents=True)
            (qa_dir / "QA-edu-1.txt").write_text(
                "\n".join(
                    [
                        "id:edu41",
                        "question:Which city in Hubei Province has the largest area?",
                        "SQL:Select name from cities limit 1",
                        "Eval:Select name from cities limit 1",
                    ]
                ),
                encoding="utf-8",
            )

            dataset_config = {
                "datasets": {
                    "spatialsql": {
                        "raw_data_path": str(root / "SpatialSQL"),
                        "source_partitions": {
                            "domain_edu": {
                                "domain": "edu",
                                "level": "edu",
                                "raw_path": "dataset2/edu",
                                "dataset_version": "dataset2",
                            }
                        },
                    }
                }
            }

            original_resolve = benchmark_formatter.resolve_db_settings
            original_connect = benchmark_formatter._connect_postgres
            original_execute = benchmark_formatter._execute_query_for_results
            try:
                benchmark_formatter.resolve_db_settings = lambda *args, **kwargs: {
                    "host": "localhost",
                    "port": 5432,
                    "database": "benchmark",
                    "user": "postgres",
                    "password": "postgres",
                }
                benchmark_formatter._connect_postgres = lambda *args, **kwargs: _FakeConnection()
                benchmark_formatter._execute_query_for_results = (
                    lambda connection, sql, *, sample_label: ("ok", [["Wuhan"]])
                )

                rows = benchmark_formatter.build_spatialsql_rows(
                    dataset_config,
                    embedded_db_config={"database": {}, "databases": {}},
                    eval_config={"evaluation": {"timeout": 60}},
                )
            finally:
                benchmark_formatter.resolve_db_settings = original_resolve
                benchmark_formatter._connect_postgres = original_connect
                benchmark_formatter._execute_query_for_results = original_execute

            self.assertEqual(len(rows), 1)
            self.assertEqual(rows[0]["id"], "edu41")
            self.assertEqual(rows[0]["results"], [["Wuhan"]])
            self.assertEqual(rows[0]["level"], "edu")
            self.assertEqual(rows[0]["sql"], "Select name from cities limit 1")

    def test_build_floodsql_rows_materializes_results(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            family_dir = root / "FloodSQL-Bench" / "benchmark" / "single_table"
            family_dir.mkdir(parents=True)
            (family_dir / "50.json").write_text(
                '[{"id":"L0_0001","question":"How many claims?","sql":"SELECT COUNT(*) FROM claims;"}]',
                encoding="utf-8",
            )

            dataset_config = {
                "datasets": {
                    "floodsql": {
                        "raw_data_path": str(root / "FloodSQL-Bench"),
                        "source_partitions": {
                            "level_l0": {
                                "family": "single_table",
                                "level": "L0",
                                "raw_path": "benchmark/single_table",
                            }
                        },
                    }
                }
            }

            original_resolve = benchmark_formatter.resolve_db_settings
            original_connect = benchmark_formatter._connect_postgres
            original_execute = benchmark_formatter._execute_query_for_results
            try:
                benchmark_formatter.resolve_db_settings = lambda *args, **kwargs: {
                    "host": "localhost",
                    "port": 5432,
                    "database": "benchmark",
                    "user": "postgres",
                    "password": "postgres",
                }
                benchmark_formatter._connect_postgres = lambda *args, **kwargs: _FakeConnection()
                benchmark_formatter._execute_query_for_results = (
                    lambda connection, sql, *, sample_label: ("ok", [[123]])
                )

                rows = benchmark_formatter.build_floodsql_rows(
                    dataset_config,
                    embedded_db_config={"database": {}, "databases": {}},
                    eval_config={"evaluation": {"timeout": 60}},
                )
            finally:
                benchmark_formatter.resolve_db_settings = original_resolve
                benchmark_formatter._connect_postgres = original_connect
                benchmark_formatter._execute_query_for_results = original_execute

            self.assertEqual(len(rows), 1)
            self.assertEqual(rows[0]["id"], "L0_0001")
            self.assertEqual(rows[0]["source_sql"], "SELECT COUNT(*) FROM claims;")
            self.assertEqual(rows[0]["results"], [[123]])
            self.assertEqual(rows[0]["level"], "L0")


if __name__ == "__main__":
    unittest.main()
