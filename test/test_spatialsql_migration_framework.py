import importlib.util
import sys
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
_ensure_package("src.datasets.loaders", ROOT / "src" / "datasets" / "loaders")
_ensure_package("src.inference", ROOT / "src" / "inference")
_ensure_package("src.sql", ROOT / "src" / "sql")

_load_module("src.datasets.base", ROOT / "src" / "datasets" / "base.py")
_load_module(
    "src.datasets.loaders.spatial_sql_loader",
    ROOT / "src" / "datasets" / "loaders" / "spatial_sql_loader.py",
)
_load_module("src.sql.sql_dialect_adapter", ROOT / "src" / "sql" / "sql_dialect_adapter.py")
_load_module("src.inference.sql_utils", ROOT / "src" / "inference" / "sql_utils.py")

migration_framework = _load_module(
    "src.sql.spatialsql_migration_framework",
    ROOT / "src" / "sql" / "spatialsql_migration_framework.py",
)


class SpatialSqlMigrationFrameworkTests(unittest.TestCase):
    def test_build_source_inventory_detects_tourism_geometry_anomaly(self):
        inventory, anomalies = migration_framework.build_source_inventory(
            ROOT / "sdbdatasets",
            versions=["dataset1"],
            domains=["tourism"],
        )

        tourism = inventory["splits"]["dataset1_tourism"]
        self.assertIn("scenicSpots", tourism["business_tables"])
        self.assertIn("geometry_columns", tourism["actual_tables"])
        self.assertTrue(
            any(
                item["classification"] == "data_geometry_error"
                and item["split"] == "dataset1_tourism"
                for item in anomalies["details"]
            )
        )

    def test_compare_sql_results_distinguishes_format_difference(self):
        status, details = migration_framework.compare_sql_results(
            [(1, "a"), (2, "b")],
            [(2, "b"), (1, "a")],
        )

        self.assertEqual(status, "format_difference")
        self.assertEqual(details["source_count"], 2)
        self.assertEqual(details["target_count"], 2)

    def test_decode_spatialite_point_blob(self):
        blob_hex = "0001E61000006AC3BC2253185D40AB093FC4C8F743406AC3BC2253185D40AB093FC4C8F743407C010000006AC3BC2253185D40AB093FC4C8F74340FE"
        x, y, srid = migration_framework._decode_spatialite_point_blob(bytes.fromhex(blob_hex))
        self.assertEqual(srid, 4326)
        self.assertAlmostEqual(x, 116.3800742)
        self.assertAlmostEqual(y, 39.93581441)

    def test_supports_manual_blob_geometry_fallback_for_point_without_metadata(self):
        table_info = {
            "expected_geometry_columns": ["Location"],
            "geometry_metadata": [],
            "columns": [
                {"name": "name", "declared_type": "TEXT"},
                {"name": "Location", "declared_type": "POINT"},
            ],
        }
        self.assertTrue(migration_framework._supports_manual_blob_geometry_fallback(table_info))

    def test_build_consistency_cluster_report_groups_similar_failures(self):
        report = migration_framework.build_consistency_cluster_report(
            {
                "details": [
                    {
                        "split": "dataset1_ada",
                        "source_id": "ada10",
                        "status": "target_error",
                        "classification": "sql_rule_gap",
                        "target_error": "syntax error at or near 'order'\nLINE 1: ...",
                        "source_sql": "s1",
                        "target_sql": "t1",
                    },
                    {
                        "split": "dataset2_ada",
                        "source_id": "ada10",
                        "status": "target_error",
                        "classification": "sql_rule_gap",
                        "target_error": "syntax error at or near 'order'\nLINE 1: ...",
                        "source_sql": "s2",
                        "target_sql": "t2",
                    },
                ]
            }
        )
        self.assertEqual(report["summary"]["cluster_count"], 1)
        self.assertEqual(report["clusters"][0]["count"], 2)

    def test_summarize_consistency_report_tracks_mismatch_subtypes(self):
        report = migration_framework.summarize_consistency_report(
            {
                "summary": {"total": 3},
                "details": [
                    {"split": "dataset1_ada", "status": "exact_match"},
                    {"split": "dataset1_ada", "status": "semantic_mismatch", "classification": "semantic_mismatch", "mismatch_subtype": "numeric_measurement_difference"},
                    {"split": "dataset2_ada", "status": "target_error", "classification": "sql_rule_gap"},
                ],
            }
        )
        self.assertEqual(report["by_status"]["exact_match"], 1)
        self.assertEqual(report["by_status"]["semantic_mismatch"], 1)
        self.assertEqual(report["by_classification"]["sql_rule_gap"], 1)
        self.assertEqual(report["by_mismatch_subtype"]["numeric_measurement_difference"], 1)

    def test_classify_semantic_mismatch_prefers_numeric_difference_for_labeled_rows(self):
        subtype = migration_framework.classify_semantic_mismatch(
            {
                "source_sql": "Select name, Area(Shape, 1) from provinces",
                "target_sql": "Select name, ST_Area(shape::geography, true) from dataset1_ada_provinces",
            },
            {
                "source_count": 1,
                "target_count": 1,
                "only_in_source": ["('新疆维吾尔自治区', 1630565307479.656)"],
                "only_in_target": ["('新疆维吾尔自治区', 1630566489471.1335)"],
            },
        )
        self.assertEqual(subtype, "numeric_measurement_difference")


if __name__ == "__main__":
    unittest.main()
