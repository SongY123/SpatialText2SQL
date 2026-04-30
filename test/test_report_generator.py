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
_ensure_package("src.evaluation", ROOT / "src" / "evaluation")

report_generator_module = _load_module(
    "src.evaluation.report_generator",
    ROOT / "src" / "evaluation" / "report_generator.py",
)


class ReportGeneratorSummaryTests(unittest.TestCase):
    def test_save_summary_writes_single_summary_file(self):
        generator = report_generator_module.ReportGenerator(
            {
                "name": "spatial_qa",
                "grouping_fields": ["level"],
                "grouping_values": {"level": [1, 2, 3]},
            }
        )
        eval_results = [
            {
                "model": "qwen3-8b__vllm",
                "config": "base",
                "statistics": {"overall": {"total": 10, "correct": 2, "accuracy": 0.2}},
            },
            {
                "model": "qwen3-coder-next-fp8__vllm",
                "config": "base",
                "statistics": {"overall": {"total": 10, "correct": 3, "accuracy": 0.3}},
            },
        ]

        with tempfile.TemporaryDirectory() as tmpdir:
            summary_path = Path(tmpdir) / "summary.json"
            generator.save_summary(eval_results, str(summary_path))

            self.assertTrue(summary_path.exists())
            data = json.loads(summary_path.read_text(encoding="utf-8"))
            self.assertEqual(data["dataset"], "spatial_qa")
            self.assertEqual(len(data["results"]), 2)
            self.assertEqual(data["results"][0]["model"], "qwen3-8b__vllm")

    def test_generate_report_supports_multiple_grouping_fields(self):
        generator = report_generator_module.ReportGenerator(
            {
                "name": "floodsql_pg",
                "grouping_fields": ["level", "family"],
                "grouping_values": {
                    "level": ["L0", "L1"],
                    "family": ["single_table", "double_table_key"],
                },
            }
        )
        report = generator.generate_report(
            [
                {
                    "model": "qwen3-8b__vllm",
                    "config": "base",
                    "statistics": {
                        "overall": {"total": 2, "correct": 1, "accuracy": 0.5},
                        "all_samples": {
                            "grouped": {
                                "level": {
                                    "L0": {"total": 1, "correct": 1, "accuracy": 1.0},
                                    "L1": {"total": 1, "correct": 0, "accuracy": 0.0},
                                },
                                "family": {
                                    "single_table": {"total": 1, "correct": 1, "accuracy": 1.0},
                                    "double_table_key": {"total": 1, "correct": 0, "accuracy": 0.0},
                                },
                            }
                        },
                    },
                }
            ]
        )

        self.assertIn("By level", report)
        self.assertIn("By family", report)
        self.assertIn("single_table", report)

    def test_generate_report_reads_grouped_stats_from_all_samples_without_na(self):
        generator = report_generator_module.ReportGenerator(
            {
                "name": "spatial_qa",
                "grouping_fields": ["level"],
                "grouping_values": {"level": [1, 2, 3]},
            }
        )
        report = generator.generate_report(
            [
                {
                    "model": "qwen3-coder-next-fp8__vllm",
                    "config": "base",
                    "statistics": {
                        "overall": {"total": 90, "correct": 27, "accuracy": 0.3},
                        "all_samples": {
                            "overall": {"total": 90, "correct": 27, "accuracy": 0.3},
                            "grouped": {
                                "level": {
                                    "1": {"total": 30, "correct": 15, "accuracy": 0.5},
                                    "2": {"total": 30, "correct": 7, "accuracy": 0.23333333333333334},
                                    "3": {"total": 30, "correct": 5, "accuracy": 0.16666666666666666},
                                }
                            },
                        },
                    },
                }
            ]
        )

        self.assertIn("50.0%", report)
        self.assertIn("23.3%", report)
        self.assertIn("16.7%", report)
        self.assertNotIn("│ N/A ", report)

    def test_benchmark_report_generator_builds_cross_dataset_summary(self):
        generator = report_generator_module.BenchmarkReportGenerator()
        eval_results = [
            {
                "dataset": "spatial_qa",
                "model": "qwen3-8b__vllm",
                "config": "base",
                "statistics": {
                    "overall": {"total": 90, "correct": 27, "accuracy": 0.3},
                    "all_samples": {
                        "grouped": {
                            "level": {
                                "1": {"total": 30, "correct": 15, "accuracy": 0.5},
                                "2": {"total": 30, "correct": 8, "accuracy": 0.26666666666666666},
                                "3": {"total": 30, "correct": 4, "accuracy": 0.13333333333333333},
                            }
                        }
                    },
                    "inference_metrics": {
                        "avg_input_tokens": 100.4,
                        "avg_output_tokens": 31.2,
                        "avg_total_tokens": 131.6,
                        "avg_latency_ms": 245.1,
                        "sum_input_tokens": 9036.0,
                        "sum_output_tokens": 2808.0,
                        "sum_total_tokens": 11844.0,
                        "sum_latency_ms": 22059.0,
                        "question_count": 90,
                    },
                },
            },
            {
                "dataset": "spatialsql_pg",
                "model": "qwen3-8b__vllm",
                "config": "base",
                "statistics": {
                    "overall": {"total": 400, "correct": 55, "accuracy": 0.1375},
                    "inference_metrics": {
                        "avg_input_tokens": 210.2,
                        "avg_output_tokens": 42.7,
                        "avg_total_tokens": 252.9,
                        "avg_latency_ms": 511.0,
                        "sum_input_tokens": 84080.0,
                        "sum_output_tokens": 17080.0,
                        "sum_total_tokens": 101160.0,
                        "sum_latency_ms": 204400.0,
                        "question_count": 400,
                    },
                    "trusted_samples": {
                        "available": True,
                        "overall": {"total": 287, "correct": 44, "accuracy": 44 / 287},
                    },
                },
            },
            {
                "dataset": "floodsql_pg",
                "model": "qwen3-8b__vllm",
                "config": "base",
                "statistics": {
                    "overall": {"total": 443, "correct": 90, "accuracy": 90 / 443},
                    "all_samples": {
                        "grouped": {
                            "level": {
                                "L0": {"total": 50, "correct": 20, "accuracy": 0.4},
                                "L1": {"total": 50, "correct": 18, "accuracy": 0.36},
                                "L2": {"total": 50, "correct": 15, "accuracy": 0.3},
                                "L3": {"total": 50, "correct": 14, "accuracy": 0.28},
                                "L4": {"total": 50, "correct": 12, "accuracy": 0.24},
                                "L5": {"total": 193, "correct": 11, "accuracy": 11 / 193},
                            }
                        }
                    },
                    "inference_metrics": {
                        "avg_input_tokens": 180.0,
                        "avg_output_tokens": 40.0,
                        "avg_total_tokens": 220.0,
                        "avg_latency_ms": 530.0,
                        "sum_input_tokens": 79740.0,
                        "sum_output_tokens": 17720.0,
                        "sum_total_tokens": 97460.0,
                        "sum_latency_ms": 234790.0,
                        "question_count": 443,
                    },
                    "trusted_samples": {
                        "available": True,
                        "overall": {"total": 430, "correct": 89, "accuracy": 89 / 430},
                    },
                },
            },
            {
                "dataset": "spatial_qa",
                "model": "qwen3-coder-next-fp8__vllm",
                "config": "base",
                "statistics": {
                    "overall": {"total": 90, "correct": 25, "accuracy": 25 / 90},
                    "trusted_samples": {
                        "available": False,
                        "reason": "No trusted samples found in dataset-specific trusted report",
                        "overall": {"total": 0, "correct": 0, "accuracy": 0.0},
                    },
                },
            },
        ]
        run_metadata = {
            "datasets": ["spatial_qa", "spatialsql_pg", "floodsql_pg"],
            "models": ["qwen3-8b", "qwen3-coder-next-fp8"],
            "configs": ["base"],
            "backend": "vllm",
            "task_source": "task latest",
            "benchmark_mode": "aggregate_only",
            "dataset_index_status": {
                "spatial_qa": {
                    "status": "ready",
                    "index_profile": "spatial_qa_geography_v1",
                    "missing_indexes": [],
                },
                "spatialsql_pg": {
                    "status": "not_required",
                },
                "floodsql_pg": {
                    "status": "managed_by_migration",
                    "index_profile": "floodsql_geometry_v1",
                },
            },
            "validation_notes": {
                "status": "gold_unstable",
                "issues": [
                    {
                        "dataset": "spatial_qa",
                        "model": "qwen3-8b__vllm",
                        "config": "base",
                        "sample_count": 1,
                        "issue_breakdown": {"gold_execution_error": 1},
                    }
                ],
            },
            "model_catalog": {
                "qwen3-8b": {
                    "display_name": "Qwen3-8B",
                    "size_label": "8B",
                }
            },
            "dataset_catalog": {
                "spatial_qa": {
                    "name": "spatial_qa",
                    "grouping_fields": ["level"],
                    "grouping_values": {"level": [1, 2, 3]},
                },
                "spatialsql_pg": {
                    "name": "spatialsql_pg",
                    "grouping_fields": ["split"],
                    "grouping_values": {
                        "split": ["dataset1_ada", "dataset1_edu"],
                    },
                },
                "floodsql_pg": {
                    "name": "floodsql_pg",
                    "grouping_fields": ["level"],
                    "grouping_values": {
                        "level": ["L0", "L1", "L2", "L3", "L4", "L5"],
                    },
                },
            },
        }

        summary = generator.build_summary(eval_results, run_metadata)
        self.assertIn("qwen3-8b", summary["matrices"]["overall"])
        self.assertEqual(
            summary["matrices"]["overall"]["qwen3-8b"]["spatial_qa"]["accuracy"],
            0.3,
        )
        self.assertEqual(summary["results"][0]["avg_input_tokens"], 100.4)
        self.assertNotIn("trusted_datasets", summary)
        self.assertEqual(set(summary["matrices"].keys()), {"overall"})
        self.assertEqual(len(summary["missing_results"]), 2)

        report = generator.generate_report(eval_results, run_metadata)
        self.assertIn("Overall", report)
        self.assertNotIn("Trusted", report)
        self.assertIn("spatial_qa", report)
        self.assertIn("spatialsql_pg", report)
        self.assertIn("floodsql_pg", report)
        self.assertIn("qwen3-8b", report)
        self.assertIn("Source: task latest", report)
        self.assertIn("Mode: aggregate only", report)
        self.assertIn("Dataset: spatial_qa", report)
        self.assertIn("By level", report)
        self.assertIn("Index Status", report)
        self.assertIn("Validation Notes: gold_unstable", report)
        self.assertIn("spatial_qa: ready", report)
        self.assertIn("floodsql_pg: managed_by_migration", report)
        self.assertNotIn("Prediction Postprocess", report)

    def test_benchmark_latest_uses_fixed_filenames(self):
        generator = report_generator_module.BenchmarkReportGenerator()
        eval_results = [
            {
                "dataset": "spatial_qa",
                "model": "qwen3-8b__vllm",
                "config": "base",
                "statistics": {
                    "overall": {"total": 90, "correct": 27, "accuracy": 0.3},
                },
            }
        ]
        run_metadata = {
            "datasets": ["spatial_qa"],
            "models": ["qwen3-8b"],
            "configs": ["base"],
            "backend": "vllm",
            "prediction_postprocess_enabled": False,
            "task_source": "latest",
            "model_catalog": {
                "qwen3-8b": {
                    "display_name": "Qwen3-8B",
                    "size_label": "8B",
                    "paper_group": "open_source",
                    "paper_order": 10,
                }
            },
            "dataset_catalog": {
                "spatial_qa": {
                    "name": "spatial_qa",
                    "grouping_fields": ["level"],
                    "grouping_values": {"level": [1, 2, 3]},
                }
            },
        }

        with tempfile.TemporaryDirectory() as tmpdir:
            latest_dir = Path(tmpdir) / "latest"
            saved = generator.save_summary(eval_results, str(latest_dir), run_metadata)
            self.assertEqual(
                saved,
                [
                    str(latest_dir / "summary.json"),
                    str(latest_dir / "summary.txt"),
                    str(latest_dir / "paper_tables.md"),
                    str(latest_dir / "overall_performance.csv"),
                    str(latest_dir / "avg_tokens.csv"),
                    str(latest_dir / "avg_latency.csv"),
                ],
            )

    def test_paper_tables_export_open_source_rows_with_expected_mapping(self):
        generator = report_generator_module.BenchmarkReportGenerator()
        eval_results = [
            {
                "dataset": "spatial_qa",
                "model": "qwen2.5-coder-7b__vllm",
                "config": "base",
                "statistics": {
                    "overall": {"total": 90, "correct": 36, "accuracy": 0.4},
                    "all_samples": {
                        "grouped": {
                            "level": {
                                "1": {"total": 30, "correct": 18, "accuracy": 0.6},
                                "2": {"total": 30, "correct": 12, "accuracy": 0.4},
                                "3": {"total": 30, "correct": 6, "accuracy": 0.2},
                            }
                        }
                    },
                    "inference_metrics": {
                        "avg_input_tokens": 100.0,
                        "avg_output_tokens": 25.0,
                        "avg_total_tokens": 125.0,
                        "avg_latency_ms": 220.0,
                    },
                },
            },
            {
                "dataset": "spatialsql_pg",
                "model": "qwen2.5-coder-7b__vllm",
                "config": "base",
                "statistics": {
                    "overall": {"total": 400, "correct": 80, "accuracy": 0.2},
                    "inference_metrics": {
                        "avg_input_tokens": 200.0,
                        "avg_output_tokens": 40.0,
                        "avg_total_tokens": 240.0,
                        "avg_latency_ms": 510.0,
                    },
                },
            },
            {
                "dataset": "floodsql_pg",
                "model": "qwen2.5-coder-7b__vllm",
                "config": "base",
                "statistics": {
                    "overall": {"total": 443, "correct": 111, "accuracy": 111 / 443},
                    "all_samples": {
                        "grouped": {
                            "level": {
                                "L0": {"total": 50, "correct": 25, "accuracy": 0.5},
                                "L1": {"total": 50, "correct": 20, "accuracy": 0.4},
                                "L2": {"total": 50, "correct": 15, "accuracy": 0.3},
                                "L3": {"total": 50, "correct": 10, "accuracy": 0.2},
                                "L4": {"total": 50, "correct": 5, "accuracy": 0.1},
                                "L5": {"total": 193, "correct": 36, "accuracy": 36 / 193},
                            }
                        }
                    },
                    "inference_metrics": {
                        "avg_input_tokens": 150.2,
                        "avg_output_tokens": 30.4,
                        "avg_total_tokens": 180.6,
                        "avg_latency_ms": 800.9,
                    },
                },
            },
        ]
        run_metadata = {
            "datasets": ["spatial_qa", "spatialsql_pg", "floodsql_pg"],
            "models": ["qwen2.5-coder-7b"],
            "configs": ["base"],
            "backend": "vllm",
            "prediction_postprocess_enabled": False,
            "task_source": "latest",
            "model_catalog": {
                "qwen2.5-coder-7b": {
                    "display_name": "Qwen2.5-Coder-7B-Instruct",
                    "size_label": "7B",
                    "paper_group": "open_source",
                    "paper_order": 10,
                }
            },
            "dataset_catalog": {
                "spatial_qa": {
                    "name": "spatial_qa",
                    "grouping_fields": ["level"],
                    "grouping_values": {"level": [1, 2, 3]},
                },
                "spatialsql_pg": {
                    "name": "spatialsql_pg",
                    "grouping_fields": ["split"],
                    "grouping_values": {"split": []},
                },
                "floodsql_pg": {
                    "name": "floodsql_pg",
                    "grouping_fields": ["level"],
                    "grouping_values": {"level": ["L0", "L1", "L2", "L3", "L4", "L5"]},
                },
            },
        }

        summary = generator.build_summary(eval_results, run_metadata)
        table4 = summary["paper_tables"]["table4_overall_performance"]
        table6 = summary["paper_tables"]["table6_avg_tokens"]
        table7 = summary["paper_tables"]["table7_avg_latency"]

        self.assertEqual(table4["rows"][0][3:7], ["60.0", "40.0", "20.0", "40.0"])
        self.assertEqual(table4["rows"][0][7:14], ["50.0", "40.0", "30.0", "20.0", "10.0", "18.7", "25.1"])
        self.assertEqual(table4["rows"][0][14:], ["-", "-", "-", "-"])
        self.assertEqual(table6["rows"][0][2:11], ["200", "40", "240", "100", "25", "125", "150", "30", "181"])
        self.assertEqual(table7["rows"][0][2:], ["510", "220", "801", "-"])

    def test_paper_tables_tolerate_missing_paper_order(self):
        generator = report_generator_module.BenchmarkReportGenerator()
        eval_results = [
            {
                "dataset": "spatial_qa",
                "model": "qwen2.5-coder-7b__vllm",
                "config": "base",
                "statistics": {
                    "overall": {"total": 90, "correct": 36, "accuracy": 0.4},
                    "all_samples": {
                        "grouped": {
                            "level": {
                                "1": {"total": 30, "correct": 18, "accuracy": 0.6},
                                "2": {"total": 30, "correct": 12, "accuracy": 0.4},
                                "3": {"total": 30, "correct": 6, "accuracy": 0.2},
                            }
                        }
                    },
                },
            },
            {
                "dataset": "spatial_qa",
                "model": "qwen3-8b__vllm",
                "config": "base",
                "statistics": {
                    "overall": {"total": 90, "correct": 27, "accuracy": 0.3},
                    "all_samples": {
                        "grouped": {
                            "level": {
                                "1": {"total": 30, "correct": 15, "accuracy": 0.5},
                                "2": {"total": 30, "correct": 8, "accuracy": 0.26666666666666666},
                                "3": {"total": 30, "correct": 4, "accuracy": 0.13333333333333333},
                            }
                        }
                    },
                },
            },
        ]
        run_metadata = {
            "datasets": ["spatial_qa"],
            "models": ["qwen2.5-coder-7b", "qwen3-8b"],
            "configs": ["base"],
            "backend": "vllm",
            "task_source": "task latest",
            "benchmark_mode": "aggregate_only",
            "model_catalog": {
                "qwen2.5-coder-7b": {
                    "display_name": "Qwen2.5-Coder-7B-Instruct",
                    "size_label": "7B",
                    "paper_group": "open_source",
                    "paper_order": 10,
                },
                "qwen3-8b": {
                    "display_name": "Qwen3-8B",
                    "size_label": "8B",
                    "paper_group": "open_source",
                    "paper_order": None,
                },
            },
            "dataset_catalog": {
                "spatial_qa": {
                    "name": "spatial_qa",
                    "grouping_fields": ["level"],
                    "grouping_values": {"level": [1, 2, 3]},
                }
            },
        }

        summary = generator.build_summary(eval_results, run_metadata)
        table4 = summary["paper_tables"]["table4_overall_performance"]
        self.assertEqual([row[0] for row in table4["rows"]], ["Qwen2.5-Coder-7B-Instruct", "Qwen3-8B"])


if __name__ == "__main__":
    unittest.main()
