import importlib.util
import io
import json
import sys
import tempfile
import types
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest import mock


ROOT = Path(__file__).resolve().parents[1]

try:
    import yaml as _yaml  # type: ignore
except ModuleNotFoundError:
    _yaml = types.ModuleType("yaml")

    def _safe_load(stream):
        data = stream.read()
        return json.loads(data)

    _yaml.safe_load = _safe_load
    sys.modules["yaml"] = _yaml


def _load_module(module_name: str, file_path: Path):
    spec = importlib.util.spec_from_file_location(module_name, file_path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


def _ensure_package(name: str, path: Path) -> None:
    if name in sys.modules:
        return

    module = types.ModuleType(name)
    module.__path__ = [str(path)]
    sys.modules[name] = module


_ensure_package("src", ROOT / "src")
_ensure_package("src.datasets", ROOT / "src" / "datasets")
_ensure_package("src.pipeline", ROOT / "src" / "pipeline")
_load_module("src.datasets.path_utils", ROOT / "src" / "datasets" / "path_utils.py")
pipeline_module = _load_module("src.pipeline.main", ROOT / "src" / "pipeline" / "main.py")


class _DummyLoader:
    def get_dataset_info(self):
        return {
            "name": "spatial_qa",
            "grouping_fields": [],
            "grouping_values": {},
        }


class _DummyDataLoaderFactory:
    @staticmethod
    def create(loader_class_name, dataset_info_dict):
        del loader_class_name, dataset_info_dict
        return _DummyLoader()


class _DummyEvaluator:
    evaluate_calls = []
    save_calls = []

    def __init__(self, db_config, eval_config):
        del db_config, eval_config

    def evaluate(self, predictions, dataset_info, model_name, config_type, output_dir, resume, overwrite):
        del resume, overwrite
        self.__class__.evaluate_calls.append(output_dir)
        return {
            "model": model_name,
            "config": config_type,
            "dataset": dataset_info["name"],
            "statistics": {
                "overall": {
                    "total": len(predictions),
                    "correct": len(predictions),
                    "accuracy": 1.0,
                }
            },
        }

    def save_evaluation(self, eval_result, output_dir):
        self.__class__.save_calls.append(output_dir)
        os_path = Path(output_dir)
        os_path.mkdir(parents=True, exist_ok=True)
        (os_path / "evaluation.json").write_text(
            json.dumps(eval_result),
            encoding="utf-8",
        )


def _install_stub_modules():
    src_pkg = types.ModuleType("src")
    src_pkg.__path__ = [str(ROOT / "src")]
    sys.modules["src"] = src_pkg

    evaluation_pkg = types.ModuleType("src.evaluation")
    evaluation_pkg.__path__ = [str(ROOT / "src" / "evaluation")]
    sys.modules["src.evaluation"] = evaluation_pkg

    datasets_processing = types.ModuleType("src.datasets.processing")
    datasets_processing.DataLoaderFactory = _DummyDataLoaderFactory
    sys.modules["src.datasets.processing"] = datasets_processing

    evaluation_evaluator = types.ModuleType("src.evaluation.evaluator")
    evaluation_evaluator.Evaluator = _DummyEvaluator
    sys.modules["src.evaluation.evaluator"] = evaluation_evaluator

    _load_module(
        "src.evaluation.report_generator",
        ROOT / "src" / "evaluation" / "report_generator.py",
    )

    inference_module = types.ModuleType("src.inference.model_inference")
    inference_module.build_model_run_name = lambda model_name, backend: f"{model_name}__{backend}"
    sys.modules["src.inference.model_inference"] = inference_module


class PipelineResultsPathTests(unittest.TestCase):
    def setUp(self):
        _DummyEvaluator.evaluate_calls = []
        _DummyEvaluator.save_calls = []
        _install_stub_modules()

        self.temp_dir = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp_dir.cleanup)
        self.temp_path = Path(self.temp_dir.name)
        self.config_dir = self.temp_path / "config"
        self.config_dir.mkdir()

        results_root = self.temp_path / "results"
        tasks_dir = results_root / "tasks"
        benchmarks_dir = results_root / "benchmarks"
        sessions_dir = results_root / "sessions"

        (self.config_dir / "dataset_config.yaml").write_text(
            json.dumps(
                {
                    "default_dataset": "spatial_qa",
                    "datasets": {
                        "spatial_qa": {
                            "loader_class": "DummyLoader",
                            "database": "default",
                        },
                        "spatialsql_pg": {
                            "loader_class": "DummyLoader",
                            "database": "default",
                        },
                        "floodsql_pg": {
                            "loader_class": "DummyLoader",
                            "database": "default",
                        },
                    },
                }
            ),
            encoding="utf-8",
        )
        (self.config_dir / "db_config.yaml").write_text(
            json.dumps(
                {
                    "database": {
                        "host": "localhost",
                        "port": 5432,
                        "database": "test",
                        "user": "test",
                        "password": "test",
                    }
                }
            ),
            encoding="utf-8",
        )
        (self.config_dir / "model_config.yaml").write_text(
            json.dumps(
                {
                    "default_models": ["qwen3-8b"],
                    "default_backend": "vllm",
                }
            ),
            encoding="utf-8",
        )
        (self.config_dir / "eval_config.yaml").write_text(
            json.dumps(
                {
                    "default_configs": ["base"],
                    "results": {
                        "output_dir": str(results_root),
                        "tasks_dir": str(tasks_dir),
                        "benchmarks_dir": str(benchmarks_dir),
                        "sessions_dir": str(sessions_dir),
                    },
                }
            ),
            encoding="utf-8",
        )

        args = SimpleNamespace(
            config_dir=str(self.config_dir),
            dataset=["spatial_qa"],
            models=None,
            backend=None,
            configs=None,
            preprocess=False,
            build_rag=False,
            inference=False,
            evaluate=True,
            benchmark=False,
            enable_prediction_postprocess=False,
        )
        self.pipeline = pipeline_module.MainPipeline(args)

    def test_task_paths_include_dataset_backend_model_config(self):
        task_dir = self.pipeline._get_task_dir("qwen3-8b", "base")
        latest_summary = self.pipeline._get_task_summary_file(
            "qwen3-8b",
            "base",
            latest=True,
        )
        latest_prompt = self.pipeline._get_task_prompt_file(
            "qwen3-8b",
            "base",
            latest=True,
        )

        self.assertEqual(
            task_dir,
            str(
                self.temp_path
                / "results"
                / "tasks"
                / "spatial_qa"
                / "vllm"
                / "qwen3-8b"
                / "base"
            ),
        )
        self.assertEqual(
            latest_summary,
            str(
                self.temp_path
                / "results"
                / "tasks"
                / "spatial_qa"
                / "vllm"
                / "qwen3-8b"
                / "base"
                / "latest"
                / "summary.json"
            ),
        )
        self.assertEqual(
            latest_prompt,
            str(
                self.temp_path
                / "results"
                / "tasks"
                / "spatial_qa"
                / "vllm"
                / "qwen3-8b"
                / "base"
                / "latest"
                / "prompts.json"
            ),
        )

    def test_dataset_all_includes_floodsql_pg(self):
        args = SimpleNamespace(
            config_dir=str(self.config_dir),
            dataset=["all"],
            models=None,
            backend=None,
            configs=None,
            preprocess=False,
            build_rag=False,
            inference=False,
            evaluate=False,
            benchmark=False,
            enable_prediction_postprocess=False,
        )
        pipeline = pipeline_module.MainPipeline(args)
        self.assertEqual(
            pipeline.dataset_names,
            ["spatial_qa", "spatialsql_pg", "floodsql_pg"],
        )

    def test_evaluation_only_reads_latest_task_predictions_and_writes_history_and_latest_outputs(self):
        prediction_file = Path(
            self.pipeline._get_task_prediction_file("qwen3-8b", "base", latest=True)
        )
        prediction_file.parent.mkdir(parents=True, exist_ok=True)
        prediction_file.write_text(
            json.dumps([{"id": "1", "predicted_sql": "SELECT 1;", "gold_sql": "SELECT 1;"}]),
            encoding="utf-8",
        )

        results = self.pipeline._run_evaluation_only()

        expected_eval_dir = str(
            self.temp_path
            / "results"
            / "tasks"
            / "spatial_qa"
            / "vllm"
            / "qwen3-8b"
            / "base"
        )
        expected_save_dir = str(
            self.temp_path
            / "results"
            / "tasks"
            / "spatial_qa"
            / "vllm"
            / "qwen3-8b"
            / "base"
            / "runs"
            / self.pipeline.run_id
        )

        self.assertEqual(len(results), 1)
        self.assertEqual(_DummyEvaluator.evaluate_calls, [expected_eval_dir])
        self.assertEqual(_DummyEvaluator.save_calls, [expected_save_dir])
        self.assertTrue(
            Path(
                self.pipeline._get_task_evaluation_file("qwen3-8b", "base", latest=True)
            ).exists()
        )
        self.assertTrue(
            Path(
                self.pipeline._get_task_summary_file("qwen3-8b", "base", latest=True)
            ).exists()
        )

    def test_evaluation_only_falls_back_to_history_predictions(self):
        prediction_file = Path(
            self.pipeline._get_task_prediction_file("qwen3-8b", "base", latest=False)
        )
        prediction_file.parent.mkdir(parents=True, exist_ok=True)
        prediction_file.write_text(
            json.dumps([{"id": "1", "predicted_sql": "SELECT 1;", "gold_sql": "SELECT 1;"}]),
            encoding="utf-8",
        )

        results = self.pipeline._run_evaluation_only()

        self.assertEqual(len(results), 1)
        self.assertEqual(len(_DummyEvaluator.evaluate_calls), 1)
        self.assertEqual(len(_DummyEvaluator.save_calls), 1)

    def test_collect_task_eval_results_reads_latest_task_outputs(self):
        latest_eval = Path(
            self.pipeline._get_task_evaluation_file("qwen3-8b", "base", latest=True)
        )
        latest_eval.parent.mkdir(parents=True, exist_ok=True)
        latest_eval.write_text(
            json.dumps(
                {
                    "dataset": "spatial_qa",
                    "model": "qwen3-8b__vllm",
                    "config": "base",
                    "statistics": {"overall": {"total": 90, "correct": 27, "accuracy": 0.3}},
                }
            ),
            encoding="utf-8",
        )

        collected = self.pipeline._collect_task_eval_results()
        self.assertEqual(len(collected), 1)
        self.assertEqual(collected[0]["model"], "qwen3-8b__vllm")

    def test_collect_task_eval_results_skips_mismatched_latest_prompt(self):
        template_path = self.temp_path / "prompts" / "text2sql_prompt.txt"
        template_path.parent.mkdir(parents=True, exist_ok=True)
        template_path.write_text(
            "CURRENT TEMPLATE\n\n## User Question\n{{question_block}}\n",
            encoding="utf-8",
        )
        latest_eval = Path(
            self.pipeline._get_task_evaluation_file("qwen3-8b", "base", latest=True)
        )
        latest_eval.parent.mkdir(parents=True, exist_ok=True)
        latest_eval.write_text(
            json.dumps(
                {
                    "dataset": "spatial_qa",
                    "model": "qwen3-8b__vllm",
                    "config": "base",
                    "statistics": {"overall": {"total": 90, "correct": 27, "accuracy": 0.3}},
                }
            ),
            encoding="utf-8",
        )
        latest_prompt = Path(
            self.pipeline._get_task_prompt_file("qwen3-8b", "base", latest=True)
        )
        latest_prompt.write_text(
            json.dumps(
                [
                    {
                        "id": "1",
                        "prompt": "OLDER TEMPLATE\n\n## User Question\nQ",
                    }
                ]
            ),
            encoding="utf-8",
        )

        collected = self.pipeline._collect_task_eval_results()

        self.assertEqual(collected, [])
        self.assertEqual(len(self.pipeline._benchmark_prompt_issues), 1)
        self.assertEqual(
            self.pipeline._benchmark_prompt_issues[0]["reason"],
            "prompt_prefix_sha256_mismatch",
        )

    def test_benchmark_mode_is_exclusive(self):
        with mock.patch.object(
            sys,
            "argv",
            [
                "run_pipeline.py",
                "--config-dir",
                str(self.config_dir),
                "--benchmark",
                "--evaluate",
            ],
        ):
            stderr = io.StringIO()
            with self.assertRaises(SystemExit) as ctx, mock.patch("sys.stderr", stderr):
                pipeline_module.main()

        self.assertNotEqual(ctx.exception.code, 0)
        self.assertIn("--benchmark 仅汇总各 task 的 latest 结果", stderr.getvalue())
        self.assertIn("--evaluate", stderr.getvalue())

    def test_build_benchmark_run_metadata_includes_setup_and_validation(self):
        self.pipeline._dataset_index_status = {
            "spatial_qa": {
                "status": "ready",
                "index_profile": "spatial_qa_geography_v1",
            }
        }
        self.pipeline._evaluation_validation = {
            "status": "ready",
            "issues": [],
        }

        metadata = self.pipeline._build_benchmark_run_metadata()

        self.assertIn("dataset_index_status", metadata)
        self.assertEqual(
            metadata["dataset_index_status"]["spatial_qa"]["index_profile"],
            "spatial_qa_geography_v1",
        )
        self.assertEqual(metadata["task_source"], "task latest")
        self.assertEqual(metadata["benchmark_mode"], "aggregate_only")
        self.assertNotIn("prediction_postprocess_enabled", metadata)
        self.assertEqual(metadata["validation_notes"]["status"], "ready")

    def test_collect_benchmark_setup_metadata_marks_dataset_policies(self):
        args = SimpleNamespace(
            config_dir=str(self.config_dir),
            dataset=["all"],
            models=None,
            backend=None,
            configs=None,
            preprocess=False,
            build_rag=False,
            inference=False,
            evaluate=False,
            benchmark=True,
            enable_prediction_postprocess=False,
        )
        pipeline = pipeline_module.MainPipeline(args)

        spatial_setup_module = types.ModuleType("src.sql.spatial_qa_benchmark_setup")
        spatial_setup_module.inspect_spatial_qa_benchmark_setup = lambda db_config: {
            "dataset": "spatial_qa",
            "status": "ready",
            "index_profile": "spatial_qa_geography_v1",
            "database_name": db_config["database"],
        }
        sys.modules["src.sql.spatial_qa_benchmark_setup"] = spatial_setup_module

        metadata = pipeline._collect_benchmark_setup_metadata()

        self.assertEqual(metadata["spatial_qa"]["status"], "ready")
        self.assertEqual(metadata["floodsql_pg"]["status"], "managed_by_migration")
        self.assertEqual(metadata["spatialsql_pg"]["status"], "not_required")


if __name__ == "__main__":
    unittest.main()
