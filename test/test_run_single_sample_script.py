import importlib.util
import sys
import types
import unittest
from argparse import Namespace
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
_ensure_package("src.inference", ROOT / "src" / "inference")
_ensure_package("scripts", ROOT / "scripts")
_ensure_package("scripts.evaluation", ROOT / "scripts" / "evaluation")

base_module = _load_module(
    "src.inference.base",
    ROOT / "src" / "inference" / "base.py",
)
_load_module(
    "src.inference.model_inference",
    ROOT / "src" / "inference" / "model_inference.py",
)
script_module = _load_module(
    "scripts.evaluation.run_single_sample",
    ROOT / "scripts" / "evaluation" / "run_single_sample.py",
)


class FakePromptBuilder:
    def build_prompt(self, **kwargs):
        del kwargs
        return "SELECT * FROM demo"


class FakeLoader:
    def generate(self, prompt: str):
        del prompt
        return base_module.GenerationResult(
            sql="SELECT id FROM demo",
            raw_text="SELECT id FROM demo",
            usage={"prompt_tokens": 12, "completion_tokens": 6, "total_tokens": 18},
            response_metadata={"reasoning": "chain", "content": "SELECT id FROM demo"},
        )


class FakeModelInference:
    def _build_inference_metrics(
        self,
        model_loader,
        prompt,
        generation_result,
        started_at_unix_ms,
        finished_at_unix_ms,
        latency_ms,
        status,
    ):
        del model_loader, prompt, started_at_unix_ms, finished_at_unix_ms
        return {
            "input_tokens": generation_result.usage["prompt_tokens"],
            "output_tokens": generation_result.usage["completion_tokens"],
            "total_tokens": generation_result.usage["total_tokens"],
            "latency_ms": latency_ms,
            "measurement_source": "api_usage",
            "status": status,
        }

    def _normalize_prediction(self, final_sql, sample):
        del sample
        return final_sql.lower()


class RunSingleSampleScriptTests(unittest.TestCase):
    def test_run_one_sample_collects_inference_metrics(self):
        result = script_module.run_one_sample(
            sample={
                "id": 1,
                "question": "q",
                "schema": "table demo(id integer)",
                "gold_sql": "SELECT id FROM demo",
                "metadata": {"split": "dataset1_ada"},
            },
            args=Namespace(config="base", dataset="spatialsql_pg"),
            prompt_builder=FakePromptBuilder(),
            loader=FakeLoader(),
            model_inference=FakeModelInference(),
            evaluator=None,
        )

        self.assertEqual(result["raw_reasoning"], "chain")
        self.assertEqual(result["raw_content"], "SELECT id FROM demo")
        self.assertEqual(result["final_sql"], "SELECT id FROM demo")
        self.assertEqual(result["normalized_sql"], "select id from demo")
        self.assertEqual(result["inference_metrics"]["input_tokens"], 12)
        self.assertEqual(result["inference_metrics"]["output_tokens"], 6)
        self.assertEqual(result["inference_metrics"]["total_tokens"], 18)
        self.assertEqual(result["inference_metrics"]["measurement_source"], "api_usage")
        self.assertEqual(result["inference_metrics"]["status"], "success")
        self.assertIsInstance(result["inference_metrics"]["latency_ms"], float)

    def test_summarize_inference_metrics_aggregates_tokens_and_latency(self):
        summary = script_module.summarize_inference_metrics(
            [
                {"inference_metrics": {"input_tokens": 10, "output_tokens": 4, "total_tokens": 14, "latency_ms": 120}},
                {"inference_metrics": {"input_tokens": 20, "output_tokens": 6, "total_tokens": 26, "latency_ms": 180}},
            ]
        )

        self.assertEqual(summary["question_count"], 2)
        self.assertEqual(summary["sum_input_tokens"], 30.0)
        self.assertEqual(summary["sum_output_tokens"], 10.0)
        self.assertEqual(summary["sum_total_tokens"], 40.0)
        self.assertEqual(summary["sum_latency_ms"], 300.0)
        self.assertEqual(summary["avg_input_tokens"], 15.0)
        self.assertEqual(summary["avg_output_tokens"], 5.0)
        self.assertEqual(summary["avg_total_tokens"], 20.0)
        self.assertEqual(summary["avg_latency_ms"], 150.0)


if __name__ == "__main__":
    unittest.main()
