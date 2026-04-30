import importlib.util
import io
import json
import sys
import tempfile
import types
import unittest
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path
from unittest.mock import Mock, patch

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


class _OperationalError(Exception):
    pass


class _InterfaceError(Exception):
    pass


class _DatabaseError(Exception):
    pass


class _QueryCanceled(_DatabaseError):
    pass


psycopg2_stub = types.ModuleType("psycopg2")
psycopg2_stub.OperationalError = _OperationalError
psycopg2_stub.InterfaceError = _InterfaceError
psycopg2_stub.DatabaseError = _DatabaseError
psycopg2_stub.errors = types.SimpleNamespace(QueryCanceled=_QueryCanceled)
sys.modules["psycopg2"] = psycopg2_stub

evaluator_module = _load_module(
    "src.evaluation.evaluator",
    ROOT / "src" / "evaluation" / "evaluator.py",
)


class EvaluatorRecoveryTests(unittest.TestCase):
    def _build_evaluator(self):
        return evaluator_module.Evaluator(
            db_config={
                "host": "localhost",
                "port": 5432,
                "database": "db",
                "user": "user",
                "password": "pw",
            },
            eval_config={
                "evaluation": {
                    "timeout": 20,
                    "connect_timeout": 5,
                    "retry_backoff_sec": 0.0,
                    "max_retry_backoff_sec": 0.0,
                    "network_recovery_timeout": 30,
                    "retry_forever_on_connection_error": True,
                }
            },
        )

    @staticmethod
    def _ok_exec(sql: str, rows, stage: str) -> dict:
        result = set(rows)
        return {
            "stage": stage,
            "sql": sql,
            "status": "ok",
            "result": result,
            "result_count": len(result),
            "error": None,
            "error_type": None,
        }

    @staticmethod
    def _error_exec(sql: str, status: str, error: str, error_type: str, stage: str) -> dict:
        return {
            "stage": stage,
            "sql": sql,
            "status": status,
            "result": None,
            "result_count": None,
            "error": error,
            "error_type": error_type,
        }

    def test_connect_with_retry_retries_on_connection_failure_then_succeeds(self):
        evaluator = self._build_evaluator()
        conn = object()

        with patch.object(
            evaluator_module.psycopg2,
            "connect",
            side_effect=[
                evaluator_module.psycopg2.OperationalError("could not connect to server"),
                conn,
            ],
            create=True,
        ) as connect_mock, patch.object(
            evaluator_module.time,
            "sleep",
            return_value=None,
        ) as sleep_mock:
            result = evaluator._connect_with_retry(sample_label="id=1")

        self.assertIs(result, conn)
        self.assertEqual(connect_mock.call_count, 2)
        self.assertEqual(sleep_mock.call_count, 1)

    def test_pred_timeout_gold_ok_is_wrong(self):
        evaluator = self._build_evaluator()
        with patch.object(
            evaluator,
            "_execute_sql",
            side_effect=[
                self._error_exec(
                    "SELECT pg_sleep(30);",
                    "timeout",
                    "canceling statement due to statement timeout",
                    "QueryCanceled",
                    "预测SQL",
                ),
                self._ok_exec("SELECT 1;", [("gold",)], "标准SQL"),
            ],
        ):
            result = evaluator._execution_accuracy(
                predicted_sql="SELECT pg_sleep(30);",
                gold_sql="SELECT 1;",
                sample_label="id=40",
            )

        self.assertEqual(result["error_type"], "pred_timeout_gold_ok")
        self.assertEqual(result["judgement_status"], "wrong")
        self.assertFalse(result["is_indeterminate"])

    def test_pred_ok_gold_timeout_is_indeterminate(self):
        evaluator = self._build_evaluator()
        with patch.object(
            evaluator,
            "_execute_sql",
            side_effect=[
                self._ok_exec("SELECT 1;", [("pred",)], "预测SQL"),
                self._error_exec(
                    "SELECT slow_gold();",
                    "timeout",
                    "canceling statement due to statement timeout",
                    "QueryCanceled",
                    "标准SQL",
                ),
            ],
        ):
            result = evaluator._execution_accuracy(
                predicted_sql="SELECT 1;",
                gold_sql="SELECT slow_gold();",
                sample_label="id=41",
            )

        self.assertEqual(result["error_type"], "pred_ok_gold_timeout")
        self.assertEqual(result["judgement_status"], "indeterminate")
        self.assertTrue(result["is_indeterminate"])

    def test_both_timeout_is_indeterminate(self):
        evaluator = self._build_evaluator()
        with patch.object(
            evaluator,
            "_execute_sql",
            side_effect=[
                self._error_exec(
                    "SELECT pg_sleep(30);",
                    "timeout",
                    "canceling statement due to statement timeout",
                    "QueryCanceled",
                    "预测SQL",
                ),
                self._error_exec(
                    "SELECT slow_gold();",
                    "timeout",
                    "canceling statement due to statement timeout",
                    "QueryCanceled",
                    "标准SQL",
                ),
            ],
        ):
            result = evaluator._execution_accuracy(
                predicted_sql="SELECT pg_sleep(30);",
                gold_sql="SELECT slow_gold();",
                sample_label="id=42",
            )

        self.assertEqual(result["error_type"], "both_timeout")
        self.assertEqual(result["judgement_status"], "indeterminate")
        self.assertTrue(result["is_indeterminate"])

    def test_connection_error_is_indeterminate(self):
        evaluator = self._build_evaluator()
        with patch.object(
            evaluator,
            "_execute_sql",
            return_value=self._error_exec(
                "SELECT 1;",
                "connection_error",
                "could not connect to server",
                "OperationalError",
                "预测SQL",
            ),
        ):
            result = evaluator._execution_accuracy(
                predicted_sql="SELECT 1;",
                gold_sql="SELECT 1;",
                sample_label="id=43",
            )

        self.assertEqual(result["error_type"], "connection_error")
        self.assertEqual(result["judgement_status"], "indeterminate")
        self.assertTrue(result["is_indeterminate"])

    def test_falls_back_to_gold_candidates_when_primary_gold_fails(self):
        evaluator = self._build_evaluator()
        with patch.object(
            evaluator,
            "_execute_sql",
            side_effect=[
                self._ok_exec("SELECT 1;", [("matched",)], "预测SQL"),
                self._error_exec(
                    "SELECT broken();",
                    "execution_error",
                    "primary gold failed",
                    "DatabaseError",
                    "标准SQL",
                ),
                self._ok_exec("SELECT 1;", [("matched",)], "标准SQL候选"),
            ],
        ):
            result = evaluator._execution_accuracy(
                predicted_sql="SELECT 1;",
                gold_sql="SELECT broken();",
                gold_sql_candidates=["SELECT 1;"],
                sample_label="id=41",
            )

        self.assertEqual(result["correct"], 1)
        self.assertEqual(result["matched_gold_index"], 1)
        self.assertIn("gold_execution_errors", result)
        self.assertEqual(result["gold_execution_errors"][0]["stage"], "标准SQL")
        self.assertEqual(result["judgement_status"], "correct")

    def test_statistics_track_judged_accuracy_and_indeterminate_breakdown(self):
        evaluator = self._build_evaluator()
        with patch.object(
            evaluator,
            "_execution_accuracy",
            side_effect=[
                {
                    "correct": 1,
                    "error_type": None,
                    "error_message": None,
                    "pred_result_count": 1,
                    "gold_result_count": 1,
                    "execution_error": None,
                },
                {
                    "correct": 0,
                    "error_type": "pred_timeout_gold_ok",
                    "error_message": "预测SQL执行超时，但标准SQL可正常执行",
                    "pred_result_count": None,
                    "gold_result_count": 1,
                    "execution_error": {
                        "sql": "select pg_sleep(30);",
                        "error": "canceling statement due to statement timeout",
                        "error_type": "QueryCanceled",
                        "status": "timeout",
                        "stage": "预测SQL",
                    },
                },
            ],
        ):
            result = evaluator.evaluate(
                predictions=[
                    {"predicted_sql": "select 1", "gold_sql": "select 1", "metadata": {"level": 1}},
                    {"predicted_sql": "select pg_sleep(30)", "gold_sql": "select 1", "metadata": {"level": 1}},
                ],
                dataset_info={"name": "spatial_qa", "grouping_fields": ["level"]},
                model_name="m",
                config_type="base",
            )

        overall = result["statistics"]["overall"]
        self.assertEqual(overall["total"], 2)
        self.assertEqual(overall["correct"], 1)
        self.assertAlmostEqual(overall["accuracy"], 0.5)
        self.assertEqual(overall["judged_total"], 2)
        self.assertAlmostEqual(overall["judged_accuracy"], 0.5)
        self.assertEqual(overall["indeterminate_total"], 0)
        self.assertEqual(overall["error_breakdown"]["pred_timeout_gold_ok"], 1)

    def test_ignores_legacy_trusted_config_and_does_not_emit_trusted_fields(self):
        report_path = ROOT / "test" / "tmp_consistency_report.json"
        report_path.write_text(
            json.dumps(
                {
                    "details": [
                        {"split": "dataset1_ada", "source_id": "ada01", "status": "exact_match"},
                        {"split": "dataset1_ada", "source_id": "ada02", "status": "semantic_mismatch", "classification": "semantic_mismatch"},
                    ]
                },
                ensure_ascii=False,
            ),
            encoding="utf-8",
        )
        try:
            evaluator = evaluator_module.Evaluator(
                db_config={
                    "host": "localhost",
                    "port": 5432,
                    "database": "db",
                    "user": "user",
                    "password": "pw",
                },
                eval_config={
                    "evaluation": {
                        "timeout": 20,
                        "consistency_report_path": str(report_path),
                    }
                },
            )
            with patch.object(
                evaluator,
                "_execution_accuracy",
                side_effect=[
                    {
                        "correct": 1,
                        "error_type": None,
                        "error_message": None,
                        "pred_result_count": 1,
                        "gold_result_count": 1,
                        "execution_error": None,
                    },
                    {
                        "correct": 0,
                        "error_type": "result_mismatch",
                        "error_message": "mismatch",
                        "pred_result_count": 1,
                        "gold_result_count": 1,
                        "execution_error": None,
                    },
                ],
            ):
                result = evaluator.evaluate(
                    predictions=[
                        {"predicted_sql": "select 1", "gold_sql": "select 1", "metadata": {"split": "dataset1_ada", "source_id": "ada01"}},
                        {"predicted_sql": "select 2", "gold_sql": "select 1", "metadata": {"split": "dataset1_ada", "source_id": "ada02"}},
                    ],
                    dataset_info={"name": "spatialsql_pg", "grouping_fields": ["split"]},
                    model_name="m",
                    config_type="base",
                )
            self.assertNotIn("is_trusted_sample", result["details"][0])
            self.assertNotIn("consistency_status", result["details"][0])
            self.assertEqual(result["statistics"]["all_samples"]["overall"]["total"], 2)
            self.assertNotIn("trusted_samples", result["statistics"])
        finally:
            if report_path.exists():
                report_path.unlink()

    def test_does_not_emit_trusted_samples_in_summary(self):
        evaluator = self._build_evaluator()
        with patch.object(
            evaluator,
            "_execution_accuracy",
            return_value={
                "correct": 1,
                "error_type": None,
                "error_message": None,
                "pred_result_count": 1,
                "gold_result_count": 1,
                "execution_error": None,
            },
        ):
            result = evaluator.evaluate(
                predictions=[
                    {
                        "predicted_sql": "select 1",
                        "gold_sql": "select 1",
                        "metadata": {"level": 1},
                    }
                ],
                dataset_info={"name": "spatial_qa", "grouping_fields": ["level"]},
                model_name="m",
                config_type="base",
            )

        self.assertNotIn("trusted_samples", result["statistics"])

        output = io.StringIO()
        with patch.object(sys, "stdout", output):
            evaluator._print_summary(result["statistics"], {"name": "spatial_qa", "grouping_fields": ["level"]})
        self.assertNotIn("可信样本准确率", output.getvalue())

    def test_save_evaluation_serializes_date_datetime_and_decimal(self):
        evaluator = self._build_evaluator()
        eval_result = {
            "model": "m",
            "details": [
                {
                    "sample_date": date(2024, 1, 2),
                    "sample_datetime": datetime(2024, 1, 2, 3, 4, 5),
                    "score": Decimal("1.25"),
                }
            ],
        }

        with tempfile.TemporaryDirectory() as tmpdir:
            evaluator.save_evaluation(eval_result, tmpdir)
            payload = json.loads((Path(tmpdir) / "evaluation.json").read_text(encoding="utf-8"))

        self.assertEqual(payload["details"][0]["sample_date"], "2024-01-02")
        self.assertEqual(payload["details"][0]["sample_datetime"], "2024-01-02T03:04:05")
        self.assertEqual(payload["details"][0]["score"], 1.25)


if __name__ == "__main__":
    unittest.main()
