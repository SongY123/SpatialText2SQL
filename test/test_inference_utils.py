import importlib.util
import subprocess
import sys
import time
import types
import unittest
from unittest.mock import Mock, patch
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
_ensure_package("src.inference.loaders", ROOT / "src" / "inference" / "loaders")
_ensure_package("src.sql", ROOT / "src" / "sql")

_load_module("src.inference.base", ROOT / "src" / "inference" / "base.py")
_load_module("src.sql.sql_dialect_adapter", ROOT / "src" / "sql" / "sql_dialect_adapter.py")
sql_utils = _load_module("src.inference.sql_utils", ROOT / "src" / "inference" / "sql_utils.py")
_load_module(
    "src.inference.vllm_subprocess_runner",
    ROOT / "src" / "inference" / "vllm_subprocess_runner.py",
)
vllm_loader = _load_module(
    "src.inference.loaders.vllm_openai_loader",
    ROOT / "src" / "inference" / "loaders" / "vllm_openai_loader.py",
)
_load_module("src.inference.loaders.qwen_model_loader", ROOT / "src" / "inference" / "loaders" / "qwen_model_loader.py")
model_inference_module = _load_module(
    "src.inference.model_inference",
    ROOT / "src" / "inference" / "model_inference.py",
)


class ExtractSqlFromTextTests(unittest.TestCase):
    def test_removes_think_block_before_extracting_sql(self):
        text = "<think>先分析一下</think>\nSELECT * FROM poi WHERE fclass = 'bench';"
        self.assertEqual(
            sql_utils.extract_sql_from_text(text),
            "SELECT * FROM poi WHERE fclass = 'bench';",
        )

    def test_extracts_sql_from_answer_sql_zone(self):
        text = (
            "这里是分析。\n"
            "<answer_sql>\n"
            "SELECT name FROM roads WHERE maxspeed > 50;\n"
            "</answer_sql>"
        )
        self.assertEqual(
            sql_utils.extract_sql_from_text(text),
            "SELECT name FROM roads WHERE maxspeed > 50;",
        )

    def test_extracts_sql_from_final_sql_marker(self):
        text = (
            "我先思考一下。\n"
            "Final SQL:\n"
            "SELECT ST_Length(geom::geography) AS length_m FROM roads WHERE gid = 360036;"
        )
        self.assertEqual(
            sql_utils.extract_sql_from_text(text),
            "SELECT ST_Length(geom::geography) AS length_m FROM roads WHERE gid = 360036;",
        )

    def test_answer_zone_without_sql_returns_empty(self):
        text = (
            "<answer_sql>\n"
            "这里不是真正的SQL，只是解释。\n"
            "</answer_sql>\n"
            "SELECT name FROM roads;"
        )
        self.assertEqual(sql_utils.extract_sql_from_text(text), "")

    def test_extracts_sql_after_closing_think_tag(self):
        text = (
            "先分析题意，下面都是思考过程。\n"
            "</think>\n\n"
            "SELECT osm_id, name FROM poi WHERE fclass = 'bench';"
        )
        self.assertEqual(
            sql_utils.extract_sql_from_text(text),
            "SELECT osm_id, name FROM poi WHERE fclass = 'bench';",
        )

    def test_extracts_answer_zone_after_closing_think_tag(self):
        text = (
            "长篇思考内容……\n"
            "</think>\n\n"
            "<answer_sql>\n"
            "SELECT * FROM poi WHERE fclass = 'bench';\n"
            "</answer_sql>"
        )
        self.assertEqual(
            sql_utils.extract_sql_from_text(text),
            "SELECT * FROM poi WHERE fclass = 'bench';",
        )

    def test_post_thinking_tail_without_sql_returns_empty(self):
        text = (
            "这里是模型思考。\n"
            "</think>\n\n"
            "最终答案是长凳相关记录。"
        )
        self.assertEqual(sql_utils.extract_sql_from_text(text), "")

    def test_rejects_incomplete_with_fragment(self):
        self.assertEqual(sql_utils.extract_sql_from_text("Within;"), "")

    def test_rejects_select_star_without_from_clause(self):
        self.assertEqual(sql_utils.extract_sql_from_text("SELECT *;"), "")

    def test_normalizes_code_fence_output(self):
        text = "```sql\nSELECT name FROM roads WHERE maxspeed > 50;\n```"
        self.assertEqual(
            sql_utils.extract_sql_from_text(text),
            "SELECT name FROM roads WHERE maxspeed > 50;",
        )

    def test_removes_thinking_tag_variant_before_extracting_sql(self):
        text = "<thinking>先分析一下</thinking>\nSELECT name FROM poi WHERE fclass = 'bench';"
        self.assertEqual(
            sql_utils.extract_sql_from_text(text),
            "SELECT name FROM poi WHERE fclass = 'bench';",
        )

    def test_strips_fullwidth_period_after_sql(self):
        text = "SELECT * FROM ghcn WHERE state = 'SD'。;"
        self.assertEqual(
            sql_utils.extract_sql_from_text(text),
            "SELECT * FROM ghcn WHERE state = 'SD';",
        )

    def test_preserves_chinese_literals_inside_sql(self):
        text = "SELECT name FROM lakes WHERE name = '太湖';"
        self.assertEqual(
            sql_utils.extract_sql_from_text(text),
            "SELECT name FROM lakes WHERE name = '太湖';",
        )

    def test_truncates_chinese_explanation_but_keeps_sql_literal(self):
        text = "SELECT name FROM lakes WHERE name = '洞庭湖'\n解释: 返回湖泊名称"
        self.assertEqual(
            sql_utils.extract_sql_from_text(text),
            "SELECT name FROM lakes WHERE name = '洞庭湖';",
        )


class SpatialSqlPredictionNormalizationTests(unittest.TestCase):
    def test_normalizes_split_prefix_and_wraps_shape_columns(self):
        sql = (
            "SELECT DISTINCT d.name FROM dataset1_edu_lakes l "
            "JOIN provinces d ON ST_Intersects(l.Shape, d.shape) "
            "WHERE l.name = 'Dongting Lake';"
        )
        normalized = sql_utils.normalize_spatialsql_predicted_sql(
            sql,
            {"split": "dataset1_ada"},
        )

        self.assertIn("FROM dataset1_ada_lakes l", normalized)
        self.assertIn("JOIN dataset1_ada_provinces d", normalized)
        self.assertIn(
            "ST_Intersects(l.shape, d.shape)",
            normalized,
        )
        self.assertNotIn("ST_GeomFromWKB", normalized)

    def test_keeps_valid_postgis_sql_unchanged(self):
        sql = (
            "SELECT DISTINCT p.name FROM dataset1_ada_provinces p "
            "JOIN dataset1_ada_lakes l ON ST_Intersects(p.shape, l.shape) "
            "WHERE l.name = '洞庭湖';"
        )
        normalized = sql_utils.normalize_spatialsql_predicted_sql(
            sql,
            {"split": "dataset1_ada"},
        )
        self.assertEqual(normalized, sql)


class FloodSQLPredictionNormalizationTests(unittest.TestCase):
    def test_normalizes_duckdb_specific_constructs(self):
        sql = (
            "SELECT STRFTIME('%Y', dateOfLoss) AS year, "
            "AVG(CAST(amountPaidOnBuildingClaim AS DOUBLE)) AS avg_paid "
            "FROM claims "
            "WHERE ST_Contains(floodplain.geometry, ST_Point(claims.LON, claims.LAT));"
        )
        normalized = sql_utils.normalize_floodsql_predicted_sql(sql)

        self.assertIn("TO_CHAR(dateOfLoss, 'YYYY') AS year", normalized)
        self.assertIn("CAST(amountPaidOnBuildingClaim AS DOUBLE PRECISION)", normalized)
        self.assertIn("ST_SetSRID(ST_Point(claims.LON, claims.LAT), 4326)", normalized)


class VllmRequestBuilderTests(unittest.TestCase):
    def test_build_request_kwargs_aligns_greedy_qwen3_defaults(self):
        loader = vllm_loader.VllmOpenAILoader(
            {
                "api_base": "http://example.com/v1",
                "model": "/data/llm/qwen/Qwen3-8B",
                "generation_config": {},
            }
        )

        request_kwargs = loader._build_request_kwargs(
            "SQL:",
            {
                "max_new_tokens": 1024,
                "do_sample": False,
                "repetition_penalty": 1.1,
                "stop": ["```"],
            },
        )

        self.assertEqual(request_kwargs["temperature"], 0.0)
        self.assertEqual(request_kwargs["top_p"], 1.0)
        self.assertEqual(request_kwargs["max_tokens"], 1024)
        self.assertEqual(request_kwargs["stop"], ["```"])
        self.assertEqual(
            request_kwargs["extra_body"]["chat_template_kwargs"]["enable_thinking"],
            False,
        )
        self.assertEqual(request_kwargs["extra_body"]["repetition_penalty"], 1.1)

    def test_build_request_kwargs_respects_sampling_override(self):
        loader = vllm_loader.VllmOpenAILoader(
            {
                "api_base": "http://example.com/v1",
                "model": "/data/llm/qwen/Qwen3-8B",
                "generation_config": {},
            }
        )

        request_kwargs = loader._build_request_kwargs(
            "SQL:",
            {
                "do_sample": True,
                "temperature": 0.3,
                "top_p": 0.8,
                "enable_thinking": True,
            },
        )

        self.assertEqual(request_kwargs["temperature"], 0.3)
        self.assertEqual(request_kwargs["top_p"], 0.8)
        self.assertEqual(
            request_kwargs["extra_body"]["chat_template_kwargs"]["enable_thinking"],
            True,
        )

    def test_build_request_kwargs_forces_enable_thinking_for_qwen3_thinking_model(self):
        loader = vllm_loader.VllmOpenAILoader(
            {
                "api_base": "http://example.com/v1",
                "model": "/data/llm/qwen/Qwen3-235B-A22B-Thinking-2507-FP8",
                "logical_model_name": "qwen3-235b-a22b-thinking",
                "generation_config": {},
            }
        )

        request_kwargs = loader._build_request_kwargs(
            "SQL:",
            {
                "do_sample": False,
                "enable_thinking": False,
            },
        )

        self.assertEqual(
            request_kwargs["extra_body"]["chat_template_kwargs"]["enable_thinking"],
            True,
        )

    def test_prepare_prompt_for_model_keeps_prompt_unchanged(self):
        loader = vllm_loader.VllmOpenAILoader(
            {
                "api_base": "http://example.com/v1",
                "model": "/data/llm/qwen/Qwen3-235B-A22B-Thinking-2507-FP8",
                "logical_model_name": "qwen3-235b-a22b-thinking",
                "generation_config": {},
            }
        )

        prompt = loader._prepare_prompt_for_model("原始 prompt")

        self.assertEqual(prompt, "原始 prompt")

    def test_extract_final_answer_prefers_structured_content_when_reasoning_present(self):
        loader = vllm_loader.VllmOpenAILoader(
            {
                "api_base": "http://example.com/v1",
                "model": "/data/llm/qwen/Qwen3-235B-A22B-Thinking-2507-FP8",
                "logical_model_name": "qwen3-235b-a22b-thinking",
                "generation_config": {},
            }
        )
        sql = loader._extract_final_answer_from_parts(
            {
                "reasoning": "分析过程",
                "content": "SELECT * FROM poi WHERE fclass = 'bench'",
            }
        )

        self.assertEqual(sql, "SELECT * FROM poi WHERE fclass = 'bench'")

    def test_extract_final_answer_splits_think_tags_from_content(self):
        loader = vllm_loader.VllmOpenAILoader(
            {
                "api_base": "http://example.com/v1",
                "model": "/data/llm/qwen/Qwen3-235B-A22B-Thinking-2507-FP8",
                "logical_model_name": "qwen3-235b-a22b-thinking",
                "generation_config": {},
            }
        )

        sql = loader._extract_final_answer_from_parts(
            {
                "reasoning": "",
                "content": (
                    "这里是长篇思考内容。\n"
                    "</think>\n\n"
                    "SELECT * FROM poi WHERE fclass = 'bench'"
                ),
            }
        )

        self.assertEqual(sql, "SELECT * FROM poi WHERE fclass = 'bench'")

    def test_split_think_and_answer_supports_redacted_thinking_tags(self):
        split = vllm_loader.VllmOpenAILoader._split_think_and_answer(
            (
                "<redacted_thinking>\n"
                "这部分是推理\n"
                "</redacted_thinking>\n"
                "SELECT gid FROM roads"
            )
        )

        self.assertEqual(split["think"], "这部分是推理")
        self.assertEqual(split["answer"], "SELECT gid FROM roads")
        self.assertFalse(split["think_incomplete"])

    def test_split_think_and_answer_marks_incomplete_open_tag(self):
        split = vllm_loader.VllmOpenAILoader._split_think_and_answer(
            "SELECT 1\n<think>未完成的推理"
        )

        self.assertEqual(split["think"], "未完成的推理")
        self.assertEqual(split["answer"], "SELECT 1")
        self.assertTrue(split["think_incomplete"])

    def test_extract_final_answer_returns_whole_content_without_think_tags(self):
        loader = vllm_loader.VllmOpenAILoader(
            {
                "api_base": "http://example.com/v1",
                "model": "/data/llm/qwen/Qwen3-8B",
                "logical_model_name": "qwen3-8b",
                "generation_config": {},
            }
        )

        sql = loader._extract_final_answer_from_parts(
            {
                "reasoning": "",
                "content": "SELECT name FROM roads WHERE maxspeed > 50",
            }
        )

        self.assertEqual(sql, "SELECT name FROM roads WHERE maxspeed > 50")

    def test_non_thinking_vllm_model_keeps_existing_sql_extraction_path(self):
        loader = vllm_loader.VllmOpenAILoader(
            {
                "api_base": "http://example.com/v1",
                "model": "/data/llm/qwen/Qwen3-8B",
                "logical_model_name": "qwen3-8b",
                "generation_config": {},
            }
        )
        loader.client = Mock()
        loader._create_chat_completion_with_retry = Mock(
            return_value={"reasoning": "", "content": "SELECT name FROM roads WHERE maxspeed > 50"}
        )

        sql = loader.generate_sql("dummy prompt")

        self.assertEqual(sql, "SELECT name FROM roads WHERE maxspeed > 50")

    def test_retries_retryable_errors_before_success(self):
        loader = vllm_loader.VllmOpenAILoader(
            {
                "api_base": "http://example.com/v1",
                "model": "/data/llm/qwen/Qwen3-8B",
                "generation_config": {},
                "max_retries": 2,
                "retry_backoff_sec": 0.0,
            }
        )
        loader.client = Mock()
        loader._create_client = Mock(return_value=loader.client)
        loader._invoke_completion_isolated = Mock(
            side_effect=[
                RuntimeError("Error code: 502"),
                RuntimeError("Bad gateway"),
                "ok",
            ]
        )

        with patch.object(vllm_loader.time, "sleep", return_value=None):
            result = loader._create_chat_completion_with_retry({"model": "dummy"})

        self.assertEqual(result, "ok")
        self.assertEqual(loader._invoke_completion_isolated.call_count, 3)

    def test_rebuilds_client_after_network_error(self):
        loader = vllm_loader.VllmOpenAILoader(
            {
                "api_base": "http://example.com/v1",
                "model": "/data/llm/qwen/Qwen3-8B",
                "generation_config": {},
                "retry_backoff_sec": 0.0,
                "max_retry_backoff_sec": 0.0,
                "retry_forever_on_network_error": True,
            }
        )

        loader.client = Mock()
        loader._create_client = Mock(return_value=loader.client)
        loader._invoke_completion_isolated = Mock(
            side_effect=[
                RuntimeError("connection refused"),
                "ok",
            ]
        )

        with patch.object(vllm_loader.time, "sleep", return_value=None):
            result = loader._create_chat_completion_with_retry({"model": "dummy"})

        self.assertEqual(result, "ok")
        self.assertEqual(loader._create_client.call_count, 1)

    def test_stops_retrying_after_network_recovery_timeout(self):
        loader = vllm_loader.VllmOpenAILoader(
            {
                "api_base": "http://example.com/v1",
                "model": "/data/llm/qwen/Qwen3-8B",
                "generation_config": {},
                "network_recovery_timeout": 1.0,
                "retry_forever_on_network_error": True,
            }
        )

        stop_reason = loader._get_retry_stop_reason(
            recovery_start=time.monotonic() - 2.0,
            attempt=1,
        )

        self.assertEqual(stop_reason, "network_recovery_timeout")

    def test_wall_timeout_returns_without_executor_deadlock(self):
        """subprocess.run(timeout=) 超时时应抛跳过，不得无限阻塞。"""
        loader = vllm_loader.VllmOpenAILoader(
            {
                "api_base": "http://example.com/v1",
                "model": "/data/llm/qwen/Qwen3-8B",
                "generation_config": {},
            }
        )
        loader.request_wall_timeout = 0.2
        loader.client = Mock()
        loader._rebuild_client = Mock()

        def _boom(*_a, **_k):
            raise subprocess.TimeoutExpired(cmd=["py", "runner"], timeout=0.2)

        t0 = time.monotonic()
        with patch.object(vllm_loader.subprocess, "run", side_effect=_boom):
            with self.assertRaises(vllm_loader.VllmSampleSkippedError) as ctx:
                loader._invoke_completion_isolated({"model": "dummy"})
        self.assertLess(time.monotonic() - t0, 2.0)
        self.assertEqual(ctx.exception.reason_code, "wall_clock_timeout")
        self.assertIn("墙钟超时", str(ctx.exception))
        loader._rebuild_client.assert_called_once()

    def test_raises_skipped_error_after_sample_skip_timeout(self):
        loader = vllm_loader.VllmOpenAILoader(
            {
                "api_base": "http://example.com/v1",
                "model": "/data/llm/qwen/Qwen3-8B",
                "generation_config": {},
                "sample_skip_timeout": 0.0,
                "retry_backoff_sec": 0.0,
                "max_retry_backoff_sec": 0.0,
                "retry_forever_on_network_error": True,
            }
        )
        loader.client = Mock()
        loader._create_client = Mock(return_value=loader.client)
        loader._invoke_completion_isolated = Mock(
            side_effect=RuntimeError("connection refused")
        )

        with self.assertRaises(vllm_loader.VllmSampleSkippedError) as ctx:
            loader._create_chat_completion_with_retry({"model": "dummy"})

        self.assertEqual(ctx.exception.reason_code, "network_recovery_timeout")
        self.assertIn("已跳过当前样本", str(ctx.exception))
        self.assertEqual(ctx.exception.details["last_error_type"], "RuntimeError")


class ModelInferenceFailureRecordingTests(unittest.TestCase):
    def test_build_failed_result_item_preserves_skip_reason_details(self):
        inference = model_inference_module.ModelInference.__new__(model_inference_module.ModelInference)
        exc = vllm_loader.VllmSampleSkippedError(
            "vLLM 网络恢复等待超过 180.0s，已跳过当前样本。",
            reason_code="network_recovery_timeout",
            attempts=4,
            elapsed_sec=181.5,
            last_error=RuntimeError("connection refused"),
        )
        data_item = {
            "id": 34,
            "question": "What is the total length of all Level 1 rivers within each province?",
            "gold_sql": "SELECT 1",
            "gold_sql_candidates": ["SELECT 1"],
            "metadata": {"split": "dataset1_ada"},
        }

        result_item = inference._build_failed_result_item(exc, data_item)

        self.assertTrue(result_item["skipped"])
        self.assertEqual(result_item["skip_reason_code"], "network_recovery_timeout")
        self.assertEqual(result_item["skip_details"]["attempts"], 4)
        self.assertEqual(result_item["skip_details"]["last_error_type"], "RuntimeError")

    def test_normalize_prediction_default_disabled(self):
        inference = model_inference_module.ModelInference.__new__(model_inference_module.ModelInference)
        inference.enable_spatialsql_prediction_normalization = False
        normalized = inference._normalize_prediction(
            "SELECT name FROM cities;",
            {
                "dataset": "spatialsql_pg",
                "metadata": {"split": "dataset1_ada"},
            },
        )

        self.assertEqual(normalized, "SELECT name FROM cities;")

    def test_normalize_prediction_applies_spatialsql_rules_when_enabled(self):
        inference = model_inference_module.ModelInference.__new__(model_inference_module.ModelInference)
        inference.enable_spatialsql_prediction_normalization = True
        normalized = inference._normalize_prediction(
            "SELECT name FROM cities;",
            {
                "dataset": "spatialsql_pg",
                "metadata": {"split": "dataset1_ada"},
            },
        )

        self.assertEqual(normalized, "SELECT name FROM dataset1_ada_cities;")


if __name__ == "__main__":
    unittest.main()
