import importlib.util
import json
import subprocess
import sys
import tempfile
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

    def test_rejects_explanatory_sentence_starting_with_with(self):
        text = (
            "with the highest non-null percentage of individuals with zero vulnerability "
            "components. From the schema, I can see the svi table has related columns."
        )
        self.assertEqual(sql_utils.extract_sql_from_text(text), "")

    def test_extracts_valid_with_cte_query(self):
        text = (
            "WITH ranked AS (\n"
            "  SELECT name FROM county\n"
            ")\n"
            "SELECT name FROM ranked;"
        )
        self.assertEqual(
            sql_utils.extract_sql_from_text(text),
            "WITH ranked AS ( SELECT name FROM county ) SELECT name FROM ranked;",
        )

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

    def test_extracts_sql_from_sql_code_fence_without_terminal_semicolon(self):
        text = "```sql\nSELECT name FROM roads WHERE maxspeed > 50\n```"
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

    def test_build_request_kwargs_keeps_enable_thinking_opt_in_for_qwen3_thinking_model(self):
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
            False,
        )
        self.assertNotIn("max_tokens", request_kwargs)

    def test_build_request_kwargs_keeps_explicit_max_tokens_for_qwen3_thinking_model(self):
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
                "max_new_tokens": 2048,
            },
        )

        self.assertEqual(request_kwargs["max_tokens"], 2048)

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

    def test_collect_stream_parts_preserves_think_tags_and_final_sql(self):
        event1 = types.SimpleNamespace(
            choices=[types.SimpleNamespace(delta=types.SimpleNamespace(content="<think>分析"))]
        )
        event2 = types.SimpleNamespace(
            choices=[types.SimpleNamespace(delta=types.SimpleNamespace(content="过程</think>\n\nSELECT"))]
        )
        event3 = types.SimpleNamespace(
            choices=[types.SimpleNamespace(delta=types.SimpleNamespace(content=" 1"))]
        )

        parts = vllm_loader.VllmOpenAILoader._collect_stream_parts([event1, event2, event3])

        self.assertEqual(parts["reasoning"], "")
        self.assertEqual(parts["content"], "<think>分析过程</think>\n\nSELECT 1")

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

    def test_thinking_vllm_model_uses_streaming_completion_path(self):
        loader = vllm_loader.VllmOpenAILoader(
            {
                "api_base": "http://example.com/v1",
                "model": "/data/llm/qwen/Qwen3-235B-A22B-Thinking-2507-FP8",
                "logical_model_name": "qwen3-235b-a22b-thinking",
                "generation_config": {},
                "request_wall_timeout": 1,
            }
        )
        loader.client = Mock()
        loader._invoke_streaming_completion = Mock(
            return_value={"reasoning": "", "content": "<think>分析</think>\nSELECT 1"}
        )
        loader._run_subprocess_request = Mock(return_value={"reasoning": "", "content": ""})

        parts = loader._invoke_completion_isolated({"model": "dummy", "messages": []})

        loader._invoke_streaming_completion.assert_called_once_with({"model": "dummy", "messages": []})
        loader._run_subprocess_request.assert_not_called()
        self.assertEqual(parts["content"], "<think>分析</think>\nSELECT 1")

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

    def test_generate_returns_usage_when_response_exposes_it(self):
        loader = vllm_loader.VllmOpenAILoader(
            {
                "api_base": "http://example.com/v1",
                "model": "/data/llm/qwen/Qwen3-8B",
                "logical_model_name": "qwen3-8b",
                "generation_config": {},
            }
        )
        response = types.SimpleNamespace(
            choices=[
                types.SimpleNamespace(
                    message=types.SimpleNamespace(
                        reasoning="",
                        content="SELECT gid FROM roads",
                    )
                )
            ],
            usage=types.SimpleNamespace(
                prompt_tokens=12,
                completion_tokens=6,
                total_tokens=18,
            ),
        )
        loader.client = Mock()
        loader.client.chat.completions.create = Mock(return_value=response)

        result = loader.generate("dummy prompt")

        self.assertEqual(result.sql, "SELECT gid FROM roads")
        self.assertEqual(result.raw_text, "SELECT gid FROM roads")
        self.assertEqual(result.usage["prompt_tokens"], 12)
        self.assertEqual(result.usage["completion_tokens"], 6)

    def test_collect_stream_parts_keeps_usage_from_final_chunk(self):
        event1 = types.SimpleNamespace(
            choices=[types.SimpleNamespace(delta=types.SimpleNamespace(content="<think>分析"))],
            usage=None,
        )
        event2 = types.SimpleNamespace(
            choices=[types.SimpleNamespace(delta=types.SimpleNamespace(content="过程</think>\n\nSELECT 1"))],
            usage=types.SimpleNamespace(prompt_tokens=20, completion_tokens=10, total_tokens=30),
        )

        parts = vllm_loader.VllmOpenAILoader._collect_stream_parts([event1, event2])

        self.assertEqual(parts["usage"]["prompt_tokens"], 20)
        self.assertEqual(parts["usage"]["total_tokens"], 30)

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

        result_item = inference._build_failed_result_item(
            exc,
            data_item,
            inference_metrics={"latency_ms": 181500.0, "status": "skipped"},
        )

        self.assertTrue(result_item["skipped"])
        self.assertEqual(result_item["skip_reason_code"], "network_recovery_timeout")
        self.assertEqual(result_item["skip_details"]["attempts"], 4)
        self.assertEqual(result_item["skip_details"]["last_error_type"], "RuntimeError")
        self.assertEqual(result_item["inference_metrics"]["status"], "skipped")

    def test_normalize_prediction_default_disabled(self):
        inference = model_inference_module.ModelInference.__new__(model_inference_module.ModelInference)
        inference.enable_spatialsql_prediction_normalization = False
        inference.enable_floodsql_prediction_normalization = False
        normalized = inference._normalize_prediction(
            "SELECT name FROM cities;",
            {
                "dataset": "spatialsql_pg",
                "metadata": {"split": "dataset1_ada"},
            },
        )

        self.assertEqual(normalized, "SELECT name FROM cities;")

    def test_normalize_prediction_strips_markdown_fence_globally(self):
        inference = model_inference_module.ModelInference.__new__(model_inference_module.ModelInference)
        inference.enable_spatialsql_prediction_normalization = False
        inference.enable_floodsql_prediction_normalization = False
        normalized = inference._normalize_prediction(
            "```sql\nSELECT name FROM cities;\n```",
            {
                "dataset": "spatial_qa",
                "metadata": {},
            },
        )

        self.assertEqual(normalized, "SELECT name FROM cities;")

    def test_normalize_prediction_clears_explanatory_non_sql_output(self):
        inference = model_inference_module.ModelInference.__new__(model_inference_module.ModelInference)
        inference.enable_spatialsql_prediction_normalization = False
        inference.enable_floodsql_prediction_normalization = False
        normalized = inference._normalize_prediction(
            (
                "with the highest non-null percentage of individuals with zero vulnerability "
                "components. From the schema, I can see the svi table has related columns."
            ),
            {
                "dataset": "floodsql_pg",
                "metadata": {"family": "double_table_key"},
            },
        )

        self.assertEqual(normalized, "")

    def test_normalize_prediction_applies_spatialsql_rules_when_enabled(self):
        inference = model_inference_module.ModelInference.__new__(model_inference_module.ModelInference)
        inference.enable_spatialsql_prediction_normalization = True
        inference.enable_floodsql_prediction_normalization = False
        normalized = inference._normalize_prediction(
            "SELECT name FROM cities;",
            {
                "dataset": "spatialsql_pg",
                "metadata": {"split": "dataset1_ada"},
            },
        )

        self.assertEqual(normalized, "SELECT name FROM dataset1_ada_cities;")

    def test_normalize_prediction_cleans_then_applies_floodsql_rules(self):
        inference = model_inference_module.ModelInference.__new__(model_inference_module.ModelInference)
        inference.enable_spatialsql_prediction_normalization = False
        inference.enable_floodsql_prediction_normalization = True
        normalized = inference._normalize_prediction(
            "```sql\nSELECT STRFTIME('%Y', dateOfLoss) AS year FROM claims;\n```",
            {
                "dataset": "floodsql_pg",
                "metadata": {"family": "single_table"},
            },
        )

        self.assertEqual(normalized, "SELECT TO_CHAR(dateOfLoss, 'YYYY') AS year FROM claims;")

    def test_build_inference_metrics_uses_tokenizer_fallback_when_usage_absent(self):
        inference = model_inference_module.ModelInference.__new__(model_inference_module.ModelInference)

        class _DummyLoader:
            @staticmethod
            def count_tokens(text):
                return len(text.split()) if text else 0

        metrics = inference._build_inference_metrics(
            model_loader=_DummyLoader(),
            prompt="SELECT name FROM roads",
            generation_result=model_inference_module.GenerationResult(
                sql="SELECT name FROM roads",
                raw_text="SELECT name FROM roads WHERE gid = 1",
            ),
            started_at_unix_ms=1000,
            finished_at_unix_ms=1125,
            latency_ms=125.0,
            status="success",
        )

        self.assertEqual(metrics["measurement_source"], "tokenizer_fallback")
        self.assertEqual(metrics["input_tokens"], 4)
        self.assertEqual(metrics["output_tokens"], 8)
        self.assertEqual(metrics["total_tokens"], 12)
        self.assertEqual(metrics["status"], "success")

    def test_run_inference_persists_metrics_for_success_and_failure(self):
        class DummyMetricsLoader:
            def __init__(self, _config):
                self.tokenizer = None

            def load_model(self, *args, **kwargs):
                return None

            def generate(self, prompt: str, **_gen_kwargs):
                if "bad" in prompt:
                    raise RuntimeError("boom")
                return model_inference_module.GenerationResult(
                    sql="SELECT 1;",
                    raw_text="SELECT 1;",
                )

            @staticmethod
            def count_tokens(text):
                return len(text.split()) if text else 0

            def unload(self):
                return None

        model_inference_module.ModelLoaderFactory.register_loader(
            "DummyMetricsLoader",
            DummyMetricsLoader,
        )

        with tempfile.TemporaryDirectory() as tmpdir:
            model_config_path = Path(tmpdir) / "model_config.yaml"
            eval_config_path = Path(tmpdir) / "eval_config.yaml"
            model_config_path.write_text(
                json.dumps(
                    {
                        "default_backend": "vllm",
                        "models": {
                            "dummy-model": {
                                "generation_config": {},
                                "backends": {
                                    "vllm": {
                                        "loader_class": "DummyMetricsLoader",
                                    }
                                },
                            }
                        },
                        "inference": {
                            "show_progress": False,
                            "save_interval": 100,
                        },
                    }
                ),
                encoding="utf-8",
            )
            eval_config_path.write_text(
                json.dumps(
                    {
                        "results": {
                            "predictions_dir": str(Path(tmpdir) / "predictions"),
                            "evaluations_dir": str(Path(tmpdir) / "evaluations"),
                        },
                        "prediction_postprocess": {
                            "enable_spatialsql_normalization": False,
                            "enable_floodsql_normalization": False,
                        },
                    }
                ),
                encoding="utf-8",
            )

            inference = model_inference_module.ModelInference(
                model_config_path=str(model_config_path),
                eval_config_path=str(eval_config_path),
            )
            results = inference.run_inference(
                model_name="dummy-model",
                config_type="base",
                prompts=["good prompt", "bad prompt"],
                data_items=[
                    {"id": 1, "question": "Q1", "gold_sql": "SELECT 1;", "metadata": {}},
                    {"id": 2, "question": "Q2", "gold_sql": "SELECT 2;", "metadata": {}},
                ],
                save_dir=str(Path(tmpdir) / "predictions"),
                backend="vllm",
            )

        self.assertEqual(results[0]["inference_metrics"]["status"], "success")
        self.assertEqual(results[0]["inference_metrics"]["measurement_source"], "tokenizer_fallback")
        self.assertNotIn("prompt", results[0])
        self.assertEqual(results[1]["inference_metrics"]["status"], "error")
        self.assertIn("latency_ms", results[1]["inference_metrics"])
        self.assertNotIn("prompt", results[1])
        self.assertEqual(results[1]["predicted_sql"], "")

    def test_run_inference_saves_rendered_prompt_in_separate_prompts_file(self):
        class DummyPromptPersistenceLoader:
            def __init__(self, _config):
                self.tokenizer = None

            def load_model(self, *args, **kwargs):
                return None

            def generate(self, prompt: str, **_gen_kwargs):
                return model_inference_module.GenerationResult(
                    sql=f"SELECT '{prompt}' AS prompt_text;",
                    raw_text=f"SELECT '{prompt}' AS prompt_text;",
                )

            @staticmethod
            def count_tokens(text):
                return len(text.split()) if text else 0

            def unload(self):
                return None

        model_inference_module.ModelLoaderFactory.register_loader(
            "DummyPromptPersistenceLoader",
            DummyPromptPersistenceLoader,
        )

        with tempfile.TemporaryDirectory() as tmpdir:
            predictions_dir = Path(tmpdir) / "predictions"
            model_config_path = Path(tmpdir) / "model_config.yaml"
            eval_config_path = Path(tmpdir) / "eval_config.yaml"
            model_config_path.write_text(
                json.dumps(
                    {
                        "default_backend": "vllm",
                        "models": {
                            "dummy-model": {
                                "generation_config": {},
                                "backends": {
                                    "vllm": {
                                        "loader_class": "DummyPromptPersistenceLoader",
                                    }
                                },
                            }
                        },
                        "inference": {
                            "show_progress": False,
                            "save_interval": 100,
                        },
                    }
                ),
                encoding="utf-8",
            )
            eval_config_path.write_text(
                json.dumps(
                    {
                        "results": {
                            "predictions_dir": str(predictions_dir),
                            "evaluations_dir": str(Path(tmpdir) / "evaluations"),
                        },
                        "prediction_postprocess": {
                            "enable_spatialsql_normalization": False,
                            "enable_floodsql_normalization": False,
                        },
                    }
                ),
                encoding="utf-8",
            )

            inference = model_inference_module.ModelInference(
                model_config_path=str(model_config_path),
                eval_config_path=str(eval_config_path),
            )
            inference.run_inference(
                model_name="dummy-model",
                config_type="base",
                prompts=["restored prompt"],
                data_items=[
                    {"id": 1, "question": "Q1", "gold_sql": "SELECT 1;", "metadata": {}},
                ],
                save_dir=str(predictions_dir),
                backend="vllm",
            )

            saved = json.loads((predictions_dir / "predictions.json").read_text(encoding="utf-8"))
            prompt_saved = json.loads((predictions_dir / "prompts.json").read_text(encoding="utf-8"))

        self.assertNotIn("prompt", saved[0])
        self.assertEqual(prompt_saved[0]["id"], 1)
        self.assertEqual(prompt_saved[0]["prompt"], "restored prompt")


if __name__ == "__main__":
    unittest.main()
