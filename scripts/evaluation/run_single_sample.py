#!/usr/bin/env python3
"""Run inference and optional evaluation for one or more preprocessed samples."""
from __future__ import annotations

import argparse
import json
import os
import sys
import time
from pathlib import Path
from typing import Any, Dict, List

import yaml


REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

os.chdir(REPO_ROOT)

from src.inference.model_inference import ModelInference, ModelLoaderFactory
from src.prompting.prompt_builder import PromptBuilder
from src.datasets.path_utils import (
    get_group_samples_file,
    get_preprocessed_output_dir,
    get_single_dataset_samples_file,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run one or more samples through the current inference path and print the raw/final outputs."
    )
    parser.add_argument("--dataset", required=True, help="Dataset name, e.g. spatial_qa or spatialsql_pg")
    parser.add_argument(
        "--group-value",
        default="",
        help="Grouped dataset value, e.g. 1 for spatial_qa level1 or dataset1_ada for spatialsql_pg",
    )
    parser.add_argument("--sample-id", default="", help="Sample id to run; overrides --sample-limit when set")
    parser.add_argument(
        "--sample-limit",
        type=int,
        default=1,
        help="Run the first N samples from the selected preprocessed file when --sample-id is not set",
    )
    parser.add_argument("--model", required=True, help="Logical model name from model_config.yaml")
    parser.add_argument("--backend", default="vllm", help="Inference backend name")
    parser.add_argument("--config", default="base", help="Prompt config type, e.g. base/rag/keyword/full")
    parser.add_argument(
        "--show-prompt",
        action="store_true",
        help="Print the prompt preview in the terminal output",
    )
    parser.add_argument(
        "--no-eval",
        action="store_true",
        help="Skip the single-sample execution-accuracy check",
    )
    parser.add_argument(
        "--preview-chars",
        type=int,
        default=4000,
        help="Maximum number of characters to print for long text fields",
    )
    return parser.parse_args()


def load_yaml(path: Path) -> Dict[str, Any]:
    with open(path, "r", encoding="utf-8") as handle:
        return yaml.safe_load(handle)


def clip(text: str, limit: int) -> str:
    text = (text or "").strip()
    if not text:
        return "<empty>"
    if len(text) <= limit:
        return text
    return f"{text[:limit]}\n... [truncated, total {len(text)} chars]"


def resolve_data_file(project_root: Path, dataset_cfg: Dict[str, Any], dataset_name: str, group_value: str) -> tuple[Path, str, str]:
    dataset_info = dataset_cfg["datasets"][dataset_name]
    preprocessing_cfg = dataset_cfg.get("preprocessing", {})
    dataset_dir = Path(get_preprocessed_output_dir(preprocessing_cfg, dataset_name))
    if not dataset_dir.is_absolute():
        dataset_dir = project_root / dataset_dir
    grouping = dataset_info.get("grouping", {})
    group_fields = grouping.get("fields", [])

    if not group_fields:
        data_file = Path(get_single_dataset_samples_file(str(dataset_dir)))
        return data_file, "", ""

    group_field = group_fields[0]
    if not group_value:
        values = grouping.get("values", {}).get(group_field, [])
        if values:
            group_value = str(values[0])
    if not group_value:
        raise ValueError(f"dataset {dataset_name} requires --group-value")

    data_file = Path(
        get_group_samples_file(
            dataset_name,
            str(dataset_dir),
            group_field,
            group_value,
        )
    )
    return data_file, group_field, group_value


def hydrate_schema_references(project_root: Path, items: list[Dict[str, Any]]) -> list[Dict[str, Any]]:
    hydrated = []
    for item in items:
        schema_file = item.get("schema_file")
        if schema_file and not item.get("schema"):
            schema_path = Path(schema_file)
            if not schema_path.is_absolute():
                schema_path = project_root / schema_path
            item["schema"] = schema_path.read_text(encoding="utf-8")
        hydrated.append(item)
    return hydrated


def find_sample(items: list[Dict[str, Any]], sample_id: str) -> Dict[str, Any]:
    if not items:
        raise ValueError("no preprocessed samples found")
    if not sample_id:
        return items[0]
    for item in items:
        if str(item.get("id")) == sample_id:
            return item
    preview_ids = [str(item.get("id")) for item in items[:10]]
    raise ValueError(f"sample id {sample_id} not found; first ids: {preview_ids}")


def select_samples(items: List[Dict[str, Any]], sample_id: str, sample_limit: int) -> List[Dict[str, Any]]:
    if sample_id:
        return [find_sample(items, sample_id)]
    if sample_limit <= 0:
        raise ValueError("--sample-limit must be greater than 0")
    return items[:sample_limit]


def resolve_db_config(db_cfg: Dict[str, Any], dataset_info: Dict[str, Any]) -> Dict[str, Any]:
    db_name = dataset_info.get("database", "default")
    if db_name != "default":
        databases = db_cfg.get("databases", {})
        if db_name in databases:
            return databases[db_name]
    return db_cfg.get("database", {})


def run_one_sample(
    *,
    sample: Dict[str, Any],
    args: argparse.Namespace,
    prompt_builder: PromptBuilder,
    loader: Any,
    model_inference: ModelInference,
    evaluator: Any,
) -> Dict[str, Any]:
    prompt = prompt_builder.build_prompt(
        question=sample["question"],
        schema=sample["schema"],
        config_type=args.config,
        rag_context=None,
        keyword_context=None,
        dataset_name=args.dataset,
        metadata=sample.get("metadata", {}),
    )

    raw_reasoning = ""
    raw_content = ""
    final_sql = ""
    normalized_sql = ""
    eval_info = None
    inference_metrics = {}

    started_at_unix_ms = time.time_ns() // 1_000_000
    started_perf_ns = time.perf_counter_ns()
    generation_result = loader.generate(prompt)
    finished_at_unix_ms = time.time_ns() // 1_000_000
    latency_ms = (time.perf_counter_ns() - started_perf_ns) / 1_000_000.0

    response_metadata = generation_result.response_metadata or {}
    raw_reasoning = (response_metadata.get("reasoning") or "").strip()
    raw_content = (
        response_metadata.get("content")
        or generation_result.raw_text
        or generation_result.sql
        or ""
    ).strip()
    final_sql = generation_result.sql
    inference_metrics = model_inference._build_inference_metrics(
        model_loader=loader,
        prompt=prompt,
        generation_result=generation_result,
        started_at_unix_ms=started_at_unix_ms,
        finished_at_unix_ms=finished_at_unix_ms,
        latency_ms=latency_ms,
        status="success",
    )

    normalized_sql = model_inference._normalize_prediction(final_sql, sample)

    if evaluator is not None:
        eval_info = evaluator._execution_accuracy(
            normalized_sql,
            sample.get("gold_sql", ""),
            gold_sql_candidates=sample.get("gold_sql_candidates") or None,
            sample_label=f"{args.dataset}:{sample.get('id')}",
        )

    return {
        "sample": sample,
        "prompt": prompt,
        "raw_reasoning": raw_reasoning,
        "raw_content": raw_content,
        "final_sql": final_sql,
        "normalized_sql": normalized_sql,
        "inference_metrics": inference_metrics,
        "eval_info": eval_info,
    }


def _format_metric_value(value: Any, digits: int = 1) -> str:
    if value is None:
        return "<unavailable>"
    if isinstance(value, float):
        return f"{value:.{digits}f}"
    return str(value)


def summarize_inference_metrics(results: List[Dict[str, Any]]) -> Dict[str, Any]:
    question_count = len(results)
    sum_input_tokens = 0.0
    sum_output_tokens = 0.0
    sum_total_tokens = 0.0
    sum_latency_ms = 0.0
    token_measurements = 0
    latency_measurements = 0

    for result in results:
        metrics = result.get("inference_metrics") or {}
        input_tokens = metrics.get("input_tokens")
        output_tokens = metrics.get("output_tokens")
        total_tokens = metrics.get("total_tokens")
        latency_ms = metrics.get("latency_ms")

        if input_tokens is not None:
            sum_input_tokens += float(input_tokens)
            token_measurements += 1
        if output_tokens is not None:
            sum_output_tokens += float(output_tokens)
        if total_tokens is not None:
            sum_total_tokens += float(total_tokens)
        if latency_ms is not None:
            sum_latency_ms += float(latency_ms)
            latency_measurements += 1

    return {
        "avg_input_tokens": sum_input_tokens / question_count if question_count else 0.0,
        "avg_output_tokens": sum_output_tokens / question_count if question_count else 0.0,
        "avg_total_tokens": sum_total_tokens / question_count if question_count else 0.0,
        "avg_latency_ms": sum_latency_ms / question_count if question_count else 0.0,
        "sum_input_tokens": sum_input_tokens,
        "sum_output_tokens": sum_output_tokens,
        "sum_total_tokens": sum_total_tokens,
        "sum_latency_ms": sum_latency_ms,
        "question_count": question_count,
        "token_measurements": token_measurements,
        "latency_measurements": latency_measurements,
    }


def print_sample_result(
    *,
    result: Dict[str, Any],
    args: argparse.Namespace,
    data_file: Path,
    group_field: str,
    group_value: str,
    resolved_backend: str,
    sample_index: int,
    total_samples: int,
) -> None:
    sample = result["sample"]
    prompt = result["prompt"]
    raw_reasoning = result["raw_reasoning"]
    raw_content = result["raw_content"]
    final_sql = result["final_sql"]
    normalized_sql = result["normalized_sql"]
    inference_metrics = result.get("inference_metrics") or {}
    eval_info = result["eval_info"]

    print("=" * 100)
    print(f"SAMPLE {sample_index}/{total_samples}")
    print(f"dataset         : {args.dataset}")
    print(f"group_field     : {group_field or '<none>'}")
    print(f"group_value     : {group_value or '<none>'}")
    print(f"data_file       : {data_file}")
    print(f"sample_id       : {sample.get('id')}")
    print(f"model           : {args.model}")
    print(f"backend         : {resolved_backend}")
    print(f"config_type     : {args.config}")
    print("=" * 100)
    print("QUESTION:")
    print(sample["question"])
    print("=" * 100)
    print("GOLD SQL:")
    print(sample.get("gold_sql", ""))
    print("=" * 100)

    if args.show_prompt:
        print("PROMPT PREVIEW:")
        print(clip(prompt, args.preview_chars))
        print("=" * 100)

    print(f"RAW REASONING LENGTH: {len(raw_reasoning)}")
    print("RAW REASONING PREVIEW:")
    print(clip(raw_reasoning, args.preview_chars))
    print("=" * 100)

    print(f"RAW CONTENT LENGTH: {len(raw_content)}")
    print("RAW CONTENT PREVIEW:")
    print(clip(raw_content, args.preview_chars))
    print("=" * 100)

    print("FINAL SQL:")
    print(final_sql.strip() if final_sql and final_sql.strip() else "<empty>")
    print("=" * 100)

    print("NORMALIZED SQL:")
    print(normalized_sql.strip() if normalized_sql and normalized_sql.strip() else "<empty>")
    print("=" * 100)

    print("INFERENCE METRICS:")
    print(f"input_tokens    : {_format_metric_value(inference_metrics.get('input_tokens'), digits=0)}")
    print(f"output_tokens   : {_format_metric_value(inference_metrics.get('output_tokens'), digits=0)}")
    print(f"total_tokens    : {_format_metric_value(inference_metrics.get('total_tokens'), digits=0)}")
    print(f"latency_ms      : {_format_metric_value(inference_metrics.get('latency_ms'), digits=2)}")
    print(f"metric_source   : {inference_metrics.get('measurement_source', '<unknown>')}")
    print(f"metric_status   : {inference_metrics.get('status', '<unknown>')}")
    print("=" * 100)

    if eval_info is not None:
        print("SINGLE-SAMPLE EVAL:")
        print(json.dumps(eval_info, ensure_ascii=False, indent=2, default=str))
        print("=" * 100)


def main() -> int:
    args = parse_args()
    project_root = REPO_ROOT

    dataset_config = load_yaml(project_root / "config" / "dataset_config.yaml")
    eval_config = load_yaml(project_root / "config" / "eval_config.yaml")
    db_config = load_yaml(project_root / "config" / "db_config.yaml")

    if args.dataset not in dataset_config["datasets"]:
        raise ValueError(f"unknown dataset: {args.dataset}")

    dataset_info = dataset_config["datasets"][args.dataset]
    data_file, group_field, resolved_group_value = resolve_data_file(
        project_root,
        dataset_config,
        args.dataset,
        args.group_value,
    )
    if not data_file.exists():
        raise FileNotFoundError(f"preprocessed file not found: {data_file}")

    with open(data_file, "r", encoding="utf-8") as handle:
        data_items = json.load(handle)
    data_items = hydrate_schema_references(project_root, data_items)
    samples = select_samples(data_items, args.sample_id, args.sample_limit)

    prompt_builder = PromptBuilder(eval_config)

    model_inference = ModelInference(
        model_config_path=str(project_root / "config" / "model_config.yaml"),
        eval_config_path=str(project_root / "config" / "eval_config.yaml"),
    )
    model_info, resolved_backend = model_inference.resolve_model_config(args.model, args.backend)
    loader = ModelLoaderFactory.create(model_info["loader_class"], model_info)

    evaluator = None
    if not args.no_eval:
        from src.evaluation.evaluator import Evaluator

        evaluator = Evaluator(resolve_db_config(db_config, dataset_info), eval_config)

    results = []

    loader.load_model()
    try:
        for sample_index, sample in enumerate(samples, start=1):
            try:
                result = run_one_sample(
                    sample=sample,
                    args=args,
                    prompt_builder=prompt_builder,
                    loader=loader,
                    model_inference=model_inference,
                    evaluator=evaluator,
                )
            except Exception as exc:
                result = {
                    "sample": sample,
                    "prompt": "",
                    "raw_reasoning": "",
                    "raw_content": "",
                    "final_sql": "",
                    "normalized_sql": "",
                    "inference_metrics": {},
                    "eval_info": {
                        "correct": 0,
                        "error_type": "runtime_error",
                        "error_message": str(exc),
                        "pred_result_count": None,
                        "gold_result_count": None,
                        "execution_error": {
                            "sql": "",
                            "error": str(exc),
                            "error_type": type(exc).__name__,
                        },
                    },
                }
            results.append(result)
            print_sample_result(
                result=result,
                args=args,
                data_file=data_file,
                group_field=group_field,
                group_value=resolved_group_value,
                resolved_backend=resolved_backend,
                sample_index=sample_index,
                total_samples=len(samples),
            )
    finally:
        try:
            loader.unload()
        except Exception:
            pass

    print("=" * 100)
    print("SUMMARY")
    print(f"dataset         : {args.dataset}")
    print(f"group_field     : {group_field or '<none>'}")
    print(f"group_value     : {resolved_group_value or '<none>'}")
    print(f"data_file       : {data_file}")
    print(f"model           : {args.model}")
    print(f"backend         : {resolved_backend}")
    print(f"config_type     : {args.config}")
    print(f"sample_count    : {len(samples)}")
    print("=" * 100)
    sample_ids = [str(result["sample"].get("id")) for result in results]
    print(f"sample_ids       : {', '.join(sample_ids)}")
    inference_metrics = summarize_inference_metrics(results)
    print(
        "avg_tokens      : "
        f"in={inference_metrics['avg_input_tokens']:.1f}, "
        f"out={inference_metrics['avg_output_tokens']:.1f}, "
        f"total={inference_metrics['avg_total_tokens']:.1f}"
    )
    print(f"avg_latency_ms   : {inference_metrics['avg_latency_ms']:.2f}")
    print(
        "sum_tokens      : "
        f"in={inference_metrics['sum_input_tokens']:.0f}, "
        f"out={inference_metrics['sum_output_tokens']:.0f}, "
        f"total={inference_metrics['sum_total_tokens']:.0f}"
    )
    print(f"sum_latency_ms   : {inference_metrics['sum_latency_ms']:.2f}")
    if evaluator is not None:
        correct_count = sum(
            1 for result in results
            if result.get("eval_info") and result["eval_info"].get("correct") == 1
        )
        print(f"correct_count    : {correct_count}")
        print(f"accuracy         : {correct_count}/{len(results)} = {correct_count / len(results):.4f}")
    else:
        print("evaluation       : skipped")
    print("=" * 100)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
