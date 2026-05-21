#!/usr/bin/env python3
"""Diagnose likely causes of eval_loss=nan for TRL fine-tuning runs."""

from __future__ import annotations

import argparse
import json
import math
import statistics
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import torch
import transformers

from src.finetune.config import DEFAULT_TRL_FINETUNE_CONFIG_PATH, load_trl_finetune_config, override_trl_finetune_config
from src.finetune.formatter import NL2SQLAlpacaFormatter
from src.finetune.io import load_raw_finetune_samples
from src.finetune.prompting import FinetunePromptRenderer
from src.finetune.trainer import TRLFullFinetuner


@dataclass
class SampleStats:
    index: int
    question_id: str
    database_id: str
    prompt_chars: int
    completion_chars: int
    total_chars: int
    token_count: int
    supervised_tokens: int
    over_config_max_length: bool
    output_starts_with_sql_fence: bool
    loss: float | None = None
    loss_is_nan: bool = False
    loss_is_inf: bool = False
    error: str = ""


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Diagnose likely causes of eval_loss=nan in finetune runs.")
    parser.add_argument("--config", default=str(DEFAULT_TRL_FINETUNE_CONFIG_PATH))
    parser.add_argument("--input")
    parser.add_argument("--eval-ratio", type=float)
    parser.add_argument("--model-name-or-path")
    parser.add_argument("--tokenizer-name-or-path")
    parser.add_argument("--max-samples", type=int, default=0, help="Only inspect the first N formatted samples. 0 means all.")
    parser.add_argument("--top-k", type=int, default=10, help="Show the top K longest eval samples.")
    parser.add_argument(
        "--check-model-forward",
        action="store_true",
        help="Load the model and run no-grad forward passes on eval rows to catch NaN losses directly.",
    )
    parser.add_argument(
        "--device",
        default="auto",
        help="Device for --check-model-forward. Use auto, cpu, cuda, cuda:0, etc.",
    )
    return parser


def summarize_numeric(values: list[int]) -> dict[str, float]:
    if not values:
        return {"min": 0.0, "p50": 0.0, "p95": 0.0, "max": 0.0, "mean": 0.0}
    sorted_values = sorted(values)
    return {
        "min": float(sorted_values[0]),
        "p50": float(statistics.median(sorted_values)),
        "p95": float(sorted_values[min(len(sorted_values) - 1, max(math.ceil(len(sorted_values) * 0.95) - 1, 0))]),
        "max": float(sorted_values[-1]),
        "mean": float(statistics.fmean(sorted_values)),
    }


def resolve_device(device_arg: str) -> str:
    if device_arg != "auto":
        return device_arg
    return "cuda" if torch.cuda.is_available() else "cpu"


def build_labels(input_ids: list[int], completion_mask: list[int]) -> list[int]:
    labels: list[int] = []
    for token_id, mask_value in zip(input_ids, completion_mask, strict=False):
        labels.append(token_id if int(mask_value) else -100)
    return labels


def inspect_eval_rows(
    rows,
    *,
    tokenizer,
    config,
    run_forward: bool,
    model=None,
    device: str = "cpu",
) -> list[SampleStats]:
    stats: list[SampleStats] = []
    for index, row in enumerate(rows):
        prompt = FinetunePromptRenderer.compose_prompt(row.instruction, row.input_text)
        completion = row.output_text
        full_text = prompt + completion
        question_id = f"eval_{index:05d}"
        database_id = ""
        payload = TRLFullFinetuner._tokenize_with_completion_mask(
            tokenizer=tokenizer,
            full_text=full_text,
            completion_start=len(prompt),
        )
        sample_stats = SampleStats(
            index=index,
            question_id=question_id,
            database_id=database_id,
            prompt_chars=len(prompt),
            completion_chars=len(completion),
            total_chars=len(full_text),
            token_count=len(payload["input_ids"]),
            supervised_tokens=sum(int(item) for item in payload["completion_mask"]),
            over_config_max_length=len(payload["input_ids"]) > int(config.training.max_length),
            output_starts_with_sql_fence=completion.strip().startswith("```sql"),
        )
        if run_forward and model is not None:
            try:
                labels = build_labels(payload["input_ids"], payload["completion_mask"])
                batch = {
                    "input_ids": torch.tensor([payload["input_ids"]], dtype=torch.long, device=device),
                    "attention_mask": torch.ones((1, len(payload["input_ids"])), dtype=torch.long, device=device),
                    "labels": torch.tensor([labels], dtype=torch.long, device=device),
                }
                with torch.no_grad():
                    outputs = model(**batch)
                loss_value = float(outputs.loss.detach().float().cpu().item())
                sample_stats.loss = loss_value
                sample_stats.loss_is_nan = math.isnan(loss_value)
                sample_stats.loss_is_inf = math.isinf(loss_value)
            except Exception as exc:  # pragma: no cover - best-effort diagnostic path
                sample_stats.error = f"{type(exc).__name__}: {exc}"
        stats.append(sample_stats)
    return stats


def print_summary(stats: list[SampleStats], *, config_max_length: int, model_context_limit: int | None) -> None:
    token_counts = [item.token_count for item in stats]
    supervised_counts = [item.supervised_tokens for item in stats]
    over_length = [item for item in stats if item.over_config_max_length]
    nan_losses = [item for item in stats if item.loss_is_nan]
    inf_losses = [item for item in stats if item.loss_is_inf]
    errored = [item for item in stats if item.error]

    print("== Eval Sample Summary ==")
    print(f"eval_rows: {len(stats)}")
    print(f"config_max_length: {config_max_length}")
    print(f"model_context_limit: {model_context_limit if model_context_limit else 'unknown'}")
    print(f"rows_over_config_max_length: {len(over_length)}")
    if model_context_limit:
        over_model_limit = sum(1 for item in stats if item.token_count > model_context_limit)
        print(f"rows_over_model_context_limit: {over_model_limit}")
    print(f"rows_with_nan_forward_loss: {len(nan_losses)}")
    print(f"rows_with_inf_forward_loss: {len(inf_losses)}")
    print(f"rows_with_forward_errors: {len(errored)}")
    print(f"rows_without_sql_fence: {sum(1 for item in stats if not item.output_starts_with_sql_fence)}")
    print()

    print("token_count_stats:", json.dumps(summarize_numeric(token_counts), ensure_ascii=False))
    print("supervised_token_stats:", json.dumps(summarize_numeric(supervised_counts), ensure_ascii=False))
    print()


def print_top_offenders(stats: list[SampleStats], *, top_k: int) -> None:
    print(f"== Top {top_k} Longest Eval Samples ==")
    for item in sorted(stats, key=lambda sample: sample.token_count, reverse=True)[:top_k]:
        payload = {
            "index": item.index,
            "question_id": item.question_id,
            "database_id": item.database_id,
            "token_count": item.token_count,
            "supervised_tokens": item.supervised_tokens,
            "prompt_chars": item.prompt_chars,
            "completion_chars": item.completion_chars,
            "over_config_max_length": item.over_config_max_length,
            "sql_fence": item.output_starts_with_sql_fence,
        }
        if item.loss is not None:
            payload["loss"] = item.loss
            payload["loss_is_nan"] = item.loss_is_nan
            payload["loss_is_inf"] = item.loss_is_inf
        if item.error:
            payload["error"] = item.error
        print(json.dumps(payload, ensure_ascii=False))
    print()


def print_likely_causes(stats: list[SampleStats], *, config_max_length: int, model_context_limit: int | None) -> None:
    print("== Likely Causes ==")
    if not stats:
        print("- 没有 eval 样本。先检查 eval_ratio 或数据量。")
        return

    over_config = [item for item in stats if item.over_config_max_length]
    if over_config:
        print(
            f"- 有 {len(over_config)} 条 eval 样本 token 长度超过 config.max_length={config_max_length}。"
            " 这是当前最可疑的原因。"
        )
    if model_context_limit and any(item.token_count > model_context_limit for item in stats):
        print(
            f"- 有样本超过模型 context limit={model_context_limit}。如果训练没有显式截断，这很容易导致 loss/forward 异常。"
        )
    if any(item.supervised_tokens <= 2 for item in stats):
        print("- 存在 supervision token 极少的样本。即使不直接报错，也容易让 eval loss 失真。")
    if any(not item.output_starts_with_sql_fence for item in stats):
        print("- 存在输出不以 ```sql 代码块开头的样本，格式可能和当前训练目标不一致。")
    nan_losses = [item for item in stats if item.loss_is_nan]
    if nan_losses:
        print(f"- 前向检查里直接发现 {len(nan_losses)} 条样本 loss=nan。优先查看这些样本的长度和内容。")
    forward_errors = [item for item in stats if item.error]
    if forward_errors:
        print(f"- 前向检查里有 {len(forward_errors)} 条样本直接报错，通常比 NaN 更能直接说明原因。")
    if not over_config and not nan_losses and not forward_errors:
        print("- 没有发现明显的格式问题。下一步更可能是 bf16/FlashAttention/ZeRO-3/优化器数值稳定性问题。")
    print()


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()

    config = load_trl_finetune_config(args.config)
    config = override_trl_finetune_config(
        config,
        data={key: value for key, value in {
            "input_path": args.input,
            "eval_ratio": args.eval_ratio,
        }.items() if value is not None},
        model={key: value for key, value in {
            "model_name_or_path": args.model_name_or_path,
            "tokenizer_name_or_path": args.tokenizer_name_or_path,
        }.items() if value is not None},
    )

    raw_rows = load_raw_finetune_samples(config.data.input_path)
    formatter = NL2SQLAlpacaFormatter(data_config=config.data)
    alpaca_rows = formatter.format_samples(raw_rows)
    if args.max_samples > 0:
        alpaca_rows = alpaca_rows[: args.max_samples]

    finetuner = TRLFullFinetuner(config)
    _, eval_rows = finetuner._split_rows(alpaca_rows)

    tokenizer_name = config.model.tokenizer_name_or_path or config.model.model_name_or_path
    tokenizer = transformers.AutoTokenizer.from_pretrained(
        tokenizer_name,
        trust_remote_code=config.model.trust_remote_code,
    )
    if tokenizer.pad_token is None:
        tokenizer.pad_token = tokenizer.eos_token
    if tokenizer.pad_token_id is None and tokenizer.eos_token_id is not None:
        tokenizer.pad_token_id = tokenizer.eos_token_id

    model_context_limit: int | None = None
    try:
        model_config = transformers.AutoConfig.from_pretrained(
            config.model.model_name_or_path,
            trust_remote_code=config.model.trust_remote_code,
        )
        for candidate in ("max_position_embeddings", "n_positions", "seq_length", "model_max_length"):
            value = getattr(model_config, candidate, None)
            if isinstance(value, int) and value > 0:
                model_context_limit = value
                break
    except Exception as exc:  # pragma: no cover - best-effort diagnostic path
        print(f"warning: failed to load model config for context limit detection: {type(exc).__name__}: {exc}")

    model = None
    device = resolve_device(args.device)
    if args.check_model_forward:
        torch_dtype = finetuner._resolve_torch_dtype()
        model_kwargs: dict[str, Any] = {"trust_remote_code": config.model.trust_remote_code}
        if torch_dtype is not None:
            model_kwargs["torch_dtype"] = torch_dtype
        if config.model.attn_implementation:
            model_kwargs["attn_implementation"] = config.model.attn_implementation
        model = transformers.AutoModelForCausalLM.from_pretrained(
            config.model.model_name_or_path,
            **model_kwargs,
        )
        model.eval()
        model.to(device)

    stats = inspect_eval_rows(
        eval_rows,
        tokenizer=tokenizer,
        config=config,
        run_forward=args.check_model_forward,
        model=model,
        device=device,
    )
    print_summary(stats, config_max_length=int(config.training.max_length), model_context_limit=model_context_limit)
    print_top_offenders(stats, top_k=max(args.top_k, 1))
    print_likely_causes(stats, config_max_length=int(config.training.max_length), model_context_limit=model_context_limit)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
