#!/usr/bin/env python3
"""Materialize Gemma4 shared k_norm weights for vLLM loading."""

from __future__ import annotations

import argparse
import importlib.util
import json
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]


def _load_repair_function():
    module_path = REPO_ROOT / "src" / "finetune" / "gemma4_checkpoint.py"
    spec = importlib.util.spec_from_file_location("gemma4_checkpoint", module_path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Failed to load Gemma4 checkpoint repair module: {module_path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module.materialize_gemma4_k_norm_weights


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Repair a Gemma4 HF checkpoint whose safetensors omitted KV-shared "
            "self_attn.k_norm alias keys required by vLLM."
        )
    )
    parser.add_argument(
        "--checkpoint-dir",
        required=True,
        help="Fine-tuned Gemma4 checkpoint directory to repair.",
    )
    parser.add_argument(
        "--base-model-dir",
        required=True,
        help="Original Gemma4 base model directory used as fallback for missing alias tensors.",
    )
    parser.add_argument(
        "--output-dir",
        default="",
        help="Output directory. Defaults to '<checkpoint-dir>-vllm'.",
    )
    parser.add_argument(
        "--format",
        choices=["safetensors", "bin"],
        default="safetensors",
        help="Output weight format. safetensors keeps vLLM loading fast; bin is useful for debugging.",
    )
    parser.add_argument(
        "--in-place",
        action="store_true",
        help="Rewrite the checkpoint directory in place instead of writing a sibling vLLM directory.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Only report missing Gemma4 alias keys without writing files.",
    )
    parser.add_argument(
        "--use-transformers-state-dict",
        action="store_true",
        help=(
            "Load the fine-tuned model with Transformers and use its state_dict "
            "as the first source for missing alias tensors. Falls back to base-model-dir."
        ),
    )
    parser.add_argument(
        "--trust-remote-code",
        action="store_true",
        help="Pass trust_remote_code=True when loading with Transformers.",
    )
    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    checkpoint_dir = Path(args.checkpoint_dir).expanduser().resolve()
    if args.in_place:
        output_dir = checkpoint_dir
    elif args.output_dir:
        output_dir = Path(args.output_dir).expanduser().resolve()
    else:
        output_dir = checkpoint_dir.parent / f"{checkpoint_dir.name}-vllm"

    model_state_dict = None
    if args.use_transformers_state_dict and not args.dry_run:
        from transformers import AutoModelForImageTextToText

        model = AutoModelForImageTextToText.from_pretrained(
            str(checkpoint_dir),
            torch_dtype="auto",
            trust_remote_code=args.trust_remote_code,
            low_cpu_mem_usage=True,
        )
        model_state_dict = model.state_dict

    materialize_gemma4_k_norm_weights = _load_repair_function()
    result = materialize_gemma4_k_norm_weights(
        checkpoint_dir=checkpoint_dir,
        base_model_dir=args.base_model_dir,
        output_dir=output_dir,
        model_state_dict=model_state_dict,
        safe_serialization=args.format == "safetensors",
        dry_run=args.dry_run,
    )
    print(json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
