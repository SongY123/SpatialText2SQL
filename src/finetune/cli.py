"""CLI for TRL-based full fine-tuning of spatial Text-to-SQL models."""

from __future__ import annotations

import argparse
import logging
import os
import subprocess
import sys
from pathlib import Path

import torch

from .config import (
    DEFAULT_TRL_FINETUNE_CONFIG_PATH,
    load_trl_finetune_config,
    override_trl_finetune_config,
)
from .formatter import NL2SQLAlpacaFormatter
from .io import (
    load_alpaca_finetune_samples,
    load_raw_finetune_samples,
    write_alpaca_finetune_samples,
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Automatically format nl2sql JSONL into Alpaca JSONL and run TRL full fine-tuning."
    )
    parser.add_argument("--config", default=str(DEFAULT_TRL_FINETUNE_CONFIG_PATH))
    parser.add_argument("--input")
    parser.add_argument("--alpaca-output")
    parser.add_argument("--model-name-or-path")
    parser.add_argument("--tokenizer-name-or-path")
    parser.add_argument("--output-dir")
    parser.add_argument("--eval-ratio", type=float)
    parser.add_argument("--log-level")
    parser.add_argument("--log-path")
    parser.add_argument("--nvidia-gpu-indices")
    parser.add_argument("--distributed-backend", choices=["none", "accelerate"])
    parser.add_argument("--dynamo-backend")
    parser.add_argument("--num-processes", type=int)
    parser.add_argument("--main-process-port", type=int)
    parser.add_argument("--deepspeed-config-path")
    parser.add_argument("--launched-by-accelerate", action="store_true", help=argparse.SUPPRESS)
    return parser


def _apply_runtime_environment(config) -> None:
    gpu_indices = list(config.runtime.nvidia_gpu_indices or [])
    if not gpu_indices:
        return
    gpu_value = ",".join(str(index) for index in gpu_indices)
    os.environ["CUDA_VISIBLE_DEVICES"] = gpu_value
    os.environ["NVIDIA_VISIBLE_DEVICES"] = gpu_value


def _effective_num_processes(config) -> int:
    configured = int(config.runtime.num_processes)
    if configured > 0:
        return configured
    gpu_indices = list(config.runtime.nvidia_gpu_indices or [])
    return max(len(gpu_indices), 1)


def _resolve_accelerate_mixed_precision(config) -> str:
    if config.training.bf16:
        return "bf16"
    if config.training.fp16:
        return "fp16"
    return "no"


def _is_rank_zero_env() -> bool:
    rank = os.environ.get("RANK")
    local_rank = os.environ.get("LOCAL_RANK")
    return rank in (None, "", "0") and local_rank in (None, "", "0")


def _should_launch_with_accelerate(config, args) -> bool:
    if args.launched_by_accelerate:
        return False
    if os.environ.get("LOCAL_RANK") is not None:
        return False
    if os.environ.get("WORLD_SIZE") not in (None, "", "1"):
        return False
    if str(config.runtime.distributed_backend).strip().lower() != "accelerate":
        return False
    return _effective_num_processes(config) > 1


def _build_accelerate_command(config, args) -> list[str]:
    command = [
        sys.executable,
        "-m",
        "accelerate.commands.launch",
        "--multi_gpu",
        "--mixed_precision",
        _resolve_accelerate_mixed_precision(config),
        "--num_processes",
        str(_effective_num_processes(config)),
        "--num_machines",
        str(config.runtime.num_machines),
        "--dynamo_backend",
        config.runtime.dynamo_backend,
        "--machine_rank",
        str(config.runtime.machine_rank),
        "--main_process_port",
        str(config.runtime.main_process_port),
        "-m",
        "src.finetune.cli",
        "--config",
        str(args.config),
        "--input",
        config.data.input_path,
        "--alpaca-output",
        config.data.alpaca_output_path,
        "--model-name-or-path",
        config.model.model_name_or_path,
        "--output-dir",
        config.training.output_dir,
        "--eval-ratio",
        str(config.data.eval_ratio),
        "--distributed-backend",
        "accelerate",
        "--num-processes",
        str(_effective_num_processes(config)),
        "--main-process-port",
        str(config.runtime.main_process_port),
        "--launched-by-accelerate",
    ]
    if config.model.tokenizer_name_or_path:
        command.extend(["--tokenizer-name-or-path", config.model.tokenizer_name_or_path])
    if config.logging.log_level:
        command.extend(["--log-level", config.logging.log_level])
    if config.logging.log_path:
        command.extend(["--log-path", config.logging.log_path])
    if config.runtime.nvidia_gpu_indices:
        command.extend(
            ["--nvidia-gpu-indices", ",".join(str(index) for index in config.runtime.nvidia_gpu_indices)]
        )
    if config.training.deepspeed_config_path:
        command.extend(["--deepspeed-config-path", config.training.deepspeed_config_path])
    return command


def _prepare_rows(config) -> list:
    raw_rows = load_raw_finetune_samples(config.data.input_path)
    if _is_rank_zero_env():
        logging.info("Loaded raw fine-tune samples | count=%s | input=%s", len(raw_rows), config.data.input_path)
    formatter = NL2SQLAlpacaFormatter(data_config=config.data)
    alpaca_rows = formatter.format_samples(raw_rows)
    write_alpaca_finetune_samples(config.data.alpaca_output_path, alpaca_rows)
    if _is_rank_zero_env():
        logging.info(
            "Formatted Alpaca fine-tune samples written | count=%s | output=%s",
            len(alpaca_rows),
            config.data.alpaca_output_path,
        )
    return alpaca_rows


def _cleanup_distributed() -> None:
    if torch.distributed.is_available() and torch.distributed.is_initialized():
        torch.distributed.destroy_process_group()


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(sys.argv[1:] if argv is None else argv)

    config = load_trl_finetune_config(args.config)
    config = override_trl_finetune_config(
        config,
        data={key: value for key, value in {
            "input_path": args.input,
            "alpaca_output_path": args.alpaca_output,
            "eval_ratio": args.eval_ratio,
        }.items() if value is not None},
        model={key: value for key, value in {
            "model_name_or_path": args.model_name_or_path,
            "tokenizer_name_or_path": args.tokenizer_name_or_path,
        }.items() if value is not None},
        training={key: value for key, value in {
            "output_dir": args.output_dir,
            "deepspeed_config_path": args.deepspeed_config_path,
        }.items() if value is not None},
        logging={key: value for key, value in {
            "log_level": args.log_level,
            "log_path": args.log_path,
        }.items() if value is not None},
        runtime={key: value for key, value in {
            "nvidia_gpu_indices": args.nvidia_gpu_indices,
            "distributed_backend": args.distributed_backend,
            "dynamo_backend": args.dynamo_backend,
            "num_processes": args.num_processes,
            "main_process_port": args.main_process_port,
        }.items() if value is not None},
    )
    _apply_runtime_environment(config)

    log_handlers = None
    if config.logging.log_path:
        Path(config.logging.log_path).parent.mkdir(parents=True, exist_ok=True)
        log_handlers = [logging.FileHandler(config.logging.log_path, encoding="utf-8"), logging.StreamHandler()]
    logging.basicConfig(
        level=getattr(logging, config.logging.log_level.upper(), logging.INFO),
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        handlers=log_handlers,
    )
    if _is_rank_zero_env():
        logging.info(
            "TRL fine-tune config loaded | input=%s | alpaca_output=%s | model=%s | output_dir=%s | distributed_backend=%s | nvidia_gpu_indices=%s | num_processes=%s",
            config.data.input_path,
            config.data.alpaca_output_path,
            config.model.model_name_or_path,
            config.training.output_dir,
            config.runtime.distributed_backend,
            config.runtime.nvidia_gpu_indices,
            _effective_num_processes(config),
        )

    if _should_launch_with_accelerate(config, args):
        _prepare_rows(config)
        command = _build_accelerate_command(config, args)
        logging.info("Launching distributed fine-tuning via accelerate | command=%s", command)
        completed = subprocess.run(command, env=os.environ.copy(), check=False)
        return int(completed.returncode)

    if args.launched_by_accelerate:
        alpaca_rows = load_alpaca_finetune_samples(config.data.alpaca_output_path)
    else:
        alpaca_rows = _prepare_rows(config)

    from .trainer import TRLFullFinetuner

    try:
        finetuner = TRLFullFinetuner(config)
        metrics = finetuner.train(alpaca_rows)
        if metrics is not None and _is_rank_zero_env():
            logging.info("TRL fine-tuning completed | metrics=%s", metrics)
        return 0
    finally:
        _cleanup_distributed()


if __name__ == "__main__":
    raise SystemExit(main())
