"""CLI for TRL-based full fine-tuning of spatial Text-to-SQL models."""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

from .config import (
    DEFAULT_TRL_FINETUNE_CONFIG_PATH,
    load_trl_finetune_config,
    override_trl_finetune_config,
)
from .dataset import SpatialText2SQLDatasetBuilder
from .io import (
    load_prepared_finetune_samples,
    load_raw_finetune_samples,
    write_prepared_finetune_samples,
)
from .trainer import TRLFullFinetuner


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Prepare and run TRL full fine-tuning for spatial Text-to-SQL.")
    parser.add_argument("--config", default=str(DEFAULT_TRL_FINETUNE_CONFIG_PATH))
    parser.add_argument("--input")
    parser.add_argument("--prepared-output")
    parser.add_argument("--model-name-or-path")
    parser.add_argument("--tokenizer-name-or-path")
    parser.add_argument("--output-dir")
    parser.add_argument("--eval-ratio", type=float)
    parser.add_argument("--prepare-only", action="store_true")
    parser.add_argument("--train-only", action="store_true")
    parser.add_argument("--log-level")
    parser.add_argument("--log-path")
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(sys.argv[1:] if argv is None else argv)
    if args.prepare_only and args.train_only:
        raise ValueError("--prepare-only and --train-only cannot be used together.")

    config = load_trl_finetune_config(args.config)
    config = override_trl_finetune_config(
        config,
        data={key: value for key, value in {
            "input_path": args.input,
            "prepared_output_path": args.prepared_output,
            "eval_ratio": args.eval_ratio,
        }.items() if value is not None},
        model={key: value for key, value in {
            "model_name_or_path": args.model_name_or_path,
            "tokenizer_name_or_path": args.tokenizer_name_or_path,
        }.items() if value is not None},
        training={key: value for key, value in {
            "output_dir": args.output_dir,
        }.items() if value is not None},
        logging={key: value for key, value in {
            "log_level": args.log_level,
            "log_path": args.log_path,
        }.items() if value is not None},
    )

    log_handlers = None
    if config.logging.log_path:
        Path(config.logging.log_path).parent.mkdir(parents=True, exist_ok=True)
        log_handlers = [logging.FileHandler(config.logging.log_path, encoding="utf-8"), logging.StreamHandler()]
    logging.basicConfig(
        level=getattr(logging, config.logging.log_level.upper(), logging.INFO),
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        handlers=log_handlers,
    )
    logging.info(
        "TRL fine-tune config loaded | input=%s | prepared_output=%s | model=%s | output_dir=%s",
        config.data.input_path,
        config.data.prepared_output_path,
        config.model.model_name_or_path,
        config.training.output_dir,
    )

    if args.train_only:
        prepared_rows = load_prepared_finetune_samples(config.data.prepared_output_path)
    else:
        raw_rows = load_raw_finetune_samples(config.data.input_path)
        logging.info("Loaded raw fine-tune samples | count=%s", len(raw_rows))
        builder = SpatialText2SQLDatasetBuilder(
            db_config=config.database,
            data_config=config.data,
        )
        prepared_rows = builder.prepare_samples(raw_rows)
        write_prepared_finetune_samples(config.data.prepared_output_path, prepared_rows)
        logging.info(
            "Prepared fine-tune samples written | count=%s | output=%s",
            len(prepared_rows),
            config.data.prepared_output_path,
        )
        if args.prepare_only:
            return 0

    finetuner = TRLFullFinetuner(config)
    metrics = finetuner.train(prepared_rows)
    logging.info("TRL fine-tuning completed | metrics=%s", metrics)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
