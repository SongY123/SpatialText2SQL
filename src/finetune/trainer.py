"""TRL full-parameter fine-tuning runner."""

from __future__ import annotations

import inspect
import json
import logging
import math
import os
from pathlib import Path
from typing import Any, Sequence

import datasets
import numpy as np
import torch
import transformers
import trl

from .config import SpatialText2SQLFinetuneConfig
from .models import AlpacaFinetuneSample
from .prompting import FinetunePromptRenderer
from .utils import stable_jsonify

LOGGER = logging.getLogger(__name__)


class TRLFullFinetuner:
    def __init__(self, config: SpatialText2SQLFinetuneConfig) -> None:
        self.config = config

    def train(self, rows: Sequence[AlpacaFinetuneSample]) -> dict[str, Any] | None:
        if not rows:
            raise ValueError("No Alpaca fine-tune rows were provided.")

        tokenizer_name = self.config.model.tokenizer_name_or_path or self.config.model.model_name_or_path
        tokenizer = transformers.AutoTokenizer.from_pretrained(
            tokenizer_name,
            trust_remote_code=self.config.model.trust_remote_code,
        )
        if tokenizer.pad_token is None:
            tokenizer.pad_token = tokenizer.eos_token
        if tokenizer.pad_token_id is None and tokenizer.eos_token_id is not None:
            tokenizer.pad_token_id = tokenizer.eos_token_id

        filtered_rows, dropped_rows = self._filter_rows_exceeding_max_length(rows, tokenizer)
        if not filtered_rows:
            raise ValueError(
                "All Alpaca fine-tune rows exceed training.max_length after tokenization. "
                "Reduce prompt size or increase training.max_length."
            )
        if dropped_rows and self._is_rank_zero_env():
            LOGGER.warning(
                "Dropped overlength fine-tune rows before train/eval split | dropped=%s | kept=%s | max_length=%s",
                len(dropped_rows),
                len(filtered_rows),
                self.config.training.max_length,
            )
            LOGGER.warning(
                "Example dropped question ids: %s",
                ", ".join(item["question_id"] for item in dropped_rows[:10]),
            )

        train_rows, eval_rows = self._split_rows(filtered_rows)
        train_dataset = datasets.Dataset.from_list(self._build_tokenized_dataset_rows(train_rows, tokenizer))
        eval_dataset = (
            datasets.Dataset.from_list(self._build_tokenized_dataset_rows(eval_rows, tokenizer))
            if eval_rows
            else None
        )

        model_kwargs = {
            "trust_remote_code": self.config.model.trust_remote_code,
        }
        torch_dtype = self._resolve_torch_dtype()
        if torch_dtype is not None:
            model_kwargs["dtype"] = torch_dtype
        if self.config.model.attn_implementation:
            model_kwargs["attn_implementation"] = self.config.model.attn_implementation
        model = transformers.AutoModelForCausalLM.from_pretrained(
            self.config.model.model_name_or_path,
            **model_kwargs,
        )
        if self.config.training.gradient_checkpointing:
            if hasattr(model, "gradient_checkpointing_enable"):
                model.gradient_checkpointing_enable()
            if hasattr(model.config, "use_cache"):
                model.config.use_cache = False

        trainable_params = sum(param.numel() for param in model.parameters() if param.requires_grad)
        total_params = sum(param.numel() for param in model.parameters())
        if self._is_rank_zero_env():
            LOGGER.info(
                "Loaded fine-tune model | name=%s | trainable_params=%s | total_params=%s",
                self.config.model.model_name_or_path,
                trainable_params,
                total_params,
            )

        resolved_warmup_steps = self._resolve_warmup_steps(len(train_rows))
        if self._is_rank_zero_env():
            LOGGER.info("Resolved warmup steps | warmup_steps=%s", resolved_warmup_steps)
        sft_config = self._build_sft_config(
            trl.SFTConfig,
            has_eval=bool(eval_rows),
            warmup_steps=resolved_warmup_steps,
        )
        output_dir = Path(self.config.training.output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)

        trainer = self._build_trainer(
            trl.SFTTrainer,
            model=model,
            sft_config=sft_config,
            train_dataset=train_dataset,
            eval_dataset=eval_dataset,
            tokenizer=tokenizer,
        )
        train_result = trainer.train(
            resume_from_checkpoint=self.config.training.resume_from_checkpoint or None
        )
        metrics = dict(train_result.metrics or {})
        metrics["train_rows"] = len(train_rows)
        metrics["eval_rows"] = len(eval_rows)
        metrics["dropped_overlength_rows"] = len(dropped_rows)
        self._persist_training_artifacts(
            trainer=trainer,
            tokenizer=tokenizer,
            output_dir=output_dir,
            metrics=metrics,
        )
        return metrics if self._is_main_process(trainer) else None

    @staticmethod
    def _build_trainer(
        trainer_cls,
        *,
        model,
        sft_config,
        train_dataset,
        eval_dataset,
        tokenizer,
    ):
        signature = inspect.signature(trainer_cls.__init__)
        kwargs: dict[str, Any] = {
            "model": model,
            "args": sft_config,
            "train_dataset": train_dataset,
            "eval_dataset": eval_dataset,
        }
        if "processing_class" in signature.parameters:
            kwargs["processing_class"] = tokenizer
        elif "tokenizer" in signature.parameters:
            kwargs["tokenizer"] = tokenizer
        return trainer_cls(**kwargs)

    def _split_rows(
        self,
        rows: Sequence[AlpacaFinetuneSample],
    ) -> tuple[list[AlpacaFinetuneSample], list[AlpacaFinetuneSample]]:
        if len(rows) < 2 or self.config.data.eval_ratio <= 0:
            return list(rows), []
        rng = np.random.default_rng(self.config.data.shuffle_seed)
        indices = list(rng.permutation(len(rows)))
        eval_count = max(1, int(round(len(rows) * self.config.data.eval_ratio)))
        eval_count = min(eval_count, len(rows) - 1)
        eval_indices = set(indices[:eval_count])
        train_rows: list[AlpacaFinetuneSample] = []
        eval_rows: list[AlpacaFinetuneSample] = []
        for index, row in enumerate(rows):
            if index in eval_indices:
                eval_rows.append(row)
            else:
                train_rows.append(row)
        return train_rows, eval_rows

    @staticmethod
    def _prompt_from_row(row: AlpacaFinetuneSample) -> str:
        return FinetunePromptRenderer.compose_prompt(row.instruction, row.input_text)

    @staticmethod
    def _completion_from_row(row: AlpacaFinetuneSample) -> str:
        return row.output_text

    def _build_tokenized_dataset_rows(
        self,
        rows: Sequence[AlpacaFinetuneSample],
        tokenizer,
    ) -> list[dict[str, Any]]:
        tokenized_rows: list[dict[str, Any]] = []
        for row in rows:
            prompt = self._prompt_from_row(row)
            completion = self._completion_from_row(row)
            full_text = prompt + completion
            tokenized_rows.append(
                self._tokenize_with_completion_mask(
                    tokenizer=tokenizer,
                    full_text=full_text,
                    completion_start=len(prompt),
                )
            )
        return tokenized_rows

    def _filter_rows_exceeding_max_length(
        self,
        rows: Sequence[AlpacaFinetuneSample],
        tokenizer,
    ) -> tuple[list[AlpacaFinetuneSample], list[dict[str, Any]]]:
        kept_rows: list[AlpacaFinetuneSample] = []
        dropped_rows: list[dict[str, Any]] = []
        max_length = max(int(self.config.training.max_length), 1)
        for index, row in enumerate(rows):
            prompt = self._prompt_from_row(row)
            completion = self._completion_from_row(row)
            payload = self._tokenize_with_completion_mask(
                tokenizer=tokenizer,
                full_text=prompt + completion,
                completion_start=len(prompt),
            )
            token_count = len(payload["input_ids"])
            if token_count > max_length:
                dropped_rows.append(
                    {
                        "index": index,
                        "question_id": self._question_id_for_row(row, index),
                        "token_count": token_count,
                    }
                )
                continue
            kept_rows.append(row)
        return kept_rows, dropped_rows

    @staticmethod
    def _question_id_for_row(row: AlpacaFinetuneSample, index: int) -> str:
        for candidate_attr in ("question_id", "database_id"):
            candidate = getattr(row, candidate_attr, "")
            if isinstance(candidate, str) and candidate.strip():
                return candidate.strip()
        return f"row_{index:05d}"

    @staticmethod
    def _tokenize_with_completion_mask(
        *,
        tokenizer,
        full_text: str,
        completion_start: int,
    ) -> dict[str, Any]:
        if not getattr(tokenizer, "is_fast", False):
            raise ValueError(
                "Completion-only SFT requires a fast tokenizer with offset mappings so completion masks can be built "
                "from a single consistent tokenization pass."
            )
        tokenized = tokenizer(
            full_text,
            add_special_tokens=True,
            return_offsets_mapping=True,
            return_special_tokens_mask=True,
        )
        input_ids = list(tokenized["input_ids"])
        offsets = list(tokenized["offset_mapping"])
        special_tokens_mask = list(tokenized.get("special_tokens_mask", [0] * len(input_ids)))
        completion_mask: list[int] = []
        completion_started = False
        for (start, end), is_special in zip(offsets, special_tokens_mask, strict=False):
            if not is_special and end > completion_start:
                completion_started = True
            completion_mask.append(1 if completion_started else 0)
        if not any(completion_mask):
            raise ValueError(
                "Failed to build a non-empty completion mask from the formatted Alpaca sample. "
                "Check the prompt/completion boundary and tokenizer behavior."
            )
        return {
            "input_ids": input_ids,
            "completion_mask": completion_mask,
        }

    def _persist_training_artifacts(
        self,
        *,
        trainer,
        tokenizer,
        output_dir: Path,
        metrics: dict[str, Any],
    ) -> None:
        self._wait_for_everyone(trainer)
        # DeepSpeed ZeRO-3 and other sharded backends may require collectives during
        # save_model(), so every rank must enter the call even though only the save
        # rank will actually write files.
        trainer.save_model(str(output_dir))
        self._wait_for_everyone(trainer)
        if self._is_main_process(trainer):
            tokenizer.save_pretrained(str(output_dir))
            trainer.save_state()
            self._write_metrics_file(output_dir, metrics)
        self._wait_for_everyone(trainer)

    @staticmethod
    def _write_metrics_file(output_dir: Path, metrics: dict[str, Any]) -> None:
        metrics_path = output_dir / "train_metrics.json"
        metrics_path.write_text(
            json.dumps(stable_jsonify(metrics), ensure_ascii=False, indent=2, sort_keys=True),
            encoding="utf-8",
        )

    @staticmethod
    def _wait_for_everyone(trainer) -> None:
        accelerator = getattr(trainer, "accelerator", None)
        if accelerator is not None and hasattr(accelerator, "wait_for_everyone"):
            accelerator.wait_for_everyone()
            return
        if torch.distributed.is_available() and torch.distributed.is_initialized():
            torch.distributed.barrier()

    @staticmethod
    def _is_main_process(trainer) -> bool:
        if hasattr(trainer, "is_world_process_zero"):
            return bool(trainer.is_world_process_zero())
        accelerator = getattr(trainer, "accelerator", None)
        if accelerator is not None and hasattr(accelerator, "is_main_process"):
            return bool(accelerator.is_main_process)
        return True

    @staticmethod
    def _is_rank_zero_env() -> bool:
        rank = os.environ.get("RANK")
        local_rank = os.environ.get("LOCAL_RANK")
        return rank in (None, "", "0") and local_rank in (None, "", "0")

    def _build_sft_config(self, sft_config_cls, *, has_eval: bool, warmup_steps: int):
        signature = inspect.signature(sft_config_cls.__init__)
        kwargs: dict[str, Any] = {}

        def maybe_set(name: str, value: Any) -> None:
            if name in signature.parameters:
                kwargs[name] = value

        report_to = self.config.training.report_to
        if report_to.strip().lower() == "none":
            report_value: Any = []
        else:
            report_value = [item.strip() for item in report_to.split(",") if item.strip()]

        maybe_set("output_dir", self.config.training.output_dir)
        maybe_set("overwrite_output_dir", self.config.training.overwrite_output_dir)
        maybe_set("per_device_train_batch_size", self.config.training.per_device_train_batch_size)
        maybe_set("per_device_eval_batch_size", self.config.training.per_device_eval_batch_size)
        maybe_set("gradient_accumulation_steps", self._resolve_gradient_accumulation_steps())
        maybe_set("learning_rate", self.config.training.learning_rate)
        maybe_set("num_train_epochs", self.config.training.num_train_epochs)
        maybe_set("max_steps", self.config.training.max_steps)
        maybe_set("weight_decay", self.config.training.weight_decay)
        maybe_set("warmup_steps", warmup_steps)
        maybe_set("lr_scheduler_type", self.config.training.lr_scheduler_type)
        maybe_set("logging_steps", self.config.training.logging_steps)
        maybe_set("save_steps", self.config.training.save_steps)
        maybe_set("save_total_limit", self.config.training.save_total_limit)
        maybe_set("max_length", self.config.training.max_length)
        maybe_set("max_seq_length", self.config.training.max_length)
        maybe_set("packing", self.config.training.packing)
        maybe_set("completion_only_loss", self.config.training.completion_only_loss)
        maybe_set("gradient_checkpointing", self.config.training.gradient_checkpointing)
        maybe_set("bf16", self.config.training.bf16)
        maybe_set("fp16", self.config.training.fp16)
        maybe_set("dataloader_num_workers", self.config.training.dataloader_num_workers)
        maybe_set("report_to", report_value)
        maybe_set("seed", self.config.training.seed)
        maybe_set("remove_unused_columns", False)
        if self.config.training.deepspeed_config_path:
            maybe_set("deepspeed", self.config.training.deepspeed_config_path)

        strategy_key = "eval_strategy" if "eval_strategy" in signature.parameters else "evaluation_strategy"
        if has_eval:
            maybe_set(strategy_key, "steps")
            maybe_set("eval_steps", self.config.training.eval_steps)
        else:
            maybe_set(strategy_key, "no")
        maybe_set("save_strategy", "steps")
        maybe_set("logging_strategy", "steps")
        return sft_config_cls(**kwargs)

    def _resolve_torch_dtype(self):
        dtype_name = self.config.model.torch_dtype.strip().lower()
        if not dtype_name or dtype_name == "auto":
            return None

        mapping = {
            "bfloat16": torch.bfloat16,
            "bf16": torch.bfloat16,
            "float16": torch.float16,
            "fp16": torch.float16,
            "float32": torch.float32,
            "fp32": torch.float32,
        }
        if dtype_name not in mapping:
            raise ValueError(f"Unsupported torch_dtype: {self.config.model.torch_dtype}")
        return mapping[dtype_name]

    def _resolve_warmup_steps(self, train_row_count: int) -> int:
        explicit_steps = int(self.config.training.warmup_steps)
        if explicit_steps > 0:
            return explicit_steps
        ratio = float(self.config.training.warmup_ratio)
        if ratio <= 0:
            return 0
        total_steps = self._estimate_total_training_steps(train_row_count)
        return max(0, int(round(total_steps * ratio)))

    def _estimate_total_training_steps(self, train_row_count: int) -> int:
        if self.config.training.max_steps > 0:
            return int(self.config.training.max_steps)
        world_size = self._distributed_world_size()
        per_device_batch_size = max(int(self.config.training.per_device_train_batch_size), 1)
        grad_accum = self._resolve_gradient_accumulation_steps()
        per_step_examples = per_device_batch_size * world_size
        dataloader_steps = max(1, math.ceil(train_row_count / per_step_examples))
        update_steps_per_epoch = max(1, math.ceil(dataloader_steps / grad_accum))
        return max(1, math.ceil(update_steps_per_epoch * float(self.config.training.num_train_epochs)))

    def _resolve_gradient_accumulation_steps(self) -> int:
        deepspeed_path = str(self.config.training.deepspeed_config_path or "").strip()
        if deepspeed_path:
            try:
                payload = json.loads(Path(deepspeed_path).read_text(encoding="utf-8"))
            except (OSError, json.JSONDecodeError) as exc:
                raise ValueError(f"Failed to read DeepSpeed config: {deepspeed_path}") from exc
            deepspeed_value = payload.get("gradient_accumulation_steps")
            if isinstance(deepspeed_value, int) and deepspeed_value > 0:
                return deepspeed_value
            if isinstance(deepspeed_value, str) and deepspeed_value.strip().isdigit():
                return max(int(deepspeed_value.strip()), 1)
        return max(int(self.config.training.gradient_accumulation_steps), 1)

    @staticmethod
    def _distributed_world_size() -> int:
        world_size = os.environ.get("WORLD_SIZE")
        if not world_size:
            return 1
        try:
            return max(int(world_size), 1)
        except ValueError:
            return 1
