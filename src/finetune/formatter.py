"""Formatter for converting nl2sql rows into Alpaca-style fine-tune data."""

from __future__ import annotations

import logging
from typing import Any, Mapping, Sequence

from .config import FinetuneDataConfig
from .models import AlpacaFinetuneSample, RawFinetuneSample
from .prompting import FinetunePromptRenderer
from .utils import stable_jsonify

LOGGER = logging.getLogger(__name__)


class NL2SQLAlpacaFormatter:
    def __init__(
        self,
        *,
        data_config: FinetuneDataConfig,
        prompt_renderer: FinetunePromptRenderer | None = None,
    ) -> None:
        self.data_config = data_config
        self.prompt_renderer = prompt_renderer or FinetunePromptRenderer(
            task_description=self.data_config.task_description,
            max_representative_rows=self.data_config.max_representative_rows,
        )

    def format_samples(self, rows: Sequence[RawFinetuneSample]) -> list[AlpacaFinetuneSample]:
        formatted_rows: list[AlpacaFinetuneSample] = []
        for row in rows:
            if row.instruction and row.input_text and row.output_text:
                formatted_rows.append(
                    AlpacaFinetuneSample(
                        instruction=row.instruction,
                        input_text=row.input_text,
                        output_text=row.output_text,
                    )
                )
                continue
            metadata = self._load_embedded_metadata(row)
            schema_lines, representative_values = FinetunePromptRenderer.build_runtime_prompt_context(
                metadata,
                max_representative_rows=self.data_config.max_representative_rows,
            )
            instruction = self.prompt_renderer.render_instruction()
            input_text = self.prompt_renderer.render_input(
                database_id=row.database_id,
                question=row.question,
                schema_lines=schema_lines,
                representative_values=representative_values,
            )
            output_text = self.prompt_renderer.render_output(row.sql_reasoning_summary, row.sql)
            formatted_rows.append(
                AlpacaFinetuneSample(
                    instruction=instruction,
                    input_text=input_text,
                    output_text=output_text,
                )
            )
        return formatted_rows

    @staticmethod
    def _load_embedded_metadata(row: RawFinetuneSample) -> dict[str, Any] | None:
        metadata = row.metadata if isinstance(row.metadata, Mapping) else {}
        database_context = metadata.get("database_context")
        if isinstance(database_context, Mapping):
            if database_context.get("schema_ddls") or isinstance(database_context.get("representative_values"), Mapping):
                return {str(key): stable_jsonify(value) for key, value in database_context.items()}
            tables = database_context.get("tables")
            if isinstance(tables, Sequence) and not isinstance(tables, (str, bytes, bytearray)):
                return {str(key): stable_jsonify(value) for key, value in database_context.items()}
        tables = metadata.get("tables")
        if isinstance(tables, Sequence) and not isinstance(tables, (str, bytes, bytearray)):
            return {str(key): stable_jsonify(value) for key, value in metadata.items()}
        LOGGER.warning(
            "Fine-tune sample %s is missing embedded metadata.database_context; instruction/input schema context will be empty.",
            row.question_id or row.database_id,
        )
        return None
