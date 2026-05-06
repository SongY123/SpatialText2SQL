"""Dataset preparation for TRL spatial Text-to-SQL fine-tuning."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any, Mapping, Sequence

from src.synthesis.database.utils import stable_jsonify, to_text
from src.synthesis.sql.prompt_metadata import PostGISPromptMetadataProvider

from .config import FinetuneDBConfig, FinetuneDataConfig
from .models import PreparedFinetuneSample, RawFinetuneSample
from .prompting import FinetunePromptRenderer

LOGGER = logging.getLogger(__name__)


@dataclass(frozen=True)
class _MetadataLookupRequest:
    database_id: str
    city: str
    selected_table_names: list[str]
    selected_tables: list[Any]


class SpatialText2SQLDatasetBuilder:
    def __init__(
        self,
        *,
        db_config: FinetuneDBConfig,
        data_config: FinetuneDataConfig,
        metadata_provider: PostGISPromptMetadataProvider | None = None,
        prompt_renderer: FinetunePromptRenderer | None = None,
    ) -> None:
        self.db_config = db_config
        self.data_config = data_config
        self.metadata_provider = metadata_provider or PostGISPromptMetadataProvider(self._to_sql_db_config())
        self.prompt_renderer = prompt_renderer or FinetunePromptRenderer(
            template_path=self.data_config.prompt_template_path,
            task_description=self.data_config.task_description,
            max_representative_rows=self.data_config.max_representative_rows,
        )

    def prepare_samples(self, rows: Sequence[RawFinetuneSample]) -> list[PreparedFinetuneSample]:
        prepared_rows: list[PreparedFinetuneSample] = []
        next_question_id = self.data_config.question_id_start
        for row in rows:
            metadata = self._load_metadata(row)
            schema_lines, spatial_lines, representative_values = FinetunePromptRenderer.build_runtime_prompt_context(
                metadata,
                included_tables=row.used_tables,
                max_representative_rows=self.data_config.max_representative_rows,
            )
            prompt = self.prompt_renderer.render_prompt(
                question=row.question,
                schema_lines=schema_lines,
                spatial_lines=spatial_lines,
                representative_values=representative_values,
            )
            cot = self._build_synthetic_cot(row)
            completion = self.prompt_renderer.render_completion(cot, row.sql)
            prepared_rows.append(
                PreparedFinetuneSample(
                    question_id=next_question_id,
                    database_id=row.database_id,
                    question=row.question,
                    sql=row.sql,
                    difficulty=row.difficulty,
                    prompt=prompt,
                    completion=completion,
                    cot=cot,
                    schema=list(schema_lines),
                    spatial_field_metadata=list(spatial_lines),
                    representative_values=stable_jsonify(representative_values),
                    used_tables=list(row.used_tables),
                    used_columns=list(row.used_columns),
                    used_spatial_functions=list(row.used_spatial_functions),
                )
            )
            next_question_id += 1
        return prepared_rows

    def _load_metadata(self, row: RawFinetuneSample) -> dict[str, Any] | None:
        embedded_metadata = self._load_embedded_metadata(row)
        if embedded_metadata is not None:
            return embedded_metadata

        table_names = [to_text(name) for name in row.used_tables if to_text(name)]
        if not table_names:
            LOGGER.warning(
                "Fine-tune sample %s has no used_tables; schema prompt context will be empty.",
                row.question_id or row.database_id,
            )
            return None
        lookup = _MetadataLookupRequest(
            database_id=row.database_id,
            city=row.city,
            selected_table_names=table_names,
            selected_tables=[],
        )
        return self.metadata_provider.load_database_metadata(lookup)  # type: ignore[arg-type]

    @staticmethod
    def _load_embedded_metadata(row: RawFinetuneSample) -> dict[str, Any] | None:
        metadata = row.metadata if isinstance(row.metadata, Mapping) else {}
        database_context = metadata.get("database_context")
        if (
            isinstance(database_context, Mapping)
            and isinstance(database_context.get("tables"), Sequence)
            and not isinstance(database_context.get("tables"), (str, bytes))
        ):
            return {str(key): stable_jsonify(value) for key, value in database_context.items()}
        if isinstance(metadata.get("tables"), Sequence) and not isinstance(metadata.get("tables"), (str, bytes)):
            return {str(key): stable_jsonify(value) for key, value in metadata.items()}
        return None

    def _build_synthetic_cot(self, row: RawFinetuneSample) -> str:
        steps: list[str] = []
        tables = ", ".join(row.used_tables) if row.used_tables else "the relevant schema tables"
        steps.append(f"Identify {tables} as the tables needed for this {row.difficulty} spatial question.")

        if row.used_spatial_functions:
            function_text = ", ".join(row.used_spatial_functions)
            steps.append(f"Use {function_text} to express the required spatial relation, distance check, or geometry operation.")
        else:
            steps.append("Match the question against the available spatial columns and preserve the required spatial semantics.")

        feature_constraints = self._summarize_sql_features(row.sql_features)
        if feature_constraints:
            steps.append(feature_constraints)

        steps.append("Return one executable PostgreSQL/PostGIS SQL query that answers the question exactly.")
        return "\n".join(f"{index + 1}. {step}" for index, step in enumerate(steps))

    @staticmethod
    def _summarize_sql_features(sql_features: Mapping[str, Any]) -> str:
        if not sql_features:
            return "Keep the SQL filters, ranking, and output columns aligned with the question wording."
        parts: list[str] = []
        aggregates = [to_text(item) for item in sql_features.get("aggregates", []) if to_text(item)]
        if aggregates:
            parts.append(f"preserve the aggregation logic ({', '.join(aggregates)})")
        group_by_columns = [to_text(item) for item in sql_features.get("group_by_columns", []) if to_text(item)]
        if group_by_columns:
            parts.append(f"group by {', '.join(group_by_columns)} when needed")
        order_by = sql_features.get("order_by", [])
        if isinstance(order_by, list) and order_by:
            order_parts = []
            for item in order_by:
                if not isinstance(item, Mapping):
                    continue
                column_name = to_text(item.get("column") or item.get("expression"))
                direction = to_text(item.get("direction")).upper() or "ASC"
                if column_name:
                    order_parts.append(f"{column_name} {direction}")
            if order_parts:
                parts.append(f"respect the ranking/order ({', '.join(order_parts)})")
        limit = sql_features.get("limit")
        if limit not in (None, ""):
            parts.append(f"keep the top-k/limit constraint at {limit}")
        if sql_features.get("has_cte") or sql_features.get("has_subquery"):
            parts.append("preserve the nested query structure where it is required")
        filters = [to_text(item) for item in sql_features.get("filters", []) if to_text(item)]
        if filters:
            parts.append("keep the SQL filters aligned with the question entities and constraints")
        if not parts:
            return "Keep the SQL filters, ranking, and output columns aligned with the question wording."
        return "Preserve the query structure: " + "; ".join(parts) + "."

    def _to_sql_db_config(self):
        from src.synthesis.sql.config import SQLSynthesisDBConfig

        return SQLSynthesisDBConfig(
            host=self.db_config.host,
            port=self.db_config.port,
            database=self.db_config.database,
            user=self.db_config.user,
            password=self.db_config.password,
            search_path=self.db_config.search_path,
            connect_timeout=self.db_config.connect_timeout,
            statement_timeout=self.db_config.statement_timeout,
        )
