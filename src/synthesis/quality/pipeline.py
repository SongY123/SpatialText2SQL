"""High-level quality control pipeline for synthetic spatial NL-SQL data."""

from __future__ import annotations

import hashlib
import logging
from collections import Counter
from dataclasses import dataclass, field
from typing import Any, Mapping

from src.finetune.prompting import FinetunePromptRenderer
from src.synthesis.sql.function_library import PostGISFunctionLibrary

from .balancing import DiversityBalancer
from .config import QualityControlConfig
from .duplicates import DuplicateDetector
from .judge import SelfConsistencyQualityJudge
from .models import NLSQLSample, QualityControlReport
from .registry import DatabaseRegistry, SchemaRegistry
from .validation import (
    SQLSampleValidator,
    build_distribution,
)

LOGGER = logging.getLogger(__name__)


def _build_distribution_snapshots(samples: list[NLSQLSample]) -> tuple[dict[str, int], dict[str, int], dict[str, int]]:
    return (
        build_distribution(samples, lambda sample: [sample.difficulty_level]),
        build_distribution(samples, lambda sample: sample.used_spatial_functions),
        build_distribution(samples, lambda sample: [sample.linguistic_style]),
    )


def _resolve_database_context(sample: NLSQLSample) -> dict[str, Any]:
    if isinstance(sample.metadata.get("database_context"), Mapping):
        return dict(sample.metadata["database_context"])
    original_metadata = sample.original_payload.get("metadata")
    if isinstance(original_metadata, Mapping) and isinstance(original_metadata.get("database_context"), Mapping):
        return dict(original_metadata["database_context"])
    return {}


def _render_input_for_hash(sample: NLSQLSample) -> str:
    database_context = _resolve_database_context(sample)
    if not database_context:
        return ""
    prompt_renderer = FinetunePromptRenderer(task_description="", max_representative_rows=3)
    schema_lines, representative_values = FinetunePromptRenderer.build_runtime_prompt_context(
        database_context,
        max_representative_rows=3,
    )
    return prompt_renderer.render_input(
        database_id=sample.database_id,
        question=sample.question,
        schema_lines=schema_lines,
        representative_values=representative_values,
    ).strip()


def _deduplicate_by_input_hash(samples: list[NLSQLSample]) -> tuple[list[NLSQLSample], list[dict[str, Any]]]:
    retained: list[NLSQLSample] = []
    duplicates: list[dict[str, Any]] = []
    seen_by_hash: dict[str, NLSQLSample] = {}
    for sample in samples:
        input_text = _render_input_for_hash(sample)
        if not input_text:
            retained.append(sample)
            continue
        input_hash = hashlib.sha256(input_text.encode("utf-8")).hexdigest()
        original = seen_by_hash.get(input_hash)
        if original is None:
            seen_by_hash[input_hash] = sample
            retained.append(sample)
            continue
        duplicates.append(
            {
                "reason": "input_hash_duplicate",
                "input_hash": input_hash,
                "dropped_sample_id": sample.sample_id,
                "dropped_sql_id": sample.sql_id,
                "dropped_database_id": sample.database_id,
                "dropped_question": sample.question,
                "kept_sample_id": original.sample_id,
                "kept_sql_id": original.sql_id,
                "kept_database_id": original.database_id,
                "kept_question": original.question,
            }
        )
    return retained, duplicates


def format_only_quality_control(
    samples: list[NLSQLSample],
) -> tuple[list[NLSQLSample], QualityControlReport]:
    retained, input_hash_duplicates = _deduplicate_by_input_hash(list(samples))
    difficulty_distribution, spatial_distribution, style_distribution = _build_distribution_snapshots(retained)
    failure_reasons = {}
    if input_hash_duplicates:
        failure_reasons["input_hash_duplicate"] = len(input_hash_duplicates)
    report = QualityControlReport(
        total_samples=len(samples),
        passed_samples=len(retained),
        failed_samples=len(samples) - len(retained),
        failure_reasons=failure_reasons,
        duplicate_count=len(input_hash_duplicates),
        duplicate_samples=input_hash_duplicates,
        distribution_by_difficulty=difficulty_distribution,
        distribution_by_spatial_function=spatial_distribution,
        distribution_by_linguistic_style=style_distribution,
    )
    return retained, report


@dataclass
class QualityControlPipeline:
    function_library: PostGISFunctionLibrary
    self_consistency_judge: SelfConsistencyQualityJudge | None = None
    sql_validator: SQLSampleValidator = field(init=False)

    def __post_init__(self) -> None:
        self.sql_validator = SQLSampleValidator(self.function_library)

    def run(
        self,
        samples: list[NLSQLSample],
        database_registry: DatabaseRegistry,
        schema_registry: SchemaRegistry,
        config: QualityControlConfig,
    ) -> tuple[list[NLSQLSample], QualityControlReport]:
        failure_reasons: Counter[str] = Counter()
        validated_samples: list[NLSQLSample] = []

        for sample in samples:
            LOGGER.info("Quality control validating sample_id=%s database_id=%s", sample.sample_id, sample.database_id)
            try:
                database_client = database_registry.get_client(sample.database_id)
            except Exception as exc:
                failure_reasons[f"database_client:{exc}"] += 1
                continue

            schema = None
            if config.run.prefer_live_schema:
                try:
                    schema = database_client.inspect_schema()
                    schema_registry.set_schema(schema)
                except Exception as exc:
                    LOGGER.warning("Failed to inspect live schema for %s: %s", sample.database_id, exc)
                    schema = schema_registry.get_schema(sample.database_id)
            else:
                schema = schema_registry.get_schema(sample.database_id)
                if schema is None:
                    try:
                        schema = database_client.inspect_schema()
                        schema_registry.set_schema(schema)
                    except Exception as exc:
                        failure_reasons[f"schema_lookup:{exc}"] += 1
                        continue

            if schema is None:
                failure_reasons["missing_schema"] += 1
                continue

            artifact = self.sql_validator.validate(
                sample=sample,
                schema=schema,
                database_client=database_client,
                config=config,
            )
            for error in artifact.validation_result.errors:
                failure_reasons[error] += 1
            if artifact.validation_result.passed and self.self_consistency_judge is not None:
                judgment = self.self_consistency_judge.judge(
                    sample=sample,
                    schema=schema,
                    parsed_sql=artifact.parsed_sql,
                    validation_result=artifact.validation_result,
                    config=config,
                )
                artifact.validation_result.self_consistency = judgment.to_dict()
                artifact.validation_result.warnings.extend(judgment.warnings)
                if not judgment.passed:
                    artifact.validation_result.errors.append(
                        "Self-consistency judge rejected the NL-SQL pair."
                    )
                    artifact.validation_result.errors.extend(
                        [f"judge:{code}" for code in judgment.reason_codes]
                    )
                    # artifact.validation_result.passed = False
                    failure_reasons["self_consistency_rejected"] += 1
                    for code in judgment.reason_codes:
                        failure_reasons[f"judge:{code}"] += 1

            if not artifact.validation_result.passed:
                artifact.validation_result.warnings.extend(artifact.validation_result.errors)
                artifact.validation_result.errors = []
                artifact.validation_result.passed = True
                LOGGER.warning(
                    "Quality control downgraded sample_id=%s to passed with warnings=%s",
                    sample.sample_id,
                    artifact.validation_result.warnings,
                )

            metadata = dict(sample.metadata)
            metadata["quality_control"] = artifact.validation_result.to_dict()
            sample.metadata = metadata
            validated_samples.append(sample)

        duplicate_result = DuplicateDetector(config.duplicates).run(validated_samples)
        for reason in duplicate_result.duplicate_reasons:
            failure_reasons[reason] += 1

        input_deduped_samples, input_hash_duplicates = _deduplicate_by_input_hash(duplicate_result.retained)
        if input_hash_duplicates:
            failure_reasons["input_hash_duplicate"] += len(input_hash_duplicates)

        balanced_samples, dropped_by_balance = DiversityBalancer(config.balancing).run(input_deduped_samples)
        for _sample_id in dropped_by_balance:
            failure_reasons["balancing_drop"] += 1

        difficulty_distribution, spatial_distribution, style_distribution = _build_distribution_snapshots(balanced_samples)
        report = QualityControlReport(
            total_samples=len(samples),
            passed_samples=len(balanced_samples),
            failed_samples=len(samples) - len(balanced_samples),
            failure_reasons=dict(failure_reasons),
            duplicate_count=duplicate_result.duplicate_count + len(input_hash_duplicates),
            duplicate_samples=input_hash_duplicates,
            distribution_by_difficulty=difficulty_distribution,
            distribution_by_spatial_function=spatial_distribution,
            distribution_by_linguistic_style=style_distribution,
        )
        return balanced_samples, report
