"""Question synthesis entrypoint for executable spatial SQL."""

from __future__ import annotations

import logging
import re
import time
from typing import Any, Callable, Mapping, Sequence

import numpy as np

from src.prompting.prompt_builder import PromptBuilder
from src.synthesis.database.utils import stable_jsonify

from .config import QuestionGenerationConfig
from .features import SQLFeatureExtractor
from .generator import QuestionLLMClient
from .models import (
    QuestionGenerationCandidate,
    QuestionGenerationContext,
    SQLQuestionSource,
    SynthesizedQuestion,
)
from .parser import parse_question_generation_response
from .style import STYLE_DESCRIPTIONS, SpatialPhraseSelector, StyleSelector
from .validator import QuestionValidationResult, QuestionValidator

LOGGER = logging.getLogger(__name__)


class DiversityAwareQuestionSynthesizer:
    def __init__(
        self,
        *,
        config: QuestionGenerationConfig,
        llm_client: QuestionLLMClient,
        prompt_builder: PromptBuilder,
        feature_extractor: SQLFeatureExtractor | None = None,
        style_selector: StyleSelector | None = None,
        spatial_phrase_selector: SpatialPhraseSelector | None = None,
        validator: QuestionValidator | None = None,
        existing_question_id_offsets: Mapping[str, int] | None = None,
    ) -> None:
        self.config = config
        self.llm_client = llm_client
        self.prompt_builder = prompt_builder
        self.feature_extractor = feature_extractor or SQLFeatureExtractor()
        self.style_selector = style_selector or StyleSelector()
        self.spatial_phrase_selector = spatial_phrase_selector or SpatialPhraseSelector()
        self.validator = validator or QuestionValidator()
        self.rng = np.random.default_rng(self.config.generation.random_seed)
        self._question_id_offsets: dict[str, int] = dict(existing_question_id_offsets or {})

    def run(
        self,
        sql_queries: Sequence[SQLQuestionSource],
        context_by_database_id: Mapping[str, QuestionGenerationContext],
        on_row_generated: Callable[[SynthesizedQuestion], None] | None = None,
    ) -> list[SynthesizedQuestion]:
        rows: list[SynthesizedQuestion] = []
        for sql_query in sql_queries:
            context = self._resolve_context_for_sql(sql_query, context_by_database_id)
            if context is None:
                LOGGER.warning(
                    "Skipping sql_id=%s because database context %s is missing.",
                    sql_query.sql_id,
                    sql_query.database_id,
                )
                continue
            rows.extend(self.run_for_sql(sql_query, context, on_row_generated=on_row_generated))
        return rows

    def run_for_sql(
        self,
        sql_query: SQLQuestionSource,
        context: QuestionGenerationContext,
        on_row_generated: Callable[[SynthesizedQuestion], None] | None = None,
    ) -> list[SynthesizedQuestion]:
        features = self.feature_extractor.extract(sql_query.sql)
        style_plan = self.style_selector.build_style_plan(
            features=features,
            total_questions=self.config.generation.num_questions_per_sql,
            rng=self.rng,
            fixed_style=self.config.generation.fixed_style,
            style_weights=self.config.generation.style_weights,
        )
        LOGGER.info(
            "Question synthesis plan | sql_id=%s | database_id=%s | styles=%s",
            sql_query.sql_id,
            sql_query.database_id,
            style_plan,
        )
        rows: list[SynthesizedQuestion] = []
        for question_index, style in enumerate(style_plan):
            spatial_constraints = self.spatial_phrase_selector.build_constraints(
                features=features,
                rng=self.rng,
            )
            row = self._run_single_question(
                sql_query=sql_query,
                context=context,
                question_index=question_index,
                style=style,
                features=features,
                spatial_constraints=spatial_constraints,
            )
            if row is None:
                continue
            rows.append(row)
            if on_row_generated is not None:
                on_row_generated(row)
        return rows

    def _next_question_id(self, database_id: str) -> str:
        next_value = self._question_id_offsets.get(database_id, 0) + 1
        self._question_id_offsets[database_id] = next_value
        return f"{database_id}_{next_value:04d}"

    def _run_single_question(
        self,
        *,
        sql_query: SQLQuestionSource,
        context: QuestionGenerationContext,
        question_index: int,
        style: str,
        features,
        spatial_constraints,
    ) -> SynthesizedQuestion | None:
        sample_tag = f"{sql_query.sql_id}/q_{question_index + 1:03d}"
        prompt_build_start = time.perf_counter()
        prompt = self.prompt_builder.build_question_generation_prompt(
            sql_query=sql_query,
            database_context=context.to_prompt_payload(),
            sql_features=features.to_dict(),
            style_constraint={
                "style": style,
                "description": STYLE_DESCRIPTIONS.get(style, ""),
            },
            spatial_relation_constraints=[item.to_dict() for item in spatial_constraints],
        )
        prompt_build_ms = (time.perf_counter() - prompt_build_start) * 1000.0
        LOGGER.info(
            "Question prompt built | sample=%s | style=%s | prompt_chars=%s | build_time_ms=%.1f",
            sample_tag,
            style,
            len(prompt),
            prompt_build_ms,
        )
        feedback_prompts: list[str] = []
        generation_rounds: list[dict[str, Any]] = []
        candidate = QuestionGenerationCandidate(question="")
        validation_result = QuestionValidationResult(is_valid=False, errors=["Question generation did not start."])
        current_prompt = prompt
        max_revision_rounds = max(int(getattr(self.config.generation, "max_revision_rounds", 1)), 1)

        LOGGER.info(
            "Question LLM prompt | sample=%s | round=%s/%s\n%s",
            sample_tag,
            1,
            max_revision_rounds,
            current_prompt,
        )
        LOGGER.info(
            "Question LLM request start | sample=%s | round=%s/%s | style=%s | prompt_chars=%s",
            sample_tag,
            1,
            max_revision_rounds,
            style,
            len(current_prompt),
        )
        generation_start = time.perf_counter()
        response = self.llm_client.generate(current_prompt)
        generation_ms = (time.perf_counter() - generation_start) * 1000.0
        LOGGER.info(
            "Question LLM request done | sample=%s | round=%s/%s | attempts=%s | response_chars=%s | time_ms=%.1f",
            sample_tag,
            1,
            max_revision_rounds,
            response.attempts,
            len(response.text or ""),
            generation_ms,
        )
        candidate = parse_question_generation_response(
            response.text,
            raw_response=response.raw_response,
        )
        if self._should_force_reasoning_fallback(candidate, response):
            LOGGER.warning(
                "Question candidate appears truncated; forcing SQL reasoning fallback | sample=%s | parse_error=%s",
                sample_tag,
                candidate.parse_error,
            )
            candidate = QuestionGenerationCandidate(
                question="",
                style=candidate.style,
                reasoning_summary=candidate.reasoning_summary,
                spatial_phrases=list(candidate.spatial_phrases),
                raw_response_text=candidate.raw_response_text,
                raw_response=candidate.raw_response,
                parse_error=candidate.parse_error or "Question-generation response appears truncated.",
            )
        generation_rounds.append(
            {
                "round": 0,
                "prompt_type": "initial",
                "raw_response_text": candidate.raw_response_text,
                "parse_error": candidate.parse_error,
                "usage": stable_jsonify(response.usage),
                "attempts": response.attempts,
            }
        )
        if candidate.parse_error:
            LOGGER.warning(
                "Question candidate parse degraded | sample=%s | round=%s/%s | error=%s | raw_preview=%s",
                sample_tag,
                1,
                max_revision_rounds,
                candidate.parse_error,
                (candidate.raw_response_text or "")[:400],
            )
        if candidate.question.strip():
            LOGGER.info(
                "Generated question | sample=%s | round=%s/%s\n%s",
                sample_tag,
                1,
                max_revision_rounds,
                candidate.question,
            )
            validation_result = self._validate_candidate(
                candidate=candidate,
                requested_style=style,
                features=features,
                spatial_constraints=spatial_constraints,
            )
            LOGGER.info(
                "Question validation done | sample=%s | round=%s/%s | is_valid=%s | errors=%s | warnings=%s",
                sample_tag,
                1,
                max_revision_rounds,
                validation_result.is_valid,
                len(validation_result.errors),
                len(validation_result.warnings),
            )
            revision_round = 1
            while candidate.question.strip() and not validation_result.is_valid and revision_round < max_revision_rounds:
                revision_feedback = self._build_question_revision_feedback(validation_result)
                current_prompt = self.prompt_builder.build_question_revision_prompt(
                    sql_query=sql_query,
                    database_context=context.to_prompt_payload(),
                    sql_features=features.to_dict(),
                    current_question=candidate.question,
                    style_constraint={
                        "style": style,
                        "description": STYLE_DESCRIPTIONS.get(style, ""),
                    },
                    spatial_relation_constraints=[item.to_dict() for item in spatial_constraints],
                    revision_feedback=revision_feedback,
                )
                feedback_prompts.append(current_prompt)
                LOGGER.info(
                    "Question revision prompt built | sample=%s | round=%s/%s | prompt_chars=%s",
                    sample_tag,
                    revision_round + 1,
                    max_revision_rounds,
                    len(current_prompt),
                )
                LOGGER.info(
                    "Question LLM prompt | sample=%s | round=%s/%s\n%s",
                    sample_tag,
                    revision_round + 1,
                    max_revision_rounds,
                    current_prompt,
                )
                LOGGER.info(
                    "Question LLM request start | sample=%s | round=%s/%s | style=%s | prompt_chars=%s",
                    sample_tag,
                    revision_round + 1,
                    max_revision_rounds,
                    style,
                    len(current_prompt),
                )
                generation_start = time.perf_counter()
                response = self.llm_client.generate(current_prompt)
                generation_ms = (time.perf_counter() - generation_start) * 1000.0
                LOGGER.info(
                    "Question LLM request done | sample=%s | round=%s/%s | attempts=%s | response_chars=%s | time_ms=%.1f",
                    sample_tag,
                    revision_round + 1,
                    max_revision_rounds,
                    response.attempts,
                    len(response.text or ""),
                    generation_ms,
                )
                candidate = parse_question_generation_response(
                    response.text,
                    raw_response=response.raw_response,
                )
                if self._should_force_reasoning_fallback(candidate, response):
                    LOGGER.warning(
                        "Question revision appears truncated; forcing SQL reasoning fallback | sample=%s | parse_error=%s",
                        sample_tag,
                        candidate.parse_error,
                    )
                    candidate = QuestionGenerationCandidate(
                        question="",
                        style=candidate.style,
                        reasoning_summary=candidate.reasoning_summary,
                        spatial_phrases=list(candidate.spatial_phrases),
                        raw_response_text=candidate.raw_response_text,
                        raw_response=candidate.raw_response,
                        parse_error=candidate.parse_error or "Question-revision response appears truncated.",
                    )
                generation_rounds.append(
                    {
                        "round": revision_round,
                        "prompt_type": "revision",
                        "raw_response_text": candidate.raw_response_text,
                        "parse_error": candidate.parse_error,
                        "usage": stable_jsonify(response.usage),
                        "attempts": response.attempts,
                    }
                )
                if candidate.parse_error:
                    LOGGER.warning(
                        "Question revision parse degraded | sample=%s | round=%s/%s | error=%s | raw_preview=%s",
                        sample_tag,
                        revision_round + 1,
                        max_revision_rounds,
                        candidate.parse_error,
                        (candidate.raw_response_text or "")[:400],
                    )
                if not candidate.question.strip():
                    break
                LOGGER.info(
                    "Revised question | sample=%s | round=%s/%s\n%s",
                    sample_tag,
                    revision_round + 1,
                    max_revision_rounds,
                    candidate.question,
                )
                validation_result = self._validate_candidate(
                    candidate=candidate,
                    requested_style=style,
                    features=features,
                    spatial_constraints=spatial_constraints,
                )
                LOGGER.info(
                    "Question validation done | sample=%s | round=%s/%s | is_valid=%s | errors=%s | warnings=%s",
                    sample_tag,
                    revision_round + 1,
                    max_revision_rounds,
                    validation_result.is_valid,
                    len(validation_result.errors),
                    len(validation_result.warnings),
                )
                revision_round += 1
        else:
            fallback_question = self._build_fallback_question(
                sql_query=sql_query,
                style=style,
                sql_features=features.to_dict(),
            )
            if fallback_question:
                candidate = QuestionGenerationCandidate(
                    question=fallback_question,
                    style=style,
                    reasoning_summary="Recovered from SQL reasoning summary because the model response was empty or unreadable.",
                    spatial_phrases=[],
                    raw_response_text=candidate.raw_response_text,
                    raw_response=candidate.raw_response,
                    parse_error=candidate.parse_error or "Recovered from SQL reasoning summary fallback.",
                )
                LOGGER.warning(
                    "Recovered question from SQL reasoning summary fallback | sample=%s | question=%s",
                    sample_tag,
                    fallback_question,
                )
            validation_result = QuestionValidationResult(
                is_valid=bool(fallback_question),
                errors=[] if fallback_question else [candidate.parse_error or "Question-generation response did not contain a recoverable question."],
                warnings=[candidate.parse_error or "Recovered from SQL reasoning summary fallback."] if fallback_question else [],
            )

        if not candidate.question.strip():
            LOGGER.warning(
                "Discarding question sample with unrecoverable empty question | sample=%s | parse_error=%s",
                sample_tag,
                candidate.parse_error or "",
            )
            return None

        synthesized = SynthesizedQuestion(
            question_id=self._next_question_id(sql_query.database_id),
            sql_id=sql_query.sql_id,
            database_id=sql_query.database_id,
            city=sql_query.city,
            style=style,
            question=candidate.question,
            sql=sql_query.sql,
            reasoning_summary=candidate.reasoning_summary,
            sql_reasoning_summary=sql_query.reasoning_summary,
            spatial_phrases=list(candidate.spatial_phrases),
            source_difficulty_level=sql_query.difficulty_level,
            used_tables=list(sql_query.used_tables or features.tables),
            used_columns=list(sql_query.used_columns or features.columns),
            used_spatial_functions=list(sql_query.used_spatial_functions or features.postgis_functions),
            spatial_relation_constraints=[item.to_dict() for item in spatial_constraints],
            sql_features=features.to_dict(),
            metadata=self._build_question_metadata(sql_query, context),
            prompt=prompt,
            feedback_prompts=feedback_prompts,
            validation_result=validation_result.to_dict(),
            generation_metadata={
                "style": style,
                "style_description": STYLE_DESCRIPTIONS.get(style, ""),
                "generation_rounds": generation_rounds,
                "sql_difficulty": sql_query.difficulty_level,
                "success": validation_result.is_valid,
            },
        )
        return synthesized

    @staticmethod
    def _resolve_context_for_sql(
        sql_query: SQLQuestionSource,
        context_by_database_id: Mapping[str, QuestionGenerationContext],
    ) -> QuestionGenerationContext | None:
        metadata_context = QuestionGenerationContext.from_sql_metadata(
            sql_query.metadata,
            database_id=sql_query.database_id,
            city=sql_query.city,
        )
        if metadata_context is not None:
            return metadata_context
        return context_by_database_id.get(sql_query.database_id)

    @staticmethod
    def _context_to_database_context_payload(context: QuestionGenerationContext) -> dict[str, Any]:
        return {
            "database_id": context.database_id,
            "city": context.city,
            "selected_table_names": list(context.selected_table_names),
            "schema_ddls": list(context.schema_ddls),
            "tables": stable_jsonify(context.table_contexts),
        }

    def _build_question_metadata(
        self,
        sql_query: SQLQuestionSource,
        context: QuestionGenerationContext,
    ) -> dict[str, Any]:
        metadata = dict(sql_query.metadata or {})
        database_context = metadata.get("database_context")
        if not isinstance(database_context, Mapping) or not database_context.get("tables"):
            metadata["database_context"] = self._context_to_database_context_payload(context)
        return stable_jsonify(metadata)

    def _validate_candidate(
        self,
        *,
        candidate: QuestionGenerationCandidate,
        requested_style: str,
        features,
        spatial_constraints,
    ) -> QuestionValidationResult:
        validation_result = self.validator.validate(
            candidate=candidate,
            requested_style=requested_style,
            sql_features=features,
            spatial_constraints=spatial_constraints,
        )
        if candidate.parse_error:
            validation_result.warnings = [
                f"Recovered from non-standard model response: {candidate.parse_error}",
                *validation_result.warnings,
            ]
        return validation_result

    @staticmethod
    def _build_question_revision_feedback(validation_result: QuestionValidationResult) -> str:
        feedback_lines: list[str] = []
        if validation_result.errors:
            feedback_lines.append("Fix all of the following semantic issues:")
            feedback_lines.extend(f"- {item}" for item in validation_result.errors[:8])
        if validation_result.warnings:
            feedback_lines.append("If possible, also address these quality issues:")
            feedback_lines.extend(f"- {item}" for item in validation_result.warnings[:8])
        return "\n".join(feedback_lines).strip() or "Make the question align with the SQL exactly."

    def _should_force_reasoning_fallback(
        self,
        candidate: QuestionGenerationCandidate,
        response,
    ) -> bool:
        parse_error = str(candidate.parse_error or "")
        if not parse_error:
            return False
        raw_text = str(candidate.raw_response_text or "").strip()
        if "Unterminated string" in parse_error:
            return True
        usage = response.usage if isinstance(response.usage, Mapping) else {}
        completion_tokens = usage.get("completion_tokens")
        if (
            isinstance(completion_tokens, int)
            and completion_tokens >= int(self.config.llm.max_tokens)
            and raw_text.startswith("{")
            and not raw_text.rstrip().endswith("}")
        ):
            return True
        question = str(candidate.question or "").strip()
        if raw_text.startswith("{") and '"question"' in raw_text and question and not question.endswith(("?", ".")):
            return True
        return False

    @staticmethod
    def _build_fallback_question(
        *,
        sql_query: SQLQuestionSource,
        style: str,
        sql_features: Mapping[str, Any],
    ) -> str:
        summary = str(getattr(sql_query, "reasoning_summary", "") or "").strip()
        if not summary:
            return ""
        first_sentence = re.split(r"(?<=[.!?])\s+", summary, maxsplit=1)[0].strip()
        if not first_sentence:
            return ""
        clause = re.sub(r"(?i)^(the query|this query)\s+", "", first_sentence).strip().rstrip(".?!")
        if not clause:
            return ""
        remainder = clause
        for verb in ("retrieves", "returns", "lists", "selects", "finds", "identifies"):
            if clause.lower().startswith(f"{verb} "):
                remainder = clause[len(verb) + 1 :].strip()
                break
        limit = sql_features.get("limit")
        limit_phrase = ""
        if isinstance(limit, int) and limit > 0 and str(limit) not in remainder:
            limit_phrase = f" up to {limit}"
        style_prefixes = {
            "conversational": "Can you tell me ",
            "formal": "Please provide ",
            "direct": "List",
            "concise": "List",
            "polite": "Could you please provide ",
            "analytical": "Identify",
        }
        prefix = style_prefixes.get(style, "List")
        if prefix.endswith(" "):
            return f"{prefix}{remainder}{limit_phrase}?".strip()
        return f"{prefix}{limit_phrase} {remainder}".strip() + "."

    # Backward-compatible aliases
    generate_all = run
    generate_for_sql = run_for_sql
    _generate_single_question = _run_single_question


class DiversityAwareQuestionGenerator(DiversityAwareQuestionSynthesizer):
    """Backward-compatible alias for the legacy question generator name."""
