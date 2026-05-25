"""Configuration handling for diversity-aware question generation."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Mapping

import yaml

from src.synthesis.database.utils import stable_jsonify, to_text
from src.synthesis.llm import SynthesisLLMConfig, build_llm_config_from_section

from .models import QUESTION_STYLES


def _project_root() -> Path:
    return Path(__file__).resolve().parents[3]


DEFAULT_QUESTION_GENERATION_CONFIG_PATH = _project_root() / "config" / "question_synthesis.yaml"


@dataclass(frozen=True)
class QuestionGenerationLLMConfig(SynthesisLLMConfig):
    max_tokens: int = 1400


@dataclass(frozen=True)
class QuestionGenerationDBConfig:
    host: str = "localhost"
    port: int = 5432
    database: str = "postgres"
    user: str = "postgres"
    password: str = "123456"
    connect_timeout: int = 10
    pool_min_size: int = 1
    pool_max_size: int = 50


@dataclass(frozen=True)
class QuestionExecutionConfig:
    enable_result_fetch: bool = True
    execution_timeout: int = 30
    max_result_rows: int = 3


@dataclass(frozen=True)
class QuestionGenerationRunConfig:
    sql_input_path: str = str(_project_root() / "data" / "processed" / "synthesized_sql_queries.jsonl")
    database_context_path: str = str(_project_root() / "data" / "processed" / "synthesized_spatial_databases.jsonl")
    output_path: str = str(_project_root() / "data" / "processed" / "synthesized_questions.jsonl")
    parallel_workers: int = 10
    num_questions_per_sql: int = 1
    max_revision_rounds: int = 2
    fixed_style: str = ""
    style_weights: dict[str, float] = field(default_factory=lambda: {style: 1.0 for style in QUESTION_STYLES})
    random_seed: int = 42


@dataclass(frozen=True)
class QuestionGenerationLoggingConfig:
    log_level: str = "INFO"
    log_path: str = ""


@dataclass(frozen=True)
class QuestionGenerationConfig:
    database: QuestionGenerationDBConfig = field(default_factory=QuestionGenerationDBConfig)
    llm: QuestionGenerationLLMConfig = field(default_factory=QuestionGenerationLLMConfig)
    execution: QuestionExecutionConfig = field(default_factory=QuestionExecutionConfig)
    generation: QuestionGenerationRunConfig = field(default_factory=QuestionGenerationRunConfig)
    logging: QuestionGenerationLoggingConfig = field(default_factory=QuestionGenerationLoggingConfig)


def _as_text(value: Any, default: str = "") -> str:
    text = to_text(value)
    return text if text else default


def _resolve_path(value: Any, config_path: Path, default: str) -> str:
    text = _as_text(value)
    if not text:
        return default
    path = Path(text)
    if path.is_absolute():
        return str(path)
    return str((config_path.parent.parent / path).resolve())


def _as_positive_int(value: Any, default: int) -> int:
    if value in (None, ""):
        return default
    parsed = int(value)
    if parsed <= 0:
        raise ValueError(f"Expected a positive integer, got {value!r}")
    return parsed


def _as_non_negative_int(value: Any, default: int) -> int:
    if value in (None, ""):
        return default
    parsed = int(value)
    if parsed < 0:
        raise ValueError(f"Expected a non-negative integer, got {value!r}")
    return parsed


def _as_float(value: Any, default: float) -> float:
    if value in (None, ""):
        return default
    return float(value)


def _as_bool(value: Any, default: bool) -> bool:
    if value in (None, ""):
        return default
    if isinstance(value, bool):
        return value
    lowered = str(value).strip().lower()
    if lowered in {"1", "true", "yes", "y", "on"}:
        return True
    if lowered in {"0", "false", "no", "n", "off"}:
        return False
    raise ValueError(f"Expected a boolean-like value, got {value!r}")


def _normalize_style(value: Any) -> str:
    text = _as_text(value).lower()
    if not text:
        return ""
    if text not in QUESTION_STYLES:
        raise ValueError(f"Unsupported question style: {value!r}")
    return text


def _normalize_style_weights(value: Any) -> dict[str, float]:
    if value in (None, ""):
        return {style: 1.0 for style in QUESTION_STYLES}
    if isinstance(value, str):
        parsed: dict[str, float] = {}
        for part in [item.strip() for item in value.split(",") if item.strip()]:
            if "=" not in part:
                raise ValueError(f"Invalid style weight item: {part!r}")
            key, raw_weight = part.split("=", 1)
            parsed[key.strip().lower()] = float(raw_weight)
        value = parsed
    if not isinstance(value, Mapping):
        raise ValueError("style_weights must be a mapping or comma-separated string.")
    weights = {style: float(value.get(style, 0.0)) for style in QUESTION_STYLES}
    if sum(max(weight, 0.0) for weight in weights.values()) <= 0:
        raise ValueError("style_weights must contain at least one positive weight.")
    return weights


def load_question_generation_config(config_path: str | Path | None = None) -> QuestionGenerationConfig:
    path = Path(config_path or DEFAULT_QUESTION_GENERATION_CONFIG_PATH)
    if not path.is_file():
        raise FileNotFoundError(f"Question generation config not found: {path}")
    payload = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    return _build_question_generation_config_from_payload(payload, path)


def _build_question_generation_config_from_payload(
    payload: Mapping[str, Any],
    path: Path,
) -> QuestionGenerationConfig:
    if not isinstance(payload, Mapping):
        raise ValueError(f"Invalid question generation config in {path}: top level must be a mapping.")

    llm_section = payload.get("llm") or {}
    database_section = payload.get("database") or {}
    execution_section = payload.get("execution") or {}
    generation_section = payload.get("generation") or {}
    logging_section = payload.get("logging") or {}
    for section_name, section in (
        ("database", database_section),
        ("llm", llm_section),
        ("execution", execution_section),
        ("generation", generation_section),
        ("logging", logging_section),
    ):
        if section and not isinstance(section, Mapping):
            raise ValueError(f"Invalid question generation config: '{section_name}' must be a mapping.")

    default_db = QuestionGenerationDBConfig()
    default_llm = QuestionGenerationLLMConfig()
    default_execution = QuestionExecutionConfig()
    default_generation = QuestionGenerationRunConfig()
    default_logging = QuestionGenerationLoggingConfig()

    return QuestionGenerationConfig(
        database=QuestionGenerationDBConfig(
            host=_as_text(database_section.get("host"), default_db.host),
            port=_as_positive_int(database_section.get("port"), default_db.port),
            database=_as_text(database_section.get("database"), default_db.database),
            user=_as_text(database_section.get("user"), default_db.user),
            password=_as_text(database_section.get("password"), default_db.password),
            connect_timeout=_as_positive_int(database_section.get("connect_timeout"), default_db.connect_timeout),
            pool_min_size=_as_positive_int(database_section.get("pool_min_size"), default_db.pool_min_size),
            pool_max_size=_as_positive_int(database_section.get("pool_max_size"), default_db.pool_max_size),
        ),
        llm=build_llm_config_from_section(
            llm_section,
            default_llm,
            as_text=_as_text,
            as_float=_as_float,
            as_positive_int=_as_positive_int,
            as_non_negative_int=_as_non_negative_int,
        ),
        execution=QuestionExecutionConfig(
            enable_result_fetch=_as_bool(
                execution_section.get("enable_result_fetch"),
                default_execution.enable_result_fetch,
            ),
            execution_timeout=_as_positive_int(
                execution_section.get("execution_timeout"),
                default_execution.execution_timeout,
            ),
            max_result_rows=_as_positive_int(
                execution_section.get("max_result_rows"),
                default_execution.max_result_rows,
            ),
        ),
        generation=QuestionGenerationRunConfig(
            sql_input_path=_resolve_path(generation_section.get("sql_input_path"), path, default_generation.sql_input_path),
            database_context_path=_resolve_path(
                generation_section.get("database_context_path"),
                path,
                default_generation.database_context_path,
            ),
            output_path=_resolve_path(generation_section.get("output_path"), path, default_generation.output_path),
            parallel_workers=_as_positive_int(
                generation_section.get("parallel_workers"),
                default_generation.parallel_workers,
            ),
            num_questions_per_sql=_as_positive_int(
                generation_section.get("num_questions_per_sql"),
                default_generation.num_questions_per_sql,
            ),
            max_revision_rounds=_as_non_negative_int(
                generation_section.get("max_revision_rounds"),
                default_generation.max_revision_rounds,
            ),
            fixed_style=_normalize_style(generation_section.get("style") or generation_section.get("fixed_style")),
            style_weights=_normalize_style_weights(
                generation_section.get("style_weights", default_generation.style_weights)
            ),
            random_seed=int(generation_section.get("random_seed", default_generation.random_seed)),
        ),
        logging=QuestionGenerationLoggingConfig(
            log_level=_as_text(logging_section.get("log_level"), default_logging.log_level),
            log_path=_resolve_path(logging_section.get("log_path"), path, default_logging.log_path)
            if to_text(logging_section.get("log_path"))
            else default_logging.log_path,
        ),
    )


def override_question_generation_config(
    base: QuestionGenerationConfig,
    *,
    llm: Mapping[str, Any] | None = None,
    generation: Mapping[str, Any] | None = None,
    logging: Mapping[str, Any] | None = None,
) -> QuestionGenerationConfig:
    merged = {
        "database": {**base.database.__dict__},
        "llm": {**base.llm.__dict__, **dict(llm or {})},
        "execution": {**base.execution.__dict__},
        "generation": {**base.generation.__dict__, **dict(generation or {})},
        "logging": {**base.logging.__dict__, **dict(logging or {})},
    }
    return _build_question_generation_config_from_payload(
        stable_jsonify(merged),
        DEFAULT_QUESTION_GENERATION_CONFIG_PATH,
    )
