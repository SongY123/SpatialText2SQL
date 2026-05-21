"""CLI for NL-SQL quality control."""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

from src.synthesis.sql.config import SQLSynthesisDBConfig
from src.synthesis.sql.prompt_metadata import PostGISPromptMetadataProvider

from .config import (
    DEFAULT_QUALITY_CONTROL_CONFIG_PATH,
    load_quality_control_config,
    override_quality_control_config,
)
from .io import (
    load_nl_sql_samples,
    load_sql_context_by_sql_id,
    write_nl_sql_samples,
    write_quality_control_report,
)
from .pipeline import QualityControlPipeline, format_only_quality_control


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run quality control over synthetic spatial NL-SQL samples.")
    parser.add_argument("--config", default=str(DEFAULT_QUALITY_CONTROL_CONFIG_PATH))
    parser.add_argument("--input")
    parser.add_argument("--schema-context-path")
    parser.add_argument("--output")
    parser.add_argument("--report-path")
    parser.add_argument("--allow-empty-result", action="store_true")
    parser.add_argument("--semantic-mode")
    parser.add_argument("--debug-mode", action="store_true")
    parser.add_argument("--max-result-rows", type=int)
    parser.add_argument("--provider")
    parser.add_argument("--model")
    parser.add_argument("--base-url")
    parser.add_argument("--api-key-env")
    parser.add_argument("--temperature", type=float)
    parser.add_argument("--max-tokens", type=int)
    parser.add_argument("--timeout", type=int)
    parser.add_argument("--max-retries", type=int)
    parser.add_argument("--question-similarity-threshold", type=float)
    parser.add_argument("--same-sql-similarity-threshold", type=float)
    parser.add_argument("--host")
    parser.add_argument("--port", type=int)
    parser.add_argument("--database")
    parser.add_argument("--user")
    parser.add_argument("--password")
    parser.add_argument("--search-path")
    parser.add_argument("--connect-timeout", type=int)
    parser.add_argument("--statement-timeout", type=int)
    parser.add_argument("--log-level")
    parser.add_argument("--log-path")
    return parser


def _run_full_quality_control(samples, config):
    from src.prompting.prompt_builder import PromptBuilder
    from src.synthesis.sql.function_library import PostGISFunctionLibrary

    from .database import PostgreSQLDatabaseRegistry
    from .generator import build_quality_control_llm
    from .io import load_schema_registry_from_contexts
    from .judge import SelfConsistencyQualityJudge

    schema_registry = load_schema_registry_from_contexts(config.run.schema_context_path)
    database_registry = PostgreSQLDatabaseRegistry(config.database)
    function_library = PostGISFunctionLibrary.load(
        Path(config.functions.postgis_function_json_path),
        Path(config.functions.st_function_markdown_path),
        list(config.functions.exclude_categories),
    )
    llm_client = build_quality_control_llm(config=config.llm)
    prompt_builder = PromptBuilder(
        {
            "project_root": Path(__file__).resolve().parents[3],
            "prompt_template_path": config.judge.prompt_template_path,
        }
    )
    judge = SelfConsistencyQualityJudge(
        llm_client=llm_client,
        prompt_builder=prompt_builder,
    )
    pipeline = QualityControlPipeline(
        function_library=function_library,
        self_consistency_judge=judge,
    )
    return pipeline.run(samples, database_registry, schema_registry, config)


def _merge_database_context(existing, incoming):
    if not isinstance(incoming, dict):
        return existing if isinstance(existing, dict) else {}
    if not isinstance(existing, dict):
        return dict(incoming)
    merged = dict(existing)
    for key, value in incoming.items():
        if key == "tables" and isinstance(value, list) and not merged.get("tables"):
            merged["tables"] = value
        elif key in {"selected_table_names", "schema_ddls"} and isinstance(value, list) and not merged.get(key):
            merged[key] = value
        elif key not in merged or merged.get(key) in (None, "", [], {}):
            merged[key] = value
    return merged


def _enrich_samples_from_sql_context(samples, sql_context_by_id):
    for sample in samples:
        sql_context = sql_context_by_id.get(sample.sql_id or sample.sample_id)
        if not isinstance(sql_context, dict):
            continue
        if not sample.sql_reasoning_summary:
            sample.sql_reasoning_summary = str(sql_context.get("reasoning_summary") or "").strip()
        if not sample.difficulty_level:
            sample.difficulty_level = str(sql_context.get("difficulty_level") or "").strip()
        if not sample.used_tables:
            sample.used_tables = [str(item).strip() for item in (sql_context.get("used_tables") or []) if str(item).strip()]
        if not sample.used_columns:
            sample.used_columns = [str(item).strip() for item in (sql_context.get("used_columns") or []) if str(item).strip()]
        if not sample.used_spatial_functions:
            sample.used_spatial_functions = [
                str(item).strip() for item in (sql_context.get("used_spatial_functions") or []) if str(item).strip()
            ]

        merged_metadata = dict(sample.metadata or {})
        incoming_metadata = sql_context.get("metadata")
        if isinstance(incoming_metadata, dict):
            incoming_db_context = incoming_metadata.get("database_context")
            if incoming_db_context or "database_context" not in merged_metadata:
                merged_metadata["database_context"] = _merge_database_context(
                    merged_metadata.get("database_context"),
                    incoming_db_context if isinstance(incoming_db_context, dict) else {},
                )
            for key, value in incoming_metadata.items():
                if key == "database_context":
                    continue
                if key not in merged_metadata or merged_metadata.get(key) in (None, "", [], {}):
                    merged_metadata[key] = value
        sample.metadata = merged_metadata
        sample.original_payload["metadata"] = merged_metadata
        if sample.sql_reasoning_summary:
            sample.original_payload["sql_reasoning_summary"] = sample.sql_reasoning_summary
        if sample.difficulty_level:
            if "source_difficulty_level" in sample.original_payload:
                sample.original_payload["source_difficulty_level"] = sample.difficulty_level
            elif "difficulty_level" in sample.original_payload:
                sample.original_payload["difficulty_level"] = sample.difficulty_level
        for key in ("execution_result", "structural_constraints", "spatial_function_constraints"):
            if key not in sample.original_payload and key in sql_context:
                sample.original_payload[key] = sql_context.get(key)


def _load_live_database_contexts(samples, config):
    if not config.run.prefer_live_schema:
        return {}
    provider = PostGISPromptMetadataProvider(
        SQLSynthesisDBConfig(
            host=config.database.host,
            port=config.database.port,
            database=config.database.database,
            user=config.database.user,
            password=config.database.password,
            search_path=config.database.search_path,
            connect_timeout=config.database.connect_timeout,
            statement_timeout=config.database.statement_timeout,
        )
    )
    contexts: dict[str, dict] = {}
    for sample in samples:
        database_id = str(sample.database_id).strip()
        if not database_id or database_id in contexts:
            continue
        city = str(sample.original_payload.get("city") or sample.metadata.get("city") or "").strip()
        context = provider.load_database_metadata_by_id(database_id=database_id, city=city)
        if not isinstance(context, dict):
            raise RuntimeError(f"Failed to load live database context for {database_id}.")
        sanitized = {str(key): value for key, value in context.items() if key != "tables"}
        contexts[database_id] = sanitized
    return contexts


def _inject_live_database_context(samples, contexts_by_database_id):
    for sample in samples:
        context = contexts_by_database_id.get(sample.database_id)
        if not isinstance(context, dict):
            continue
        merged_metadata = dict(sample.metadata or {})
        merged_metadata["database_context"] = context
        sample.metadata = merged_metadata
        sample.original_payload["metadata"] = merged_metadata


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(sys.argv[1:] if argv is None else argv)
    config = load_quality_control_config(args.config)
    config = override_quality_control_config(
        config,
        database={key: value for key, value in {
            "host": args.host,
            "port": args.port,
            "database": args.database,
            "user": args.user,
            "password": args.password,
            "search_path": args.search_path,
            "connect_timeout": args.connect_timeout,
            "statement_timeout": args.statement_timeout,
        }.items() if value is not None},
        llm={key: value for key, value in {
            "provider": args.provider,
            "model": args.model,
            "base_url": args.base_url,
            "api_key_env": args.api_key_env,
            "temperature": args.temperature,
            "max_tokens": args.max_tokens,
            "timeout": args.timeout,
            "max_retries": args.max_retries,
        }.items() if value is not None},
        run={key: value for key, value in {
            "input_path": args.input,
            "schema_context_path": args.schema_context_path,
            "output_path": args.output,
            "report_path": args.report_path,
            "allow_empty_result": args.allow_empty_result if args.allow_empty_result else None,
            "max_result_rows": args.max_result_rows,
        }.items() if value is not None},
        semantic={key: value for key, value in {
            "mode": args.semantic_mode,
            "debug_mode": args.debug_mode if args.debug_mode else None,
        }.items() if value is not None},
        duplicates={key: value for key, value in {
            "question_similarity_threshold": args.question_similarity_threshold,
            "same_sql_similarity_threshold": args.same_sql_similarity_threshold,
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
        "Quality control config loaded | input=%s | schema_context=%s | output=%s | report=%s",
        config.run.input_path,
        config.run.schema_context_path,
        config.run.output_path,
        config.run.report_path,
    )

    samples = load_nl_sql_samples(config.run.input_path)
    sql_context_by_id = load_sql_context_by_sql_id(config.run.sql_context_path)
    _enrich_samples_from_sql_context(samples, sql_context_by_id)
    _inject_live_database_context(samples, _load_live_database_contexts(samples, config))
    logging.info(
        "Quality control formatter-only mode enabled; skipping validator, database, judge, duplicate, and balancing logic."
    )
    # retained, report = _run_full_quality_control(samples, config)
    retained, report = format_only_quality_control(samples)
    write_nl_sql_samples(config.run.output_path, retained)
    write_quality_control_report(config.run.report_path, report)
    logging.info(
        "Quality control finished | total=%s | retained=%s | failed=%s | duplicates=%s",
        report.total_samples,
        report.passed_samples,
        report.failed_samples,
        report.duplicate_count,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
