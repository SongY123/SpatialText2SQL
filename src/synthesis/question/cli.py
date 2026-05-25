"""CLI for diversity-aware question generation."""

from __future__ import annotations

import argparse
import logging
import sys
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

from src.prompting.prompt_builder import PromptBuilder

from .config import (
    DEFAULT_QUESTION_GENERATION_CONFIG_PATH,
    load_question_generation_config,
    override_question_generation_config,
)
from .execution import QuestionExecutionResultFetcher
from .synthesizer import DiversityAwareQuestionSynthesizer
from .generator import build_question_llm
from .io import (
    build_question_generation_contexts_from_sql_sources,
    ensure_question_output,
    load_existing_question_id_offsets,
    load_question_generation_contexts,
    load_sql_question_sources,
    merge_synthesized_questions_with_lock,
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Generate diverse natural-language questions from spatial SQL.")
    parser.add_argument("--config", default=str(DEFAULT_QUESTION_GENERATION_CONFIG_PATH))
    parser.add_argument("--sql-input")
    parser.add_argument("--database-context-path")
    parser.add_argument("--output")
    parser.add_argument("--parallel-workers", type=int)
    parser.add_argument("--num-questions-per-sql", type=int)
    parser.add_argument("--max-revision-rounds", type=int)
    parser.add_argument("--style")
    parser.add_argument("--style-weights")
    parser.add_argument("--provider")
    parser.add_argument("--model")
    parser.add_argument("--base-url")
    parser.add_argument("--api-key-env")
    parser.add_argument("--temperature", type=float)
    parser.add_argument("--max-tokens", type=int)
    parser.add_argument("--timeout", type=int)
    parser.add_argument("--max-retries", type=int)
    parser.add_argument("--random-seed", type=int)
    parser.add_argument("--log-level")
    parser.add_argument("--log-path")
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(sys.argv[1:] if argv is None else argv)
    config = load_question_generation_config(args.config)
    config = override_question_generation_config(
        config,
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
        generation={key: value for key, value in {
            "sql_input_path": args.sql_input,
            "database_context_path": args.database_context_path,
            "output_path": args.output,
            "parallel_workers": args.parallel_workers,
            "num_questions_per_sql": args.num_questions_per_sql,
            "max_revision_rounds": args.max_revision_rounds,
            "style": args.style,
            "style_weights": args.style_weights,
            "random_seed": args.random_seed,
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
        "Question generation config loaded | provider=%s | model=%s | sql_input=%s | context_input=%s | output=%s | parallel_workers=%s | num_questions_per_sql=%s",
        config.llm.provider,
        config.llm.model,
        config.generation.sql_input_path,
        config.generation.database_context_path,
        config.generation.output_path,
        config.generation.parallel_workers,
        config.generation.num_questions_per_sql,
    )

    sql_queries = load_sql_question_sources(config.generation.sql_input_path)
    if not sql_queries:
        raise ValueError("Question generation SQL input is empty.")
    query_city_counts = Counter(item.city for item in sql_queries)
    logging.info(
        "Loaded SQL question sources | count=%s | city_distribution=%s",
        len(sql_queries),
        dict(query_city_counts),
    )

    metadata_contexts = build_question_generation_contexts_from_sql_sources(sql_queries)
    contexts = dict(metadata_contexts)
    missing_database_ids = sorted({item.database_id for item in sql_queries if item.database_id not in contexts})
    if missing_database_ids:
        fallback_contexts = load_question_generation_contexts(config.generation.database_context_path)
        for database_id in missing_database_ids:
            if database_id in fallback_contexts:
                contexts[database_id] = fallback_contexts[database_id]
    logging.info(
        "Loaded question generation contexts | count=%s | from_sql_metadata=%s | missing_after_merge=%s",
        len(contexts),
        len(metadata_contexts),
        len([item.database_id for item in sql_queries if item.database_id not in contexts]),
    )

    ensure_question_output(config.generation.output_path)
    existing_question_id_offsets = load_existing_question_id_offsets(config.generation.output_path)
    logging.info(
        "Question output ready for append | path=%s | tracked_databases=%s",
        config.generation.output_path,
        len(existing_question_id_offsets),
    )

    prompt_builder = PromptBuilder({"project_root": Path(__file__).resolve().parents[3]})
    execution_result_fetcher = QuestionExecutionResultFetcher(
        db_config=config.database,
        execution_config=config.execution,
    )
    parallel_workers = max(1, int(config.generation.parallel_workers))
    grouped_sql_queries: dict[str, list] = {}
    for row in sql_queries:
        grouped_sql_queries.setdefault(row.database_id, []).append(row)

    written_count = 0

    def _run_database_task(task_index, database_id, rows_for_db):
        local_config = override_question_generation_config(
            config,
            generation={"random_seed": int(config.generation.random_seed) + task_index},
        )
        local_llm_client = build_question_llm(config=local_config.llm)
        local_generator = DiversityAwareQuestionSynthesizer(
            config=local_config,
            llm_client=local_llm_client,
            prompt_builder=prompt_builder,
            execution_result_fetcher=execution_result_fetcher,
            existing_question_id_offsets={
                database_id: existing_question_id_offsets.get(database_id, 0),
            },
        )
        context = contexts.get(database_id)
        if context is None:
            logging.warning("Skipping database_id=%s because question context is missing.", database_id)
            return []
        return local_generator.run_for_sql_group(rows_for_db, context)

    with ThreadPoolExecutor(max_workers=min(parallel_workers, len(grouped_sql_queries) or 1)) as executor:
        future_map = {
            executor.submit(_run_database_task, task_index, database_id, rows_for_db): database_id
            for task_index, (database_id, rows_for_db) in enumerate(grouped_sql_queries.items())
        }
        for future in as_completed(future_map):
            database_id = future_map[future]
            try:
                rows = future.result()
            except Exception as exc:
                logging.exception(
                    "Question generation database task failed | database_id=%s | error=%s",
                    database_id,
                    exc,
                )
                continue
            merge_synthesized_questions_with_lock(config.generation.output_path, rows)
            written_count += len(rows)
            for row in rows:
                logging.info(
                    "Merged question batch row | output=%s | written_count=%s | question_id=%s",
                    config.generation.output_path,
                    written_count,
                    row.question_id,
                )

    logging.info("Appended %s synthesized questions to %s", written_count, config.generation.output_path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
