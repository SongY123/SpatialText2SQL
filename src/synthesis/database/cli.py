"""CLI for relation-aware spatial database synthesis."""

from __future__ import annotations

import argparse
import logging
import sys

from src.dataset_construction.crawl.profiles import DEFAULT_CITY_ORDER, parse_city_list

from .embeddings import DEFAULT_EMBEDDING_MODEL, SentenceTransformerEmbeddingProvider
from .io import load_canonical_tables, write_synthesized_databases
from .synthesizer import SpatialDatabaseSynthesizer


def probability_argument(value: str) -> float:
    parsed = float(value)
    if not 0.0 <= parsed <= 1.0:
        raise argparse.ArgumentTypeError("exploration-prob must be within [0, 1].")
    return parsed


def positive_int_argument(value: str) -> int:
    parsed = int(value)
    if parsed < 1:
        raise argparse.ArgumentTypeError("value must be at least 1.")
    return parsed


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Synthesize relation-aware spatial databases from canonical tables.")
    parser.add_argument("--input", required=True, help="Input canonical table JSON or JSONL path.")
    parser.add_argument("--output", required=True, help="Output synthesized database JSONL path.")
    parser.add_argument(
        "--cities",
        default="all",
        help=f"Comma-separated city ids or 'all'. Choices: {', '.join(DEFAULT_CITY_ORDER)}.",
    )
    parser.add_argument("--target-avg-degree", type=float, default=4.0)
    parser.add_argument("--exploration-prob", type=probability_argument, default=0.1)
    parser.add_argument("--size-mean", type=float, default=8.0)
    parser.add_argument("--size-std", type=float, default=2.0)
    parser.add_argument("--min-tables", type=positive_int_argument, default=2)
    parser.add_argument("--max-tables", type=positive_int_argument, default=12)
    parser.add_argument("--embedding-model", default=DEFAULT_EMBEDDING_MODEL)
    parser.add_argument("--random-seed", type=int, default=42)
    parser.add_argument("--max-sampling-steps", type=positive_int_argument, default=100)
    parser.add_argument("--log-level", default="INFO")
    parser.add_argument("--list-cities", action="store_true", help="Print configured city ids and exit.")
    return parser


def main(argv: list[str] | None = None) -> int:
    argv = list(sys.argv[1:] if argv is None else argv)
    if "--list-cities" in argv:
        print(",".join(DEFAULT_CITY_ORDER))
        return 0
    parser = build_parser()
    args = parser.parse_args(argv)
    if args.max_tables < args.min_tables:
        parser.error("--max-tables must be greater than or equal to --min-tables.")
    try:
        selected_profiles = parse_city_list(args.cities)
    except ValueError as exc:
        parser.error(str(exc))

    logging.basicConfig(
        level=getattr(logging, str(args.log_level).upper(), logging.INFO),
        format="[%(levelname)s] %(message)s",
    )

    tables = load_canonical_tables(args.input)
    if not tables:
        logging.warning("Input canonical table file is empty. Writing an empty output file: %s", args.output)
        write_synthesized_databases(args.output, [])
        return 0

    embedding_provider = SentenceTransformerEmbeddingProvider(model_name=args.embedding_model)
    synthesizer = SpatialDatabaseSynthesizer(
        embedding_provider=embedding_provider,
        target_avg_degree=args.target_avg_degree,
        exploration_prob=args.exploration_prob,
        size_mean=args.size_mean,
        size_std=args.size_std,
        min_tables=args.min_tables,
        max_tables=args.max_tables,
        max_sampling_steps=args.max_sampling_steps,
        random_seed=args.random_seed,
        embedding_model=args.embedding_model,
    )
    databases = synthesizer.synthesize(
        tables,
        selected_city_ids=[profile.city_id for profile in selected_profiles],
    )
    write_synthesized_databases(args.output, databases)
    logging.info("Wrote %s synthesized database(s) to %s", len(databases), args.output)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
