"""CLI for migrating synthesized spatial databases into PostGIS."""

from __future__ import annotations

import argparse
import logging
import sys

from src.dataset_construction.crawl.profiles import DEFAULT_CITY_ORDER, parse_city_list

from ..io import load_synthesized_databases
from .config import DEFAULT_MIGRATE_CONFIG_PATH, load_migration_config
from .core import PostGISConnectionSettings, PostGISSynthesizedDatabaseMigrator


def positive_int(value: str) -> int:
    parsed = int(value)
    if parsed <= 0:
        raise argparse.ArgumentTypeError("Value must be a positive integer.")
    return parsed


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Migrate synthesized spatial databases into PostGIS.")
    parser.add_argument(
        "--config",
        default=str(DEFAULT_MIGRATE_CONFIG_PATH),
        help="Migration YAML config path. Default: config/migrate.yaml",
    )
    parser.add_argument("--input", help="Input synthesized_spatial_databases.jsonl path.")
    parser.add_argument(
        "--cities",
        help=f"Comma-separated city ids or 'all'. Choices: {', '.join(DEFAULT_CITY_ORDER)}.",
    )
    parser.add_argument("--host")
    parser.add_argument("--port", type=positive_int)
    parser.add_argument("--user")
    parser.add_argument("--password")
    parser.add_argument("--catalog")
    parser.add_argument("--bootstrap-db")
    parser.add_argument("--maintenance-db", help=argparse.SUPPRESS)
    parser.add_argument("--insert-batch-size", type=positive_int)
    parser.add_argument("--log-level")
    parser.add_argument("--list-cities", action="store_true", help="Print configured city ids and exit.")
    return parser


def main(argv: list[str] | None = None) -> int:
    argv = list(sys.argv[1:] if argv is None else argv)
    if "--list-cities" in argv:
        print(",".join(DEFAULT_CITY_ORDER))
        return 0

    parser = build_parser()
    args = parser.parse_args(argv)
    file_config = load_migration_config(args.config)
    merged_input = args.input or file_config.input_path
    merged_cities = args.cities or file_config.cities
    merged_log_level = args.log_level or file_config.log_level
    merged_insert_batch_size = args.insert_batch_size or file_config.insert_batch_size
    merged_connection = PostGISConnectionSettings(
        host=args.host or file_config.connection.host,
        port=args.port or file_config.connection.port,
        user=args.user or file_config.connection.user,
        password=args.password or file_config.connection.password,
        catalog=args.catalog or file_config.connection.catalog,
        bootstrap_db=(
            args.bootstrap_db
            or args.maintenance_db
            or file_config.connection.bootstrap_db
        ),
    )

    try:
        selected_profiles = parse_city_list(merged_cities)
    except ValueError as exc:
        parser.error(str(exc))

    logging.basicConfig(
        level=getattr(logging, str(merged_log_level).upper(), logging.INFO),
        format="[%(levelname)s] %(message)s",
    )

    databases = load_synthesized_databases(merged_input)
    selected_city_ids = {profile.city_id for profile in selected_profiles}
    filtered = [item for item in databases if item.city in selected_city_ids]
    migrator = PostGISSynthesizedDatabaseMigrator(
        merged_connection,
        insert_batch_size=merged_insert_batch_size,
    )
    migrated = migrator.migrate_databases(filtered)
    logging.info("Migrated %s synthesized database(s).", len(migrated))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
