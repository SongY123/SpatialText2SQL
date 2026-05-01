"""CLI entrypoints for PostGIS documentation parsing workflows."""
from __future__ import annotations

import argparse
import os

from .validate_postgis import DEFAULT_EXTERNAL_TABLE_SOURCES_FILE, PostGISValidator


def _default_db_config() -> dict[str, object]:
    return {
        "dbname": os.getenv("POSTGIS_DBNAME", "postgis_test_db"),
        "user": os.getenv("POSTGIS_DBUSER", "postgres"),
        "password": os.getenv("POSTGIS_DBPASSWORD", "1234"),
        "host": os.getenv("POSTGIS_DBHOST", "localhost"),
        "port": int(os.getenv("POSTGIS_DBPORT", "5432")),
    }


def _add_db_arguments(parser: argparse.ArgumentParser) -> None:
    defaults = _default_db_config()
    parser.add_argument("--db-name", default=defaults["dbname"])
    parser.add_argument("--db-user", default=defaults["user"])
    parser.add_argument("--db-password", default=defaults["password"])
    parser.add_argument("--db-host", default=defaults["host"])
    parser.add_argument("--db-port", type=int, default=defaults["port"])


def _build_db_config(args: argparse.Namespace) -> dict[str, object]:
    return {
        "dbname": args.db_name,
        "user": args.db_user,
        "password": args.db_password,
        "host": args.db_host,
        "port": args.db_port,
    }


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run PostGIS documentation extraction, validation, and import helpers.",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    extract_parser = subparsers.add_parser("extract", help="Extract structured examples from PostGIS XML docs.")
    extract_parser.add_argument("--input-dir", default="xml_data")
    extract_parser.add_argument("--output-file", default="extract_result/postgis_extracted.json")

    validate_parser = subparsers.add_parser("validate", help="Validate extracted SQL examples against PostGIS.")
    validate_parser.add_argument("--mode", choices=["full", "external_only", "import_external"], default="full")
    validate_parser.add_argument("--from-review", default=None)
    validate_parser.add_argument("--input", default="extract_result/postgis_extracted.json")
    validate_parser.add_argument("--output", default="validation_result/postgis_validated.json")
    validate_parser.add_argument("--review", default="manual_review/manual_review.json")
    validate_parser.add_argument("--external-sources", default=str(DEFAULT_EXTERNAL_TABLE_SOURCES_FILE))
    validate_parser.add_argument("--if-exists", choices=["fail", "replace", "append"], default="append")
    validate_parser.add_argument("--schema", default=None)
    _add_db_arguments(validate_parser)

    shp_parser = subparsers.add_parser("shp2db", help="Import SHP data into PostGIS or SpatiaLite.")
    shp_parser.add_argument("--input-path", required=True)
    shp_parser.add_argument("--db-url", required=True)
    shp_parser.add_argument("--table-name", default=None)
    shp_parser.add_argument("--schema", default=None)
    shp_parser.add_argument("--if-exists", choices=["fail", "replace", "append"], default="replace")

    pbf_parser = subparsers.add_parser("pbf2db", help="Import OSM PBF layers into PostGIS or SpatiaLite.")
    pbf_parser.add_argument("--input-path", required=True)
    pbf_parser.add_argument("--db-url", required=True)
    pbf_parser.add_argument("--schema", default=None)
    pbf_parser.add_argument("--if-exists", choices=["fail", "replace", "append"], default="replace")

    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    if args.command == "extract":
        from .postgis_doc_extract import PostGISFormalParser

        PostGISFormalParser().batch_process(args.input_dir, args.output_file)
        return 0

    if args.command == "validate":
        validator = PostGISValidator(
            db_config=_build_db_config(args),
            input_file=args.input,
            output_file=args.output,
            manual_review_file=args.review,
            external_table_sources_file=args.external_sources,
        )
        if args.mode == "import_external":
            if not args.from_review:
                parser.error("--mode import_external requires --from-review.")
            validator.import_external_tables_via_shp2db(args.from_review, if_exists=args.if_exists, schema=args.schema)
            return 0
        if args.mode == "external_only":
            if not args.from_review:
                parser.error("--mode external_only requires --from-review.")
            validator.validate_external_import_only(args.from_review)
            return 0
        validator.validate_dataset()
        return 0

    if args.command == "shp2db":
        from .shp2db import shp2db

        shp2db(
            input_path=args.input_path,
            db_url=args.db_url,
            table_name=args.table_name,
            schema=args.schema,
            if_exists=args.if_exists,
        )
        return 0

    if args.command == "pbf2db":
        from .pbf2db import pbf2db

        pbf2db(
            input_path=args.input_path,
            db_url=args.db_url,
            schema=args.schema,
            if_exists=args.if_exists,
        )
        return 0

    parser.error(f"Unsupported command: {args.command}")
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
