"""CLI entrypoint for metadata-driven table canonicalization."""
from __future__ import annotations

import argparse
from pathlib import Path
import sys

from src.dataset_construction.crawl.profiles import DEFAULT_CITY_ORDER, parse_city_list

from .core import canonicalize_metadata_file


def _positive_int(value: str) -> int:
    parsed = int(value)
    if parsed <= 0:
        raise argparse.ArgumentTypeError("Value must be a positive integer.")
    return parsed


def build_argument_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Read a crawl metadata.json file, canonicalize each dataset schema, and "
            "write metadata_canonicalized.json beside it."
        )
    )
    parser.add_argument(
        "metadata_path",
        type=Path,
        help="Path to the input metadata.json file.",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=None,
        help="Optional output path. Default: metadata_canonicalized.json beside the input file.",
    )
    parser.add_argument(
        "--max-rows-for-inference",
        type=_positive_int,
        default=100,
        help="Maximum sampled rows per dataset for type and spatial inference.",
    )
    parser.add_argument(
        "--cities",
        default="all",
        help=f"Comma-separated city ids or 'all'. Choices: {', '.join(DEFAULT_CITY_ORDER)}.",
    )
    parser.add_argument("--list-cities", action="store_true", help="Print configured city ids and exit.")
    return parser


def main(argv: list[str] | None = None) -> int:
    argv = list(sys.argv[1:] if argv is None else argv)
    if "--list-cities" in argv:
        print(",".join(DEFAULT_CITY_ORDER))
        return 0
    parser = build_argument_parser()
    args = parser.parse_args(argv)
    try:
        selected_profiles = parse_city_list(args.cities)
    except ValueError as exc:
        parser.error(str(exc))
    output_path = canonicalize_metadata_file(
        args.metadata_path,
        output_path=args.output,
        max_rows_for_inference=args.max_rows_for_inference,
        selected_city_ids=[profile.city_id for profile in selected_profiles],
    )
    print(f"[canonicalization] wrote {output_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
