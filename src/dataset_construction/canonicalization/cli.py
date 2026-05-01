"""CLI entrypoint for metadata-driven table canonicalization."""
from __future__ import annotations

import argparse
from pathlib import Path

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
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_argument_parser()
    args = parser.parse_args(argv)
    output_path = canonicalize_metadata_file(
        args.metadata_path,
        output_path=args.output,
        max_rows_for_inference=args.max_rows_for_inference,
    )
    print(f"[canonicalization] wrote {output_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
