"""Unified command-line entrypoint for seven-city map GeoJSON crawls."""
from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
import inspect
from pathlib import Path
import sys
from threading import Lock
from typing import Any

from .ckan import CkanGeoJsonCrawler
from .metadata import (
    build_city_metadata,
    index_dataset_records,
    load_root_metadata,
    write_column_type_metadata,
    write_root_metadata,
)
from .profiles import CITY_PROFILES, DEFAULT_CITY_ORDER, CityProfile, parse_city_list, should_skip_dataset_name
from .socrata import SocrataMapCrawler


REPO_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_OUT_ROOT = REPO_ROOT / "data" / "raw"
DEFAULT_METADATA_NAME = "metadata.json"
DEFAULT_COLUMN_TYPE_NAME = "columntype.json"


def _positive_int(value: str) -> int:
    try:
        parsed = int(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f"Expected an integer, got {value!r}.") from exc
    if parsed <= 0:
        raise argparse.ArgumentTypeError("Value must be a positive integer.")
    return parsed


def build_argument_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Download seven-city open-data map datasets as GeoJSON. "
            "Without --sample, every configured city is crawled until its map catalog is exhausted."
        )
    )
    parser.add_argument(
        "--sample",
        type=_positive_int,
        default=None,
        help="Download at most this many datasets per city. Omit to download all map datasets.",
    )
    parser.add_argument(
        "--cities",
        default="all",
        help=f"Comma-separated city ids or 'all'. Choices: {', '.join(DEFAULT_CITY_ORDER)}.",
    )
    parser.add_argument(
        "--out-root",
        type=Path,
        default=DEFAULT_OUT_ROOT,
        help=f"Root output directory. Default: {DEFAULT_OUT_ROOT}.",
    )
    parser.add_argument(
        "--metadata-name",
        default=DEFAULT_METADATA_NAME,
        help=f"Root metadata filename under --out-root. Default: {DEFAULT_METADATA_NAME}.",
    )
    parser.add_argument("--page-size", type=_positive_int, default=100, help="Catalog page size.")
    parser.add_argument(
        "--row-limit",
        type=_positive_int,
        default=5_000_000,
        help="Socrata /resource GeoJSON fallback row limit.",
    )
    parser.add_argument("--sleep", type=float, default=0.0, help="Seconds to sleep between downloads.")
    parser.add_argument("--timeout", type=float, default=120.0, help="HTTP timeout in seconds.")
    parser.add_argument(
        "--override",
        action="store_true",
        dest="override",
        help="Re-download files even if they already exist in metadata or on disk.",
    )
    parser.add_argument("--list-cities", action="store_true", help="Print configured city ids and exit.")
    return parser


def _build_crawler(
    profile: CityProfile,
    args: argparse.Namespace,
    *,
    existing_datasets: dict[str, dict[str, Any]] | None = None,
    intermediate_writer=None,
):
    output_dir = args.out_root.resolve() / profile.output_dir_name
    if profile.portal_type == "socrata":
        return SocrataMapCrawler(
            profile,
            output_dir=output_dir,
            page_size=args.page_size,
            row_limit=args.row_limit,
            timeout=args.timeout,
            sleep_seconds=args.sleep,
            overwrite=args.override,
            existing_datasets=existing_datasets,
            intermediate_writer=intermediate_writer,
        )
    if profile.portal_type == "ckan":
        return CkanGeoJsonCrawler(
            profile,
            output_dir=output_dir,
            page_size=args.page_size,
            timeout=args.timeout,
            sleep_seconds=args.sleep,
            overwrite=args.override,
            existing_datasets=existing_datasets,
            intermediate_writer=intermediate_writer,
        )
    raise ValueError(f"Unsupported portal type for {profile.city_id}: {profile.portal_type}")


def _ordered_city_metadata(city_metadata_by_id: dict[str, dict[str, Any]]) -> list[dict[str, Any]]:
    ordered = [
        city_metadata_by_id[city_id]
        for city_id in DEFAULT_CITY_ORDER
        if city_id in city_metadata_by_id
    ]
    for city_id, city_metadata in city_metadata_by_id.items():
        if city_id not in DEFAULT_CITY_ORDER:
            ordered.append(city_metadata)
    return ordered


def _crawler_accepts_intermediate_writer() -> bool:
    try:
        sig = inspect.signature(_build_crawler)
    except Exception:
        return False
    return "intermediate_writer" in sig.parameters


def _filter_existing_errors(profile: CityProfile, errors: list[Any]) -> list[Any]:
    filtered: list[Any] = []
    for error in errors:
        if isinstance(error, dict) and should_skip_dataset_name(profile, str(error.get("name") or "")):
            continue
        filtered.append(error)
    return filtered


def run(args: argparse.Namespace) -> dict[str, Any]:
    profiles = parse_city_list(args.cities)
    args.out_root = args.out_root.resolve()
    args.out_root.mkdir(parents=True, exist_ok=True)
    metadata_path = args.out_root / args.metadata_name
    existing_root_metadata = load_root_metadata(metadata_path)
    city_metadata_by_id: dict[str, dict[str, Any]] = {}
    for entry in existing_root_metadata:
        city_id = str(entry.get("city_id") or "").strip()
        if city_id:
            city_metadata_by_id[city_id] = entry

    print(f"[config] out_root={args.out_root}")
    print(f"[config] cities={','.join(profile.city_id for profile in profiles)}")
    print(f"[config] sample={args.sample if args.sample is not None else 'all'}")
    print(f"[config] override={bool(args.override)}")
    print("[config] format=geojson")
    print(f"[config] city_workers={len(profiles)}")

    summary: dict[str, Any] = {"cities": {}}
    any_failed = False
    initial_city_metadata_by_id = dict(city_metadata_by_id)
    metadata_lock = Lock()
    supports_intermediate_writer = _crawler_accepts_intermediate_writer()

    def _make_intermediate_writer(p: CityProfile):
        def _writer(partial_result: dict[str, Any]) -> None:
            try:
                city_data_dir = args.out_root / p.output_dir_name
                partial_result = dict(partial_result)
                partial_result["data_dir"] = str(city_data_dir.resolve())
                city_meta = build_city_metadata(p, partial_result)

                with metadata_lock:
                    city_metadata_by_id[p.city_id] = city_meta
                    final_list = _ordered_city_metadata(city_metadata_by_id)
                    write_root_metadata(metadata_path, final_list)
                print(f"[metadata] wrote {metadata_path} (intermediate)")
            except Exception:
                # Do not let metadata writing interrupt crawling.
                return

        return _writer

    def _run_city(profile: CityProfile) -> tuple[CityProfile, dict[str, Any], dict[str, Any]]:
        existing_city_metadata = initial_city_metadata_by_id.get(profile.city_id) or {}
        existing_dataset_records = list(existing_city_metadata.get("datasets") or [])
        existing_datasets = index_dataset_records(existing_dataset_records)
        intermediate_writer = _make_intermediate_writer(profile)

        if supports_intermediate_writer:
            crawler = _build_crawler(
                profile,
                args,
                existing_datasets=existing_datasets,
                intermediate_writer=intermediate_writer,
            )
        else:
            crawler = _build_crawler(
                profile,
                args,
                existing_datasets=existing_datasets,
            )

        result = crawler.run(download_limit=args.sample)
        city_data_dir = args.out_root / profile.output_dir_name
        result["data_dir"] = str(city_data_dir.resolve())
        combined_errors = _filter_existing_errors(profile, list(existing_city_metadata.get("errors") or []))
        combined_errors.extend(result.get("errors") or [])
        result["errors"] = combined_errors
        city_metadata = build_city_metadata(profile, result)
        return profile, result, city_metadata

    with ThreadPoolExecutor(max_workers=max(1, len(profiles))) as executor:
        future_to_profile = {executor.submit(_run_city, profile): profile for profile in profiles}
        for future in as_completed(future_to_profile):
            profile = future_to_profile[future]
            try:
                completed_profile, result, city_metadata = future.result()
            except Exception as exc:  # noqa: BLE001
                any_failed = True
                summary["cities"][profile.city_id] = {"status": "failed", "error": repr(exc)}
                print(f"[failed] {profile.city_id}: {exc!r}", file=sys.stderr)
                continue

            city_data_dir = args.out_root / completed_profile.output_dir_name
            with metadata_lock:
                city_metadata_by_id[completed_profile.city_id] = city_metadata
            summary["cities"][completed_profile.city_id] = {
                "status": "ok",
                "data_dir": str(city_data_dir.resolve()),
                "downloaded_count": result.get("meta", {}).get("downloaded_count", 0),
                "error_count": result.get("meta", {}).get("error_count", 0),
            }

    with metadata_lock:
        final_city_metadata = _ordered_city_metadata(city_metadata_by_id)

    if not any_failed:
        write_root_metadata(metadata_path, final_city_metadata)
        print(f"[metadata] wrote {metadata_path}")
        persisted_root_metadata = load_root_metadata(metadata_path) or final_city_metadata
        column_type_path = args.out_root / DEFAULT_COLUMN_TYPE_NAME
        write_column_type_metadata(column_type_path, persisted_root_metadata)
        print(f"[columntype] wrote {column_type_path}")
    else:
        print(f"[metadata] skipped writing {metadata_path} because one or more cities failed", file=sys.stderr)

    summary["metadata"] = str(metadata_path.resolve())
    summary["metadata_written"] = not any_failed
    if not any_failed:
        summary["columntype"] = str((args.out_root / DEFAULT_COLUMN_TYPE_NAME).resolve())
    return summary


def main(argv: list[str] | None = None) -> int:
    parser = build_argument_parser()
    args = parser.parse_args(argv)
    if args.list_cities:
        for city_id in DEFAULT_CITY_ORDER:
            profile = CITY_PROFILES[city_id]
            print(f"{city_id}\t{profile.label}\t{profile.portal_type}\t{profile.output_dir_name}")
        return 0

    try:
        summary = run(args)
    except ValueError as exc:
        parser.error(str(exc))
    if any(city.get("status") == "failed" for city in summary["cities"].values()):
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
