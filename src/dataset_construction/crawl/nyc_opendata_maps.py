"""Crawler for NYC Open Data map assets and their CSV exports."""
from __future__ import annotations

import argparse
import json
import os
import re
import shutil
import time
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Mapping
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

DISCOVERY_API_URL = "https://api.us.socrata.com/api/catalog/v1"
VIEW_METADATA_URL_TEMPLATE = "https://data.cityofnewyork.us/api/views/{view_id}.json"
CSV_EXPORT_URL_PATTERNS = (
    "https://data.cityofnewyork.us/api/v3/views/{view_id}/export?format=csv",
    "https://data.cityofnewyork.us/api/views/{view_id}/rows.csv?accessType=DOWNLOAD",
)
DEFAULT_OUTPUT_DIR = Path("data/nyc-opendata")
DEFAULT_MANIFEST_NAME = "nyc_opendata_maps.json"
DEFAULT_EXPECTED_COUNT = 293


class CrawlError(RuntimeError):
    """Raised when the crawler cannot complete a required fetch."""


@dataclass(slots=True)
class NycOpenDataMapRecord:
    """Manifest entry for one NYC Open Data map asset."""

    asset_id: str
    csv_view_id: str
    name: str
    description: str
    tags: list[str]
    last_updated: str | None
    views: int | None
    asset_url: str | None
    csv_download_url: str
    csv_filename: str

    def to_dict(self) -> dict[str, Any]:
        """Serialize the record to a JSON-compatible dictionary."""
        return asdict(self)


def _coerce_identifier(value: Any) -> str | None:
    """Normalize a Socrata identifier that may appear in different shapes."""
    if not value:
        return None
    if isinstance(value, str):
        return value.strip() or None
    if isinstance(value, Mapping):
        for key in ("id", "fxf", "uid", "identifier"):
            candidate = value.get(key)
            if isinstance(candidate, str) and candidate.strip():
                return candidate.strip()
    return None


def sanitize_filename(value: str) -> str:
    """Create a filesystem-safe slug from a dataset name."""
    slug = re.sub(r"[^A-Za-z0-9]+", "_", value).strip("_").lower()
    return slug or "dataset"


def make_csv_filename(name: str, csv_view_id: str) -> str:
    """Build a deterministic CSV filename for a downloaded dataset."""
    return f"{sanitize_filename(name)}_{csv_view_id}.csv"


def extract_tags(classification: Mapping[str, Any] | None) -> list[str]:
    """Merge domain tags and regular tags while preserving order."""
    if not classification:
        return []

    merged: list[str] = []
    seen: set[str] = set()
    for key in ("domain_tags", "tags"):
        values = classification.get(key) or []
        if not isinstance(values, list):
            continue
        for value in values:
            if not isinstance(value, str):
                continue
            normalized = value.strip()
            if normalized and normalized not in seen:
                seen.add(normalized)
                merged.append(normalized)
    return merged


def extract_last_updated(resource: Mapping[str, Any]) -> str | None:
    """Select the most relevant 'last updated' timestamp for the asset."""
    for key in ("updatedAt", "data_updated_at", "metadata_updated_at"):
        value = resource.get(key)
        if isinstance(value, str) and value.strip():
            return value
    return None


def extract_views(resource: Mapping[str, Any]) -> int | None:
    """Read the total page view count when present."""
    page_views = resource.get("page_views")
    if isinstance(page_views, Mapping):
        total = page_views.get("page_views_total")
        if isinstance(total, int):
            return total
        if isinstance(total, str) and total.isdigit():
            return int(total)
    return None


def build_export_candidate_ids(
    asset_id: str,
    resource: Mapping[str, Any],
    view_metadata: Mapping[str, Any] | None = None,
) -> list[str]:
    """Collect possible view identifiers that may expose the CSV export."""
    candidates: list[str] = []

    def add(candidate: Any) -> None:
        normalized = _coerce_identifier(candidate)
        if normalized and normalized not in candidates:
            candidates.append(normalized)

    for parent in resource.get("parent_fxf") or []:
        add(parent)

    if view_metadata:
        for key in (
            "tableId",
            "table_id",
            "parentUid",
            "parent_uid",
            "rootUid",
            "root_uid",
            "source_fxf",
            "id",
        ):
            add(view_metadata.get(key))

        query = view_metadata.get("query")
        if isinstance(query, Mapping):
            for key in ("originalViewId", "sourceViewId", "source_view_id"):
                add(query.get(key))

        for field in ("childViews", "child_views"):
            children = view_metadata.get(field) or []
            if isinstance(children, list):
                for child in children:
                    add(child)

    add(asset_id)
    return candidates


class NycOpenDataMapsCrawler:
    """Fetch all NYC Open Data map assets and their CSV exports."""

    def __init__(
        self,
        output_dir: str | Path = DEFAULT_OUTPUT_DIR,
        manifest_name: str = DEFAULT_MANIFEST_NAME,
        page_size: int = 100,
        expected_count: int | None = DEFAULT_EXPECTED_COUNT,
        timeout: float = 120.0,
        sleep_seconds: float = 0.0,
        overwrite: bool = False,
    ) -> None:
        self.output_dir = Path(output_dir)
        self.manifest_path = self.output_dir / manifest_name
        self.page_size = page_size
        self.expected_count = expected_count
        self.timeout = timeout
        self.sleep_seconds = sleep_seconds
        self.overwrite = overwrite
        self.app_token = os.environ.get("SOCRATA_APP_TOKEN")
        self._view_metadata_cache: dict[str, dict[str, Any]] = {}

    @property
    def default_headers(self) -> dict[str, str]:
        """Headers shared by all outbound HTTP requests."""
        headers = {
            "User-Agent": "SpatialText2SQL NYC Open Data crawler/1.0",
            "Accept": "application/json, text/csv, */*",
        }
        if self.app_token:
            headers["X-App-Token"] = self.app_token
        return headers

    def run(self) -> Path:
        """Run the full crawl with incremental manifest updates and resume support."""
        self.output_dir.mkdir(parents=True, exist_ok=True)
        completed_records = self._load_existing_records()
        self._prepare_manifest_file(completed_records)
        completed_asset_ids = set(completed_records)

        catalog_results = self.fetch_catalog_assets()
        if self.expected_count is not None and len(catalog_results) != self.expected_count:
            raise CrawlError(
                "Unexpected record count from NYC Open Data maps listing: "
                f"expected {self.expected_count}, got {len(catalog_results)}."
            )

        total = len(catalog_results)
        skipped = 0
        written = 0
        for index, asset_payload in enumerate(catalog_results, start=1):
            resource = asset_payload.get("resource") or {}
            asset_id = resource.get("id", "<unknown>")

            if asset_id in completed_asset_ids:
                skipped += 1
                print(f"[{index}/{total}] Skipping {asset_id} (already recorded).")
                continue

            print(f"[{index}/{total}] Processing {asset_id}...")
            record = self._process_asset(asset_payload)
            self._append_manifest_record(record)
            completed_asset_ids.add(record.asset_id)
            written += 1
            if self.sleep_seconds > 0:
                time.sleep(self.sleep_seconds)

        print(
            f"Saved manifest to {self.manifest_path} "
            f"(new records: {written}, skipped existing: {skipped}, total recorded: {len(completed_asset_ids)})"
        )
        return self.manifest_path

    def fetch_catalog_assets(self) -> list[dict[str, Any]]:
        """Page through the Socrata discovery API and collect all map assets."""
        offset = 0
        assets: list[dict[str, Any]] = []
        seen_ids: set[str] = set()

        while True:
            payload = self._get_json(
                DISCOVERY_API_URL,
                {
                    "search_context": "data.cityofnewyork.us",
                    "only": "map",
                    "limit": self.page_size,
                    "offset": offset,
                },
            )
            page_results = payload.get("results") or []
            if not isinstance(page_results, list) or not page_results:
                break

            for item in page_results:
                if not isinstance(item, Mapping):
                    continue
                resource = item.get("resource") or {}
                asset_id = resource.get("id")
                if isinstance(asset_id, str) and asset_id not in seen_ids:
                    assets.append(dict(item))
                    seen_ids.add(asset_id)

            offset += len(page_results)
            if len(page_results) < self.page_size:
                break

        return assets

    def _process_asset(self, asset_payload: Mapping[str, Any]) -> NycOpenDataMapRecord:
        """Download the asset's CSV and build its manifest record."""
        resource = asset_payload.get("resource") or {}
        classification = asset_payload.get("classification") or {}
        asset_id = resource["id"]
        csv_view_id, csv_download_url, csv_filename = self._download_csv_for_asset(asset_payload)

        return NycOpenDataMapRecord(
            asset_id=asset_id,
            csv_view_id=csv_view_id,
            name=(resource.get("name") or "").strip(),
            description=(resource.get("description") or "").strip(),
            tags=extract_tags(classification),
            last_updated=extract_last_updated(resource),
            views=extract_views(resource),
            asset_url=asset_payload.get("link") or asset_payload.get("permalink"),
            csv_download_url=csv_download_url,
            csv_filename=csv_filename,
        )

    def _download_csv_for_asset(
        self,
        asset_payload: Mapping[str, Any],
    ) -> tuple[str, str, str]:
        """Resolve the correct export endpoint and download the CSV file."""
        resource = asset_payload.get("resource") or {}
        asset_id = resource["id"]
        name = (resource.get("name") or asset_id).strip()

        initial_candidates = build_export_candidate_ids(asset_id, resource)
        downloaded = self._try_candidates(name, initial_candidates)
        if downloaded is not None:
            return downloaded

        view_metadata = self.get_view_metadata(asset_id)
        full_candidates = build_export_candidate_ids(asset_id, resource, view_metadata)
        extra_candidates = [candidate for candidate in full_candidates if candidate not in initial_candidates]
        downloaded = self._try_candidates(name, extra_candidates)
        if downloaded is not None:
            return downloaded

        raise CrawlError(f"Could not resolve a CSV export endpoint for asset {asset_id}.")

    def _try_candidates(
        self,
        name: str,
        candidate_view_ids: Iterable[str],
    ) -> tuple[str, str, str] | None:
        """Try candidate view identifiers until one successfully exports CSV."""
        for view_id in candidate_view_ids:
            csv_filename = make_csv_filename(name, view_id)
            csv_path = self.output_dir / csv_filename

            for download_url in self._build_csv_export_urls(view_id):
                if csv_path.exists() and not self.overwrite:
                    return view_id, download_url, csv_filename

                try:
                    self._download_csv(download_url, csv_path)
                    return view_id, download_url, csv_filename
                except CrawlError:
                    if csv_path.exists():
                        csv_path.unlink()
                    continue

        return None

    def get_view_metadata(self, view_id: str) -> dict[str, Any]:
        """Fetch and cache the Socrata metadata for a specific view."""
        if view_id not in self._view_metadata_cache:
            url = VIEW_METADATA_URL_TEMPLATE.format(view_id=view_id)
            self._view_metadata_cache[view_id] = self._get_json(url)
        return self._view_metadata_cache[view_id]

    def _build_csv_export_urls(self, view_id: str) -> tuple[str, ...]:
        """Construct candidate CSV export URLs for a view identifier."""
        return tuple(pattern.format(view_id=view_id) for pattern in CSV_EXPORT_URL_PATTERNS)

    def _get_json(self, url: str, params: Mapping[str, Any] | None = None) -> dict[str, Any]:
        """Perform a GET request and parse the JSON response."""
        if params:
            url = f"{url}?{urlencode(params, doseq=True)}"

        request = Request(url, headers=self.default_headers)
        try:
            with urlopen(request, timeout=self.timeout) as response:
                payload = response.read()
        except (HTTPError, URLError) as exc:
            raise CrawlError(f"GET {url} failed: {exc}") from exc

        try:
            data = json.loads(payload.decode("utf-8"))
        except json.JSONDecodeError as exc:
            raise CrawlError(f"GET {url} did not return valid JSON.") from exc

        if not isinstance(data, dict):
            raise CrawlError(f"GET {url} returned unexpected JSON structure.")
        return data

    def _download_csv(self, download_url: str, destination: Path) -> None:
        """Download a CSV export to the local filesystem."""
        request = Request(download_url, headers=self.default_headers)
        try:
            with urlopen(request, timeout=self.timeout) as response:
                first_chunk = response.read(4096)
                if not first_chunk:
                    raise CrawlError(f"CSV export {download_url} returned an empty body.")

                content_type = (response.headers.get("Content-Type") or "").lower()
                if "html" in content_type or "json" in content_type or self._looks_like_error_payload(first_chunk):
                    raise CrawlError(
                        f"CSV export {download_url} returned unexpected content type '{content_type or 'unknown'}'."
                    )

                with destination.open("wb") as sink:
                    sink.write(first_chunk)
                    shutil.copyfileobj(response, sink)
        except (HTTPError, URLError) as exc:
            raise CrawlError(f"CSV download failed for {download_url}: {exc}") from exc

    @staticmethod
    def _looks_like_error_payload(first_chunk: bytes) -> bool:
        """Heuristic guard against saving HTML or JSON error pages as CSV."""
        sample = first_chunk.lstrip().lower()
        return sample.startswith(b"<") or sample.startswith(b"{") or sample.startswith(b"[")

    def _load_existing_records(self) -> dict[str, dict[str, Any]]:
        """Load previously recorded entries so interrupted runs can resume."""
        if not self.manifest_path.exists():
            return {}

        raw_text = self.manifest_path.read_text(encoding="utf-8").strip()
        if not raw_text:
            return {}

        try:
            payload = json.loads(raw_text)
        except json.JSONDecodeError as exc:
            raise CrawlError(f"Manifest file is not valid JSON: {self.manifest_path}") from exc

        if isinstance(payload, dict):
            payload = payload.get("records", [])

        if not isinstance(payload, list):
            raise CrawlError(f"Manifest file must contain a JSON array of records: {self.manifest_path}")

        records: dict[str, dict[str, Any]] = {}
        for item in payload:
            if not isinstance(item, Mapping):
                continue
            asset_id = item.get("asset_id")
            if isinstance(asset_id, str) and asset_id:
                records[asset_id] = dict(item)
        return records

    def _prepare_manifest_file(self, completed_records: Mapping[str, dict[str, Any]]) -> None:
        """Ensure the manifest exists and uses the append-friendly JSON array format."""
        if not self.manifest_path.exists():
            self.manifest_path.write_text("[]\n", encoding="utf-8")
            return

        raw_text = self.manifest_path.read_text(encoding="utf-8").strip()
        if not raw_text:
            self.manifest_path.write_text("[]\n", encoding="utf-8")
            return

        payload = json.loads(raw_text)
        if isinstance(payload, list):
            return

        serialized = json.dumps(list(completed_records.values()), ensure_ascii=False, indent=2)
        self.manifest_path.write_text(f"{serialized}\n", encoding="utf-8")

    def _append_manifest_record(self, record: NycOpenDataMapRecord) -> None:
        """Append one completed record to the manifest while keeping valid JSON."""
        if not self.manifest_path.exists():
            self.manifest_path.write_text("[]\n", encoding="utf-8")

        serialized = json.dumps(record.to_dict(), ensure_ascii=False, indent=2)

        with self.manifest_path.open("r+b") as sink:
            last_pos = self._seek_last_non_whitespace(sink)
            if last_pos is None:
                sink.seek(0)
                sink.write(b"[]\n")
                sink.truncate()
                last_pos = 1

            sink.seek(last_pos)
            last_char = sink.read(1)
            if last_char != b"]":
                raise CrawlError(f"Manifest file does not end with a JSON array closing bracket: {self.manifest_path}")

            is_empty = self._json_array_is_empty(sink, last_pos)
            sink.seek(last_pos)

            prefix = "\n" if is_empty else ",\n"
            payload = f"{prefix}{serialized}\n]\n".encode("utf-8")
            sink.write(payload)
            sink.truncate()

    @staticmethod
    def _seek_last_non_whitespace(handle) -> int | None:
        """Return the position of the last non-whitespace byte in a file."""
        handle.seek(0, os.SEEK_END)
        position = handle.tell() - 1
        while position >= 0:
            handle.seek(position)
            char = handle.read(1)
            if char not in b" \t\r\n":
                return position
            position -= 1
        return None

    @staticmethod
    def _json_array_is_empty(handle, closing_bracket_pos: int) -> bool:
        """Check whether the JSON array currently contains any records."""
        position = closing_bracket_pos - 1
        while position >= 0:
            handle.seek(position)
            char = handle.read(1)
            if char in b" \t\r\n":
                position -= 1
                continue
            return char == b"["
        return True


def build_argument_parser() -> argparse.ArgumentParser:
    """Create the command-line interface for the crawler."""
    parser = argparse.ArgumentParser(
        description="Crawl all NYC Open Data map assets and save their CSV exports plus metadata."
    )
    parser.add_argument(
        "--output-dir",
        default=str(DEFAULT_OUTPUT_DIR),
        help="Directory used to store downloaded CSV files and the manifest JSON.",
    )
    parser.add_argument(
        "--manifest-name",
        default=DEFAULT_MANIFEST_NAME,
        help="Filename of the generated JSON manifest inside the output directory.",
    )
    parser.add_argument(
        "--page-size",
        type=int,
        default=100,
        help="Page size used for the Socrata discovery API.",
    )
    parser.add_argument(
        "--expected-count",
        type=int,
        default=DEFAULT_EXPECTED_COUNT,
        help="Expected number of records. Use a negative value to disable the check.",
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=120.0,
        help="Per-request timeout in seconds.",
    )
    parser.add_argument(
        "--sleep-seconds",
        type=float,
        default=0.0,
        help="Optional delay between processed records to reduce request pressure.",
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Re-download CSV files even if the local file already exists.",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    """CLI entry point."""
    args = build_argument_parser().parse_args(argv)
    expected_count = None if args.expected_count is not None and args.expected_count < 0 else args.expected_count

    crawler = NycOpenDataMapsCrawler(
        output_dir=args.output_dir,
        manifest_name=args.manifest_name,
        page_size=args.page_size,
        expected_count=expected_count,
        timeout=args.timeout,
        sleep_seconds=args.sleep_seconds,
        overwrite=args.overwrite,
    )
    crawler.run()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
