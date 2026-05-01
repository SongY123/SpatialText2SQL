"""Reusable Socrata map-asset crawler that downloads GeoJSON exports."""
from __future__ import annotations

from dataclasses import asdict, dataclass
from pathlib import Path
import time
from typing import Any, Iterable, Mapping

from .common import (
    CrawlError,
    default_headers,
    download_geojson,
    http_get_json,
    make_geojson_filename,
)
from .profiles import CityProfile, should_skip_dataset_name


DISCOVERY_API_URL = "https://api.us.socrata.com/api/catalog/v1"
GEOJSON_EXPORT_PATTERNS = (
    "https://{domain}/api/views/{view_id}/rows.geojson?accessType=DOWNLOAD",
    "https://{domain}/resource/{view_id}.geojson?$limit={row_limit}",
    "https://{domain}/api/v3/views/{view_id}/export?format=geojson",
)


@dataclass(slots=True)
class SocrataMapRecord:
    """Manifest entry for one Socrata map asset."""

    city: str
    domain: str
    asset_id: str
    geojson_view_id: str
    id: str
    name: str
    description: str
    tags: list[str]
    columns: list[dict[str, str]]
    last_updated: str | None
    views: int | None
    asset_url: str | None
    source_link: str
    download_url: str
    geojson_filename: str
    path: str
    geojson_path: str
    bytes: int
    download_format: str
    skipped_existing: bool

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def _coerce_identifier(value: Any) -> str | None:
    """Normalize a Socrata identifier that may appear in different shapes."""
    if not value:
        return None
    if isinstance(value, str):
        normalized = value.strip().lower()
        return normalized or None
    if isinstance(value, Mapping):
        for key in ("id", "fxf", "uid", "identifier"):
            candidate = value.get(key)
            if isinstance(candidate, str) and candidate.strip():
                return candidate.strip().lower()
    return None


def metadata_dataset_name(asset_payload: Mapping[str, Any]) -> str:
    """Return the dataset name that will be written into root metadata."""
    resource = asset_payload.get("resource") or {}
    asset_id = str(resource.get("id") or "").strip().lower()
    return str(resource.get("name") or asset_id).strip()


def _as_iterable(value: Any) -> Iterable[Any]:
    if value is None:
        return ()
    if isinstance(value, (list, tuple)):
        return value
    return (value,)


def _merge_unique_strings(*groups: Iterable[str]) -> list[str]:
    merged: list[str] = []
    seen: set[str] = set()
    for group in groups:
        for value in group:
            normalized = value.strip()
            if normalized and normalized not in seen:
                seen.add(normalized)
                merged.append(normalized)
    return merged


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


def extract_view_tags(view_metadata: Mapping[str, Any] | None) -> list[str]:
    """Read Socrata tags from the view metadata endpoint."""
    if not view_metadata:
        return []
    tags = view_metadata.get("tags") or []
    if not isinstance(tags, list):
        return []
    return [str(tag).strip() for tag in tags if isinstance(tag, str) and str(tag).strip()]


def extract_view_columns(view_metadata: Mapping[str, Any] | None) -> list[dict[str, str]]:
    """Normalize Socrata column metadata for downstream metadata output."""
    if not view_metadata:
        return []

    columns = view_metadata.get("columns") or []
    if not isinstance(columns, list):
        return []

    normalized: list[dict[str, str]] = []
    for column in columns:
        if not isinstance(column, Mapping):
            continue
        name = str(column.get("name") or "").strip()
        if not name:
            continue
        normalized.append(
            {
                "name": name,
                "description": str(column.get("description") or "").strip(),
                "type": str(column.get("dataTypeName") or column.get("renderTypeName") or "").strip(),
            }
        )
    return normalized


def extract_last_updated(resource: Mapping[str, Any]) -> str | None:
    """Select the most relevant last-updated timestamp for the asset."""
    for key in ("updatedAt", "data_updated_at", "metadata_updated_at", "rowsUpdatedAt", "viewLastModified"):
        value = resource.get(key)
        if isinstance(value, str) and value.strip():
            return value
    return None


def extract_views(resource: Mapping[str, Any]) -> int | None:
    """Read the total page-view count when present."""
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
    """Collect possible view identifiers that may expose a GeoJSON export."""
    candidates: list[str] = []

    def add(candidate: Any) -> None:
        normalized = _coerce_identifier(candidate)
        if normalized and normalized not in candidates:
            candidates.append(normalized)

    for parent in _as_iterable(resource.get("parent_fxf")):
        add(parent)

    for key in ("tableId", "table_id", "parentUid", "parent_uid", "rootUid", "root_uid", "source_fxf"):
        add(resource.get(key))

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
            for child in _as_iterable(view_metadata.get(field)):
                add(child)

    add(asset_id)
    return candidates


class SocrataMapCrawler:
    """Fetch Socrata map assets and download their GeoJSON exports."""

    def __init__(
        self,
        profile: CityProfile,
        *,
        output_dir: Path,
        page_size: int = 100,
        row_limit: int = 5_000_000,
        timeout: float = 120.0,
        sleep_seconds: float = 0.0,
        overwrite: bool = False,
        existing_datasets: dict[str, dict[str, Any]] | None = None,
        intermediate_writer=None,
    ) -> None:
        if profile.portal_type != "socrata":
            raise ValueError(f"{profile.city_id} is not a Socrata profile.")
        self.profile = profile
        self.output_dir = output_dir
        self.geojson_dir = self.output_dir / "geojson"
        self.page_size = max(1, min(100, page_size))
        self.row_limit = max(1, row_limit)
        self.timeout = timeout
        self.sleep_seconds = max(0.0, sleep_seconds)
        self.overwrite = overwrite
        self.existing_datasets = {str(key).strip().lower(): dict(value) for key, value in (existing_datasets or {}).items()}
        # optional callable to invoke with partial crawl results after each dataset
        self._intermediate_writer = intermediate_writer
        self._view_metadata_cache: dict[str, dict[str, Any]] = {}
        self._headers = default_headers()

    def run(self, *, download_limit: int | None = None) -> dict[str, Any]:
        """Run the crawl. ``download_limit`` applies per city after successful records."""
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.geojson_dir.mkdir(parents=True, exist_ok=True)

        datasets: list[dict[str, Any]] = []
        errors: list[dict[str, Any]] = []
        completed_count = 0
        catalog_count = 0
        sampled_count = 0
        skipped_name_count = 0
        skipped_existing_count = 0
        total_bytes = 0

        print(f"[city] {self.profile.city_id} ({self.profile.domain})")
        for index, asset_payload in enumerate(self.iter_catalog_map_assets(), start=1):
            catalog_count = index
            resource = asset_payload.get("resource") or {}
            asset_id = str(resource.get("id") or "").strip().lower()
            if not asset_id:
                continue

            metadata_name = metadata_dataset_name(asset_payload)
            if should_skip_dataset_name(self.profile, metadata_name):
                skipped_name_count += 1
                print(f"[skip] {asset_id} {metadata_name[:80]}")
                continue

            if download_limit is not None and sampled_count >= download_limit:
                break
            sampled_count += 1

            existing_record = self._match_existing_record(resource)
            if not self.overwrite and existing_record is not None:
                if self._record_file_exists(existing_record):
                    datasets.append(self._hydrate_existing_record(existing_record, asset_payload).to_dict())
                    skipped_existing_count += 1
                    continue

            print(f"[download] {completed_count + 1} {asset_id} {metadata_name[:80]}")
            try:
                record = self._process_asset(asset_payload)
            except Exception as exc:  # noqa: BLE001
                errors.append(
                    {
                        "asset_id": asset_id,
                        "name": metadata_name,
                        "error": repr(exc),
                    }
                )
                print(f"          [error] {exc!r}")
                time.sleep(self.sleep_seconds)
                continue

            datasets.append(record.to_dict())
            completed_count += 1
            skipped_existing_count += int(record.skipped_existing)
            total_bytes += record.bytes
            # write intermediate metadata after each completed dataset
            if self._intermediate_writer:
                partial_meta = {
                    "city": self.profile.city_id,
                    "portal_type": "socrata",
                    "downloaded_count": completed_count,
                    "skipped_name_count": skipped_name_count,
                    "error_count": len(errors),
                }
                partial_result = {"meta": partial_meta, "datasets": list(datasets), "errors": list(errors), "data_dir": str(self.output_dir.resolve())}
                try:
                    self._intermediate_writer(partial_result)
                except Exception:
                    # Intermediate metadata writer must not break the crawl
                    pass
            time.sleep(self.sleep_seconds)

        meta = {
            "city": self.profile.city_id,
            "city_label": self.profile.label,
            "portal_type": "socrata",
            "domain": self.profile.domain,
            "browse_url": self.profile.browse_url,
            "source_api": DISCOVERY_API_URL,
            "catalog_filter": "only=map",
            "catalog_seen_count": catalog_count,
            "downloaded_count": completed_count,
            "skipped_name_count": skipped_name_count,
            "skipped_existing_count": skipped_existing_count,
            "error_count": len(errors),
            "total_mb": round(total_bytes / 1024 / 1024, 3),
        }
        print(f"[done] {self.profile.city_id} downloaded={completed_count} errors={len(errors)}")
        return {"meta": meta, "datasets": datasets, "errors": errors}

    def iter_catalog_map_assets(self) -> Iterable[dict[str, Any]]:
        """Page through Socrata Discovery API map assets."""
        offset = 0
        seen_ids: set[str] = set()
        while True:
            payload = http_get_json(
                DISCOVERY_API_URL,
                {
                    "domains": self.profile.domain,
                    "search_context": self.profile.domain,
                    "only": "map",
                    "limit": self.page_size,
                    "offset": offset,
                },
                headers=self._headers,
                timeout=self.timeout,
            )
            page_results = payload.get("results") or []
            if not isinstance(page_results, list) or not page_results:
                break

            for item in page_results:
                if not isinstance(item, Mapping):
                    continue
                resource = item.get("resource") or {}
                asset_id = _coerce_identifier(resource.get("id"))
                if not asset_id or asset_id in seen_ids:
                    continue
                seen_ids.add(asset_id)
                yield dict(item)

            offset += len(page_results)
            if len(page_results) < self.page_size:
                break

    def fetch_catalog_assets(self, max_assets: int | None = None) -> list[dict[str, Any]]:
        """Eager helper used by tests and one-off inspection."""
        out: list[dict[str, Any]] = []
        for item in self.iter_catalog_map_assets():
            out.append(item)
            if max_assets is not None and len(out) >= max_assets:
                break
        return out

    def _match_existing_record(self, resource: Mapping[str, Any]) -> dict[str, Any] | None:
        asset_id = str(resource.get("id") or "").strip().lower()
        for candidate_id in build_export_candidate_ids(asset_id, resource):
            existing_record = self.existing_datasets.get(candidate_id)
            if existing_record is not None:
                return existing_record
        return None

    def _process_asset(self, asset_payload: Mapping[str, Any]) -> SocrataMapRecord:
        return self._build_record(
            asset_payload,
            *self._download_geojson_for_asset(asset_payload),
        )

    def _build_record(
        self,
        asset_payload: Mapping[str, Any],
        view_id: str,
        download_url: str,
        filename: str,
        path: Path,
        nbytes: int,
        skipped_existing: bool,
    ) -> SocrataMapRecord:
        resource = asset_payload.get("resource") or {}
        classification = asset_payload.get("classification") or {}
        asset_id = str(resource["id"]).strip().lower()
        name = metadata_dataset_name(asset_payload)
        view_metadata = self.get_view_metadata(view_id)

        return SocrataMapRecord(
            city=self.profile.city_id,
            domain=self.profile.domain,
            asset_id=asset_id,
            geojson_view_id=view_id,
            id=view_id,
            name=name,
            description=str(view_metadata.get("description") or resource.get("description") or "").strip(),
            tags=_merge_unique_strings(extract_tags(classification), extract_view_tags(view_metadata)),
            columns=extract_view_columns(view_metadata),
            last_updated=extract_last_updated(resource),
            views=extract_views(resource),
            asset_url=asset_payload.get("link") or asset_payload.get("permalink"),
            source_link=self._view_metadata_url(view_id),
            download_url=download_url,
            geojson_filename=filename,
            path=str(path.resolve()),
            geojson_path=str(path.resolve()),
            bytes=nbytes,
            download_format="geojson",
            skipped_existing=skipped_existing,
        )

    def _hydrate_existing_record(
        self,
        existing_record: Mapping[str, Any],
        asset_payload: Mapping[str, Any],
    ) -> SocrataMapRecord:
        path = Path(str(existing_record.get("geojson_path") or existing_record.get("path") or ""))
        view_id = str(existing_record.get("geojson_view_id") or existing_record.get("id") or "").strip().lower()
        if not view_id:
            resource = asset_payload.get("resource") or {}
            view_id = str(resource.get("id") or "").strip().lower()
        download_url = str(existing_record.get("download_url") or self._build_geojson_export_urls(view_id)[0]).strip()
        return self._build_record(
            asset_payload,
            view_id,
            download_url,
            path.name,
            path,
            path.stat().st_size,
            True,
        )

    def _download_geojson_for_asset(
        self,
        asset_payload: Mapping[str, Any],
    ) -> tuple[str, str, str, Path, int, bool]:
        resource = asset_payload.get("resource") or {}
        asset_id = str(resource["id"]).strip().lower()
        name = metadata_dataset_name(asset_payload)

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

        raise CrawlError(f"Could not resolve a GeoJSON export endpoint for asset {asset_id}.")

    def _try_candidates(
        self,
        name: str,
        candidate_view_ids: Iterable[str],
    ) -> tuple[str, str, str, Path, int, bool] | None:
        for view_id in candidate_view_ids:
            filename = make_geojson_filename(name, view_id)
            path = self.geojson_dir / filename

            for download_url in self._build_geojson_export_urls(view_id):
                if path.exists() and not self.overwrite and path.stat().st_size > 0:
                    return view_id, download_url, filename, path, path.stat().st_size, True

                try:
                    print(f"          [download_link] {download_url}")
                    nbytes, _ = download_geojson(
                        download_url,
                        path,
                        headers=self._headers,
                        timeout=max(300.0, self.timeout),
                    )
                    return view_id, download_url, filename, path, nbytes, False
                except CrawlError:
                    if path.exists():
                        path.unlink()
                    continue

        return None

    def get_view_metadata(self, view_id: str) -> dict[str, Any]:
        """Fetch and cache Socrata view metadata."""
        if view_id not in self._view_metadata_cache:
            self._view_metadata_cache[view_id] = http_get_json(
                self._view_metadata_url(view_id),
                headers=self._headers,
                timeout=self.timeout,
            )
        return self._view_metadata_cache[view_id]

    def _view_metadata_url(self, view_id: str) -> str:
        return f"https://{self.profile.domain}/api/views/{view_id}.json"

    def _build_geojson_export_urls(self, view_id: str) -> tuple[str, ...]:
        return tuple(
            pattern.format(domain=self.profile.domain, view_id=view_id, row_limit=self.row_limit)
            for pattern in GEOJSON_EXPORT_PATTERNS
        )

    @staticmethod
    def _record_file_exists(record: Mapping[str, Any]) -> bool:
        path = Path(str(record.get("geojson_path") or record.get("path") or ""))
        return bool(path.is_file() and path.stat().st_size > 0)
