"""Reusable CKAN crawler that downloads GeoJSON resources only."""
from __future__ import annotations

from pathlib import Path
import re
import time
from typing import Any, Iterable, Mapping
from urllib.parse import urlparse

from .common import (
    CrawlError,
    default_headers,
    download_geojson,
    http_get_json,
    sanitize_filename,
)
from .profiles import CityProfile, should_skip_dataset_name


def is_geojson_resource(resource: Mapping[str, Any]) -> bool:
    """Return True when a CKAN resource is a direct GeoJSON export."""
    url = str(resource.get("url") or "").strip().lower()
    fmt = str(resource.get("format") or "").strip().lower()
    mimetype = str(resource.get("mimetype") or resource.get("mimetype_inner") or "").strip().lower()
    if not url:
        return False
    geojson_markers = ("geojson", "geo+json", "f=geojson", "format=geojson")
    return any(marker in fmt or marker in mimetype or marker in url for marker in geojson_markers)


def pick_geojson_resource(dataset: Mapping[str, Any]) -> dict[str, Any] | None:
    """Pick the first GeoJSON resource from a CKAN package."""
    for resource in dataset.get("resources") or []:
        if isinstance(resource, Mapping) and is_geojson_resource(resource):
            return dict(resource)
    return None


def metadata_dataset_name(dataset: Mapping[str, Any]) -> str:
    """Return the dataset name that will be written into root metadata."""
    dataset_id = str(dataset.get("id") or dataset.get("name") or "").strip()
    dataset_slug = str(dataset.get("name") or dataset_id).strip()
    return str(dataset.get("title") or dataset_slug or dataset_id).strip()


def is_arcgis_service_resource(resource: Mapping[str, Any]) -> bool:
    """Return True when the resource points to an ArcGIS REST layer/service."""
    fmt = str(resource.get("format") or "").strip().lower()
    url = str(resource.get("url") or "").strip().lower()
    return "arcgis geoservices rest api" in fmt or "/featureserver/" in url or "/mapserver/" in url


def extract_ckan_tags(dataset: Mapping[str, Any]) -> list[Any]:
    """Preserve CKAN tag payloads for downstream normalization."""
    tags = dataset.get("tags") or []
    return list(tags) if isinstance(tags, list) else []


def extract_arcgis_columns(layer_metadata: Mapping[str, Any]) -> list[dict[str, str]]:
    """Normalize ArcGIS REST field metadata for downstream metadata output."""
    normalized: list[dict[str, str]] = []
    fields = layer_metadata.get("fields") or []
    if isinstance(fields, list):
        for field in fields:
            if not isinstance(field, Mapping):
                continue
            name = str(field.get("name") or field.get("alias") or "").strip()
            if not name:
                continue
            normalized.append(
                {
                    "name": name,
                    "description": str(field.get("alias") or field.get("description") or field.get("name") or "").strip(),
                    "type": str(field.get("type") or field.get("actualType") or field.get("sqlType") or "").strip(),
                }
            )

    geometry_type = str(layer_metadata.get("geometryType") or "").strip()
    if geometry_type:
        normalized.append(
            {
                "name": "geometry",
                "description": "Feature geometry",
                "type": geometry_type,
            }
        )
    return normalized


class CkanGeoJsonCrawler:
    """Fetch CKAN packages and download their GeoJSON resources."""

    def __init__(
        self,
        profile: CityProfile,
        *,
        output_dir: Path,
        page_size: int = 100,
        timeout: float = 120.0,
        sleep_seconds: float = 0.0,
        overwrite: bool = False,
        existing_datasets: dict[str, dict[str, Any]] | None = None,
        intermediate_writer=None,
    ) -> None:
        if profile.portal_type != "ckan":
            raise ValueError(f"{profile.city_id} is not a CKAN profile.")
        self.profile = profile
        self.output_dir = output_dir
        self.geojson_dir = self.output_dir / "geojson"
        self.page_size = max(1, min(200, page_size))
        self.timeout = timeout
        self.sleep_seconds = max(0.0, sleep_seconds)
        self.overwrite = overwrite
        self.existing_datasets = {str(key).strip().lower(): dict(value) for key, value in (existing_datasets or {}).items()}
        # optional callable to invoke with partial crawl results after each dataset
        self._intermediate_writer = intermediate_writer
        self._headers = default_headers(accept="application/json,text/plain,*/*", include_socrata_token=False)
        self._dataset_metadata_cache: dict[str, dict[str, Any]] = {}
        self._layer_metadata_cache: dict[str, dict[str, Any]] = {}

    def run(self, *, download_limit: int | None = None) -> dict[str, Any]:
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.geojson_dir.mkdir(parents=True, exist_ok=True)

        print(f"[city] {self.profile.city_id} ({self.profile.base_url})")
        candidates: list[dict[str, Any]] = []
        skipped_name_count = 0
        for dataset in self.iter_packages():
            if not isinstance(dataset, Mapping):
                continue
            dataset_id = str(dataset.get("id") or dataset.get("name") or "").strip()
            if not dataset_id:
                continue
            metadata_name = metadata_dataset_name(dataset)
            if should_skip_dataset_name(self.profile, metadata_name):
                skipped_name_count += 1
                print(f"[skip] {dataset_id} {metadata_name[:80]}")
                continue
            resource = pick_geojson_resource(dataset)
            if resource is None:
                continue
            candidates.append(dict(dataset))

        candidates.sort(key=self._package_sort_key)

        datasets: list[dict[str, Any]] = []
        errors: list[dict[str, Any]] = []
        completed_count = 0
        catalog_count = 0
        skipped_existing_count = 0
        total_bytes = 0

        for index, dataset in enumerate(candidates, start=1):
            if download_limit is not None and index > download_limit:
                break
            catalog_count = index

            dataset_id = str(dataset.get("id") or dataset.get("name") or "").strip()
            metadata_name = metadata_dataset_name(dataset)
            if not dataset_id:
                continue

            # resource selection and download will be handled by _download_dataset

            if not self.overwrite and dataset_id in self.existing_datasets:
                existing_record = self.existing_datasets[dataset_id]
                if self._record_file_exists(existing_record):
                    datasets.append(self._hydrate_existing_record(existing_record, dataset))
                    skipped_existing_count += 1
                    continue

            print(f"[download] {completed_count + 1} {dataset_id} {metadata_name[:80]}")
            try:
                record = self._download_dataset(dataset)
            except Exception as exc:  # noqa: BLE001
                errors.append({"dataset_id": dataset_id, "name": metadata_name, "error": repr(exc), "url": None})
                print(f"          [error] {exc!r}")
                time.sleep(self.sleep_seconds)
                continue

            datasets.append(record)
            completed_count += 1
            skipped_existing_count += int(record["skipped_existing"])
            total_bytes += int(record["bytes"])
            # write intermediate metadata after each completed dataset
            if self._intermediate_writer:
                partial_meta = {
                    "city": self.profile.city_id,
                    "portal_type": "ckan",
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
            "portal_type": "ckan",
            "domain": urlparse(self.profile.base_url).netloc,
            "browse_url": self.profile.browse_url,
            "base_url": self.profile.base_url,
            "package_search_fq": self.profile.ckan_fq,
            "catalog_seen_count": catalog_count,
            "downloaded_count": completed_count,
            "skipped_name_count": skipped_name_count,
            "skipped_existing_count": skipped_existing_count,
            "error_count": len(errors),
            "total_mb": round(total_bytes / 1024 / 1024, 3),
        }
        print(f"[done] {self.profile.city_id} downloaded={completed_count} errors={len(errors)}")
        return {"meta": meta, "datasets": datasets, "errors": errors}

    @staticmethod
    def _package_sort_key(dataset: Mapping[str, Any]) -> tuple[str, str]:
        # sort primarily by human-readable title, then by id for deterministic order
        title = metadata_dataset_name(dataset).lower()
        dataset_id = str(dataset.get("id") or dataset.get("name") or "").strip().lower()
        return (title, dataset_id)

    def iter_packages(self) -> Iterable[dict[str, Any]]:
        """Page through CKAN package_search."""
        base_url = self.profile.base_url.rstrip("/")
        start = 0
        while True:
            params = {"rows": str(self.page_size), "start": str(start)}
            if self.profile.ckan_fq.strip():
                params["fq"] = self.profile.ckan_fq.strip()
            payload = http_get_json(
                f"{base_url}/api/3/action/package_search",
                params,
                headers=self._headers,
                timeout=self.timeout,
            )
            if not payload.get("success"):
                raise CrawlError(f"CKAN package_search failed for {base_url}")
            result = payload.get("result") or {}
            packages = result.get("results") or []
            if not isinstance(packages, list) or not packages:
                break

            for package in packages:
                if isinstance(package, Mapping):
                    yield dict(package)

            start += len(packages)
            if len(packages) < self.page_size:
                break

    def _download_dataset(self, dataset: Mapping[str, Any]) -> dict[str, Any]:
        dataset = self._get_dataset_metadata(dataset)
        dataset_id = str(dataset.get("id") or dataset.get("name") or "").strip()
        dataset_name = str(dataset.get("name") or dataset_id).strip()
        metadata_name = metadata_dataset_name(dataset)
        page_link = self._page_link(dataset_name)
        source_link, columns = self._source_metadata(dataset)
        tags = extract_ckan_tags(dataset)

        # Try all geojson-like resources in the package until one downloads successfully.
        resources = [r for r in (dataset.get("resources") or []) if isinstance(r, Mapping) and is_geojson_resource(r)]
        if not resources:
            raise CrawlError(f"No GeoJSON resources found for CKAN package {dataset_id}")

        last_exc: Exception | None = None
        for resource in resources:
            resource_id = str(resource.get("id") or sanitize_filename(metadata_name, max_len=16)).strip()
            resource_token = re.sub(r"[^0-9a-zA-Z]+", "", resource_id)[:12] or "res"
            filename = f"{sanitize_filename(metadata_name)}__{resource_token}.geojson"
            path = self.geojson_dir / filename
            url = str(resource.get("url") or "").strip()
            if not url:
                last_exc = CrawlError(f"CKAN resource for {dataset_id} has no URL.")
                continue

            try:
                if path.exists() and not self.overwrite and path.stat().st_size > 0:
                    nbytes = path.stat().st_size
                    skipped_existing = True
                    server_filename = path.name
                else:
                    print(f"          [download_link] {url}")
                    nbytes, server_filename = download_geojson(
                        url,
                        path,
                        headers=self._download_headers(page_link),
                        timeout=max(300.0, self.timeout),
                    )
                    skipped_existing = False

                    # If server provided a different filename, rename the file
                    if server_filename and server_filename != filename:
                        sanitized_server_name = sanitize_filename(server_filename, max_len=200)
                        new_path = path.parent / sanitized_server_name
                        if not new_path.exists():
                            path.rename(new_path)
                            path = new_path
                            filename = sanitized_server_name

                return {
                    "city": self.profile.city_id,
                    "domain": urlparse(self.profile.base_url).netloc,
                    "id": dataset_id,
                    "dataset_id": dataset_id,
                    "resource_id": resource_id,
                    "name": metadata_name,
                    "description": str(dataset.get("notes") or "")[:500],
                    "tags": tags,
                    "columns": columns,
                    "resource_name": str(resource.get("name") or ""),
                    "portal_format_detected": str(resource.get("format") or "GeoJSON"),
                    "download_format": "geojson",
                    "download_url": url,
                    "source_link": source_link,
                    "geojson_filename": filename,
                    "path": str(path.resolve()),
                    "geojson_path": str(path.resolve()),
                    "bytes": nbytes,
                    "skipped_existing": skipped_existing,
                }
            except Exception as exc:  # noqa: BLE001
                last_exc = exc
                if path.exists():
                    try:
                        path.unlink()
                    except Exception:
                        pass
                continue

        # If we reach here, none of the resources succeeded
        raise CrawlError(f"All GeoJSON resource downloads failed for package {dataset_id}: {last_exc!r}")

    @staticmethod
    def _record_file_exists(record: Mapping[str, Any]) -> bool:
        path = Path(str(record.get("geojson_path") or record.get("path") or ""))
        return bool(path.is_file() and path.stat().st_size > 0)

    def _page_link(self, dataset_name: str) -> str:
        if dataset_name:
            return f"{self.profile.base_url.rstrip('/')}/dataset/{dataset_name}"
        return self.profile.base_url

    def _source_metadata(self, dataset: Mapping[str, Any]) -> tuple[str, list[dict[str, str]]]:
        source_link = self._page_link(str(dataset.get("name") or dataset.get("id") or "").strip())
        schema_resource_url = self._schema_resource_url(dataset)
        if not schema_resource_url:
            return source_link, []

        try:
            layer_metadata = self._get_layer_metadata(schema_resource_url)
        except CrawlError:
            return schema_resource_url, []
        return schema_resource_url, extract_arcgis_columns(layer_metadata)

    def _schema_resource_url(self, dataset: Mapping[str, Any]) -> str | None:
        for resource in dataset.get("resources") or []:
            if isinstance(resource, Mapping) and is_arcgis_service_resource(resource):
                url = str(resource.get("url") or "").strip()
                if url:
                    return url
        return None

    def _get_layer_metadata(self, url: str) -> dict[str, Any]:
        if url not in self._layer_metadata_cache:
            self._layer_metadata_cache[url] = http_get_json(
                url,
                {"f": "pjson"},
                headers=self._headers,
                timeout=self.timeout,
            )
        return self._layer_metadata_cache[url]

    def _hydrate_existing_record(
        self,
        existing_record: Mapping[str, Any],
        dataset: Mapping[str, Any],
    ) -> dict[str, Any]:
        dataset = self._get_dataset_metadata(dataset)
        path = Path(str(existing_record.get("geojson_path") or existing_record.get("path") or ""))
        source_link, columns = self._source_metadata(dataset)
        hydrated = dict(existing_record)
        hydrated["name"] = metadata_dataset_name(dataset)
        hydrated["description"] = str(dataset.get("notes") or "")[:500]
        hydrated["tags"] = extract_ckan_tags(dataset)
        hydrated["columns"] = columns
        hydrated["source_link"] = source_link
        hydrated["path"] = str(path.resolve())
        hydrated["geojson_path"] = str(path.resolve())
        hydrated["geojson_filename"] = path.name
        hydrated["bytes"] = path.stat().st_size
        hydrated["skipped_existing"] = True
        return hydrated

    def _get_dataset_metadata(self, dataset: Mapping[str, Any]) -> dict[str, Any]:
        dataset_id = str(dataset.get("id") or dataset.get("name") or "").strip()
        if not dataset_id:
            return dict(dataset)
        if dataset_id not in self._dataset_metadata_cache:
            payload = http_get_json(
                f"{self.profile.base_url.rstrip('/')}/api/3/action/package_show",
                {"id": dataset_id},
                headers=self._headers,
                timeout=self.timeout,
            )
            if not payload.get("success"):
                raise CrawlError(f"CKAN package_show failed for {dataset_id}")
            result = payload.get("result")
            if not isinstance(result, Mapping):
                raise CrawlError(f"CKAN package_show returned unexpected result for {dataset_id}")
            self._dataset_metadata_cache[dataset_id] = dict(result)
        return dict(self._dataset_metadata_cache[dataset_id])

    @staticmethod
    def _download_headers(source_link: str) -> dict[str, str]:
        accept = (
            "text/html,application/xhtml+xml,application/xml;q=0.9,"
            "application/geo+json,application/json,*/*;q=0.8"
        )
        headers = default_headers(accept=accept, include_socrata_token=False)
        headers["Referer"] = source_link
        headers["Upgrade-Insecure-Requests"] = "1"
        return headers
