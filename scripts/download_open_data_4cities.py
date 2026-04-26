#!/usr/bin/env python3
"""Download GeoJSON resources from CKAN portals (Boston, Phoenix open data, etc.)."""
from __future__ import annotations

import argparse
import json
import re
import time
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


DEFAULT_BASE_URL = "https://data.boston.gov"
DEFAULT_FQ_BOSTON = "organization:boston-maps"
DEFAULT_BASE_URL_PHOENIX = "https://www.phoenixopendata.com"
DEFAULT_FQ_PHOENIX = "groups:mapping"

CITY_PROFILES: dict[str, dict[str, str]] = {
    "boston": {
        "base_url": DEFAULT_BASE_URL,
        "fq_default": DEFAULT_FQ_BOSTON,
    },
    "phoenix": {
        "base_url": DEFAULT_BASE_URL_PHOENIX,
        "fq_default": DEFAULT_FQ_PHOENIX,
    },
}


def _slug(text: str, max_len: int = 100) -> str:
    text = re.sub(r"[^\w\s-]", "", text)
    text = re.sub(r"[-\s]+", "_", text.strip()).strip("_").lower()
    return (text[:max_len] or "dataset").lower()


def _headers() -> dict[str, str]:
    return {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/124.0.0.0 Safari/537.36"
        ),
        "Accept": "application/json,text/plain,*/*",
    }


def _http_get_json(url: str, timeout: int = 120) -> dict[str, Any]:
    req = urllib.request.Request(url, headers=_headers(), method="GET")
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return json.loads(resp.read().decode("utf-8"))


def _iter_ckan_packages(base_url: str, fq: str, page_size: int, max_packages: int) -> list[dict[str, Any]]:
    packages: list[dict[str, Any]] = []
    start = 0
    total_count = None
    while len(packages) < max_packages:
        params = {"rows": str(page_size), "start": str(start)}
        if fq.strip():
            params["fq"] = fq.strip()
        url = f"{base_url}/api/3/action/package_search?{urllib.parse.urlencode(params)}"
        payload = _http_get_json(url)
        if not payload.get("success"):
            raise RuntimeError(f"CKAN package_search failed: {url}")
        result = payload.get("result", {})
        if total_count is None:
            total_count = int(result.get("count") or 0)
            print(f"[catalog] total packages matched: {total_count}")
        block = result.get("results") or []
        if not block:
            break
        packages.extend(block)
        start += len(block)
        visible_total = min(total_count or len(packages), max_packages)
        print(f"[catalog] collected {min(len(packages), max_packages)}/{visible_total} packages")
        if len(block) < page_size:
            break
    return packages[:max_packages]


def _resource_download_kind(res: dict[str, Any]) -> str | None:
    fmt = str(res.get("format") or "").lower()
    url = str(res.get("url") or "").strip()
    if not url:
        return None
    url_l = url.lower()
    if "csv" in fmt or url_l.endswith(".csv"):
        return "csv"
    if "geojson" in fmt or url_l.endswith(".geojson") or "format=geojson" in url_l:
        return "geojson"
    if "hub.arcgis.com" in url_l and "geojson" in url_l:
        return "geojson"
    return None


def _pick_preferred_resource(ds: dict[str, Any]) -> dict[str, Any] | None:
    resources = [r for r in (ds.get("resources") or []) if isinstance(r, dict)]
    csv_res: dict[str, Any] | None = None
    geojson_res: dict[str, Any] | None = None
    for res in resources:
        kind = _resource_download_kind(res)
        if kind == "csv" and csv_res is None:
            csv_res = res
        elif kind == "geojson" and geojson_res is None:
            geojson_res = res
    return csv_res or geojson_res


def _download_with_progress(url: str, dest: Path, timeout: int = 300) -> int:
    req = urllib.request.Request(url, headers=_headers(), method="GET")
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        total = int(resp.headers.get("Content-Length") or 0)
        downloaded = 0
        last_print = 0.0
        chunk_size = 1024 * 128
        dest.parent.mkdir(parents=True, exist_ok=True)
        with dest.open("wb") as f:
            while True:
                chunk = resp.read(chunk_size)
                if not chunk:
                    break
                f.write(chunk)
                downloaded += len(chunk)
                now = time.time()
                if now - last_print >= 0.3:
                    if total > 0:
                        pct = downloaded / total * 100
                        print(
                            f"      -> {downloaded / 1024 / 1024:.2f}MB / {total / 1024 / 1024:.2f}MB ({pct:.1f}%)",
                            end="\r",
                            flush=True,
                        )
                    else:
                        print(f"      -> {downloaded / 1024 / 1024:.2f}MB", end="\r", flush=True)
                    last_print = now
        print(" " * 90, end="\r")
    return downloaded


def _resource_file_suffix(resource: dict[str, Any]) -> str:
    kind = _resource_download_kind(resource)
    if kind == "csv":
        return ".csv"
    return ".geojson"


def run_download(
    *,
    city_id: str,
    base_url: str,
    fq: str,
    out_dir: Path,
    page_size: int,
    max_packages: int,
    sleep_seconds: float,
    skip_existing: bool,
) -> dict[str, Any]:
    base_url = base_url.rstrip("/")
    host = urllib.parse.urlparse(base_url).netloc or base_url.replace("https://", "").replace("http://", "")

    out_dir.mkdir(parents=True, exist_ok=True)
    geo_dir = out_dir / "geojson"
    geo_dir.mkdir(parents=True, exist_ok=True)

    packages = _iter_ckan_packages(base_url, fq, page_size=page_size, max_packages=max_packages)
    print(f"[scan] start scanning {len(packages)} packages for CSV/GeoJSON resources")

    download_jobs: list[dict[str, Any]] = []
    for idx, ds in enumerate(packages, start=1):
        title = str(ds.get("title") or ds.get("name") or "")
        selected_resource = _pick_preferred_resource(ds)
        supported = 1 if selected_resource else 0
        print(f"[scan] {idx}/{len(packages)} {title[:60]} -> supported resources: {supported}")
        dataset_name = str(ds.get("name") or "")
        notes = str(ds.get("notes") or "")
        if not selected_resource:
            continue
        rid = str(selected_resource.get("id") or _slug(title, max_len=16))
        rid_token = re.sub(r"[^0-9a-zA-Z]+", "", rid)[:12] or "res"
        ext = _resource_file_suffix(selected_resource)
        file_name = f"{_slug(title)}__{rid_token}{ext}"
        dest = (geo_dir / file_name).resolve()
        download_jobs.append(
            {
                "dataset_title": title,
                "dataset_name": dataset_name,
                "dataset_id": str(ds.get("id") or ""),
                "resource_id": rid,
                "resource_name": str(selected_resource.get("name") or ""),
                "resource_format": str(selected_resource.get("format") or ""),
                "url": str(selected_resource.get("url") or ""),
                "dest": str(dest),
                "description": notes[:2000],
                "source_link": f"{base_url}/dataset/{dataset_name}" if dataset_name else base_url,
            }
        )

    print(f"[download] total resource jobs: {len(download_jobs)}")
    datasets: list[dict[str, Any]] = []
    errors: list[dict[str, Any]] = []
    total_bytes = 0
    skipped_existing_count = 0
    now_iso = datetime.now(timezone.utc).isoformat()

    for i, job in enumerate(download_jobs, start=1):
        dest = Path(job["dest"])
        print(f"[download] {i}/{len(download_jobs)} {job['dataset_title'][:70]}")
        print(f"          {job['url']}")
        try:
            reused = bool(skip_existing and dest.exists() and dest.stat().st_size > 0)
            if reused:
                nbytes = dest.stat().st_size
                skipped_existing_count += 1
                print(f"          skip (exists): {dest.name} ({nbytes / 1024 / 1024:.2f}MB)")
            else:
                nbytes = _download_with_progress(job["url"], dest)
                print(f"          saved: {dest.name} ({nbytes / 1024 / 1024:.2f}MB)")
            total_bytes += nbytes
            datasets.append(
                {
                    "city": city_id,
                    "domain": host,
                    "id": job["resource_id"],
                    "name": job["dataset_title"],
                    "description": (job.get("description") or "")[:500],
                    "portal_format_detected": str(job.get("resource_format") or "GeoJSON"),
                    "download_format": (
                        "geojson" if dest.suffix.lower() == ".geojson" else dest.suffix.lower().lstrip(".")
                    ),
                    "download_url": job["url"],
                    "source_link": job.get("source_link") or "",
                    "path": str(dest.resolve()),
                    "csv_path": str(dest.resolve()),
                    "bytes": nbytes,
                    "updated_at": now_iso,
                    "skipped_existing": reused,
                }
            )
        except Exception as exc:  # noqa: BLE001
            errors.append(
                {
                    "id": job["resource_id"],
                    "name": job["dataset_title"],
                    "url": job["url"],
                    "error": repr(exc),
                }
            )
            print(f"          [error] {exc!r}")
        time.sleep(max(0.0, sleep_seconds))

    payload = {
        "meta": {
            "city": city_id,
            "portal_type": "ckan",
            "domain": host,
            "browse_url": base_url,
            "package_search_fq": fq,
            "filter_by_geo_keywords": False,
            "catalog_hits": len(packages),
            "selected_hits": len(download_jobs),
            "downloaded_count": len(datasets),
            "skipped_existing_count": skipped_existing_count,
            "error_count": len(errors),
            "total_mb": round(total_bytes / 1024 / 1024, 3),
            "errors": errors,
        },
        "datasets": datasets,
    }
    manifest_path = out_dir / "manifest.json"
    manifest_path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")
    print(f"[done] manifest saved: {manifest_path}")
    return payload


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Download CSV/GeoJSON resources from CKAN portals (Boston, Phoenix)."
    )
    parser.add_argument(
        "--city",
        choices=tuple(CITY_PROFILES.keys()),
        default="boston",
        help="Which CKAN city portal to mirror.",
    )
    parser.add_argument(
        "--base-url",
        default="",
        help="Override CKAN base URL (default depends on --city).",
    )
    parser.add_argument(
        "--fq",
        default="",
        help="Override package_search fq (default depends on --city).",
    )
    parser.add_argument("--all-site", action="store_true", help="Use empty fq (scan all matching packages).")
    parser.add_argument(
        "--out-dir",
        type=Path,
        default=None,
        help="Output directory (geojson/ + manifest.json). Default: scripts/artifacts/socrata_maps/<city>/",
    )
    parser.add_argument("--page-size", type=int, default=100, help="package_search page size.")
    parser.add_argument("--max-packages", type=int, default=5000, help="Max datasets to scan from catalog.")
    parser.add_argument(
        "--sleep",
        type=float,
        default=30.0,
        help="Pause seconds between two downloads (default: 30s, sequential download).",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Re-download even if the target file already exists and is non-empty.",
    )
    args = parser.parse_args()

    profile = CITY_PROFILES[args.city]
    base_url = (args.base_url or profile["base_url"]).rstrip("/")
    if args.all_site:
        fq = ""
    elif args.fq.strip():
        fq = args.fq.strip()
    else:
        fq = profile["fq_default"]

    script_dir = Path(__file__).resolve().parent
    default_out = script_dir / "artifacts" / "socrata_maps" / args.city
    out_dir = (args.out_dir or default_out).resolve()
    out_dir.mkdir(parents=True, exist_ok=True)
    skip_existing = not args.force

    print(f"[config] city={args.city} base_url={base_url} fq={fq!r} out_dir={out_dir} skip_existing={skip_existing}")
    run_download(
        city_id=args.city,
        base_url=base_url,
        fq=fq,
        out_dir=out_dir,
        page_size=max(1, min(200, args.page_size)),
        max_packages=max(1, args.max_packages),
        sleep_seconds=max(0.0, args.sleep),
        skip_existing=skip_existing,
    )


if __name__ == "__main__":
    main()