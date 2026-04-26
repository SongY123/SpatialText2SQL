#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import re
import time
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Any


DOMAIN = "data.seattle.gov"
BROWSE_URL = "https://data.seattle.gov/browse?limitTo=maps"
VIEWS_SEARCH_URL = f"https://{DOMAIN}/api/search/views.json"
GEO_DTYPES = {"location", "point", "polygon", "line", "multipolygon", "multiline", "multipoint"}


def _headers() -> dict[str, str]:
    return {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/124.0.0.0 Safari/537.36"
        ),
        "Accept": "application/json,text/plain,*/*",
    }


def _slug(text: str, max_len: int = 90) -> str:
    text = re.sub(r"[^\w\s-]", "", text)
    text = re.sub(r"[-\s]+", "_", text.strip()).strip("_").lower()
    return (text[:max_len] or "dataset").lower()


def _http_get_json(url: str, timeout: int = 120) -> dict[str, Any]:
    req = urllib.request.Request(url, headers=_headers(), method="GET")
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return json.loads(resp.read().decode("utf-8"))


def _download_with_progress(url: str, dest: Path, timeout: int = 300) -> int:
    req = urllib.request.Request(url, headers=_headers(), method="GET")
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        total = int(resp.headers.get("Content-Length") or 0)
        chunk_size = 128 * 1024
        downloaded = 0
        last_show = 0.0

        dest.parent.mkdir(parents=True, exist_ok=True)
        with dest.open("wb") as f:
            while True:
                chunk = resp.read(chunk_size)
                if not chunk:
                    break
                f.write(chunk)
                downloaded += len(chunk)
                now = time.time()
                if now - last_show >= 0.35:
                    if total > 0:
                        pct = downloaded * 100.0 / total
                        print(
                            f"      -> {downloaded / 1024 / 1024:.2f}MB / {total / 1024 / 1024:.2f}MB ({pct:.1f}%)",
                            end="\r",
                            flush=True,
                        )
                    else:
                        print(f"      -> {downloaded / 1024 / 1024:.2f}MB", end="\r", flush=True)
                    last_show = now
        print(" " * 95, end="\r")
    return downloaded


def _export_geojson_url(rid: str, row_limit: int) -> str | None:
    rid = str(rid or "").strip()
    if not re.fullmatch(r"[0-9a-z]{4}-[0-9a-z]{4}", rid, flags=re.I):
        return None
    return f"https://{DOMAIN}/resource/{rid.lower()}.geojson?$limit={row_limit}"


def _view_has_geo_columns(view: dict[str, Any]) -> bool:
    columns = view.get("columns") or []
    dtype_hits: set[str] = set()
    name_blob: list[str] = []
    for col in columns:
        if not isinstance(col, dict):
            continue
        dtype = str(col.get("dataTypeName") or "").lower()
        if dtype:
            dtype_hits.add(dtype)
        name_blob.append(str(col.get("name") or ""))
        name_blob.append(str(col.get("fieldName") or ""))
    if dtype_hits & GEO_DTYPES:
        return True
    merged = " ".join(name_blob).lower()
    for kw in ("the_geom", "geom", "geometry", "shape", "latitude", "longitude", "lat", "lon"):
        if kw in merged:
            return True
    return False


def iter_seattle_geo_views(page_size: int, max_views: int) -> list[dict[str, Any]]:
    """通过 /api/search/views.json 分页扫描，筛出可导出 GeoJSON 的地理数据集。"""
    out: list[dict[str, Any]] = []
    seen_ids: set[str] = set()
    page = 1
    limit = min(max(1, page_size), 100)
    total_count = None

    while len(out) < max_views:
        params = {"limit": str(limit), "page": str(page)}
        url = f"{VIEWS_SEARCH_URL}?{urllib.parse.urlencode(params)}"
        payload = _http_get_json(url)
        results = payload.get("results") or []
        if total_count is None:
            total_count = int(payload.get("count") or 0)
            print(f"[catalog] searchable views total: {total_count}")
        if not results:
            break

        page_geo = 0
        for item in results:
            view = item.get("view")
            if not isinstance(view, dict):
                continue
            rid = str(view.get("id") or "").lower()
            if not rid or rid in seen_ids:
                continue
            if not _view_has_geo_columns(view):
                continue
            seen_ids.add(rid)
            out.append(view)
            page_geo += 1
            if len(out) >= max_views:
                break

        scanned = min(page * limit, total_count or page * limit)
        print(f"[catalog] page={page} scanned≈{scanned} geo_candidates+={page_geo} total_geo={len(out)}")
        if len(results) < limit:
            break
        page += 1

    return out[:max_views]


def run(
    out_dir: Path,
    max_items: int,
    page_size: int,
    row_limit: int,
    sleep_seconds: float,
    *,
    skip_existing: bool,
) -> dict[str, Any]:
    out_dir.mkdir(parents=True, exist_ok=True)
    geo_dir = out_dir / "geojson"
    geo_dir.mkdir(parents=True, exist_ok=True)

    geo_views = iter_seattle_geo_views(page_size=page_size, max_views=max_items)
    print(f"[scan] geo-capable datasets found: {len(geo_views)}")

    manifest: list[dict[str, Any]] = []
    errors: list[dict[str, Any]] = []
    total_bytes = 0

    for idx, view in enumerate(geo_views, start=1):
        rid = str(view.get("id") or "").lower()
        name = str(view.get("name") or rid or f"dataset_{idx}")
        geojson_url = _export_geojson_url(rid=rid, row_limit=row_limit)
        if not geojson_url:
            errors.append(
                {
                    "index": idx,
                    "name": name,
                    "id": rid,
                    "error": "missing_or_invalid_resource_id",
                }
            )
            print(f"[skip] {idx}/{len(geo_views)} {name} -> invalid resource id")
            continue

        file_name = f"{_slug(name)}__{rid}.geojson" if rid else f"{_slug(name)}__idx{idx}.geojson"
        dest = geo_dir / file_name

        print(f"[download] {idx}/{len(geo_views)} {name[:80]}")
        print(f"          {geojson_url}")
        try:
            reused = bool(skip_existing and dest.exists() and dest.stat().st_size > 0)
            if reused:
                nbytes = dest.stat().st_size
                print(f"          skip (exists): {dest.name} ({nbytes / 1024 / 1024:.2f}MB)")
            else:
                nbytes = _download_with_progress(geojson_url, dest)
                print(f"          saved: {dest.name} ({nbytes / 1024 / 1024:.2f}MB)")
            total_bytes += nbytes
            manifest.append(
                {
                    "index": idx,
                    "id": rid,
                    "name": name,
                    "description": str(view.get("description") or "")[:500],
                    "download_url": geojson_url,
                    "source_link": f"https://{DOMAIN}/d/{rid}",
                    "path": str(dest.resolve()),
                    "bytes": nbytes,
                    "updated_at": view.get("rowsUpdatedAt") or view.get("viewLastModified"),
                    "skipped_existing": reused,
                }
            )
        except Exception as exc:  # noqa: BLE001
            errors.append(
                {
                    "index": idx,
                    "name": name,
                    "id": rid,
                    "url": geojson_url,
                    "error": repr(exc),
                }
            )
            print(f"          [error] {exc!r}")
        time.sleep(max(0.0, sleep_seconds))

    payload = {
        "meta": {
            "domain": DOMAIN,
            "browse_url": BROWSE_URL,
            "source_api": VIEWS_SEARCH_URL,
            "filter_rule": "view has geospatial columns or geometry-like field names",
            "catalog_count": len(geo_views),
            "downloaded_count": len(manifest),
            "skipped_existing_count": sum(1 for m in manifest if m.get("skipped_existing")),
            "error_count": len(errors),
            "total_mb": round(total_bytes / 1024 / 1024, 3),
        },
        "downloads": manifest,
        "errors": errors,
    }
    manifest_path = out_dir / "manifest_seattle_maps_geojson.json"
    manifest_path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")
    print(f"[done] manifest: {manifest_path}")
    return payload


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Batch download Seattle geo-capable datasets as GeoJSON with progress output. "
            "This is broader than `only=map` and matches what portal Maps usually displays."
        )
    )
    _script_dir = Path(__file__).resolve().parent
    _default_out = _script_dir / "artifacts" / "socrata_maps" / "seattle_maps_geojson"
    parser.add_argument(
        "--out-dir",
        type=Path,
        default=_default_out,
        help="Output directory for GeoJSON files and manifest.",
    )
    parser.add_argument("--max-items", type=int, default=5000, help="Max number of map items to process.")
    parser.add_argument("--page-size", type=int, default=100, help="Catalog page size (1-100).")
    parser.add_argument("--row-limit", type=int, default=200000, help="Per-resource row limit for GeoJSON export.")
    parser.add_argument("--sleep", type=float, default=0.05, help="Sleep seconds between downloads.")
    parser.add_argument(
        "--force",
        action="store_true",
        help="Re-download even if the GeoJSON file already exists and is non-empty.",
    )
    args = parser.parse_args()

    print(f"[config] domain={DOMAIN} source=views-search out_dir={args.out_dir.resolve()}")
    run(
        out_dir=args.out_dir.resolve(),
        max_items=max(1, args.max_items),
        page_size=max(1, min(100, args.page_size)),
        row_limit=max(1, args.row_limit),
        sleep_seconds=max(0.0, args.sleep),
        skip_existing=not args.force,
    )


if __name__ == "__main__":
    main()
