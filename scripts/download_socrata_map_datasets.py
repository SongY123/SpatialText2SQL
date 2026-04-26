#!/usr/bin/env python3
"""
Download map / geospatial-related datasets from Socrata-based open data portals.

Uses the public Socrata Catalog API (no HTML scraping):
  https://dev.socrata.com/docs/other/catalog.html

Targets (same portals as in the browse URLs you shared):
  - Los Angeles: https://data.lacity.org/browse?sortBy=relevance&pageSize=20&limitTo=maps
  - Chicago:   https://data.cityofchicago.org/browse?q=map&sortBy=relevance

Examples:
  python scripts/download_socrata_map_datasets.py --portal lacity
  python scripts/download_socrata_map_datasets.py --portal lacity --out-dir D:/open-data/lacity
  python scripts/download_socrata_map_datasets.py --portal chicago --out-dir D:/open-data/chicago --max-datasets 30

If --out-dir is omitted, files go to scripts/artifacts/socrata_maps/<portal>/ next to these scripts.

Optional: set SOCRATA_APP_TOKEN for higher throttles on some portals (see https://dev.socrata.com/docs/app-tokens.html).
"""

from __future__ import annotations

import argparse
import json
import os
import re
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Any


CATALOG_URL = "https://api.us.socrata.com/api/catalog/v1"

PORTALS: dict[str, dict[str, str]] = {
    "lacity": {
        "domain": "data.lacity.org",
        "browse": "https://data.lacity.org/browse?sortBy=relevance&pageSize=20&limitTo=maps",
        "default_q": "map",
    },
    "chicago": {
        "domain": "data.cityofchicago.org",
        "browse": "https://data.cityofchicago.org/browse?q=map&sortBy=relevance",
        "default_q": "map",
    },
}


def _http_get_json(url: str, headers: dict[str, str] | None = None) -> dict[str, Any]:
    req = urllib.request.Request(url, headers=headers or {}, method="GET")
    with urllib.request.urlopen(req, timeout=120) as resp:
        return json.loads(resp.read().decode("utf-8"))


def _http_download(url: str, dest: Path, headers: dict[str, str] | None = None) -> int:
    req = urllib.request.Request(url, headers=headers or {}, method="GET")
    with urllib.request.urlopen(req, timeout=300) as resp:
        data = resp.read()
    dest.parent.mkdir(parents=True, exist_ok=True)
    dest.write_bytes(data)
    return len(data)


def _slug(s: str, max_len: int = 60) -> str:
    s = re.sub(r"[^\w\s-]", "", s, flags=re.UNICODE)
    s = re.sub(r"[-\s]+", "_", s.strip()).strip("_")
    return (s[:max_len] or "dataset").lower()


def _socrata_headers() -> dict[str, str]:
    token = os.environ.get("SOCRATA_APP_TOKEN", "").strip()
    h = {"Accept": "application/json", "User-Agent": "spatialtext2sql-downloader/1.0"}
    if token:
        h["X-App-Token"] = token
    return h


def fetch_catalog_page(
    domain: str,
    *,
    q: str,
    limit: int,
    offset: int,
) -> dict[str, Any]:
    params = {
        "domains": domain,
        "only": "datasets",
        "limit": str(limit),
        "offset": str(offset),
    }
    if q:
        params["q"] = q
    url = f"{CATALOG_URL}?{urllib.parse.urlencode(params)}"
    return _http_get_json(url, headers=_socrata_headers())


def iter_catalog_results(
    domain: str,
    *,
    q: str,
    page_size: int,
    max_results: int,
) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    offset = 0
    while len(out) < max_results:
        payload = fetch_catalog_page(domain, q=q, limit=page_size, offset=offset)
        batch = payload.get("results") or []
        if not batch:
            break
        out.extend(batch)
        offset += len(batch)
        if len(batch) < page_size:
            break
    return out[:max_results]


def resource_id_from_result(item: dict[str, Any]) -> str | None:
    res = item.get("resource") or {}
    rid = res.get("id")
    if isinstance(rid, str) and re.fullmatch(r"[0-9a-z]{4}-[0-9a-z]{4}", rid, re.I):
        return rid.lower()
    return None


def is_geospatialish(item: dict[str, Any]) -> bool:
    """Heuristic: keep catalog rows that look map / GIS / location related."""
    res = item.get("resource") or {}
    name = (res.get("name") or "").lower()
    desc = (res.get("description") or "").lower()
    blob = f"{name} {desc}"
    keywords = (
        "map",
        "geo",
        "gis",
        "shape",
        "polygon",
        "boundary",
        "parcel",
        "location",
        "lat",
        "lon",
        "coordinate",
        "spatial",
        "footprint",
        "zone",
        "district",
        "census",
        "address",
        "route",
        "street",
        "line",
        "point",
        "kmz",
        "kml",
    )
    if any(k in blob for k in keywords):
        return True
    # Socrata often exposes geography types in metadata
    meta = res.get("metadata") or {}
    geo = meta.get("geo") or {}
    if geo.get("bbox") or geo.get("type"):
        return True
    cols = res.get("columns_name") or res.get("columns_field_name") or []
    if isinstance(cols, list):
        joined = " ".join(str(c).lower() for c in cols)
        if any(x in joined for x in ("geom", "location", "latitude", "longitude", "the_geom", "shape")):
            return True
    return False


def build_manifest_entry(item: dict[str, Any], rid: str) -> dict[str, Any]:
    res = item.get("resource") or {}
    link = res.get("link") or ""
    return {
        "id": rid,
        "name": res.get("name"),
        "description": (res.get("description") or "")[:500],
        "link": link,
        "updatedAt": res.get("updatedAt"),
        "domain": (item.get("metadata") or {}).get("domain") or res.get("domain"),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Download Socrata map-related datasets (LA / Chicago).")
    parser.add_argument(
        "--portal",
        choices=tuple(PORTALS.keys()),
        required=True,
        help="Which portal to mirror (lacity = data.lacity.org, chicago = data.cityofchicago.org).",
    )
    parser.add_argument(
        "--out-dir",
        type=Path,
        default=None,
        help="Output directory (CSV files + manifest.json). Default: scripts/artifacts/socrata_maps/<portal>/",
    )
    parser.add_argument(
        "--q",
        type=str,
        default="",
        help="Catalog search query (default: portal-specific, e.g. 'map').",
    )
    parser.add_argument("--page-size", type=int, default=50, help="Catalog API page size (max 100 typical).")
    parser.add_argument("--max-catalog", type=int, default=200, help="Max catalog rows to pull before filtering.")
    parser.add_argument("--max-datasets", type=int, default=50, help="Max datasets to download after filter.")
    parser.add_argument(
        "--row-limit",
        type=int,
        default=200_000,
        help="Append $limit=... to CSV export (portal may cap lower without app token).",
    )
    parser.add_argument("--sleep", type=float, default=0.35, help="Seconds between downloads.")
    parser.add_argument(
        "--no-geo-filter",
        action="store_true",
        help="Download first N catalog hits without geospatial keyword filter.",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Re-download even if the target CSV already exists and is non-empty.",
    )
    args = parser.parse_args()

    script_dir = Path(__file__).resolve().parent
    out_dir = args.out_dir
    if out_dir is None:
        out_dir = script_dir / "artifacts" / "socrata_maps" / args.portal

    cfg = PORTALS[args.portal]
    domain = cfg["domain"]
    q = args.q.strip() if args.q.strip() else cfg["default_q"]
    headers = _socrata_headers()

    out_dir = out_dir.resolve()
    print(f"Portal: {args.portal}  domain={domain}")
    print(f"Out dir: {out_dir}")
    print(f"Browse: {cfg['browse']}")
    print(f"Catalog q={q!r}  max_catalog={args.max_catalog}")

    raw = iter_catalog_results(domain, q=q, page_size=min(args.page_size, 100), max_results=args.max_catalog)
    if not args.no_geo_filter:
        filtered = [r for r in raw if is_geospatialish(r)]
    else:
        filtered = list(raw)

    print(f"Catalog hits: {len(raw)}  after filter: {len(filtered)}")

    csv_dir = out_dir / "csv"
    csv_dir.mkdir(parents=True, exist_ok=True)

    manifest: list[dict[str, Any]] = []
    errors: list[dict[str, Any]] = []
    downloaded = 0
    skipped_existing = 0
    skip_existing = not args.force

    for item in filtered:
        if downloaded >= args.max_datasets:
            break
        rid = resource_id_from_result(item)
        if not rid:
            continue
        res = item.get("resource") or {}
        name = res.get("name") or rid
        slug = _slug(str(name))
        fname = f"{slug}__{rid}.csv"
        dest = csv_dir / fname

        export_url = f"https://{domain}/resource/{rid}.csv?$limit={args.row_limit}"
        try:
            if skip_existing and dest.exists() and dest.stat().st_size > 0:
                nbytes = dest.stat().st_size
                skipped_existing += 1
                entry = build_manifest_entry(item, rid)
                entry["csv_path"] = str(dest.resolve())
                entry["bytes"] = nbytes
                entry["skipped_existing"] = True
                manifest.append(entry)
                downloaded += 1
                print(f"[{downloaded}] {rid}  skip-existing  {nbytes // 1024} KiB  {str(name)[:70]}")
                time.sleep(args.sleep)
                continue
            nbytes = _http_download(export_url, dest, headers=headers)
        except urllib.error.HTTPError as e:
            errors.append({"id": rid, "name": name, "error": str(e), "url": export_url})
            time.sleep(args.sleep)
            continue
        except Exception as e:  # noqa: BLE001
            errors.append({"id": rid, "name": name, "error": repr(e), "url": export_url})
            time.sleep(args.sleep)
            continue

        entry = build_manifest_entry(item, rid)
        entry["csv_path"] = str(dest.resolve())
        entry["bytes"] = nbytes
        entry["skipped_existing"] = False
        manifest.append(entry)
        downloaded += 1
        print(f"[{downloaded}] {rid}  {nbytes // 1024} KiB  {str(name)[:70]}")
        time.sleep(args.sleep)

    meta = {
        "portal": args.portal,
        "domain": domain,
        "browse_url": cfg["browse"],
        "catalog_query": q,
        "row_limit": args.row_limit,
        "downloaded_count": len(manifest),
        "skipped_existing_count": skipped_existing,
        "errors": errors,
    }
    (out_dir / "manifest.json").write_text(
        json.dumps({"meta": meta, "datasets": manifest}, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )
    print(f"Done. CSV dir: {csv_dir}")
    print(f"Manifest: {out_dir / 'manifest.json'}")
    if errors:
        print(f"Errors: {len(errors)} (see manifest.json -> meta.errors)")


if __name__ == "__main__":
    main()
