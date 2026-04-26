#!/usr/bin/env python3
"""按本地 CSV/GeoJSON 统计各城：表数、平均每表字段数、平均每表空间字段数、平均每表行数。

扫描规则与本地聚类流水线一致：城市根目录下递归 *.csv、*.geojson（跳过 manifest.json）。
Seattle 优先使用 seattle_maps_geojson，不存在则用 seattle。"""
from __future__ import annotations

import argparse
import csv
import json
from dataclasses import dataclass
from pathlib import Path

# 与 spatial_benchmark.clustering.detect_spatial_columns 一致
_SPATIAL_HINTS = (
    "the_geom",
    "geom",
    "geometry",
    "shape",
    "wkt",
    "latitude",
    "longitude",
    "lat",
    "lon",
    "point_x",
    "point_y",
    "x_coord",
    "y_coord",
)


def _count_spatial_columns(columns: list[str]) -> int:
    n = 0
    for column in columns:
        lower = column.lower()
        if any(hint == lower or hint in lower for hint in _SPATIAL_HINTS):
            n += 1
    return n


def _iter_data_files(city_root: Path) -> list[Path]:
    if not city_root.is_dir():
        return []
    out: list[Path] = []
    seen: set[str] = set()
    for pattern in ("*.csv", "*.geojson"):
        for p in city_root.rglob(pattern):
            if not p.is_file() or p.name.startswith("."):
                continue
            if p.name.lower() == "manifest.json":
                continue
            k = str(p.resolve())
            if k in seen:
                continue
            seen.add(k)
            out.append(p)
    return sorted(out, key=lambda x: str(x).lower())


def _read_csv_header(path: Path) -> list[str] | None:
    import csv as _csv

    for enc in ("utf-8-sig", "utf-8", "latin-1"):
        try:
            with path.open("r", encoding=enc, newline="") as f:
                return next(_csv.reader(f))
        except (UnicodeDecodeError, StopIteration):
            continue
    return None


def _count_csv_rows(path: Path) -> int:
    """快速数行（换行符计数，减表头）；大文件用二进制扫描。"""
    try:
        nlines = 0
        with path.open("rb") as f:
            while True:
                chunk = f.read(1 << 22)
                if not chunk:
                    break
                nlines += chunk.count(b"\n")
        return max(0, nlines - 1)
    except OSError:
        return 0


def _stats_csv(path: Path) -> tuple[int, int, int] | None:
    """返回 (n_rows, n_fields, n_spatial_fields)。"""
    cols = _read_csv_header(path)
    if not cols:
        return None
    rows = _count_csv_rows(path)
    return rows, len(cols), _count_spatial_columns(cols)


def _stats_geojson(path: Path) -> tuple[int, int, int] | None:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None
    if not isinstance(payload, dict):
        return None
    if payload.get("type") != "FeatureCollection":
        return None
    feats = payload.get("features") or []
    n_rows = len(feats)
    if n_rows == 0:
        return 0, 0, 0
    f0 = feats[0] if feats else {}
    props = (f0.get("properties") or {}) if isinstance(f0, dict) else {}
    cols = [str(k) for k in props.keys()]
    if isinstance(f0, dict) and f0.get("geometry") is not None:
        cols.append("geometry")
    n_fields = len(cols)
    n_sp = _count_spatial_columns(cols)
    return n_rows, n_fields, n_sp


def _stats_file(path: Path) -> tuple[int, int, int] | None:
    suf = path.suffix.lower()
    if suf == ".csv":
        return _stats_csv(path)
    if suf in {".geojson", ".json"}:
        return _stats_geojson(path)
    return None


@dataclass
class CityAgg:
    city: str
    n_tables: int = 0
    sum_fields: int = 0
    sum_spatial: int = 0
    sum_rows: int = 0

    def add(self, n_rows: int, n_fields: int, n_sp: int) -> None:
        self.n_tables += 1
        self.sum_rows += n_rows
        self.sum_fields += n_fields
        self.sum_spatial += n_sp

    @property
    def avg_fields(self) -> float:
        return self.sum_fields / self.n_tables if self.n_tables else 0.0

    @property
    def avg_spatial(self) -> float:
        return self.sum_spatial / self.n_tables if self.n_tables else 0.0

    @property
    def avg_rows(self) -> float:
        return self.sum_rows / self.n_tables if self.n_tables else 0.0


def default_city_roots(socrata: Path) -> list[tuple[str, Path]]:
    seattle = socrata / "seattle_maps_geojson"
    if not seattle.is_dir():
        seattle = socrata / "seattle"
    return [
        ("Boston", socrata / "boston"),
        ("Chicago", socrata / "chicago"),
        ("Los Angeles", socrata / "lacity"),
        ("New York City", socrata / "nyc-opendata"),
        ("Phoenix", socrata / "phoenix"),
        ("San Francisco", socrata / "sf"),
        ("Seattle", seattle),
    ]


def main() -> int:
    script_dir = Path(__file__).resolve().parent
    ap = argparse.ArgumentParser(description="本地源数据统计（表/字段/空间字段/行）")
    ap.add_argument(
        "--socrata-root",
        type=Path,
        default=script_dir / "artifacts" / "socrata_maps",
        help="socrata_maps 根目录",
    )
    ap.add_argument(
        "--out-dir",
        type=Path,
        default=script_dir / "artifacts" / "seven_city_output",
        help="输出 JSON/CSV/Markdown",
    )
    args = ap.parse_args()

    socrata = args.socrata_root.resolve()
    rows_json: list[dict[str, float | int | str]] = []
    aggs: list[CityAgg] = []

    for city_name, root in default_city_roots(socrata):
        agg = CityAgg(city=city_name)
        for fp in _iter_data_files(root):
            st = _stats_file(fp)
            if st is None:
                continue
            n_rows, n_fields, n_sp = st
            agg.add(n_rows, n_fields, n_sp)
        aggs.append(agg)
        rows_json.append(
            {
                "city": city_name,
                "n_tables": agg.n_tables,
                "avg_fields_per_table": round(agg.avg_fields, 4),
                "avg_spatial_fields_per_table": round(agg.avg_spatial, 4),
                "avg_rows_per_table": round(agg.avg_rows, 4),
                "sum_rows": agg.sum_rows,
            }
        )

    tot_tables = sum(a.n_tables for a in aggs)
    tot_sf = sum(a.sum_fields for a in aggs)
    tot_ss = sum(a.sum_spatial for a in aggs)
    tot_sr = sum(a.sum_rows for a in aggs)
    overall = {
        "city": "Overall",
        "n_tables": tot_tables,
        "avg_fields_per_table": round(tot_sf / tot_tables, 4) if tot_tables else 0.0,
        "avg_spatial_fields_per_table": round(tot_ss / tot_tables, 4) if tot_tables else 0.0,
        "avg_rows_per_table": round(tot_sr / tot_tables, 4) if tot_tables else 0.0,
        "sum_rows": tot_sr,
    }
    rows_json.append(overall)

    args.out_dir.mkdir(parents=True, exist_ok=True)
    jpath = args.out_dir / "source_dataset_stats.json"
    jpath.write_text(json.dumps(rows_json, ensure_ascii=False, indent=2), encoding="utf-8")

    csv_path = args.out_dir / "source_dataset_stats.csv"
    with csv_path.open("w", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        w.writerow(
            ["City", "#Table", "#Field/Table", "#Spatial Field/Table", "#Row/Table", "sum_rows"]
        )
        for r in rows_json:
            w.writerow(
                [
                    r["city"],
                    r["n_tables"],
                    f"{r['avg_fields_per_table']:.4f}",
                    f"{r['avg_spatial_fields_per_table']:.4f}",
                    f"{r['avg_rows_per_table']:.4f}",
                    r["sum_rows"],
                ]
            )

    md_lines = [
        "# Table 1: Statistics of source data collected from 7 cities",
        "",
        "| City | #Table | #Field/Table | #Spatial Field/Table | #Row/Table |",
        "|------|--------|--------------|------------------------|------------|",
    ]
    for r in rows_json:
        md_lines.append(
            f"| {r['city']} | {r['n_tables']} | {r['avg_fields_per_table']:.2f} | "
            f"{r['avg_spatial_fields_per_table']:.2f} | {r['avg_rows_per_table']:.2f} |"
        )
    md_path = args.out_dir / "source_dataset_stats.md"
    md_path.write_text("\n".join(md_lines) + "\n", encoding="utf-8")

    print(f"[done] tables total={tot_tables}  -> {jpath}")
    print(f"       {csv_path}")
    print(f"       {md_path}")
    for r in rows_json:
        print(
            f"  {r['city']}: n={r['n_tables']}  "
            f"fields/table={r['avg_fields_per_table']:.2f}  "
            f"spatial/table={r['avg_spatial_fields_per_table']:.2f}  "
            f"rows/table={r['avg_rows_per_table']:.2f}"
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
