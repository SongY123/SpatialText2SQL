#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any


SPATIAL_HINTS = (
    "the_geom",
    "geom",
    "geometry",
    "shape",
    "latitude",
    "longitude",
    "lat",
    "lon",
    "point_x",
    "point_y",
    "x_coord",
    "y_coord",
)


@dataclass
class TableStat:
    city: str
    file_path: str
    n_fields: int
    n_spatial_fields: int
    n_rows: int


def read_header(path: Path) -> list[str]:
    for enc in ("utf-8-sig", "utf-8", "latin-1"):
        try:
            with path.open("r", encoding=enc, newline="") as handle:
                return next(csv.reader(handle))
        except UnicodeDecodeError:
            continue
        except StopIteration:
            return []
    return []


def count_rows(path: Path) -> int:
    for enc in ("utf-8-sig", "utf-8", "latin-1"):
        try:
            with path.open("r", encoding=enc, newline="") as handle:
                reader = csv.reader(handle)
                next(reader, None)
                return sum(1 for _ in reader)
        except UnicodeDecodeError:
            continue
    return 0


def is_spatial_col(name: str) -> bool:
    lower = name.strip().lower()
    return any(hint == lower or hint in lower for hint in SPATIAL_HINTS)


def collapse_lat_lon(columns: list[str]) -> tuple[int, int]:
    lowered = [col.strip().lower() for col in columns]
    total_fields = len(columns)
    spatial_fields = sum(1 for col in columns if is_spatial_col(col))

    has_lat = any(col in {"lat", "latitude"} for col in lowered)
    has_lon = any(col in {"lon", "lng", "longitude"} for col in lowered)
    if has_lat and has_lon:
        total_fields -= 1
        spatial_fields -= 1

    return max(total_fields, 0), max(spatial_fields, 0)


def iter_manifest_table_paths(manifest_path: Path) -> list[Path]:
    payload = json.loads(manifest_path.read_text(encoding="utf-8"))
    paths: list[Path] = []
    for item in payload.get("datasets", []):
        path = item.get("path") or item.get("csv_path")
        if not path:
            continue
        p = Path(path)
        if p.suffix.lower() == ".csv" and p.exists():
            paths.append(p)
    return paths


def collect_city_stats(city: str, table_paths: list[Path]) -> list[TableStat]:
    out: list[TableStat] = []
    for path in table_paths:
        cols = read_header(path)
        fields, spatial_fields = collapse_lat_lon(cols)
        rows = count_rows(path)
        out.append(
            TableStat(
                city=city,
                file_path=str(path),
                n_fields=fields,
                n_spatial_fields=spatial_fields,
                n_rows=rows,
            )
        )
    return out


def summarize(stats: list[TableStat]) -> dict[str, Any]:
    n_tables = len(stats)
    if n_tables == 0:
        return {
            "Table": 0,
            "Field/Table": 0.0,
            "Spatial Field/Table": 0.0,
            "Row/Table": 0.0,
            "spatial_field_table_gt_1": False,
        }
    sum_fields = sum(s.n_fields for s in stats)
    sum_spatial_fields = sum(s.n_spatial_fields for s in stats)
    sum_rows = sum(s.n_rows for s in stats)
    spatial_per_table = sum_spatial_fields / n_tables
    return {
        "Table": n_tables,
        "Field/Table": round(sum_fields / n_tables, 2),
        "Spatial Field/Table": round(spatial_per_table, 2),
        "Row/Table": round(sum_rows / n_tables, 2),
        "spatial_field_table_gt_1": spatial_per_table > 1.0,
    }


def write_markdown(path: Path, city_rows: list[dict[str, Any]]) -> None:
    lines = [
        "# Table 1: Statistics of source data collected from 7 cities",
        "",
        "| City | Table | Field/Table | Spatial Field/Table | Row/Table |",
        "|---|---:|---:|---:|---:|",
    ]
    for row in city_rows:
        lines.append(
            f"| {row['City']} | {row['Table']} | {row['Field/Table']} | {row['Spatial Field/Table']} | {row['Row/Table']} |"
        )
    lines.append("")
    warns = [r["City"] for r in city_rows if r.get("spatial_field_table_gt_1") is False]
    if warns:
        lines.append(f"> Warning: Spatial Field/Table <= 1 for: {', '.join(warns)}")
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> None:
    parser = argparse.ArgumentParser(description="Compute source-data statistics table for 7 cities.")
    parser.add_argument("--nyc-dir", type=Path, required=True, help="NYC CSV directory.")
    parser.add_argument("--chicago-manifest", type=Path, required=True, help="Chicago manifest.json.")
    parser.add_argument("--lacity-manifest", type=Path, required=True, help="LA manifest.json.")
    parser.add_argument("--seattle-manifest", type=Path, required=True, help="Seattle manifest.json.")
    parser.add_argument("--boston-manifest", type=Path, required=True, help="Boston manifest.json.")
    parser.add_argument("--sf-manifest", type=Path, required=True, help="San Francisco manifest.json.")
    parser.add_argument("--phoenix-manifest", type=Path, required=True, help="Phoenix manifest.json.")
    parser.add_argument("--out-dir", type=Path, default=Path("scripts/artifacts"), help="Output directory.")
    args = parser.parse_args()

    city_to_paths: dict[str, list[Path]] = {}
    city_to_paths["NYC"] = [p for p in sorted(args.nyc_dir.glob("*.csv")) if p.is_file()]
    city_to_paths["Chicago"] = iter_manifest_table_paths(args.chicago_manifest)
    city_to_paths["LA"] = iter_manifest_table_paths(args.lacity_manifest)
    city_to_paths["Seattle"] = iter_manifest_table_paths(args.seattle_manifest)
    city_to_paths["Boston"] = iter_manifest_table_paths(args.boston_manifest)
    city_to_paths["SF"] = iter_manifest_table_paths(args.sf_manifest)
    city_to_paths["Phoenix"] = iter_manifest_table_paths(args.phoenix_manifest)

    out_dir = args.out_dir.resolve()
    out_dir.mkdir(parents=True, exist_ok=True)

    city_rows: list[dict[str, Any]] = []
    details: list[dict[str, Any]] = []
    for city, paths in city_to_paths.items():
        stats = collect_city_stats(city, paths)
        summary = summarize(stats)
        row = {"City": city, **summary}
        city_rows.append(row)
        details.extend([s.__dict__ for s in stats])
        print(f"{city}: tables={summary['Table']} spatial_field_table={summary['Spatial Field/Table']}")

    (out_dir / "source_data_stats_7cities.json").write_text(
        json.dumps({"table_1": city_rows, "table_details": details}, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )
    write_markdown(out_dir / "source_data_stats_7cities.md", city_rows)
    print(f"Done: {out_dir / 'source_data_stats_7cities.md'}")


if __name__ == "__main__":
    main()
