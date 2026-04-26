#!/usr/bin/env python3
"""Compute weighted-mean checks from source_dataset_stats.json."""
from __future__ import annotations

import json
from pathlib import Path


def main() -> int:
    out_dir = Path(__file__).resolve().parent / "artifacts" / "seven_city_output"
    stats_path = out_dir / "source_dataset_stats.json"
    rows = json.loads(stats_path.read_text(encoding="utf-8"))

    cities = [r for r in rows if r["city"] != "Overall"]
    overall = next(r for r in rows if r["city"] == "Overall")

    total_tables = sum(r["n_tables"] for r in cities)
    weighted_fields = sum(r["n_tables"] * r["avg_fields_per_table"] for r in cities) / total_tables
    weighted_spatial = (
        sum(r["n_tables"] * r["avg_spatial_fields_per_table"] for r in cities) / total_tables
    )
    weighted_rows = sum(r["n_tables"] * r["avg_rows_per_table"] for r in cities) / total_tables

    check = {
        "city": "WeightedSum",
        "n_tables": total_tables,
        "avg_fields_per_table_weighted": round(weighted_fields, 4),
        "avg_spatial_fields_per_table_weighted": round(weighted_spatial, 4),
        "avg_rows_per_table_weighted": round(weighted_rows, 4),
        "overall_avg_fields_per_table": overall["avg_fields_per_table"],
        "overall_avg_spatial_fields_per_table": overall["avg_spatial_fields_per_table"],
        "overall_avg_rows_per_table": overall["avg_rows_per_table"],
        "delta_fields": round(weighted_fields - overall["avg_fields_per_table"], 8),
        "delta_spatial": round(weighted_spatial - overall["avg_spatial_fields_per_table"], 8),
        "delta_rows": round(weighted_rows - overall["avg_rows_per_table"], 8),
    }

    json_out = out_dir / "source_dataset_weighted_check.json"
    json_out.write_text(json.dumps(check, ensure_ascii=False, indent=2), encoding="utf-8")

    md_lines = [
        "# Weighted Sum Check",
        "",
        "| Metric | Weighted by #Table | Overall | Delta |",
        "|---|---:|---:|---:|",
        (
            f"| #Field/Table | {weighted_fields:.4f} | "
            f"{overall['avg_fields_per_table']:.4f} | "
            f"{weighted_fields - overall['avg_fields_per_table']:.8f} |"
        ),
        (
            f"| #Spatial Field/Table | {weighted_spatial:.4f} | "
            f"{overall['avg_spatial_fields_per_table']:.4f} | "
            f"{weighted_spatial - overall['avg_spatial_fields_per_table']:.8f} |"
        ),
        (
            f"| #Row/Table | {weighted_rows:.4f} | "
            f"{overall['avg_rows_per_table']:.4f} | "
            f"{weighted_rows - overall['avg_rows_per_table']:.8f} |"
        ),
        "",
        f"sum(#Table) = {total_tables}",
    ]
    md_out = out_dir / "source_dataset_weighted_check.md"
    md_out.write_text("\n".join(md_lines) + "\n", encoding="utf-8")

    print("[done] weighted-check outputs:")
    print(f"       {json_out}")
    print(f"       {md_out}")
    print(f"weighted_fields={weighted_fields:.6f}, overall={overall['avg_fields_per_table']:.6f}")
    print(f"weighted_spatial={weighted_spatial:.6f}, overall={overall['avg_spatial_fields_per_table']:.6f}")
    print(f"weighted_rows={weighted_rows:.6f}, overall={overall['avg_rows_per_table']:.6f}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
