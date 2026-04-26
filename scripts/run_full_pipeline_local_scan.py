#!/usr/bin/env python3
"""全流程：以各城 socrata_maps/<city>/ 下本地 CSV/GeoJSON 为准重建聚类产物，并生成分类输入、统计、树图、SQL。

NYC 仍使用 nyc_opendata_maps.json + 同目录 CSV（build_profiles）；其余六城递归扫描整城目录（含 chicago/csv、boston/geojson 等）。
"""
from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SOCRATA = ROOT / "scripts" / "artifacts" / "socrata_maps"
OUT = ROOT / "scripts" / "artifacts" / "seven_city_output"


def default_local_city_roots() -> dict[str, Path]:
    roots: dict[str, Path] = {}
    for key in ("chicago", "lacity", "seattle", "boston", "sf", "phoenix"):
        d = SOCRATA / key
        if d.is_dir():
            roots[key] = d
    return roots


def main() -> int:
    ap = argparse.ArgumentParser(description="本地文件为准的七城聚类 + 分类导出全流程")
    ap.add_argument(
        "--nyc-raw-dir",
        type=Path,
        default=SOCRATA / "nyc-opendata",
        help="含 nyc_opendata_maps.json 与同目录 NYC CSV 的目录（默认仓库内 nyc-opendata）",
    )
    ap.add_argument(
        "--artifacts-dir",
        type=Path,
        default=OUT,
        help="输出 table_catalog / unified_inventory 等（默认 seven_city_output）",
    )
    ap.add_argument(
        "--skip-downstream",
        action="store_true",
        help="仅运行聚类，不跑 build_ai_classification_input / 报告 / dendrogram",
    )
    args = ap.parse_args()

    nyc = args.nyc_raw_dir.resolve()
    if not nyc.is_dir():
        print(f"找不到 NYC 目录: {nyc}", file=sys.stderr)
        return 1
    maps = nyc / "nyc_opendata_maps.json"
    if not maps.is_file():
        print(f"缺少元数据（需与 NYC CSV 同目录）: {maps}", file=sys.stderr)
        return 1

    sys.path.insert(0, str(ROOT / "src"))
    from spatial_benchmark.clustering import run_multi_city_clustering_pipeline

    local_roots = default_local_city_roots()
    if not local_roots:
        print(f"在 {SOCRATA} 下未找到任何城市子目录，无法做非 NYC 本地扫描", file=sys.stderr)
        return 1

    art = args.artifacts_dir.resolve()
    art.mkdir(parents=True, exist_ok=True)

    print("[cluster] NYC raw:", nyc)
    print("[cluster] local scan roots:", {k: str(v) for k, v in sorted(local_roots.items())})

    summary = run_multi_city_clustering_pipeline(
        nyc_raw_dir=nyc,
        artifacts_dir=art,
        chicago_manifest_path=SOCRATA / "chicago" / "manifest.json",
        lacity_manifest_path=SOCRATA / "lacity" / "manifest.json",
        seattle_manifest_path=SOCRATA / "seattle" / "manifest.json",
        boston_manifest_path=SOCRATA / "boston" / "manifest.json",
        sf_manifest_path=SOCRATA / "sf" / "manifest.json",
        phoenix_manifest_path=SOCRATA / "phoenix" / "manifest.json",
        local_socrata_city_roots=local_roots,
    )
    for k, v in summary.items():
        print(f"       {k}: {v}")

    if args.skip_downstream:
        return 0

    py = sys.executable
    steps = [
        [py, str(ROOT / "scripts" / "build_ai_classification_input.py"), "--out-dir", str(art)],
        [py, str(ROOT / "scripts" / "report_taxonomy_tree.py"), "--out-dir", str(art)],
        [py, str(ROOT / "scripts" / "export_taxonomy_dendrogram.py"), "--out-html", str(art / "taxonomy_dendrogram.html")],
        [py, str(ROOT / "scripts" / "export_taxonomy_m2m_for_pg.py"), "--out-dir", str(art)],
    ]
    for cmd in steps:
        print("[run]", " ".join(cmd))
        subprocess.run(cmd, check=True, cwd=str(ROOT))
    print("[done] 产物目录:", art)
    print("       - table_catalog.json / unified_inventory.json / ai_classification_input.jsonl / csv")
    print("       - taxonomy_tree_stats.json / taxonomy_tree_report.html / taxonomy_dendrogram.html")
    print("       - dataset_taxonomy_flat.csv / pg_import_taxonomy.sql")
    print("       - taxonomy_dataset_utax_edge.csv / taxonomy_dataset_l3_edge.csv / pg_import_taxonomy_m2m.sql")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
