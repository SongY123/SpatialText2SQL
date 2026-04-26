#!/usr/bin/env python3
"""从 ai_classification_input.jsonl 导出「一张表 ↔ 多个三级分类」的多对多边表 CSV + SQL，供 PostgreSQL / pgAdmin 导入。

每张表可同时对应：
  - 多个 utax 章节（urban_chapter_ids_for_layer3）
  - 多条 l3 细项（layer3_labeling_candidates）

主表 dataset_taxonomy_flat 仍是一行一表；边表允许多行同一 dataset_uid。"""
from __future__ import annotations

import argparse
import csv
import json
from pathlib import Path


def main() -> int:
    script_dir = Path(__file__).resolve().parent
    ap = argparse.ArgumentParser(description="导出 utax/l3 多对多边表供 PostgreSQL")
    ap.add_argument(
        "--input-jsonl",
        type=Path,
        default=script_dir / "artifacts" / "seven_city_output" / "ai_classification_input.jsonl",
    )
    ap.add_argument(
        "--out-dir",
        type=Path,
        default=script_dir / "artifacts" / "seven_city_output",
    )
    args = ap.parse_args()

    rows_out: list[dict] = []
    utax_edges: list[dict] = []
    l3_edges: list[dict] = []

    for line in args.input_jsonl.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line:
            continue
        r = json.loads(line)
        uid = str(r.get("dataset_uid") or "").strip()
        tax = r.get("taxonomy") or {}
        for u in tax.get("urban_chapter_ids_for_layer3") or []:
            utax_edges.append({"dataset_uid": uid, "utax_id": str(u)})
        for c in tax.get("layer3_labeling_candidates") or []:
            if not isinstance(c, dict):
                continue
            l3_edges.append(
                {
                    "dataset_uid": uid,
                    "l3_id": str(c.get("id") or ""),
                    "label_zh": str(c.get("label_zh") or ""),
                    "urban_chapter_id": str(c.get("urban_chapter_id") or ""),
                }
            )

    args.out_dir.mkdir(parents=True, exist_ok=True)
    utax_csv = args.out_dir / "taxonomy_dataset_utax_edge.csv"
    l3_csv = args.out_dir / "taxonomy_dataset_l3_edge.csv"

    if utax_edges:
        with utax_csv.open("w", encoding="utf-8", newline="") as f:
            w = csv.DictWriter(f, fieldnames=["dataset_uid", "utax_id"])
            w.writeheader()
            w.writerows(utax_edges)
    if l3_edges:
        with l3_csv.open("w", encoding="utf-8", newline="") as f:
            w = csv.DictWriter(f, fieldnames=["dataset_uid", "l3_id", "label_zh", "urban_chapter_id"])
            w.writeheader()
            w.writerows(l3_edges)

    sql_path = args.out_dir / "pg_import_taxonomy_m2m.sql"
    sql_lines = [
        "-- 多对多：同一 dataset_uid 可有多行",
        "CREATE SCHEMA IF NOT EXISTS taxonomy;",
        "DROP TABLE IF EXISTS taxonomy.dataset_utax_edge;",
        "DROP TABLE IF EXISTS taxonomy.dataset_l3_edge;",
        """CREATE TABLE taxonomy.dataset_utax_edge (
  dataset_uid TEXT NOT NULL,
  utax_id TEXT NOT NULL,
  PRIMARY KEY (dataset_uid, utax_id)
);""",
        """CREATE TABLE taxonomy.dataset_l3_edge (
  dataset_uid TEXT NOT NULL,
  l3_id TEXT NOT NULL,
  label_zh TEXT,
  urban_chapter_id TEXT,
  PRIMARY KEY (dataset_uid, l3_id)
);""",
        "",
        "-- 在 pgAdmin：右键表 -> Import -> 选对应 CSV（UTF8，含表头）",
        f"--   {utax_csv.name}",
        f"--   {l3_csv.name}",
        "",
    ]
    sql_path.write_text("\n".join(sql_lines), encoding="utf-8")

    print(f"[done] utax edges: {len(utax_edges)} -> {utax_csv}")
    print(f"       l3 edges: {len(l3_edges)} -> {l3_csv}")
    print(f"       ddl: {sql_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
