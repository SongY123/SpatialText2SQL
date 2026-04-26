#!/usr/bin/env python3
"""读取 ai_classification_input.jsonl，生成分类统计 JSON、树状 HTML、扁平 CSV 与 pgAdmin 可用 SQL。"""
from __future__ import annotations

import argparse
import csv
import json
import html
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any


def load_jsonl(path: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line:
            continue
        rows.append(json.loads(line))
    return rows


def main() -> int:
    script_dir = Path(__file__).resolve().parent
    ap = argparse.ArgumentParser(description="生成分类树报告与 PostgreSQL 导入文件")
    ap.add_argument(
        "--input-jsonl",
        type=Path,
        default=script_dir / "artifacts" / "seven_city_output" / "ai_classification_input.jsonl",
    )
    ap.add_argument(
        "--taxonomy-dir",
        type=Path,
        default=script_dir / "taxonomy",
    )
    ap.add_argument(
        "--out-dir",
        type=Path,
        default=script_dir / "artifacts" / "seven_city_output",
    )
    args = ap.parse_args()

    iso_path = args.taxonomy_dir / "iso_ggim_14.json"
    city_path = args.taxonomy_dir / "city_ggim_layer3_taxonomy.json"
    ggim_labels: dict[str, str] = {}
    utax_titles: dict[str, str] = {}
    if iso_path.is_file():
        iso = json.loads(iso_path.read_text(encoding="utf-8"))
        for c in iso.get("categories") or []:
            if isinstance(c, dict) and c.get("id"):
                ggim_labels[str(c["id"])] = str(c.get("label_en") or "")
    if city_path.is_file():
        city_doc = json.loads(city_path.read_text(encoding="utf-8"))
        for ch in city_doc.get("urban_chapters") or []:
            if isinstance(ch, dict) and ch.get("id"):
                utax_titles[str(ch["id"])] = str(ch.get("title_zh") or "")

    rows = load_jsonl(args.input_jsonl)
    total = len(rows)

    # --- 统计 ---
    by_city: Counter[str] = Counter()
    by_city_label: dict[str, str] = {}
    by_ggim: Counter[str] = Counter()
    by_city_ggim: dict[tuple[str, str], int] = defaultdict(int)
    by_utax: Counter[str] = Counter()
    by_l3_in_candidates: Counter[str] = Counter()

    flat_rows: list[dict[str, Any]] = []

    for r in rows:
        city = str(r.get("city") or "").strip() or "(unknown)"
        clabel = str(r.get("city_label") or "").strip()
        by_city[city] += 1
        if city not in by_city_label and clabel:
            by_city_label[city] = clabel

        tax = r.get("taxonomy") or {}
        l2 = tax.get("layer2_iso_ggim") or {}
        gid = str(l2.get("id") or "").strip() or "(no_ggim)"
        g_en = str(l2.get("label_en") or ggim_labels.get(gid, ""))
        by_ggim[gid] += 1
        by_city_ggim[(city, gid)] += 1

        utax_list = list(tax.get("urban_chapter_ids_for_layer3") or [])
        for u in utax_list:
            by_utax[str(u)] += 1

        for c in tax.get("layer3_labeling_candidates") or []:
            if isinstance(c, dict) and c.get("id"):
                by_l3_in_candidates[str(c["id"])] += 1

        flat_rows.append(
            {
                "dataset_uid": r.get("dataset_uid"),
                "city": city,
                "city_label": clabel,
                "dataset_name": r.get("dataset_name"),
                "layer2_ggim_id": gid,
                "layer2_ggim_label_en": g_en,
                "utax_chapters": ",".join(utax_list),
                "layer3_candidate_count": len(tax.get("layer3_labeling_candidates") or []),
            }
        )

    cities_sorted = sorted(by_city.keys())
    ggim_order = [f"ggim_{i}" for i in range(1, 15)]

    stats: dict[str, Any] = {
        "schema_version": "1.0",
        "source_jsonl": str(args.input_jsonl),
        "total_tables": total,
        "layer1_by_city": {c: by_city[c] for c in cities_sorted},
        "layer2_by_ggim": {g: by_ggim[g] for g in ggim_order if by_ggim[g]},
        "layer2_by_ggim_other": {g: by_ggim[g] for g in sorted(by_ggim.keys()) if g not in set(ggim_order)},
        "utax_chapter_table_hits": {
            u: by_utax[u] for u in sorted(by_utax.keys(), key=lambda x: (len(x), x))
        },
        "note_zh": "utax 计数表示「有多少张表在该表的第三层候选章节中包含该 utax」（一张表可计入多个 utax）。",
    }

    args.out_dir.mkdir(parents=True, exist_ok=True)
    stats_path = args.out_dir / "taxonomy_tree_stats.json"
    stats_path.write_text(json.dumps(stats, ensure_ascii=False, indent=2), encoding="utf-8")

    csv_path = args.out_dir / "dataset_taxonomy_flat.csv"
    if flat_rows:
        with csv_path.open("w", encoding="utf-8", newline="") as f:
            w = csv.DictWriter(f, fieldnames=list(flat_rows[0].keys()))
            w.writeheader()
            w.writerows(flat_rows)

    # --- HTML 树 ---
    def esc(s: Any) -> str:
        return html.escape(str(s), quote=True)

    parts: list[str] = []
    parts.append("<!DOCTYPE html><html lang='zh-CN'><head><meta charset='utf-8'/>")
    parts.append("<title>三层分类统计树</title>")
    parts.append(
        "<style>"
        "body{font-family:Segoe UI,Microsoft YaHei,sans-serif;margin:24px;background:#f6f7f9;color:#1a1a1a;}"
        "h1{font-size:1.35rem;}"
        "h2{font-size:1.1rem;margin-top:2rem;color:#333;}"
        ".tree{margin-left:0;padding-left:0;list-style:none;}"
        ".tree ul{margin:4px 0 8px 1rem;padding-left:1rem;border-left:1px solid #ccc;}"
        ".tree li{margin:6px 0;}"
        ".n{font-weight:600;color:#0b57d0;}"
        ".badge{display:inline-block;background:#e8f0fe;color:#174ea6;padding:1px 8px;border-radius:10px;font-size:0.85rem;margin-left:6px;}"
        ".muted{color:#555;font-size:0.92rem;}"
        "details{margin:4px 0;}"
        "summary{cursor:pointer;}"
        "</style></head><body>"
    )
    parts.append(f"<h1>数据集三层分类架构（共 <span class='badge'>{total}</span> 张表）</h1>")
    parts.append(
        "<p class='muted'>第一层：城市 · 第二层：ISO ggim_1..14 · 第三层：按 utax 章节展开的候选标签（见 jsonl 中 taxonomy 字段）。"
        "下列「utax 命中」表示有多少张表在该表的候选章节列表中包含该章节。</p>"
    )

    # 树 1：城市 → ggim
    parts.append("<h2>树状图 1：城市 → 第二层（ISO GGIM）表数量</h2><ul class='tree'>")
    for city in cities_sorted:
        ccount = by_city[city]
        cl = by_city_label.get(city, "")
        parts.append("<li><details open>")
        parts.append(
            f"<summary><span class='n'>{esc(city)}</span>"
            f"<span class='badge'>{ccount}</span>"
            f"<span class='muted'> {esc(cl)}</span></summary><ul>"
        )
        sub: dict[str, int] = {}
        for (ct, gid), cnt in by_city_ggim.items():
            if ct == city:
                sub[gid] = cnt
        for gid in sorted(sub.keys(), key=lambda x: (x not in ggim_order, ggim_order.index(x) if x in ggim_order else 99, x)):
            gname = ggim_labels.get(gid, "")
            parts.append(
                "<li>"
                f"<span class='n'>{esc(gid)}</span> {esc(gname)}"
                f"<span class='badge'>{sub[gid]}</span>"
                "</li>"
            )
        parts.append("</ul></details></li>")
    parts.append("</ul>")

    # 树 2：全局 GGIM
    parts.append("<h2>树状图 2：全局 · 第二层（ISO GGIM）表数量</h2><ul class='tree'>")
    for gid in ggim_order:
        if not by_ggim[gid]:
            continue
        parts.append(
            "<li>"
            f"<span class='n'>{esc(gid)}</span> {esc(ggim_labels.get(gid,''))}"
            f"<span class='badge'>{by_ggim[gid]}</span>"
            "</li>"
        )
    for gid in sorted(by_ggim.keys()):
        if gid in ggim_order:
            continue
        parts.append(
            "<li>"
            f"<span class='n'>{esc(gid)}</span>"
            f"<span class='badge'>{by_ggim[gid]}</span>"
            "</li>"
        )
    parts.append("</ul>")

    # 树 3：utax 章节
    parts.append("<h2>树状图 3：城市场景章节（utax）· 表命中数</h2>")
    parts.append("<p class='muted'>「命中」= 该表 <code>urban_chapter_ids_for_layer3</code> 含此 utax。</p>")
    parts.append("<ul class='tree'>")
    for u in sorted(by_utax.keys(), key=lambda x: (len(x), x)):
        title = utax_titles.get(u, "")
        parts.append(
            "<li>"
            f"<span class='n'>{esc(u)}</span> {esc(title)}"
            f"<span class='badge'>{by_utax[u]}</span>"
            "</li>"
        )
    parts.append("</ul>")

    # Top layer3 候选出现频次（可选）
    parts.append("<h2>第三层候选标签出现频次（按表计数：候选列表中含该 l3_id 的表数）</h2>")
    parts.append("<p class='muted'>仅作分布参考；正式「第三层归类」需人工或模型在候选中择一。</p><ul class='tree'>")
    for lid, cnt in by_l3_in_candidates.most_common(40):
        parts.append(f"<li><code>{esc(lid)}</code> <span class='badge'>{cnt}</span></li>")
    if len(by_l3_in_candidates) > 40:
        parts.append(f"<li class='muted'>… 其余 {len(by_l3_in_candidates) - 40} 个 l3 项略</li>")
    parts.append("</ul>")

    parts.append("</body></html>")
    html_path = args.out_dir / "taxonomy_tree_report.html"
    html_path.write_text("".join(parts), encoding="utf-8")

    # --- SQL：CREATE + INSERT（便于 Query Tool 一次执行）---
    sql_lines = [
        "-- 在目标库执行；表位于 taxonomy schema",
        "CREATE SCHEMA IF NOT EXISTS taxonomy;",
        "DROP TABLE IF EXISTS taxonomy.dataset_classification;",
        """CREATE TABLE taxonomy.dataset_classification (
  dataset_uid TEXT PRIMARY KEY,
  city TEXT,
  city_label TEXT,
  dataset_name TEXT,
  layer2_ggim_id TEXT,
  layer2_ggim_label_en TEXT,
  utax_chapters TEXT,
  layer3_candidate_count INTEGER
);""",
        "",
    ]
    for fr in flat_rows:
        vals = [
            "'" + str(fr["dataset_uid"]).replace("'", "''") + "'",
            "'" + str(fr["city"]).replace("'", "''") + "'",
            "'" + str(fr["city_label"]).replace("'", "''") + "'",
            "'" + str(fr["dataset_name"]).replace("'", "''") + "'",
            "'" + str(fr["layer2_ggim_id"]).replace("'", "''") + "'",
            "'" + str(fr["layer2_ggim_label_en"]).replace("'", "''") + "'",
            "'" + str(fr["utax_chapters"]).replace("'", "''") + "'",
            str(int(fr["layer3_candidate_count"])),
        ]
        sql_lines.append(
            "INSERT INTO taxonomy.dataset_classification "
            f"VALUES ({', '.join(vals)});"
        )
    sql_path = args.out_dir / "pg_import_taxonomy.sql"
    sql_path.write_text("\n".join(sql_lines), encoding="utf-8")

    print(f"[done] total_tables={total}")
    print(f"       stats_json={stats_path}")
    print(f"       html_tree={html_path}")
    print(f"       flat_csv={csv_path}")
    print(f"       pg_sql={sql_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
