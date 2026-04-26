#!/usr/bin/env python3
"""从 table_catalog.json（及可选 unified_inventory.json）导出供标注用的精简输入。

默认叠加三层分类约定（见 scripts/taxonomy/）：城市 → ISO ggim_1..14 → 第三层精细化（utax 章节 + l3_xx_yy）。
可用 --no-taxonomy 关闭。"""
from __future__ import annotations

import argparse
import csv
import json
from pathlib import Path
from typing import Any


def _load_taxonomy_context(tax_dir: Path) -> dict[str, Any] | None:
    """加载 iso GGIM 14、utax 桥接与第三层目录；文件不全则返回 None。"""
    iso_path = tax_dir / "iso_ggim_14.json"
    bridge_path = tax_dir / "ggim_to_urban_chapters.json"
    city_path = tax_dir / "city_ggim_layer3_taxonomy.json"
    if not iso_path.is_file() or not bridge_path.is_file() or not city_path.is_file():
        return None
    iso = json.loads(iso_path.read_text(encoding="utf-8"))
    bridge_doc = json.loads(bridge_path.read_text(encoding="utf-8"))
    city = json.loads(city_path.read_text(encoding="utf-8"))
    bridge = bridge_doc.get("mappings") or {}
    chapters_by_id = {c["id"]: c for c in (city.get("urban_chapters") or []) if isinstance(c, dict) and c.get("id")}
    label_by_ggim = {c["id"]: c["label_en"] for c in (iso.get("categories") or []) if isinstance(c, dict) and c.get("id")}
    return {
        "bridge": bridge,
        "chapters_by_id": chapters_by_id,
        "label_by_ggim": label_by_ggim,
        "meta": {
            "schema_version": city.get("schema_version"),
            "description_zh": city.get("description_zh"),
        },
    }


def _layer3_candidates_for_ggim(
    primary_ggim: str | None,
    bridge: dict[str, Any],
    chapters_by_id: dict[str, Any],
) -> tuple[list[str], list[dict[str, Any]]]:
    """返回 (urban_chapter_ids, 去重后的第三层候选列表)。"""
    gid = str(primary_ggim or "").strip()
    if not gid:
        return [], []
    utax_ids = list(bridge.get(gid) or [])
    seen: set[str] = set()
    items: list[dict[str, Any]] = []
    for u in utax_ids:
        ch = chapters_by_id.get(u)
        if not ch:
            continue
        for l3 in ch.get("layer3") or []:
            if not isinstance(l3, dict):
                continue
            lid = str(l3.get("id") or "").strip()
            if not lid or lid in seen:
                continue
            seen.add(lid)
            items.append(
                {
                    "id": lid,
                    "label_zh": l3.get("label_zh"),
                    "urban_chapter_id": u,
                }
            )
    return utax_ids, items


def _dataset_uid(row: dict[str, Any]) -> str:
    city = str(row.get("city") or "").strip()
    did = str(row.get("dataset_id") or "").strip()
    return f"{city}:{did}" if city and did else ""


def _load_unified_descriptions(path: Path) -> dict[str, str]:
    if not path.exists():
        return {}
    data = json.loads(path.read_text(encoding="utf-8"))
    out: dict[str, str] = {}
    for item in data:
        if not isinstance(item, dict):
            continue
        uid = str(item.get("dataset_uid") or "").strip()
        desc = str(item.get("description") or "").strip()
        if uid and desc:
            out[uid] = desc
    return out


def _truncate_list(items: list[str], max_items: int) -> list[str]:
    if max_items <= 0:
        return []
    return items[:max_items]


def _truncate_text(text: str, max_chars: int) -> str:
    if max_chars <= 0:
        return ""
    text = text.strip()
    if len(text) <= max_chars:
        return text
    return text[: max_chars - 3] + "..."


def build_row(
    row: dict[str, Any],
    *,
    desc_by_uid: dict[str, str],
    max_columns: int,
    max_desc_chars: int,
    include_explanations: bool,
    taxonomy_ctx: dict[str, Any] | None = None,
) -> dict[str, Any]:
    uid = _dataset_uid(row)
    description = desc_by_uid.get(uid, "")
    columns = row.get("columns") or []
    if not isinstance(columns, list):
        columns = []
    col_strs = [str(c) for c in columns]
    spatial = row.get("spatial_columns") or []
    if not isinstance(spatial, list):
        spatial = []
    joins = row.get("joinable_keys") or []
    if not isinstance(joins, list):
        joins = []

    out: dict[str, Any] = {
        "dataset_uid": uid,
        "city": row.get("city"),
        "city_label": row.get("city_label"),
        "dataset_id": row.get("dataset_id"),
        "dataset_name": row.get("dataset_name"),
        "file_name": row.get("file_name"),
        "description": _truncate_text(description, max_desc_chars),
        "baseline": {
            "theme_label": row.get("theme_label"),
            "primary_scenario": row.get("primary_scenario"),
            "primary_ggim_category": row.get("primary_ggim_category"),
            "theme_confidence": row.get("theme_confidence"),
            "scenario_confidence": row.get("scenario_confidence"),
            "ggim_category_confidence": row.get("ggim_category_confidence"),
        },
        "schema_hint": {
            "n_columns": row.get("n_columns"),
            "columns_sample": _truncate_list(col_strs, max_columns),
            "geometry_type": row.get("geometry_type"),
            "has_spatial_columns": row.get("has_spatial_columns"),
            "spatial_columns": spatial,
            "joinable_keys": joins,
        },
        "provenance": {
            "asset_url": row.get("asset_url"),
            "last_updated": row.get("last_updated"),
            "views": row.get("views"),
        },
    }
    if include_explanations:
        out["baseline_explanations"] = {
            "theme_explanation": _truncate_text(str(row.get("theme_explanation") or ""), 1200),
            "scenario_explanation": _truncate_text(str(row.get("scenario_explanation") or ""), 1200),
            "ggim_category_explanation": _truncate_text(str(row.get("ggim_category_explanation") or ""), 1200),
        }
        out["baseline"]["ggim_category_candidates"] = row.get("ggim_category_candidates")
        out["baseline"]["scenario_candidates"] = row.get("scenario_candidates")

    if taxonomy_ctx:
        pg = str(row.get("primary_ggim_category") or "").strip()
        label_en = taxonomy_ctx["label_by_ggim"].get(pg, "")
        utax_ids, l3_items = _layer3_candidates_for_ggim(
            pg, taxonomy_ctx["bridge"], taxonomy_ctx["chapters_by_id"]
        )
        out["taxonomy"] = {
            "schema_version": taxonomy_ctx["meta"].get("schema_version"),
            "layer1_city": {
                "city_code": row.get("city"),
                "city_label": row.get("city_label"),
            },
            "layer2_iso_ggim": {
                "id": pg,
                "label_en": label_en,
            },
            "urban_chapter_ids_for_layer3": utax_ids,
            "layer3_labeling_candidates": l3_items,
        }
    return out


def _write_jsonl(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="\n") as f:
        for row in rows:
            f.write(json.dumps(row, ensure_ascii=False) + "\n")


def _flatten_for_csv(row: dict[str, Any]) -> dict[str, Any]:
    flat = {
        "dataset_uid": row.get("dataset_uid"),
        "city": row.get("city"),
        "city_label": row.get("city_label"),
        "dataset_id": row.get("dataset_id"),
        "dataset_name": row.get("dataset_name"),
        "file_name": row.get("file_name"),
        "description": row.get("description"),
        "theme_label": row["baseline"].get("theme_label"),
        "primary_scenario": row["baseline"].get("primary_scenario"),
        "primary_ggim_category": row["baseline"].get("primary_ggim_category"),
        "theme_confidence": row["baseline"].get("theme_confidence"),
        "scenario_confidence": row["baseline"].get("scenario_confidence"),
        "ggim_category_confidence": row["baseline"].get("ggim_category_confidence"),
        "n_columns": row["schema_hint"].get("n_columns"),
        "geometry_type": row["schema_hint"].get("geometry_type"),
        "has_spatial_columns": row["schema_hint"].get("has_spatial_columns"),
        "columns_sample": " | ".join(row["schema_hint"].get("columns_sample") or []),
        "spatial_columns": " | ".join(row["schema_hint"].get("spatial_columns") or []),
        "joinable_keys": " | ".join(row["schema_hint"].get("joinable_keys") or []),
        "asset_url": row["provenance"].get("asset_url"),
        "last_updated": row["provenance"].get("last_updated"),
        "views": row["provenance"].get("views"),
    }
    if "taxonomy" in row:
        t = row["taxonomy"]
        l2 = t.get("layer2_iso_ggim") or {}
        flat["taxonomy_layer2_ggim_id"] = l2.get("id")
        flat["taxonomy_layer2_ggim_label_en"] = l2.get("label_en")
        flat["taxonomy_utax_chapters"] = " | ".join(t.get("urban_chapter_ids_for_layer3") or [])
        cands = t.get("layer3_labeling_candidates") or []
        flat["taxonomy_layer3_candidate_count"] = len(cands)
        flat["taxonomy_layer3_candidates_preview"] = " | ".join(
            str(x.get("label_zh") or "") for x in cands[:8]
        )
    if "baseline_explanations" in row:
        be = row["baseline_explanations"]
        flat["theme_explanation"] = be.get("theme_explanation")
        flat["scenario_explanation"] = be.get("scenario_explanation")
        flat["ggim_category_explanation"] = be.get("ggim_category_explanation")
    return flat


def _write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    if not rows:
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    flat_rows = [_flatten_for_csv(r) for r in rows]
    fieldnames = list(flat_rows[0].keys())
    with path.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        w.writerows(flat_rows)


def main() -> None:
    script_dir = Path(__file__).resolve().parent
    default_catalog = script_dir / "artifacts" / "seven_city_output" / "table_catalog.json"
    default_unified = script_dir / "artifacts" / "seven_city_output" / "unified_inventory.json"
    default_out_dir = script_dir / "artifacts" / "seven_city_output"

    parser = argparse.ArgumentParser(
        description="Export AI-friendly rows from table_catalog (+ optional unified_inventory for descriptions)."
    )
    parser.add_argument(
        "--table-catalog",
        type=Path,
        default=default_catalog,
        help="Path to table_catalog.json",
    )
    parser.add_argument(
        "--unified-inventory",
        type=Path,
        default=default_unified,
        help="Path to unified_inventory.json (for description merge). Use empty string to skip.",
    )
    parser.add_argument(
        "--out-dir",
        type=Path,
        default=default_out_dir,
        help="Output directory for jsonl/csv/json",
    )
    parser.add_argument("--basename", default="ai_classification_input", help="Output file basename (without suffix).")
    parser.add_argument("--max-columns", type=int, default=40, help="Max column names in columns_sample.")
    parser.add_argument("--max-desc-chars", type=int, default=1200, help="Max length for description field.")
    parser.add_argument(
        "--include-explanations",
        action="store_true",
        help="Include long theme/scenario/ggim explanations (larger output).",
    )
    parser.add_argument("--write-json", action="store_true", help="Also write a single JSON array file.")
    parser.add_argument(
        "--taxonomy-dir",
        type=Path,
        default=script_dir / "taxonomy",
        help="含 iso_ggim_14.json / ggim_to_urban_chapters.json / city_ggim_layer3_taxonomy.json 的目录",
    )
    parser.add_argument(
        "--no-taxonomy",
        action="store_true",
        help="不附加三层 taxonomy 字段（即使 taxonomy 目录存在）",
    )
    args = parser.parse_args()

    catalog_path = args.table_catalog.resolve()
    unified_path = args.unified_inventory
    if unified_path is not None and str(unified_path).strip() == "":
        unified_path = None
    if unified_path is not None:
        unified_path = Path(unified_path).resolve()

    raw = json.loads(catalog_path.read_text(encoding="utf-8"))
    if not isinstance(raw, list):
        raise SystemExit("table_catalog.json must be a JSON array")

    desc_by_uid: dict[str, str] = {}
    if unified_path and unified_path.exists():
        desc_by_uid = _load_unified_descriptions(unified_path)
        print(f"[merge] descriptions loaded: {len(desc_by_uid)} from {unified_path}")
    elif unified_path:
        print(f"[merge] unified_inventory not found, skip descriptions: {unified_path}")

    taxonomy_ctx: dict[str, Any] | None = None
    if not args.no_taxonomy:
        taxonomy_ctx = _load_taxonomy_context(args.taxonomy_dir.resolve())
        if taxonomy_ctx:
            print(f"[taxonomy] loaded from {args.taxonomy_dir.resolve()}")
        else:
            print(f"[taxonomy] skipped (missing files under {args.taxonomy_dir.resolve()})")

    rows_out: list[dict[str, Any]] = []
    for row in raw:
        if not isinstance(row, dict):
            continue
        rows_out.append(
            build_row(
                row,
                desc_by_uid=desc_by_uid,
                max_columns=max(0, args.max_columns),
                max_desc_chars=max(0, args.max_desc_chars),
                include_explanations=args.include_explanations,
                taxonomy_ctx=taxonomy_ctx,
            )
        )

    out_dir = args.out_dir.resolve()
    out_dir.mkdir(parents=True, exist_ok=True)
    base = args.basename
    jsonl_path = out_dir / f"{base}.jsonl"
    csv_path = out_dir / f"{base}.csv"

    _write_jsonl(jsonl_path, rows_out)
    _write_csv(csv_path, rows_out)
    print(f"[done] rows={len(rows_out)}")
    print(f"       jsonl={jsonl_path}")
    print(f"       csv={csv_path}")

    if args.write_json:
        json_path = out_dir / f"{base}.json"
        json_path.write_text(json.dumps(rows_out, indent=2, ensure_ascii=False), encoding="utf-8")
        print(f"       json={json_path}")


if __name__ == "__main__":
    main()
