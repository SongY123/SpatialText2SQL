#!/usr/bin/env python3
"""Export executable-oriented DB blueprints from sampled databases."""

from __future__ import annotations

import argparse
import csv
import json
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


def _slug_table_name(dataset_uid: str) -> str:
    # city:dataset_id -> t_dataset_id
    if ":" in dataset_uid:
        _, did = dataset_uid.split(":", 1)
    else:
        did = dataset_uid
    out = "".join(ch if ch.isalnum() or ch == "_" else "_" for ch in did.lower())
    out = "_".join([p for p in out.split("_") if p])
    if not out:
        out = "table_unknown"
    if out[0].isdigit():
        out = "t_" + out
    return "t_" + out


def _normalize_col_name(name: str) -> str:
    out = "".join(ch if ch.isalnum() or ch == "_" else "_" for ch in (name or "").lower())
    out = "_".join([p for p in out.split("_") if p])
    if not out:
        out = "col_unknown"
    if out[0].isdigit():
        out = "c_" + out
    return out


def _map_to_pg_type(t: str) -> str:
    x = (t or "").lower()
    if x == "bigint":
        return "BIGINT"
    if x == "double":
        return "DOUBLE PRECISION"
    if x == "boolean":
        return "BOOLEAN"
    if x == "date":
        return "DATE"
    if x == "timestamp":
        return "TIMESTAMP"
    if x == "json":
        return "JSONB"
    if x == "geometry":
        return "GEOMETRY"
    return "TEXT"


def _guess_edge_kind(
    left: dict[str, Any],
    right: dict[str, Any],
    edge_score: float,
) -> tuple[str, dict[str, Any]]:
    lkeys = set(str(x).lower() for x in (left.get("joinable_keys") or []))
    rkeys = set(str(x).lower() for x in (right.get("joinable_keys") or []))
    shared = sorted(lkeys & rkeys)
    if shared:
        return "key_join", {"key_candidates": shared[:5]}

    lsp = left.get("spatial_columns") or []
    rsp = right.get("spatial_columns") or []
    if lsp and rsp:
        return "spatial_join", {"predicate_candidates": ["ST_Intersects", "ST_Within", "ST_DWithin"]}

    return "semantic_link", {"score": round(float(edge_score), 6)}


def build_blueprints(
    canonical_rows: list[dict[str, Any]],
    sampled_rows: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    c_map = {str(r.get("dataset_uid") or ""): r for r in canonical_rows}
    out: list[dict[str, Any]] = []

    for s in sampled_rows:
        sample_id = str(s.get("sample_id") or "")
        city = str(s.get("city") or "")
        schema_name = "synth_" + "".join(ch if ch.isalnum() or ch == "_" else "_" for ch in sample_id.lower())

        tables_payload = []
        uid_to_table_name: dict[str, str] = {}
        for uid in s.get("tables") or []:
            uid = str(uid)
            crow = c_map.get(uid, {})
            tname = _slug_table_name(uid)
            uid_to_table_name[uid] = tname

            cols = []
            for c in crow.get("normalized_columns") or []:
                cname = _normalize_col_name(str(c.get("name") or ""))
                ctype = _map_to_pg_type(str(c.get("normalized_type") or "text"))
                cols.append({"name": cname, "type": ctype, "source_name": c.get("name")})
            if not cols:
                cols = [{"name": "col_placeholder", "type": "TEXT", "source_name": None}]

            spatial_fields = []
            geom_type = ((crow.get("spatial_meta") or {}).get("geometry_type") if isinstance(crow.get("spatial_meta"), dict) else None) or "UNKNOWN"
            crs = ((crow.get("spatial_meta") or {}).get("crs") if isinstance(crow.get("spatial_meta"), dict) else None) or "unknown"
            for sf in crow.get("spatial_columns") or []:
                spatial_fields.append({"field": _normalize_col_name(str(sf)), "source_field": sf, "geometry_type": geom_type, "crs": crs})

            tables_payload.append(
                {
                    "dataset_uid": uid,
                    "table_name": tname,
                    "dataset_name": crow.get("dataset_name"),
                    "columns": cols,
                    "spatial_fields": spatial_fields,
                    "joinable_keys": crow.get("joinable_keys") or [],
                    "thematic_labels": crow.get("thematic_labels") or [],
                }
            )

        relations = []
        for e in s.get("edges") or []:
            src = str(e.get("src") or "")
            dst = str(e.get("dst") or "")
            if src not in c_map or dst not in c_map:
                continue
            kind, detail = _guess_edge_kind(c_map[src], c_map[dst], float(e.get("score") or 0.0))
            relations.append(
                {
                    "left_dataset_uid": src,
                    "right_dataset_uid": dst,
                    "left_table": uid_to_table_name.get(src, _slug_table_name(src)),
                    "right_table": uid_to_table_name.get(dst, _slug_table_name(dst)),
                    "edge_score": round(float(e.get("score") or 0.0), 6),
                    "kind": kind,
                    "detail": detail,
                }
            )

        out.append(
            {
                "sample_id": sample_id,
                "city": city,
                "schema_name": schema_name,
                "seed_table": s.get("seed_table"),
                "stats": s.get("stats") or {},
                "tables": tables_payload,
                "relations": relations,
            }
        )
    return out


def write_sql(path: Path, blueprints: list[dict[str, Any]]) -> None:
    lines: list[str] = ["-- Auto-generated synthesized DB blueprints (DDL sketch)"]
    for bp in blueprints:
        schema_name = bp["schema_name"]
        lines.append("")
        lines.append(f"-- sample_id: {bp['sample_id']}  city: {bp['city']}")
        lines.append(f"CREATE SCHEMA IF NOT EXISTS {schema_name};")
        for t in bp.get("tables") or []:
            col_defs = []
            for c in t.get("columns") or []:
                col_defs.append(f"  {c['name']} {c['type']}")
            if not col_defs:
                col_defs.append("  col_placeholder TEXT")
            lines.append(f"CREATE TABLE IF NOT EXISTS {schema_name}.{t['table_name']} (")
            lines.append(",\n".join(col_defs))
            lines.append(");")
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> int:
    script_dir = Path(__file__).resolve().parent
    ap = argparse.ArgumentParser(description="Export executable-oriented DB blueprints.")
    ap.add_argument(
        "--canonical-jsonl",
        type=Path,
        default=script_dir / "artifacts" / "seven_city_output" / "canonical_tables.jsonl",
    )
    ap.add_argument(
        "--sampled-jsonl",
        type=Path,
        default=script_dir / "artifacts" / "seven_city_output" / "sampled_databases.jsonl",
    )
    ap.add_argument(
        "--out-dir",
        type=Path,
        default=script_dir / "artifacts" / "seven_city_output",
    )
    args = ap.parse_args()

    canonical = load_jsonl(args.canonical_jsonl)
    sampled = load_jsonl(args.sampled_jsonl)
    blueprints = build_blueprints(canonical, sampled)

    args.out_dir.mkdir(parents=True, exist_ok=True)
    out_jsonl = args.out_dir / "db_blueprints.jsonl"
    with out_jsonl.open("w", encoding="utf-8", newline="\n") as f:
        for row in blueprints:
            f.write(json.dumps(row, ensure_ascii=False) + "\n")

    out_csv = args.out_dir / "db_blueprints_summary.csv"
    with out_csv.open("w", encoding="utf-8", newline="") as f:
        fields = ["sample_id", "city", "schema_name", "n_tables", "n_relations", "avg_similarity", "jump_count"]
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        for bp in blueprints:
            st = bp.get("stats") or {}
            w.writerow(
                {
                    "sample_id": bp["sample_id"],
                    "city": bp["city"],
                    "schema_name": bp["schema_name"],
                    "n_tables": len(bp.get("tables") or []),
                    "n_relations": len(bp.get("relations") or []),
                    "avg_similarity": st.get("avg_similarity"),
                    "jump_count": st.get("jump_count"),
                }
            )

    out_sql = args.out_dir / "db_blueprints.sql"
    write_sql(out_sql, blueprints)

    print(f"[done] blueprints={len(blueprints)}")
    print(f"       {out_jsonl.resolve()}")
    print(f"       {out_csv.resolve()}")
    print(f"       {out_sql.resolve()}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
