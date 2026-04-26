#!/usr/bin/env python3
"""Build canonical spatial tables and synthesized DB samples from table catalog.

Stage mapping:
1) Table Canonicalization
   - table normalization
   - spatial column identification
   - thematic labeling pass-through
2) DB Synthesis
   - relation graph construction
   - relation discovery matrix export
   - random-walk schema sampling
"""

from __future__ import annotations

import argparse
import csv
import json
import math
import random
import re
import sys
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

TOKEN_RE = re.compile(r"[a-z0-9_]+")
SPATIAL_HINTS = (
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
STOPWORDS = {
    "the",
    "and",
    "or",
    "of",
    "for",
    "to",
    "in",
    "on",
    "by",
    "with",
    "from",
    "a",
    "an",
    "is",
    "are",
    "at",
    "as",
}


def _ensure_csv_field_limit() -> None:
    limit = sys.maxsize
    while True:
        try:
            csv.field_size_limit(limit)
            return
        except OverflowError:
            limit = int(limit / 10)


@dataclass
class CanonicalRow:
    dataset_uid: str
    city: str
    city_label: str
    dataset_id: str
    dataset_name: str
    file_name: str
    summary: str
    thematic_labels: list[str]
    columns: list[str]
    normalized_columns: list[dict[str, str]]
    representative_values: dict[str, str]
    spatial_columns: list[str]
    geometry_type: str
    crs: str
    joinable_keys: list[str]

    def to_json(self) -> dict[str, Any]:
        return {
            "dataset_uid": self.dataset_uid,
            "city": self.city,
            "city_label": self.city_label,
            "dataset_id": self.dataset_id,
            "dataset_name": self.dataset_name,
            "file_name": self.file_name,
            "summary": self.summary,
            "thematic_labels": self.thematic_labels,
            "columns": self.columns,
            "normalized_columns": self.normalized_columns,
            "representative_values": self.representative_values,
            "spatial_columns": self.spatial_columns,
            "spatial_meta": {
                "geometry_type": self.geometry_type,
                "crs": self.crs,
            },
            "joinable_keys": self.joinable_keys,
        }


def _tokenize(text: str) -> list[str]:
    out = [tok for tok in TOKEN_RE.findall((text or "").lower()) if tok and tok not in STOPWORDS]
    return out


def _cosine(c1: Counter[str], c2: Counter[str]) -> float:
    if not c1 or not c2:
        return 0.0
    dot = 0.0
    for token, weight in c1.items():
        dot += weight * c2.get(token, 0.0)
    if dot <= 0:
        return 0.0
    n1 = math.sqrt(sum(v * v for v in c1.values()))
    n2 = math.sqrt(sum(v * v for v in c2.values()))
    if n1 <= 0 or n2 <= 0:
        return 0.0
    return float(dot / (n1 * n2))


def _load_csv_sample(path: Path) -> dict[str, str]:
    for enc in ("utf-8-sig", "utf-8", "latin-1"):
        try:
            with path.open("r", encoding=enc, newline="") as handle:
                reader = csv.DictReader(handle)
                for row in reader:
                    if row:
                        return {str(k): str(v) for k, v in row.items()}
            return {}
        except UnicodeDecodeError:
            continue
    return {}


def _load_geojson_sample(path: Path) -> tuple[dict[str, str], str, str]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:  # noqa: BLE001
        return {}, "UNKNOWN", "unknown"
    features = payload.get("features") or []
    first = (features[0] if features else {}) or {}
    props = first.get("properties") or {}
    geom = first.get("geometry") or {}
    geom_type = str(geom.get("type") or "UNKNOWN").upper()
    crs = "unknown"
    crs_obj = payload.get("crs") or {}
    if isinstance(crs_obj, dict):
        p = crs_obj.get("properties") or {}
        crs = str(p.get("name") or crs_obj.get("type") or "unknown")
    return ({str(k): str(v) for k, v in props.items()}, geom_type, crs)


def _normalize_type(col: str, value: str) -> str:
    cl = (col or "").lower()
    val = (value or "").strip()
    if any(h == cl or h in cl for h in SPATIAL_HINTS):
        return "geometry"
    if "geom" in cl or "shape" in cl:
        return "geometry"
    if not val:
        if "date" in cl:
            return "date"
        if "time" in cl:
            return "timestamp"
        if cl.startswith("is_") or cl.startswith("has_"):
            return "boolean"
        if any(x in cl for x in ("lat", "lon", "lng", "x_", "y_", "_x", "_y")):
            return "double"
        if any(x in cl for x in ("count", "num", "year", "id")):
            return "bigint"
        return "text"
    lv = val.lower()
    if lv in {"true", "false", "t", "f", "yes", "no", "y", "n"}:
        return "boolean"
    if re.match(r"^-?\d+$", val):
        return "bigint"
    if re.match(r"^-?\d+\.\d+$", val):
        return "double"
    if re.match(r"^\d{4}-\d{2}-\d{2}$", val) or re.match(r"^\d{1,2}/\d{1,2}/\d{2,4}$", val):
        return "date"
    if re.match(r"^\d{4}-\d{2}-\d{2}[ tT]\d{2}:\d{2}", val):
        return "timestamp"
    if val.startswith("{") or val.startswith("["):
        return "json"
    if re.match(r"^(POINT|LINESTRING|POLYGON|MULTI)", lv.upper()):
        return "geometry"
    return "text"


def _spatial_score(left: CanonicalRow, right: CanonicalRow) -> float:
    score = 0.0
    lk = set(left.joinable_keys)
    rk = set(right.joinable_keys)
    if lk and rk:
        inter = len(lk & rk)
        union = len(lk | rk)
        score += 0.5 * (inter / union if union else 0.0)
    lg = left.geometry_type
    rg = right.geometry_type
    if lg == rg and lg and lg != "UNKNOWN":
        score += 0.35
    elif {lg, rg} & {"POINT", "POINT_COORDINATES"} and {lg, rg} & {"POLYGON", "MULTIPOLYGON"}:
        score += 0.25
    elif lg != "UNKNOWN" and rg != "UNKNOWN":
        score += 0.15
    ls = set(left.spatial_columns)
    rs = set(right.spatial_columns)
    if ls and rs:
        score += 0.15 * (len(ls & rs) / max(len(ls), len(rs)))
    return min(score, 1.0)


def _weighted_tokens(row: CanonicalRow) -> Counter[str]:
    c: Counter[str] = Counter()
    for t in _tokenize(row.dataset_name):
        c[t] += 3
    for t in _tokenize(row.summary):
        c[t] += 2
    for label in row.thematic_labels:
        for t in _tokenize(label):
            c[t] += 2
    for col in row.columns:
        for t in _tokenize(col):
            c[t] += 1
    for _k, v in row.representative_values.items():
        for t in _tokenize(v):
            c[t] += 1
    return c


def _city_roots(socrata_root: Path) -> dict[str, Path]:
    seattle = socrata_root / "seattle_maps_geojson"
    if not seattle.is_dir():
        seattle = socrata_root / "seattle"
    return {
        "boston": socrata_root / "boston",
        "chicago": socrata_root / "chicago",
        "lacity": socrata_root / "lacity",
        "nyc": socrata_root / "nyc-opendata",
        "phoenix": socrata_root / "phoenix",
        "sf": socrata_root / "sf",
        "seattle": seattle,
    }


def _index_city_files(root: Path) -> dict[str, list[Path]]:
    out: dict[str, list[Path]] = defaultdict(list)
    if not root.is_dir():
        return out
    for pattern in ("*.csv", "*.geojson"):
        for p in root.rglob(pattern):
            if not p.is_file():
                continue
            out[p.name].append(p)
    return out


def _resolve_file_path(file_name: str, city_index: dict[str, list[Path]]) -> Path | None:
    candidates = city_index.get(file_name) or []
    if not candidates:
        return None
    candidates = sorted(candidates, key=lambda x: len(str(x)))
    return candidates[0]


def _sample_size(rng: random.Random, lo: int, hi: int, mean: float, std: float = 1.8) -> int:
    for _ in range(20):
        x = int(round(rng.gauss(mean, std)))
        if lo <= x <= hi:
            return x
    return max(lo, min(hi, int(round(mean))))


def main() -> int:
    _ensure_csv_field_limit()
    script_dir = Path(__file__).resolve().parent
    ap = argparse.ArgumentParser(description="Run table canonicalization and DB synthesis.")
    ap.add_argument(
        "--catalog",
        type=Path,
        default=script_dir / "artifacts" / "seven_city_output" / "table_catalog.json",
        help="Input table_catalog.json",
    )
    ap.add_argument(
        "--unified",
        type=Path,
        default=script_dir / "artifacts" / "seven_city_output" / "unified_inventory.json",
        help="Input unified_inventory.json for descriptions",
    )
    ap.add_argument(
        "--socrata-root",
        type=Path,
        default=script_dir / "artifacts" / "socrata_maps",
        help="Root for local source files",
    )
    ap.add_argument(
        "--out-dir",
        type=Path,
        default=script_dir / "artifacts" / "seven_city_output",
        help="Output directory",
    )
    ap.add_argument("--similarity-threshold", type=float, default=0.55)
    ap.add_argument("--spatial-threshold", type=float, default=0.35)
    ap.add_argument(
        "--target-avg-degree",
        type=float,
        default=3.0,
        help="Target average degree per city graph (edge count induced by ranking)",
    )
    ap.add_argument("--jump-probability", type=float, default=0.1)
    ap.add_argument("--sample-size-min", type=int, default=3)
    ap.add_argument("--sample-size-max", type=int, default=12)
    ap.add_argument("--sample-size-mean", type=float, default=8.0)
    ap.add_argument("--sample-size-std", type=float, default=2.0)
    ap.add_argument("--max-jump-per-walk", type=int, default=2)
    ap.add_argument("--samples-per-city", type=int, default=80)
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument(
        "--max-value-samples-per-table",
        type=int,
        default=20,
        help="Maximum representative value cells stored per table",
    )
    ap.add_argument(
        "--max-value-chars",
        type=int,
        default=80,
        help="Maximum characters per representative value",
    )
    args = ap.parse_args()

    out_dir = args.out_dir.resolve()
    out_dir.mkdir(parents=True, exist_ok=True)

    table_catalog = json.loads(args.catalog.read_text(encoding="utf-8"))
    unified = {}
    if args.unified.is_file():
        for row in json.loads(args.unified.read_text(encoding="utf-8")):
            uid = str(row.get("dataset_uid") or "").strip()
            if uid:
                unified[uid] = str(row.get("description") or "")

    city_to_root = _city_roots(args.socrata_root.resolve())
    city_to_index = {city: _index_city_files(root) for city, root in city_to_root.items()}

    canonical_rows: list[CanonicalRow] = []
    for row in table_catalog:
        city = str(row.get("city") or "")
        did = str(row.get("dataset_id") or "")
        uid = f"{city}:{did}" if city and did else ""
        summary = str(unified.get(uid) or row.get("theme_explanation") or "")
        file_name = str(row.get("file_name") or "")
        sample_values: dict[str, str] = {}
        crs = "unknown"
        geometry_type = str(row.get("geometry_type") or "UNKNOWN")
        if city in city_to_index and file_name:
            fpath = _resolve_file_path(file_name, city_to_index[city])
            if fpath and fpath.is_file():
                if fpath.suffix.lower() == ".csv":
                    sample_values = _load_csv_sample(fpath)
                else:
                    sample_values, inferred_geom, inferred_crs = _load_geojson_sample(fpath)
                    if inferred_geom and inferred_geom != "UNKNOWN":
                        geometry_type = inferred_geom
                    crs = inferred_crs
        columns = [str(c) for c in (row.get("columns") or [])]
        normalized_cols = []
        for c in columns:
            normalized_cols.append(
                {
                    "name": c,
                    "normalized_type": _normalize_type(c, sample_values.get(c, "")),
                }
            )
        representative_values: dict[str, str] = {}
        if sample_values and args.max_value_samples_per_table > 0:
            for c in columns:
                if len(representative_values) >= args.max_value_samples_per_table:
                    break
                raw = str(sample_values.get(c, "")).strip()
                if not raw:
                    continue
                if len(raw) > args.max_value_chars:
                    raw = raw[: args.max_value_chars - 3] + "..."
                representative_values[c] = raw
        thematic_labels = [
            str(row.get("theme_label") or ""),
            str(row.get("primary_ggim_category") or ""),
            str(row.get("primary_scenario") or ""),
        ]
        thematic_labels = [x for x in thematic_labels if x]
        canonical_rows.append(
            CanonicalRow(
                dataset_uid=uid,
                city=city,
                city_label=str(row.get("city_label") or city),
                dataset_id=did,
                dataset_name=str(row.get("dataset_name") or ""),
                file_name=file_name,
                summary=summary,
                thematic_labels=thematic_labels,
                columns=columns,
                normalized_columns=normalized_cols,
                representative_values=representative_values,
                spatial_columns=[str(c) for c in (row.get("spatial_columns") or [])],
                geometry_type=geometry_type,
                crs=crs,
                joinable_keys=[str(c) for c in (row.get("joinable_keys") or [])],
            )
        )

    canonical_rows = sorted(canonical_rows, key=lambda r: (r.city, r.dataset_uid))
    canonical_jsonl = out_dir / "canonical_tables.jsonl"
    with canonical_jsonl.open("w", encoding="utf-8", newline="\n") as handle:
        for row in canonical_rows:
            handle.write(json.dumps(row.to_json(), ensure_ascii=False) + "\n")

    canonical_csv = out_dir / "canonical_tables.csv"
    with canonical_csv.open("w", encoding="utf-8", newline="") as handle:
        fieldnames = [
            "dataset_uid",
            "city",
            "dataset_name",
            "file_name",
            "geometry_type",
            "crs",
            "n_columns",
            "n_spatial_columns",
            "n_representative_values",
            "thematic_labels",
            "joinable_keys",
        ]
        w = csv.DictWriter(handle, fieldnames=fieldnames)
        w.writeheader()
        for row in canonical_rows:
            w.writerow(
                {
                    "dataset_uid": row.dataset_uid,
                    "city": row.city,
                    "dataset_name": row.dataset_name,
                    "file_name": row.file_name,
                    "geometry_type": row.geometry_type,
                    "crs": row.crs,
                    "n_columns": len(row.columns),
                    "n_spatial_columns": len(row.spatial_columns),
                    "n_representative_values": len(row.representative_values),
                    "thematic_labels": " | ".join(row.thematic_labels),
                    "joinable_keys": " | ".join(row.joinable_keys),
                }
            )

    by_city: dict[str, list[CanonicalRow]] = defaultdict(list)
    for row in canonical_rows:
        by_city[row.city].append(row)

    edge_rows: list[dict[str, Any]] = []
    matrix_files: list[str] = []
    graph_payload: dict[str, Any] = {
        "params": {
            "semantic_threshold": args.similarity_threshold,
            "spatial_threshold": args.spatial_threshold,
            "target_avg_degree": args.target_avg_degree,
        },
        "cities": {},
    }
    weight_lookup: dict[tuple[str, str], float] = {}
    neigh: dict[str, list[tuple[str, float]]] = defaultdict(list)

    for city, rows in sorted(by_city.items()):
        token_map = {r.dataset_uid: _weighted_tokens(r) for r in rows}
        ids = [r.dataset_uid for r in rows]
        id_to_row = {r.dataset_uid: r for r in rows}
        matrix: list[list[float]] = [[0.0 for _ in ids] for _ in ids]
        city_edges: list[dict[str, Any]] = []
        pair_candidates: list[dict[str, Any]] = []
        for i, uid_i in enumerate(ids):
            for j in range(i + 1, len(ids)):
                uid_j = ids[j]
                semantic = _cosine(token_map[uid_i], token_map[uid_j])
                spatial = _spatial_score(id_to_row[uid_i], id_to_row[uid_j])
                combined = 0.75 * semantic + 0.25 * spatial
                matrix[i][j] = combined
                matrix[j][i] = combined
                pair_candidates.append(
                    {
                        "city": city,
                        "src": uid_i,
                        "dst": uid_j,
                        "semantic_score": round(semantic, 6),
                        "spatial_score": round(spatial, 6),
                        "combined_score": round(combined, 6),
                    }
                )

        pair_candidates.sort(key=lambda x: x["combined_score"], reverse=True)
        n_nodes = len(ids)
        max_edges = n_nodes * (n_nodes - 1) // 2
        target_edges = int(round(args.target_avg_degree * n_nodes / 2.0))
        target_edges = max(0, min(max_edges, target_edges))
        for cand in pair_candidates[:target_edges]:
            semantic = float(cand["semantic_score"])
            spatial = float(cand["spatial_score"])
            cand["edge_types"] = [
                t
                for t, ok in (
                    ("semantic_sim", semantic >= args.similarity_threshold),
                    ("spatial_related", spatial >= args.spatial_threshold),
                )
                if ok
            ]
            if not cand["edge_types"]:
                cand["edge_types"] = ["mixed_ranked_link"]
            city_edges.append(cand)
            edge_rows.append(cand)
            score = float(cand["combined_score"])
            uid_i = str(cand["src"])
            uid_j = str(cand["dst"])
            weight_lookup[(uid_i, uid_j)] = score
            weight_lookup[(uid_j, uid_i)] = score
            neigh[uid_i].append((uid_j, score))
            neigh[uid_j].append((uid_i, score))

        graph_payload["cities"][city] = {
            "n_nodes": len(ids),
            "n_edges": len(city_edges),
            "edges": city_edges,
        }

        mpath = out_dir / f"relation_matrix_{city}.csv"
        with mpath.open("w", encoding="utf-8", newline="") as handle:
            w = csv.writer(handle)
            w.writerow(["dataset_uid", *ids])
            for i, uid in enumerate(ids):
                w.writerow([uid, *[f"{matrix[i][j]:.6f}" for j in range(len(ids))]])
        matrix_files.append(str(mpath))

    graph_json = out_dir / "relation_graph.json"
    graph_json.write_text(json.dumps(graph_payload, ensure_ascii=False, indent=2), encoding="utf-8")

    edges_csv = out_dir / "relation_edges.csv"
    with edges_csv.open("w", encoding="utf-8", newline="") as handle:
        fieldnames = ["city", "src", "dst", "semantic_score", "spatial_score", "combined_score", "edge_types"]
        w = csv.DictWriter(handle, fieldnames=fieldnames)
        w.writeheader()
        for row in edge_rows:
            out = dict(row)
            out["edge_types"] = " | ".join(out["edge_types"])
            w.writerow(out)

    rng = random.Random(args.seed)
    samples: list[dict[str, Any]] = []
    sample_summary_rows: list[dict[str, Any]] = []
    for city, rows in sorted(by_city.items()):
        if not rows:
            continue
        all_ids = [r.dataset_uid for r in rows]
        id_to_row = {r.dataset_uid: r for r in rows}
        n_samples = min(args.samples_per_city, max(10, len(rows) * 2))
        for i in range(n_samples):
            target = _sample_size(
                rng,
                args.sample_size_min,
                args.sample_size_max,
                args.sample_size_mean,
                args.sample_size_std,
            )
            seed_uid = rng.choice(all_ids)
            picked = [seed_uid]
            picked_set = {seed_uid}
            trace: list[dict[str, Any]] = [{"step": 0, "node": seed_uid, "mode": "seed"}]
            jumps = 0
            cur = seed_uid
            guard = 0
            while len(picked) < target and guard < target * 20:
                guard += 1
                do_jump = rng.random() < args.jump_probability and jumps < args.max_jump_per_walk
                candidate = None
                mode = "walk"
                if not do_jump:
                    opts = [(n, w) for n, w in neigh.get(cur, []) if n not in picked_set and n.split(":")[0] == city]
                    if opts:
                        s = sum(max(w, 1e-6) for _n, w in opts)
                        rnum = rng.random() * s
                        acc = 0.0
                        for nid, w in opts:
                            acc += max(w, 1e-6)
                            if acc >= rnum:
                                candidate = nid
                                break
                if candidate is None:
                    remain = [nid for nid in all_ids if nid not in picked_set]
                    if not remain:
                        break
                    candidate = rng.choice(remain)
                    mode = "jump"
                    jumps += 1
                picked.append(candidate)
                picked_set.add(candidate)
                trace.append({"step": len(trace), "node": candidate, "mode": mode})
                cur = candidate
            edges_in_sample = []
            sem_vals = []
            for a in range(len(picked)):
                for b in range(a + 1, len(picked)):
                    u = picked[a]
                    v = picked[b]
                    w = weight_lookup.get((u, v), 0.0)
                    if w > 0:
                        edges_in_sample.append({"src": u, "dst": v, "score": round(w, 6)})
                        sem_vals.append(w)
            spatial_tables = sum(1 for uid in picked if id_to_row[uid].spatial_columns)
            avg_sim = (sum(sem_vals) / len(sem_vals)) if sem_vals else 0.0
            payload = {
                "sample_id": f"{city}_{i+1:04d}",
                "city": city,
                "seed_table": seed_uid,
                "tables": picked,
                "edges": edges_in_sample,
                "walk_trace": trace,
                "stats": {
                    "n_tables": len(picked),
                    "n_edges": len(edges_in_sample),
                    "avg_similarity": round(avg_sim, 6),
                    "jump_count": jumps,
                    "spatial_coverage": round(spatial_tables / len(picked), 6) if picked else 0.0,
                },
            }
            samples.append(payload)
            sample_summary_rows.append(
                {
                    "sample_id": payload["sample_id"],
                    "city": city,
                    "seed_table": seed_uid,
                    "n_tables": payload["stats"]["n_tables"],
                    "n_edges": payload["stats"]["n_edges"],
                    "avg_similarity": payload["stats"]["avg_similarity"],
                    "jump_count": payload["stats"]["jump_count"],
                    "spatial_coverage": payload["stats"]["spatial_coverage"],
                }
            )

    samples_jsonl = out_dir / "sampled_databases.jsonl"
    with samples_jsonl.open("w", encoding="utf-8", newline="\n") as handle:
        for s in samples:
            handle.write(json.dumps(s, ensure_ascii=False) + "\n")

    samples_csv = out_dir / "sampled_databases_summary.csv"
    with samples_csv.open("w", encoding="utf-8", newline="") as handle:
        fields = ["sample_id", "city", "seed_table", "n_tables", "n_edges", "avg_similarity", "jump_count", "spatial_coverage"]
        w = csv.DictWriter(handle, fieldnames=fields)
        w.writeheader()
        w.writerows(sample_summary_rows)

    by_city_sample = defaultdict(list)
    for row in sample_summary_rows:
        by_city_sample[row["city"]].append(row)
    report = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "inputs": {
            "catalog": str(args.catalog.resolve()),
            "unified": str(args.unified.resolve()),
            "socrata_root": str(args.socrata_root.resolve()),
        },
        "decisions": {
            "labeling_participates_in_edge_weight": False,
            "representative_values_in_similarity": True,
            "max_value_samples_per_table": args.max_value_samples_per_table,
            "max_value_chars": args.max_value_chars,
            "similarity_threshold": args.similarity_threshold,
            "spatial_threshold": args.spatial_threshold,
            "jump_probability": args.jump_probability,
            "target_avg_degree": args.target_avg_degree,
            "sample_size_range": [args.sample_size_min, args.sample_size_max],
            "sample_size_mean": args.sample_size_mean,
            "sample_size_std": args.sample_size_std,
            "max_jump_per_walk": args.max_jump_per_walk,
            "seed": args.seed,
        },
        "outputs": {
            "canonical_jsonl": str(canonical_jsonl),
            "canonical_csv": str(canonical_csv),
            "relation_graph_json": str(graph_json),
            "relation_edges_csv": str(edges_csv),
            "relation_matrix_files": matrix_files,
            "sampled_databases_jsonl": str(samples_jsonl),
            "sampled_databases_summary_csv": str(samples_csv),
        },
        "stats": {
            "n_canonical_tables": len(canonical_rows),
            "n_relation_edges": len(edge_rows),
            "n_sampled_databases": len(samples),
            "by_city_samples": {
                city: {
                    "n_samples": len(rows),
                    "avg_tables": round(sum(r["n_tables"] for r in rows) / len(rows), 4) if rows else 0.0,
                    "avg_jump_count": round(sum(r["jump_count"] for r in rows) / len(rows), 4) if rows else 0.0,
                    "avg_spatial_coverage": round(sum(r["spatial_coverage"] for r in rows) / len(rows), 4) if rows else 0.0,
                }
                for city, rows in sorted(by_city_sample.items())
            },
        },
    }
    report_path = out_dir / "canonical_db_synthesis_report.json"
    report_path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    report_md = out_dir / "canonical_db_synthesis_report.md"
    md_lines = [
        "# Canonicalization + DB Synthesis Report",
        "",
        f"- generated_at: `{report['generated_at']}`",
        f"- n_canonical_tables: `{report['stats']['n_canonical_tables']}`",
        f"- n_relation_edges: `{report['stats']['n_relation_edges']}`",
        f"- n_sampled_databases: `{report['stats']['n_sampled_databases']}`",
        "",
        "## Decisions",
        f"- similarity_threshold: `{args.similarity_threshold}`",
        f"- spatial_threshold: `{args.spatial_threshold}`",
        f"- representative_values_in_similarity: `True`",
        f"- max_value_samples_per_table: `{args.max_value_samples_per_table}`",
        f"- max_value_chars: `{args.max_value_chars}`",
        f"- jump_probability: `{args.jump_probability}`",
        f"- target_avg_degree: `{args.target_avg_degree}`",
        f"- sample_size_range: `[{args.sample_size_min}, {args.sample_size_max}]`",
        f"- sample_size_mean: `{args.sample_size_mean}`",
        f"- sample_size_std: `{args.sample_size_std}`",
        f"- max_jump_per_walk: `{args.max_jump_per_walk}`",
        f"- labeling_participates_in_edge_weight: `False`",
        "",
        "## Outputs",
        f"- canonical: `{canonical_jsonl}` / `{canonical_csv}`",
        f"- relation graph: `{graph_json}` / `{edges_csv}`",
        f"- sampled databases: `{samples_jsonl}` / `{samples_csv}`",
        "",
        "## By City Sample Stats",
    ]
    for city, payload in sorted(report["stats"]["by_city_samples"].items()):
        md_lines.append(
            f"- {city}: samples={payload['n_samples']}, avg_tables={payload['avg_tables']}, "
            f"avg_jump_count={payload['avg_jump_count']}, avg_spatial_coverage={payload['avg_spatial_coverage']}"
        )
    report_md.write_text("\n".join(md_lines) + "\n", encoding="utf-8")

    print(f"[done] canonical tables: {len(canonical_rows)} -> {canonical_jsonl}")
    print(f"[done] relation graph edges: {len(edge_rows)} -> {graph_json}")
    print(f"[done] sampled databases: {len(samples)} -> {samples_jsonl}")
    print(f"[done] report: {report_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
