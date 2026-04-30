"""FloodSQL-Bench 数据集加载器。"""
from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

from src.datasets.base import BaseDataLoader


FLOODSQL_BENCHMARK_SPECS: List[Dict[str, str]] = [
    {"family": "single_table", "questions_file": "50.json", "results_file": "50_results.jsonl"},
    {"family": "double_table_key", "questions_file": "100.json", "results_file": "100_results.jsonl"},
    {"family": "double_table_spatial", "questions_file": "150.json", "results_file": "150_results.jsonl"},
    {"family": "triple_table_key", "questions_file": "50.json", "results_file": "50_results.jsonl"},
    {
        "family": "triple_table_key_spatial_updated",
        "questions_file": "50.json",
        "results_file": "50_results.jsonl",
    },
    {
        "family": "triple_table_spatial_spatial",
        "questions_file": "50.json",
        "results_file": "50_results.jsonl",
    },
]
FLOODSQL_LEVELS = ["L0", "L1", "L2", "L3", "L4", "L5"]
FLOODSQL_FAMILIES = [spec["family"] for spec in FLOODSQL_BENCHMARK_SPECS]
FLOODSQL_CATEGORY_FALLBACKS = {
    "single_table": "single table",
    "double_table_key": "double table key-based",
    "double_table_spatial": "double table spatial",
    "triple_table_key": "triple table key-based",
    "triple_table_key_spatial_updated": "triple table key-spatial",
    "triple_table_spatial_spatial": "triple table spatial-spatial",
}
REPO_ROOT = Path(__file__).resolve().parents[3]


def _load_json(path: str) -> Any:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def _load_jsonl(path: str) -> Iterable[Dict[str, Any]]:
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            stripped = line.strip()
            if stripped:
                yield json.loads(stripped)


def _candidate_benchmark_roots(data_path: str | os.PathLike[str]) -> List[Path]:
    raw_path = Path(data_path).expanduser()
    candidates: List[Path] = []
    if raw_path.is_absolute():
        candidates.append(raw_path)
    else:
        candidates.append(raw_path.resolve())
        candidates.append((REPO_ROOT / raw_path).resolve())
    candidates.append((REPO_ROOT / "FloodSQL-Bench").resolve())
    candidates.append((REPO_ROOT.parent / "FloodSQL-Bench").resolve())

    deduped: List[Path] = []
    seen = set()
    for candidate in candidates:
        normalized = str(candidate)
        if normalized in seen:
            continue
        seen.add(normalized)
        deduped.append(candidate)
    return deduped


def _resolve_benchmark_root(data_path: str | os.PathLike[str]) -> Path:
    for candidate in _candidate_benchmark_roots(data_path):
        if (candidate / "benchmark").is_dir():
            return candidate
        if candidate.name == "benchmark" and candidate.is_dir():
            return candidate.parent
    first_candidate = _candidate_benchmark_roots(data_path)[0]
    if first_candidate.name == "benchmark":
        return first_candidate.parent
    return first_candidate


class FloodSQLLoader(BaseDataLoader):
    """从 FloodSQL-Bench benchmark 目录加载更新版 443 条官方样本。"""

    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        self.data_path = config.get("data_path", "../FloodSQL-Bench")
        self.benchmark_specs = config.get("benchmark_specs", FLOODSQL_BENCHMARK_SPECS)

    def load_raw_data(self, data_path: str) -> List[Dict[str, Any]]:
        root = _resolve_benchmark_root(data_path or self.data_path)
        benchmark_root = root / "benchmark"
        raw_records: List[Dict[str, Any]] = []

        if not benchmark_root.exists():
            print(f"警告: FloodSQL benchmark 根目录不存在 {benchmark_root}")

        for spec in self.benchmark_specs:
            family = spec["family"]
            family_dir = benchmark_root / family
            questions_path = family_dir / spec["questions_file"]
            results_path = family_dir / spec["results_file"]
            if not questions_path.exists():
                print(f"警告: FloodSQL benchmark 文件不存在 {questions_path}")
                continue

            question_rows = _load_json(str(questions_path))
            result_by_id = {}
            if results_path.exists():
                result_by_id = {
                    row.get("id"): row
                    for row in _load_jsonl(str(results_path))
                    if isinstance(row, dict) and row.get("id")
                }
            else:
                print(f"警告: FloodSQL 结果文件不存在 {results_path}")

            for row in question_rows:
                if not isinstance(row, dict):
                    continue
                source_id = row.get("id")
                result_row = result_by_id.get(source_id, {})
                merged = dict(row)
                merged["_family"] = family
                merged["_questions_file"] = str(questions_path)
                merged["_results_file"] = str(results_path) if results_path.exists() else None
                merged["row_count"] = result_row.get("row_count", row.get("row_count"))
                merged["result"] = result_row.get("result", row.get("result"))
                merged["elapsed"] = result_row.get("elapsed", row.get("elapsed"))
                raw_records.append(merged)

        if raw_records:
            print(f"成功加载 FloodSQL 原始记录: {len(raw_records)} 条")
        return raw_records

    def extract_questions_and_sqls(self, raw_data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        extracted: List[Dict[str, Any]] = []
        for row in raw_data:
            question = (row.get("question") or "").strip()
            source_sql = (row.get("sql") or "").strip()
            source_id = (row.get("id") or "").strip()
            if not question or not source_sql or not source_id:
                continue

            level = (row.get("level") or source_id.split("_", 1)[0] or "unknown").strip()
            family = (row.get("_family") or row.get("family") or "unknown").strip()
            category = (
                (row.get("category") or FLOODSQL_CATEGORY_FALLBACKS.get(family) or family)
                .strip()
            )
            output_type = (row.get("output_type") or "").strip()
            expected_columns = row.get("expected_columns") or []
            if not isinstance(expected_columns, list):
                expected_columns = [str(expected_columns)]

            extracted.append(
                {
                    "id": source_id,
                    "question": question,
                    "source_sql": source_sql,
                    "gold_sql": source_sql,
                    "gold_sql_candidates": [],
                    "source_backend": "duckdb",
                    "target_backend": "postgres",
                    "repair_status": "raw",
                    "repair_source": "source",
                    "metadata": {
                        "source_id": source_id,
                        "level": level,
                        "family": family,
                        "category": category,
                        "output_type": output_type,
                        "expected_columns": expected_columns,
                        "official_row_count": row.get("row_count"),
                        "official_result": row.get("result"),
                        "official_elapsed": row.get("elapsed"),
                        "questions_file": row.get("_questions_file"),
                        "results_file": row.get("_results_file"),
                    },
                }
            )
        return extracted

    def get_dataset_info(self) -> Dict[str, Any]:
        return {
            "name": "floodsql_pg",
            "grouping_fields": ["level", "family"],
            "grouping_values": {
                "level": FLOODSQL_LEVELS,
                "family": FLOODSQL_FAMILIES,
            },
        }
