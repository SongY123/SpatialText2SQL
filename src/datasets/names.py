"""Canonical dataset name helpers."""

from __future__ import annotations

from typing import Iterable


DATASET_ALIASES = {
    "spatial_qa": "spatialqueryqa",
    "spatialsql_pg": "spatialsql",
    "floodsql_pg": "floodsql",
}


def canonicalize_dataset_name(dataset_name: str | None) -> str:
    normalized = str(dataset_name or "").strip()
    if not normalized:
        return ""
    return DATASET_ALIASES.get(normalized, normalized)


def dataset_name_matches(dataset_name: str | None, *expected_names: str) -> bool:
    canonical = canonicalize_dataset_name(dataset_name)
    return canonical in {canonicalize_dataset_name(name) for name in expected_names}


def canonicalize_dataset_names(dataset_names: Iterable[str]) -> list[str]:
    deduped: list[str] = []
    seen: set[str] = set()
    for dataset_name in dataset_names:
        canonical = canonicalize_dataset_name(dataset_name)
        if not canonical or canonical in seen:
            continue
        seen.add(canonical)
        deduped.append(canonical)
    return deduped
