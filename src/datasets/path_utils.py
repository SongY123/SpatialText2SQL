"""Shared path helpers for preprocessed dataset artifacts and schema caches."""

from __future__ import annotations

import os
import re
from typing import Any, Dict, List


def _slugify(value: Any) -> str:
    text = str(value).strip()
    normalized = re.sub(r"[^a-zA-Z0-9._-]+", "_", text)
    normalized = re.sub(r"_+", "_", normalized).strip("_")
    return normalized or "unknown"


def get_preprocessed_output_dir(preprocessing_config: Dict[str, Any], dataset_name: str) -> str:
    output_root = preprocessing_config.get("output_dir", "data/preprocessed")
    return os.path.join(output_root, dataset_name)


def get_schema_cache_dir(preprocessing_config: Dict[str, Any]) -> str:
    return preprocessing_config.get("schema_cache_dir", "data/schemas")


def get_schema_cache_file(
    schema_cache_dir: str,
    schema_key: str,
) -> str:
    return os.path.join(schema_cache_dir, f"{_slugify(schema_key)}_schema.txt")


def get_single_dataset_samples_file(dataset_dir: str) -> str:
    return os.path.join(dataset_dir, "samples.json")


def get_group_samples_file(
    dataset_name: str,
    dataset_dir: str,
    group_field: str,
    group_value: Any,
) -> str:
    normalized_value = str(group_value).strip()

    if dataset_name == "spatial_qa" and group_field == "level":
        return os.path.join(dataset_dir, f"level_{_slugify(normalized_value)}_samples.json")

    if dataset_name == "spatialsql_pg" and group_field == "split":
        version, _, remainder = normalized_value.partition("_")
        domain = remainder or "unknown"
        return os.path.join(
            dataset_dir,
            _slugify(version or "unknown"),
            f"{_slugify(domain)}_samples.json",
        )

    if dataset_name == "floodsql_pg" and group_field == "level":
        return os.path.join(dataset_dir, f"{_slugify(normalized_value)}_samples.json")

    return os.path.join(
        dataset_dir,
        f"{_slugify(group_field)}_{_slugify(normalized_value)}_samples.json",
    )


def get_expected_preprocessed_files(
    dataset_name: str,
    dataset_dir: str,
    grouping_fields: List[str],
    grouping_values: Dict[str, List[Any]],
) -> List[str]:
    if not grouping_fields:
        return [get_single_dataset_samples_file(dataset_dir)]

    group_field = grouping_fields[0]
    values = grouping_values.get(group_field, [])
    return [
        get_group_samples_file(dataset_name, dataset_dir, group_field, value)
        for value in values
    ]
