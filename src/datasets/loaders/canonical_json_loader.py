"""Loader for already-normalized benchmark JSON inputs."""

from __future__ import annotations

import json
import re
from typing import Any, Dict, List

from src.datasets.base import BaseDataLoader
from src.datasets.names import canonicalize_dataset_name


class CanonicalJSONLoader(BaseDataLoader):
    """Load unified benchmark samples from a single JSON file."""

    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        self.data_path = config.get("data_path", "")
        self.dataset_name = canonicalize_dataset_name(config.get("dataset_name", "unknown"))
        self.grouping = config.get("grouping", {})
        self.source_partitions = config.get("source_partitions", {})

    def load_raw_data(self, data_path: str) -> List[Dict[str, Any]]:
        target_path = data_path or self.data_path
        with open(target_path, "r", encoding="utf-8") as handle:
            payload = json.load(handle)
        if not isinstance(payload, list):
            raise ValueError(f"统一 benchmark 输入必须是 JSON array: {target_path}")
        return payload

    def extract_questions_and_sqls(self, raw_data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        extracted: List[Dict[str, Any]] = []
        for idx, row in enumerate(raw_data, start=1):
            if not isinstance(row, dict):
                continue
            item = dict(row)

            question = str(item.get("question") or "").strip()
            gold_sql = str(item.get("gold_sql") or item.get("sql") or "").strip()
            if not question or not gold_sql:
                continue

            item["id"] = item.get("id") or f"{self.dataset_name}_{idx:05d}"
            item["question"] = question
            item["gold_sql"] = gold_sql
            item["sql"] = str(item.get("sql") or gold_sql).strip()
            item["dataset"] = self.dataset_name

            metadata = item.get("metadata")
            if not isinstance(metadata, dict):
                metadata = {}
            metadata.update(self._derive_metadata(item))
            item["metadata"] = metadata

            level = item.get("level") or metadata.get("level")
            if level is not None:
                item["level"] = level
                metadata.setdefault("level", level)

            extracted.append(item)
        return extracted

    def get_dataset_info(self) -> Dict[str, Any]:
        grouping_fields = self.grouping.get("fields", [])
        grouping_values = self.grouping.get("values", {})
        return {
            "name": self.dataset_name,
            "grouping_fields": grouping_fields,
            "grouping_values": grouping_values,
        }

    def _derive_metadata(self, item: Dict[str, Any]) -> Dict[str, Any]:
        metadata: Dict[str, Any] = {}
        for key in ("level", "domain", "family", "dataset_version", "source_id", "database_key", "schema_name"):
            value = item.get(key)
            if value not in (None, ""):
                metadata[key] = value

        partition = self._derive_partition_from_id(str(item.get("id") or ""))
        if partition:
            for key in ("level", "level_id", "domain", "family", "dataset_version", "database_key", "schema_name"):
                value = partition.get(key)
                if value not in (None, ""):
                    metadata.setdefault(key, value)

        if self.dataset_name == "spatialsql":
            level = str(item.get("level") or "").strip()
            if level:
                metadata.setdefault("domain", level)

        return metadata

    def _derive_partition_from_id(self, item_id: str) -> Dict[str, Any]:
        normalized_id = str(item_id or "").strip()
        if not normalized_id:
            return {}

        prefix_pattern = re.compile(rf"^{re.escape(self.dataset_name)}_(?P<partition>[^_]+_[^_]+)_")
        match = prefix_pattern.match(normalized_id)
        if not match:
            return {}
        partition_key = match.group("partition")
        partition = self.source_partitions.get(partition_key)
        return dict(partition) if isinstance(partition, dict) else {}
