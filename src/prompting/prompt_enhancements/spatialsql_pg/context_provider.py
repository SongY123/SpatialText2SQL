"""Load raw SpatialSQL QA context blocks for prompt enrichment."""

from __future__ import annotations

import re
from pathlib import Path
from typing import Dict, Optional, Tuple


def _parse_qa_block(block: str) -> Optional[Dict[str, str]]:
    lines = [line.strip() for line in block.strip().splitlines() if line.strip()]
    if not lines:
        return None

    record: Dict[str, str] = {}
    for line in lines:
        idx = line.find(":")
        if idx <= 0:
            continue
        key = line[:idx].strip()
        value = line[idx + 1 :].strip()
        record[key] = value

    if not record.get("id"):
        return None
    return record


class SpatialSQLContextProvider:
    """Read original SpatialSQL QA hints from `sdbdatasets` on demand."""

    def __init__(self, project_root: Path | str, raw_data_path: str = "sdbdatasets"):
        self.project_root = Path(project_root).resolve()
        raw_root = Path(raw_data_path)
        if raw_root.is_absolute():
            self.raw_data_root = raw_root
        else:
            self.raw_data_root = (self.project_root / raw_data_path).resolve()
        self._split_cache: Dict[Tuple[str, str], Dict[str, Dict[str, str]]] = {}

    def get_context(
        self,
        dataset_name: str,
        metadata: Optional[Dict[str, object]],
    ) -> Optional[Dict[str, str]]:
        if not dataset_name or not dataset_name.startswith("spatialsql_pg"):
            return None

        metadata = metadata or {}
        split = str(metadata.get("split") or "").strip()
        source_id = str(metadata.get("source_id") or "").strip()
        if not split or not source_id:
            return None

        split_parts = split.split("_", 1)
        if len(split_parts) != 2:
            return None

        version, domain = split_parts
        records = self._load_split(version, domain)
        return records.get(source_id)

    def _load_split(self, version: str, domain: str) -> Dict[str, Dict[str, str]]:
        cache_key = (version, domain)
        if cache_key in self._split_cache:
            return self._split_cache[cache_key]

        split_dir = self.raw_data_root / version / domain
        records: Dict[str, Dict[str, str]] = {}
        if not split_dir.is_dir():
            self._split_cache[cache_key] = records
            return records

        for qa_file in sorted(split_dir.glob("QA-*.txt")):
            try:
                content = qa_file.read_text(encoding="utf-8", errors="replace")
            except OSError:
                continue

            for block in re.split(r"\n\s*\n", content):
                record = _parse_qa_block(block)
                if not record:
                    continue
                records[record["id"]] = record

        self._split_cache[cache_key] = records
        return records
