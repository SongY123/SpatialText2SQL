"""I/O for canonical tables and synthesized databases."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Mapping

from .models import CanonicalSpatialTable, SynthesizedSpatialDatabase
from .utils import to_text


def _load_canonical_tables_from_jsonl(path: Path) -> list[CanonicalSpatialTable]:
    tables: list[CanonicalSpatialTable] = []
    with path.open("r", encoding="utf-8") as handle:
        for line_number, line in enumerate(handle, start=1):
            stripped = line.strip()
            if not stripped:
                continue
            try:
                payload = json.loads(stripped)
            except json.JSONDecodeError as exc:
                raise ValueError(f"Invalid JSON on line {line_number} of {path}: {exc}") from exc
            try:
                table = CanonicalSpatialTable.from_dict(payload)
            except ValueError as exc:
                raise ValueError(f"Invalid canonical table on line {line_number} of {path}: {exc}") from exc
            tables.append(table)
    return tables


def _dataset_to_table_payload(
    city_metadata: Mapping[str, Any],
    dataset: Mapping[str, Any],
) -> dict[str, Any]:
    payload = dict(dataset)
    payload.setdefault("table_id", dataset.get("table_id") or dataset.get("id"))
    payload.setdefault("city", city_metadata.get("city_id") or city_metadata.get("City") or city_metadata.get("city"))
    payload.setdefault(
        "table_name",
        dataset.get("table_name") or dataset.get("canonical_name") or dataset.get("name"),
    )
    payload.setdefault("normalized_schema", dataset.get("normalized_schema") or dataset.get("columns"))
    payload.setdefault("themes", dataset.get("themes") or dataset.get("thematic_labels"))
    return payload


def _iter_table_payloads_from_json(payload: Any) -> list[dict[str, Any]]:
    table_payloads: list[dict[str, Any]] = []
    if isinstance(payload, Mapping):
        datasets = payload.get("datasets")
        if isinstance(datasets, list):
            for dataset in datasets:
                if isinstance(dataset, Mapping):
                    table_payloads.append(_dataset_to_table_payload(payload, dataset))
        else:
            table_payloads.append(dict(payload))
        return table_payloads

    if isinstance(payload, list):
        for item in payload:
            if not isinstance(item, Mapping):
                continue
            datasets = item.get("datasets")
            if isinstance(datasets, list):
                for dataset in datasets:
                    if isinstance(dataset, Mapping):
                        table_payloads.append(_dataset_to_table_payload(item, dataset))
            else:
                table_payloads.append(dict(item))
        return table_payloads
    return table_payloads


def _load_canonical_tables_from_json(path: Path) -> list[CanonicalSpatialTable]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise ValueError(f"Invalid JSON in {path}: {exc}") from exc

    tables: list[CanonicalSpatialTable] = []
    for index, table_payload in enumerate(_iter_table_payloads_from_json(payload), start=1):
        try:
            table = CanonicalSpatialTable.from_dict(table_payload)
        except ValueError as exc:
            raise ValueError(f"Invalid canonical table entry #{index} in {path}: {exc}") from exc
        tables.append(table)
    return tables


def load_canonical_tables(input_path: str) -> list[CanonicalSpatialTable]:
    path = Path(input_path)
    if not path.is_file():
        raise FileNotFoundError(f"Input canonical table file not found: {path}")

    if path.suffix.lower() == ".jsonl":
        return _load_canonical_tables_from_jsonl(path)
    return _load_canonical_tables_from_json(path)


def write_synthesized_databases(
    output_path: str,
    databases: list[SynthesizedSpatialDatabase],
) -> None:
    path = Path(output_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        for database in databases:
            handle.write(json.dumps(database.to_dict(), ensure_ascii=False))
            handle.write("\n")


def load_synthesized_databases(input_path: str) -> list[SynthesizedSpatialDatabase]:
    path = Path(input_path)
    if not path.is_file():
        raise FileNotFoundError(f"Synthesized database JSONL file not found: {path}")

    databases: list[SynthesizedSpatialDatabase] = []
    with path.open("r", encoding="utf-8") as handle:
        for line_number, line in enumerate(handle, start=1):
            stripped = line.strip()
            if not stripped:
                continue
            try:
                payload = json.loads(stripped)
            except json.JSONDecodeError as exc:
                raise ValueError(f"Invalid JSON on line {line_number} of {path}: {exc}") from exc
            try:
                databases.append(SynthesizedSpatialDatabase.from_dict(payload))
            except ValueError as exc:
                raise ValueError(
                    f"Invalid synthesized database on line {line_number} of {path}: {exc}"
                ) from exc
    return databases
