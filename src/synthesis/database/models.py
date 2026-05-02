"""Structured data models for spatial database synthesis."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Mapping

from .utils import (
    normalize_representative_values,
    normalize_schema,
    normalize_spatial_fields,
    normalize_themes,
    stable_jsonify,
    to_text,
    unique_preserve_order,
)


def _require_text(value: Any, field_name: str) -> str:
    text = to_text(value)
    if not text:
        raise ValueError(f"Missing required field: {field_name}")
    return text


@dataclass
class CanonicalSpatialTable:
    """Normalized table metadata used for synthesis."""

    table_id: str
    city: str
    table_name: str
    semantic_summary: str = ""
    normalized_schema: list[dict[str, Any]] = field(default_factory=list)
    representative_values: dict[str, Any] = field(default_factory=dict)
    themes: list[str] = field(default_factory=list)
    spatial_fields: list[dict[str, Any]] = field(default_factory=list)
    extra_metadata: dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_dict(cls, payload: Mapping[str, Any]) -> "CanonicalSpatialTable":
        raw = dict(payload)
        table_id = _require_text(raw.get("table_id", raw.get("id")), "table_id")
        city = _require_text(raw.get("city", raw.get("source_city", raw.get("City"))), "city")
        table_name = _require_text(
            raw.get("table_name", raw.get("canonical_name", raw.get("name"))),
            "table_name",
        )
        semantic_summary = to_text(raw.get("semantic_summary"))
        normalized_schema = normalize_schema(
            raw.get("normalized_schema", raw.get("schema", raw.get("columns")))
        )
        representative_values = normalize_representative_values(raw.get("representative_values"))
        themes = normalize_themes(raw.get("themes", raw.get("thematic_labels")))
        spatial_fields = normalize_spatial_fields(raw.get("spatial_fields"))

        consumed = {
            "table_id",
            "id",
            "city",
            "source_city",
            "City",
            "table_name",
            "canonical_name",
            "name",
            "semantic_summary",
            "normalized_schema",
            "schema",
            "columns",
            "representative_values",
            "themes",
            "thematic_labels",
            "spatial_fields",
        }
        extra_metadata = {
            str(key): stable_jsonify(value)
            for key, value in raw.items()
            if key not in consumed
        }
        return cls(
            table_id=table_id,
            city=city,
            table_name=table_name,
            semantic_summary=semantic_summary,
            normalized_schema=normalized_schema,
            representative_values=representative_values,
            themes=themes,
            spatial_fields=spatial_fields,
            extra_metadata=extra_metadata,
        )

    def to_dict(self) -> dict[str, Any]:
        payload = {
            "table_id": self.table_id,
            "city": self.city,
            "table_name": self.table_name,
            "semantic_summary": self.semantic_summary,
            "normalized_schema": normalize_schema(self.normalized_schema),
            "representative_values": stable_jsonify(self.representative_values),
            "themes": list(self.themes),
            "spatial_fields": stable_jsonify(self.spatial_fields),
        }
        for key, value in self.extra_metadata.items():
            if key not in payload:
                payload[key] = stable_jsonify(value)
        return payload


@dataclass
class SynthesizedSpatialDatabase:
    """Synthesized multi-table spatial database metadata."""

    database_id: str
    city: str
    table_ids: list[str]
    selected_table_names: list[str]
    selected_tables: list[CanonicalSpatialTable]
    schema: list[dict[str, Any]]
    spatial_fields: list[dict[str, Any]]
    thematic_labels: list[str]
    representative_values: dict[str, Any]
    sampling_trace: list[dict[str, Any]]
    graph_stats: dict[str, Any]
    synthesize_config: dict[str, Any]

    @classmethod
    def from_dict(cls, payload: Mapping[str, Any]) -> "SynthesizedSpatialDatabase":
        raw = dict(payload)
        selected_tables = [
            CanonicalSpatialTable.from_dict(item)
            for item in raw.get("selected_tables", [])
            if isinstance(item, Mapping)
        ]
        table_ids = [to_text(item) for item in raw.get("table_ids", []) if to_text(item)]
        selected_table_names = [
            to_text(item) for item in raw.get("selected_table_names", []) if to_text(item)
        ]
        if not table_ids:
            table_ids = [table.table_id for table in selected_tables]
        if not selected_table_names:
            selected_table_names = [table.table_name for table in selected_tables]
        return cls(
            database_id=_require_text(raw.get("database_id"), "database_id"),
            city=_require_text(raw.get("city"), "city"),
            table_ids=table_ids,
            selected_table_names=selected_table_names,
            selected_tables=selected_tables,
            schema=stable_jsonify(raw.get("schema", [])),
            spatial_fields=stable_jsonify(raw.get("spatial_fields", [])),
            thematic_labels=normalize_themes(raw.get("thematic_labels", raw.get("themes"))),
            representative_values=normalize_representative_values(raw.get("representative_values")),
            sampling_trace=stable_jsonify(raw.get("sampling_trace", [])),
            graph_stats=stable_jsonify(raw.get("graph_stats", {})),
            synthesize_config=stable_jsonify(raw.get("synthesize_config", {})),
        )

    @classmethod
    def from_selected_tables(
        cls,
        *,
        database_id: str,
        city: str,
        selected_tables: list[CanonicalSpatialTable],
        sampling_trace: list[dict[str, Any]],
        graph_stats: Mapping[str, Any],
        synthesize_config: Mapping[str, Any],
    ) -> "SynthesizedSpatialDatabase":
        schema: list[dict[str, Any]] = []
        spatial_fields: list[dict[str, Any]] = []
        thematic_labels: list[str] = []
        representative_values: dict[str, Any] = {}
        for table in selected_tables:
            schema.append(
                {
                    "table_id": table.table_id,
                    "table_name": table.table_name,
                    "normalized_schema": normalize_schema(table.normalized_schema),
                }
            )
            for field_item in table.spatial_fields:
                enriched = {"table_id": table.table_id, "table_name": table.table_name}
                enriched.update(stable_jsonify(field_item))
                spatial_fields.append(enriched)
            thematic_labels.extend(table.themes)
            representative_values[table.table_id] = stable_jsonify(table.representative_values)
        return cls(
            database_id=database_id,
            city=city,
            table_ids=[table.table_id for table in selected_tables],
            selected_table_names=[table.table_name for table in selected_tables],
            selected_tables=selected_tables,
            schema=schema,
            spatial_fields=spatial_fields,
            thematic_labels=[label for label in unique_preserve_order(thematic_labels) if label],
            representative_values=representative_values,
            sampling_trace=[stable_jsonify(item) for item in sampling_trace],
            graph_stats=stable_jsonify(graph_stats),
            synthesize_config=stable_jsonify(synthesize_config),
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "database_id": self.database_id,
            "city": self.city,
            "table_ids": list(self.table_ids),
            "selected_table_names": list(self.selected_table_names),
            "selected_tables": [table.to_dict() for table in self.selected_tables],
            "schema": stable_jsonify(self.schema),
            "spatial_fields": stable_jsonify(self.spatial_fields),
            "thematic_labels": list(self.thematic_labels),
            "representative_values": stable_jsonify(self.representative_values),
            "sampling_trace": stable_jsonify(self.sampling_trace),
            "graph_stats": stable_jsonify(self.graph_stats),
            "synthesize_config": stable_jsonify(self.synthesize_config),
        }
