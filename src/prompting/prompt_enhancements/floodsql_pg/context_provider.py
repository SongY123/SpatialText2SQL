"""Load FloodSQL metadata while excluding answer-side and function-list hints."""

from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple


TABLE_LINE_PATTERN = re.compile(
    r"^(?:-\s+|table\s+)?([A-Za-z0-9_]+)\((.*)\)$",
    re.IGNORECASE,
)
SENTENCE_SPLIT_PATTERN = re.compile(r"(?<=[.!?])\s+")
SQL_FUNCTION_HINT_PATTERN = re.compile(r"\b[A-Z_]+\s*\(|\bST_[A-Za-z0-9_]+\b")
ANSWER_SIDE_KEYS = {
    "expected_columns",
    "official_elapsed",
    "official_result",
    "official_row_count",
    "output_type",
    "result",
    "row_count",
    "source_sql",
    "sql",
}


def _normalize_identifier(value: str) -> str:
    normalized = re.sub(r"[^a-z0-9]+", "_", (value or "").strip().lower())
    return normalized.strip("_")


def _clean_text(text: str) -> str:
    return " ".join((text or "").strip().split())


class FloodSQLContextProvider:
    """Build FloodSQL prompt context from the original benchmark metadata."""

    def __init__(
        self,
        project_root: Path | str,
        metadata_path: str | Path | None = None,
    ) -> None:
        self.project_root = Path(project_root).resolve()
        self.metadata_path = Path(metadata_path).expanduser() if metadata_path else None
        self._metadata_cache: Optional[Dict[str, Any]] = None

    def build_grounding(self, metadata: Dict[str, Any]) -> str:
        metadata = {
            key: value
            for key, value in (metadata or {}).items()
            if key not in ANSWER_SIDE_KEYS
        }

        lines: List[str] = [
            "- FloodSQL-Bench covers flood-management data layers for Texas, Florida, and Louisiana.",
            "- Target dialect is PostgreSQL + PostGIS; use the table and column names exactly as shown in the prompt schema.",
            "- State FIPS grounding: Texas = '48', Florida = '12', Louisiana = '22'.",
            "- GEOID values encode geography: first 2 digits are STATEFP, first 5 digits are county FIPS, and 11 digits identify census tracts.",
            "- Treat NULL as missing data rather than zero, and add null checks when the question asks for non-null values or when nullable fields affect aggregation.",
            "- City values in the hospitals and schools tables are stored in uppercase.",
            "- USPS ZIP values and Census ZCTA polygons are not identical; use ZCTA geometry when the question requires ZCTA spatial regions.",
            "- Use spatial reasoning only when the question requires spatial relationships; otherwise prefer key-based joins and ordinary filters grounded in the schema.",
        ]

        level = str(metadata.get("level") or "").strip()
        if level:
            lines.append(f"- FloodSQL difficulty tier: {level}.")

        metadata_doc = self._load_metadata()
        if metadata_doc:
            lines.extend(self._format_key_relationships(metadata_doc))
            lines.extend(self._format_spatial_layer_relationships(metadata_doc))

        return "\n".join(lines)

    def build_schema_semantics(self, compact_schema: str) -> str:
        metadata_doc = self._load_metadata()
        if not metadata_doc:
            return ""

        table_lookup = {
            _normalize_identifier(table_name): (table_name, table_info)
            for table_name, table_info in metadata_doc.items()
            if table_name != "_global" and isinstance(table_info, dict)
        }

        lines: List[str] = []
        for prompt_table, prompt_columns in self._parse_prompt_schema(compact_schema):
            table_entry = table_lookup.get(_normalize_identifier(prompt_table))
            if table_entry is None:
                continue

            _source_table, table_info = table_entry
            table_description = self._safe_table_description(prompt_table, table_info)
            if table_description:
                lines.append(f"- {prompt_table}: {table_description}")

            identifier_summary = self._identifier_summary(table_info)
            if identifier_summary:
                lines.append(f"- {prompt_table} identifiers: {identifier_summary}")

            column_lookup = self._column_lookup(table_info)
            for column_name in prompt_columns:
                column_info = column_lookup.get(_normalize_identifier(column_name))
                if column_info is None:
                    continue
                description = self._safe_column_description(
                    table_name=prompt_table,
                    column_name=column_name,
                    column_info=column_info,
                    table_info=table_info,
                )
                if description:
                    lines.append(f"- {prompt_table}.{column_name}: {description}")

        return "\n".join(lines)

    def _load_metadata(self) -> Dict[str, Any]:
        if self._metadata_cache is not None:
            return self._metadata_cache

        metadata_path = self._resolve_metadata_path()
        if metadata_path is None:
            self._metadata_cache = {}
            return self._metadata_cache

        try:
            self._metadata_cache = json.loads(metadata_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            self._metadata_cache = {}
        return self._metadata_cache

    def _resolve_metadata_path(self) -> Optional[Path]:
        candidates: List[Path] = []
        if self.metadata_path is not None:
            candidates.append(self.metadata_path)
        candidates.extend(
            [
                self.project_root / "../FloodSQL-Bench/data/metadata_parquet.json",
                self.project_root / "FloodSQL-Bench/data/metadata_parquet.json",
                self.project_root.parent / "FloodSQL-Bench/data/metadata_parquet.json",
            ]
        )

        seen: set[str] = set()
        for candidate in candidates:
            resolved = candidate.resolve()
            if str(resolved) in seen:
                continue
            seen.add(str(resolved))
            if resolved.exists():
                return resolved
        return None

    def _format_key_relationships(self, metadata_doc: Dict[str, Any]) -> List[str]:
        join_rules = metadata_doc.get("_global", {}).get("join_rules", {})
        key_based = join_rules.get("key_based", {})
        direct_pairs = self._safe_join_pairs(key_based.get("direct", []))

        lines: List[str] = []
        if direct_pairs:
            lines.append("- Key relationships: " + "; ".join(direct_pairs) + ".")

        prefix_sources = self._prefix_join_sources(key_based.get("concat", []))
        if prefix_sources:
            sources = ", ".join(prefix_sources)
            lines.append(
                "- County-level relationships: the five-digit county prefix of "
                f"{sources} corresponds to county.geoid."
            )
        return lines

    def _format_spatial_layer_relationships(self, metadata_doc: Dict[str, Any]) -> List[str]:
        join_rules = metadata_doc.get("_global", {}).get("join_rules", {})
        spatial = join_rules.get("spatial", {})

        point_layers = self._layers_from_pairs(spatial.get("point_polygon", []), side=0)
        polygon_layers = self._layers_from_pairs(spatial.get("point_polygon", []), side=1)
        polygon_layers.update(self._layers_from_pairs(spatial.get("polygon_polygon", []), side=0))
        polygon_layers.update(self._layers_from_pairs(spatial.get("polygon_polygon", []), side=1))

        lines: List[str] = []
        if point_layers and polygon_layers:
            lines.append(
                "- Spatial layer relationships: point facility layers "
                f"({', '.join(sorted(point_layers))}) can be related to polygon layers "
                f"({', '.join(sorted(polygon_layers))}) when the question asks for spatial containment or overlay."
            )
        return lines

    @staticmethod
    def _safe_join_pairs(raw_pairs: Iterable[Dict[str, Any]]) -> List[str]:
        pairs: List[str] = []
        for item in raw_pairs:
            pair = item.get("pair", []) if isinstance(item, dict) else []
            if len(pair) != 2:
                continue
            left = FloodSQLContextProvider._normalize_dotted_identifier(str(pair[0]))
            right = FloodSQLContextProvider._normalize_dotted_identifier(str(pair[1]))
            if left and right:
                pairs.append(f"{left} = {right}")
        return pairs

    @staticmethod
    def _normalize_dotted_identifier(value: str) -> str:
        parts = [
            _normalize_identifier(part)
            for part in (value or "").strip().split(".")
            if part.strip()
        ]
        if len(parts) != 2:
            return ""
        return ".".join(parts)

    @staticmethod
    def _prefix_join_sources(raw_pairs: Iterable[Dict[str, Any]]) -> List[str]:
        sources: List[str] = []
        for item in raw_pairs:
            pair = item.get("pair", []) if isinstance(item, dict) else []
            if len(pair) != 2:
                continue
            raw_left = str(pair[0])
            match = re.search(r"([A-Za-z0-9_]+)\.GEOID", raw_left, re.IGNORECASE)
            if match:
                sources.append(f"{_normalize_identifier(match.group(1))}.geoid")
        return sorted(set(sources))

    @staticmethod
    def _layers_from_pairs(raw_pairs: Iterable[Dict[str, Any]], side: int) -> set[str]:
        layers: set[str] = set()
        for item in raw_pairs:
            pair = item.get("pair", []) if isinstance(item, dict) else []
            if len(pair) != 2:
                continue
            table_match = re.search(r"([A-Za-z0-9_]+)\.", str(pair[side]))
            if table_match:
                layers.add(_normalize_identifier(table_match.group(1)))
        return layers

    def _safe_table_description(self, table_name: str, table_info: Dict[str, Any]) -> str:
        description = self._drop_function_sentences(str(table_info.get("_meta") or ""))
        layer_category = _clean_text(str(table_info.get("layer_category") or ""))
        if layer_category:
            if description:
                description += f" Layer category: {layer_category}."
            else:
                description = f"Layer category: {layer_category}."
        if table_name in {"hospitals", "schools"}:
            description = description.replace(
                "This table supports spatial joins through coordinates (LON, LAT) for point-in-polygon operations with polygon layers.",
                "This table supports spatial relationships through its point geometry and coordinate columns.",
            )
        return _clean_text(description)

    def _safe_column_description(
        self,
        table_name: str,
        column_name: str,
        column_info: Dict[str, Any],
        table_info: Dict[str, Any],
    ) -> str:
        if column_name == "geometry":
            return self._geometry_description(table_name, table_info)
        description = str(column_info.get("description") or "")
        return self._drop_function_sentences(description)

    @staticmethod
    def _geometry_description(table_name: str, table_info: Dict[str, Any]) -> str:
        layer_category = str(table_info.get("layer_category") or "").lower()
        if table_name == "claims":
            return (
                "Geometry column is available, but claims are primarily linked to "
                "other FloodSQL layers through tract-level geoid."
            )
        if "point" in layer_category:
            return (
                "Point geometry for the facility record in WGS84/OGC:CRS84; "
                "use it for spatial reasoning only when requested."
            )
        return (
            "Polygon geometry for the feature boundary in WGS84/OGC:CRS84; "
            "avoid returning geometry unless the question explicitly asks for it."
        )

    @staticmethod
    def _identifier_summary(table_info: Dict[str, Any]) -> str:
        parts: List[str] = []
        key_ids = [
            _normalize_identifier(str(item))
            for item in table_info.get("key_identifier", [])
            if str(item).strip()
        ]
        spatial_ids = [
            _normalize_identifier(str(item))
            for item in table_info.get("spatial_identifier", [])
            if str(item).strip()
        ]
        if key_ids:
            parts.append("key columns " + ", ".join(key_ids))
        if spatial_ids:
            parts.append("spatial columns " + ", ".join(spatial_ids))
        return "; ".join(parts)

    @staticmethod
    def _column_lookup(table_info: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
        lookup: Dict[str, Dict[str, Any]] = {}
        for column in table_info.get("schema", []):
            if not isinstance(column, dict):
                continue
            column_name = str(column.get("column_name") or "")
            if column_name:
                lookup[_normalize_identifier(column_name)] = column
        return lookup

    @staticmethod
    def _drop_function_sentences(text: str) -> str:
        cleaned_sentences: List[str] = []
        for sentence in SENTENCE_SPLIT_PATTERN.split(_clean_text(text)):
            if not sentence:
                continue
            if SQL_FUNCTION_HINT_PATTERN.search(sentence):
                continue
            cleaned_sentences.append(sentence)
        return _clean_text(" ".join(cleaned_sentences))

    @staticmethod
    def _parse_prompt_schema(compact_schema: str) -> List[Tuple[str, List[str]]]:
        tables: List[Tuple[str, List[str]]] = []
        for raw_line in compact_schema.splitlines():
            line = raw_line.strip()
            if not line:
                continue
            match = TABLE_LINE_PATTERN.match(line)
            if match is None:
                continue

            table_name, raw_columns = match.groups()
            columns: List[str] = []
            for raw_column in raw_columns.split(","):
                tokens = raw_column.strip().split()
                if not tokens:
                    continue
                columns.append(tokens[0].strip('"').lower())
            tables.append((table_name.lower(), columns))
        return tables
