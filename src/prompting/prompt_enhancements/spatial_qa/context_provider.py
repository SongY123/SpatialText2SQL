"""Build level-aware grounding hints for the Spatial QA prompt enhancement."""

from __future__ import annotations

import re
from typing import Any, Dict, List


COMMON_HINTS = (
    "Spatial QA runs on PostgreSQL/PostGIS; use only tables, columns, and literals grounded by the schema, sample data, and question.",
    "Use spatial SQL only when the question requires spatial reasoning; otherwise keep the query as standard SQL.",
    "Return only the columns or aggregate values needed to answer the question, and avoid extra diagnostic fields unless requested.",
)

LEVEL_PROFILES = {
    "1": {
        "focus": "Single-table lookups or one direct spatial relation.",
        "hints": [
            "Favor the simplest executable query shape that satisfies the question; avoid unnecessary nesting.",
            "Prefer direct filters on one target table and return only the columns needed by the question.",
        ],
    },
    "2": {
        "focus": "Focused joins or filtering across a small number of relevant tables.",
        "hints": [
            "Resolve named entities through schema-grounded filters before joining them with target records.",
            "Keep join and filtering logic aligned with the wording instead of adding broader conditions than requested.",
        ],
    },
    "3": {
        "focus": "Multi-step reasoning with joins, aggregation, grouping, or quantitative outputs.",
        "hints": [
            "Respect whether the requested result is per entity, a ranked list, an average, a count, or one overall total.",
            "When multiple intermediate steps are required, keep each step grounded in the schema and final question.",
            "Make aggregation logic explicit and consistent with the selected output columns.",
        ],
    },
}

TABLE_SEMANTICS = {
    "blockgroups": "U.S. Census block group boundaries with polygon geometries.",
    "counties": "U.S. Census county boundaries with polygon geometries.",
    "ghcn": "Global Historical Climatology Network weather stations with point geometries.",
    "ne_protected_areas": "Natural Earth protected areas with worldwide polygon geometries.",
    "ne_time_zones": "Natural Earth time zone regions with worldwide polygon geometries.",
    "poi": "OpenStreetMap points of interest in Pennsylvania with point geometries.",
    "roads": "OpenStreetMap road segments in Pennsylvania with line geometries.",
    "states": "U.S. Census state boundaries with polygon geometries.",
    "tracts": "U.S. Census tract boundaries with polygon geometries.",
}

GENERIC_COLUMN_SEMANTICS = {
    "aland": "land area attribute for the geography record.",
    "awater": "water area attribute for the geography record.",
    "code": "source-specific feature code.",
    "county": "county identifier or name carried with the record.",
    "date": "observation date field.",
    "division": "U.S. Census division label.",
    "element": "weather observation variable code.",
    "elev": "station elevation attribute.",
    "fclass": "OpenStreetMap feature class or category label.",
    "featurecla": "Natural Earth feature class label.",
    "geom": "geometry column used for spatial predicates, joins, and measurements.",
    "geoid": "Census geographic identifier code.",
    "iso_8601": "ISO 8601 UTC offset string.",
    "lat": "latitude attribute for the point record.",
    "lon": "longitude attribute for the point record.",
    "maxspeed": "road speed limit attribute.",
    "name": "human-readable entity name.",
    "nps_region": "National Park Service region label.",
    "objectid": "source object identifier.",
    "oneway": "one-way traffic flag.",
    "osm_id": "OpenStreetMap feature identifier.",
    "places": "place names associated with the time zone polygon.",
    "ref": "route or road reference identifier.",
    "region": "U.S. Census region label.",
    "state": "state identifier carried with the record.",
    "statefp": "state FIPS code.",
    "station_id": "weather station identifier.",
    "stusps": "two-letter state abbreviation.",
    "time_zone": "human-readable time zone name.",
    "unit_code": "protected area unit code.",
    "unit_name": "protected area name.",
    "unit_type": "protected area type or category.",
    "utc_format": "UTC offset text representation.",
    "value": "recorded measurement value.",
    "zone": "time zone offset or zone number.",
}

TABLE_LINE_PATTERN = re.compile(
    r"^(?:-\s+|table\s+)?([A-Za-z0-9_]+)\((.*)\)$",
    re.IGNORECASE,
)
MAX_COLUMN_HINTS_PER_TABLE = 4


class SpatialQAContextProvider:
    """Return lightweight level-aware prompt hints for `spatial_qa`."""

    def get_context(self, metadata: Dict[str, Any] | None) -> Dict[str, Any]:
        metadata = metadata or {}
        level = str(metadata.get("level") or "").strip()
        profile = LEVEL_PROFILES.get(level, {})

        hints: List[str] = list(COMMON_HINTS)
        hints.extend(profile.get("hints", []))
        return {
            "level": level,
            "focus": profile.get("focus", ""),
            "hints": hints,
        }

    def build_schema_semantics(self, compact_schema: str) -> str:
        lines: List[str] = []
        for table_name, columns in self._parse_prompt_schema(compact_schema):
            table_description = TABLE_SEMANTICS.get(table_name)
            if table_description:
                lines.append(f"- {table_name}: {table_description}")

            added = 0
            for column_name in columns:
                description = GENERIC_COLUMN_SEMANTICS.get(column_name)
                if not description:
                    continue
                lines.append(f"- {table_name}.{column_name}: {description}")
                added += 1
                if added >= MAX_COLUMN_HINTS_PER_TABLE:
                    break

        return "\n".join(lines)

    @staticmethod
    def _parse_prompt_schema(compact_schema: str) -> List[tuple[str, List[str]]]:
        tables: List[tuple[str, List[str]]] = []
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
