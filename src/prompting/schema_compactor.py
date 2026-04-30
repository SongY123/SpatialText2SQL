"""Prepare prompt-ready schema text for prompt rendering."""

from __future__ import annotations

import csv
import re
import sqlite3
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple


DEFAULT_PROJECT_ROOT = Path(__file__).resolve().parents[2]
GEOMETRY_COLUMN_NAMES = {"shape", "location", "geom", "geometry"}
GEOMETRY_TYPES = {
    "GEOMETRY",
    "USER-DEFINED",
    "POINT",
    "LINESTRING",
    "POLYGON",
    "MULTIPOINT",
    "MULTILINESTRING",
    "MULTIPOLYGON",
    "POIINT",
}
IRRELEVANT_TABLE_MARKERS = (
    "geometry_columns",
    "views_geometry_columns",
    "virts_geometry_columns",
    "sqlite_stat",
    "spatialite_history",
    "spatial_ref_sys",
    "special_ref",
    "sql_statements_log",
)
RUNTIME_TYPE_PREFIXES = (
    ("character varying", "text"),
    ("double precision", "double"),
    ("timestamp with time zone", "timestamptz"),
    ("bigint", "bigint"),
    ("integer", "integer"),
    ("boolean", "boolean"),
    ("text", "text"),
    ("bytea", "bytea"),
    ("user-defined", "geometry"),
    ("geometry", "geometry"),
)
DECLARED_TYPE_MAP = {
    "TEXT": "text",
    "INTEGER": "integer",
    "DOUBLE": "double",
    "DOUBLET": "double",
    "BOOLEAN": "boolean",
}
TEXT_VALUE_TYPE_MARKERS = ("char", "text", "clob")
VALUE_HINT_EXCLUDED_COLUMNS = {
    "adcode",
    "administrative_division_code",
    "classid",
    "entityid",
    "eng_name",
    "gbcode",
    "objectid",
    "pinyin_name",
}
VALUE_HINT_PRIORITY_COLUMNS = {
    "city",
    "county",
    "district",
    "line",
    "name",
    "province",
    "region",
}
VALUE_HINT_MAX_COLUMNS_PER_TABLE = 2
VALUE_HINT_MAX_SAMPLES_PER_COLUMN = 5
VALUE_HINT_MAX_LITERAL_LENGTH = 32
QUESTION_TOKEN_PATTERN = re.compile(r"[A-Za-z0-9_]+")
GENERIC_KEEP_ALL_TABLE_THRESHOLD = 2
GENERIC_KEEP_ALL_COLUMN_THRESHOLD = 12
GENERIC_MIN_COLUMN_KEEP = 8
SPATIAL_QA_TABLE_TOP_K = {"1": 4, "2": 5, "3": 6}
FLOODSQL_TABLE_TOP_K = {"L0": 3, "L1": 4, "L2": 4, "L3": 5, "L4": 5, "L5": 5}
TABLE_ALIAS_PHRASES: Dict[str, Dict[str, Tuple[str, ...]]] = {
    "spatial_qa": {
        "poi": ("poi", "pois", "point of interest", "points of interest"),
        "blockgroups": ("block group", "block groups"),
        "counties": ("county", "counties"),
        "ghcn": ("ghcn", "station", "stations", "weather station", "weather stations"),
        "ne_protected_areas": ("protected area", "protected areas", "park", "parks"),
        "ne_time_zones": ("time zone", "time zones", "timezone", "timezones"),
        "roads": ("road", "roads", "road segment", "road segments"),
        "states": ("state", "states"),
        "tracts": ("tract", "tracts", "census tract", "census tracts"),
    },
    "floodsql_pg": {
        "claims": ("claim", "claims", "nfip claim", "nfip claims"),
        "county": ("county", "counties"),
        "census_tracts": ("tract", "tracts", "census tract", "census tracts"),
        "floodplain": ("floodplain", "flood plain", "flood zone", "flood zones"),
        "hospitals": ("hospital", "hospitals"),
        "schools": ("school", "schools"),
        "svi": ("social vulnerability", "social vulnerability index", "svi"),
        "cre": ("community resilience", "resilience", "cre"),
        "nri": ("national risk index", "risk index", "nri"),
        "zcta": ("zcta", "zip code", "zip codes", "zipcode", "zipcodes", "zip code tabulation area"),
    },
}
COLUMN_ALIAS_PHRASES: Dict[str, Tuple[str, ...]] = {
    "area": ("area",),
    "awater": ("water area",),
    "aland": ("land area",),
    "dateofloss": ("date of loss", "loss date"),
    "elev": ("elevation", "altitude"),
    "fclass": ("class", "classified", "category"),
    "geom": ("geometry", "wgs 84", "wgs84", "shape", "location"),
    "geometry": ("geometry", "wgs 84", "wgs84", "shape", "location"),
    "maxspeed": ("maximum speed", "max speed", "speed limit"),
    "name": ("name",),
    "num_claims": ("number of claims", "claim count"),
    "perimeter": ("perimeter",),
    "popu": ("population",),
    "shape_length": ("length",),
    "station_id": ("station id",),
    "time_zone": ("time zone", "timezone"),
    "unit_name": ("protected area name", "park name", "unit name"),
    "unit_type": ("type", "unit type"),
    "zone": ("time zone", "timezone", "zone"),
}
JOIN_IDENTIFIER_FRAGMENTS = (
    "id",
    "gid",
    "geoid",
    "statefp",
    "countyfp",
    "countyfips",
    "fips",
    "zip",
    "tract",
    "code",
)
LABEL_COLUMN_FRAGMENTS = (
    "name",
    "region",
    "division",
    "type",
    "category",
    "state",
    "county",
    "city",
    "province",
    "zone",
    "date",
    "year",
)


class SchemaCompactor:
    """Build a compact schema view tailored for prompting."""

    def __init__(self, project_root: str | Path | None = None):
        self.project_root = Path(project_root).resolve() if project_root else DEFAULT_PROJECT_ROOT
        self._semantic_hints_cache: Dict[str, Dict[str, Dict[str, Any]]] = {}
        self._value_hints_cache: Dict[str, Dict[str, Dict[str, List[str]]]] = {}

    def compact_schema(
        self,
        schema: str,
        question: Optional[str] = None,
        dataset_name: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> str:
        metadata = metadata or {}
        if not schema:
            return ""
        if dataset_name == "spatialsql_pg":
            return self._compact_spatialsql_schema(schema, question, metadata)
        return self._compact_generic_schema(
            schema=schema,
            question=question,
            dataset_name=dataset_name,
            metadata=metadata,
        )

    def extract_geometry_columns(self, schema: str) -> List[str]:
        compact_hints = self._extract_geometry_columns_from_compact_schema(schema)
        if compact_hints:
            return compact_hints

        geometry_columns: List[str] = []
        for table_name, columns in self._parse_runtime_schema(schema):
            for column_name, normalized_type in columns:
                if normalized_type == "geometry":
                    geometry_columns.append(f"{table_name}.{column_name}")
        return geometry_columns

    def _compact_spatialsql_schema(
        self,
        schema: str,
        question: Optional[str],
        metadata: Dict[str, Any],
    ) -> str:
        split = (metadata.get("split") or "").strip()
        if not split:
            return schema.strip()

        business_tables = self._load_split_business_tables(split)
        if business_tables:
            rendered = self._render_from_split_catalog(split, business_tables, question=question)
            if rendered:
                return rendered

            rendered = self._render_runtime_schema_subset(
                schema=schema,
                split=split,
                allowed_suffixes=business_tables,
                question=question,
            )
            if rendered:
                return rendered

        fallback = self._render_runtime_schema_subset(schema=schema, split=split, question=question)
        return fallback or schema.strip()

    def _render_from_split_catalog(
        self,
        split: str,
        business_tables: Sequence[str],
        question: Optional[str] = None,
    ) -> str:
        schema_path = self._split_domain_dir(split) / f"{self._split_domain(split)}.schema"
        parsed_schema = self._parse_domain_schema(schema_path)
        if not parsed_schema:
            return ""

        table_blocks: List[Tuple[Tuple[int, str], str]] = []
        for table_name in business_tables:
            columns = self._lookup_declared_table(parsed_schema, table_name)
            if not columns:
                continue
            full_table_name = f"{split}_{table_name}"
            table_blocks.append(
                (
                    self._table_sort_key(
                        table_name=full_table_name,
                        columns=columns,
                        question=question,
                        dataset_name="spatialsql_pg",
                        metadata={"split": split},
                    ),
                    self._format_table_block(table_name=full_table_name, columns=columns),
                )
            )
        table_blocks.sort(key=lambda item: item[0])
        return "\n".join(block for _key, block in table_blocks)

    def _compact_generic_schema(
        self,
        schema: str,
        question: Optional[str],
        dataset_name: Optional[str],
        metadata: Dict[str, Any],
    ) -> str:
        parsed_tables = [
            (table_name, columns)
            for table_name, columns in self._parse_runtime_schema(schema)
            if not self._is_irrelevant_table(table_name)
        ]
        if not parsed_tables:
            return schema.strip()

        rendered_tables: List[str] = []
        for table_name, columns in parsed_tables:
            rendered_tables.append(self._format_table_block(table_name=table_name, columns=columns))
        return "\n".join(rendered_tables)

    def _render_runtime_schema_subset(
        self,
        schema: str,
        split: str,
        allowed_suffixes: Optional[Sequence[str]] = None,
        question: Optional[str] = None,
    ) -> str:
        prefix = f"{split}_"
        allowed_lookup = {name.lower() for name in allowed_suffixes or []}
        table_blocks: List[Tuple[Tuple[int, str], str]] = []

        for table_name, columns in self._parse_runtime_schema(schema):
            if not table_name.startswith(prefix):
                continue

            suffix = table_name[len(prefix) :]
            if self._is_irrelevant_table(table_name) or self._is_irrelevant_table(suffix):
                continue
            if allowed_lookup and suffix.lower() not in allowed_lookup:
                continue
            table_blocks.append(
                (
                    self._table_sort_key(
                        table_name=table_name,
                        columns=columns,
                        question=question,
                        dataset_name="spatialsql_pg",
                        metadata={"split": split},
                    ),
                    self._format_table_block(table_name=table_name, columns=columns),
                )
            )

        table_blocks.sort(key=lambda item: item[0])
        return "\n".join(block for _key, block in table_blocks)

    def _select_relevant_tables(
        self,
        tables: Sequence[Tuple[str, List[Tuple[str, str]]]],
        question: Optional[str],
        dataset_name: Optional[str],
        metadata: Dict[str, Any],
    ) -> List[Tuple[str, List[Tuple[str, str]]]]:
        if len(tables) <= GENERIC_KEEP_ALL_TABLE_THRESHOLD:
            return list(tables)

        scored_tables: List[Tuple[int, str, Tuple[str, List[Tuple[str, str]]], bool]] = []
        for table_name, columns in tables:
            score, explicit = self._score_table_relevance(
                table_name=table_name,
                columns=columns,
                question=question,
                dataset_name=dataset_name,
                metadata=metadata,
            )
            scored_tables.append((score, table_name, (table_name, columns), explicit))

        explicit_tables = [
            table_entry
            for score, _name, table_entry, explicit in scored_tables
            if explicit
        ]
        if explicit_tables:
            selected = explicit_tables
        else:
            top_k = self._generic_table_top_k(dataset_name=dataset_name, metadata=metadata, total_tables=len(tables))
            scored_tables.sort(key=lambda item: (-item[0], item[1]))
            selected = [table_entry for _score, _name, table_entry, _explicit in scored_tables[:top_k]]

        selected.sort(
            key=lambda item: self._table_sort_key(
                table_name=item[0],
                columns=item[1],
                question=question,
                dataset_name=dataset_name,
                metadata=metadata,
            )
        )
        return selected

    def _select_relevant_columns(
        self,
        table_name: str,
        columns: Sequence[Tuple[str, str]],
        question: Optional[str],
        dataset_name: Optional[str],
        metadata: Dict[str, Any],
    ) -> List[Tuple[str, str]]:
        column_list = list(columns)
        if len(column_list) <= GENERIC_KEEP_ALL_COLUMN_THRESHOLD:
            return column_list

        scored_columns: List[Tuple[int, int, Tuple[str, str]]] = []
        for index, (column_name, column_type) in enumerate(column_list):
            score = self._score_column_relevance(
                table_name=table_name,
                column_name=column_name,
                column_type=column_type,
                question=question,
                dataset_name=dataset_name,
                metadata=metadata,
            )
            scored_columns.append((score, index, (column_name, column_type)))

        selected_indices = {
            index
            for score, index, _column in scored_columns
            if score >= 40
        }
        if not selected_indices:
            selected_indices = {
                index
                for _score, index, _column in sorted(scored_columns, key=lambda item: (-item[0], item[1]))[
                    : min(GENERIC_MIN_COLUMN_KEEP, len(scored_columns))
                ]
            }

        if len(selected_indices) < min(GENERIC_MIN_COLUMN_KEEP, len(scored_columns)):
            for _score, index, _column in sorted(scored_columns, key=lambda item: (-item[0], item[1])):
                selected_indices.add(index)
                if len(selected_indices) >= min(GENERIC_MIN_COLUMN_KEEP, len(scored_columns)):
                    break

        return [
            column
            for index, column in enumerate(column_list)
            if index in selected_indices
        ]

    def _score_table_relevance(
        self,
        table_name: str,
        columns: Sequence[Tuple[str, str]],
        question: Optional[str],
        dataset_name: Optional[str],
        metadata: Dict[str, Any],
    ) -> Tuple[int, bool]:
        question_text = self._normalize_text(question)
        question_tokens = self._tokenize_question(question)
        normalized_table = self._normalize_identifier_key(table_name)
        table_tokens = self._identifier_tokens(table_name)

        score = 0
        explicit = False
        for phrase in self._table_alias_phrases(dataset_name, table_name):
            if phrase and phrase in question_text:
                score += 10
                explicit = True

        score += 6 * len(question_tokens & table_tokens)
        if normalized_table in question_tokens:
            score += 6

        top_column_boosts = sorted(
            (
                min(
                    4,
                    self._score_column_relevance(
                        table_name=table_name,
                        column_name=column_name,
                        column_type=column_type,
                        question=question,
                        dataset_name=dataset_name,
                        metadata=metadata,
                    )
                    // 20,
                )
                for column_name, column_type in columns
            ),
            reverse=True,
        )[:3]
        score += sum(top_column_boosts)
        if self._table_contains_expected_columns(columns, metadata):
            score += 8

        return score, explicit

    def _score_column_relevance(
        self,
        table_name: str,
        column_name: str,
        column_type: str,
        question: Optional[str],
        dataset_name: Optional[str],
        metadata: Dict[str, Any],
    ) -> int:
        del table_name, dataset_name
        question_text = self._normalize_text(question)
        question_tokens = self._tokenize_question(question)
        normalized_column = self._normalize_identifier_key(column_name)
        column_tokens = self._identifier_tokens(column_name)
        expected_columns = {
            self._normalize_identifier_key(str(item))
            for item in (metadata.get("expected_columns") or [])
        }

        score = 0
        if column_type == "geometry":
            score += 80
        if normalized_column in expected_columns:
            score += 90
        if self._is_join_identifier_column(normalized_column):
            score += 45
        if self._is_label_column(normalized_column):
            score += 40

        score += 14 * len(question_tokens & column_tokens)
        for phrase in self._column_alias_phrases(column_name):
            if phrase and phrase in question_text:
                score += 24
        return score

    @staticmethod
    def _normalize_text(value: Optional[str]) -> str:
        return " ".join((value or "").strip().lower().split())

    @staticmethod
    def _singularize_token(token: str) -> str:
        if token.endswith("ies") and len(token) > 4:
            return token[:-3] + "y"
        if token.endswith("s") and len(token) > 3 and not token.endswith("ss"):
            return token[:-1]
        return token

    def _tokenize_question(self, question: Optional[str]) -> set[str]:
        tokens = set()
        for raw_token in QUESTION_TOKEN_PATTERN.findall((question or "").lower()):
            token = raw_token.strip("_")
            if not token:
                continue
            tokens.add(token)
            singular = self._singularize_token(token)
            if singular:
                tokens.add(singular)
        return tokens

    def _identifier_tokens(self, identifier: str) -> set[str]:
        spaced = re.sub(r"([a-z0-9])([A-Z])", r"\1 \2", identifier or "")
        raw_tokens = QUESTION_TOKEN_PATTERN.findall(spaced.replace("_", " "))
        tokens = set()
        for raw_token in raw_tokens:
            token = raw_token.lower().strip("_")
            if not token:
                continue
            tokens.add(token)
            singular = self._singularize_token(token)
            if singular:
                tokens.add(singular)
        return tokens

    def _table_alias_phrases(self, dataset_name: Optional[str], table_name: str) -> List[str]:
        normalized_table = self._normalize_identifier_key(table_name)
        dataset_aliases = TABLE_ALIAS_PHRASES.get(dataset_name or "", {})
        derived_phrases = {
            " ".join(
                token
                for token in re.sub(r"([a-z0-9])([A-Z])", r"\1 \2", table_name or "").replace("_", " ").lower().split()
            ).strip()
        }
        derived_phrases.update(dataset_aliases.get(normalized_table, ()))
        return [phrase for phrase in sorted(derived_phrases) if phrase]

    def _column_alias_phrases(self, column_name: str) -> Tuple[str, ...]:
        normalized_column = self._normalize_identifier_key(column_name)
        return COLUMN_ALIAS_PHRASES.get(normalized_column, ())

    @staticmethod
    def _is_join_identifier_column(normalized_column: str) -> bool:
        return any(fragment in normalized_column for fragment in JOIN_IDENTIFIER_FRAGMENTS)

    @staticmethod
    def _is_label_column(normalized_column: str) -> bool:
        return any(fragment in normalized_column for fragment in LABEL_COLUMN_FRAGMENTS)

    def _table_contains_expected_columns(
        self,
        columns: Sequence[Tuple[str, str]],
        metadata: Dict[str, Any],
    ) -> bool:
        expected_columns = {
            self._normalize_identifier_key(str(item))
            for item in (metadata.get("expected_columns") or [])
        }
        if not expected_columns:
            return False
        column_names = {
            self._normalize_identifier_key(column_name)
            for column_name, _column_type in columns
        }
        return bool(expected_columns & column_names)

    def _generic_table_top_k(
        self,
        dataset_name: Optional[str],
        metadata: Dict[str, Any],
        total_tables: int,
    ) -> int:
        if dataset_name == "spatial_qa":
            level = str(metadata.get("level") or "").strip()
            return min(total_tables, SPATIAL_QA_TABLE_TOP_K.get(level, 5))
        if dataset_name == "floodsql_pg":
            level = str(metadata.get("level") or "").strip()
            return min(total_tables, FLOODSQL_TABLE_TOP_K.get(level, 5))
        return min(total_tables, 5)

    def _table_sort_key(
        self,
        table_name: str,
        columns: Sequence[Tuple[str, str]],
        question: Optional[str],
        dataset_name: Optional[str],
        metadata: Dict[str, Any],
    ) -> Tuple[int, str]:
        score, _explicit = self._score_table_relevance(
            table_name=table_name,
            columns=columns,
            question=question,
            dataset_name=dataset_name,
            metadata=metadata,
        )
        return (-score, table_name.lower())

    def _parse_runtime_schema(self, schema: str) -> List[Tuple[str, List[Tuple[str, str]]]]:
        tables: List[Tuple[str, List[Tuple[str, str]]]] = []
        for table_name, body in self._parse_create_table_blocks(schema):
            columns: List[Tuple[str, str]] = []
            for raw_line in body.splitlines():
                line = raw_line.strip().rstrip(",")
                if not line or line.upper().startswith(("PRIMARY KEY", "FOREIGN KEY", "UNIQUE", "CONSTRAINT")):
                    continue
                match = re.match(r'"?([A-Za-z0-9_]+)"?\s+(.+)', line)
                if not match:
                    continue
                column_name, remainder = match.groups()
                normalized_type = self._normalize_runtime_type(column_name, remainder)
                columns.append((column_name.lower(), normalized_type))
            if columns:
                tables.append((table_name, columns))
        return tables

    def _parse_domain_schema(self, schema_path: Path) -> Dict[str, List[Tuple[str, str]]]:
        if not schema_path.exists():
            return {}

        parsed: Dict[str, List[Tuple[str, str]]] = {}
        text = schema_path.read_text(encoding="utf-8")
        for table_name, body in self._parse_create_table_blocks(text):
            columns: List[Tuple[str, str]] = []
            for raw_line in body.splitlines():
                line = raw_line.strip().rstrip(",")
                if not line or line.upper().startswith(("PRIMARY KEY", "FOREIGN KEY", "UNIQUE", "CONSTRAINT")):
                    continue
                parts = line.split()
                if len(parts) < 2:
                    continue
                column_name = parts[0].strip('"').lower()
                declared_type = parts[1].upper()
                columns.append((column_name, self._normalize_declared_type(column_name, declared_type)))
            if columns:
                parsed[table_name.lower()] = columns
        return parsed

    @staticmethod
    def _parse_create_table_blocks(text: str) -> List[Tuple[str, str]]:
        blocks: List[Tuple[str, str]] = []
        current_name: Optional[str] = None
        current_lines: List[str] = []
        depth = 0

        for raw_line in text.splitlines():
            line = raw_line.rstrip()
            if current_name is None:
                match = re.match(r'\s*CREATE TABLE\s+"?([^"\s(]+)"?\s*\((.*)$', line, re.I)
                if not match:
                    continue
                current_name = match.group(1)
                remainder = match.group(2)
                current_lines = [remainder]
                depth = 1 + remainder.count("(") - remainder.count(")")
                if depth <= 0:
                    blocks.append((current_name, SchemaCompactor._clean_table_body("\n".join(current_lines))))
                    current_name = None
                    current_lines = []
                    depth = 0
                continue

            current_lines.append(line)
            depth += line.count("(") - line.count(")")
            if depth <= 0:
                blocks.append((current_name, SchemaCompactor._clean_table_body("\n".join(current_lines))))
                current_name = None
                current_lines = []
                depth = 0

        return blocks

    @staticmethod
    def _clean_table_body(body: str) -> str:
        cleaned = body.strip()
        if cleaned.endswith(");"):
            cleaned = cleaned[:-2]
        elif cleaned.endswith(")"):
            cleaned = cleaned[:-1]
        return cleaned.strip()

    def _load_split_business_tables(self, split: str) -> List[str]:
        domain = self._split_domain(split)
        table_catalog_path = self._split_domain_dir(split) / f"{domain}.table.csv"
        if not table_catalog_path.exists():
            return []

        business_tables: List[str] = []
        with open(table_catalog_path, "r", encoding="utf-8") as handle:
            reader = csv.DictReader(handle)
            for row in reader:
                table_name = (row.get("name") or "").strip()
                if table_name:
                    business_tables.append(table_name)
        return business_tables

    @staticmethod
    def _lookup_declared_table(
        parsed_schema: Dict[str, List[Tuple[str, str]]],
        table_name: str,
    ) -> List[Tuple[str, str]]:
        return parsed_schema.get(table_name.lower(), [])

    def _load_split_semantic_hints(self, split: str) -> Dict[str, Dict[str, Any]]:
        cached = self._semantic_hints_cache.get(split)
        if cached is not None:
            return cached

        hints: Dict[str, Dict[str, Any]] = {}
        domain_dir = self._split_domain_dir(split)
        domain = self._split_domain(split)

        info_path = domain_dir / f"{domain}_EN_Column_Seq.info"
        if info_path.exists():
            self._merge_info_semantic_hints(info_path, hints)

        description_dir = domain_dir / "database_description"
        if description_dir.exists():
            for csv_path in sorted(description_dir.glob("*.csv")):
                self._merge_database_description_hints(csv_path, hints)

        self._semantic_hints_cache[split] = hints
        return hints

    def _load_split_value_hints(
        self,
        split: str,
        semantic_hints: Dict[str, Dict[str, Any]],
    ) -> Dict[str, Dict[str, List[str]]]:
        cached = self._value_hints_cache.get(split)
        if cached is not None:
            return cached

        domain_dir = self._split_domain_dir(split)
        db_path = domain_dir / f"{self._split_domain(split)}.sqlite"
        if not db_path.exists():
            self._value_hints_cache[split] = {}
            return {}

        hints: Dict[str, Dict[str, List[str]]] = {}
        allowed_tables = {
            self._normalize_identifier_key(table_name)
            for table_name in self._load_split_business_tables(split)
        }
        try:
            with sqlite3.connect(db_path) as connection:
                table_names = [
                    row[0]
                    for row in connection.execute(
                        "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
                    )
                ]
                for table_name in table_names:
                    normalized_table = self._normalize_identifier_key(table_name)
                    if allowed_tables and normalized_table not in allowed_tables:
                        continue
                    if self._is_irrelevant_table(table_name):
                        continue
                    try:
                        table_hints = self._collect_table_value_hints(
                            connection=connection,
                            table_name=table_name,
                            semantic_entry=semantic_hints.get(normalized_table, {}),
                        )
                    except sqlite3.Error:
                        continue
                    if table_hints:
                        hints[normalized_table] = table_hints
        except sqlite3.Error:
            hints = {}

        self._value_hints_cache[split] = hints
        return hints

    def _merge_info_semantic_hints(
        self,
        info_path: Path,
        hints: Dict[str, Dict[str, Any]],
    ) -> None:
        for raw_line in info_path.read_text(encoding="utf-8").splitlines():
            line = " ".join(raw_line.strip().split())
            if not line.startswith('The table "'):
                continue

            table_match = re.match(r'^The table "([^"]+)"\s+(.*)$', line)
            if not table_match:
                continue

            table_name = table_match.group(1)
            segments = [segment.strip() for segment in line.split(";") if segment.strip()]
            entry = self._get_or_create_semantic_entry(hints, table_name)

            table_segment = segments[0]
            table_desc_match = re.match(r'^The table "([^"]+)"\s+(.*)$', table_segment)
            if table_desc_match and not entry.get("table_description"):
                entry["table_description"] = self._clean_semantic_text(table_desc_match.group(2))

            for segment in segments[1:]:
                field_match = re.match(r'^The field "([^"]+)"\s+(.*)$', segment)
                if not field_match:
                    continue
                column_name, description = field_match.groups()
                normalized_column = self._normalize_identifier_key(column_name)
                if not normalized_column:
                    continue
                entry["column_descriptions"].setdefault(
                    normalized_column,
                    self._clean_semantic_text(description),
                )

    def _merge_database_description_hints(
        self,
        csv_path: Path,
        hints: Dict[str, Dict[str, Any]],
    ) -> None:
        entry = self._get_or_create_semantic_entry(hints, csv_path.stem)
        with open(csv_path, "r", encoding="utf-8") as handle:
            reader = csv.DictReader(handle)
            for row in reader:
                description = (
                    (row.get("column_description") or "").strip()
                    or (row.get("column_description_CN") or "").strip()
                )
                if not description:
                    continue
                cleaned_description = self._clean_semantic_text(description)
                for candidate_name in (row.get("original_column_name"), row.get("column_name")):
                    normalized_column = self._normalize_identifier_key(candidate_name or "")
                    if not normalized_column:
                        continue
                    entry["column_descriptions"][normalized_column] = cleaned_description

    @staticmethod
    def _get_or_create_semantic_entry(
        hints: Dict[str, Dict[str, Any]],
        table_name: str,
    ) -> Dict[str, Any]:
        key = SchemaCompactor._normalize_identifier_key(table_name)
        return hints.setdefault(
            key,
            {
                "table_description": "",
                "column_descriptions": {},
            },
        )

    def _split_domain_dir(self, split: str) -> Path:
        version = split.split("_", 1)[0]
        return self.project_root / "sdbdatasets" / version / self._split_domain(split)

    @staticmethod
    def _split_domain(split: str) -> str:
        parts = split.split("_", 1)
        return parts[1] if len(parts) == 2 else ""

    @staticmethod
    def _normalize_runtime_type(column_name: str, raw_type: str) -> str:
        lowered = raw_type.lower()
        if column_name.lower() in GEOMETRY_COLUMN_NAMES:
            return "geometry"
        for prefix, normalized in RUNTIME_TYPE_PREFIXES:
            if lowered.startswith(prefix):
                return normalized
        return raw_type.split()[0].lower()

    @staticmethod
    def _normalize_declared_type(column_name: str, declared_type: str) -> str:
        if column_name.lower() in GEOMETRY_COLUMN_NAMES or declared_type in GEOMETRY_TYPES:
            return "geometry"
        return DECLARED_TYPE_MAP.get(declared_type, declared_type.lower())

    def _format_table_block(
        self,
        table_name: str,
        columns: Iterable[Tuple[str, str]],
    ) -> str:
        return self._format_table_line(table_name, list(columns))

    @staticmethod
    def _format_table_line(table_name: str, columns: Iterable[Tuple[str, str]]) -> str:
        rendered_columns = [f"{column_name} {column_type}" for column_name, column_type in columns]
        return f"- {table_name}({', '.join(rendered_columns)})"

    def _format_semantic_hints(
        self,
        table_suffix: str,
        columns: Sequence[Tuple[str, str]],
        semantic_hints: Dict[str, Dict[str, Any]],
    ) -> str:
        entry = semantic_hints.get(self._normalize_identifier_key(table_suffix), {})
        if not entry:
            return ""

        parts: List[str] = []
        table_description = self._clean_semantic_text(entry.get("table_description", ""))
        if table_description:
            parts.append(f"table = {table_description}")

        column_descriptions = entry.get("column_descriptions", {})
        for column_name, _column_type in columns:
            description = column_descriptions.get(self._normalize_identifier_key(column_name))
            if description:
                parts.append(f"{column_name} = {self._clean_semantic_text(description)}")

        return "; ".join(parts)

    def _format_value_hints(
        self,
        table_suffix: str,
        columns: Sequence[Tuple[str, str]],
        value_hints: Dict[str, Dict[str, List[str]]],
    ) -> str:
        entry = value_hints.get(self._normalize_identifier_key(table_suffix), {})
        if not entry:
            return ""

        parts: List[str] = []
        for column_name, _column_type in columns:
            samples = entry.get(self._normalize_identifier_key(column_name))
            if not samples:
                continue
            rendered_samples = " | ".join(f"'{sample}'" for sample in samples)
            parts.append(f"{column_name} = {rendered_samples}")

        return "; ".join(parts)

    @staticmethod
    def _is_irrelevant_table(table_name: str) -> bool:
        lowered = table_name.lower()
        return any(marker in lowered for marker in IRRELEVANT_TABLE_MARKERS)

    @staticmethod
    def _normalize_identifier_key(value: str) -> str:
        normalized = re.sub(r"[^a-z0-9]+", "_", (value or "").strip().lower())
        return normalized.strip("_")

    @staticmethod
    def _clean_semantic_text(text: str) -> str:
        cleaned = " ".join((text or "").strip().split())
        cleaned = re.sub(r'^The field "[^"]+"\s+', "", cleaned)
        cleaned = re.sub(r'^The table "[^"]+"\s+', "", cleaned)
        return cleaned.rstrip(" .;")

    def _collect_table_value_hints(
        self,
        connection: sqlite3.Connection,
        table_name: str,
        semantic_entry: Dict[str, Any],
    ) -> Dict[str, List[str]]:
        column_descriptions = semantic_entry.get("column_descriptions", {})
        candidates: List[Tuple[int, str, str]] = []

        quoted_table = self._quote_sqlite_identifier(table_name)
        for _cid, column_name, declared_type, _notnull, _default, _pk in connection.execute(
            f"PRAGMA table_info({quoted_table})"
        ):
            normalized_column = self._normalize_identifier_key(column_name)
            score = self._score_value_hint_column(
                column_name=column_name,
                declared_type=declared_type,
                description=column_descriptions.get(normalized_column, ""),
            )
            if score <= 0:
                continue
            candidates.append((score, normalized_column, column_name))

        candidates.sort(key=lambda item: (-item[0], item[1]))
        collected: Dict[str, List[str]] = {}
        for _score, normalized_column, column_name in candidates[:VALUE_HINT_MAX_COLUMNS_PER_TABLE]:
            samples = self._sample_distinct_text_values(connection, table_name, column_name)
            if samples:
                collected[normalized_column] = samples
        return collected

    def _score_value_hint_column(
        self,
        column_name: str,
        declared_type: str,
        description: str,
    ) -> int:
        normalized_column = self._normalize_identifier_key(column_name)
        if not normalized_column:
            return 0
        if normalized_column in VALUE_HINT_EXCLUDED_COLUMNS:
            return 0
        if normalized_column.startswith(("eng_", "english_", "pinyin_")):
            return 0
        if normalized_column.endswith("_id") or normalized_column.endswith("code"):
            return 0
        if not self._is_text_value_column(declared_type):
            return 0

        score = 0
        lowered_description = (description or "").lower()
        if normalized_column in VALUE_HINT_PRIORITY_COLUMNS:
            score += 4
        if normalized_column.endswith("_name"):
            score += 4
        if "name" in lowered_description:
            score += 3
        if any(token in lowered_description for token in ("province", "city", "district", "county", "region")):
            score += 2
        return score

    @staticmethod
    def _is_text_value_column(declared_type: str) -> bool:
        lowered = (declared_type or "").strip().lower()
        return any(marker in lowered for marker in TEXT_VALUE_TYPE_MARKERS)

    def _sample_distinct_text_values(
        self,
        connection: sqlite3.Connection,
        table_name: str,
        column_name: str,
    ) -> List[str]:
        quoted_table = self._quote_sqlite_identifier(table_name)
        quoted_column = self._quote_sqlite_identifier(column_name)
        query = (
            f"SELECT {quoted_column}, COUNT(*) AS freq "
            f"FROM {quoted_table} "
            f"WHERE {quoted_column} IS NOT NULL AND TRIM(CAST({quoted_column} AS TEXT)) <> '' "
            f"GROUP BY {quoted_column} "
            f"ORDER BY freq DESC, LENGTH(CAST({quoted_column} AS TEXT)) ASC, CAST({quoted_column} AS TEXT) ASC "
            f"LIMIT ?"
        )

        values: List[str] = []
        for raw_value, _freq in connection.execute(
            query,
            (VALUE_HINT_MAX_SAMPLES_PER_COLUMN * 3,),
        ):
            cleaned_value = self._clean_value_hint_literal(raw_value)
            if not cleaned_value:
                continue
            values.append(cleaned_value)
            if len(values) >= VALUE_HINT_MAX_SAMPLES_PER_COLUMN:
                break
        return values

    @staticmethod
    def _quote_sqlite_identifier(identifier: str) -> str:
        escaped = (identifier or "").replace('"', '""')
        return f'"{escaped}"'

    @staticmethod
    def _clean_value_hint_literal(raw_value: Any) -> str:
        cleaned = " ".join(str(raw_value or "").strip().split())
        if not cleaned:
            return ""
        if len(cleaned) > VALUE_HINT_MAX_LITERAL_LENGTH:
            return ""
        if re.fullmatch(r"[A-Za-z0-9_.-]+", cleaned):
            return ""
        return cleaned.replace("'", "''")

    @staticmethod
    def _extract_geometry_columns_from_compact_schema(schema: str) -> List[str]:
        geometry_columns: List[str] = []
        for line in schema.splitlines():
            stripped = line.strip()
            if not stripped.startswith("- ") or "(" not in stripped or ")" not in stripped:
                continue
            table_name = stripped[2:].split("(", 1)[0].strip()
            raw_columns = stripped.split("(", 1)[1].rsplit(")", 1)[0]
            for raw_column in raw_columns.split(","):
                column_tokens = raw_column.strip().split()
                if len(column_tokens) < 2:
                    continue
                column_name, column_type = column_tokens[0], column_tokens[1]
                if column_type == "geometry":
                    geometry_columns.append(f"{table_name}.{column_name}")
        return geometry_columns
