"""Prompt builder backed by a standalone template file."""

from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import yaml

from src.datasets.names import canonicalize_dataset_name

from .prompt_enhancements.registry import PromptEnhancementRegistry
from .schema_compactor import DEFAULT_PROJECT_ROOT, SchemaCompactor
from .sample_data_provider import PostgresSampleDataProvider


class PromptBuilder:
    """Build prompts from a shared text template plus structured context."""

    def __init__(self, config: Dict):
        self.config = config
        project_root = config.get("project_root")
        self.project_root = Path(project_root).resolve() if project_root else DEFAULT_PROJECT_ROOT
        dataset_config_path = config.get("dataset_config_path")
        self.dataset_config_path = (
            Path(dataset_config_path).resolve()
            if dataset_config_path
            else self.project_root / "config" / "dataset_config.yaml"
        )
        template_path = config.get("prompt_template_path")
        self.template_path = (
            Path(template_path).resolve()
            if template_path
            else self.project_root / "prompts" / "text2sql_prompt.txt"
        )
        self.ablation_configs = config.get("ablation_configs", {})
        self.prompt_styles = config.get("prompt_styles", {})
        self._template_cache: Dict[str, str] = {}
        self.dataset_config = self._load_dataset_config()
        self.schema_compactor = SchemaCompactor(project_root=self.project_root)
        self.sample_data_provider = config.get("sample_data_provider") or PostgresSampleDataProvider(
            project_root=self.project_root,
            dataset_config_path=config.get("dataset_config_path"),
        )
        self.prompt_enhancement_registry = (
                config.get("prompt_enhancement_registry")
                or PromptEnhancementRegistry(self.project_root)
        )
        self.named_prompt_templates = {
            "sql_synthesis": self.project_root / "prompts" / "sql_synthesis_prompt.txt",
            "sql_revision": self.project_root / "prompts" / "sql_revision_prompt.txt",
            "question_generation": self.project_root / "prompts" / "question_generation_prompt.txt",
            "question_revision": self.project_root / "prompts" / "question_revision_prompt.txt",
            "quality_control": self.project_root / "prompts" / "quality_control_prompt.txt",
        }

    def build_prompt(
            self,
            question: str,
            schema: str,
            config_type: str = 'base',
            rag_context: Optional[str] = None,
            keyword_context: Optional[str] = None,
            dataset_name: Optional[str] = None,
            metadata: Optional[Dict[str, Any]] = None,
            data_item: Optional[Dict[str, Any]] = None,
    ) -> str:
        metadata = metadata or {}
        data_item = data_item or {}
        dataset_name = canonicalize_dataset_name(dataset_name)
        config_spec = self._resolve_ablation_config(config_type)
        prompt_style = str(config_spec.get("prompt_style") or "default")
        style_spec = self._resolve_prompt_style(prompt_style, dataset_name)
        template_text = self._load_template_text(prompt_style, style_spec)
        compact_schema = self.schema_compactor.compact_schema(
            schema=schema,
            question=question,
            dataset_name=dataset_name,
            metadata=metadata,
        )
        sample_data_block = ""
        if style_spec.get("include_sample_data", True):
            sample_data_block = self.sample_data_provider.build_sample_data(
                dataset_name=dataset_name or "",
                metadata=metadata,
                compact_schema=compact_schema,
            )
        custom_sections_block = self._build_custom_sections_block(dataset_name, data_item)
        if prompt_style == "finetune_alpaca":
            structured_schema_block, foreign_key_lines = self._build_structured_schema_context(
                schema=schema,
                compact_schema=compact_schema,
                sample_data_block=sample_data_block,
                metadata=metadata,
            )
            database_id = self._resolve_database_display_id(
                dataset_name=dataset_name,
                data_item=data_item,
                metadata=metadata,
            )
            return self._build_finetune_alpaca_prompt(
                template_text=template_text,
                question=question,
                database_id=database_id,
                schema_block=structured_schema_block,
                foreign_key_lines=foreign_key_lines,
                custom_sections_block=custom_sections_block,
            )
        grounding_block = self._build_grounding_block(
            dataset_name=dataset_name,
            metadata=metadata,
            style_spec=style_spec,
        )
        schema_semantics_block = self._build_schema_semantics_block(
            dataset_name=dataset_name,
            metadata=metadata,
            style_spec=style_spec,
            compact_schema=compact_schema,
        )
        placeholders = {
            "schema_block": compact_schema.strip(),
            "schema_semantics_block": schema_semantics_block.strip(),
            "sample_data_block": sample_data_block.strip(),
            "content_information_block": sample_data_block.strip(),
            "rag_block": self._stringify_value(
                rag_context if config_spec.get("use_rag") else None,
            ),
            "keyword_block": self._stringify_value(
                keyword_context if config_spec.get("use_keyword") else None,
            ),
            "grounding_block": grounding_block.strip(),
            "question_block": (question or "").strip(),
            "custom_sections_block": custom_sections_block,
        }
        return self._render_template(template_text, placeholders)

    def _build_finetune_alpaca_prompt(
            self,
            *,
            template_text: str,
            question: str,
            database_id: str,
            schema_block: str,
            foreign_key_lines: List[str],
            custom_sections_block: str,
    ) -> str:
        return self._render_template(
            template_text,
            {
                "database_id": self._stringify_value(database_id),
                "schema_block": schema_block,
                "foreign_keys_block": self._build_foreign_keys_block(foreign_key_lines),
                "question_block": self._stringify_value((question or "").strip()),
                "custom_sections_block": custom_sections_block,
            },
        )

    def _load_dataset_config(self) -> Dict[str, Any]:
        if not self.dataset_config_path.exists():
            return {}
        with open(self.dataset_config_path, "r", encoding="utf-8") as handle:
            payload = yaml.safe_load(handle) or {}
        return payload if isinstance(payload, dict) else {}

    def _build_structured_schema_context(
            self,
            *,
            schema: str,
            compact_schema: str,
            sample_data_block: str,
            metadata: Dict[str, Any],
    ) -> Tuple[str, List[str]]:
        schema_text = (schema or compact_schema or "").strip()
        geometry_hints = self._build_finetune_geometry_hints(
            schema=schema,
            compact_schema=compact_schema,
            metadata=metadata,
        )
        representative_values = self._parse_sample_data_block(sample_data_block)
        schema_lines = self._build_finetune_schema_lines(
            schema_text,
            representative_values,
            geometry_hints,
        )
        foreign_key_lines = self._extract_foreign_key_lines(
            schema=schema,
            compact_schema=compact_schema,
            metadata=metadata,
        )
        return (
            "\n".join(schema_lines) if schema_lines else "No schema available.",
            foreign_key_lines,
        )

    def _extract_foreign_key_lines(
            self,
            *,
            schema: str,
            compact_schema: str,
            metadata: Dict[str, Any],
    ) -> List[str]:
        lines: List[str] = []
        database_context = metadata.get("database_context")
        if isinstance(database_context, dict):
            for ddl in database_context.get("schema_ddls", []) or []:
                parsed = self._parse_finetune_create_table_ddl(str(ddl or ""))
                if parsed:
                    lines.extend(parsed.get("foreign_key_lines") or [])

            for table_meta in database_context.get("tables", []) or []:
                if not isinstance(table_meta, dict):
                    continue
                table_name = str(table_meta.get("table_name") or "").strip()
                for raw_fk in table_meta.get("foreign_keys", []) or []:
                    lines.extend(self._parse_foreign_key_lines_for_table(table_name, str(raw_fk or "")))

        schema_text = (schema or compact_schema or "").strip()
        if re.search(r"\bCREATE\s+TABLE\b", schema_text, flags=re.I):
            for ddl in self._split_finetune_schema_ddls(schema_text):
                parsed = self._parse_finetune_create_table_ddl(ddl)
                if parsed:
                    lines.extend(parsed.get("foreign_key_lines") or [])

        deduped: List[str] = []
        seen: set[str] = set()
        for line in lines:
            cleaned = str(line or "").strip()
            if not cleaned or cleaned in seen:
                continue
            seen.add(cleaned)
            deduped.append(cleaned)
        return deduped

    def _build_foreign_keys_block(self, foreign_key_lines: List[str]) -> str:
        if not foreign_key_lines:
            return ""
        return "[Foreign Keys]\n" + "\n".join(foreign_key_lines)

    def _build_custom_sections_block(
            self,
            dataset_name: Optional[str],
            data_item: Dict[str, Any],
    ) -> str:
        if not dataset_name:
            return ""
        dataset_info = (self.dataset_config.get("datasets") or {}).get(dataset_name) or {}
        sections = dataset_info.get("inference_prompt_sections") or []
        rendered: List[str] = []
        for section in sections:
            if not isinstance(section, dict):
                continue
            static_text = self._format_prompt_section_value(
                section.get("text") or section.get("body")
            )
            if static_text:
                title = self._normalize_prompt_section_title(
                    section.get("title") or section.get("label") or ""
                )
                if title:
                    rendered.append(f"[{title}]\n{static_text}")
                else:
                    rendered.append(static_text)
                continue
            source_key = str(section.get("source_key") or "").strip()
            if not source_key:
                continue
            title = self._normalize_prompt_section_title(
                section.get("title") or section.get("label") or source_key.split(".")[-1]
            )
            value = self._lookup_nested_value(data_item, source_key)
            rendered_value = self._format_prompt_section_value(value)
            if not rendered_value:
                continue
            rendered.append(f"[{title}]\n{rendered_value}")
        return "\n\n".join(rendered)

    def _resolve_database_display_id(
            self,
            *,
            dataset_name: Optional[str],
            data_item: Dict[str, Any],
            metadata: Dict[str, Any],
    ) -> str:
        for value in [
            dataset_name,
            data_item.get("database_id"),
            metadata.get("database_id"),
            metadata.get("database_key"),
        ]:
            text = self._stringify_value(value)
            if text:
                return text
        return ""

    def _build_named_section(self, title: str, body: Any) -> str:
        rendered_body = self._format_prompt_section_value(body)
        if not rendered_body:
            return ""
        return f"[{title}]\n{rendered_body}"

    @staticmethod
    def _lookup_nested_value(payload: Any, dotted_key: str) -> Any:
        current = payload
        for part in str(dotted_key or "").split("."):
            if not part:
                continue
            if isinstance(current, dict) and part in current:
                current = current[part]
                continue
            return None
        return current

    @staticmethod
    def _normalize_prompt_section_title(title: Any) -> str:
        raw = str(title or "").strip()
        if not raw:
            return ""
        return raw.strip("[]")

    def _format_prompt_section_value(self, value: Any) -> str:
        if value in (None, ""):
            return ""
        if isinstance(value, (dict, list, tuple)):
            return self._stable_json_text(value)
        return self._stringify_value(value)

    def _build_finetune_schema_lines(
            self,
            compact_schema: str,
            representative_values: Dict[str, List[Dict[str, Any]]],
            geometry_hints: Dict[str, Dict[str, Dict[str, Any]]],
    ) -> List[str]:
        schema = (compact_schema or "").strip()
        if not schema:
            return ["No schema available."]
        if re.search(r"\bCREATE\s+TABLE\b", schema, flags=re.I):
            ddls = self._split_finetune_schema_ddls(schema)
            rendered = self._build_finetune_schema_blocks_from_ddls(
                ddls,
                representative_values,
                geometry_hints,
            )
            return rendered or [schema]

        tables = self._parse_compact_schema_tables(schema)
        if not tables:
            return [schema]

        blocks: List[str] = []
        for table_name, columns in tables:
            rendered_columns = [
                {
                    "column_name": column_name,
                    "column_type": self._format_finetune_column_type(column_type),
                }
                for column_name, column_type in columns
            ]
            blocks.append(
                self._render_finetune_schema_block(
                    table_name=table_name,
                    columns=rendered_columns,
                    representative_rows=list(representative_values.get(table_name) or []),
                    primary_key_columns=set(),
                    geometry_hints=geometry_hints,
                )
            )
        return blocks

    @staticmethod
    def _split_finetune_schema_ddls(schema: str) -> List[str]:
        ddls: List[str] = []
        parts = re.split(r";\s*(?=CREATE\s+TABLE\b)", schema, flags=re.I)
        for part in parts:
            cleaned = part.strip()
            if not cleaned:
                continue
            if not cleaned.endswith(";"):
                cleaned = f"{cleaned};"
            ddls.append(cleaned)
        return ddls

    @staticmethod
    def _format_finetune_column_type(column_type: str) -> str:
        original = (column_type or "text").strip()
        normalized = original.lower()
        type_map = {
            "double": "double precision",
            "float": "double precision",
            "real": "double precision",
            "geometry": "geometry",
            "user-defined": "geometry",
            "numeric": "numeric",
            "integer": "integer",
            "int": "integer",
            "bigint": "bigint",
            "smallint": "smallint",
            "boolean": "boolean",
            "bool": "boolean",
            "date": "date",
            "timestamp": "timestamp",
            "timestamptz": "timestamp with time zone",
            "bytea": "bytea",
            "text": "text",
        }
        return type_map.get(normalized, original or "text")

    def _parse_compact_schema_tables(self, compact_schema: str) -> List[Tuple[str, List[Tuple[str, str]]]]:
        tables: List[Tuple[str, List[Tuple[str, str]]]] = []
        for raw_line in (compact_schema or "").splitlines():
            line = raw_line.strip()
            if not line:
                continue
            match = re.match(r"^-\s+([A-Za-z0-9_]+)\((.*)\)$", line)
            if match is None:
                match = re.match(r"^(?:table\s+)?([A-Za-z0-9_]+)\((.*)\)$", line, re.I)
            if match is None:
                continue
            table_name, raw_columns = match.groups()
            columns = self._parse_compact_schema_columns(raw_columns)
            if columns:
                tables.append((table_name, columns))
        return tables

    @staticmethod
    def _parse_compact_schema_columns(raw_columns: str) -> List[Tuple[str, str]]:
        columns: List[Tuple[str, str]] = []
        for raw_column in PromptBuilder._split_compact_schema_columns(raw_columns):
            match = re.match(
                r'^\s*"?(?P<name>[A-Za-z0-9_]+)"?\s+(?P<column_type>.+?)\s*$',
                raw_column,
                flags=re.DOTALL,
            )
            if match is None:
                continue
            columns.append(
                (
                    match.group("name").strip('"'),
                    match.group("column_type").strip(),
                )
            )
        return columns

    @staticmethod
    def _split_compact_schema_columns(raw_columns: str) -> List[str]:
        items: List[str] = []
        current: List[str] = []
        depth = 0
        for char in raw_columns or "":
            if char == "(":
                depth += 1
            elif char == ")" and depth > 0:
                depth -= 1
            if char == "," and depth == 0:
                item = "".join(current).strip()
                if item:
                    items.append(item)
                current = []
                continue
            current.append(char)
        tail = "".join(current).strip()
        if tail:
            items.append(tail)
        return items

    def _build_finetune_schema_blocks_from_ddls(
            self,
            schema_ddls: List[str],
            representative_values: Dict[str, List[Dict[str, Any]]],
            geometry_hints: Dict[str, Dict[str, Dict[str, Any]]],
    ) -> List[str]:
        schema_blocks: List[str] = []
        for ddl in schema_ddls:
            parsed = self._parse_finetune_create_table_ddl(ddl)
            if not parsed:
                cleaned = str(ddl or "").strip()
                if cleaned:
                    schema_blocks.append(cleaned)
                continue
            table_name = str(parsed.get("table_name") or "").strip()
            columns = list(parsed.get("columns") or [])
            primary_key_columns = {
                str(name).strip()
                for name in (parsed.get("primary_key_columns") or [])
                if str(name).strip()
            }
            representative_rows = list(representative_values.get(table_name) or []) if table_name else []
            schema_blocks.append(
                self._render_finetune_schema_block(
                    table_name=table_name,
                    columns=columns,
                    representative_rows=representative_rows,
                    primary_key_columns=primary_key_columns,
                    geometry_hints=geometry_hints,
                )
            )
        return schema_blocks

    @staticmethod
    def _render_finetune_schema_block(
            *,
            table_name: str,
            columns: List[Dict[str, Any]],
            representative_rows: List[Dict[str, Any]],
            primary_key_columns: set[str],
            geometry_hints: Dict[str, Dict[str, Dict[str, Any]]],
    ) -> str:
        lines = [f"# Table: {table_name}", "["]
        rendered_columns: List[str] = []
        for column in columns:
            if not isinstance(column, dict):
                continue
            column_name = str(column.get("column_name") or "").strip()
            raw_column_type = str(column.get("column_type") or column.get("data_type") or "text").strip()
            if not column_name:
                continue
            column_type = PromptBuilder._normalize_finetune_geometry_column_type(
                table_name,
                column_name,
                raw_column_type,
                geometry_hints,
            )
            fragments = [f"{column_name}:{column_type}"]
            if column_name in primary_key_columns:
                fragments.append("Primary Key")
            examples = PromptBuilder._collect_finetune_example_values(
                table_name,
                column_name,
                column_type,
                representative_rows,
                geometry_hints,
            )
            if examples:
                fragments.append(f"Examples: [{', '.join(examples)}]")
            rendered_columns.append(f"  ({', '.join(fragments)})")
        for index, rendered in enumerate(rendered_columns):
            suffix = "," if index < len(rendered_columns) - 1 else ""
            lines.append(f"{rendered}{suffix}")
        lines.append("]")
        return "\n".join(lines)

    @staticmethod
    def _collect_finetune_example_values(
            table_name: str,
            column_name: str,
            column_type: str,
            representative_rows: List[Dict[str, Any]],
            geometry_hints: Dict[str, Dict[str, Dict[str, Any]]],
    ) -> List[str]:
        examples: List[str] = []
        seen: set[str] = set()
        for row in representative_rows:
            if not isinstance(row, dict) or column_name not in row:
                continue
            rendered = PromptBuilder._format_finetune_example_value(
                table_name,
                column_name,
                column_type,
                row.get(column_name),
                geometry_hints,
            )
            if not rendered or rendered in seen:
                continue
            seen.add(rendered)
            examples.append(rendered)
        return examples

    @staticmethod
    def _format_finetune_example_value(
            table_name: str,
            column_name: str,
            column_type: str,
            value: Any,
            geometry_hints: Dict[str, Dict[str, Dict[str, Any]]],
    ) -> str:
        if value is None:
            return ""
        hint = PromptBuilder._get_finetune_geometry_hint(geometry_hints, table_name, column_name)
        if PromptBuilder._looks_finetune_geometry_column(column_type, hint):
            return PromptBuilder._format_finetune_geometry_example(
                table_name,
                column_name,
                column_type,
                value,
                hint,
            )
        return str(value).strip()

    def _build_finetune_geometry_hints(
            self,
            *,
            schema: str,
            compact_schema: str,
            metadata: Dict[str, Any],
    ) -> Dict[str, Dict[str, Dict[str, Any]]]:
        hints: Dict[str, Dict[str, Dict[str, Any]]] = {}
        database_context = metadata.get("database_context")
        if isinstance(database_context, dict):
            for ddl in database_context.get("schema_ddls", []) or []:
                parsed = self._parse_finetune_create_table_ddl(str(ddl or ""))
                if not parsed:
                    continue
                table_name = str(parsed.get("table_name") or "").strip()
                for column in parsed.get("columns", []) or []:
                    if not isinstance(column, dict):
                        continue
                    self._register_finetune_geometry_hint(
                        hints,
                        table_name,
                        str(column.get("column_name") or "").strip(),
                        str(column.get("column_type") or "").strip(),
                    )

            for table_meta in database_context.get("tables", []) or []:
                if not isinstance(table_meta, dict):
                    continue
                table_name = str(table_meta.get("table_name") or "").strip()
                for column in table_meta.get("columns", []) or []:
                    if not isinstance(column, dict):
                        continue
                    self._register_finetune_geometry_hint(
                        hints,
                        table_name,
                        str(column.get("column_name") or "").strip(),
                        str(column.get("column_type") or column.get("data_type") or "").strip(),
                    )
                for field in table_meta.get("spatial_fields", []) or []:
                    if not isinstance(field, dict):
                        continue
                    self._register_finetune_geometry_hint(
                        hints,
                        table_name,
                        str(field.get("column_name") or field.get("canonical_name") or "").strip(),
                        str(field.get("column_type") or "").strip(),
                        geometry_type=str(field.get("geometry_type") or "").strip(),
                        srid=field.get("srid"),
                    )

        for schema_text in [schema, compact_schema]:
            parsed_tables = self._parse_compact_schema_tables(str(schema_text or ""))
            for table_name, columns in parsed_tables:
                for column_name, column_type in columns:
                    self._register_finetune_geometry_hint(
                        hints,
                        table_name,
                        column_name,
                        column_type,
                    )
        return hints

    def _register_finetune_geometry_hint(
            self,
            hints: Dict[str, Dict[str, Dict[str, Any]]],
            table_name: str,
            column_name: str,
            column_type: str,
            *,
            geometry_type: str = "",
            srid: Any = None,
    ) -> None:
        table_key = str(table_name or "").strip()
        column_key = str(column_name or "").strip()
        type_text = str(column_type or "").strip()
        if not table_key or not column_key:
            return
        if not self._looks_geometry_like_type(type_text) and not geometry_type:
            return
        table_hints = hints.setdefault(table_key, {})
        entry = table_hints.setdefault(column_key, {})
        if type_text and not entry.get("source_type"):
            entry["source_type"] = type_text
        parsed_geometry_type, parsed_srid = self._parse_geometry_type_details(type_text)
        if geometry_type and not entry.get("geometry_type"):
            entry["geometry_type"] = self._normalize_finetune_geometry_name(geometry_type)
        elif parsed_geometry_type and not entry.get("geometry_type"):
            entry["geometry_type"] = self._normalize_finetune_geometry_name(parsed_geometry_type)
        if srid not in (None, "") and not entry.get("srid"):
            try:
                entry["srid"] = int(srid)
            except (TypeError, ValueError):
                entry["srid"] = srid
        elif parsed_srid is not None and not entry.get("srid"):
            entry["srid"] = parsed_srid

    @staticmethod
    def _get_finetune_geometry_hint(
            geometry_hints: Dict[str, Dict[str, Dict[str, Any]]],
            table_name: str,
            column_name: str,
    ) -> Dict[str, Any]:
        return dict((geometry_hints.get(table_name) or {}).get(column_name) or {})

    @staticmethod
    def _looks_geometry_like_type(column_type: str) -> bool:
        normalized = str(column_type or "").strip().lower()
        return normalized in {"geometry", "user-defined"} or normalized.startswith("geometry(")

    @staticmethod
    def _parse_geometry_type_details(column_type: str) -> Tuple[str, Optional[int]]:
        match = re.match(
            r"^\s*geometry\s*\(\s*([A-Za-z]+)\s*(?:,\s*(\d+)\s*)?\)\s*$",
            str(column_type or "").strip(),
            flags=re.I,
        )
        if not match:
            return "", None
        geometry_type = match.group(1) or ""
        srid_text = match.group(2)
        return geometry_type, int(srid_text) if srid_text else None

    @staticmethod
    def _normalize_finetune_geometry_name(geometry_type: str) -> str:
        mapping = {
            "POINT": "Point",
            "MULTIPOINT": "MultiPoint",
            "LINESTRING": "LineString",
            "MULTILINESTRING": "MultiLineString",
            "POLYGON": "Polygon",
            "MULTIPOLYGON": "MultiPolygon",
            "GEOMETRY": "Geometry",
            "GEOMETRYCOLLECTION": "GeometryCollection",
        }
        normalized = str(geometry_type or "").strip().upper()
        return mapping.get(normalized, str(geometry_type or "").strip() or "Geometry")

    @staticmethod
    def _infer_finetune_geometry_name(table_name: str, column_name: str) -> str:
        name = f"{table_name} {column_name}".lower()
        point_tokens = [
            "poi",
            "point",
            "station",
            "shelter",
            "stop",
            "office",
            "library",
            "parking",
            "address",
            "location",
            "ghcn",
        ]
        line_tokens = [
            "road",
            "street",
            "line",
            "route",
            "corridor",
            "track",
            "trail",
            "path",
            "rail",
        ]
        if any(token in name for token in point_tokens):
            return "Point"
        if any(token in name for token in line_tokens):
            return "MultiLineString"
        return "MultiPolygon"

    @staticmethod
    def _normalize_finetune_geometry_column_type(
            table_name: str,
            column_name: str,
            column_type: str,
            geometry_hints: Dict[str, Dict[str, Dict[str, Any]]],
    ) -> str:
        original = str(column_type or "text").strip()
        normalized = original.lower()
        type_map = {
            "double": "double precision",
            "float": "double precision",
            "real": "double precision",
            "numeric": "numeric",
            "integer": "integer",
            "int": "integer",
            "bigint": "bigint",
            "smallint": "smallint",
            "boolean": "boolean",
            "bool": "boolean",
            "date": "date",
            "timestamp": "timestamp",
            "timestamptz": "timestamp with time zone",
            "bytea": "bytea",
            "text": "text",
        }
        hint = PromptBuilder._get_finetune_geometry_hint(geometry_hints, table_name, column_name)
        if PromptBuilder._looks_finetune_geometry_column(original, hint):
            srid = hint.get("srid")
            if srid in (None, ""):
                _parsed_geometry_type, parsed_srid = PromptBuilder._parse_geometry_type_details(original)
                srid = parsed_srid
            if srid in (None, ""):
                srid = 4326
            return f"geometry(Geometry,{srid})"
        return type_map.get(normalized, original or "text")

    @staticmethod
    def _looks_finetune_geometry_column(column_type: str, hint: Dict[str, Any]) -> bool:
        return PromptBuilder._looks_geometry_like_type(column_type) or bool(hint)

    @staticmethod
    def _format_finetune_geometry_example(
            table_name: str,
            column_name: str,
            column_type: str,
            value: Any,
            hint: Dict[str, Any],
    ) -> str:
        text = str(value or "").strip()
        geometry_type = ""
        srid = hint.get("srid")
        if isinstance(value, dict):
            geometry_type = str(value.get("type") or "").strip()
        elif text and text != "<geometry>":
            upper = text.upper()
            if upper.startswith("SRID=") and ";" in text:
                srid_text, text = text.split(";", 1)
                srid_match = re.search(r"SRID\s*=\s*(\d+)", srid_text, flags=re.I)
                if srid_match:
                    srid = int(srid_match.group(1))
                text = text.strip()
            match = re.match(r"^([A-Za-z]+)", text)
            if match:
                geometry_type = match.group(1)
        if not geometry_type:
            geometry_type = str(hint.get("geometry_type") or "").strip()
        if not geometry_type:
            _parsed_geometry_type, parsed_srid = PromptBuilder._parse_geometry_type_details(column_type)
            geometry_type = _parsed_geometry_type
            if srid in (None, ""):
                srid = parsed_srid
        normalized_geometry_type = PromptBuilder._normalize_finetune_geometry_name(geometry_type)
        if normalized_geometry_type == "Geometry" and text in {"", "<geometry>", "GEOMETRY"}:
            geometry_type = PromptBuilder._infer_finetune_geometry_name(table_name, column_name)
        if not geometry_type:
            geometry_type = PromptBuilder._infer_finetune_geometry_name(table_name, column_name)
        geometry_display = PromptBuilder._normalize_finetune_geometry_name(geometry_type)
        if srid in (None, ""):
            srid = 4326
        return f"{geometry_display} (SRID={srid})"

    def _parse_finetune_create_table_ddl(self, ddl: str) -> Optional[Dict[str, Any]]:
        ddl_text = str(ddl or "").strip()
        if not ddl_text:
            return None
        match = re.search(
            r"CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?(?P<name>[^\s(]+)\s*\((?P<body>.*)\)\s*;?\s*$",
            ddl_text,
            flags=re.IGNORECASE | re.DOTALL,
        )
        if not match:
            return None
        raw_table_name = match.group("name").strip()
        table_name = raw_table_name.split(".")[-1].strip('"')
        body = match.group("body")
        entries = self._split_finetune_sql_entries(body)
        columns: List[Dict[str, str]] = []
        primary_key_columns: set[str] = set()
        foreign_key_lines: List[str] = []
        for entry in entries:
            cleaned = entry.strip()
            if not cleaned:
                continue
            upper = cleaned.upper()
            if "FOREIGN KEY" in upper and (
                    upper.startswith("CONSTRAINT") or upper.startswith("FOREIGN KEY")
            ):
                foreign_key_lines.extend(self._parse_foreign_key_lines_for_table(table_name, cleaned))
                continue
            if "PRIMARY KEY" in upper and (
                    upper.startswith("CONSTRAINT") or upper.startswith("PRIMARY KEY")
            ):
                primary_key_columns.update(self._parse_finetune_primary_key_columns(cleaned))
                continue
            parsed_column = self._parse_finetune_column_definition(cleaned)
            if not parsed_column:
                continue
            columns.append(parsed_column)
            if "PRIMARY KEY" in upper:
                primary_key_columns.add(parsed_column["column_name"])
            foreign_key_lines.extend(
                self._parse_inline_foreign_key_lines(
                    table_name=table_name,
                    column_name=parsed_column["column_name"],
                    entry=cleaned,
                )
            )
        return {
            "table_name": table_name,
            "columns": columns,
            "primary_key_columns": sorted(primary_key_columns),
            "foreign_key_lines": self._dedupe_string_list(foreign_key_lines),
        }

    @staticmethod
    def _split_finetune_sql_entries(body: str) -> List[str]:
        items: List[str] = []
        current: List[str] = []
        depth = 0
        for char in body:
            if char == "(":
                depth += 1
            elif char == ")" and depth > 0:
                depth -= 1
            if char == "," and depth == 0:
                item = "".join(current).strip()
                if item:
                    items.append(item)
                current = []
                continue
            current.append(char)
        tail = "".join(current).strip()
        if tail:
            items.append(tail)
        return items

    @staticmethod
    def _parse_finetune_primary_key_columns(entry: str) -> set[str]:
        match = re.search(r"PRIMARY\s+KEY\s*\((?P<columns>[^)]+)\)", entry, flags=re.IGNORECASE)
        if not match:
            return set()
        return {
            str(item).strip().strip('"')
            for item in match.group("columns").split(",")
            if str(item).strip()
        }

    @staticmethod
    def _parse_finetune_column_definition(entry: str) -> Optional[Dict[str, str]]:
        match = re.match(r'^\s*"?(?P<name>[A-Za-z0-9_]+)"?\s+(?P<rest>.+?)\s*$', entry, flags=re.DOTALL)
        if not match:
            return None
        column_name = str(match.group("name") or "").strip()
        remainder = match.group("rest").strip()
        constraint_match = re.search(
            r"\s+(?:NOT\s+NULL|NULL|DEFAULT|PRIMARY\s+KEY|UNIQUE|REFERENCES|CHECK|CONSTRAINT)\b",
            remainder,
            flags=re.IGNORECASE,
        )
        column_type = remainder[: constraint_match.start()].strip() if constraint_match else remainder
        return {"column_name": column_name, "column_type": column_type}

    def _parse_foreign_key_lines_for_table(self, table_name: str, entry: str) -> List[str]:
        match = re.search(
            r"FOREIGN\s+KEY\s*\((?P<local_columns>[^)]+)\)\s*REFERENCES\s+(?P<ref_table>[^\s(]+)\s*\((?P<ref_columns>[^)]+)\)",
            str(entry or ""),
            flags=re.IGNORECASE,
        )
        if not match:
            return []
        local_columns = self._split_identifier_list(match.group("local_columns"))
        ref_table = self._normalize_identifier(match.group("ref_table"))
        ref_columns = self._split_identifier_list(match.group("ref_columns"))
        return self._build_foreign_key_pairs(table_name, local_columns, ref_table, ref_columns)

    def _parse_inline_foreign_key_lines(
            self,
            *,
            table_name: str,
            column_name: str,
            entry: str,
    ) -> List[str]:
        match = re.search(
            r"\bREFERENCES\s+(?P<ref_table>[^\s(]+)\s*\((?P<ref_columns>[^)]+)\)",
            str(entry or ""),
            flags=re.IGNORECASE,
        )
        if not match:
            return []
        ref_table = self._normalize_identifier(match.group("ref_table"))
        ref_columns = self._split_identifier_list(match.group("ref_columns"))
        return self._build_foreign_key_pairs(table_name, [column_name], ref_table, ref_columns)

    def _build_foreign_key_pairs(
            self,
            table_name: str,
            local_columns: List[str],
            ref_table: str,
            ref_columns: List[str],
    ) -> List[str]:
        if not table_name or not ref_table or not local_columns or not ref_columns:
            return []
        pair_count = min(len(local_columns), len(ref_columns))
        return [
            f"{table_name}.{local_columns[index]} = {ref_table}.{ref_columns[index]}"
            for index in range(pair_count)
            if local_columns[index] and ref_columns[index]
        ]

    @staticmethod
    def _split_identifier_list(raw_columns: str) -> List[str]:
        return [
            PromptBuilder._normalize_identifier(part)
            for part in str(raw_columns or "").split(",")
            if PromptBuilder._normalize_identifier(part)
        ]

    @staticmethod
    def _normalize_identifier(identifier: Any) -> str:
        text = str(identifier or "").strip()
        if not text:
            return ""
        text = text.split(".")[-1].strip()
        return text.strip('"')

    @staticmethod
    def _dedupe_string_list(values: List[str]) -> List[str]:
        deduped: List[str] = []
        seen: set[str] = set()
        for value in values:
            cleaned = str(value or "").strip()
            if not cleaned or cleaned in seen:
                continue
            seen.add(cleaned)
            deduped.append(cleaned)
        return deduped

    @staticmethod
    def _parse_sample_data_block(sample_data_block: str) -> Dict[str, List[Dict[str, Any]]]:
        representative_values: Dict[str, List[Dict[str, Any]]] = {}
        current_table: Optional[str] = None
        for raw_line in (sample_data_block or "").splitlines():
            line = raw_line.strip()
            if not line:
                continue
            table_match = re.match(r"^-\s+([A-Za-z0-9_]+)\s*$", line)
            if table_match is not None:
                current_table = table_match.group(1)
                representative_values.setdefault(current_table, [])
                continue
            if current_table is None:
                continue
            try:
                payload = json.loads(line)
            except json.JSONDecodeError:
                payload = {"value": line}
            if isinstance(payload, dict):
                representative_values[current_table].append(payload)
        return representative_values

    def build_sql_synthesis_prompt(
            self,
            *,
            database: Any,
            difficulty_level: str,
            structural_constraints: Dict[str, Any],
            sampled_functions: List[Dict[str, Any]],
            database_runtime_metadata: Optional[Dict[str, Any]] = None,
            expected_limit: int | None = None,
            allow_limit: bool = True,
            require_order_by_with_limit: bool = False,
    ) -> str:
        schema_block = self._build_sql_prompt_context(
            database,
            database_runtime_metadata=database_runtime_metadata,
        )
        selected_tables = []
        if isinstance(database_runtime_metadata, dict):
            selected_tables = list(database_runtime_metadata.get("selected_table_names", []) or [])
        if not selected_tables:
            selected_tables = list(getattr(database, "selected_table_names", []) or [])

        functions_payload = []
        for item in sampled_functions:
            functions_payload.append(
                {
                    "function_name": item.get("function_name"),
                    "signature": item.get("signature"),
                    "categories": item.get("categories"),
                    "sampling_role": item.get("sampling_role"),
                    "description": item.get("description"),
                    "example_usages": item.get("example_usages"),
                }
            )

        template_text = self._load_named_template_text("sql_synthesis")
        return self._render_template(
            template_text,
            {
                "database_id": self._stringify_value(getattr(database, "database_id", "")),
                "city": self._stringify_value(getattr(database, "city", "")),
                "selected_tables": ", ".join(selected_tables),
                "schema_block": schema_block,
                "difficulty_level": self._stringify_value(difficulty_level),
                "difficulty_constraint_block": self._format_sql_difficulty_constraint(
                    difficulty_level,
                    structural_constraints,
                ),
                "error_coverage_block": self._format_sql_error_coverage_guidance(
                    structural_constraints.get("error_coverage")
                    if isinstance(structural_constraints, dict)
                    else None
                ),
                "required_function_block": self._stable_json_text(functions_payload),
                "result_window_guidance_block": self._format_sql_result_window_guidance(
                    expected_limit=expected_limit,
                    allow_limit=allow_limit,
                    require_order_by_with_limit=require_order_by_with_limit,
                ),
            },
        )

    def build_sql_revision_prompt(
            self,
            *,
            database: Any,
            original_sql: str,
            execution_error: str,
            used_tables: List[str],
            database_runtime_metadata: Optional[Dict[str, Any]] = None,
            expected_limit: int | None = None,
            allow_limit: bool = True,
            require_order_by_with_limit: bool = False,
    ) -> str:
        included_tables = {
                              str(table_name).strip()
                              for table_name in used_tables
                              if str(table_name).strip()
                          } or None
        schema_block = self._build_sql_prompt_context(
            database,
            database_runtime_metadata=database_runtime_metadata,
            included_tables=included_tables,
        )
        template_text = self._load_named_template_text("sql_revision")
        return self._render_template(
            template_text,
            {
                "database_id": self._stringify_value(getattr(database, "database_id", "")),
                "city": self._stringify_value(getattr(database, "city", "")),
                "involved_tables": ", ".join(sorted(included_tables)) if included_tables else "",
                "schema_block": schema_block,
                "original_sql": self._stringify_value(original_sql),
                "execution_error": self._stringify_value(execution_error or "unknown execution error"),
                "result_window_guidance_block": self._format_sql_result_window_guidance(
                    expected_limit=expected_limit,
                    allow_limit=allow_limit,
                    require_order_by_with_limit=require_order_by_with_limit,
                ),
            },
        )

    def build_question_revision_prompt(
            self,
            *,
            sql_query: Any,
            database_context: Dict[str, Any],
            sql_features: Dict[str, Any],
            current_question: str,
            execution_result_override: Dict[str, Any] | None = None,
            style_constraint: Dict[str, Any],
            spatial_relation_constraints: List[Dict[str, Any]],
            revision_feedback: str,
    ) -> str:
        schema_lines = self._build_question_schema_lines(database_context)
        execution_results = self._prepare_question_execution_results(
            execution_result_override
            if execution_result_override is not None
            else (getattr(sql_query, "execution_result", {}) or {})
        )
        used_function_names = {
            str(name).strip().upper()
            for name in (
                getattr(sql_query, "used_spatial_functions", None)
                or sql_features.get("postgis_functions", [])
                or []
            )
            if str(name).strip()
        }
        function_docs = []
        raw_constraints = getattr(sql_query, "spatial_function_constraints", None) or spatial_relation_constraints
        for item in raw_constraints or []:
            if not isinstance(item, dict):
                continue
            function_name = self._stringify_value(item.get("function_name")).upper()
            if used_function_names and function_name and function_name not in used_function_names:
                continue
            function_docs.append(
                {
                    "function_name": item.get("function_name"),
                    "signature": item.get("signature"),
                    "description": self._truncate_text(item.get("description"), 320),
                    "example_usages": [
                        self._truncate_text(example, 180)
                        for example in (item.get("example_usages") or [])[:1]
                        if self._truncate_text(example, 180)
                    ],
                }
            )
        template_text = self._load_named_template_text("question_revision")
        return self._render_template(
            template_text,
            {
                "sql_query": self._stringify_value(getattr(sql_query, "sql", "")),
                "current_question": self._stringify_value(current_question),
                "sql_feature_block": self._stable_json_text(sql_features),
                "database_id": self._stringify_value(database_context.get("database_id")),
                "city": self._stringify_value(database_context.get("city")),
                "selected_tables": ", ".join(database_context.get("selected_table_names", []) or []),
                "schema_block": chr(10).join(schema_lines) if schema_lines else "No schema available.",
                "execution_results_block": self._stable_json_text(execution_results),
                "style_constraint_block": self._stable_json_text(style_constraint),
                "spatial_relation_block": self._stable_json_text(function_docs),
                "revision_feedback_block": self._stringify_value(revision_feedback),
                "style_name": self._stringify_value(style_constraint.get("style")),
            },
        )

    @staticmethod
    def _detect_geometry_columns(table: Any) -> set[str]:
        names: set[str] = set()
        for column in getattr(table, "normalized_schema", []):
            if not isinstance(column, dict):
                continue
            canonical_type = str(column.get("canonical_type") or column.get("type") or "").strip().lower()
            if canonical_type != "spatial":
                continue
            for key in ("canonical_name", "name"):
                value = str(column.get(key) or "").strip().lower()
                if value:
                    names.add(value)
        for field in getattr(table, "spatial_fields", []):
            if not isinstance(field, dict):
                continue
            value = str(field.get("canonical_name") or "").strip().lower()
            if value:
                names.add(value)
        return names

    def _prepare_representative_rows(
            self,
            representative_values: Any,
            *,
            geometry_columns: set[str],
            limit: int,
    ) -> List[Dict[str, Any]]:
        normalized = representative_values
        if isinstance(normalized, dict) and isinstance(normalized.get("rows"), list):
            normalized = normalized.get("rows")

        rows: List[Dict[str, Any]] = []
        if isinstance(normalized, list):
            for item in normalized:
                if isinstance(item, dict):
                    rows.append(dict(item))
                else:
                    rows.append({"value": item})
        elif isinstance(normalized, dict):
            if self._looks_column_oriented_samples(normalized):
                rows = self._transpose_column_oriented_samples(normalized)
            else:
                rows = [dict(normalized)]

        prepared: List[Dict[str, Any]] = []
        seen: set[str] = set()
        for row in rows:
            normalized_row: Dict[str, Any] = {}
            for key, raw_value in row.items():
                column_name = str(key).strip()
                if not column_name:
                    continue
                normalized_row[column_name] = self._normalize_representative_value(
                    column_name,
                    raw_value,
                    geometry_columns=geometry_columns,
                )
            signature = self._stable_json_text(normalized_row)
            if signature in seen:
                continue
            seen.add(signature)
            prepared.append(normalized_row)
            if len(prepared) >= limit:
                break
        return prepared

    @staticmethod
    def _looks_column_oriented_samples(values: Dict[str, Any]) -> bool:
        if not values:
            return False
        if any(isinstance(item, dict) for item in values.values()):
            return False
        return any(isinstance(item, (list, tuple)) for item in values.values())

    @staticmethod
    def _transpose_column_oriented_samples(values: Dict[str, Any]) -> List[Dict[str, Any]]:
        normalized_columns: Dict[str, List[Any]] = {}
        max_len = 0
        for key, raw_value in values.items():
            column_name = str(key).strip()
            if not column_name:
                continue
            if isinstance(raw_value, (list, tuple)):
                items = list(raw_value)
            else:
                items = [raw_value]
            normalized_columns[column_name] = items
            max_len = max(max_len, len(items))

        rows: List[Dict[str, Any]] = []
        for index in range(max_len):
            row: Dict[str, Any] = {}
            for column_name, items in normalized_columns.items():
                row[column_name] = items[index] if index < len(items) else None
            rows.append(row)
        return rows

    def _normalize_representative_value(
            self,
            column_name: str,
            value: Any,
            *,
            geometry_columns: set[str],
    ) -> Any:
        if column_name.lower() in geometry_columns:
            if value in (None, ""):
                return None
            return self._geometry_preview(value)
        return value

    @staticmethod
    def _geometry_preview(value: Any) -> str:
        if isinstance(value, dict):
            geometry_type = str(value.get("type") or "").strip()
            return geometry_type.upper() if geometry_type else "GEOMETRY"
        text = str(value or "").strip()
        if not text:
            return "GEOMETRY"
        upper = text.upper()
        if upper.startswith("SRID=") and ";" in text:
            text = text.split(";", 1)[1].strip()
        match = re.match(r"^([A-Za-z]+)", text)
        if match:
            return match.group(1).upper()
        return "GEOMETRY"

    def _build_sql_prompt_context(
            self,
            database: Any,
            *,
            database_runtime_metadata: Optional[Dict[str, Any]] = None,
            included_tables: Optional[set[str]] = None,
    ) -> str:
        if isinstance(database_runtime_metadata, dict) and database_runtime_metadata.get("tables"):
            filtered_tables: List[Dict[str, Any]] = []
            schema_ddls: List[str] = []
            compact_schema_lines: List[str] = []
            for table_meta in database_runtime_metadata.get("tables", []):
                if not isinstance(table_meta, dict):
                    continue
                table_name = str(table_meta.get("table_name") or "").strip()
                if included_tables is not None and table_name not in included_tables:
                    continue
                filtered_tables.append(table_meta)
                create_table_ddl = str(table_meta.get("create_table_ddl") or "").strip()
                columns = []
                for column in table_meta.get("columns", []):
                    if not isinstance(column, dict):
                        continue
                    column_name = str(column.get("column_name") or "").strip()
                    column_type = str(column.get("column_type") or column.get("data_type") or "text").strip()
                    if column_name:
                        columns.append(f"{column_name} {column_type}")
                if create_table_ddl:
                    schema_ddls.append(create_table_ddl)
                elif table_name:
                    compact_schema_lines.append(f"- {table_name}({', '.join(columns)})")

            metadata = {
                "database_context": {
                    "tables": filtered_tables,
                    "schema_ddls": schema_ddls,
                }
            }
            geometry_hints = self._build_finetune_geometry_hints(
                schema="\n".join(schema_ddls),
                compact_schema="\n".join(compact_schema_lines),
                metadata=metadata,
            )
            schema_blocks: List[str] = []
            for table_meta in filtered_tables:
                table_name = str(table_meta.get("table_name") or "").strip()
                if not table_name:
                    continue
                parsed = self._parse_finetune_create_table_ddl(str(table_meta.get("create_table_ddl") or "").strip())
                if parsed:
                    columns = list(parsed.get("columns") or [])
                    primary_key_columns = {
                        str(name).strip()
                        for name in (parsed.get("primary_key_columns") or [])
                        if str(name).strip()
                    }
                else:
                    columns = []
                    for column in table_meta.get("columns", []):
                        if not isinstance(column, dict):
                            continue
                        column_name = str(column.get("column_name") or "").strip()
                        column_type = str(column.get("column_type") or column.get("data_type") or "text").strip()
                        if not column_name:
                            continue
                        columns.append(
                            {
                                "column_name": column_name,
                                "column_type": column_type or "text",
                            }
                        )
                    primary_key_columns = {
                        str(column.get("column_name") or "").strip()
                        for column in table_meta.get("columns", [])
                        if isinstance(column, dict) and bool(column.get("is_primary_key"))
                    }
                geometry_columns = {
                    str(field.get("column_name") or field.get("canonical_name") or "").strip().lower()
                    for field in table_meta.get("spatial_fields", [])
                    if isinstance(field, dict)
                }
                representative_rows = self._prepare_representative_rows(
                    table_meta.get("representative_values") or {},
                    geometry_columns=geometry_columns,
                    limit=3,
                )
                schema_blocks.append(
                    self._render_finetune_schema_block(
                        table_name=table_name,
                        columns=columns,
                        representative_rows=representative_rows,
                        primary_key_columns=primary_key_columns,
                        geometry_hints=geometry_hints,
                    )
                )
            return "\n".join(schema_blocks) if schema_blocks else "No schema available."

        geometry_hints: Dict[str, Dict[str, Dict[str, Any]]] = {}
        schema_blocks: List[str] = []
        for table in getattr(database, "selected_tables", []):
            table_name = str(getattr(table, "table_name", "")).strip()
            if included_tables is not None and table_name not in included_tables:
                continue
            geometry_columns = self._detect_geometry_columns(table)
            columns = []
            for field in getattr(table, "spatial_fields", []):
                if not isinstance(field, dict):
                    continue
                spatial_name = str(field.get("canonical_name") or "").strip()
                if not spatial_name:
                    continue
                crs_text = str(field.get("crs") or "").strip()
                srid_match = re.search(r"(\d+)$", crs_text)
                srid = int(srid_match.group(1)) if srid_match else 4326
                self._register_finetune_geometry_hint(
                    geometry_hints,
                    table_name,
                    spatial_name,
                    "geometry",
                    geometry_type=str(field.get("geometry_type") or "").strip(),
                    srid=srid,
                )
            for column in getattr(table, "normalized_schema", []):
                if not isinstance(column, dict):
                    continue
                column_name = str(column.get("canonical_name") or column.get("name") or "").strip()
                column_type = str(column.get("canonical_type") or column.get("type") or "text").strip()
                if not column_name:
                    continue
                normalized_type = "geometry" if column_type.lower() == "spatial" else column_type
                columns.append(
                    {
                        "column_name": column_name,
                        "column_type": normalized_type or "text",
                    }
                )
            representative_rows = self._prepare_representative_rows(
                getattr(table, "representative_values", None) or {},
                geometry_columns=geometry_columns,
                limit=3,
            )
            schema_blocks.append(
                self._render_finetune_schema_block(
                    table_name=table_name,
                    columns=columns,
                    representative_rows=representative_rows,
                    primary_key_columns=set(),
                    geometry_hints=geometry_hints,
                )
            )
        return "\n".join(schema_blocks) if schema_blocks else "No schema available."

    def build_question_generation_prompt(
            self,
            *,
            sql_query: Any,
            database_context: Dict[str, Any],
            sql_features: Dict[str, Any],
            execution_result_override: Dict[str, Any] | None = None,
            style_constraint: Dict[str, Any],
            spatial_relation_constraints: List[Dict[str, Any]],
    ) -> str:
        schema_lines = self._build_question_schema_lines(database_context)
        execution_results = self._prepare_question_execution_results(
            execution_result_override
            if execution_result_override is not None
            else (getattr(sql_query, "execution_result", {}) or {})
        )
        used_function_names = {
            str(name).strip().upper()
            for name in (
                    getattr(sql_query, "used_spatial_functions", None)
                    or sql_features.get("postgis_functions", [])
                    or []
            )
            if str(name).strip()
        }
        function_docs = []
        raw_constraints = getattr(sql_query, "spatial_function_constraints", None) or spatial_relation_constraints
        for item in raw_constraints or []:
            if not isinstance(item, dict):
                continue
            function_name = self._stringify_value(item.get("function_name")).upper()
            if used_function_names and function_name and function_name not in used_function_names:
                continue
            function_docs.append(
                {
                    "function_name": item.get("function_name"),
                    "signature": item.get("signature"),
                    "description": self._truncate_text(item.get("description"), 320),
                    "example_usages": [
                        self._truncate_text(example, 180)
                        for example in (item.get("example_usages") or [])[:1]
                        if self._truncate_text(example, 180)
                    ],
                }
            )
        template_text = self._load_named_template_text("question_generation")
        return self._render_template(
            template_text,
            {
                "sql_query": self._stringify_value(getattr(sql_query, "sql", "")),
                "sql_feature_block": self._stable_json_text(sql_features),
                "database_id": self._stringify_value(database_context.get("database_id")),
                "city": self._stringify_value(database_context.get("city")),
                "selected_tables": ", ".join(database_context.get("selected_table_names", []) or []),
                "schema_block": chr(10).join(schema_lines) if schema_lines else "No schema available.",
                "execution_results_block": self._stable_json_text(execution_results),
                "style_constraint_block": self._stable_json_text(style_constraint),
                "spatial_relation_block": self._stable_json_text(function_docs),
                "style_name": self._stringify_value(style_constraint.get("style")),
            },
        )

    @staticmethod
    def _truncate_text(value: Any, limit: int) -> str:
        text = str(value or "").strip()
        if len(text) <= limit:
            return text
        return text[: max(limit - 3, 0)].rstrip() + "..."

    def _prepare_question_execution_results(self, execution_results: Any) -> Dict[str, Any]:
        normalized = execution_results if isinstance(execution_results, dict) else {}
        summary: Dict[str, Any] = {
            "success": bool(normalized.get("success")),
            "empty_result": bool(normalized.get("empty_result")),
            "row_count": normalized.get("row_count"),
        }
        columns = normalized.get("columns")
        if isinstance(columns, list) and columns:
            summary["columns"] = [str(item) for item in columns if str(item).strip()]
        sample_rows = normalized.get("sample_rows") or normalized.get("rows") or []
        prepared_rows: List[Dict[str, Any]] = []
        for row in sample_rows[:3]:
            if not isinstance(row, dict):
                continue
            prepared_row: Dict[str, Any] = {}
            for key, value in row.items():
                column_name = str(key).strip()
                if not column_name:
                    continue
                lowered = column_name.lower()
                if any(token in lowered for token in ("geom", "geometry", "geography", "shape")):
                    prepared_row[column_name] = "[spatial value omitted]"
                    continue
                if isinstance(value, str):
                    prepared_row[column_name] = self._truncate_text(value, 120)
                else:
                    prepared_row[column_name] = value
            if prepared_row:
                prepared_rows.append(prepared_row)
        if prepared_rows:
            summary["sample_rows"] = prepared_rows
            if "columns" not in summary:
                summary["columns"] = list(prepared_rows[0].keys())
        error_message = self._stringify_value(normalized.get("error_message"))
        if error_message:
            summary["error_message"] = self._truncate_text(error_message, 200)
        return summary

    def build_quality_control_prompt(
            self,
            *,
            sample: Dict[str, Any],
            schema_lines: List[str],
            sql_feature_summary: Dict[str, Any],
            execution_summary: Dict[str, Any],
            representative_values: Dict[str, Any],
            judge_rules: Dict[str, Any],
    ) -> str:
        template_text = self._load_named_template_text("quality_control")
        return self._render_template(
            template_text,
            {
                "sample_block": self._stable_json_text(sample),
                "schema_block": chr(10).join(schema_lines) if schema_lines else "No schema available.",
                "sql_feature_block": self._stable_json_text(sql_feature_summary),
                "execution_summary_block": self._stable_json_text(execution_summary),
                "representative_values_block": self._stable_json_text(representative_values),
                "judge_rules_block": self._stable_json_text(judge_rules),
            },
        )

    @staticmethod
    def _build_question_schema_lines(database_context: Dict[str, Any]) -> List[str]:
        schema_ddls = [
            str(item).strip()
            for item in (database_context.get("schema_ddls", []) or [])
            if str(item).strip()
        ]
        if schema_ddls:
            return schema_ddls

        ddl_lines = [
            str(item.get("create_table_ddl") or "").strip()
            for item in (database_context.get("table_contexts", []) or [])
            if isinstance(item, dict) and str(item.get("create_table_ddl") or "").strip()
        ]
        if ddl_lines:
            return ddl_lines

        schema_lines: List[str] = []
        for table_item in database_context.get("schema", []) or []:
            if not isinstance(table_item, dict):
                continue
            table_name = str(table_item.get("table_name") or "").strip()
            columns = []
            for column in table_item.get("normalized_schema", []) or []:
                if not isinstance(column, dict):
                    continue
                column_name = str(column.get("canonical_name") or column.get("name") or "").strip()
                column_type = str(column.get("canonical_type") or column.get("type") or "text").strip()
                if column_name:
                    columns.append(f"{column_name} {column_type}")
            if table_name:
                schema_lines.append(f"- {table_name}({', '.join(columns)})")
        return schema_lines

    def _build_question_representative_values(self, database_context: Dict[str, Any]) -> Dict[str, Any]:
        representative_values: Dict[str, Any] = {}
        geometry_columns_by_table: Dict[str, set[str]] = {}

        for table_item in database_context.get("table_contexts", []) or []:
            if not isinstance(table_item, dict):
                continue
            table_name = str(table_item.get("table_name") or "").strip()
            if not table_name:
                continue
            geometry_columns_by_table[table_name] = {
                str(field.get("canonical_name") or field.get("column_name") or "").strip().lower()
                for field in table_item.get("spatial_fields", []) or []
                if isinstance(field, dict)
            }
            table_values = table_item.get("representative_values")
            if table_values:
                representative_values[table_name] = self._prepare_representative_rows(
                    table_values,
                    geometry_columns=geometry_columns_by_table[table_name],
                    limit=3,
                )

        if representative_values:
            return representative_values

        values = database_context.get("representative_values")
        if isinstance(values, dict):
            normalized: Dict[str, Any] = {}
            for table_name, table_values in values.items():
                geometry_columns = geometry_columns_by_table.get(str(table_name).strip(), set())
                normalized[str(table_name)] = self._prepare_representative_rows(
                    table_values,
                    geometry_columns=geometry_columns,
                    limit=3,
                )
            return normalized
        return {}

    @staticmethod
    def _build_question_spatial_lines(database_context: Dict[str, Any]) -> List[str]:
        spatial_lines: List[str] = []
        for field in database_context.get("spatial_fields", []) or []:
            if not isinstance(field, dict):
                continue
            table_name = str(field.get("table_name") or "").strip()
            column_name = str(
                field.get("column_name")
                or field.get("canonical_name")
                or field.get("field_name")
                or ""
            ).strip()
            column_type = str(field.get("column_type") or field.get("family") or "spatial").strip()
            spatial_type = str(field.get("spatial_type") or "").strip()
            geometry_type = str(field.get("geometry_type") or "").strip()
            srid = field.get("srid")
            if table_name and column_name:
                details = []
                if column_type:
                    details.append(f"type={column_type}")
                if spatial_type:
                    details.append(f"family={spatial_type}")
                if geometry_type:
                    details.append(f"geometry_type={geometry_type}")
                if srid not in (None, ""):
                    details.append(f"srid={srid}")
                spatial_lines.append(f"- {table_name}.{column_name} ({', '.join(details)})")
        return spatial_lines

    @staticmethod
    def _stringify_value(value: Any) -> str:
        return str(value).strip() if value not in (None, "") else ""

    @staticmethod
    def _stable_json_text(value: Any) -> str:
        return json.dumps(value, ensure_ascii=False, sort_keys=True, indent=2)

    def _format_sql_difficulty_constraint(
            self,
            difficulty_level: str,
            structural_constraints: Dict[str, Any],
    ) -> str:
        min_tables = structural_constraints.get("min_tables")
        max_tables = structural_constraints.get("max_tables")
        lines = [f"Difficulty tier: {difficulty_level}"]

        if difficulty_level == "easy":
            lines.extend(
                [
                    "- Authoritative rule: easy = one table.",
                    "- Use exactly 1 table.",
                    "- Prefer a direct filter, lookup, or scalar aggregate.",
                    "- Use ranking only when the question explicitly asks for top-k, nearest, farthest, largest, or smallest results.",
                    "- Do not use joins, subqueries, or CTEs.",
                ]
            )
        elif difficulty_level == "medium":
            lines.extend(
                [
                    "- Authoritative rule: medium = two tables with one spatial join.",
                    "- Use exactly 2 tables.",
                    "- Include exactly 1 spatial join connecting those two tables.",
                    "- Keep the query flat and direct. Do not use subqueries or CTEs.",
                ]
            )
        elif difficulty_level == "hard":
            lines.extend(
                [
                    "- Authoritative rule: hard = three tables with two spatial joins.",
                    "- Use exactly 3 tables.",
                    "- Include exactly 2 spatial joins connecting the three tables.",
                    "- Keep the logic flat and direct. Do not use nested subqueries or CTEs.",
                ]
            )
        else:
            lines.extend(
                [
                    "- Authoritative rule: extra-hard = three to four tables, at least one spatial join, and total spatial joins plus nested queries between two and four.",
                    f"- Use between {min_tables} and {max_tables} tables.",
                    "- Include at least 1 spatial join.",
                    "- Count each spatial join and each nested query (subquery or CTE) as one advanced operation.",
                    "- Keep the total number of spatial joins plus nested queries between 2 and 4.",
                    "- Prefer the simplest executable SQL that satisfies the tier instead of making the query harder for its own sake.",
                    "- Use nested structure only when it is semantically necessary.",
                ]
            )
        return "\n".join(lines)

    def _format_sql_error_coverage_guidance(self, error_coverage: Any) -> str:
        if not isinstance(error_coverage, dict) or not error_coverage:
            return ""
        lines: List[str] = []

        function_names = [
            str(name).strip()
            for name in (error_coverage.get("function_names") or [])
            if str(name).strip()
        ]
        if function_names:
            lines.append(
                "- For this sample, prioritize these profile functions when they fit naturally: "
                + ", ".join(f"`{name}`" for name in function_names)
                + "."
            )

        query_shape = str(error_coverage.get("query_shape") or "").strip()
        if query_shape:
            lines.append(f"- For this sample, keep the query shape aligned with: {query_shape}")

        constraints = [
            str(item).strip()
            for item in (error_coverage.get("constraints") or [])
            if str(item).strip()
        ]
        for constraint in constraints:
            lines.append(f"- For this sample, enforce: {constraint}")

        return "\n".join(lines)

    @staticmethod
    def _format_sql_result_window_guidance(
        *,
        expected_limit: int | None,
        allow_limit: bool,
        require_order_by_with_limit: bool,
    ) -> str:
        lines: list[str] = []
        if expected_limit is None and not allow_limit:
            lines.append("- Do not use LIMIT for this sample.")
            lines.append("- Use ORDER BY only when it is semantically necessary.")
            lines.append("- Prefer a scalar aggregate or direct lookup when it matches the question.")
            lines.append("- Prefer exactly one answer column; use two columns only for a natural label-plus-metric result.")
            return "\n".join(lines)

        if expected_limit is not None:
            lines.append("- This sample should behave like a bounded ranked result query.")
            if require_order_by_with_limit:
                lines.append("- Include ORDER BY before LIMIT.")
            lines.append(f"- Use LIMIT {expected_limit} exactly.")
            lines.append("- Do not use LIMIT for scalar aggregate queries.")
            lines.append("- Prefer exactly one answer column; include a second label or metric only when it is essential to the answer.")
            lines.append("- ORDER BY must use a scalar business field, scalar measurement, or distance, not a geometry-valued expression.")
            lines.append("- Do not project helper ranking metrics unless they are part of the answer.")
            return "\n".join(lines)

        lines.append("- LIMIT is optional for this sample.")
        if require_order_by_with_limit:
            lines.append("- If you use LIMIT, include ORDER BY before it.")
        lines.append("- Keep the output compact: prefer one answer column, or two columns only for a natural label-plus-metric result.")
        return "\n".join(lines)

    def _render_template(self, template_text: str, placeholders: Dict[str, str]) -> str:
        rendered = template_text
        for key, value in placeholders.items():
            rendered = rendered.replace(f"{{{{{key}}}}}", value.strip())
        rendered = re.sub(r"\{\{[^{}]+\}\}", "", rendered)
        return self._cleanup_rendered_prompt(rendered)

    def _resolve_ablation_config(self, config_type: str) -> Dict[str, Any]:
        config = self.ablation_configs.get(config_type)
        if config is not None:
            return config
        return {
            "use_rag": config_type in ["rag", "full"],
            "use_keyword": config_type in ["keyword", "full"],
            "prompt_style": "default",
        }

    def _resolve_prompt_style(
            self,
            prompt_style: str,
            dataset_name: Optional[str],
    ) -> Dict[str, Any]:
        default_style = self.prompt_styles.get("default", {})
        style_config = self.prompt_styles.get(prompt_style, {})
        merged = dict(default_style)
        merged.update(style_config)
        if merged.get("dataset_specific") and dataset_name:
            merged.update(
                self.prompt_enhancement_registry.resolve_dataset_override(dataset_name)
            )
        return merged

    def _load_template_text(self, prompt_style: str, style_spec: Dict[str, Any]) -> str:
        template_path = style_spec.get("template_path")
        if template_path:
            path = Path(template_path)
            if not path.is_absolute():
                path = (self.project_root / template_path).resolve()
        else:
            path = self.template_path

        cache_key = f"{prompt_style}:{path}"
        if cache_key not in self._template_cache:
            self._template_cache[cache_key] = path.read_text(encoding="utf-8")
        return self._template_cache[cache_key]

    def _load_named_template_text(self, template_name: str) -> str:
        path = self.named_prompt_templates[template_name]
        cache_key = f"named:{template_name}:{path}"
        if cache_key not in self._template_cache:
            self._template_cache[cache_key] = path.read_text(encoding="utf-8")
        return self._template_cache[cache_key]

    def _build_grounding_block(
            self,
            dataset_name: Optional[str],
            metadata: Dict[str, Any],
            style_spec: Dict[str, Any],
    ) -> str:
        if not style_spec.get("use_dataset_context"):
            return ""
        return self.prompt_enhancement_registry.build_grounding_block(
            dataset_name or "",
            metadata,
            )

    def _build_schema_semantics_block(
            self,
            dataset_name: Optional[str],
            metadata: Dict[str, Any],
            style_spec: Dict[str, Any],
            compact_schema: str,
    ) -> str:
        if not style_spec.get("use_dataset_context"):
            return ""
        build_fn = getattr(
            self.prompt_enhancement_registry,
            "build_schema_semantics_block",
            None,
        )
        if build_fn is None:
            return ""
        return build_fn(
            dataset_name or "",
            metadata,
            compact_schema,
            )

    @staticmethod
    def _cleanup_rendered_prompt(rendered: str) -> str:
        preamble, sections = PromptBuilder._split_prompt_sections(rendered)
        cleaned_sections: List[Tuple[str, List[str]]] = []

        for header, body_lines in sections:
            cleaned_body = [
                line.rstrip()
                for line in body_lines
                if not PromptBuilder._is_empty_metadata_line(line)
            ]
            cleaned_body = PromptBuilder._trim_blank_lines(cleaned_body)
            if cleaned_body or PromptBuilder._should_keep_empty_section(header):
                cleaned_sections.append((header, cleaned_body))

        prompt_lines = PromptBuilder._trim_blank_lines([line.rstrip() for line in preamble])
        for header, body in cleaned_sections:
            if prompt_lines:
                prompt_lines.append("")
            prompt_lines.append(header)
            prompt_lines.extend(body)

        return "\n".join(PromptBuilder._trim_blank_lines(prompt_lines))

    @staticmethod
    def _split_prompt_sections(rendered: str) -> Tuple[List[str], List[Tuple[str, List[str]]]]:
        preamble: List[str] = []
        sections: List[Tuple[str, List[str]]] = []
        current_header: Optional[str] = None
        current_body: List[str] = []

        for line in rendered.splitlines():
            if line.startswith("## ") or re.match(r"^\[[^\]]+\]\s*$", line.strip()):
                if current_header is None:
                    pass
                else:
                    sections.append((current_header, current_body))
                current_header = line.rstrip()
                current_body = []
                continue

            if current_header is None:
                preamble.append(line.rstrip())
            else:
                current_body.append(line.rstrip())

        if current_header is not None:
            sections.append((current_header, current_body))

        return preamble, sections

    @staticmethod
    def _is_empty_metadata_line(line: str) -> bool:
        return bool(re.match(r"^\s*-\s+[A-Za-z0-9_ ]+:\s*$", line))

    @staticmethod
    def _should_keep_empty_section(header: str) -> bool:
        return header.strip() in {"[SQL]"}

    @staticmethod
    def _trim_blank_lines(lines: List[str]) -> List[str]:
        start = 0
        end = len(lines)
        while start < end and not lines[start].strip():
            start += 1
        while end > start and not lines[end - 1].strip():
            end -= 1
        trimmed = lines[start:end]
        compacted: List[str] = []
        previous_blank = False
        for line in trimmed:
            is_blank = not line.strip()
            if is_blank and previous_blank:
                continue
            compacted.append(line)
            previous_blank = is_blank
        return compacted

    def build_batch_prompts(
            self,
            questions: list,
            schema: str,
            config_type: str = 'base',
            rag_contexts: Optional[list] = None,
            keyword_contexts: Optional[list] = None,
            dataset_name: Optional[str] = None,
            metadatas: Optional[list] = None,
            data_items: Optional[list] = None,
    ) -> list:
        """
        批量构建prompts

        Args:
            questions: 问题列表
            schema: 数据库Schema
            config_type: 配置类型
            rag_contexts: RAG context列表（可选）
            keyword_contexts: Keyword context列表（可选）

        Returns:
            prompt列表
        """
        prompts = []

        for i, question in enumerate(questions):
            rag_ctx = rag_contexts[i] if rag_contexts and i < len(rag_contexts) else None
            kw_ctx = keyword_contexts[i] if keyword_contexts and i < len(keyword_contexts) else None
            metadata = metadatas[i] if metadatas and i < len(metadatas) else None
            data_item = data_items[i] if data_items and i < len(data_items) else None

            prompt = self.build_prompt(
                question=question,
                schema=schema,
                config_type=config_type,
                rag_context=rag_ctx,
                keyword_context=kw_ctx,
                dataset_name=dataset_name,
                metadata=metadata,
                data_item=data_item,
            )
            prompts.append(prompt)

        return prompts

    @staticmethod
    def get_config_description(config_type: str) -> str:
        """
        获取配置类型的描述

        Args:
            config_type: 配置类型

        Returns:
            配置描述字符串
        """
        descriptions = {
            'base': 'Question + Schema',
            'rag': 'Question + Schema + Retrieved Context',
            'keyword': 'Question + Schema + Keyword Context',
            'full': 'Question + Schema + Retrieved Context + Keyword Context',
            'finetune_alpaca': 'Instruction + Schema + Representative Values + Question',
        }
        return descriptions.get(config_type, 'Unknown')
