"""Prompt rendering for spatial Text-to-SQL fine-tuning."""

from __future__ import annotations

import json
import re
from typing import Any, Mapping, Sequence

from .utils import stable_jsonify, to_text


class FinetunePromptRenderer:
    INSTRUCTION_HEADER = (
        "You are a PostgreSQL/PostGIS expert. Read and understand the following database schema, "
        "and use PostgreSQL/PostGIS knowledge to generate a SQL query that answers the user question."
    )

    def __init__(
        self,
        *,
        task_description: str,
        max_representative_rows: int = 3,
    ) -> None:
        self.task_description = to_text(task_description)
        self.max_representative_rows = max(int(max_representative_rows), 1)

    def render_instruction(self) -> str:
        return self.INSTRUCTION_HEADER

    def render_input(
        self,
        *,
        database_id: str = "",
        question: str,
        schema_lines: Sequence[str],
        representative_values: Mapping[str, Any],
    ) -> str:
        del representative_values
        sections = [
            "[Database Schema]",
            f"[DB_ID] {to_text(database_id)}" if to_text(database_id) else "[DB_ID]",
            "[Schema]",
            "\n".join(schema_lines) if schema_lines else "No schema available.",
            "",
            "[User Question]",
            to_text(question),
            "",
            "```sql",
        ]
        return "\n".join(sections).strip()

    def render_prompt(
        self,
        *,
        question: str,
        schema_lines: Sequence[str],
        representative_values: Mapping[str, Any],
    ) -> str:
        return self.compose_prompt(
            self.render_instruction(),
            self.render_input(
                database_id="",
                question=question,
                schema_lines=schema_lines,
                representative_values=representative_values,
            ),
        )

    @staticmethod
    def compose_prompt(instruction: str, input_text: str) -> str:
        instruction_text = to_text(instruction).strip()
        input_block = to_text(input_text).strip()
        prompt_body = instruction_text or input_block
        if instruction_text and input_block:
            prompt_body = f"{instruction_text}\n\n{input_block}"
        return prompt_body if prompt_body else ""

    def render_output(self, reasoning_summary: str, sql: str) -> str:
        del reasoning_summary
        sql_text = to_text(sql).strip()
        return sql_text

    @staticmethod
    def build_runtime_prompt_context(
        database_runtime_metadata: Mapping[str, Any] | None,
        *,
        included_tables: Sequence[str] | None = None,
        max_representative_rows: int = 3,
    ) -> tuple[list[str], dict[str, Any]]:
        schema_ddls = [
            to_text(item)
            for item in ((database_runtime_metadata or {}).get("schema_ddls") or [])
            if to_text(item)
        ]
        direct_representative_values = (database_runtime_metadata or {}).get("representative_values")
        if schema_ddls or isinstance(direct_representative_values, Mapping):
            representative_values: dict[str, Any] = {}
            if isinstance(direct_representative_values, Mapping):
                for table_name, table_values in direct_representative_values.items():
                    normalized_name = to_text(table_name)
                    if not normalized_name:
                        continue
                    representative_values[normalized_name] = FinetunePromptRenderer._prepare_representative_rows(
                        table_values,
                        geometry_columns=set(),
                        limit=max_representative_rows,
                    )
            return (
                FinetunePromptRenderer._build_schema_blocks_from_schema_ddls(schema_ddls, representative_values),
                representative_values,
            )

        included = {
            to_text(table_name)
            for table_name in (included_tables or [])
            if to_text(table_name)
        } or None
        schema_lines: list[str] = []
        representative_values: dict[str, Any] = {}
        for table_meta in (database_runtime_metadata or {}).get("tables", []):
            if not isinstance(table_meta, Mapping):
                continue
            table_name = to_text(table_meta.get("table_name"))
            if not table_name:
                continue
            if included is not None and table_name not in included:
                continue
            columns = []
            for column in table_meta.get("columns", []):
                if not isinstance(column, Mapping):
                    continue
                column_name = to_text(column.get("column_name"))
                column_type = to_text(column.get("column_type") or column.get("data_type") or "text")
                if column_name:
                    columns.append({"column_name": column_name, "column_type": column_type})
            geometry_columns = {
                to_text(field.get("column_name") or field.get("canonical_name")).lower()
                for field in table_meta.get("spatial_fields", [])
                if isinstance(field, Mapping) and to_text(field.get("column_name") or field.get("canonical_name"))
            }
            table_rep_values = table_meta.get("representative_values") or {}
            prepared_rows = FinetunePromptRenderer._prepare_representative_rows(
                table_rep_values,
                geometry_columns=geometry_columns,
                limit=max_representative_rows,
            )
            representative_values[table_name] = prepared_rows
            schema_lines.append(
                FinetunePromptRenderer._render_schema_block(
                    table_name=table_name,
                    columns=columns,
                    representative_rows=prepared_rows,
                    primary_key_columns=set(),
                )
            )
        return schema_lines, representative_values

    @staticmethod
    def _build_schema_blocks_from_schema_ddls(
        schema_ddls: Sequence[str],
        representative_values: Mapping[str, Any],
    ) -> list[str]:
        schema_blocks: list[str] = []
        for ddl in schema_ddls:
            parsed = FinetunePromptRenderer._parse_create_table_ddl(ddl)
            if not parsed:
                cleaned = to_text(ddl).strip()
                if cleaned:
                    schema_blocks.append(cleaned)
                continue
            table_name = to_text(parsed.get("table_name"))
            columns = list(parsed.get("columns") or [])
            primary_key_columns = {
                to_text(name)
                for name in (parsed.get("primary_key_columns") or [])
                if to_text(name)
            }
            representative_rows = []
            if table_name:
                representative_rows = list(representative_values.get(table_name) or [])
            schema_blocks.append(
                FinetunePromptRenderer._render_schema_block(
                    table_name=table_name,
                    columns=columns,
                    representative_rows=representative_rows,
                    primary_key_columns=primary_key_columns,
                )
            )
        return schema_blocks

    @staticmethod
    def _render_schema_block(
        *,
        table_name: str,
        columns: Sequence[Mapping[str, Any]],
        representative_rows: Sequence[Mapping[str, Any]],
        primary_key_columns: set[str],
    ) -> str:
        lines = [f"# Table: {table_name}", "["]
        rendered_columns: list[str] = []
        for column in columns:
            if not isinstance(column, Mapping):
                continue
            column_name = to_text(column.get("column_name"))
            column_type = to_text(column.get("column_type") or column.get("data_type") or "TEXT")
            if not column_name:
                continue
            fragments = [f"{column_name}:{column_type}"]
            if column_name in primary_key_columns:
                fragments.append("Primary Key")
            examples = FinetunePromptRenderer._collect_example_values(column_name, representative_rows)
            if examples:
                fragments.append(f"Examples: [{', '.join(examples)}]")
            rendered_columns.append(f"  ({', '.join(fragments)})")
        if rendered_columns:
            rendered_columns[-1] = rendered_columns[-1]
        for index, rendered in enumerate(rendered_columns):
            suffix = "," if index < len(rendered_columns) - 1 else ""
            lines.append(f"{rendered}{suffix}")
        lines.append("]")
        return "\n".join(lines)

    @staticmethod
    def _collect_example_values(column_name: str, representative_rows: Sequence[Mapping[str, Any]]) -> list[str]:
        examples: list[str] = []
        seen: set[str] = set()
        for row in representative_rows:
            if not isinstance(row, Mapping) or column_name not in row:
                continue
            value = row.get(column_name)
            rendered = FinetunePromptRenderer._format_example_value(value)
            if not rendered or rendered in seen:
                continue
            seen.add(rendered)
            examples.append(rendered)
        return examples

    @staticmethod
    def _format_example_value(value: Any) -> str:
        if value is None:
            return ""
        if isinstance(value, str):
            rendered = value.strip()
        else:
            rendered = str(value).strip()
        return rendered

    @staticmethod
    def _parse_create_table_ddl(ddl: str) -> dict[str, Any] | None:
        ddl_text = to_text(ddl).strip()
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
        entries = FinetunePromptRenderer._split_sql_entries(body)
        columns: list[dict[str, str]] = []
        primary_key_columns: set[str] = set()
        for entry in entries:
            cleaned = entry.strip()
            if not cleaned:
                continue
            upper = cleaned.upper()
            if "PRIMARY KEY" in upper and (
                upper.startswith("CONSTRAINT") or upper.startswith("PRIMARY KEY")
            ):
                primary_key_columns.update(FinetunePromptRenderer._parse_primary_key_columns(cleaned))
                continue
            parsed_column = FinetunePromptRenderer._parse_column_definition(cleaned)
            if not parsed_column:
                continue
            columns.append(parsed_column)
            if "PRIMARY KEY" in upper:
                primary_key_columns.add(parsed_column["column_name"])
        return {
            "table_name": table_name,
            "columns": columns,
            "primary_key_columns": sorted(primary_key_columns),
        }

    @staticmethod
    def _split_sql_entries(body: str) -> list[str]:
        items: list[str] = []
        current: list[str] = []
        depth = 0
        for char in body:
            if char == "(":
                depth += 1
            elif char == ")" and depth > 0:
                depth -= 1
            if char == "," and depth == 0:
                items.append("".join(current).strip())
                current = []
                continue
            current.append(char)
        tail = "".join(current).strip()
        if tail:
            items.append(tail)
        return items

    @staticmethod
    def _parse_primary_key_columns(entry: str) -> set[str]:
        match = re.search(r"PRIMARY\s+KEY\s*\((?P<columns>[^)]+)\)", entry, flags=re.IGNORECASE)
        if not match:
            return set()
        return {
            to_text(item).strip().strip('"')
            for item in match.group("columns").split(",")
            if to_text(item).strip()
        }

    @staticmethod
    def _parse_column_definition(entry: str) -> dict[str, str] | None:
        match = re.match(r'^\s*"?(?P<name>[A-Za-z0-9_]+)"?\s+(?P<rest>.+?)\s*$', entry, flags=re.DOTALL)
        if not match:
            return None
        column_name = to_text(match.group("name")).strip()
        remainder = match.group("rest").strip()
        constraint_match = re.search(
            r"\s+(?:NOT\s+NULL|NULL|DEFAULT|PRIMARY\s+KEY|UNIQUE|REFERENCES|CHECK|CONSTRAINT)\b",
            remainder,
            flags=re.IGNORECASE,
        )
        column_type = remainder[: constraint_match.start()].strip() if constraint_match else remainder
        return {"column_name": column_name, "column_type": column_type}

    @staticmethod
    def _prepare_representative_rows(
        representative_values: Any,
        *,
        geometry_columns: set[str],
        limit: int,
    ) -> list[dict[str, Any]]:
        normalized = representative_values
        if isinstance(normalized, Mapping) and isinstance(normalized.get("rows"), list):
            normalized = normalized.get("rows")
        rows: list[dict[str, Any]] = []
        if isinstance(normalized, list):
            for item in normalized:
                if isinstance(item, Mapping):
                    rows.append(dict(item))
                else:
                    rows.append({"value": item})
        elif isinstance(normalized, Mapping):
            if FinetunePromptRenderer._looks_column_oriented_samples(normalized):
                rows = FinetunePromptRenderer._transpose_column_oriented_samples(normalized)
            else:
                rows = [dict(normalized)]

        prepared: list[dict[str, Any]] = []
        seen: set[str] = set()
        for row in rows:
            normalized_row: dict[str, Any] = {}
            for key, raw_value in row.items():
                column_name = to_text(key)
                if not column_name:
                    continue
                normalized_row[column_name] = FinetunePromptRenderer._normalize_representative_value(
                    column_name,
                    raw_value,
                    geometry_columns=geometry_columns,
                )
            signature = FinetunePromptRenderer._stable_json_text(normalized_row)
            if signature in seen:
                continue
            seen.add(signature)
            prepared.append(normalized_row)
            if len(prepared) >= limit:
                break
        return prepared

    @staticmethod
    def _normalize_representative_value(
        column_name: str,
        value: Any,
        *,
        geometry_columns: set[str],
    ) -> Any:
        if column_name.lower() in geometry_columns:
            if value in (None, ""):
                return None
            return FinetunePromptRenderer._geometry_preview(value)
        return value

    @staticmethod
    def _geometry_preview(value: Any) -> str:
        if isinstance(value, Mapping):
            geometry_type = to_text(value.get("type"))
            return geometry_type.upper() if geometry_type else "GEOMETRY"
        text = to_text(value).strip()
        if not text:
            return "GEOMETRY"
        upper = text.upper()
        if upper.startswith("SRID=") and ";" in text:
            text = text.split(";", 1)[1].strip()
        match = re.match(r"^([A-Za-z]+)", text)
        if match:
            return match.group(1).upper()
        return "GEOMETRY"

    @staticmethod
    def _looks_column_oriented_samples(values: Mapping[str, Any]) -> bool:
        if not values:
            return False
        if any(isinstance(item, Mapping) for item in values.values()):
            return False
        return any(isinstance(item, (list, tuple)) for item in values.values())

    @staticmethod
    def _transpose_column_oriented_samples(values: Mapping[str, Any]) -> list[dict[str, Any]]:
        normalized_columns: dict[str, list[Any]] = {}
        max_len = 0
        for key, raw_value in values.items():
            column_name = to_text(key)
            if not column_name:
                continue
            items = list(raw_value) if isinstance(raw_value, (list, tuple)) else [raw_value]
            normalized_columns[column_name] = items
            max_len = max(max_len, len(items))
        rows: list[dict[str, Any]] = []
        for index in range(max_len):
            row: dict[str, Any] = {}
            for column_name, items in normalized_columns.items():
                row[column_name] = items[index] if index < len(items) else None
            rows.append(row)
        return rows

    @staticmethod
    def _stable_json_text(value: Any) -> str:
        return json.dumps(stable_jsonify(value), ensure_ascii=False, indent=2, sort_keys=True)
