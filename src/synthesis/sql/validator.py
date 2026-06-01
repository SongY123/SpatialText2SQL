"""Static validation for synthesized PostGIS SQL queries."""

from __future__ import annotations

import re
from typing import Iterable, Mapping, Sequence

from src.synthesis.database.models import SynthesizedSpatialDatabase
from src.synthesis.database.utils import to_text

from .function_library import PostGISFunctionLibrary, fixed_spatial_join_function_names
from .models import SQLValidationResult

import sqlglot
from sqlglot import exp


DANGEROUS_SQL_PATTERN = re.compile(
    r"\b(drop|delete|update|insert|alter|truncate|create|grant|revoke|comment|copy|vacuum|analyze|refresh)\b",
    re.I,
)
SQL_KEYWORDS = {
    "select", "from", "where", "join", "left", "right", "inner", "outer", "on", "and", "or", "not",
    "group", "by", "order", "limit", "with", "as", "distinct", "count", "sum", "avg", "min", "max",
    "case", "when", "then", "else", "end", "exists", "in", "union", "intersect", "except", "having",
    "asc", "desc", "is", "null", "like", "between", "true", "false",
}


def _split_top_level_commas(text: str) -> list[str]:
    parts: list[str] = []
    current: list[str] = []
    depth = 0
    for char in text:
        if char == "(":
            depth += 1
        elif char == ")":
            depth = max(depth - 1, 0)
        elif char == "," and depth == 0:
            value = "".join(current).strip()
            if value:
                parts.append(value)
            current = []
            continue
        current.append(char)
    trailing = "".join(current).strip()
    if trailing:
        parts.append(trailing)
    return parts


def contains_dangerous_sql(sql: str) -> bool:
    return DANGEROUS_SQL_PATTERN.search(sql or "") is not None


def _strip_string_literals(sql: str) -> str:
    return re.sub(r"'(?:''|[^'])*'", "''", sql)


def _split_sql_statements(sql: str) -> list[str]:
    statements: list[str] = []
    current: list[str] = []
    in_string = False
    for char in sql:
        if char == "'":
            in_string = not in_string
        if char == ";" and not in_string:
            statement = "".join(current).strip()
            if statement:
                statements.append(statement)
            current = []
            continue
        current.append(char)
    trailing = "".join(current).strip()
    if trailing:
        statements.append(trailing)
    return statements


def _build_allowed_schema(
    database: SynthesizedSpatialDatabase,
    runtime_metadata: Mapping[str, object] | None = None,
) -> tuple[set[str], dict[str, set[str]]]:
    if isinstance(runtime_metadata, Mapping):
        tables_payload = runtime_metadata.get("tables")
        if isinstance(tables_payload, Sequence) and not isinstance(tables_payload, (str, bytes)):
            allowed_tables: set[str] = set()
            allowed_columns: dict[str, set[str]] = {}
            union_columns: set[str] = set()
            for table_meta in tables_payload:
                if not isinstance(table_meta, Mapping):
                    continue
                table_name = to_text(table_meta.get("table_name"))
                if not table_name:
                    continue
                allowed_tables.add(table_name)
                columns = {
                    to_text(column.get("column_name"))
                    for column in table_meta.get("columns", [])
                    if isinstance(column, Mapping)
                }
                columns = {column for column in columns if column}
                allowed_columns[table_name] = columns
                union_columns.update(columns)
            if allowed_tables:
                allowed_columns["*"] = union_columns
                return allowed_tables, allowed_columns

    allowed_tables: set[str] = set()
    allowed_columns: dict[str, set[str]] = {}
    union_columns: set[str] = set()
    for table in database.selected_tables:
        table_name = to_text(table.table_name)
        allowed_tables.add(table_name)
        columns = {
            to_text(column.get("canonical_name") or column.get("name"))
            for column in table.normalized_schema
            if isinstance(column, Mapping)
        }
        columns = {column for column in columns if column}
        allowed_columns[table_name] = columns
        union_columns.update(columns)
    allowed_columns["*"] = union_columns
    return allowed_tables, allowed_columns


def _detect_tables_regex(sql: str) -> tuple[list[str], dict[str, str]]:
    pattern = re.compile(
        r"\b(?:from|join)\s+([a-zA-Z_][\w\.]*)(?:\s+(?:as\s+)?([a-zA-Z_][\w]*))?",
        re.I,
    )
    tables: list[str] = []
    aliases: dict[str, str] = {}
    for match in pattern.finditer(sql):
        raw_table = match.group(1).split(".")[-1]
        alias = to_text(match.group(2))
        if raw_table.lower() in {"select"}:
            continue
        tables.append(raw_table)
        if alias and alias.lower() not in SQL_KEYWORDS:
            aliases[alias] = raw_table
    return tables, aliases


def _detect_columns_regex(sql: str, aliases: Mapping[str, str]) -> list[str]:
    columns: list[str] = []
    for alias, column in re.findall(r"\b([a-zA-Z_][\w]*)\.([a-zA-Z_][\w]*)\b", sql):
        if alias.lower() in SQL_KEYWORDS:
            continue
        if alias in aliases or alias.lower().startswith("st_"):
            columns.append(column)
    return columns


def _detect_functions(sql: str) -> list[str]:
    return sorted(set(match.group(1) for match in re.finditer(r"\b(ST_[A-Za-z0-9_]+)\s*\(", sql, re.I)))


def _function_call_arg_counts(sql: str) -> dict[str, list[int]]:
    arg_counts: dict[str, list[int]] = {}
    cleaned = _strip_string_literals(sql)
    for match in re.finditer(r"\b(ST_[A-Za-z0-9_]+)\s*\(", cleaned, re.I):
        name = match.group(1)
        start = match.end()
        depth = 1
        index = start
        while index < len(cleaned) and depth > 0:
            char = cleaned[index]
            if char == "(":
                depth += 1
            elif char == ")":
                depth -= 1
            index += 1
        if depth != 0:
            continue
        inner = cleaned[start : index - 1].strip()
        if not inner:
            count = 0
        else:
            count = 1
            level = 0
            for char in inner:
                if char == "(":
                    level += 1
                elif char == ")":
                    level -= 1
                elif char == "," and level == 0:
                    count += 1
        arg_counts.setdefault(name.lower(), []).append(count)
    return arg_counts


def _iter_spatial_function_calls(sql: str) -> Iterable[tuple[str, str]]:
    cleaned = _strip_string_literals(sql)
    for match in re.finditer(r"\b(ST_[A-Za-z0-9_]+)\s*\(", cleaned, re.I):
        name = match.group(1)
        start = match.end()
        depth = 1
        index = start
        while index < len(cleaned) and depth > 0:
            char = cleaned[index]
            if char == "(":
                depth += 1
            elif char == ")":
                depth -= 1
            index += 1
        if depth != 0:
            continue
        yield name, cleaned[start : index - 1]


def _extract_condition_segments(sql: str) -> list[str]:
    cleaned = _strip_string_literals(sql)
    segments: list[str] = []
    join_pattern = re.compile(
        r"\bjoin\b.*?\bon\b(?P<condition>.*?)(?=\bjoin\b|\bwhere\b|\bgroup\s+by\b|\bhaving\b|\border\s+by\b|\blimit\b|\bunion\b|\bintersect\b|\bexcept\b|$)",
        re.I | re.S,
    )
    where_pattern = re.compile(
        r"\bwhere\b(?P<condition>.*?)(?=\bgroup\s+by\b|\bhaving\b|\border\s+by\b|\blimit\b|\bunion\b|\bintersect\b|\bexcept\b|$)",
        re.I | re.S,
    )
    for pattern in (join_pattern, where_pattern):
        for match in pattern.finditer(cleaned):
            condition = match.group("condition").strip()
            if condition:
                segments.append(condition)
    return segments


def _referenced_tables_in_expression(expression: str, aliases: Mapping[str, str]) -> set[str]:
    referenced: set[str] = set()
    for qualifier, _column in re.findall(r"\b([a-zA-Z_][\w]*)\.([a-zA-Z_][\w]*)\b", expression):
        table_name = aliases.get(qualifier, qualifier)
        referenced.add(table_name)
    return referenced


def _detect_spatial_join_count(
    sql: str,
    aliases: Mapping[str, str],
) -> int:
    count = 0
    for segment in _extract_condition_segments(sql):
        for _name, args in _iter_spatial_function_calls(segment):
            if len(_referenced_tables_in_expression(args, aliases)) >= 2:
                count += 1
    return count


def _detect_difficulty_features(
    sql: str,
    detected_tables: Sequence[str],
    aliases: Mapping[str, str],
) -> dict[str, object]:
    lowered = sql.lower()
    subquery_count = len(re.findall(r"\(\s*select\b", lowered))
    cte_count = 1 if lowered.lstrip().startswith("with ") else 0
    select_count = len(re.findall(r"\bselect\b", lowered))
    return {
        "table_count": len(set(detected_tables)),
        "join_count": len(re.findall(r"\bjoin\b", lowered)),
        "spatial_join_count": _detect_spatial_join_count(sql, aliases),
        "has_group_by": " group by " in f" {lowered} ",
        "has_order_by": " order by " in f" {lowered} ",
        "has_limit": " limit " in f" {lowered} ",
        "has_cte": cte_count > 0,
        "cte_count": cte_count,
        "has_subquery": subquery_count > 0,
        "subquery_count": subquery_count,
        "has_exists": " exists " in f" {lowered} ",
        "has_set_operation": bool(re.search(r"\b(union|intersect|except)\b", lowered)),
        "select_count": select_count,
    }


def _extract_projection_features(sql: str) -> dict[str, object]:
    try:  # pragma: no cover - optional path
        expression = sqlglot.parse_one(sql, read="postgres")
        select_node = expression if isinstance(expression, exp.Select) else expression.find(exp.Select)
        if select_node is None:
            return {
                "select_expression_count": 0,
                "aggregate_projection_count": 0,
                "non_aggregate_projection_count": 0,
                "has_select_star": False,
                "has_distinct": False,
                "returns_geometry": False,
            }
        expressions = list(select_node.expressions)
        aggregate_projection_count = 0
        has_select_star = False
        returns_geometry = False
        for item in expressions:
            item_sql = item.sql(dialect="postgres").lower()
            if isinstance(item, exp.Star) or (
                item.find(exp.Star)
                and re.search(r"\bcount\s*\(\s*\*\s*\)", item_sql, re.I) is None
            ):
                has_select_star = True
            if re.search(r"\b(count|sum|avg|min|max)\s*\(", item_sql, re.I):
                aggregate_projection_count += 1
            if any(token in item_sql for token in ("geom", "geometry", "geography", "shape", "the_geom", "location")):
                returns_geometry = True
        return {
            "select_expression_count": len(expressions),
            "aggregate_projection_count": aggregate_projection_count,
            "non_aggregate_projection_count": max(len(expressions) - aggregate_projection_count, 0),
            "has_select_star": has_select_star,
            "has_distinct": select_node.args.get("distinct") is not None,
            "returns_geometry": returns_geometry,
        }
    except Exception:
        cleaned = _strip_string_literals(sql)
        match = re.search(r"^\s*select\s+(distinct\s+)?(.+?)\s+from\b", cleaned, re.I | re.S)
        if not match:
            return {
                "select_expression_count": 0,
                "aggregate_projection_count": 0,
                "non_aggregate_projection_count": 0,
                "has_select_star": False,
                "has_distinct": False,
                "returns_geometry": False,
            }
        expressions = _split_top_level_commas(match.group(2).strip())
        aggregate_projection_count = 0
        has_select_star = False
        returns_geometry = False
        for item in expressions:
            lowered = item.lower()
            if "*" in item and re.search(r"\bcount\s*\(\s*\*\s*\)", lowered, re.I) is None:
                has_select_star = True
            if re.search(r"\b(count|sum|avg|min|max)\s*\(", lowered, re.I):
                aggregate_projection_count += 1
            if any(token in lowered for token in ("geom", "geometry", "geography", "shape", "the_geom", "location")):
                returns_geometry = True
        return {
            "select_expression_count": len(expressions),
            "aggregate_projection_count": aggregate_projection_count,
            "non_aggregate_projection_count": max(len(expressions) - aggregate_projection_count, 0),
            "has_select_star": has_select_star,
            "has_distinct": bool(match.group(1)),
            "returns_geometry": returns_geometry,
        }


def _validate_error_coverage_profile(
    sql: str,
    error_coverage_profile: Mapping[str, object] | None,
) -> list[str]:
    profile = error_coverage_profile if isinstance(error_coverage_profile, Mapping) else {}
    profile_id = to_text(profile.get("profile_id")).lower()
    if not profile_id:
        return []

    errors: list[str] = []
    cleaned = _strip_string_literals(sql)
    detected_function_names = _detect_functions(cleaned)
    if bool(profile.get("forbid_spatial_functions")) and detected_function_names:
        errors.append(
            "Coverage rule: this attribute-only sample must not use ST_* functions: "
            + ", ".join(sorted(set(detected_function_names)))
            + "."
        )
    required_exact_function_names = {
        to_text(item)
        for item in profile.get("required_exact_function_names", []) or []
        if to_text(item)
    }
    if required_exact_function_names:
        detected_functions = {func.lower() for func in detected_function_names}
        missing_exact = sorted(
            function_name
            for function_name in required_exact_function_names
            if function_name.lower() not in detected_functions
        )
        if missing_exact:
            errors.append(
                "Coverage rule: this sample must use the exact target tail function(s): "
                + ", ".join(missing_exact)
                + "."
            )

    if profile_id == "spatialsql_geography_spheroid_measurement":
        if re.search(r"\bST_Transform\s*\(", cleaned, re.I):
            errors.append(
                "Coverage rule: geography measurement samples must not use ST_Transform."
            )
        geography_calls: list[str] = []
        missing_true: list[str] = []
        for function_name, args in _iter_spatial_function_calls(cleaned):
            lowered_name = function_name.lower()
            lowered_args = args.lower()
            if lowered_name not in {"st_area", "st_length", "st_distance"}:
                continue
            if "::geography" not in lowered_args:
                continue
            geography_calls.append(function_name)
            top_level_args = [item.strip().lower() for item in _split_top_level_commas(args)]
            if len(top_level_args) < 2 or top_level_args[-1] != "true":
                missing_true.append(function_name)
        if not geography_calls:
            errors.append(
                "Coverage rule: geography measurement samples must use ST_Area/ST_Length/ST_Distance on a geography cast."
            )
        if missing_true:
            errors.append(
                "Coverage rule: geography measurement samples must use the explicit spheroid=true signature for "
                + ", ".join(sorted(set(missing_true)))
                + "."
            )
        return errors

    if profile_id == "geography_scaled_measurement_output":
        has_geography_measurement = False
        has_scaling = re.search(
            r"/\s*(1000(?:\.0+)?|1000000(?:\.0+)?)\b|/\s*1e6\b|\*\s*0\.001\b|\*\s*0\.000001\b",
            cleaned,
            re.I,
        ) is not None
        for function_name, args in _iter_spatial_function_calls(cleaned):
            if function_name.lower() not in {"st_area", "st_length", "st_distance"}:
                continue
            if "::geography" in args.lower():
                has_geography_measurement = True
                break
        if not has_geography_measurement or not has_scaling:
            errors.append(
                "Coverage rule: geography-scaled samples must measure a geography cast and apply /1000 or /1000000-style scaling."
            )
        if re.search(r"\bST_Transform\s*\(", cleaned, re.I):
            errors.append(
                "Coverage rule: geography-scaled samples must not use ST_Transform."
            )
        return errors

    if profile_id == "spatialqueryqa_projected_units_measurement":
        if "::geography" in cleaned.lower():
            errors.append(
                "Coverage rule: projected-unit measurement samples must not use geography casts."
            )
        has_projected_measurement = False
        for function_name, args in _iter_spatial_function_calls(cleaned):
            if function_name.lower() not in {"st_area", "st_length", "st_distance"}:
                continue
            if re.search(r"\bST_Transform\s*\(", args, re.I):
                has_projected_measurement = True
                break
        if not has_projected_measurement:
            errors.append(
                "Coverage rule: projected-unit measurement samples must measure ST_Transform(...) output."
            )
        return errors

    if profile_id == "unit_scaled_measurement_output":
        has_measurement = any(
            function_name.lower() in {"st_area", "st_length", "st_distance"}
            for function_name, _args in _iter_spatial_function_calls(cleaned)
        )
        has_scaling = re.search(
            r"/\s*(1000(?:\.0+)?|1000000(?:\.0+)?)\b|/\s*1e6\b|\*\s*0\.001\b|\*\s*0\.000001\b",
            cleaned,
            re.I,
        ) is not None
        if not has_measurement or not has_scaling:
            errors.append(
                "Coverage rule: unit-scaled measurement samples must convert measurement output with /1000 or /1000000-style scaling."
            )
        return errors

    if profile_id == "direct_geometry_area_validity":
        lowered = cleaned.lower()
        if "st_area" not in lowered or "st_isvalid" not in lowered:
            errors.append(
                "Coverage rule: direct geometry area samples must use ST_Area together with ST_IsValid."
            )
        if "::geography" in lowered or re.search(r"\bST_Transform\s*\(", cleaned, re.I):
            errors.append(
                "Coverage rule: direct geometry area samples must not use geography casts or ST_Transform."
            )
        return errors

    if profile_id == "attribute_only_aggregate_output":
        upper = cleaned.upper()
        has_aggregate = re.search(r"\b(COUNT|SUM|AVG|MIN|MAX)\s*\(", cleaned, re.I) is not None
        if "JOIN" in upper:
            errors.append(
                "Coverage rule: attribute-only scalar samples should not add joins."
            )
        if not has_aggregate:
            errors.append(
                "Coverage rule: attribute-only scalar samples should use a scalar aggregate."
            )
        return errors

    if profile_id == "attribute_join_grouped_distinct_output":
        upper = cleaned.upper()
        if "JOIN" not in upper:
            errors.append(
                "Coverage rule: attribute-join samples must use at least one ordinary JOIN."
            )
        if not (
            "DISTINCT" in upper
            or "GROUP BY" in upper
            or re.search(r"\b(COUNT|SUM|AVG|MIN|MAX)\s*\(", cleaned, re.I)
        ):
            errors.append(
                "Coverage rule: attribute-join samples must use DISTINCT, GROUP BY, or an aggregate."
            )
        return errors

    if profile_id == "bbox_extent_accessor_tail":
        if not re.search(r"\bST_(XMIN|XMAX|YMIN|YMAX)\s*\(", cleaned, re.I):
            errors.append(
                "Coverage rule: extent-accessor tail samples must use ST_XMin, ST_XMax, ST_YMin, or ST_YMax."
            )
        return errors

    if profile_id == "point_coordinate_accessor_tail":
        if not re.search(r"\bST_(X|Y)\s*\(", cleaned, re.I):
            errors.append(
                "Coverage rule: point-coordinate tail samples must use ST_X or ST_Y."
            )
        return errors

    if profile_id == "geometry_metadata_tail":
        if not re.search(r"\bST_(SRID|AsText|GeometryType)\s*\(", cleaned, re.I):
            errors.append(
                "Coverage rule: geometry-metadata tail samples must use ST_SRID, ST_AsText, or ST_GeometryType."
            )
        return errors

    if profile_id == "buffered_relation_tail":
        has_buffer = re.search(r"\bST_Buffer\s*\(", cleaned, re.I) is not None
        has_relation = re.search(r"\bST_(Within|Intersects|Contains)\s*\(", cleaned, re.I) is not None
        if not has_buffer or not has_relation:
            errors.append(
                "Coverage rule: buffered-relation tail samples must use ST_Buffer together with one spatial relation."
            )
        return errors

    if profile_id == "collection_union_tail":
        has_collection = re.search(r"\bST_(Collect|UnaryUnion|CollectionExtract|IsEmpty)\s*\(", cleaned, re.I) is not None
        has_staging = cleaned.upper().lstrip().startswith("WITH ") or re.search(r"\(\s*SELECT\b", cleaned, re.I) is not None
        if not has_collection or not has_staging:
            errors.append(
                "Coverage rule: collection tail samples must stage ST_Collect, ST_UnaryUnion, ST_CollectionExtract, or ST_IsEmpty in one CTE or subquery."
            )
        return errors

    if profile_id == "crosses_predicate_tail":
        if not re.search(r"\bST_Crosses\s*\(", cleaned, re.I):
            errors.append(
                "Coverage rule: crosses tail samples must use ST_Crosses as the spatial predicate."
            )
        return errors

    if profile_id == "repair_valid_intersection_tail":
        has_makevalid = re.search(r"\bST_MakeValid\s*\(", cleaned, re.I) is not None
        has_overlap_measure = re.search(r"\bST_(Intersection|Area)\s*\(", cleaned, re.I) is not None
        if not has_makevalid or not has_overlap_measure:
            errors.append(
                "Coverage rule: repair tail samples must use ST_MakeValid before an overlap measurement path."
            )
        return errors

    if profile_id == "perimeter_measurement_tail":
        if not re.search(r"\bST_Perimeter\s*\(", cleaned, re.I):
            errors.append(
                "Coverage rule: perimeter tail samples must use ST_Perimeter."
            )
        return errors

    if profile_id == "floodsql_valid_geometry_measurement":
        if not re.search(r"\bST_IsValid\s*\(", cleaned, re.I):
            errors.append(
                "Coverage rule: valid-geometry samples must filter with ST_IsValid(...) before geometry measurement or spatial join."
            )
        return errors

    if profile_id == "aggregation_output_shape":
        if not re.search(r"\b(COUNT|SUM|AVG|MIN|MAX)\s*\(", cleaned, re.I) and "GROUP BY" not in cleaned.upper():
            errors.append(
                "Coverage rule: aggregate-shape samples must use an aggregate expression or GROUP BY."
            )
        return errors

    if profile_id == "distinct_grouped_result_shape":
        if "DISTINCT" not in cleaned.upper() and "GROUP BY" not in cleaned.upper():
            errors.append(
                "Coverage rule: distinct/grouped samples must use DISTINCT or GROUP BY."
            )
        return errors

    if profile_id == "offset_ranked_result":
        upper = cleaned.upper()
        if "ORDER BY" not in upper or "LIMIT" not in upper or "OFFSET" not in upper:
            errors.append(
                "Coverage rule: nth-result samples must use ORDER BY, LIMIT, and OFFSET together."
            )
        elif not re.search(r"\bOFFSET\s+[1-9]\d*\b", upper, re.I):
            errors.append(
                "Coverage rule: nth-result samples must use a positive OFFSET."
            )
        return errors

    if profile_id == "ranked_limit_output_shape":
        upper = cleaned.upper()
        if "ORDER BY" not in upper or "LIMIT" not in upper:
            errors.append(
                "Coverage rule: ranked samples must use ORDER BY and LIMIT together."
            )
        if re.search(r"\b(COUNT|SUM|AVG|MIN|MAX)\s*\(", cleaned, re.I) and "GROUP BY" not in upper:
            errors.append(
                "Coverage rule: ranked samples must not add LIMIT to scalar aggregate queries."
            )
        return errors

    if profile_id == "nested_cte_subquery_shape":
        upper = cleaned.upper()
        has_cte = upper.lstrip().startswith("WITH ")
        has_subquery = re.search(r"\(\s*SELECT\b", cleaned, re.I) is not None
        if not has_cte and not has_subquery:
            errors.append(
                "Coverage rule: nested-shape samples must include one CTE or one nested subquery."
            )
        return errors

    return errors


def _difficulty_matches(
    target: str,
    features: Mapping[str, object],
    error_coverage_profile: Mapping[str, object] | None = None,
) -> tuple[bool, str]:
    table_count = int(features.get("table_count", 0))
    join_count = int(features.get("join_count", 0))
    spatial_join_count = int(features.get("spatial_join_count", 0))
    has_group_by = bool(features.get("has_group_by"))
    has_cte = bool(features.get("has_cte"))
    cte_count = int(features.get("cte_count", 0))
    has_subquery = bool(features.get("has_subquery"))
    subquery_count = int(features.get("subquery_count", 0))
    has_set_operation = bool(features.get("has_set_operation"))
    select_count = int(features.get("select_count", 0))
    if isinstance(error_coverage_profile, Mapping) and bool(error_coverage_profile.get("allow_no_spatial_functions")):
        if has_set_operation:
            return False, "Attribute-only coverage samples should avoid UNION/INTERSECT/EXCEPT."
        if target == "easy":
            if table_count != 1 or join_count > 0 or has_cte or has_subquery:
                return False, "Easy attribute-only samples must stay single-table and flat."
            return True, ""
        if target == "medium":
            if table_count < 1 or table_count > 2 or join_count > 1 or has_cte or has_subquery:
                return False, "Medium attribute-only samples should use one or two tables without nested structure."
            return True, ""
        if target == "hard":
            if table_count < 1 or table_count > 3 or join_count > 2 or has_cte or has_subquery:
                return False, "Hard attribute-only samples should use up to three tables and stay flat."
            return True, ""
        if target == "extra-hard":
            if table_count < 1 or table_count > 4 or select_count > 5:
                return False, "Extra-hard attribute-only samples should stay bounded to one to four tables."
            return True, ""
    if target == "easy":
        if table_count != 1:
            return False, "Easy queries must use exactly one table."
        if join_count > 0 or spatial_join_count > 0 or has_group_by or has_cte or has_subquery:
            return False, "Easy queries must stay a single-table spatial filter or lookup without joins, GROUP BY, CTEs, or subqueries."
        return True, ""
    if target == "medium":
        if table_count != 2:
            return False, "Medium queries must use exactly two tables."
        if spatial_join_count != 1:
            return False, "Medium queries must contain exactly one spatial join between the two tables."
        if join_count > 1:
            return False, "Medium queries should not contain more than one join."
        if has_cte or has_subquery or has_set_operation:
            return False, "Medium queries should stay flat and avoid subqueries, CTEs, and set operations."
        return True, ""
    if target == "hard":
        if table_count != 3:
            return False, "Hard queries must use exactly three tables."
        if spatial_join_count != 2:
            return False, "Hard queries must contain exactly two spatial joins across the three tables."
        if has_cte or has_subquery or has_set_operation:
            return False, "Hard queries should stay flat and avoid nested subqueries, CTEs, and set operations."
        return True, ""
    if target == "extra-hard":
        if table_count < 3 or table_count > 4:
            return False, "Extra-hard queries must use between three and four tables."
        if spatial_join_count < 1:
            return False, "Extra-hard queries must include at least one spatial join."
        nested_query_count = cte_count + subquery_count
        advanced_op_count = spatial_join_count + nested_query_count
        if advanced_op_count < 2 or advanced_op_count > 4:
            return False, "Extra-hard queries must keep spatial joins plus nested queries between two and four operations in total."
        if has_set_operation:
            return False, "Extra-hard queries should avoid UNION/INTERSECT/EXCEPT so the SQL stays executable."
        if select_count > 5:
            return False, "Extra-hard queries should keep the structure bounded to avoid over-complex SQL."
        return True, ""
    return True, ""


class SQLValidator:
    def __init__(self, function_library: PostGISFunctionLibrary):
        self.function_library = function_library

    def validate(
        self,
        *,
        sql: str,
        database: SynthesizedSpatialDatabase,
        sampled_functions: Sequence[str],
        difficulty_level: str,
        database_runtime_metadata: Mapping[str, object] | None = None,
        error_coverage_profile: Mapping[str, object] | None = None,
        expected_limit: int | None = None,
        allow_limit: bool = True,
        require_order_by_with_limit: bool = False,
    ) -> SQLValidationResult:
        sql_text = to_text(sql)
        errors: list[str] = []
        warnings: list[str] = []
        if not sql_text:
            errors.append("SQL is empty.")
            return SQLValidationResult(is_valid=False, errors=errors)

        statements = _split_sql_statements(sql_text)
        if len(statements) != 1:
            errors.append("SQL must contain exactly one statement.")
        if contains_dangerous_sql(sql_text):
            errors.append("SQL contains dangerous or non-read-only operations.")
        if not re.match(r"^\s*(select|with)\b", sql_text, re.I):
            errors.append("SQL must be a SELECT or WITH query.")

        detected_tables: list[str]
        detected_columns: list[str]
        aliases: dict[str, str]
        if sqlglot is not None:
            detected_tables, detected_columns, aliases = self._validate_with_sqlglot(sql_text, warnings)
        else:
            detected_tables, aliases = _detect_tables_regex(sql_text)
            detected_columns = _detect_columns_regex(sql_text, aliases)

        allowed_tables, allowed_columns = _build_allowed_schema(database, database_runtime_metadata)
        unknown_tables = [table for table in detected_tables if table not in allowed_tables]
        if unknown_tables:
            errors.append(f"Unknown tables referenced: {', '.join(sorted(set(unknown_tables)))}")

        unknown_columns = [column for column in detected_columns if column not in allowed_columns["*"]]
        if unknown_columns:
            errors.append(f"Unknown columns referenced: {', '.join(sorted(set(unknown_columns)))}")

        detected_functions = _detect_functions(sql_text)
        allow_no_spatial_functions = (
            isinstance(error_coverage_profile, Mapping)
            and bool(error_coverage_profile.get("allow_no_spatial_functions"))
        )
        if not detected_functions and not allow_no_spatial_functions:
            errors.append("SQL does not use any PostGIS ST_* function.")
        sampled_lower = {name.lower() for name in sampled_functions}
        if difficulty_level in {"medium", "hard", "extra-hard"}:
            sampled_lower.update(name.lower() for name in fixed_spatial_join_function_names())
        if (
            sampled_lower
            and not allow_no_spatial_functions
            and not any(func.lower() in sampled_lower for func in detected_functions)
        ):
            errors.append("SQL does not use any of the sampled required spatial functions.")
        if sampled_lower:
            unexpected_functions = sorted(
                {
                    func
                    for func in detected_functions
                    if func.lower() not in sampled_lower
                }
            )
            if unexpected_functions:
                errors.append(
                    "SQL uses PostGIS functions outside the externally provided candidate set: "
                    + ", ".join(unexpected_functions)
                )

        errors.extend(_validate_error_coverage_profile(sql_text, error_coverage_profile))

        raster_topology = [
            func for func in detected_functions
            if any(token in func.lower() for token in ("raster", "topology"))
        ]
        if raster_topology:
            errors.append(f"Raster/topology functions are not allowed: {', '.join(raster_topology)}")

        arg_counts = _function_call_arg_counts(sql_text)
        for function_name, observed_counts in arg_counts.items():
            signatures = self.function_library.get_function_signatures(function_name)
            if not signatures:
                warnings.append(f"Function {function_name} is not present in the filtered PostGIS library.")
                continue
            allowed_counts = {
                len(item.input_args)
                for item in signatures
                if item.input_args
            }
            if allowed_counts and any(count not in allowed_counts for count in observed_counts):
                errors.append(
                    f"Function {function_name} appears to use an incompatible number of arguments."
                )

        difficulty_features = _detect_difficulty_features(sql_text, detected_tables, aliases)
        projection_features = _extract_projection_features(sql_text)
        difficulty_features.update(projection_features)
        difficulty_ok, difficulty_message = _difficulty_matches(
            difficulty_level,
            difficulty_features,
            error_coverage_profile=error_coverage_profile,
        )
        if not difficulty_ok:
            errors.append(difficulty_message)

        if projection_features["has_select_star"]:
            errors.append("SQL must not use SELECT * or table.* projections.")
        if int(projection_features["select_expression_count"]) > 3:
            errors.append("SQL must project at most three output expressions.")
        uses_aggregate_or_group_by = (
            int(projection_features["aggregate_projection_count"]) > 0
            or bool(difficulty_features.get("has_group_by"))
        )
        if difficulty_features.get("has_limit"):
            limit_match = re.search(r"\blimit\s+(\d+)", sql_text, re.I)
            if limit_match:
                limit_number = int(limit_match.group(1))
                if limit_number < 1 or limit_number > 5:
                    errors.append("SQL LIMIT must stay between 1 and 5.")
                if not allow_limit:
                    errors.append("This sample should not use LIMIT.")
                if uses_aggregate_or_group_by and int(projection_features["aggregate_projection_count"]) > 0 and not bool(difficulty_features.get("has_group_by")):
                    errors.append("Scalar aggregate queries must not use LIMIT.")
                if require_order_by_with_limit and not bool(difficulty_features.get("has_order_by")):
                    errors.append("LIMIT queries for this sample must include ORDER BY.")
                if expected_limit is not None and limit_number != int(expected_limit):
                    errors.append(
                        f"SQL LIMIT must equal the sampled bounded row cap {int(expected_limit)}."
                    )
        elif expected_limit is not None:
            errors.append(f"This sample must use ORDER BY with LIMIT {int(expected_limit)}.")
        if (
            int(projection_features["aggregate_projection_count"]) > 0
            and not bool(difficulty_features.get("has_group_by"))
            and int(projection_features["select_expression_count"]) != 1
        ):
            errors.append("Scalar aggregate queries must project exactly one expression.")

        return SQLValidationResult(
            is_valid=not errors,
            errors=errors,
            warnings=warnings,
            detected_tables=sorted(set(detected_tables)),
            detected_columns=sorted(set(detected_columns)),
            detected_spatial_functions=sorted(set(detected_functions)),
            detected_difficulty_features=difficulty_features,
        )

    @staticmethod
    def _validate_with_sqlglot(sql_text: str, warnings: list[str]) -> tuple[list[str], list[str], dict[str, str]]:
        tables: list[str] = []
        columns: list[str] = []
        aliases: dict[str, str] = {}
        try:  # pragma: no cover - optional path
            expression = sqlglot.parse_one(sql_text, read="postgres")
            for table in expression.find_all(exp.Table):
                table_name = to_text(table.name)
                if table_name:
                    tables.append(table_name)
                    alias = to_text(table.alias)
                    if alias:
                        aliases[alias] = table_name
            for column in expression.find_all(exp.Column):
                column_name = to_text(column.name)
                if column_name:
                    columns.append(column_name)
        except Exception as exc:  # pragma: no cover
            warnings.append(f"sqlglot parsing failed; falling back to regex validation: {exc}")
            tables, aliases = _detect_tables_regex(sql_text)
            columns = _detect_columns_regex(sql_text, aliases)
        return tables, columns, aliases
