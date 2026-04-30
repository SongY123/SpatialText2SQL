"""SQL 方言转换工具：SpatiaLite / DuckDB -> PostGIS。"""
from __future__ import annotations

import re
from typing import Dict, Iterable, List, Optional, Tuple


FUNCTION_REPLACEMENTS: List[Tuple[re.Pattern, str]] = [
    (re.compile(r"\bIntersects\s*\(", re.I), "ST_Intersects("),
    (re.compile(r"\bIntersection\s*\(", re.I), "ST_Intersection("),
    (re.compile(r"\bSRID\s*\(", re.I), "ST_SRID("),
    (re.compile(r"\bGLength\s*\(", re.I), "ST_Length("),
    (re.compile(r"\bArea\s*\(", re.I), "ST_Area("),
    (re.compile(r"\bGeomFromText\s*\(", re.I), "ST_GeomFromText("),
    (re.compile(r"\bAsText\s*\(", re.I), "ST_AsText("),
    (re.compile(r"\bDistance\s*\(", re.I), "ST_Distance("),
    (re.compile(r"\bBuffer\s*\(", re.I), "ST_Buffer("),
    (re.compile(r"\bConvexHull\s*\(", re.I), "ST_ConvexHull("),
    (re.compile(r"\bCentroid\s*\(", re.I), "ST_Centroid("),
    (re.compile(r"\bContains\s*\(", re.I), "ST_Contains("),
    (re.compile(r"\bWithin\s*\(", re.I), "ST_Within("),
    (re.compile(r"\bTouches\s*\(", re.I), "ST_Touches("),
    (re.compile(r"\bOverlaps\s*\(", re.I), "ST_Overlaps("),
    (re.compile(r"\bCrosses\s*\(", re.I), "ST_Crosses("),
    (re.compile(r"\bDisjoint\s*\(", re.I), "ST_Disjoint("),
    (re.compile(r"\bUnion\s*\(", re.I), "ST_Union("),
    (re.compile(r"\bDifference\s*\(", re.I), "ST_Difference("),
    (re.compile(r"\bSymDifference\s*\(", re.I), "ST_SymDifference("),
    (re.compile(r"\bBoundary\s*\(", re.I), "ST_Boundary("),
    (re.compile(r"\bStartPoint\s*\(", re.I), "ST_StartPoint("),
    (re.compile(r"\bEndPoint\s*\(", re.I), "ST_EndPoint("),
    (re.compile(r"\bPointN\s*\(", re.I), "ST_PointN("),
    (re.compile(r"\bNumPoints\s*\(", re.I), "ST_NPoints("),
    (re.compile(r"\bNumGeometries\s*\(", re.I), "ST_NumGeometries("),
    (re.compile(r"\bGeometryN\s*\(", re.I), "ST_GeometryN("),
    (re.compile(r"\bX\s*\(", re.I), "ST_X("),
    (re.compile(r"\bY\s*\(", re.I), "ST_Y("),
    (re.compile(r"\bTransform\s*\(", re.I), "ST_Transform("),
    (re.compile(r"\bExtent\s*\(", re.I), "ST_Extent("),
    (re.compile(r"\bMbrMinX\s*\(", re.I), "ST_XMin("),
    (re.compile(r"\bMbrMaxX\s*\(", re.I), "ST_XMax("),
    (re.compile(r"\bMbrMinY\s*\(", re.I), "ST_YMin("),
    (re.compile(r"\bMbrMaxY\s*\(", re.I), "ST_YMax("),
]

BOOLEAN_FUNCTION_NAMES = {
    "ST_Intersects",
    "ST_Contains",
    "ST_Within",
    "ST_Touches",
    "ST_Overlaps",
    "ST_Crosses",
    "ST_Disjoint",
}

AGGREGATE_PATTERN = re.compile(r"\b(count|sum|avg|min|max)\s*\(", re.I)
DATASET_PREFIX_PATTERN = re.compile(r"\bdataset\d+_[a-z]+_([A-Za-z_][A-Za-z0-9_]*)\b")
TABLE_CASE_MAP = {
    "scenicspots": "scenicSpots",
    "gdps": "GDPs",
}
KNOWN_FUNCTION_NAMES = {
    "ST_Intersects",
    "ST_Intersection",
    "ST_Length",
    "ST_Area",
    "ST_GeomFromText",
    "ST_AsText",
    "ST_Distance",
    "ST_Buffer",
    "ST_ConvexHull",
    "ST_Centroid",
    "ST_Contains",
    "ST_Within",
    "ST_Touches",
    "ST_Overlaps",
    "ST_Crosses",
    "ST_Disjoint",
    "ST_Union",
    "ST_Difference",
    "ST_SymDifference",
    "ST_Boundary",
    "ST_StartPoint",
    "ST_EndPoint",
    "ST_PointN",
    "ST_NPoints",
    "ST_NumGeometries",
    "ST_GeometryN",
    "ST_SRID",
    "ST_X",
    "ST_Y",
    "ST_XMin",
    "ST_XMax",
    "ST_YMin",
    "ST_YMax",
    "ST_Transform",
    "ST_Extent",
}
SQL_KEYWORD_FUNCTIONS = {
    "Select",
    "Count",
    "Sum",
    "Avg",
    "Min",
    "Max",
    "Inner",
    "Outer",
    "Left",
    "Right",
    "On",
    "Where",
    "From",
    "Group",
    "Order",
    "By",
    "As",
    "And",
    "Or",
    "Not",
    "NOT",
    "In",
    "Like",
    "Between",
    "Case",
    "When",
    "Then",
    "Else",
    "End",
    "Cast",
    "Null",
    "True",
    "False",
    "Upper",
    "Lower",
    "Trim",
    "Substring",
    "Replace",
    "Coalesce",
}


def canonicalize_table_name(table_name: str) -> str:
    """规范化已知业务表名的大小写。"""
    if not table_name:
        return table_name

    dataset_match = re.match(r"^(dataset\d+_[a-z]+_)(.+)$", table_name, re.I)
    if dataset_match:
        prefix, base = dataset_match.groups()
        return prefix.lower() + TABLE_CASE_MAP.get(base.lower(), base)
    return TABLE_CASE_MAP.get(table_name.lower(), table_name)


def add_table_prefix(sql: str, prefix: str) -> str:
    """为 SQL 中的表名添加统一 split 前缀。"""
    if not prefix:
        return sql

    pattern = re.compile(
        r"\b(FROM|JOIN|from|join)\s+([a-zA-Z_][a-zA-Z0-9_]*)\b",
    )

    def replace_table(match: re.Match) -> str:
        keyword = match.group(1)
        table = canonicalize_table_name(match.group(2))
        prefixed_match = re.match(r"^dataset\d+_[a-z]+_(.+)$", table, re.I)
        base_table = prefixed_match.group(1) if prefixed_match else table
        return f"{keyword} {prefix}{canonicalize_table_name(base_table)}"

    return pattern.sub(replace_table, sql)


def classify_spatialsql_failure(
    error_message: Optional[str] = None,
    issues: Optional[Iterable[str]] = None,
) -> str:
    """根据错误消息或规则告警归类失败。"""
    text = (error_message or "").lower()
    issue_text = " ".join(issues or []).lower()
    merged = f"{text} {issue_text}".strip()

    if not merged:
        return "sql_rule_gap"
    if "unknown wkb type" in merged or "parse error - invalid geometry" in merged:
        return "data_geometry_error"
    if "must appear in the group by" in merged or "aggregate" in merged:
        return "sql_aggregate_error"
    if "does not exist" in merged or "unknown column" in merged or "no such column" in merged:
        return "sql_mapping_error"
    if "possible unconverted function" in merged or "syntax error" in merged:
        return "sql_rule_gap"
    return "semantic_mismatch"


def _normalize_geometry_columns(sql: str) -> str:
    sql = re.sub(r"\bShape\b", "shape", sql)
    sql = re.sub(r"\bLocation\b", "location", sql)
    return sql


def _normalize_existing_split_prefixes(sql: str, table_prefix: Optional[str]) -> str:
    if not table_prefix:
        return sql

    def replace_prefix(match: re.Match) -> str:
        return table_prefix + canonicalize_table_name(match.group(1))

    return DATASET_PREFIX_PATTERN.sub(replace_prefix, sql)


def _collect_table_tokens(sql: str) -> Dict[str, str]:
    mapping: Dict[str, str] = {}
    pattern = re.compile(
        r"\b(?:FROM|JOIN)\s+([a-zA-Z_][a-zA-Z0-9_]*)(?:\s+(?:AS\s+)?([a-zA-Z_][a-zA-Z0-9_]*))?",
        re.I,
    )
    for match in pattern.finditer(sql):
        token = canonicalize_table_name(match.group(1))
        base = re.sub(r"^dataset\d+_[a-z]+_", "", token, flags=re.I)
        mapping.setdefault(base.lower(), token)
    return mapping


def _qualify_base_table_references(sql: str, table_tokens: Dict[str, str]) -> str:
    for base, token in table_tokens.items():
        sql = re.sub(rf"\b{re.escape(base)}\.", f"{token}.", sql, flags=re.I)
    return sql


def _find_matching_paren(text: str, open_idx: int) -> int:
    depth = 0
    in_single = False
    in_double = False
    for idx in range(open_idx, len(text)):
        ch = text[idx]
        if ch == "'" and not in_double:
            in_single = not in_single
            continue
        if ch == '"' and not in_single:
            in_double = not in_double
            continue
        if in_single or in_double:
            continue
        if ch == "(":
            depth += 1
        elif ch == ")":
            depth -= 1
            if depth == 0:
                return idx
    return -1


def _split_top_level_args(arg_text: str) -> List[str]:
    args: List[str] = []
    buf: List[str] = []
    depth = 0
    in_single = False
    in_double = False
    for ch in arg_text:
        if ch == "'" and not in_double:
            in_single = not in_single
        elif ch == '"' and not in_single:
            in_double = not in_double
        elif not in_single and not in_double:
            if ch == "(":
                depth += 1
            elif ch == ")":
                depth -= 1
            elif ch == "," and depth == 0:
                args.append("".join(buf).strip())
                buf = []
                continue
        buf.append(ch)
    if buf:
        args.append("".join(buf).strip())
    return args


def _rewrite_function_calls(sql: str, function_name: str, rewriter) -> str:
    pattern = re.compile(rf"\b{re.escape(function_name)}\s*\(", re.I)
    cursor = 0
    parts: List[str] = []
    while True:
        match = pattern.search(sql, cursor)
        if match is None:
            parts.append(sql[cursor:])
            break
        open_idx = sql.find("(", match.start())
        close_idx = _find_matching_paren(sql, open_idx)
        if open_idx < 0 or close_idx < 0:
            parts.append(sql[cursor:])
            break

        parts.append(sql[cursor:match.start()])
        args = _split_top_level_args(sql[open_idx + 1 : close_idx])
        replacement = rewriter(match.group(0).split("(")[0], args)
        parts.append(replacement)
        cursor = close_idx + 1
    return "".join(parts)


def _wrap_geography_expression(expr: str) -> str:
    expr = expr.strip()
    if not expr:
        return expr
    if "::geography" in expr.lower():
        return expr
    if re.fullmatch(r"[A-Za-z_][A-Za-z0-9_\.]*", expr):
        return f"{expr}::geography"
    if expr.endswith(")") and "(" in expr:
        return f"{expr}::geography"
    return f"({expr})::geography"


def _rewrite_measurement_calls(sql: str) -> str:
    def rewriter(func_name: str, args: List[str]) -> str:
        normalized_name = func_name.upper()
        if normalized_name in {"ST_LENGTH", "ST_AREA"} and len(args) == 2:
            flag = args[1].strip().lower()
            if flag in {"1", "true"}:
                return f"{func_name}({_wrap_geography_expression(args[0])}, true)"
        return f"{func_name}({', '.join(args)})"

    sql = _rewrite_function_calls(sql, "ST_Length", rewriter)
    sql = _rewrite_function_calls(sql, "ST_Area", rewriter)
    return sql


def _rewrite_distance_calls(sql: str) -> str:
    def rewriter(func_name: str, args: List[str]) -> str:
        if len(args) == 3 and args[2].strip().lower() in {"1", "true"}:
            return (
                f"{func_name}("
                f"{_wrap_geography_expression(args[0])}, "
                f"{_wrap_geography_expression(args[1])}, true)"
            )
        return f"{func_name}({', '.join(args)})"

    return _rewrite_function_calls(sql, "ST_Distance", rewriter)


def _rewrite_duckdb_point_calls(sql: str) -> str:
    qualified_column_pattern = re.compile(
        r"([A-Za-z_][A-Za-z0-9_]*)\.([A-Za-z_][A-Za-z0-9_]*)$",
        re.I,
    )

    def rewriter(func_name: str, args: List[str]) -> str:
        if len(args) == 2:
            lon_match = qualified_column_pattern.fullmatch(args[0].strip())
            lat_match = qualified_column_pattern.fullmatch(args[1].strip())
            if (
                lon_match
                and lat_match
                and lon_match.group(1) == lat_match.group(1)
                and lon_match.group(2).lower() == "lon"
                and lat_match.group(2).lower() == "lat"
            ):
                return f"{lon_match.group(1)}.geometry"
            return f"ST_SetSRID({func_name}({args[0]}, {args[1]}), 4326)"
        return f"{func_name}({', '.join(args)})"

    return _rewrite_function_calls(sql, "ST_Point", rewriter)


def _rewrite_desc_order_by_nulls_last(sql: str) -> str:
    rebuilt_parts: List[str] = []
    cursor = 0
    while True:
        open_idx = sql.find("(", cursor)
        if open_idx < 0:
            rebuilt_parts.append(sql[cursor:])
            break
        close_idx = _find_matching_paren(sql, open_idx)
        if close_idx < 0:
            rebuilt_parts.append(sql[cursor:])
            break

        rebuilt_parts.append(sql[cursor:open_idx + 1])
        inner = sql[open_idx + 1 : close_idx]
        if re.search(r"\bselect\b", inner, re.I):
            rebuilt_parts.append(_rewrite_desc_order_by_nulls_last(inner))
        else:
            rebuilt_parts.append(inner)
        cursor = close_idx
    sql = "".join(rebuilt_parts)

    order_pos = _find_top_level_keyword(sql, "order by")
    if order_pos < 0:
        return sql

    order_end_candidates = [
        _find_top_level_keyword(sql, "limit", start=order_pos),
        _find_top_level_keyword(sql, "offset", start=order_pos),
    ]
    order_end_candidates = [idx for idx in order_end_candidates if idx >= 0]
    order_end = min(order_end_candidates) if order_end_candidates else len(sql)

    order_list = sql[order_pos + len("order by") : order_end].strip()
    if not order_list:
        return sql

    rewritten_items: List[str] = []
    changed = False
    for expression in _split_top_level_expressions(order_list):
        expr = expression.strip()
        if re.search(r"\bnulls\s+(first|last)\b", expr, re.I):
            rewritten_items.append(expr)
            continue
        if re.search(r"\bdesc\b", expr, re.I):
            rewritten_items.append(f"{expr} NULLS LAST")
            changed = True
            continue
        rewritten_items.append(expr)

    if not changed:
        return sql

    rewritten_order = ", ".join(rewritten_items)
    suffix = sql[order_end:]
    if suffix and not suffix.startswith(" "):
        suffix = " " + suffix
    return f"{sql[:order_pos]}ORDER BY {rewritten_order}{suffix}"


def _rewrite_boolean_comparisons(sql: str) -> str:
    for function_name in sorted(BOOLEAN_FUNCTION_NAMES):
        sql = _rewrite_single_boolean_comparison(sql, function_name)
    return sql


def _rewrite_single_boolean_comparison(sql: str, function_name: str) -> str:
    pattern = re.compile(rf"\b{re.escape(function_name)}\s*\(", re.I)
    cursor = 0
    parts: List[str] = []
    while True:
        match = pattern.search(sql, cursor)
        if match is None:
            parts.append(sql[cursor:])
            break
        open_idx = sql.find("(", match.start())
        close_idx = _find_matching_paren(sql, open_idx)
        if open_idx < 0 or close_idx < 0:
            parts.append(sql[cursor:])
            break

        parts.append(sql[cursor:match.start()])
        call_sql = sql[match.start() : close_idx + 1]
        tail = sql[close_idx + 1 :]
        equal_match = re.match(r"\s*=\s*([01])", tail, re.I)
        if equal_match is None:
            parts.append(call_sql)
            cursor = close_idx + 1
            continue

        flag = equal_match.group(1)
        if flag == "1":
            parts.append(call_sql)
        else:
            parts.append(f"NOT ({call_sql})")
        cursor = close_idx + 1 + equal_match.end()
    return "".join(parts)


def _fix_missing_join_on(sql: str) -> str:
    pattern = re.compile(
        r"(\binner\s+join\s+[a-zA-Z_][a-zA-Z0-9_]*(?:\s+[a-zA-Z_][a-zA-Z0-9_]*)?)"
        r"(?=\s+(?:inner\s+join|where|group\s+by|order\s+by|limit|offset)\b|\s*$)",
        re.I,
    )
    return pattern.sub(r"\1 on true", sql)


def _is_top_level_position(sql: str, position: int) -> bool:
    depth = 0
    in_single = False
    in_double = False
    for idx, ch in enumerate(sql):
        if idx >= position:
            break
        if ch == "'" and not in_double:
            in_single = not in_single
        elif ch == '"' and not in_single:
            in_double = not in_double
        elif not in_single and not in_double:
            if ch == "(":
                depth += 1
            elif ch == ")":
                depth = max(0, depth - 1)
    return depth == 0 and not in_single and not in_double


def _find_top_level_keyword(sql: str, keyword: str, start: int = 0) -> int:
    pattern = re.compile(
        r"\b" + r"\s+".join(re.escape(part) for part in keyword.split()) + r"\b",
        re.I,
    )
    for match in pattern.finditer(sql, start):
        if _is_top_level_position(sql, match.start()):
            return match.start()
    return -1


def _split_top_level_expressions(expr_text: str) -> List[str]:
    return [expr for expr in _split_top_level_args(expr_text) if expr.strip()]


def _remove_alias(expr: str) -> str:
    expr = expr.strip()
    expr = re.sub(r"\s+as\s+[A-Za-z_][A-Za-z0-9_]*\s*$", "", expr, flags=re.I)
    expr = re.sub(r"^\s*distinct\s+", "", expr, flags=re.I)
    return expr


def _maybe_qualify_simple_column(expr: str, sql: str) -> str:
    expr = expr.strip()
    if "." in expr or not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", expr):
        return expr

    table_tokens = list(_collect_table_tokens(sql).values())
    if len(table_tokens) == 1:
        return f"{table_tokens[0]}.{expr}"
    return expr


def _append_group_by(sql: str, group_fields: List[str]) -> str:
    insertion_points = [
        _find_top_level_keyword(sql, "order by"),
        _find_top_level_keyword(sql, "limit"),
        _find_top_level_keyword(sql, "offset"),
    ]
    insertion_points = [idx for idx in insertion_points if idx >= 0]
    insert_at = min(insertion_points) if insertion_points else len(sql)
    group_sql = f" GROUP BY {', '.join(group_fields)} "
    return f"{sql[:insert_at]}{group_sql}{sql[insert_at:]}"


def _fix_incomplete_group_by(sql: str) -> str:
    group_pos = _find_top_level_keyword(sql, "group by")
    if group_pos < 0:
        return sql

    select_pos = _find_top_level_keyword(sql, "select")
    from_pos = _find_top_level_keyword(sql, "from", start=max(0, select_pos))
    if select_pos < 0 or from_pos < 0:
        return sql

    group_end_candidates = [
        _find_top_level_keyword(sql, "order by", start=group_pos),
        _find_top_level_keyword(sql, "limit", start=group_pos),
        _find_top_level_keyword(sql, "offset", start=group_pos),
    ]
    group_end_candidates = [idx for idx in group_end_candidates if idx >= 0]
    group_end = min(group_end_candidates) if group_end_candidates else len(sql)

    select_list = sql[select_pos + len("select") : from_pos].strip()
    group_list = sql[group_pos + len("group by") : group_end].strip()
    if not select_list or not group_list:
        return sql

    required_fields: List[str] = []
    for expression in _split_top_level_expressions(select_list):
        plain_expr = _remove_alias(expression)
        if AGGREGATE_PATTERN.search(plain_expr):
            continue
        required_fields.append(_maybe_qualify_simple_column(plain_expr, sql))

    existing_fields = [
        _maybe_qualify_simple_column(expr.strip(), sql)
        for expr in _split_top_level_expressions(group_list)
    ]
    missing_fields = [field for field in required_fields if field and field not in existing_fields]
    if not missing_fields:
        return sql

    updated_group_list = ", ".join(existing_fields + missing_fields)
    suffix = sql[group_end:]
    if suffix and not suffix.startswith(" "):
        suffix = " " + suffix
    return f"{sql[:group_pos]}GROUP BY {updated_group_list}{suffix}"


def _fix_missing_group_by(sql: str) -> str:
    if _find_top_level_keyword(sql, "group by") >= 0:
        return sql

    select_pos = _find_top_level_keyword(sql, "select")
    from_pos = _find_top_level_keyword(sql, "from", start=max(0, select_pos))
    if select_pos < 0 or from_pos < 0:
        return sql

    select_list = sql[select_pos + len("select") : from_pos].strip()
    expressions = _split_top_level_expressions(select_list)
    if not expressions:
        return sql

    non_agg_fields: List[str] = []
    has_aggregate = False
    for expression in expressions:
        plain_expr = _remove_alias(expression)
        if AGGREGATE_PATTERN.search(plain_expr):
            has_aggregate = True
            continue
        non_agg_fields.append(_maybe_qualify_simple_column(plain_expr, sql))

    if has_aggregate and non_agg_fields:
        return _append_group_by(sql, non_agg_fields)
    return sql


def _fix_distance_alias_in_join(sql: str) -> str:
    select_pos = _find_top_level_keyword(sql, "select")
    from_pos = _find_top_level_keyword(sql, "from", start=max(0, select_pos))
    if select_pos < 0 or from_pos < 0:
        return sql

    alias_map: Dict[str, str] = {}
    select_list = sql[select_pos + len("select") : from_pos]
    for expression in _split_top_level_expressions(select_list):
        match = re.search(
            r"(.+?)\s+as\s+([A-Za-z_][A-Za-z0-9_]*)\s*$",
            expression.strip(),
            re.I,
        )
        if match:
            alias_map[match.group(2)] = match.group(1).strip()

    for alias, expression in alias_map.items():
        sql = re.sub(
            rf"\bOn\s+{re.escape(alias)}\s*([<>=])",
            f"On {expression} \\1",
            sql,
            flags=re.I,
        )
    return sql


def _fix_known_missing_join_patterns(sql: str) -> str:
    pattern = re.compile(
        r"(\bfrom\s+([a-zA-Z_][a-zA-Z0-9_]*)\s+inner\s+join\s+([a-zA-Z_][a-zA-Z0-9_]*))\s+inner\s+join\s+([a-zA-Z_][a-zA-Z0-9_]*)\s+on",
        re.I,
    )
    match = pattern.search(sql)
    if not match:
        return sql

    left_table = match.group(2)
    mid_table = match.group(3)
    condition = None
    if left_table.lower().endswith("provinces") and mid_table.lower().endswith("cities"):
        condition = f" on ST_Contains({left_table}.shape, {mid_table}.shape)"
    if condition is None:
        return sql

    original = match.group(1)
    replacement = f"{original}{condition}"
    return sql[: match.start(1)] + replacement + sql[match.end(1) :]


def _collect_unconverted_functions(sql: str) -> List[str]:
    issues: List[str] = []
    remaining = re.findall(r"\b([A-Z][a-zA-Z0-9_]*)\s*\(", sql)
    for name in remaining:
        if name in KNOWN_FUNCTION_NAMES or name in SQL_KEYWORD_FUNCTIONS:
            continue
        if name.startswith("ST_"):
            continue
        issues.append(f"Possible unconverted function: {name}(...)")
    return issues


def convert_spatialite_to_postgis(
    sql: str,
    table_prefix: Optional[str] = None,
) -> Tuple[str, List[str]]:
    """将一条 SpatiaLite SQL 转为 PostGIS 方言。"""
    if not sql or not sql.strip():
        return sql, []

    converted = sql.strip()
    converted = _normalize_existing_split_prefixes(converted, table_prefix)
    converted = _normalize_geometry_columns(converted)
    for pattern, replacement in FUNCTION_REPLACEMENTS:
        converted = pattern.sub(replacement, converted)

    if table_prefix:
        converted = add_table_prefix(converted, table_prefix)

    table_tokens = _collect_table_tokens(converted)
    converted = _qualify_base_table_references(converted, table_tokens)
    converted = _rewrite_measurement_calls(converted)
    converted = _rewrite_distance_calls(converted)
    converted = _rewrite_boolean_comparisons(converted)
    converted = _fix_known_missing_join_patterns(converted)
    converted = _fix_missing_join_on(converted)
    converted = _fix_distance_alias_in_join(converted)
    converted = _fix_missing_group_by(converted)
    converted = _fix_incomplete_group_by(converted)
    converted = _rewrite_desc_order_by_nulls_last(converted)
    converted = re.sub(r"\s+", " ", converted).strip()

    issues = _collect_unconverted_functions(converted)
    return converted, issues


def convert_duckdb_to_postgis(
    sql: str,
    table_prefix: Optional[str] = None,
) -> Tuple[str, List[str]]:
    """将一条 FloodSQL 风格的 DuckDB SQL 转为 PostgreSQL/PostGIS 方言。"""
    if not sql or not sql.strip():
        return sql, []

    converted = sql.strip()
    issues: List[str] = []
    if table_prefix:
        converted = add_table_prefix(converted, table_prefix)

    strftime_pattern = re.compile(r"STRFTIME\s*\(\s*'(%[A-Za-z])'\s*,\s*(.*?)\)", re.I)

    def rewrite_strftime(match: re.Match) -> str:
        fmt = match.group(1).upper()
        expr = match.group(2).strip()
        if fmt == "%Y":
            return f"TO_CHAR({expr}, 'YYYY')"
        issues.append(f"Unsupported STRFTIME format: {fmt}")
        return match.group(0)

    converted = strftime_pattern.sub(rewrite_strftime, converted)
    converted = re.sub(
        r"\bCAST\s*\((.+?)\s+AS\s+DOUBLE\s*\)",
        r"CAST(\1 AS DOUBLE PRECISION)",
        converted,
        flags=re.I,
    )
    converted = re.sub(
        r"\bAS\s+DOUBLE\b(?!\s+PRECISION)",
        "AS DOUBLE PRECISION",
        converted,
        flags=re.I,
    )
    converted = _rewrite_duckdb_point_calls(converted)
    converted = _rewrite_desc_order_by_nulls_last(converted)
    converted = re.sub(r"\s+", " ", converted).strip()
    return converted, issues


def convert_batch_and_collect_unconverted(
    sql_list: List[str],
    source_ids: Optional[List[str]] = None,
    output_path: Optional[str] = None,
) -> Tuple[List[str], List[dict]]:
    """批量转换并收集未覆盖项。"""
    if source_ids is None:
        source_ids = [str(i) for i in range(len(sql_list))]
    converted_list: List[str] = []
    unconverted_records: List[dict] = []
    for sql, sid in zip(sql_list, source_ids):
        converted, issues = convert_spatialite_to_postgis(sql)
        converted_list.append(converted)
        if issues:
            unconverted_records.append(
                {
                    "source_id": sid,
                    "original": sql[:500],
                    "converted": converted[:500],
                    "issues": issues,
                    "classification": classify_spatialsql_failure(issues=issues),
                }
            )
    if output_path and unconverted_records:
        import json

        with open(output_path, "w", encoding="utf-8") as f:
            for record in unconverted_records:
                f.write(json.dumps(record, ensure_ascii=False) + "\n")
    return converted_list, unconverted_records
