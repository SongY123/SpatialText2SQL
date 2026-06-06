#!/usr/bin/env python3
"""Analyze query-type coverage in synthesized SQL JSONL files.

Each SQL can be assigned to multiple query types. Percentages are computed as:
category_count / number_of_sql_rows_in_the_same_difficulty.
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from collections import Counter, defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable


PROJECT_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_INPUT = PROJECT_ROOT / "data" / "processed" / "synthesized_sql_queries.jsonl"
DEFAULT_OUTPUT_MD = PROJECT_ROOT / "data" / "processed" / "synthesized_sql_query_type_stats.md"

DIFFICULTY_ORDER = ["easy", "medium", "hard", "extra-hard"]

QUERY_TYPES = [
    "Distance/Proximity Query",
    "Spatial Range Query",
    "Spatial Join",
    "Spatial Measurement",
    "Spatial Aggregation/Ranking",
    "Geometry Processing",
]

SPATIAL_RELATION_FUNCS = {
    "ST_INTERSECTS",
    "ST_CONTAINS",
    "ST_WITHIN",
    "ST_DWITHIN",
    "ST_TOUCHES",
    "ST_OVERLAPS",
    "ST_CROSSES",
    "ST_DISJOINT",
    "ST_EQUALS",
    "ST_COVERS",
    "ST_COVEREDBY",
}
DISTANCE_FUNCS = {"ST_DWITHIN", "ST_DISTANCE"}
COORD_RANGE_FUNCS = {"ST_XMIN", "ST_XMAX", "ST_YMIN", "ST_YMAX", "ST_X", "ST_Y"}
MEASUREMENT_FUNCS = {
    "ST_AREA",
    "ST_LENGTH",
    "ST_PERIMETER",
    "ST_DISTANCE",
    "ST_XMIN",
    "ST_XMAX",
    "ST_YMIN",
    "ST_YMAX",
    "ST_X",
    "ST_Y",
}
GEOMETRY_PROCESSING_FUNCS = {
    "ST_ASTEXT",
    "ST_BOUNDARY",
    "ST_BUFFER",
    "ST_CENTROID",
    "ST_COLLECT",
    "ST_COLLECTIONEXTRACT",
    "ST_CONVEXHULL",
    "ST_DIFFERENCE",
    "ST_DUMP",
    "ST_DUMPRINGS",
    "ST_ENVELOPE",
    "ST_GEOMETRYTYPE",
    "ST_INTERSECTION",
    "ST_ISEMPTY",
    "ST_ISVALID",
    "ST_MULTI",
    "ST_SETSRID",
    "ST_SIMPLIFY",
    "ST_SNAPTOGRID",
    "ST_SRID",
    "ST_SYMDIFFERENCE",
    "ST_TRANSFORM",
    "ST_UNARYUNION",
    "ST_UNION",
}
RANGE_LITERAL_FUNCS = {"ST_MAKEENVELOPE"}


def sorted_counter_dict(counter: Counter[str]) -> dict[str, int]:
    return dict(sorted(counter.items(), key=lambda item: (-item[1], item[0])))


@dataclass(frozen=True)
class FunctionCall:
    name: str
    args: str
    start: int
    end: int


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Analyze query-type counts and ratios by difficulty for synthesized SQL JSONL.",
    )
    parser.add_argument(
        "--input",
        default=str(DEFAULT_INPUT),
        help="Input synthesized_sql_queries.jsonl path.",
    )
    parser.add_argument(
        "--output-md",
        default=str(DEFAULT_OUTPUT_MD),
        help="Markdown report path. Pass an empty string to disable writing.",
    )
    parser.add_argument(
        "--output-json",
        default="",
        help="Optional JSON report path.",
    )
    parser.add_argument(
        "--unlabeled-examples",
        type=int,
        default=10,
        help="Number of unlabeled SQL examples to include in the report.",
    )
    return parser.parse_args()


def strip_sql_comments(sql: str) -> str:
    sql = re.sub(r"--[^\n\r]*", " ", sql)
    sql = re.sub(r"/\*.*?\*/", " ", sql, flags=re.S)
    return sql


def find_matching_paren(text: str, open_index: int) -> int:
    depth = 0
    quote: str | None = None
    index = open_index
    while index < len(text):
        char = text[index]
        if quote:
            if char == quote:
                if quote == "'" and index + 1 < len(text) and text[index + 1] == "'":
                    index += 2
                    continue
                quote = None
            index += 1
            continue
        if char in {"'", '"'}:
            quote = char
            index += 1
            continue
        if char == "(":
            depth += 1
        elif char == ")":
            depth -= 1
            if depth == 0:
                return index
        index += 1
    return len(text) - 1


def extract_st_function_calls(sql: str) -> list[FunctionCall]:
    calls: list[FunctionCall] = []
    for match in re.finditer(r"\b(ST_[A-Za-z0-9_]+)\s*\(", sql):
        open_index = sql.find("(", match.start())
        close_index = find_matching_paren(sql, open_index)
        calls.append(
            FunctionCall(
                name=match.group(1).upper(),
                args=sql[open_index + 1 : close_index],
                start=match.start(),
                end=close_index + 1,
            )
        )
    return calls


def extract_aliases(sql: str) -> set[str]:
    aliases: set[str] = set()
    stop_words = {
        "ON",
        "USING",
        "WHERE",
        "JOIN",
        "LEFT",
        "RIGHT",
        "FULL",
        "INNER",
        "CROSS",
        "GROUP",
        "ORDER",
        "LIMIT",
        "HAVING",
        "UNION",
        "EXCEPT",
        "INTERSECT",
    }
    pattern = re.compile(
        r"\b(?:FROM|JOIN)\s+"
        r"(?P<table>\"[^\"]+\"|[A-Za-z_][\w$]*(?:\.(?:\"[^\"]+\"|[A-Za-z_][\w$]*))?)"
        r"(?:\s+(?:AS\s+)?(?P<alias>\"[^\"]+\"|[A-Za-z_][\w$]*))?",
        flags=re.I,
    )
    for match in pattern.finditer(sql):
        table = strip_identifier(match.group("table").split(".")[-1])
        alias = strip_identifier(match.group("alias") or "")
        if alias and alias.upper() not in stop_words:
            aliases.add(alias)
        elif table:
            aliases.add(table)
    return aliases


def strip_identifier(value: str) -> str:
    value = str(value or "").strip()
    if value.startswith('"') and value.endswith('"'):
        return value[1:-1].replace('""', '"')
    return value


def alias_refs(text: str, known_aliases: set[str]) -> set[str]:
    refs = {
        strip_identifier(match.group(1))
        for match in re.finditer(r"\b(\"[^\"]+\"|[A-Za-z_][\w$]*)\s*\.", text)
    }
    return refs & known_aliases if known_aliases else refs


def has_literal_geometry(args: str) -> bool:
    upper_args = args.upper()
    return bool(
        re.search(r"\b(POINT|LINESTRING|POLYGON|MULTIPOINT|MULTILINESTRING|MULTIPOLYGON|GEOMETRYCOLLECTION)\s*\(", upper_args)
        or "::GEOMETRY" in upper_args
        or "ST_GEOMFROMTEXT" in upper_args
        or "ST_MAKEENVELOPE" in upper_args
        or "ST_MAKEPOINT" in upper_args
        or "ST_POINT" in upper_args
    )


def where_clause(sql_upper: str) -> str:
    match = re.search(r"\bWHERE\b(?P<body>.*?)(?:\bGROUP\s+BY\b|\bORDER\s+BY\b|\bLIMIT\b|$)", sql_upper, flags=re.S)
    return match.group("body") if match else ""


def classify_sql(sql: str) -> set[str]:
    clean_sql = strip_sql_comments(sql)
    sql_upper = clean_sql.upper()
    calls = extract_st_function_calls(clean_sql)
    funcs = {call.name for call in calls}
    aliases = extract_aliases(clean_sql)
    labels: set[str] = set()

    spatial_join = is_spatial_join(sql_upper, calls, aliases)
    if spatial_join:
        labels.add("Spatial Join")

    if is_distance_or_proximity(sql_upper, funcs):
        labels.add("Distance/Proximity Query")

    if is_spatial_range(sql_upper, calls, funcs, spatial_join):
        labels.add("Spatial Range Query")

    if funcs & MEASUREMENT_FUNCS:
        labels.add("Spatial Measurement")

    if is_spatial_aggregation_or_ranking(sql_upper, bool(funcs)):
        labels.add("Spatial Aggregation/Ranking")

    if funcs & GEOMETRY_PROCESSING_FUNCS:
        labels.add("Geometry Processing")

    return labels


def is_spatial_join(sql_upper: str, calls: Iterable[FunctionCall], aliases: set[str]) -> bool:
    has_join_keyword = bool(re.search(r"\bJOIN\b", sql_upper))
    for call in calls:
        if call.name not in SPATIAL_RELATION_FUNCS and call.name != "ST_DISTANCE":
            continue
        refs = alias_refs(call.args, aliases)
        if len(refs) >= 2:
            return True
    return False if not has_join_keyword else any(
        call.name in SPATIAL_RELATION_FUNCS for call in calls
    )


def is_distance_or_proximity(sql_upper: str, funcs: set[str]) -> bool:
    if funcs & DISTANCE_FUNCS:
        return True
    return "ST_BUFFER" in funcs and bool(funcs & SPATIAL_RELATION_FUNCS) and bool(
        re.search(r"\bWHERE\b|\bON\b", sql_upper)
    )


def is_spatial_range(
    sql_upper: str,
    calls: Iterable[FunctionCall],
    funcs: set[str],
    spatial_join: bool,
) -> bool:
    if "&&" in sql_upper or funcs & RANGE_LITERAL_FUNCS:
        return True
    where_text = where_clause(sql_upper)
    if where_text and funcs & COORD_RANGE_FUNCS:
        return bool(
            re.search(
                r"\bST_(?:XMIN|XMAX|YMIN|YMAX|X|Y)\s*\([^)]*\)\s*(?:=|<>|!=|<|>|<=|>=|BETWEEN\b)",
                where_text,
            )
        )
    for call in calls:
        if call.name not in SPATIAL_RELATION_FUNCS:
            continue
        if not spatial_join or has_literal_geometry(call.args):
            return True
    return False


def is_spatial_aggregation_or_ranking(sql_upper: str, has_spatial_function: bool) -> bool:
    if not has_spatial_function:
        return False
    has_aggregate = bool(re.search(r"\b(?:COUNT|SUM|AVG|MIN|MAX)\s*\(", sql_upper))
    has_group_or_having = bool(re.search(r"\bGROUP\s+BY\b|\bHAVING\b", sql_upper))
    has_rank_window = bool(re.search(r"\b(?:RANK|DENSE_RANK|ROW_NUMBER)\s*\(", sql_upper))
    has_ordered_topk = bool(
        re.search(r"\bORDER\s+BY\b", sql_upper)
        and re.search(r"\bLIMIT\b|\bFETCH\s+FIRST\b", sql_upper)
    )
    return has_aggregate or has_group_or_having or has_rank_window or has_ordered_topk


def read_jsonl(path: Path) -> Iterable[tuple[int, dict[str, Any]]]:
    with path.open("r", encoding="utf-8") as handle:
        for line_number, line in enumerate(handle, start=1):
            text = line.strip()
            if not text:
                continue
            try:
                payload = json.loads(text)
            except json.JSONDecodeError as exc:
                raise ValueError(f"Invalid JSON at {path}:{line_number}: {exc}") from exc
            if not isinstance(payload, dict):
                raise ValueError(f"Expected JSON object at {path}:{line_number}")
            yield line_number, payload


def difficulty_key(row: dict[str, Any]) -> str:
    value = row.get("difficulty") or row.get("difficulty_level") or "unknown"
    return str(value).strip() or "unknown"


def ordered_difficulties(values: Iterable[str]) -> list[str]:
    seen = set(values)
    ordered = [difficulty for difficulty in DIFFICULTY_ORDER if difficulty in seen]
    ordered.extend(sorted(seen - set(ordered)))
    return ordered


def analyze(path: Path, *, unlabeled_examples: int) -> dict[str, Any]:
    totals: Counter[str] = Counter()
    category_counts: dict[str, Counter[str]] = defaultdict(Counter)
    overall_counts: Counter[str] = Counter()
    spatial_functions: set[str] = set()
    spatial_functions_by_difficulty: dict[str, set[str]] = defaultdict(set)
    spatial_function_sql_counts: Counter[str] = Counter()
    spatial_function_call_counts: Counter[str] = Counter()
    unlabeled: list[dict[str, Any]] = []
    unlabeled_count = 0
    row_count = 0

    for line_number, row in read_jsonl(path):
        row_count += 1
        difficulty = difficulty_key(row)
        sql = str(row.get("sql") or "").strip()
        if not sql:
            raise ValueError(f"Missing sql at {path}:{line_number}")
        _schema = row.get("schema") or ""

        labels = classify_sql(sql)
        st_calls = extract_st_function_calls(strip_sql_comments(sql))
        sql_spatial_functions = {call.name for call in st_calls}
        spatial_functions.update(sql_spatial_functions)
        spatial_functions_by_difficulty[difficulty].update(sql_spatial_functions)
        for function_name in sql_spatial_functions:
            spatial_function_sql_counts[function_name] += 1
        for call in st_calls:
            spatial_function_call_counts[call.name] += 1

        totals[difficulty] += 1
        for label in labels:
            category_counts[difficulty][label] += 1
            overall_counts[label] += 1
        if not labels:
            unlabeled_count += 1
            if len(unlabeled) < unlabeled_examples:
                unlabeled.append(
                    {
                        "line": line_number,
                        "sql_id": row.get("sql_id"),
                        "difficulty": difficulty,
                        "sql": sql,
                    }
                )

    return {
        "input": str(path),
        "row_count": row_count,
        "difficulties": ordered_difficulties(totals.keys()),
        "totals": dict(totals),
        "category_counts": {
            difficulty: {label: category_counts[difficulty].get(label, 0) for label in QUERY_TYPES}
            for difficulty in ordered_difficulties(totals.keys())
        },
        "overall_counts": {label: overall_counts.get(label, 0) for label in QUERY_TYPES},
        "distinct_spatial_function_count": len(spatial_functions),
        "distinct_spatial_functions": sorted(spatial_functions),
        "distinct_spatial_function_counts_by_difficulty": {
            difficulty: len(spatial_functions_by_difficulty[difficulty])
            for difficulty in ordered_difficulties(totals.keys())
        },
        "spatial_functions_by_difficulty": {
            difficulty: sorted(spatial_functions_by_difficulty[difficulty])
            for difficulty in ordered_difficulties(totals.keys())
        },
        "spatial_function_sql_counts": sorted_counter_dict(spatial_function_sql_counts),
        "spatial_function_call_counts": sorted_counter_dict(spatial_function_call_counts),
        "unlabeled_count": unlabeled_count,
        "unlabeled_examples": unlabeled,
    }


def pct(count: int, total: int) -> str:
    if total <= 0:
        return "0.00%"
    return f"{count / total * 100:.2f}%"


def markdown_report(result: dict[str, Any]) -> str:
    lines = [
        "# Synthesized SQL Query Type Coverage",
        "",
        f"- Input: `{result['input']}`",
        f"- Total SQL rows: {result['row_count']}",
        f"- Unlabeled SQL rows: {result['unlabeled_count']} ({pct(result['unlabeled_count'], result['row_count'])})",
        "- Percentages use difficulty-level row counts as denominators; query types are multi-label.",
        "",
        "## By Difficulty",
        "",
        "| Difficulty | Total SQL | Query Type | Count | Ratio |",
        "|---|---:|---|---:|---:|",
    ]
    totals = result["totals"]
    category_counts = result["category_counts"]
    for difficulty in result["difficulties"]:
        total = totals[difficulty]
        for label in QUERY_TYPES:
            count = category_counts[difficulty][label]
            lines.append(f"| {difficulty} | {total} | {label} | {count} | {pct(count, total)} |")

    lines.extend(
        [
            "",
            "## Overall Counts",
            "",
            "| Query Type | Count | Ratio vs Total SQL |",
            "|---|---:|---:|",
        ]
    )
    for label in QUERY_TYPES:
        count = result["overall_counts"][label]
        lines.append(f"| {label} | {count} | {pct(count, result['row_count'])} |")

    lines.extend(
        [
            "",
            "## Distinct Spatial Functions",
            "",
            f"- Distinct `ST_` functions: {result['distinct_spatial_function_count']}",
            "",
            "| Difficulty | Distinct Function Count | Functions |",
            "|---|---:|---|",
        ]
    )
    function_counts_by_difficulty = result["distinct_spatial_function_counts_by_difficulty"]
    functions_by_difficulty = result["spatial_functions_by_difficulty"]
    for difficulty in result["difficulties"]:
        functions = ", ".join(f"`{name}`" for name in functions_by_difficulty[difficulty])
        lines.append(f"| {difficulty} | {function_counts_by_difficulty[difficulty]} | {functions or '-'} |")

    lines.extend(
        [
            "",
            "| Spatial Function | SQL Rows Using Function | Function Call Count |",
            "|---|---:|---:|",
        ]
    )
    sql_counts = result["spatial_function_sql_counts"]
    call_counts = result["spatial_function_call_counts"]
    if not sql_counts:
        lines.append("| - | 0 | 0 |")
    else:
        for function_name, sql_count in sql_counts.items():
            lines.append(f"| `{function_name}` | {sql_count} | {call_counts.get(function_name, 0)} |")

    examples = result["unlabeled_examples"]
    lines.extend(["", "## Unlabeled Examples", ""])
    if not examples:
        lines.append("No unlabeled examples found.")
    else:
        for example in examples:
            sql = " ".join(str(example["sql"]).split())
            lines.append(
                f"- line {example['line']}, sql_id={example.get('sql_id')}, "
                f"difficulty={example['difficulty']}: `{sql}`"
            )
    lines.append("")
    return "\n".join(lines)


def print_summary(result: dict[str, Any]) -> None:
    totals = result["totals"]
    category_counts = result["category_counts"]
    print(f"Input: {result['input']}")
    print(f"Total SQL rows: {result['row_count']}")
    for difficulty in result["difficulties"]:
        total = totals[difficulty]
        print(f"\n[{difficulty}] total={total}")
        for label in QUERY_TYPES:
            count = category_counts[difficulty][label]
            print(f"  {label}: {count} ({pct(count, total)})")
        function_count = result["distinct_spatial_function_counts_by_difficulty"][difficulty]
        print(f"  Distinct ST_ functions: {function_count}")
    print(f"\nDistinct ST_ functions overall: {result['distinct_spatial_function_count']}")
    unlabeled_count = len(result["unlabeled_examples"])
    total_unlabeled = result["unlabeled_count"]
    if total_unlabeled:
        print(
            f"\nUnlabeled SQL rows: {total_unlabeled} "
            f"({pct(total_unlabeled, result['row_count'])}); "
            f"showing {unlabeled_count} example(s) in the Markdown report."
        )


def main() -> None:
    args = parse_args()
    input_path = Path(args.input)
    if not input_path.is_absolute():
        input_path = PROJECT_ROOT / input_path

    result = analyze(input_path, unlabeled_examples=max(0, args.unlabeled_examples))
    print_summary(result)

    if args.output_md:
        output_md = Path(args.output_md)
        if not output_md.is_absolute():
            output_md = PROJECT_ROOT / output_md
        output_md.parent.mkdir(parents=True, exist_ok=True)
        output_md.write_text(markdown_report(result), encoding="utf-8")
        print(f"\nWrote Markdown report: {output_md}")

    if args.output_json:
        output_json = Path(args.output_json)
        if not output_json.is_absolute():
            output_json = PROJECT_ROOT / output_json
        output_json.parent.mkdir(parents=True, exist_ok=True)
        output_json.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
        print(f"Wrote JSON report: {output_json}")


if __name__ == "__main__":
    main()
