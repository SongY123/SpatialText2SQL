"""SQL 生成结果的清洗、提取与 SpatialSQL 归一化工具。"""
from __future__ import annotations

import re
from typing import Dict, Optional

from src.sql.sql_dialect_adapter import convert_duckdb_to_postgis, convert_spatialite_to_postgis


SQL_LEADING_PATTERN = re.compile(
    r"^\s*(SELECT|INSERT|UPDATE|DELETE|CREATE|DROP|ALTER)\b",
    re.I,
)
SQL_LEADING_WITH_CTE_PATTERN = re.compile(
    r'^\s*WITH\s+(?:RECURSIVE\s+)?(?:"[^"]+"|[A-Za-z_][A-Za-z0-9_]*)\s+AS\s*\(',
    re.I,
)
ANSWER_TAG_PATTERN = re.compile(r"<answer_sql>\s*(.*?)\s*</answer_sql>", re.I | re.S)
ANSWER_MARKER_PATTERN = re.compile(r"(?:最终答案SQL|Final SQL|Answer SQL)\s*[:：]\s*(.*)", re.I | re.S)
SQL_FENCE_PATTERN = re.compile(r"```sql\s*(.*?)```", re.I | re.S)
SQL_TRIPLE_SINGLE_QUOTE_PATTERN = re.compile(r"'''\s*(.*?)\s*'''", re.S)
THINKING_END_PATTERN = re.compile(r"</think(?:ing)?>", re.I)


def _strip_think_blocks(text: str) -> str:
    text = re.sub(r"<think(?:ing)?>.*?</think(?:ing)?>", "", text, flags=re.I | re.S)
    return re.sub(r"<thinking>.*?</thinking>", "", text, flags=re.I | re.S)


def _strip_markdown_fences(text: str) -> str:
    text = re.sub(r"```sql\s*", "", text, flags=re.I)
    return re.sub(r"```", "", text)


def _extract_answer_zone(text: str) -> str:
    tag_matches = ANSWER_TAG_PATTERN.findall(text)
    if tag_matches:
        return tag_matches[-1].strip()

    marker_matches = ANSWER_MARKER_PATTERN.findall(text)
    if marker_matches:
        return marker_matches[-1].strip()

    fence_matches = SQL_FENCE_PATTERN.findall(text)
    if fence_matches:
        return fence_matches[-1].strip()

    triple_quote_matches = SQL_TRIPLE_SINGLE_QUOTE_PATTERN.findall(text)
    if triple_quote_matches:
        return triple_quote_matches[-1].strip()

    return ""


def _extract_post_thinking_tail(text: str) -> str:
    matches = list(THINKING_END_PATTERN.finditer(text))
    if not matches:
        return ""
    return text[matches[-1].end() :].strip()


def _trim_after_non_sql_explanation(text: str) -> str:
    lines = []
    for line in text.splitlines():
        stripped = line.strip()
        if re.match(r"^(解释|说明|answer|analysis)\s*[:：]", stripped, re.I):
            break
        lines.append(line)
    return "\n".join(lines)


def _normalize_terminal_punctuation(text: str) -> str:
    text = text.replace("。;", ";").replace("；", ";")
    return re.sub(r";+\s*$", ";", text.strip())


def looks_like_sql_text(text: str) -> bool:
    candidate = (text or "").strip()
    if not candidate:
        return False
    return bool(
        SQL_LEADING_PATTERN.match(candidate)
        or SQL_LEADING_WITH_CTE_PATTERN.match(candidate)
    )


def _find_sql_start(text: str) -> int | None:
    offset = 0
    for line in text.splitlines(keepends=True):
        stripped = line.lstrip()
        if looks_like_sql_text(stripped):
            return offset + (len(line) - len(stripped))
        offset += len(line)

    stripped_text = text.lstrip()
    if looks_like_sql_text(stripped_text):
        return len(text) - len(stripped_text)
    return None


def _extract_sql_body(text: str) -> str:
    start = _find_sql_start(text)
    if start is None:
        return ""

    sql = text[start:].strip()
    semicolon_pos = sql.find(";")
    if semicolon_pos >= 0:
        return sql[: semicolon_pos + 1]

    sql = _trim_after_non_sql_explanation(sql)
    if not sql.endswith(";"):
        sql += ";"
    return sql


def extract_sql_from_text(generated_text: str, prompt: str = "") -> str:
    """从模型输出中提取一条可执行 SQL。"""
    text = generated_text or ""
    if prompt and text.startswith(prompt):
        text = text[len(prompt):]

    text = _strip_think_blocks(text)
    answer_zone = _extract_answer_zone(text)
    post_thinking_tail = _extract_post_thinking_tail(text)

    candidate_text = answer_zone or post_thinking_tail or text
    candidate_text = _strip_markdown_fences(candidate_text)
    candidate_text = _trim_after_non_sql_explanation(candidate_text)
    candidate_text = _normalize_terminal_punctuation(candidate_text)
    sql = _extract_sql_body(candidate_text)
    if not sql:
        if answer_zone or post_thinking_tail:
            return ""

        text = _strip_markdown_fences(text)
        text = _trim_after_non_sql_explanation(text)
        text = _normalize_terminal_punctuation(text)
        sql = _extract_sql_body(text)
    if not sql:
        return ""

    sql = re.sub(r"--.*$", "", sql, flags=re.MULTILINE)
    sql = re.sub(r"\s+", " ", sql).strip()
    sql = _normalize_terminal_punctuation(sql)

    if not looks_like_sql_text(sql):
        return ""
    if re.fullmatch(r"Within\s*;", sql, re.I):
        return ""
    if re.fullmatch(r"SELECT\s+\*\s*;", sql, re.I):
        return ""
    if not sql.endswith(";"):
        sql += ";"
    return sql


def normalize_spatialsql_predicted_sql(
    sql: str,
    metadata: Optional[Dict[str, str]] = None,
) -> str:
    """将模型输出的 SpatialSQL 预测 SQL 归一化到当前 split 的 PG 约定。"""
    if not sql or not sql.strip():
        return sql

    split = (metadata or {}).get("split", "")
    table_prefix = f"{split}_" if split else None
    normalized, _ = convert_spatialite_to_postgis(sql, table_prefix=table_prefix)
    normalized = re.sub(r"\s+", " ", normalized).strip()
    if sql.strip().endswith(";") and not normalized.endswith(";"):
        normalized += ";"
    return normalized


def normalize_floodsql_predicted_sql(
    sql: str,
    metadata: Optional[Dict[str, str]] = None,
) -> str:
    """将 FloodSQL 预测 SQL 归一化到 PostgreSQL/PostGIS 约定。"""
    del metadata
    if not sql or not sql.strip():
        return sql

    normalized, _ = convert_duckdb_to_postgis(sql)
    normalized = re.sub(r"\s+", " ", normalized).strip()
    if sql.strip().endswith(";") and not normalized.endswith(";"):
        normalized += ";"
    return normalized
