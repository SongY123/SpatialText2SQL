"""
SpatiaLite -> PostGIS SQL 方言转换器。
仅用于 spatialsql_pg 数据集路径；不修改原有 spatial_qa 流程。
对无法自动转换的片段打标并可通过 unconverted 清单落盘供人工修订。
"""
import re
from typing import List, Tuple


# 函数名映射：SpatiaLite (pattern) -> PostGIS 替换
# 使用正则以便匹配括号前单词边界，避免误替换
FUNCTION_REPLACEMENTS: List[Tuple[re.Pattern, str]] = [
    # Intersects(...) = 1 或 =1 单独一行
    (re.compile(r"\bIntersects\s*\(", re.I), "ST_Intersects("),
    (re.compile(r"\bIntersection\s*\(", re.I), "ST_Intersection("),
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
]
# 几何列名：SpatiaLite 常见 Shape -> PostGIS 常用 shape（小写，与迁移脚本 -lco GEOMETRY_NAME=shape 一致）
COLUMN_SHAPE_NORMALIZE = re.compile(r"\b([a-zA-Z_][a-zA-Z0-9_]*\.)\s*Shape\b", re.I)


def add_table_prefix(sql: str, prefix: str) -> str:
    """
    为 SQL 中的表名添加前缀
    
    Args:
        sql: 原始 SQL
        prefix: 表名前缀（如 dataset1_ada_）
    
    Returns:
        添加前缀后的 SQL
    """
    if not prefix:
        return sql
    
    # 匹配 FROM/JOIN 后的表名
    # 支持: FROM table, FROM table AS alias, JOIN table, JOIN table ON...
    pattern = r'\b(FROM|JOIN|from|join)\s+([a-zA-Z_][a-zA-Z0-9_]*)\b'
    
    def replace_table(match):
        keyword = match.group(1)
        table = match.group(2)
        # 如果表名已经有前缀，不再添加
        if table.startswith(prefix):
            return match.group(0)
        return f"{keyword} {prefix}{table}"
    
    return re.sub(pattern, replace_table, sql)


def convert_spatialite_to_postgis(sql: str, table_prefix: str = None) -> Tuple[str, List[str]]:
    """
    将一条 SpatiaLite SQL 转为 PostGIS 方言。

    Args:
        sql: 原始 SQL 字符串
        table_prefix: 可选的表名前缀（如 dataset1_ada_）

    Returns:
        (converted_sql, issues) 其中 issues 为无法自动处理的片段或警告列表（可落盘作人工修订清单）
    """
    if not sql or not sql.strip():
        return sql, []
    issues: List[str] = []
    s = sql

    # 1) 几何列名统一为小写 shape（与迁移脚本一致）
    s = COLUMN_SHAPE_NORMALIZE.sub(r"\1shape", s)

    # 2) 函数名替换
    for pattern, replacement in FUNCTION_REPLACEMENTS:
        s = pattern.sub(replacement, s)

    # 3) Intersects(...)=1 在 SpatiaLite 中表示 true；PostGIS 返回 boolean，保留 = 1 仍可执行，可选去掉
    # 不强制去掉，避免破坏其它比较

    # 4) 添加表名前缀（如果指定）
    if table_prefix:
        s = add_table_prefix(s, table_prefix)

    # 5) 打标可能未覆盖的 SpatiaLite 函数（首字母大写且接括号，且未加 ST_）
    remaining_spatial = re.findall(
        r"\b([A-Z][a-zA-Z]*)\s*\(",
        s,
    )
    known_postgis = {"ST_Intersects", "ST_Intersection", "ST_Length", "ST_Area", "ST_GeomFromText",
                     "ST_AsText", "ST_Distance", "ST_Buffer", "ST_ConvexHull", "ST_Centroid",
                     "ST_Contains", "ST_Within", "ST_Touches", "ST_Overlaps", "ST_Crosses",
                     "ST_Disjoint", "ST_Union", "ST_Difference", "ST_SymDifference", "ST_Boundary",
                     "ST_StartPoint", "ST_EndPoint", "ST_PointN", "ST_NPoints", "ST_NumGeometries",
                     "ST_GeometryN", "ST_X", "ST_Y", "ST_Transform", "ST_Extent"}
    sql_functions = {"Select", "Count", "Sum", "Avg", "Min", "Max", "Inner", "Outer", "Left", "Right",
                    "On", "Where", "From", "Group", "Order", "By", "As", "And", "Or", "Not", "In",
                    "Like", "Between", "Case", "When", "Then", "Else", "End", "Cast", "Null",
                    "True", "False", "Upper", "Lower", "Trim", "Substring", "Replace", "Coalesce"}
    for name in remaining_spatial:
        if name in known_postgis or name in sql_functions:
            continue
        if name.startswith("ST_"):
            continue
        issues.append(f"Possible unconverted function: {name}(...)")

    return s.strip(), issues


def convert_batch_and_collect_unconverted(
    sql_list: List[str],
    source_ids: List[str] | None = None,
    output_path: str | None = None,
) -> Tuple[List[str], List[dict]]:
    """
    批量转换并收集未覆盖项。

    Args:
        sql_list: SQL 列表
        source_ids: 可选，与 sql_list 一一对应的 id（如 source_id）
        output_path: 可选，将未覆盖清单写入此文件（JSON 行格式）

    Returns:
        (converted_list, unconverted_records) 每条 record 为 {"source_id": str, "original": str, "converted": str, "issues": list}
    """
    if source_ids is None:
        source_ids = [str(i) for i in range(len(sql_list))]
    converted_list: List[str] = []
    unconverted_records: List[dict] = []
    for sql, sid in zip(sql_list, source_ids):
        converted, issues = convert_spatialite_to_postgis(sql)
        converted_list.append(converted)
        if issues:
            unconverted_records.append({
                "source_id": sid,
                "original": sql[:500],
                "converted": converted[:500],
                "issues": issues,
            })
    if output_path and unconverted_records:
        import json
        with open(output_path, "w", encoding="utf-8") as f:
            for rec in unconverted_records:
                f.write(json.dumps(rec, ensure_ascii=False) + "\n")
    return converted_list, unconverted_records
