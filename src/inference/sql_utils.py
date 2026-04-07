"""SQL 生成结果的清洗与提取工具。"""
import re


def extract_sql_from_text(generated_text: str, prompt: str = "") -> str:
    """
    从生成文本中提取 SQL 语句。

    Args:
        generated_text: 模型完整输出文本
        prompt: 原始输入 prompt，用于去除回显

    Returns:
        提取并清洗后的 SQL 语句
    """
    if prompt and generated_text.startswith(prompt):
        sql = generated_text[len(prompt):].strip()
    else:
        sql = generated_text.strip()

    # 移除 markdown 代码块标记
    sql = re.sub(r"```sql\s*", "", sql)
    sql = re.sub(r"```\s*", "", sql)

    # 提取第一个完整的 SQL 语句（从 SQL 关键字开始，到第一个分号结束）
    sql_keywords_pattern = r"(SELECT|INSERT|UPDATE|DELETE|CREATE|DROP|ALTER|WITH)\s+.*?;"
    match = re.search(sql_keywords_pattern, sql, re.IGNORECASE | re.DOTALL)

    if match:
        sql = match.group(0)
    else:
        # 如果没有标准分号结尾，至少保留从 SQL 关键字开始的主体
        sql_keywords = r"(SELECT|INSERT|UPDATE|DELETE|CREATE|DROP|ALTER|WITH)"
        keyword_match = re.search(sql_keywords, sql, re.IGNORECASE)
        if keyword_match:
            start_pos = keyword_match.start()
            sql = sql[start_pos:]

            semicolon_pos = sql.find(";")
            if semicolon_pos > 0:
                sql = sql[:semicolon_pos + 1]
            else:
                chinese_match = re.search(r"[\u4e00-\u9fff]", sql)
                if chinese_match:
                    sql = sql[:chinese_match.start()].strip()
                    if sql and not sql.endswith(";"):
                        last_semicolon = sql.rfind(";")
                        if last_semicolon > 0:
                            sql = sql[:last_semicolon + 1]

    # 移除注释
    sql = re.sub(r"--.*$", "", sql, flags=re.MULTILINE)

    # 清理多余空白
    sql = re.sub(r"\s+", " ", sql).strip()

    # 确保以分号结尾
    if sql and not sql.endswith(";"):
        sql += ";"

    return sql
