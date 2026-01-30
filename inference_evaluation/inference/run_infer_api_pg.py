import argparse
import json
import os
from typing import Any, Dict, List, Optional, Tuple

import psycopg2
from psycopg2.extras import RealDictCursor
from psycopg2.sql import SQL, Identifier

from openai_text2sql import generate_sql


def connect_db(db_config: Dict[str, Any], db_name: str):
    config = dict(db_config)
    config["dbname"] = db_name
    config.setdefault("options", "-c client_encoding=UTF8")
    try:
        return psycopg2.connect(**config)
    except UnicodeDecodeError as e:
        raise RuntimeError(
            "psycopg2 连接 PostgreSQL 失败且错误信息解码失败（常见于 Windows 本地编码非 UTF-8）。\n"
            "优先确认：数据库是否存在、端口是否可达、账号密码是否正确。\n"
            "然后在 PowerShell 里设置：\n"
            "  $env:PGCLIENTENCODING='UTF8'\n"
            "  $env:PYTHONUTF8='1'\n"
            "再重试。"
        ) from e


def fetch_schema_and_samples(
    conn,
    table_schema: str,
    sample_rows: int,
    max_tables: Optional[int],
) -> Tuple[Dict[str, Any], Dict[str, List[Dict[str, Any]]]]:
    schema: Dict[str, Any] = {}
    sample_data: Dict[str, List[Dict[str, Any]]] = {}

    with conn.cursor(cursor_factory=RealDictCursor) as cursor:
        cursor.execute(
            """
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = %s AND table_type = 'BASE TABLE'
            ORDER BY table_name
            """,
            (table_schema,),
        )
        tables = [r["table_name"] for r in cursor.fetchall()]
        if max_tables is not None:
            tables = tables[: max_tables]

        for table in tables:
            cursor.execute(
                """
                SELECT column_name, data_type
                FROM information_schema.columns
                WHERE table_schema = %s AND table_name = %s
                ORDER BY ordinal_position
                """,
                (table_schema, table),
            )
            cols = cursor.fetchall()
            schema[table] = {"columns": [{"name": c["column_name"], "type": c["data_type"]} for c in cols]}

        if sample_rows <= 0:
            return schema, sample_data

        for table in tables:
            cursor.execute(
                SQL("SELECT * FROM {}.{} LIMIT {}").format(
                    Identifier(table_schema), Identifier(table), SQL(str(sample_rows))
                )
            )
            rows = cursor.fetchall()
            sample_data[table] = [dict(r) for r in rows]

    return schema, sample_data


def ensure_dir(path: str) -> None:
    os.makedirs(path, exist_ok=True)


def main():
    parser = argparse.ArgumentParser(description="Text-to-SQL inference via OpenAI-compatible API (PG schema+sample).")
    parser.add_argument("--db_config", type=str, required=True, help="Database config JSON file")
    parser.add_argument("--db_id", type=str, required=True, help="Target PostgreSQL database name")
    parser.add_argument("--question", type=str, required=True, help="Natural language question")
    parser.add_argument("--table_schema", type=str, default="public", help="Schema name in PostgreSQL")
    parser.add_argument("--sample_rows", type=int, default=3, help="Sample rows per table")
    parser.add_argument("--max_tables", type=int, default=30, help="Max tables to include, 0 means no limit")
    parser.add_argument("--out_sql", type=str, default="", help="Output .sql file path (optional)")
    parser.add_argument("--force_dbname", type=str, default="", help="If set, ignore --db_id and connect to this database")
    args = parser.parse_args()

    with open(args.db_config, "r", encoding="utf-8") as f:
        db_config = json.load(f)

    max_tables = None if args.max_tables == 0 else args.max_tables
    db_name = args.force_dbname.strip() or args.db_id
    with connect_db(db_config, db_name) as conn:
        schema, sample_data = fetch_schema_and_samples(
            conn=conn,
            table_schema=args.table_schema,
            sample_rows=args.sample_rows,
            max_tables=max_tables,
        )

    sql = generate_sql(question=args.question, schema=schema, sample_data=sample_data)

    if args.out_sql:
        ensure_dir(os.path.dirname(os.path.abspath(args.out_sql)))
        with open(args.out_sql, "w", encoding="utf-8") as f:
            f.write(sql.strip())
            if sql.strip() and not sql.strip().endswith(";"):
                f.write(";")
            f.write("\n")
    else:
        print(sql)


if __name__ == "__main__":
    main()
