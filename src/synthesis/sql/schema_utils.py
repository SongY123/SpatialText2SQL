"""Shared helpers for live schema metadata used in SQL and question synthesis."""

from __future__ import annotations

from src.synthesis.database.utils import to_text


CREATE_TABLE_DDL_QUERY = """
WITH table_info AS (
    SELECT c.oid AS table_oid,
           n.nspname AS schema_name,
           c.relname AS table_name
    FROM pg_class c
    JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE c.oid = %s::regclass
),
columns AS (
    SELECT a.attrelid AS table_oid,
           a.attnum,
           format(
               '    %%I %%s%%s%%s',
               a.attname,
               format_type(a.atttypid, a.atttypmod),
               CASE
                   WHEN ad.adbin IS NOT NULL
                       THEN ' DEFAULT ' || pg_get_expr(ad.adbin, ad.adrelid)
                   ELSE ''
               END,
               CASE
                   WHEN a.attnotnull THEN ' NOT NULL'
                   ELSE ''
               END
           ) AS ddl
    FROM pg_attribute a
    LEFT JOIN pg_attrdef ad
      ON ad.adrelid = a.attrelid
     AND ad.adnum = a.attnum
    JOIN table_info t
      ON t.table_oid = a.attrelid
    WHERE a.attnum > 0
      AND NOT a.attisdropped
),
constraints AS (
    SELECT conrelid AS table_oid,
           10000 + row_number() OVER (ORDER BY conname) AS attnum,
           format(
               '    CONSTRAINT %%I %%s',
               conname,
               pg_get_constraintdef(oid, true)
           ) AS ddl
    FROM pg_constraint
    WHERE conrelid = %s::regclass
      AND contype IN ('p', 'u', 'f', 'c')
),
all_items AS (
    SELECT table_oid, attnum, ddl FROM columns
    UNION ALL
    SELECT table_oid, attnum, ddl FROM constraints
)
SELECT format(
           'CREATE TABLE %%I (%%s%%s%%s);',
           t.table_name,
           E'\\n',
           string_agg(i.ddl, E',\\n' ORDER BY i.attnum),
           E'\\n'
       ) AS create_table_ddl
FROM table_info t
JOIN all_items i ON i.table_oid = t.table_oid
GROUP BY t.schema_name, t.table_name
"""


def build_create_table_ddl_query(schema_name: str, table_name: str) -> tuple[str, tuple[str, str]]:
    qualified_name = f"{to_text(schema_name)}.{to_text(table_name)}"
    return CREATE_TABLE_DDL_QUERY, (qualified_name, qualified_name)
