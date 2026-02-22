from __future__ import annotations

from dataclasses import dataclass
import re
import time
from typing import Any, Dict, List, Optional
from urllib.parse import parse_qs, quote_plus, urlencode, urlparse

from sqlalchemy import MetaData, Table, create_engine, inspect, select, text
from sqlalchemy.engine import Engine
from sqlalchemy.schema import CreateTable


def _jdbc_sqlite_to_sqlalchemy(jdbc_body: str) -> str:
    # jdbc:sqlite::memory:
    sqlite_path = jdbc_body[len("sqlite:") :]
    if sqlite_path in {":memory:", "memory:"}:
        return "sqlite:///:memory:"

    # jdbc:sqlite:/absolute/path.db
    # jdbc:sqlite:relative/path.db
    if sqlite_path.startswith("//"):
        sqlite_path = sqlite_path[2:]
    if sqlite_path.startswith("/"):
        return f"sqlite:///{sqlite_path}"
    return f"sqlite:///{sqlite_path}"


def jdbc_to_sqlalchemy_url(jdbc_url: str) -> str:
    if not jdbc_url or not jdbc_url.startswith("jdbc:"):
        raise ValueError("jdbc_url must start with 'jdbc:'.")

    jdbc_body = jdbc_url[5:]
    if jdbc_body.startswith("sqlite:"):
        return _jdbc_sqlite_to_sqlalchemy(jdbc_body)

    # Example: jdbc:postgresql://host:5432/db?user=xx&password=yy
    driver, rest = jdbc_body.split(":", 1)
    if not rest.startswith("//"):
        raise ValueError(f"Unsupported JDBC url format: {jdbc_url}")

    parsed = urlparse(rest)
    query = parse_qs(parsed.query)

    user = parsed.username or (query.get("user", [None])[0])
    password = parsed.password or (query.get("password", [None])[0])
    host = parsed.hostname or "localhost"
    port = parsed.port
    database = parsed.path.lstrip("/")

    driver_map = {
        "postgresql": "postgresql+psycopg2",
        "postgres": "postgresql+psycopg2",
        "mysql": "mysql+pymysql",
        "mariadb": "mysql+pymysql",
        "sqlserver": "mssql+pyodbc",
    }
    sqlalchemy_driver = driver_map.get(driver, driver)

    auth = ""
    if user:
        auth = quote_plus(str(user))
        if password is not None:
            auth += ":" + quote_plus(str(password))
        auth += "@"

    netloc = f"{auth}{host}"
    if port:
        netloc += f":{port}"

    # keep extra params except auth
    extra_query = {k: v for k, v in query.items() if k not in {"user", "password"}}
    query_str = urlencode(extra_query, doseq=True)

    url = f"{sqlalchemy_driver}://{netloc}"
    if database:
        url += f"/{database}"
    if query_str:
        url += f"?{query_str}"
    return url


_IDENT_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_READ_ONLY_PREFIX_RE = re.compile(r"^\s*(WITH|SELECT|SHOW|EXPLAIN|DESCRIBE|PRAGMA)\b", re.IGNORECASE)
_DANGEROUS_SQL_RE = re.compile(
    r"\b(INSERT|UPDATE|DELETE|DROP|ALTER|TRUNCATE|CREATE|REPLACE|GRANT|REVOKE|MERGE|CALL)\b",
    re.IGNORECASE,
)


def _assert_identifier(name: str, field_name: str) -> str:
    n = str(name or "").strip()
    if not n or not _IDENT_RE.match(n):
        raise ValueError(f"Invalid {field_name}: {name!r}")
    return n


def _quote_ident(name: str) -> str:
    return '"' + str(name).replace('"', '""') + '"'


def _qualified_table_name(table_name: str, schema: Optional[str] = None) -> str:
    t = _assert_identifier(table_name, "table_name")
    if schema:
        s = _assert_identifier(schema, "schema")
        return f"{_quote_ident(s)}.{_quote_ident(t)}"
    return _quote_ident(t)


def _qualified_column_name(column_name: str) -> str:
    c = _assert_identifier(column_name, "column_name")
    return _quote_ident(c)


def _strip_sql_comments(sql: str) -> str:
    """Remove SQL comments while preserving quoted strings.

    Supported comment styles:
    - line comments: `-- ...`
    - block comments: `/* ... */` (with nested block comments)
    """
    text_sql = str(sql or "")
    n = len(text_sql)
    if n == 0:
        return ""

    out: List[str] = []
    i = 0
    in_single = False
    in_double = False
    block_comment_depth = 0
    in_line_comment = False

    while i < n:
        ch = text_sql[i]
        nxt = text_sql[i + 1] if i + 1 < n else ""

        if in_line_comment:
            if ch == "\n":
                in_line_comment = False
                out.append(ch)
            i += 1
            continue

        if block_comment_depth > 0:
            if ch == "/" and nxt == "*":
                block_comment_depth += 1
                i += 2
                continue
            if ch == "*" and nxt == "/":
                block_comment_depth -= 1
                i += 2
                continue
            if ch == "\n":
                # Preserve line breaks so error locations/format remain reasonable.
                out.append(ch)
            i += 1
            continue

        if in_single:
            out.append(ch)
            if ch == "'" and nxt == "'":
                # Escaped single quote in SQL string literal.
                out.append(nxt)
                i += 2
                continue
            if ch == "'":
                in_single = False
            i += 1
            continue

        if in_double:
            out.append(ch)
            if ch == '"' and nxt == '"':
                # Escaped double quote in quoted identifier.
                out.append(nxt)
                i += 2
                continue
            if ch == '"':
                in_double = False
            i += 1
            continue

        if ch == "-" and nxt == "-":
            in_line_comment = True
            i += 2
            continue
        if ch == "/" and nxt == "*":
            block_comment_depth = 1
            i += 2
            continue
        if ch == "'":
            in_single = True
            out.append(ch)
            i += 1
            continue
        if ch == '"':
            in_double = True
            out.append(ch)
            i += 1
            continue

        out.append(ch)
        i += 1

    return "".join(out)


def _assert_read_only_sql(sql: str) -> str:
    text_sql = _strip_sql_comments(sql).strip()
    if not text_sql:
        raise ValueError("sql must not be empty.")
    if not _READ_ONLY_PREFIX_RE.search(text_sql):
        raise ValueError("Only read-only SQL is allowed (WITH/SELECT/SHOW/EXPLAIN/DESCRIBE/PRAGMA).")
    if _DANGEROUS_SQL_RE.search(text_sql):
        raise ValueError("Dangerous SQL keyword detected in read-only execution.")
    return text_sql


def _is_geometry_column(column_meta: Dict) -> bool:
    type_text = str(column_meta.get("type", "")).strip().lower()
    if "geometry" in type_text or "geography" in type_text:
        return True

    # Some backends may reflect spatial columns as generic USER-DEFINED.
    if type_text in {"user-defined", "nulltype"}:
        name = str(column_meta.get("name", "")).strip().lower()
        if name in {"geom", "geometry", "geog", "the_geom"}:
            return True
    return False


def _normalize_binary_cell(value: Any) -> Any:
    raw: Optional[bytes] = None
    if isinstance(value, memoryview):
        raw = value.tobytes()
    elif isinstance(value, bytearray):
        raw = bytes(value)
    elif isinstance(value, bytes):
        raw = value

    if raw is None:
        return value
    if raw == b"":
        return ""

    # Keep plain text bytes readable.
    if all((32 <= b <= 126) or b in {9, 10, 13} for b in raw):
        return raw.decode("utf-8", errors="replace")

    # Some drivers return hex-encoded bytes in bytes form.
    candidate = raw
    try:
        ascii_text = raw.decode("ascii")
        if re.fullmatch(r"[0-9A-Fa-f]+", ascii_text) and len(ascii_text) % 2 == 0:
            candidate = bytes.fromhex(ascii_text)
    except Exception:
        pass

    # Best effort: decode WKB/EWKB to WKT.
    try:
        from shapely import wkb

        geom = wkb.loads(candidate)
        return geom.wkt
    except Exception:
        return raw.hex()


def _normalize_row_cells(row: Dict) -> Dict:
    return {k: _normalize_binary_cell(v) for k, v in row.items()}


@dataclass
class JdbcDatabaseTool:
    jdbc_url: str
    echo: bool = False
    connect_args: Optional[Dict] = None

    def __post_init__(self) -> None:
        self.sqlalchemy_url = jdbc_to_sqlalchemy_url(self.jdbc_url)
        self.engine: Engine = create_engine(
            self.sqlalchemy_url,
            echo=self.echo,
            connect_args=self.connect_args or {},
        )

    def close(self) -> None:
        self.engine.dispose()

    def get_metadata(self, schema: Optional[str] = None, include_views: bool = False) -> Dict:
        inspector = inspect(self.engine)
        schemas = inspector.get_schema_names()

        target_schemas: List[Optional[str]]
        if schema:
            target_schemas = [schema]
        elif self.engine.dialect.name == "sqlite":
            target_schemas = [None]
        else:
            target_schemas = [s for s in schemas if s not in {"information_schema", "pg_catalog"}]

        tables_meta: List[Dict] = []
        for sc in target_schemas:
            table_names = inspector.get_table_names(schema=sc)
            if include_views:
                table_names += inspector.get_view_names(schema=sc)
            for table_name in sorted(set(table_names)):
                columns = inspector.get_columns(table_name, schema=sc)
                tables_meta.append(
                    {
                        "schema": sc,
                        "table": table_name,
                        "columns": [
                            {
                                "name": c.get("name"),
                                "type": str(c.get("type")),
                                "nullable": c.get("nullable"),
                            }
                            for c in columns
                        ],
                    }
                )

        return {
            "dialect": self.engine.dialect.name,
            "sqlalchemy_url": self.sqlalchemy_url,
            "schemas": schemas,
            "tables": tables_meta,
        }

    def get_sample(self, table_name: str, schema: Optional[str] = None, limit: int = 5) -> List[Dict]:
        n = max(1, int(limit))
        metadata = MetaData()
        table = Table(table_name, metadata, autoload_with=self.engine, schema=schema)
        stmt = select(table).limit(n)
        with self.engine.connect() as conn:
            rows = conn.execute(stmt).mappings().all()
        return [dict(r) for r in rows]

    def get_sample_page(
        self,
        schema: Optional[str],
        object_name: str,
        page: int = 1,
        page_size: int = 20,
        object_type: Optional[str] = None,
    ) -> Dict:
        """Get paginated sample rows from one table/view."""
        name = str(object_name or "").strip()
        if not name:
            raise ValueError("object_name must not be empty.")

        p = max(1, int(page))
        size = max(1, min(int(page_size), 500))
        offset = (p - 1) * size

        schema_name = schema
        if self.engine.dialect.name == "sqlite" and schema_name in {None, "", "main"}:
            schema_name = None

        inspector = inspect(self.engine)
        tables = set(inspector.get_table_names(schema=schema_name))
        views = set(inspector.get_view_names(schema=schema_name))

        resolved_type: str
        if object_type is not None:
            t = str(object_type).strip().lower()
            if t not in {"table", "view"}:
                raise ValueError("object_type must be 'table' or 'view'.")
            if t == "table" and name not in tables:
                raise ValueError(f"table not found: schema={schema!r}, name={name!r}")
            if t == "view" and name not in views:
                raise ValueError(f"view not found: schema={schema!r}, name={name!r}")
            resolved_type = t
        else:
            if name in tables:
                resolved_type = "table"
            elif name in views:
                resolved_type = "view"
            else:
                raise ValueError(f"table/view not found: schema={schema!r}, name={name!r}")

        qualified = _qualified_table_name(name, schema=schema_name)
        count_sql = f"SELECT COUNT(*) AS cnt FROM {qualified}"
        data_sql_default = f"SELECT * FROM {qualified} LIMIT :limit OFFSET :offset"

        columns_meta = inspector.get_columns(name, schema=schema_name)
        geometry_columns = {
            str(c.get("name"))
            for c in columns_meta
            if _is_geometry_column(c)
        }

        def _build_data_sql(geom_to_text_func: Optional[str]) -> str:
            select_parts: List[str] = []
            for c in columns_meta:
                col_name = str(c.get("name"))
                quoted = _qualified_column_name(col_name)
                if geom_to_text_func and col_name in geometry_columns:
                    select_parts.append(
                        f"{geom_to_text_func}({quoted}) AS {_quote_ident(col_name)}",
                    )
                else:
                    select_parts.append(quoted)
            select_clause = ", ".join(select_parts) if select_parts else "*"
            return f"SELECT {select_clause} FROM {qualified} LIMIT :limit OFFSET :offset"

        geometry_text_funcs: List[str] = []

        with self.engine.connect() as conn:
            if self.engine.dialect.name == "postgresql":
                schema_for_pg = schema_name or "public"
                try:
                    pg_geom_rows = conn.execute(
                        text(
                            """
                            SELECT column_name
                            FROM information_schema.columns
                            WHERE table_schema = :schema_name
                              AND table_name = :table_name
                              AND udt_name IN ('geometry', 'geography')
                            """
                        ),
                        {
                            "schema_name": schema_for_pg,
                            "table_name": name,
                        },
                    ).fetchall()
                    for r in pg_geom_rows:
                        geometry_columns.add(str(r[0]))
                except Exception:
                    # Keep reflection-based geometry detection if catalog lookup is unavailable.
                    pass

            if geometry_columns:
                if self.engine.dialect.name == "postgresql":
                    geometry_text_funcs = ["ST_AsText"]
                elif self.engine.dialect.name == "sqlite":
                    geometry_text_funcs = ["AsText", "ST_AsText"]
                else:
                    geometry_text_funcs = ["ST_AsText"]

            total = conn.execute(text(count_sql)).scalar()
            rows = None
            if geometry_text_funcs:
                for fn_name in geometry_text_funcs:
                    try:
                        sql_text = _build_data_sql(fn_name)
                        rows = conn.execute(
                            text(sql_text),
                            {"limit": size, "offset": offset},
                        ).mappings().all()
                        break
                    except Exception:
                        rows = None

            if rows is None:
                rows = conn.execute(
                    text(data_sql_default),
                    {"limit": size, "offset": offset},
                ).mappings().all()

        total_count = int(total) if total is not None else 0
        total_pages = (total_count + size - 1) // size if total_count > 0 else 0

        return {
            "schema": schema if schema is not None else (schema_name or ""),
            "name": name,
            "type": resolved_type,
            "page": p,
            "page_size": size,
            "column_names": [str(c.get("name")) for c in columns_meta],
            "total": total_count,
            "total_pages": total_pages,
            "page_row_count": len(rows),
            "rows": [dict(r) for r in rows],
        }

    def execute_sql_page(
        self,
        schema: Optional[str],
        sql: str,
        page: int = 1,
        page_size: int = 20,
    ) -> Dict:
        """Execute read-only SQL and return paginated rows in sample-page format."""
        text_sql = _assert_read_only_sql(sql).rstrip().rstrip(";").strip()
        if not text_sql:
            raise ValueError("sql must not be empty.")

        p = max(1, int(page))
        size = max(1, min(int(page_size), 500))
        offset = (p - 1) * size

        schema_name = str(schema or "").strip() or None
        if self.engine.dialect.name == "sqlite" and schema_name in {"main"}:
            schema_name = None
        if schema_name is not None:
            schema_name = _assert_identifier(schema_name, "schema")

        query_sql = f"SELECT * FROM ({text_sql}) AS _q"
        count_sql = f"SELECT COUNT(*) AS cnt FROM ({text_sql}) AS _q"
        data_sql_default = f"{query_sql} LIMIT :limit OFFSET :offset"

        def _set_schema_if_needed(conn) -> None:
            if self.engine.dialect.name == "postgresql" and schema_name:
                conn.execute(text(f"SET LOCAL search_path TO {_quote_ident(schema_name)}, public"))

        with self.engine.begin() as conn:
            _set_schema_if_needed(conn)
            probe_result = conn.execute(text(f"{query_sql} LIMIT 0"))
            column_names = [str(c) for c in probe_result.keys()]

            data_sql = data_sql_default
            if self.engine.dialect.name == "postgresql" and column_names:
                desc = None
                cursor = getattr(probe_result, "cursor", None)
                if cursor is not None:
                    desc = getattr(cursor, "description", None)

                geometry_col_indexes = set()
                if desc:
                    oid_values = []
                    for item in desc:
                        try:
                            oid_values.append(int(item[1]))
                        except Exception:
                            continue

                    if oid_values:
                        oid_list = ", ".join(str(x) for x in sorted(set(oid_values)))
                        type_rows = conn.execute(
                            text(f"SELECT oid, typname FROM pg_type WHERE oid IN ({oid_list})"),
                        ).fetchall()
                        type_map = {int(r[0]): str(r[1]).lower() for r in type_rows}
                        for idx, item in enumerate(desc):
                            try:
                                oid = int(item[1])
                            except Exception:
                                continue
                            if type_map.get(oid) in {"geometry", "geography"}:
                                geometry_col_indexes.add(idx)

                if geometry_col_indexes:
                    select_parts = []
                    for idx, col_name in enumerate(column_names):
                        alias = _quote_ident(col_name)
                        col_expr = f"_q.{alias}"
                        if idx in geometry_col_indexes:
                            select_parts.append(f"ST_AsText({col_expr}) AS {alias}")
                        else:
                            select_parts.append(f"{col_expr} AS {alias}")
                    select_clause = ", ".join(select_parts)
                    data_sql = f"SELECT {select_clause} FROM ({text_sql}) AS _q LIMIT :limit OFFSET :offset"

            _set_schema_if_needed(conn)
            total = conn.execute(text(count_sql)).scalar()
            _set_schema_if_needed(conn)
            rows = conn.execute(
                text(data_sql),
                {"limit": size, "offset": offset},
            ).mappings().all()

        total_count = int(total) if total is not None else 0
        total_pages = (total_count + size - 1) // size if total_count > 0 else 0

        return {
            "schema": schema if schema is not None else (schema_name or ""),
            "name": "sql_result",
            "type": "query",
            "page": p,
            "page_size": size,
            "column_names": column_names,
            "total": total_count,
            "total_pages": total_pages,
            "page_row_count": len(rows),
            "rows": [_normalize_row_cells(dict(r)) for r in rows],
        }

    def execute_sql(self, sql: str, limit: int = 200) -> Dict:
        text_sql = str(sql or "").strip()
        if not text_sql:
            raise ValueError("sql must not be empty.")

        fetch_limit = max(1, int(limit))
        with self.engine.begin() as conn:
            result = conn.execute(text(text_sql))
            if result.returns_rows:
                rows = result.mappings().fetchmany(fetch_limit + 1)
                truncated = len(rows) > fetch_limit
                if truncated:
                    rows = rows[:fetch_limit]
                return {
                    "returns_rows": True,
                    "columns": list(result.keys()),
                    "rows": [dict(r) for r in rows],
                    "row_count": len(rows),
                    "truncated": truncated,
                }
            return {
                "returns_rows": False,
                "row_count": result.rowcount if result.rowcount is not None else 0,
            }

    def introspect_catalog(self, schema: Optional[str] = None, include_views: bool = False) -> Dict:
        """Lightweight catalog introspection including columns/indexes/constraints."""
        inspector = inspect(self.engine)
        base = self.get_metadata(schema=schema, include_views=include_views)
        tables = base.get("tables", [])
        for t in tables:
            table_name = t.get("table")
            sc = t.get("schema")
            t["foreign_keys"] = inspector.get_foreign_keys(table_name, schema=sc)
            t["indexes"] = inspector.get_indexes(table_name, schema=sc)
            t["pk"] = inspector.get_pk_constraint(table_name, schema=sc)
        return base

    def estimate_rowcount(self, table_name: str, schema: Optional[str] = None) -> Dict:
        """Estimate row count, preferring planner stats on PostgreSQL."""
        t = _assert_identifier(table_name, "table_name")
        if self.engine.dialect.name == "postgresql":
            sql = """
            SELECT c.reltuples::bigint AS estimate
            FROM pg_class c
            JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE c.relkind = 'r'
              AND c.relname = :table_name
              AND n.nspname = COALESCE(:schema_name, current_schema())
            LIMIT 1
            """
            with self.engine.connect() as conn:
                estimate = conn.execute(
                    text(sql),
                    {"table_name": t, "schema_name": schema},
                ).scalar()
            return {
                "table": t,
                "schema": schema,
                "method": "pg_reltuples",
                "row_count_estimate": int(estimate) if estimate is not None else None,
            }

        qualified = _qualified_table_name(t, schema=schema)
        sql = f"SELECT COUNT(*) AS cnt FROM {qualified}"
        with self.engine.connect() as conn:
            cnt = conn.execute(text(sql)).scalar()
        return {
            "table": t,
            "schema": schema,
            "method": "count(*)",
            "row_count_estimate": int(cnt) if cnt is not None else None,
        }

    def topk_distinct(
        self,
        table_name: str,
        column_name: str,
        k: int = 10,
        schema: Optional[str] = None,
    ) -> Dict:
        """Get top-k distinct values for one column with strict limit."""
        t = _assert_identifier(table_name, "table_name")
        c = _assert_identifier(column_name, "column_name")
        top_k = max(1, min(int(k), 100))
        qualified_table = _qualified_table_name(t, schema=schema)
        quoted_col = _qualified_column_name(c)
        sql = f"""
        SELECT {quoted_col} AS value, COUNT(*) AS freq
        FROM {qualified_table}
        WHERE {quoted_col} IS NOT NULL
        GROUP BY {quoted_col}
        ORDER BY freq DESC
        LIMIT :k
        """
        with self.engine.connect() as conn:
            rows = conn.execute(text(sql), {"k": top_k}).mappings().all()
        return {
            "table": t,
            "schema": schema,
            "column": c,
            "k": top_k,
            "values": [dict(r) for r in rows],
        }

    def execute_readonly(
        self,
        sql: str,
        params: Optional[Dict] = None,
        timeout_ms: int = 8000,
        max_rows: int = 100,
    ) -> Dict:
        """Execute read-only SQL with timeout and max rows guardrails."""
        text_sql = _assert_read_only_sql(sql)
        bind_params = params or {}
        row_limit = max(1, int(max_rows))
        timeout = max(1, int(timeout_ms))
        begin_t = time.perf_counter()

        try:
            with self.engine.begin() as conn:
                if self.engine.dialect.name == "postgresql":
                    conn.execute(text(f"SET LOCAL statement_timeout = {timeout}"))

                result = conn.execute(text(text_sql), bind_params)
                if result.returns_rows:
                    rows = result.mappings().fetchmany(row_limit + 1)
                    truncated = len(rows) > row_limit
                    if truncated:
                        rows = rows[:row_limit]
                    elapsed_ms = int((time.perf_counter() - begin_t) * 1000)
                    return {
                        "status": "OK",
                        "returns_rows": True,
                        "columns": list(result.keys()),
                        "rows": [dict(r) for r in rows],
                        "row_count": len(rows),
                        "truncated": truncated,
                        "latency_ms": elapsed_ms,
                        "error": None,
                    }

                elapsed_ms = int((time.perf_counter() - begin_t) * 1000)
                return {
                    "status": "OK",
                    "returns_rows": False,
                    "row_count": result.rowcount if result.rowcount is not None else 0,
                    "latency_ms": elapsed_ms,
                    "error": None,
                }
        except Exception as exc:
            elapsed_ms = int((time.perf_counter() - begin_t) * 1000)
            return {
                "status": "ERROR",
                "returns_rows": False,
                "row_count": 0,
                "latency_ms": elapsed_ms,
                "error": str(exc),
            }

    def explain(self, sql: str, params: Optional[Dict] = None) -> Dict:
        """Run EXPLAIN for read-only SQL."""
        text_sql = _assert_read_only_sql(sql)
        bind_params = params or {}
        explain_sql = f"EXPLAIN {text_sql}"
        try:
            with self.engine.connect() as conn:
                result = conn.execute(text(explain_sql), bind_params)
                rows = result.fetchall()
            lines = [str(r[0]) if len(r) > 0 else str(r) for r in rows]
            return {"status": "OK", "plan_lines": lines}
        except Exception as exc:
            return {"status": "ERROR", "error": str(exc)}

    def list_tables_and_views(self, schema: Optional[str] = None) -> Dict:
        """List table names and view names for one schema."""
        inspector = inspect(self.engine)
        schema_name = schema
        if self.engine.dialect.name == "sqlite" and schema_name in {None, "", "main"}:
            schema_name = None

        table_names = inspector.get_table_names(schema=schema_name)
        view_names = inspector.get_view_names(schema=schema_name)

        # Filter sqlite internal objects.
        def _clean(names: List[str]) -> List[str]:
            out: List[str] = []
            for n in names:
                name = str(n or "").strip()
                if not name:
                    continue
                if name.startswith("sqlite_"):
                    continue
                out.append(name)
            return sorted(set(out))

        return {
            "schema": schema if schema is not None else (schema_name or ""),
            "tables": _clean(table_names),
            "views": _clean(view_names),
        }

    def get_object_columns(
        self,
        schema: Optional[str],
        object_name: str,
        object_type: str,
    ) -> Dict:
        """Get columns for one table or view."""
        obj_name = str(object_name or "").strip()
        if not obj_name:
            raise ValueError("object_name must not be empty.")

        obj_type = str(object_type or "").strip().lower()
        if obj_type not in {"table", "view"}:
            raise ValueError("object_type must be 'table' or 'view'.")

        inspector = inspect(self.engine)
        schema_name = schema
        if self.engine.dialect.name == "sqlite" and schema_name in {None, "", "main"}:
            schema_name = None

        if obj_type == "table":
            names = inspector.get_table_names(schema=schema_name)
        else:
            names = inspector.get_view_names(schema=schema_name)

        if obj_name not in names:
            raise ValueError(
                f"{obj_type} not found: schema={schema!r}, name={obj_name!r}",
            )

        columns = inspector.get_columns(obj_name, schema=schema_name)
        fields = [
            {
                "field_name": c.get("name"),
                "field_type": str(c.get("type")),
            }
            for c in columns
        ]
        return {
            "schema": schema if schema is not None else (schema_name or ""),
            "name": obj_name,
            "type": obj_type,
            "fields": fields,
        }

    def get_object_ddl(
        self,
        schema: Optional[str],
        object_name: str,
        object_type: str,
    ) -> Dict:
        """Get DDL for one table or view."""
        obj_name = str(object_name or "").strip()
        if not obj_name:
            raise ValueError("object_name must not be empty.")

        obj_type = str(object_type or "").strip().lower()
        if obj_type not in {"table", "view"}:
            raise ValueError("object_type must be 'table' or 'view'.")

        inspector = inspect(self.engine)
        schema_name = schema
        if self.engine.dialect.name == "sqlite" and schema_name in {None, "", "main"}:
            schema_name = None

        if obj_type == "table":
            table_names = inspector.get_table_names(schema=schema_name)
            if obj_name not in table_names:
                raise ValueError(f"table not found: schema={schema!r}, name={obj_name!r}")
            metadata = MetaData()
            table = Table(obj_name, metadata, autoload_with=self.engine, schema=schema_name)
            ddl_text = str(CreateTable(table).compile(self.engine)).strip()
            if not ddl_text.rstrip().endswith(";"):
                ddl_text = ddl_text.rstrip() + ";"
            return {
                "schema": schema if schema is not None else (schema_name or ""),
                "name": obj_name,
                "type": "table",
                "ddl": ddl_text,
            }

        view_names = inspector.get_view_names(schema=schema_name)
        if obj_name not in view_names:
            raise ValueError(f"view not found: schema={schema!r}, name={obj_name!r}")

        definition = inspector.get_view_definition(obj_name, schema=schema_name)
        if not definition:
            raise ValueError(f"view definition is unavailable: schema={schema!r}, name={obj_name!r}")

        qualified = _qualified_table_name(obj_name, schema=schema_name)
        ddl_text = f"CREATE VIEW {qualified} AS\n{definition}"
        ddl_text = ddl_text.strip()
        if not ddl_text.rstrip().endswith(";"):
            ddl_text = ddl_text.rstrip() + ";"
        return {
            "schema": schema if schema is not None else (schema_name or ""),
            "name": obj_name,
            "type": "view",
            "ddl": ddl_text,
        }
