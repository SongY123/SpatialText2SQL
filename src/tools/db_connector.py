from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List, Optional
from urllib.parse import parse_qs, quote_plus, urlencode, urlparse

from sqlalchemy import MetaData, Table, create_engine, inspect, select, text
from sqlalchemy.engine import Engine


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
