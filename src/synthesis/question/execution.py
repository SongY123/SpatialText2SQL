"""Database-backed execution result fetching for question generation prompts."""

from __future__ import annotations

import logging
import time
from threading import Lock
from typing import Any, Mapping

from psycopg2 import sql as pg_sql
from psycopg2.extras import RealDictCursor
from psycopg2.pool import ThreadedConnectionPool

from src.synthesis.database.migration import normalize_postgres_identifier
from src.synthesis.database.utils import stable_jsonify, to_text

from .config import QuestionExecutionConfig, QuestionGenerationDBConfig

LOGGER = logging.getLogger(__name__)


class QuestionExecutionResultFetcher:
    def __init__(
        self,
        db_config: QuestionGenerationDBConfig,
        execution_config: QuestionExecutionConfig,
    ) -> None:
        self.db_config = db_config
        self.execution_config = execution_config
        self._pool: ThreadedConnectionPool | None = None
        self._pool_lock = Lock()

    @staticmethod
    def needs_live_fetch(execution_result: Any) -> bool:
        normalized = execution_result if isinstance(execution_result, Mapping) else {}
        if not normalized:
            return True
        columns = [str(item).strip() for item in (normalized.get("columns") or []) if str(item).strip()]
        if columns and all(item.upper() == "QUERY PLAN" for item in columns):
            return True
        sample_rows = normalized.get("sample_rows") or normalized.get("rows") or []
        if isinstance(sample_rows, list) and sample_rows:
            only_query_plan = True
            for row in sample_rows:
                if not isinstance(row, Mapping):
                    only_query_plan = False
                    break
                keys = [str(key).strip().upper() for key in row.keys() if str(key).strip()]
                if not keys or any(key != "QUERY PLAN" for key in keys):
                    only_query_plan = False
                    break
            if only_query_plan:
                return True
        error_message = to_text(normalized.get("error_message")).lower()
        if any(token in error_message for token in ("timeout", "timed out", "statement timeout")):
            return True
        if normalized.get("success") and columns and any(item.upper() != "QUERY PLAN" for item in columns):
            return False
        if normalized.get("success") and sample_rows:
            return False
        return True

    def fetch(self, *, sql: str, database_id: str) -> dict[str, Any]:
        sql_text = to_text(sql).rstrip(";")
        if not sql_text:
            return {
                "executed": False,
                "success": False,
                "empty_result": False,
                "row_count": 0,
                "sample_rows": [],
                "error_message": "Empty SQL query.",
            }

        catalog_name = normalize_postgres_identifier(self.db_config.database, prefix="catalog") or self.db_config.database
        schema_name = normalize_postgres_identifier(database_id, prefix="schema")
        start = time.perf_counter()
        conn = None
        try:
            conn = self._acquire_connection(catalog_name)
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                self._apply_session_settings(cur, schema_name)
                LOGGER.info(
                    "Question execution fetch start | database_id=%s | catalog=%s | schema=%s | sql_chars=%s",
                    database_id,
                    catalog_name,
                    schema_name,
                    len(sql_text),
                )
                cur.execute(sql_text)
                rows = cur.fetchmany(int(self.execution_config.max_result_rows))
                columns = [str(desc.name).strip() for desc in (cur.description or []) if str(desc.name).strip()]
            elapsed_ms = (time.perf_counter() - start) * 1000.0
            sample_rows = [dict(row) for row in rows] if rows else []
            LOGGER.info(
                "Question execution fetch done | database_id=%s | success=%s | row_count=%s | time_ms=%.1f",
                database_id,
                True,
                len(sample_rows),
                elapsed_ms,
            )
            return {
                "executed": True,
                "success": True,
                "empty_result": len(sample_rows) == 0,
                "row_count": len(sample_rows),
                "columns": stable_jsonify(columns),
                "sample_rows": stable_jsonify(sample_rows),
                "execution_time_ms": elapsed_ms,
                "catalog_name": catalog_name,
                "schema_name": schema_name,
            }
        except Exception as exc:
            elapsed_ms = (time.perf_counter() - start) * 1000.0
            LOGGER.warning(
                "Question execution fetch failed | database_id=%s | time_ms=%.1f | error=%s",
                database_id,
                elapsed_ms,
                exc,
            )
            return {
                "executed": True,
                "success": False,
                "empty_result": False,
                "row_count": 0,
                "sample_rows": [],
                "execution_time_ms": elapsed_ms,
                "error_message": to_text(exc),
                "catalog_name": catalog_name,
                "schema_name": schema_name,
            }
        finally:
            if conn is not None:
                self._release_connection(conn)

    def _acquire_connection(self, catalog_name: str):
        pool = self._get_pool(catalog_name)
        conn = pool.getconn()
        conn.autocommit = False
        return conn

    def _release_connection(self, conn) -> None:
        if self._pool is None:
            conn.close()
            return
        try:
            conn.rollback()
        except Exception:
            self._pool.putconn(conn, close=True)
            return
        self._pool.putconn(conn)

    def _get_pool(self, catalog_name: str) -> ThreadedConnectionPool:
        if self._pool is not None:
            return self._pool
        with self._pool_lock:
            if self._pool is None:
                self._pool = ThreadedConnectionPool(
                    minconn=int(self.db_config.pool_min_size),
                    maxconn=int(self.db_config.pool_max_size),
                    host=self.db_config.host,
                    port=self.db_config.port,
                    dbname=catalog_name,
                    user=self.db_config.user,
                    password=self.db_config.password,
                    connect_timeout=self.db_config.connect_timeout,
                )
        return self._pool

    def _apply_session_settings(self, cursor, schema_name: str) -> None:
        cursor.execute(
            pg_sql.SQL("SET search_path TO {}, public").format(pg_sql.Identifier(schema_name))
        )
        cursor.execute("SET SESSION CHARACTERISTICS AS TRANSACTION READ ONLY")
        cursor.execute("SET statement_timeout = %s", (int(self.execution_config.execution_timeout * 1000),))
