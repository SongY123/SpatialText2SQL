"""Execution checking for synthesized SQL queries."""

from __future__ import annotations

import logging
from threading import Lock
import time
from typing import Any

from psycopg2.pool import ThreadedConnectionPool
from psycopg2 import sql as pg_sql
from psycopg2.extras import RealDictCursor

from src.synthesis.database.migration import normalize_postgres_identifier
from src.synthesis.database.models import SynthesizedSpatialDatabase
from src.synthesis.database.utils import stable_jsonify, to_text

from .config import SQLExecutionCheckConfig, SQLSynthesisDBConfig
from .models import SQLExecutionResult

LOGGER = logging.getLogger(__name__)


class SQLExecutionChecker:
    def __init__(
        self,
        db_config: SQLSynthesisDBConfig,
        execution_config: SQLExecutionCheckConfig,
    ) -> None:
        self.db_config = db_config
        self.execution_config = execution_config
        self._pool: ThreadedConnectionPool | None = None
        self._pool_lock = Lock()

    def check(self, sql: str, database: SynthesizedSpatialDatabase) -> SQLExecutionResult:
        sql_text = to_text(sql).rstrip(";")
        if not self.execution_config.enable_execution_check or self.execution_config.dry_run:
            LOGGER.info(
                "Execution skipped | schema_id=%s | enable_execution_check=%s | dry_run=%s",
                database.database_id,
                self.execution_config.enable_execution_check,
                self.execution_config.dry_run,
            )
            return SQLExecutionResult(executed=False, success=True)
        catalog_name = (
            normalize_postgres_identifier(self.db_config.database, prefix="catalog")
            or self.db_config.database
        )
        schema_name = normalize_postgres_identifier(database.database_id, prefix="schema")
        actual_database = f"{catalog_name}.{schema_name}"
        start = time.perf_counter()
        LOGGER.info(
            "Execution connect start | schema_id=%s | target=%s | sql_chars=%s",
            database.database_id,
            actual_database,
            len(sql_text),
        )
        conn = None
        try:
            conn = self._acquire_connection(catalog_name)
        except Exception as exc:
            return SQLExecutionResult(
                executed=False,
                success=False,
                error_message=f"Database connection failed: {exc}",
                actual_database=actual_database,
            )

        try:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                self._apply_session_settings(cur, schema_name)
                execution_sql = f"EXPLAIN {sql_text}"
                LOGGER.info(
                    "Execution query start | schema_id=%s | target=%s | explain_only=%s",
                    database.database_id,
                    actual_database,
                    True,
                )
                cur.execute(execution_sql)
                rows = cur.fetchmany(self.execution_config.max_result_rows_for_check)
            elapsed_ms = (time.perf_counter() - start) * 1000.0
            sample_rows = [dict(row) for row in rows] if rows else []
            empty_result = len(sample_rows) == 0
            success = True
            LOGGER.info(
                "Execution query done | schema_id=%s | target=%s | success=%s | empty_result=%s | row_count=%s | time_ms=%.1f",
                database.database_id,
                actual_database,
                success,
                empty_result,
                len(sample_rows),
                elapsed_ms,
            )
            return SQLExecutionResult(
                executed=True,
                success=success,
                error_message="",
                row_count=len(sample_rows),
                empty_result=empty_result,
                sample_rows=stable_jsonify(sample_rows),
                execution_time_ms=elapsed_ms,
                actual_database=actual_database,
            )
        except Exception as exc:
            elapsed_ms = (time.perf_counter() - start) * 1000.0
            if self._is_timeout_error(exc):
                LOGGER.warning(
                    "Execution explain timed out but sample is kept | schema_id=%s | target=%s | time_ms=%.1f | error=%s",
                    database.database_id,
                    actual_database,
                    elapsed_ms,
                    exc,
                )
                return SQLExecutionResult(
                    executed=True,
                    success=True,
                    error_message=str(exc),
                    row_count=0,
                    empty_result=False,
                    sample_rows=[],
                    execution_time_ms=elapsed_ms,
                    actual_database=actual_database,
                )
            LOGGER.warning(
                "Execution query failed | schema_id=%s | target=%s | time_ms=%.1f | error=%s",
                database.database_id,
                actual_database,
                elapsed_ms,
                exc,
            )
            return SQLExecutionResult(
                executed=True,
                success=False,
                error_message=str(exc),
                row_count=0,
                empty_result=False,
                sample_rows=[],
                execution_time_ms=elapsed_ms,
                actual_database=actual_database,
            )
        finally:
            if conn is not None:
                self._release_connection(conn)

    def _acquire_connection(self, actual_database: str):
        pool = self._get_pool(actual_database)
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

    def _get_pool(self, actual_database: str) -> ThreadedConnectionPool:
        if self._pool is not None:
            return self._pool
        with self._pool_lock:
            if self._pool is None:
                self._pool = ThreadedConnectionPool(
                    minconn=int(self.db_config.pool_min_size),
                    maxconn=int(self.db_config.pool_max_size),
                    host=self.db_config.host,
                    port=self.db_config.port,
                    dbname=actual_database,
                    user=self.db_config.user,
                    password=self.db_config.password,
                    connect_timeout=self.db_config.connect_timeout,
                )
        return self._pool

    @staticmethod
    def _is_timeout_error(error: Exception) -> bool:
        lowered = to_text(error).lower()
        return "timeout" in lowered or "timed out" in lowered or "statement timeout" in lowered

    def _apply_session_settings(self, cursor, schema_name: str) -> None:
        cursor.execute(
            pg_sql.SQL("SET search_path TO {}, public").format(pg_sql.Identifier(schema_name))
        )
        cursor.execute("SET SESSION CHARACTERISTICS AS TRANSACTION READ ONLY")
        cursor.execute("SET statement_timeout = %s", (int(self.execution_config.execution_timeout * 1000),))
