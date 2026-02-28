from __future__ import annotations

import hashlib
import re
import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, List, Optional, Sequence, Tuple

from sqlalchemy import text
from sqlalchemy.engine import Engine

from utils.logger import logger


_MIGRATION_FILE_RE = re.compile(r"^V(?P<version>\d+(?:_\d+)*)__(?P<description>.+)\.sql$")


@dataclass(frozen=True)
class SqlMigrationFile:
    version: str
    description: str
    filename: str
    path: Path
    checksum: str

    @property
    def version_key(self) -> Tuple[int, ...]:
        return tuple(int(part) for part in self.version.split("_"))


class SqlMigrationRunner:
    def __init__(self, engine: Engine, sql_dir: Path) -> None:
        self.engine = engine
        self.sql_dir = sql_dir

    def run(self) -> None:
        if not self.sql_dir.exists():
            logger.warning("SQL migration directory not found, skip: %s", self.sql_dir)
            return
        if not self.sql_dir.is_dir():
            raise RuntimeError(f"SQL migration path is not a directory: {self.sql_dir}")

        migrations = self._load_migration_files()
        if not migrations:
            logger.info("No SQL migration files found under %s", self.sql_dir)
            return

        with self.engine.begin() as conn:
            self._ensure_history_table(conn)
            applied_rows = conn.execute(
                text(
                    """
                    SELECT version, script_name, checksum
                    FROM schema_migrations
                    ORDER BY version ASC
                    """
                )
            ).mappings().all()
            applied_by_version = {str(row["version"]): row for row in applied_rows}

            pending: List[SqlMigrationFile] = []
            for migration in migrations:
                row = applied_by_version.get(migration.version)
                if row is None:
                    pending.append(migration)
                    continue

                recorded_name = str(row["script_name"])
                recorded_checksum = str(row["checksum"])
                if recorded_name != migration.filename or recorded_checksum != migration.checksum:
                    raise RuntimeError(
                        "SQL migration checksum mismatch detected for "
                        f"{migration.filename}. Recorded migration history no longer matches local files."
                    )
                logger.info("SQL migration verified: version=%s file=%s", migration.version, migration.filename)

            if not pending:
                logger.info("SQL migrations are up to date. directory=%s", self.sql_dir)
                return

            logger.info("Applying %d SQL migration file(s) from %s", len(pending), self.sql_dir)
            for migration in pending:
                logger.info("Applying SQL migration: version=%s file=%s", migration.version, migration.filename)
                sql_text = migration.path.read_text(encoding="utf-8")
                self._execute_sql_script(conn, sql_text)
                conn.execute(
                    text(
                        """
                        INSERT INTO schema_migrations (version, script_name, checksum)
                        VALUES (:version, :script_name, :checksum)
                        """
                    ),
                    {
                        "version": migration.version,
                        "script_name": migration.filename,
                        "checksum": migration.checksum,
                    },
                )
                logger.info("Applied SQL migration: version=%s file=%s", migration.version, migration.filename)

    def _load_migration_files(self) -> List[SqlMigrationFile]:
        migrations: List[SqlMigrationFile] = []
        seen_versions = set()

        for path in sorted(self.sql_dir.iterdir()):
            if not path.is_file() or path.suffix.lower() != ".sql":
                continue

            match = _MIGRATION_FILE_RE.match(path.name)
            if match is None:
                raise RuntimeError(
                    "Invalid SQL migration filename: "
                    f"{path.name}. Expected format like V1__xxx-ddl.sql or V1_0_0_1__xxx-dml.sql"
                )

            version = match.group("version")
            if version in seen_versions:
                raise RuntimeError(f"Duplicate SQL migration version detected: {version}")
            seen_versions.add(version)

            checksum = hashlib.sha256(path.read_bytes()).hexdigest()
            migrations.append(
                SqlMigrationFile(
                    version=version,
                    description=match.group("description"),
                    filename=path.name,
                    path=path,
                    checksum=checksum,
                )
            )

        migrations.sort(key=lambda item: (item.version_key, item.filename))
        return migrations

    @staticmethod
    def _ensure_history_table(conn) -> None:
        conn.execute(
            text(
                """
                CREATE TABLE IF NOT EXISTS schema_migrations (
                    version TEXT PRIMARY KEY,
                    script_name TEXT NOT NULL UNIQUE,
                    checksum TEXT NOT NULL,
                    installed_on DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
                )
                """
            )
        )

    def _execute_sql_script(self, conn, sql_text: str) -> None:
        if self.engine.dialect.name == "sqlite":
            for stmt in self._split_sqlite_statements(sql_text):
                conn.exec_driver_sql(stmt)
            return

        for stmt in self._split_generic_statements(sql_text):
            conn.exec_driver_sql(stmt)

    @staticmethod
    def _split_sqlite_statements(sql_text: str) -> List[str]:
        statements: List[str] = []
        buffer: List[str] = []

        for line in str(sql_text or "").splitlines(keepends=True):
            buffer.append(line)
            candidate = "".join(buffer).strip()
            if not candidate:
                continue
            if sqlite3.complete_statement(candidate):
                statements.append(candidate)
                buffer = []

        trailing = "".join(buffer).strip()
        if trailing:
            statements.append(trailing)
        return [stmt for stmt in statements if stmt.strip()]

    @staticmethod
    def _split_generic_statements(sql_text: str) -> List[str]:
        text_value = str(sql_text or "")
        statements: List[str] = []
        current: List[str] = []
        in_single = False
        in_double = False
        in_line_comment = False
        in_block_comment = False

        i = 0
        n = len(text_value)
        while i < n:
            ch = text_value[i]
            nxt = text_value[i + 1] if i + 1 < n else ""

            if in_line_comment:
                current.append(ch)
                if ch == "\n":
                    in_line_comment = False
                i += 1
                continue

            if in_block_comment:
                current.append(ch)
                if ch == "*" and nxt == "/":
                    current.append(nxt)
                    in_block_comment = False
                    i += 2
                    continue
                i += 1
                continue

            if ch == "-" and nxt == "-" and not in_single and not in_double:
                current.extend([ch, nxt])
                in_line_comment = True
                i += 2
                continue

            if ch == "/" and nxt == "*" and not in_single and not in_double:
                current.extend([ch, nxt])
                in_block_comment = True
                i += 2
                continue

            if ch == "'" and not in_double:
                in_single = not in_single
                current.append(ch)
                i += 1
                continue

            if ch == '"' and not in_single:
                in_double = not in_double
                current.append(ch)
                i += 1
                continue

            if ch == ";" and not in_single and not in_double:
                statement = "".join(current).strip()
                if statement:
                    statements.append(statement)
                current = []
                i += 1
                continue

            current.append(ch)
            i += 1

        trailing = "".join(current).strip()
        if trailing:
            statements.append(trailing)
        return statements
