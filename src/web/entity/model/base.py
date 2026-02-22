from __future__ import annotations

import os
from pathlib import Path
from typing import Any, Dict, Optional

import yaml
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from sqlalchemy import text
from sqlalchemy.orm import declarative_base, sessionmaker


PROJECT_ROOT = Path(__file__).resolve().parents[4]
DEFAULT_WEB_CONFIG_PATH = PROJECT_ROOT / "src" / "web" / "resources" / "config.yaml"

Base = declarative_base()

_engine: Optional[Engine] = None
_session_factory = None


def _load_web_config(config_path: Optional[str] = None) -> Dict[str, Any]:
    env_path = os.getenv("WEB_CONFIG_PATH")
    raw_path = config_path or env_path
    path = Path(raw_path) if raw_path else DEFAULT_WEB_CONFIG_PATH
    if not path.is_absolute():
        path = PROJECT_ROOT / path

    if not path.exists():
        raise FileNotFoundError(f"Web config not found: {path}")

    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


def _resolve_db_path(db_path: str) -> Path:
    p = Path(str(db_path))
    if p.is_absolute():
        return p
    return PROJECT_ROOT / p


def get_database_url(config_path: Optional[str] = None) -> str:
    cfg = _load_web_config(config_path)
    db_cfg = cfg.get("database", {}) or {}
    db_path = db_cfg.get("db_path", "spatial_agent.db")
    resolved = _resolve_db_path(str(db_path))
    resolved.parent.mkdir(parents=True, exist_ok=True)
    return f"sqlite:///{resolved}"


def init_engine(config_path: Optional[str] = None, echo: bool = False) -> Engine:
    global _engine, _session_factory

    if _engine is None:
        db_url = get_database_url(config_path=config_path)
        _engine = create_engine(db_url, echo=echo, future=True)
        _session_factory = sessionmaker(
            bind=_engine,
            autoflush=False,
            autocommit=False,
            expire_on_commit=False,
            future=True,
        )
    return _engine


def get_engine() -> Engine:
    return init_engine()


def get_session_factory():
    if _session_factory is None:
        init_engine()
    return _session_factory


def get_db_session():
    factory = get_session_factory()
    return factory()


def create_all_tables() -> None:
    engine = get_engine()
    Base.metadata.create_all(bind=engine)
    _run_lightweight_migrations(engine)


def _run_lightweight_migrations(engine: Engine) -> None:
    # Backward-compatible migration for existing sqlite databases.
    if engine.dialect.name != "sqlite":
        return

    with engine.begin() as conn:
        users_exists = conn.execute(
            text("SELECT name FROM sqlite_master WHERE type='table' AND name='users' LIMIT 1")
        ).scalar()
        if users_exists:
            user_rows = conn.execute(text("PRAGMA table_info(users)")).fetchall()
            user_columns = {str(r[1]) for r in user_rows}
            if "role" not in user_columns:
                conn.execute(text("ALTER TABLE users ADD COLUMN role TEXT NOT NULL DEFAULT 'user'"))
            if "status" not in user_columns:
                conn.execute(text("ALTER TABLE users ADD COLUMN status TEXT NOT NULL DEFAULT 'active'"))
            if "last_login" not in user_columns:
                conn.execute(text("ALTER TABLE users ADD COLUMN last_login DATETIME NULL"))
            conn.execute(text("UPDATE users SET role = 'user' WHERE role IS NULL OR TRIM(role) = ''"))
            conn.execute(
                text(
                    "UPDATE users SET status = 'active' "
                    "WHERE status IS NULL OR TRIM(status) = '' OR status NOT IN ('active', 'disabled')"
                )
            )
            conn.execute(text("CREATE INDEX IF NOT EXISTS idx_users_status ON users (status)"))

        table_exists = conn.execute(
            text("SELECT name FROM sqlite_master WHERE type='table' AND name='database_links' LIMIT 1")
        ).scalar()
        if not table_exists:
            return

        rows = conn.execute(text("PRAGMA table_info(database_links)")).fetchall()
        columns = {str(r[1]) for r in rows}  # cid, name, type, notnull, dflt_value, pk

        if "name" not in columns:
            conn.execute(text("ALTER TABLE database_links ADD COLUMN name TEXT NOT NULL DEFAULT ''"))
        if "db_username" not in columns:
            conn.execute(text("ALTER TABLE database_links ADD COLUMN db_username TEXT NULL"))
        if "db_password" not in columns:
            conn.execute(text("ALTER TABLE database_links ADD COLUMN db_password TEXT NULL"))

        # Chat session/history tables for persistent chat records and feedback.
        conn.execute(
            text(
                """
                CREATE TABLE IF NOT EXISTS chat_sessions (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id INTEGER NOT NULL,
                    insert_time DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    update_time DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
                )
                """
            )
        )
        conn.execute(text("CREATE INDEX IF NOT EXISTS idx_chat_sessions_user_id ON chat_sessions (user_id)"))

        conn.execute(
            text(
                """
                CREATE TABLE IF NOT EXISTS chat_history (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    chat_id INTEGER NOT NULL,
                    request_id INTEGER NULL,
                    role TEXT NOT NULL CHECK (role IN ('user', 'assistant')),
                    agent_name TEXT NULL,
                    content TEXT NOT NULL,
                    context_json TEXT NULL,
                    feedback TEXT NULL CHECK (feedback IS NULL OR feedback IN ('like', 'dislike')),
                    insert_time DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    update_time DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (chat_id) REFERENCES chat_sessions(id) ON DELETE CASCADE
                )
                """
            )
        )
        chat_history_exists = conn.execute(
            text("SELECT name FROM sqlite_master WHERE type='table' AND name='chat_history' LIMIT 1")
        ).scalar()
        if chat_history_exists:
            chat_rows = conn.execute(text("PRAGMA table_info(chat_history)")).fetchall()
            chat_columns = {str(r[1]) for r in chat_rows}
            if "feedback" not in chat_columns:
                conn.execute(text("ALTER TABLE chat_history ADD COLUMN feedback TEXT NULL"))
            if "request_id" not in chat_columns:
                conn.execute(text("ALTER TABLE chat_history ADD COLUMN request_id INTEGER NULL"))
            if "agent_name" not in chat_columns:
                conn.execute(text("ALTER TABLE chat_history ADD COLUMN agent_name TEXT NULL"))
            if "context_json" not in chat_columns:
                conn.execute(text("ALTER TABLE chat_history ADD COLUMN context_json TEXT NULL"))

            if "chat_session_id" in chat_columns or "user_id" in chat_columns:
                # Rebuild chat_history to retire legacy chat_session_id/user_id and use chat_id=session_id only.
                chat_id_source_expr = "chat_session_id" if "chat_session_id" in chat_columns else "chat_id"
                conn.execute(text("DROP TABLE IF EXISTS chat_history__new"))
                conn.execute(
                    text(
                        """
                        CREATE TABLE chat_history__new (
                            id INTEGER PRIMARY KEY AUTOINCREMENT,
                            chat_id INTEGER NOT NULL,
                            request_id INTEGER NULL,
                            role TEXT NOT NULL CHECK (role IN ('user', 'assistant')),
                            agent_name TEXT NULL,
                            content TEXT NOT NULL,
                            context_json TEXT NULL,
                            feedback TEXT NULL CHECK (feedback IS NULL OR feedback IN ('like', 'dislike')),
                            insert_time DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                            update_time DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                            FOREIGN KEY (chat_id) REFERENCES chat_sessions(id) ON DELETE CASCADE
                        )
                        """
                    )
                )
                conn.execute(
                    text(
                        f"""
                        INSERT INTO chat_history__new (
                            id, chat_id, request_id, role, agent_name, content,
                            context_json, feedback, insert_time, update_time
                        )
                        SELECT
                            id,
                            CAST({chat_id_source_expr} AS INTEGER) AS chat_id,
                            request_id,
                            role,
                            agent_name,
                            content,
                            context_json,
                            feedback,
                            insert_time,
                            update_time
                        FROM chat_history
                        """
                    )
                )
                conn.execute(text("DROP TABLE chat_history"))
                conn.execute(text("ALTER TABLE chat_history__new RENAME TO chat_history"))
                chat_rows = conn.execute(text("PRAGMA table_info(chat_history)")).fetchall()
                chat_columns = {str(r[1]) for r in chat_rows}

        chat_sessions_exists = conn.execute(
            text("SELECT name FROM sqlite_master WHERE type='table' AND name='chat_sessions' LIMIT 1")
        ).scalar()
        if chat_sessions_exists and chat_history_exists:
            session_rows = conn.execute(text("PRAGMA table_info(chat_sessions)")).fetchall()
            session_columns = {str(r[1]) for r in session_rows}
            if "context_json" in session_columns:
                # Best-effort compatibility backfill before session-level context is retired in code.
                conn.execute(
                    text(
                        """
                        UPDATE chat_history
                        SET context_json = (
                            SELECT cs.context_json
                            FROM chat_sessions cs
                            WHERE cs.id = chat_history.chat_id
                        )
                        WHERE role = 'user'
                          AND (context_json IS NULL OR TRIM(context_json) = '')
                          AND EXISTS (
                              SELECT 1
                              FROM chat_sessions cs2
                              WHERE cs2.id = chat_history.chat_id
                                AND cs2.context_json IS NOT NULL
                                AND TRIM(cs2.context_json) <> ''
                          )
                        """
                    )
                )

        conn.execute(text("DROP INDEX IF EXISTS idx_chat_history_session_id"))
        conn.execute(text("CREATE INDEX IF NOT EXISTS idx_chat_history_chat_id ON chat_history (chat_id)"))
        conn.execute(text("DROP INDEX IF EXISTS idx_chat_history_user_chat_id"))
        conn.execute(text("CREATE INDEX IF NOT EXISTS idx_chat_history_request_id ON chat_history (request_id)"))
        conn.execute(text("CREATE INDEX IF NOT EXISTS idx_chat_history_agent_name ON chat_history (agent_name)"))
        conn.execute(text("CREATE INDEX IF NOT EXISTS idx_chat_history_feedback ON chat_history (feedback)"))

        conn.execute(
            text(
                """
                CREATE TABLE IF NOT EXISTS sql_execution_logs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id INTEGER NOT NULL,
                    chat_id INTEGER NULL,
                    database_id INTEGER NOT NULL,
                    execute_status TEXT NOT NULL CHECK (execute_status IN ('success', 'failure')),
                    sql_text TEXT NULL,
                    execution_time_ms INTEGER NOT NULL DEFAULT 0 CHECK (execution_time_ms >= 0),
                    row_count INTEGER NOT NULL DEFAULT 0 CHECK (row_count >= 0),
                    insert_time DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    update_time DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE,
                    FOREIGN KEY (chat_id) REFERENCES chat_sessions(id) ON DELETE SET NULL,
                    FOREIGN KEY (database_id) REFERENCES database_links(id) ON DELETE CASCADE
                )
                """
            )
        )
        sql_log_exists = conn.execute(
            text("SELECT name FROM sqlite_master WHERE type='table' AND name='sql_execution_logs' LIMIT 1")
        ).scalar()
        if sql_log_exists:
            sql_log_rows = conn.execute(text("PRAGMA table_info(sql_execution_logs)")).fetchall()
            sql_log_columns = {str(r[1]) for r in sql_log_rows}
            if "chat_id" not in sql_log_columns:
                conn.execute(text("ALTER TABLE sql_execution_logs ADD COLUMN chat_id INTEGER NULL"))
            if "execute_status" not in sql_log_columns:
                conn.execute(text("ALTER TABLE sql_execution_logs ADD COLUMN execute_status TEXT NOT NULL DEFAULT 'failure'"))
            if "sql_text" not in sql_log_columns:
                conn.execute(text("ALTER TABLE sql_execution_logs ADD COLUMN sql_text TEXT NULL"))
            if "execution_time_ms" not in sql_log_columns:
                conn.execute(text("ALTER TABLE sql_execution_logs ADD COLUMN execution_time_ms INTEGER NOT NULL DEFAULT 0"))
            if "row_count" not in sql_log_columns:
                conn.execute(text("ALTER TABLE sql_execution_logs ADD COLUMN row_count INTEGER NOT NULL DEFAULT 0"))

        conn.execute(text("CREATE INDEX IF NOT EXISTS idx_sql_execution_logs_user_id ON sql_execution_logs (user_id)"))
        conn.execute(text("CREATE INDEX IF NOT EXISTS idx_sql_execution_logs_chat_id ON sql_execution_logs (chat_id)"))
        conn.execute(text("CREATE INDEX IF NOT EXISTS idx_sql_execution_logs_database_id ON sql_execution_logs (database_id)"))
        conn.execute(text("CREATE INDEX IF NOT EXISTS idx_sql_execution_logs_status ON sql_execution_logs (execute_status)"))
        conn.execute(text("CREATE INDEX IF NOT EXISTS idx_sql_execution_logs_insert_time ON sql_execution_logs (insert_time)"))

        conn.execute(
            text(
                """
                CREATE TRIGGER IF NOT EXISTS trg_chat_sessions_update_time
                AFTER UPDATE ON chat_sessions
                FOR EACH ROW
                WHEN NEW.update_time = OLD.update_time
                BEGIN
                    UPDATE chat_sessions
                    SET update_time = CURRENT_TIMESTAMP
                    WHERE id = OLD.id;
                END
                """
            )
        )
        conn.execute(
            text(
                """
                CREATE TRIGGER IF NOT EXISTS trg_chat_history_update_time
                AFTER UPDATE ON chat_history
                FOR EACH ROW
                WHEN NEW.update_time = OLD.update_time
                BEGIN
                    UPDATE chat_history
                    SET update_time = CURRENT_TIMESTAMP
                    WHERE id = OLD.id;
                END
                """
            )
        )
        conn.execute(
            text(
                """
                CREATE TRIGGER IF NOT EXISTS trg_sql_execution_logs_update_time
                AFTER UPDATE ON sql_execution_logs
                FOR EACH ROW
                WHEN NEW.update_time = OLD.update_time
                BEGIN
                    UPDATE sql_execution_logs
                    SET update_time = CURRENT_TIMESTAMP
                    WHERE id = OLD.id;
                END
                """
            )
        )
