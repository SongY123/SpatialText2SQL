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
        table_exists = conn.execute(
            text("SELECT name FROM sqlite_master WHERE type='table' AND name='database_links' LIMIT 1")
        ).scalar()
        if not table_exists:
            return

        rows = conn.execute(text("PRAGMA table_info(database_links)")).fetchall()
        columns = {str(r[1]) for r in rows}  # cid, name, type, notnull, dflt_value, pk

        if "db_username" not in columns:
            conn.execute(text("ALTER TABLE database_links ADD COLUMN db_username TEXT NULL"))
        if "db_password" not in columns:
            conn.execute(text("ALTER TABLE database_links ADD COLUMN db_password TEXT NULL"))
