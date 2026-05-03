"""Configuration loading for PostGIS migration."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Mapping

import yaml

from .core import DEFAULT_INSERT_BATCH_SIZE, DEFAULT_SOURCE_ROW_LIMIT, PostGISConnectionSettings


def _project_root() -> Path:
    return Path(__file__).resolve().parents[4]


DEFAULT_INPUT_PATH = _project_root() / "data" / "processed" / "synthesized_spatial_databases.jsonl"
DEFAULT_MIGRATE_CONFIG_PATH = _project_root() / "config" / "migrate.yaml"


@dataclass(frozen=True)
class MigrationRuntimeConfig:
    input_path: str = str(DEFAULT_INPUT_PATH)
    cities: str = "all"
    log_level: str = "INFO"
    insert_batch_size: int = DEFAULT_INSERT_BATCH_SIZE
    source_row_limit: int = DEFAULT_SOURCE_ROW_LIMIT
    connection: PostGISConnectionSettings = field(default_factory=PostGISConnectionSettings)


def _as_text(value: Any) -> str:
    return str(value).strip() if value is not None else ""


def _as_positive_int(value: Any, default: int) -> int:
    if value is None or value == "":
        return default
    parsed = int(value)
    if parsed <= 0:
        raise ValueError(f"Expected a positive integer, got: {value!r}")
    return parsed


def _as_row_limit(value: Any, default: int) -> int:
    if value is None or value == "":
        return default
    parsed = int(value)
    if parsed == -1 or parsed > 0:
        return parsed
    raise ValueError(f"Expected -1 or a positive integer, got: {value!r}")


def _resolve_input_path(raw_input: Any, config_path: Path) -> str:
    text = _as_text(raw_input)
    if not text:
        return str(DEFAULT_INPUT_PATH)
    path = Path(text)
    if path.is_absolute():
        return str(path)
    base_dir = config_path.parent.parent if config_path.parent.name == "config" else config_path.parent
    return str((base_dir / path).resolve())


def load_migration_config(config_path: str | Path | None = None) -> MigrationRuntimeConfig:
    path = Path(config_path or DEFAULT_MIGRATE_CONFIG_PATH)
    if not path.is_file():
        raise FileNotFoundError(f"Migration config file not found: {path}")
    payload = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    if not isinstance(payload, Mapping):
        raise ValueError(f"Invalid migration config in {path}: expected a mapping at the top level.")

    database_section = payload.get("database") or {}
    if database_section and not isinstance(database_section, Mapping):
        raise ValueError(f"Invalid migration config in {path}: 'database' must be a mapping.")
    logging_section = payload.get("logging") or {}
    if logging_section and not isinstance(logging_section, Mapping):
        raise ValueError(f"Invalid migration config in {path}: 'logging' must be a mapping.")

    default_connection = PostGISConnectionSettings()
    connection = PostGISConnectionSettings(
        host=_as_text(database_section.get("host")) or default_connection.host,
        port=_as_positive_int(database_section.get("port"), default_connection.port),
        user=_as_text(database_section.get("user")) or default_connection.user,
        password=_as_text(database_section.get("password")) or default_connection.password,
        catalog=_as_text(database_section.get("catalog")) or default_connection.catalog,
        bootstrap_db=(
            _as_text(database_section.get("bootstrap_db"))
            or _as_text(database_section.get("maintenance_db"))
            or default_connection.bootstrap_db
        ),
    )
    return MigrationRuntimeConfig(
        input_path=_resolve_input_path(payload.get("input"), path),
        cities=_as_text(payload.get("cities")) or "all",
        log_level=_as_text(logging_section.get("level")) or "INFO",
        insert_batch_size=_as_positive_int(payload.get("insert_batch_size"), DEFAULT_INSERT_BATCH_SIZE),
        source_row_limit=_as_row_limit(payload.get("source_row_limit"), DEFAULT_SOURCE_ROW_LIMIT),
        connection=connection,
    )
