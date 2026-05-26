"""Shared dataset-to-database routing helpers."""

from __future__ import annotations

from typing import Any, Dict, Iterable, Optional

from psycopg2 import sql as pg_sql

from src.datasets.names import canonicalize_dataset_name


def extract_embedded_db_config(dataset_config: Dict[str, Any]) -> Dict[str, Any]:
    database = dataset_config.get("database")
    databases = dataset_config.get("databases")
    if not isinstance(database, dict) and not isinstance(databases, dict):
        return {}
    return {
        "database": database if isinstance(database, dict) else {},
        "databases": databases if isinstance(databases, dict) else {},
    }


def resolve_database_key(
    dataset_config: Dict[str, Any],
    dataset_name: str,
    metadata: Optional[Dict[str, Any]] = None,
    *,
    allow_fallback_mapping: bool = False,
) -> Optional[str]:
    dataset_name = canonicalize_dataset_name(dataset_name)
    datasets = dataset_config.get("datasets", {})
    dataset_info = datasets.get(dataset_name, {}) if isinstance(datasets, dict) else {}
    metadata = metadata or {}

    explicit_key = metadata.get("database_key") or metadata.get("db_key")
    if explicit_key:
        return str(explicit_key)

    routing = dataset_info.get("database_by_metadata", {})
    if isinstance(routing, dict):
        for field_name, value_mapping in routing.items():
            if not isinstance(value_mapping, dict):
                continue
            field_value = metadata.get(field_name)
            if field_value is None:
                continue
            resolved_key = value_mapping.get(field_value)
            if resolved_key is None:
                resolved_key = value_mapping.get(str(field_value))
            if resolved_key:
                return str(resolved_key)

        if allow_fallback_mapping:
            for value_mapping in routing.values():
                if not isinstance(value_mapping, dict) or not value_mapping:
                    continue
                first_value = next(iter(value_mapping.values()), None)
                if first_value:
                    return str(first_value)

    default_key = dataset_info.get("database")
    if default_key:
        return str(default_key)
    return None


def resolve_db_settings(
    db_config_full: Dict[str, Any],
    dataset_config: Dict[str, Any],
    dataset_name: str,
    metadata: Optional[Dict[str, Any]] = None,
    *,
    allow_fallback_mapping: bool = False,
) -> Dict[str, Any]:
    dataset_name = canonicalize_dataset_name(dataset_name)
    db_key = resolve_database_key(
        dataset_config,
        dataset_name,
        metadata,
        allow_fallback_mapping=allow_fallback_mapping,
    )
    databases = db_config_full.get("databases", {})
    if db_key and isinstance(databases, dict) and db_key in databases:
        return databases.get(db_key) or {}
    return db_config_full.get("database", {}) or {}


def resolve_schema_name(db_settings: Dict[str, Any]) -> str:
    schema = str(db_settings.get("schema") or "").strip()
    if schema:
        return schema

    raw_search_path = str(db_settings.get("search_path") or "").strip()
    if not raw_search_path:
        return "public"

    first_entry = raw_search_path.split(",", 1)[0].strip().strip('"')
    return first_entry or "public"


def build_schema_cache_key(
    dataset_name: str,
    db_key: Optional[str],
    db_settings: Dict[str, Any],
) -> str:
    schema_name = resolve_schema_name(db_settings)
    if db_key:
        return f"{dataset_name}_{db_key}_{schema_name}"
    database_name = str(db_settings.get("database") or dataset_name).strip() or dataset_name
    return f"{dataset_name}_{database_name}_{schema_name}"


def search_path_entries(db_settings: Dict[str, Any]) -> list[str]:
    configured = str(db_settings.get("search_path") or "").strip()
    if configured:
        return [entry.strip().strip('"') for entry in configured.split(",") if entry.strip()]

    schema_name = str(db_settings.get("schema") or "").strip()
    if schema_name:
        return [schema_name, "public"]
    return []


def apply_search_path(cursor, db_settings: Dict[str, Any]) -> None:
    entries = search_path_entries(db_settings)
    if not entries:
        return

    rendered: list[Any] = []
    for entry in entries:
        if entry.lower() == "public":
            rendered.append(pg_sql.Identifier("public"))
            continue
        if entry == "$user":
            rendered.append(pg_sql.SQL('"$user"'))
            continue
        rendered.append(pg_sql.Identifier(entry))

    cursor.execute(
        pg_sql.SQL("SET search_path TO {}").format(
            pg_sql.SQL(", ").join(rendered)
        )
    )


def first_nonempty(items: Iterable[Any]) -> Optional[Any]:
    for item in items:
        if item:
            return item
    return None
