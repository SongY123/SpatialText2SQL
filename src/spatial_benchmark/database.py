from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from .clustering import cluster_table_membership, load_scenario_clusters
from .scenario_specs import SCENARIO_DATABASE_SPECS, TableDef
from .taxonomy import CATEGORY_TAXONOMY, SCENARIO_TO_GGIM_CATEGORIES


def write_json(path: Path, payload: Any) -> None:
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")


def validate_database_sources(clusters: list[dict[str, Any]]) -> list[dict[str, Any]]:
    cluster_members = cluster_table_membership(clusters)
    validations: list[dict[str, Any]] = []
    for spec in SCENARIO_DATABASE_SPECS:
        cluster_tables = cluster_members.get(spec.id, set())
        expected_tables = set(spec.source_tables)
        missing_from_cluster = sorted(expected_tables - cluster_tables)
        validations.append(
            {
                "scenario_id": spec.id,
                "cluster_table_count": len(cluster_tables),
                "expected_source_table_count": len(expected_tables),
                "cluster_tables": sorted(cluster_tables),
                "expected_source_tables": sorted(expected_tables),
                "missing_expected_sources": missing_from_cluster,
                "cluster_has_all_expected_sources": not missing_from_cluster,
            }
        )
    return validations


def _sql_str(s: str) -> str:
    return "'" + s.replace("'", "''") + "'"


def ggim_view_name(scenario_id: str, table_name: str) -> str:
    """Stable view name inside ggim_* schemas (avoids collisions across scenarios)."""
    return f"{scenario_id}__{table_name}"


def render_ggim_registry_and_views_ddl() -> str:
    """GGIM meta schema (14 categories + scenario bridge) and ggim_1..ggim_14 schemas with read-only views onto canonical scenario tables."""

    lines: list[str] = []
    lines.append(
        "\n-- ---------------------------------------------------------------------------\n"
        "-- GGIM / ISO first-level layer: registry (ggim) + per-category view namespaces (ggim_1..ggim_14).\n"
        "-- Physical tables remain under the 7 scenario schemas; ETL unchanged.\n"
        "-- ---------------------------------------------------------------------------\n"
    )
    lines.append("CREATE SCHEMA IF NOT EXISTS ggim;")
    lines.append(
        """CREATE TABLE IF NOT EXISTS ggim.category (
    ggim_id TEXT PRIMARY KEY,
    label TEXT NOT NULL,
    sort_order SMALLINT NOT NULL
);
CREATE TABLE IF NOT EXISTS ggim.scenario_ggim_map (
    scenario_id TEXT NOT NULL,
    ggim_id TEXT NOT NULL,
    PRIMARY KEY (scenario_id, ggim_id),
    FOREIGN KEY (ggim_id) REFERENCES ggim.category(ggim_id)
);"""
    )

    cat_values = []
    for idx, cat in enumerate(CATEGORY_TAXONOMY, start=1):
        cat_values.append(f"({_sql_str(cat.id)}, {_sql_str(cat.label)}, {idx})")
    lines.append("INSERT INTO ggim.category (ggim_id, label, sort_order) VALUES\n" + ",\n".join(cat_values) + ";")

    map_rows = []
    for scen, cats in SCENARIO_TO_GGIM_CATEGORIES.items():
        for gid in cats:
            map_rows.append(f"({_sql_str(scen)}, {_sql_str(gid)})")
    lines.append("INSERT INTO ggim.scenario_ggim_map (scenario_id, ggim_id) VALUES\n" + ",\n".join(map_rows) + ";")

    ggim_to_scenarios: dict[str, list[str]] = {}
    for scen, cats in SCENARIO_TO_GGIM_CATEGORIES.items():
        for gid in cats:
            ggim_to_scenarios.setdefault(gid, []).append(scen)

    for cat in CATEGORY_TAXONOMY:
        gid = cat.id
        lines.append(f"\nCREATE SCHEMA IF NOT EXISTS {gid};")
        lines.append(
            f"COMMENT ON SCHEMA {gid} IS {_sql_str(cat.label + ' (views onto scenario canonical tables)')};"
        )
        for scen_id in ggim_to_scenarios.get(gid, []):
            spec = next((s for s in SCENARIO_DATABASE_SPECS if s.id == scen_id), None)
            if spec is None:
                continue
            for table in spec.tables:
                vname = ggim_view_name(scen_id, table.name)
                lines.append(
                    f"CREATE OR REPLACE VIEW {gid}.{vname} AS SELECT * FROM {scen_id}.{table.name};"
                )
                lines.append(
                    f"COMMENT ON VIEW {gid}.{vname} IS {_sql_str(f'Proxy: {scen_id}.{table.name}')};"
                )
    return "\n".join(lines)


def render_table_ddl(schema: str, table: TableDef) -> str:
    column_lines = []
    for column in table.columns:
        constraints = f" {column.constraints.format(schema=schema)}" if column.constraints else ""
        column_lines.append(f"    {column.name} {column.data_type}{constraints}")
    create_stmt = "\n".join(
        [
            f"-- {table.description}",
            f"CREATE TABLE IF NOT EXISTS {schema}.{table.name} (",
            ",\n".join(column_lines),
            ");",
        ]
    )
    index_stmts = [index.format(schema=schema) for index in table.indexes]
    return "\n".join([create_stmt, *index_stmts])


def render_database_blueprints(validations: list[dict[str, Any]] | None = None) -> tuple[list[dict[str, Any]], str]:
    validation_by_scenario = {item["scenario_id"]: item for item in validations or []}
    blueprints: list[dict[str, Any]] = []
    ddl_parts = ["CREATE EXTENSION IF NOT EXISTS postgis;"]
    for spec in SCENARIO_DATABASE_SPECS:
        schema = spec.id
        ddl_parts.append(f"\nCREATE SCHEMA IF NOT EXISTS {schema};")
        ddl_parts.append(f"\n-- {spec.label}: {spec.description}")
        for table in spec.tables:
            ddl_parts.append(render_table_ddl(schema, table))
        blueprints.append(
            {
                "database_id": spec.id,
                "label": spec.label,
                "description": spec.description,
                "source_tables": list(spec.source_tables),
                "join_paths": list(spec.join_paths),
                "cluster_alignment": validation_by_scenario.get(spec.id, {}),
                "tables": [
                    {
                        "table_name": table.name,
                        "description": table.description,
                        "columns": [
                            {
                                "name": column.name,
                                "data_type": column.data_type,
                                "constraints": column.constraints.format(schema=spec.id) if column.constraints else "",
                            }
                            for column in table.columns
                        ],
                        "indexes": [index.format(schema=spec.id) for index in table.indexes],
                    }
                    for table in spec.tables
                ],
            }
        )
    ddl_parts.append(render_ggim_registry_and_views_ddl())
    blueprints.append(
        {
            "database_id": "ggim_registry",
            "label": "GGIM / ISO first-level registry and view namespaces",
            "description": "Schema ggim holds category + scenario bridge; ggim_1..ggim_8 expose read-only views onto the 7 scenario canonical tables.",
            "registry_schema": "ggim",
            "registry_tables": ["ggim.category", "ggim.scenario_ggim_map"],
            "view_schemas": [c.id for c in CATEGORY_TAXONOMY],
            "scenario_ggim_bridge": {k: list(v) for k, v in SCENARIO_TO_GGIM_CATEGORIES.items()},
        }
    )
    return blueprints, "\n\n".join(ddl_parts) + "\n"


def build_database_summary_markdown(blueprints: list[dict[str, Any]], validations: list[dict[str, Any]]) -> str:
    scenario_blueprints = [b for b in blueprints if b.get("database_id") != "ggim_registry"]
    lines = [
        "# Spatial Benchmark Database Summary",
        "",
        f"- Scenario databases: {len(scenario_blueprints)}",
        f"- GGIM layer: schema `ggim` (registry) + view namespaces `{', '.join(c.id for c in CATEGORY_TAXONOMY)}`",
        "",
        "## Scenario Coverage",
    ]
    for blueprint in scenario_blueprints:
        coverage = next((item for item in validations if item["scenario_id"] == blueprint["database_id"]), None)
        coverage_note = "all expected clustered sources present"
        if coverage and coverage["missing_expected_sources"]:
            coverage_note = f"missing clustered sources: {', '.join(coverage['missing_expected_sources'][:5])}"
        lines.append(
            f"- {blueprint['database_id']}: {len(blueprint['tables'])} canonical tables; "
            f"{len(blueprint['source_tables'])} curated source tables; {coverage_note}"
        )
    return "\n".join(lines) + "\n"


def run_database_blueprint_pipeline(artifacts_dir: Path) -> dict[str, Any]:
    artifacts_dir.mkdir(parents=True, exist_ok=True)
    clusters = load_scenario_clusters(artifacts_dir)
    validations = validate_database_sources(clusters)
    blueprints, ddl_sql = render_database_blueprints(validations)

    write_json(artifacts_dir / "scenario_database_blueprints.json", blueprints)
    (artifacts_dir / "scenario_database_blueprints.sql").write_text(ddl_sql, encoding="utf-8")
    write_json(artifacts_dir / "database_cluster_alignment.json", validations)
    (artifacts_dir / "database_summary.md").write_text(
        build_database_summary_markdown(blueprints, validations),
        encoding="utf-8",
    )

    n_scenario = sum(1 for b in blueprints if b.get("database_id") != "ggim_registry")
    return {
        "n_databases": n_scenario,
        "n_blueprint_entries": len(blueprints),
        "database_blueprints": str((artifacts_dir / "scenario_database_blueprints.json").resolve()),
        "database_ddl": str((artifacts_dir / "scenario_database_blueprints.sql").resolve()),
    }
