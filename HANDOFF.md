# Handoff Notes

This document summarizes current progress and handoff points for continuing the pipeline after relation-driven DB synthesis.

## Current Scope Completed

- Layer 1: Table canonicalization
  - Normalized schema
  - Spatial column identification
  - Representative values added in canonical table output
- Layer 2: Relation-driven DB synthesis
  - City-level relation graph construction
  - Relation-aware random walk sampling
  - Sampled DB graph visualization
  - DB blueprint export (JSONL/CSV/DDL sketch)

## Main Scripts

- `scripts/run_canonical_db_synthesis.py`
  - Produces canonical tables, relation graph, sampled DB groups, and synthesis report
- `scripts/export_sampled_db_graphs_html.py`
  - Produces interactive HTML for sampled DB graph examples
- `scripts/export_db_blueprints.py`
  - Produces executable-oriented DB blueprints and SQL DDL sketch

## Key Inputs

- `scripts/artifacts/seven_city_output/table_catalog.json`
- `scripts/artifacts/seven_city_output/unified_inventory.json`
- `scripts/artifacts/socrata_maps/<city>/...` local source files

## Key Outputs

- `scripts/artifacts/seven_city_output/canonical_tables.jsonl`
- `scripts/artifacts/seven_city_output/relation_graph.json`
- `scripts/artifacts/seven_city_output/sampled_databases.jsonl`
- `scripts/artifacts/seven_city_output/canonical_db_synthesis_report.md`
- `scripts/artifacts/seven_city_output/sampled_db_graphs.html`
- `scripts/artifacts/seven_city_output/db_blueprints.jsonl`
- `scripts/artifacts/seven_city_output/db_blueprints.sql`

## Active Defaults (Current)

- Target average degree: `3.0`
- Jump probability (`rho`): `0.1`
- Sample DB size: truncated normal `N(8, 2^2)` with range `[3, 12]`
- Representative values are included in similarity scoring
- Labeling remains pass-through (not directly weighted in edge score)

## Known Gaps / Next Work

- Physical materialization not done:
  - No automatic per-sample `CREATE + LOAD + INDEX + validate` execution yet
- Relation semantics are still heuristic:
  - `key_join / spatial_join / semantic_link` are inferred, not strictly validated
- Downstream not implemented in this branch:
  - SQL synthesis
  - NL synthesis
  - Execution-grounded QC

## Suggested Next Step for Continuation

1. Build materialization runner from `db_blueprints.jsonl` (create schema + load data)
2. Add execution validation and relation contract checks
3. Start SQL synthesis module using `sampled_databases.jsonl` / `db_blueprints.jsonl`
