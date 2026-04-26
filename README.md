# SpatialText2SQL (Current Pipeline)

This repository currently uses a **7-city local-data pipeline** as the main path.

The old NYC-only staged README content has been archived to `pass.md`.

## Current Main Flow

### 1) Full Pipeline (recommended)

Run the end-to-end pipeline from repository root:

```bash
python scripts/run_full_pipeline_local_scan.py
```

This script does:

1. Multi-city clustering from local files under `scripts/artifacts/socrata_maps/`
2. Classification input export (`ai_classification_input.jsonl/.csv`)
3. Taxonomy report/tree export
4. Taxonomy M2M edge + Postgres SQL export

### 2) Source Dataset Statistics

```bash
python scripts/compute_source_dataset_stats.py
```

Outputs:

- `scripts/artifacts/seven_city_output/source_dataset_stats.json`
- `scripts/artifacts/seven_city_output/source_dataset_stats.csv`
- `scripts/artifacts/seven_city_output/source_dataset_stats.md`

### 3) Weighted Consistency Check (optional)

```bash
python scripts/compute_weighted_stats_check.py
```

This verifies weighted metrics using:

`sum(#Table * metric) / sum(#Table)`

and compares them against `Overall` in `source_dataset_stats.json`.

## Key Inputs

- `scripts/artifacts/socrata_maps/nyc-opendata/nyc_opendata_maps.json` (required for NYC)
- Local CSV/GeoJSON folders for other cities under:
  - `scripts/artifacts/socrata_maps/chicago`
  - `scripts/artifacts/socrata_maps/lacity`
  - `scripts/artifacts/socrata_maps/seattle` (or `seattle_maps_geojson`)
  - `scripts/artifacts/socrata_maps/boston`
  - `scripts/artifacts/socrata_maps/sf`
  - `scripts/artifacts/socrata_maps/phoenix`

## Key Outputs

Main output directory:

- `scripts/artifacts/seven_city_output/`

Typical generated files include:

- `table_catalog.json`
- `unified_inventory.json`
- `scenario_clusters.json`
- `category_clusters.json`
- `ai_classification_input.jsonl`
- `taxonomy_tree_report.html`
- `taxonomy_dendrogram.html`
- `taxonomy_dataset_utax_edge.csv`
- `taxonomy_dataset_l3_edge.csv`
- `pg_import_taxonomy.sql`
- `pg_import_taxonomy_m2m.sql`

## Notes

- Legacy scripts (`cluster_tables.py`, `build_databases.py`, `run_etl.py`) still exist, but are not the main path documented here.
- Taxonomy definitions are in `scripts/taxonomy/`.
