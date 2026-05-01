# SpatialText2SQL Crawler

The unified entry point for the crawler in this repository is:

```bash
scripts/dataset_construction/crawl_open_data_maps.sh
```

By default, it will download map datasets from all 7 cities. The format is unified as GeoJSON, and the data is saved to:

```text
data/raw/<city_name>/geojson/
```

The current city directory names are: `new_york_city`, `los_angeles`, `chicago`, `seattle`, `san_francisco`, `boston`, and `phoenix`.

The crawler will prioritize reading the existing `data/raw/metadata.json`:

- If a dataset already appears in `metadata.json`, it will not be downloaded again by default.
- The default mode is "append/skip", which will not overwrite existing data.
- The `metadata.json` will only be written back after the datasets for all cities have been processed.

A single summarized metadata file will be generated:

```text
data/raw/metadata.json
```

`metadata.json` is a JSON array, where each object corresponds to a city and contains statistical fields such as `City`, `#Table`, `#Field/Table`, `#Spatial Field/Table`, `#Row/Table`, etc.

## Common Usage

Download all map data for all cities (Full Download):

```bash
scripts/dataset_construction/crawl_open_data_maps.sh
```

Download a sample of 10 datasets for each city:

```bash
scripts/dataset_construction/crawl_open_data_maps.sh 10
```

*Note: You can also use other parameters to customize the behavior, which are detailed below.*

## Key Parameters

- `--sample N`: Download at most `N` datasets per city. If omitted, downloads all map data for all cities.
- `--cities LIST`: Comma-separated list of cities. Options: `nyc,lacity,chicago,seattle,sf,boston,phoenix`. Default: `all`.
- `--out-root PATH`: Root directory for downloads. Default: `data/raw`.
- `--metadata-name NAME`: Filename for the root metadata. Default: `metadata.json`.
- `--page-size N`: Pagination size for catalog APIs. Default: `100`.
- `--row-limit N`: Maximum number of rows exported by Socrata GeoJSON fallback. Default: `5000000`.
- `--sleep SECONDS`: Waiting time between two downloads. Default: `0`.
- `--timeout SECONDS`: HTTP request timeout. Default: `120`.
- `--override`: Force overwrite existing datasets. Default is not to overwrite (skips datasets already present in `metadata.json`).
- `--list-cities`: Print configured city ids and exit.

If the volume of requests to Socrata is large, you can configure `SOCRATA_APP_TOKEN`:

```bash
SOCRATA_APP_TOKEN=your_token scripts/dataset_construction/crawl_open_data_maps.sh --sample 20
```

## PostGIS Docs Parse

Use the unified entry point below for PostGIS documentation parsing workflows:

```bash
scripts/postgis_docs_parse/run_postgis_docs_parse.sh
```

Common commands:

```bash
scripts/postgis_docs_parse/run_postgis_docs_parse.sh extract --input-dir xml_data --output-file extract_result/postgis_extracted.json
scripts/postgis_docs_parse/run_postgis_docs_parse.sh validate --input extract_result/postgis_extracted.json --output validation_result/postgis_validated.json --review manual_review/manual_review.json
```

## Benchmarks

Benchmark implementations now live under:

```text
src/benchmark/<benchmark_name>/
```

Use the shell entrypoints under `scripts/benchmark/` to run them.

### FloodSQL

Typical commands:

```bash
scripts/benchmark/floodsql/migrate_to_postgis.sh
scripts/benchmark/floodsql/validate_gold_sql.sh --utils-first
scripts/benchmark/floodsql/build_execution_consistency.sh
```

Reports are written to `scripts/benchmark/floodsql/`.

### Spatial QA

Create or inspect the PostgreSQL indexes used by the benchmark:

```bash
scripts/benchmark/spatial_qa/create_benchmark_indexes.sh
scripts/benchmark/spatial_qa/create_benchmark_indexes.sh --check-only
```

### SpatialSQL

Fetch the dataset, validate the integration, then run the migration workflow:

```bash
scripts/benchmark/spatialsql/fetch_sdbdatasets.sh
scripts/benchmark/spatialsql/verify_adaptation.sh
scripts/benchmark/spatialsql/migrate_to_separate_db.sh
scripts/benchmark/spatialsql/validate_gold_sql.sh --utils-first
```

Legacy schema-per-database migration is still available at:

```bash
scripts/benchmark/spatialsql/migrate_sqlite_to_pg.sh
```
