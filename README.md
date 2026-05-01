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
