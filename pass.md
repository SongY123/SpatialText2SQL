# Archived README Content

## Old `README.md`

```markdown
# Spatial Benchmark Builder

This project now follows a staged workflow:

1. cluster raw NYC OpenData tables into scenario groups
2. build scenario database blueprints and ETL artifacts
3. generate Text2SQL data later if needed

The default code path no longer generates Text2SQL samples during the main build.

## Workflow

### 1. Cluster Tables

```bash
python scripts/cluster_tables.py --raw-dir nyc-opendata/nyc-opendata --artifacts-dir artifacts
```

Outputs:

- `artifacts/table_catalog.json`
- `artifacts/table_catalog.csv`
- `artifacts/scenario_clusters.json`
- `artifacts/clustering_summary.md`

### 2. Build Databases And ETL Artifacts

```bash
python scripts/build_databases.py --raw-dir nyc-opendata/nyc-opendata --artifacts-dir artifacts
```

Optional PostGIS load:

```bash
python scripts/build_databases.py --raw-dir nyc-opendata/nyc-opendata --artifacts-dir artifacts --load --load-backend docker
```

Outputs:

- `artifacts/scenario_database_blueprints.json`
- `artifacts/scenario_database_blueprints.sql`
- `artifacts/database_cluster_alignment.json`
- `artifacts/database_summary.md`
- `artifacts/etl/postgis_load.sql`
- `artifacts/etl/load_ready/...`
- `artifacts/etl/cluster_alignment.json`

### 3. Text2SQL Generation

This stage is intentionally deferred and is not part of the default build path.

## Notes

- `scripts/build_benchmark.py` is now a legacy alias for the clustering stage.
- `scripts/run_etl.py` remains available as a low-level ETL entrypoint, but it expects clustering artifacts to already exist.
- ETL uses curated canonical scenario mappings from `src/spatial_benchmark/scenario_specs.py`, and now validates them against the prior clustering output before materialization.
```

## Old `README_ZH.md`

```markdown
# Spatial Benchmark Builder

当前项目改成了分阶段流程：

1. 先对原始 NYC OpenData 表进行聚类
2. 再生成场景数据库蓝图和 ETL 产物
3. Text2SQL 数据生成暂时不进入默认流程

## 1. 聚类

```bash
python scripts/cluster_tables.py --raw-dir nyc-opendata/nyc-opendata --artifacts-dir artifacts
```

主要输出：

- `artifacts/table_catalog.json`
- `artifacts/table_catalog.csv`
- `artifacts/scenario_clusters.json`
- `artifacts/clustering_summary.md`

## 2. 建库与 ETL

```bash
python scripts/build_databases.py --raw-dir nyc-opendata/nyc-opendata --artifacts-dir artifacts
```

如果要直接加载到 PostGIS：

```bash
python scripts/build_databases.py --raw-dir nyc-opendata/nyc-opendata --artifacts-dir artifacts --load --load-backend docker
```

主要输出：

- `artifacts/scenario_database_blueprints.json`
- `artifacts/scenario_database_blueprints.sql`
- `artifacts/database_cluster_alignment.json`
- `artifacts/database_summary.md`
- `artifacts/etl/postgis_load.sql`
- `artifacts/etl/load_ready/...`
- `artifacts/etl/cluster_alignment.json`

## 3. Text2SQL

这一阶段先保留接口，但不再作为默认主流程的一部分。

## 说明

- `scripts/build_benchmark.py` 现在只是聚类阶段的兼容入口。
- `scripts/run_etl.py` 仍然可用，但它现在要求 `artifacts` 目录下已经存在聚类结果。
- ETL 仍然使用 `src/spatial_benchmark/scenario_specs.py` 里的规范化场景映射，同时会先检查它和前一步聚类结果是否对齐。
```
