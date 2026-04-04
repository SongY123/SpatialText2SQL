# SpatialSQL 数据集适配

将 [SpatialSQL](https://github.com/beta512/SpatialSQL) 的 dataset1/dataset2 接入本框架，**仅做扩展，不改变原有 spatial_qa 流程**。默认数据集仍为 `spatial_qa`，原命令行为不变。

## 1. 适配内容概览

- **数据加载**：`SpatialSQLLoader` 解析 `sdbdatasets/dataset1|2/<ada|edu|tourism|traffic>/QA-*.txt`，输出统一格式（含 `gold_sql_candidates`、`metadata.split`）。
- **数据库迁移**：脚本将 SQLite/SpatiaLite 导入 PostgreSQL/PostGIS，schema 命名为 `spatialsql_dataset1_ada` 等。
- **SQL 方言**：`sql_dialect_adapter` 将 SpatiaLite SQL 转为 PostGIS；未覆盖项写入 `data/preprocessed/spatialsql_pg/unconverted_sqls.jsonl`。
- **评估**：支持多 gold 候选（`gold_sql_candidates`），预测与任一候选结果一致即判对；评估结果中保留 `error_type`/`error_message` 及可选 `matched_gold_index`。

## 2. 验证（不依赖真实数据与 DB）

在项目根目录、使用本框架环境（如 `conda activate text2sql`）执行：

```bash
python scripts/spatialsql/verify_adaptation.py
```

- 检查默认数据集仍为 `spatial_qa`，原配置与 `SpatialQALoader` 可用。
- 检查 `SpatialSQLLoader` 与 `sql_dialect_adapter` 行为。
- 检查评估器多 gold 候选接口。

## 3. 全量流程（需 sdbdatasets 与 PG）

1. **放置数据**：将 SpatialSQL 的 `sdbdatasets` 放到项目根下（或修改 `config/dataset_config.yaml` 中 `spatialsql_pg.data_path`）。
2. **迁移数据库**（需 GDAL/ogr2ogr）：
   ```bash
   python scripts/spatialsql/migrate_sqlite_to_pg.py [path/to/sdbdatasets] --config config/db_config.yaml
   ```
   报告：`scripts/spatialsql/migration_report.json`、`migration_report.txt`。
3. **预处理**：
   ```bash
   python scripts/evaluation/run_pipeline.py --preprocess --dataset spatialsql_pg
   ```
   输出：`data/preprocessed/spatialsql_pg/split<version>_<domain>_with_schema.json`；若有未覆盖 SQL：`data/preprocessed/spatialsql_pg/unconverted_sqls.jsonl`。
4. **推理与评估**：
   ```bash
   python scripts/evaluation/run_pipeline.py --inference --evaluate --dataset spatialsql_pg --models <model> --configs base
   ```
5. **结果与统计**：
   - 评估详情：`results/evaluations/<model>_<config>_eval.json`（含按条 `correct`、`error_type`、`error_message`、`matched_gold_index` 等）。
   - 总体与按 **split**（dataset_version_domain）分组 EX：见控制台输出及 `results/evaluations/summary.json`。
   - 按 **label**（G/S）的统计可从评估详情中 `metadata.label` 自行聚合；当前报告按 `split` 分组。

## 4. 原流程回归

以下命令行为与适配前一致（默认 `--dataset spatial_qa`）：

```bash
python scripts/evaluation/run_pipeline.py --preprocess
python scripts/evaluation/run_pipeline.py --build-rag --inference --evaluate
python scripts/evaluation/run_pipeline.py --evaluate --models qwen2.5-coder-7b
```
