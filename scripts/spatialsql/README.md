# SpatialSQL 数据集适配

将 [SpatialSQL](https://github.com/beta512/SpatialSQL) 的 dataset1/dataset2 接入本框架，**仅做扩展，不改变原有 spatial_qa 流程**。默认数据集仍为 `spatial_qa`，原命令行为不变。

## 1. 适配内容概览

- **数据加载**：`SpatialSQLLoader` 解析 `sdbdatasets/dataset1|2/<ada|edu|tourism|traffic>/QA-*.txt`，输出统一格式（含 `gold_sql_candidates`、`metadata.split`）。
- **数据库迁移**：主入口为 `migrate_to_separate_db.py`，将 SQLite/SpatiaLite 迁移到独立的 `spatial_sql` 数据库，业务表统一落在 `public` schema 下，表名形如 `dataset1_ada_provinces`。
- **源库体检与闭环报告**：迁移入口会额外生成源库 inventory、异常报告、几何校验报告、SQL 转换报告、修复候选、执行一致性报告和回归样本。
- **SQL 方言**：`sql_dialect_adapter` 将 SpatiaLite SQL 转为 PostGIS；未覆盖项写入 `data/preprocessed/spatialsql_pg/unconverted_sqls.jsonl`，并生成 `sql_conversion_report.json` 供修复闭环使用。
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
2. **迁移数据库并生成闭环报告**（需 GDAL/ogr2ogr）：
   ```bash
   python scripts/spatialsql/migrate_to_separate_db.py
   ```
   主要报告：`scripts/spatialsql/source_inventory.json`、`source_anomalies.json`、`migration_report.json`、`geometry_validation_report.json`、`sql_conversion_report.json`、`sql_repair_candidates.jsonl`、`execution_consistency_report.json`。
3. **预处理**：
   ```bash
   python scripts/evaluation/run_pipeline.py --preprocess --dataset spatialsql_pg
   ```
   输出：`data/preprocessed/spatialsql_pg/dataset1/ada_samples.json` 这类分层文件；若有未覆盖 SQL：`data/preprocessed/spatialsql_pg/unconverted_sqls.jsonl`。
4. **推理与评估**：
   ```bash
   python scripts/evaluation/run_pipeline.py --inference --evaluate --dataset spatialsql_pg --models <model> --configs base
   ```
   如需测试增强版 prompt，可改成：
   ```bash
   python scripts/evaluation/run_pipeline.py --inference --evaluate --dataset spatialsql_pg --models qwen2.5-coder-32b --configs prompt_enhanced
   ```
   如需额外生成跨 task 的 benchmark 汇总，再显式追加 `--benchmark`。
5. **结果与统计**：
   - task 结果：`results/tasks/spatialsql_pg/<backend>/<model>/<config>/latest/` 下的 `predictions.json`、`evaluation.json`、`summary.json`。
   - 历史 run：`results/tasks/spatialsql_pg/<backend>/<model>/<config>/runs/<run_id>/`。
   - benchmark 汇总：仅在显式传入 `--benchmark` 时生成，输出到 `results/benchmarks/latest/summary.json`、`summary.txt`、`paper_tables.md`。
   - `summary.txt` 会同时保留整体平均，并展示按 **split**（dataset_version_domain）分组的明细矩阵。
   - 按 **label**（G/S）的统计可从评估详情中 `metadata.label` 自行聚合；当前报告按 `split` 分组。

## 4. 原流程回归

## 5. Legacy

- `migrate_sqlite_to_pg.py` 为早期按 schema 迁移的 legacy 脚本，仅保留作历史参考。

以下命令行为与适配前一致（默认 `--dataset spatial_qa`）：

```bash
python scripts/evaluation/run_pipeline.py --preprocess
python scripts/evaluation/run_pipeline.py --build-rag --inference --evaluate
python scripts/evaluation/run_pipeline.py --evaluate --models qwen2.5-coder-7b
python scripts/evaluation/run_pipeline.py --benchmark --dataset spatial_qa spatialsql_pg --models qwen2.5-coder-7b --configs base
```
