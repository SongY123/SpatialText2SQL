# SpatialText2SQL

Spatial Text2SQL 项目代码现已按职责拆分为单一根级工程结构：

- `src/` 只放可复用源码
- `scripts/` 只放执行脚本
- `config/` 统一放运行配置
- `data/`、`results/` 存放数据和运行产物

## 目录概览

```text
.
├── config/                  # 评测与数据配置
├── scripts/
│   ├── evaluation/          # 流水线入口脚本
│   └── spatialsql/          # SpatialSQL 迁移与验证脚本
├── src/
│   ├── datasets/            # 数据集加载与预处理
│   ├── evaluation/          # 指标计算与报告生成
│   ├── inference/           # 模型加载与推理
│   ├── pipeline/            # 主流程编排
│   ├── preprocess/          # 数据导入工具
│   ├── prompting/           # Prompt 构建
│   ├── retrieval/           # RAG / Keyword 检索
│   └── sql/                 # Schema 与 SQL 方言工具
└── test/
```

## 常用入口

运行评测流水线：

```bash
python scripts/evaluation/run_pipeline.py --preprocess --inference --evaluate
```

验证 SpatialSQL 适配：

```bash
python scripts/spatialsql/verify_adaptation.py
```
