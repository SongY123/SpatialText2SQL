# Spatial Text2SQL 推理评估框架

一个用于评估 Spatial Text2SQL 模型的评估框架，支持多种推理配置（Base、RAG、Keyword Search）。

## 项目简介

本项目提供了一套完整的 Spatial Text2SQL 评估工具，用于测试大模型在空间数据库查询生成任务上的表现。

**支持的数据集**：
- [spatial_qa](https://github.com/alikhosravi/Spatial-Text-to-SQL) - 空间查询问答数据集
- [spatialsql_pg](https://github.com/beta512/SpatialSQL) - PostgreSQL 空间 SQL 数据集

**支持的评估模式**：
- **base** - 基础模式（Question + Schema）
- **rag** - RAG 增强模式（Question + Schema + RAG 检索）
- **keyword** - 关键词检索模式（Question + Schema + Keyword Search）
- **full** - 完整模式（Question + Schema + RAG + Keyword）

## 环境要求

- **Python**: 3.8+
- **数据库**: PostgreSQL + PostGIS 扩展
- **GPU**: 推荐用于模型推理

## 快速开始

### 1. 克隆项目并安装依赖

```bash
git clone <repository_url>
cd reference_evaluation
pip install -r requirements.txt
```

### 2. 准备数据集

#### spatial_qa 数据集
将 Spatial QA 的 Excel 文件放在项目根目录的 `Spatial QA/` 文件夹中。

#### spatialsql_pg 数据集
1. 下载 [SpatialSQL](https://github.com/beta512/SpatialSQL) 数据集
2. 使用提供的迁移脚本将数据迁移到 PostgreSQL
3. 将数据集文件放在 `sdbdatasets/` 目录

### 3. 配置数据库连接

编辑 `config/db_config.yaml`，填入您的数据库连接信息：

```yaml
database:
  host: "localhost"
  port: 5432
  database: "postgres"
  user: "your_username"
  password: "your_password"
```

### 4. 配置模型路径

编辑 `config/model_config.yaml`，设置模型路径：

```yaml
models:
  qwen2.5-coder-7b:
    path: "~/.cache/huggingface/hub/models--Qwen--Qwen2.5-Coder-7B"
  qwen3-8b:
    path: "~/.cache/huggingface/hub/models--Qwen--Qwen3-8B"
```

### 5. 准备 PostGIS 文档（用于 RAG）

将 PostGIS 文档提取为 JSON 格式，保存为 `postgis_extracted.json`，放在项目根目录。

### 6. 运行完整流程

```bash
# 首次运行：数据预处理 + 构建 RAG 索引 + 推理 + 评估
python src/main.py --preprocess --build-rag --inference --evaluate
```

## 命令行参数

### 流程控制参数

- `--preprocess` - 执行数据预处理（读取数据集，提取 Schema）
- `--build-rag` - 构建 RAG 向量索引（基于 PostGIS 文档）
- `--inference` - 运行模型推理
- `--evaluate` - 执行评估并计算指标

### 数据集和模型选择

- `--dataset <name>` - 指定数据集，可选 `spatial_qa` 或 `spatialsql_pg`
- `--models <model1> <model2> ...` - 指定要评估的模型（默认使用配置文件中的所有模型）
- `--configs <config1> <config2> ...` - 指定评估配置，可选 `base`、`rag`、`keyword`、`full`

### 其他参数

- `--config-dir <path>` - 指定配置文件目录（默认：`./config`）

## 使用示例

### 示例 1：只运行 spatial_qa 的 base 配置

```bash
python src/main.py --inference --evaluate --dataset spatial_qa --configs base
```

### 示例 2：运行 spatialsql_pg 的全部配置

```bash
python src/main.py --inference --evaluate --dataset spatialsql_pg --configs base rag keyword full
```

### 示例 3：指定特定模型

```bash
python src/main.py --inference --evaluate --models qwen2.5-coder-7b --configs base rag
```

### 示例 4：分步执行

```bash
# 第一步：数据预处理
python src/main.py --preprocess

# 第二步：构建 RAG 索引（只需执行一次）
python src/main.py --build-rag

# 第三步：推理和评估
python src/main.py --inference --evaluate
```

## 项目结构

```
reference_evaluation/
├── config/                    # 配置文件
│   ├── db_config.yaml        # 数据库连接配置
│   ├── model_config.yaml     # 模型路径和参数配置
│   ├── dataset_config.yaml   # 数据集路径和格式配置
│   └── eval_config.yaml      # 评估参数（RAG、Keyword等）
├── src/                       # 源代码
│   ├── base/                 # 抽象基类
│   ├── loaders/              # 数据加载器实现
│   ├── main.py               # 主入口文件
│   └── *.py                  # 各功能模块
├── scripts/                   # 脚本工具
│   └── spatialsql/           # SpatialSQL 数据集迁移脚本
├── data/                      # 数据目录（需自行准备）
│   ├── preprocessed/         # 预处理后的数据
│   └── schemas/              # 数据库 Schema 缓存
├── results/                   # 结果输出（自动生成）
│   ├── predictions/          # 模型预测的 SQL
│   └── evaluations/          # 评估指标和汇总
├── rag_db/                    # RAG 向量数据库（自动生成）
├── requirements.txt           # Python 依赖
└── README.md                  # 本文档
```

## 数据集准备

### spatial_qa 数据集

1. 从 [Spatial-Text-to-SQL](https://github.com/alikhosravi/Spatial-Text-to-SQL) 下载数据集
2. 将 Excel 文件放在项目根目录的 `Spatial QA/` 文件夹中
3. 在 `config/dataset_config.yaml` 中配置数据集路径
4. 确保 PostgreSQL 中已创建相应的空间数据库和表

### spatialsql_pg 数据集

1. 从 [SpatialSQL](https://github.com/beta512/SpatialSQL) 下载数据集
2. 使用 `scripts/spatialsql/` 中的迁移脚本将数据迁移到 PostgreSQL
3. 将数据集文件放在 `sdbdatasets/` 目录
4. 在 `config/dataset_config.yaml` 中配置数据集路径和划分信息

## 配置文件说明

### 数据库配置 (`config/db_config.yaml`)

需要修改的字段：
- `host` - 数据库主机地址
- `port` - 数据库端口（默认 5432）
- `database` - 数据库名称
- `user` - 数据库用户名
- `password` - 数据库密码

### 模型配置 (`config/model_config.yaml`)

需要修改的字段：
- `models.<model_name>.path` - 模型文件路径
- `default_models` - 默认使用的模型列表

### 数据集配置 (`config/dataset_config.yaml`)

需要修改的字段：
- `datasets.<dataset_name>.data_dir` - 数据集文件路径
- `datasets.<dataset_name>.splits` - 数据集划分（用于 spatialsql_pg）

### 评估配置 (`config/eval_config.yaml`)

可选修改的字段：
- `rag.top_k` - RAG 检索返回的文档数量
- `keyword_search.top_k` - 关键词检索返回的结果数量

## 评估结果

### 结果保存位置

- **预测结果**：`results/predictions/{model_name}/{config_type}/`
  - 包含每个样本的预测 SQL 和执行结果
  
- **评估结果**：`results/evaluations/`
  - `{model_name}_{config_type}_eval.json` - 详细评估结果
  - `summary.json` - 所有实验的汇总结果

### 评估指标

- **Execution Accuracy (EX)** - SQL 执行结果准确率
- 按数据集划分（Level 或 Split）的分组准确率

## 依赖说明

主要依赖包：
- `transformers` - 大语言模型推理
- `torch` - PyTorch 深度学习框架
- `chromadb` - 向量数据库（用于 RAG）
- `psycopg2-binary` - PostgreSQL 连接
- `sentence-transformers` - 文本嵌入模型
- `scikit-learn` - TF-IDF 关键词检索
- `openpyxl` - Excel 文件读取
- `tqdm` - 进度条显示

## 常见问题

### Q: 首次运行需要执行哪些步骤？

A: 按顺序执行以下命令：
```bash
python src/main.py --preprocess --build-rag --inference --evaluate
```

### Q: RAG 索引是否需要每次都重新构建？

A: 不需要。RAG 索引只需构建一次，保存在 `rag_db/` 目录，后续运行会自动加载。

### Q: 如何只测试特定模型和配置？

A: 使用 `--models` 和 `--configs` 参数：
```bash
python src/main.py --inference --evaluate --models qwen2.5-coder-7b --configs base rag
```

### Q: 数据库连接失败怎么办？

A: 检查以下几点：
1. PostgreSQL 服务是否正常运行
2. `config/db_config.yaml` 中的连接信息是否正确
3. 数据库是否已安装 PostGIS 扩展
4. 网络连接是否正常（如使用远程数据库）

### Q: 如何添加新的模型？

A: 
1. 在 `config/model_config.yaml` 中添加模型配置
2. 如需自定义加载逻辑，在 `src/loaders/` 中创建新的 ModelLoader 类
3. 在 `ModelLoaderFactory` 中注册新模型

## 许可证

本项目采用 MIT 许可证。

## 联系方式

如有问题或建议，请通过 Issue 反馈。
