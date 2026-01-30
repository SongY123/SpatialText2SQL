# inference_evaluation：最小跑通说明（任务3/4）

在 `inference_evaluation/` 目录下运行以下流程即可跑通：**批量推理 → EX → VES**。

## 0) 前置

目录结构（你会把最小化文件放到这里）：
- `inference/`：推理脚本
- `evaluation/`：评测脚本

需要你准备的文件：
- `evaluation/db_config.json`：本地 PostgreSQL 连接信息（host/port/user/password）
- `evaluation/data/dev.json`：ground truth（每条至少包含 `question_id/db_id/question/sql` 或 `SQL`）
- `.env`：大模型 API 配置（OpenAI-compatible）

`.env` 示例：
```env
MODEL=qwen3-coder:30b-a3b-fp16
OPENAI_API_KEY=EMPTY
OPENAI_API_BASE=http://10.132.80.118:11434/v1
```

Windows（可选，用于 psycopg2 连接失败时的编码兼容）：
```powershell
$env:PGCLIENTENCODING="UTF8"
$env:PYTHONUTF8="1"
```

安装依赖（一次性）：
```bash
pip install -r inference/requirements.txt
pip install -r evaluation/requirements.txt
```

## 1) 批量推理（生成预测 SQL 文件）

运行后会在 `evaluation/exp_result/api_run/` 下生成一堆文件：`<question_id>.sql`。

```bash
python inference/run_batch_infer_api_pg.py \
  --db_config evaluation/db_config.json \
  --ground_truth evaluation/data/dev.json \
  --output_dir evaluation/exp_result/api_run \
  --table_schema public \
  --sample_rows 3 \
  --max_tables 30
```

## 2) 跑 EX（Execution Accuracy）

```bash
python evaluation/evaluation_pg.py \
  --db_config evaluation/db_config.json \
  --predicted_sql_path evaluation/exp_result/api_run \
  --ground_truth_path evaluation/data \
  --data_mode dev \
  --output_path evaluation/results/ex.json
```

## 3) 跑 VES（Valid Efficiency Score）

先用 `iterate_num=3` 冒烟跑通，再按需求调大（例如 30/100）。

```bash
python evaluation/evaluation_ves_pg.py \
  --db_config evaluation/db_config.json \
  --predicted_sql_path evaluation/exp_result/api_run \
  --ground_truth_path evaluation/data \
  --data_mode dev \
  --iterate_num 3 \
  --meta_time_out 30 \
  --output_path evaluation/results/ves.json \
  --output_log_path evaluation/results/ves_summary.json
```

