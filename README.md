# SpatialText2SQL

A Spatial Text-to-SQL service built with FastAPI and a multi-agent pipeline.

## 1. Requirements

- Python `>=3.9`

## 2. Clone the Project

```bash
git clone -b multi-agent https://github.com/SongY123/SpatialText2SQL.git
cd SpatialText2SQL
```

## 3. Install Dependencies

```bash
pip install -r requirements.txt
```

## 4. Configuration Files

### 4.1 Web Service Config (Required)

File: `src/web/resources/config.yaml`

Key fields:

- `server.host` / `server.port`: service bind address and port
- `database.db_path`: local sqlite DB path (default: `spatial_agent.db`)
- `database.init_sql_path`: initialization SQL path (default: `src/web/resources/db/init_db.sql`)
- `model.provider`: `dashscope` / `ollama` / `openai` / `gemini`
- Provider-specific fields:
  - `model.dashscope.api_key`
  - `model.ollama.host`
  - `model.openai.api_key` and optional `model.openai.api_base`
  - `model.gemini.api_key` and optional `model.gemini.api_base`

Set your own `model.dashscope.api_key` before running.

If you serve a model with vLLM, expose it as an OpenAI-compatible API server, then set:
- `model.provider: openai`
- `model.openai.api_base`: your vLLM OpenAI-compatible endpoint
- `model.openai.api_key`: value required by your gateway (can be a placeholder for local setups)
- `model.openai.model_name`: the served model name

### 4.2 DB Init SQL (Default Usually Works)

File: `src/web/resources/db/init_db.sql`

- This SQL is executed automatically at startup (idempotent).
- Usually no changes are required.

### 4.3 Preprocess Config (Optional)

File: `config/preprocess.yml`

- Used for building vector and keyword indexes (`vectorize` / `keyword_search`).
- Configure data source paths and DB connection if you need to rebuild retrieval indexes.

## 5. Start the Service

Note: this project imports from the `web` package, so set `PYTHONPATH=src`.

```bash
PYTHONPATH=src python -m web.app
```

After startup:

- Health: `http://127.0.0.1:8888/health`
- Swagger: `http://127.0.0.1:8888/docs`
- ReDoc: `http://127.0.0.1:8888/redoc`

## 6. Optional: Run Preprocess (Build Retrieval Indexes)

```bash
PYTHONPATH=src python -m preprocess.main --config config/preprocess.yml
```

If vector/keyword index outputs already exist, corresponding preprocess steps are skipped automatically.
