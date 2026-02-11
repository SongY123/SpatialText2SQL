from __future__ import annotations

import argparse
import logging
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Dict, Optional

import uvicorn
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from web.api import auth_router, database_router, user_router
from web.entity.model import create_all_tables, get_engine, init_engine
from utils.config_loader import ConfigLoader, get_config


PROJECT_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_WEB_CONFIG_PATH = PROJECT_ROOT / "src" / "web" / "resources" / "config.yaml"
CURRENT_WEB_CONFIG_PATH = DEFAULT_WEB_CONFIG_PATH
ConfigLoader.load_config(CURRENT_WEB_CONFIG_PATH)

from utils.logger import logger

def _resolve_path(path_str: Optional[str]) -> Optional[Path]:
    if not path_str:
        return None
    p = Path(path_str)
    if p.is_absolute():
        return p
    return PROJECT_ROOT / p


def _run_init_sql() -> None:
    init_sql_path = get_config("database.init_sql_path")
    sql_path = _resolve_path(init_sql_path)
    if not sql_path or not sql_path.exists():
        logger.warning("Init SQL file not found, skip: %s", sql_path)
        return

    engine = get_engine()
    sql_text = sql_path.read_text(encoding="utf-8")
    if not sql_text.strip():
        logger.info("Init SQL is empty, skip.")
        return

    if engine.dialect.name == "sqlite":
        conn = engine.raw_connection()
        try:
            conn.executescript(sql_text)
            conn.commit()
        finally:
            conn.close()
    else:
        stmts = [s.strip() for s in sql_text.split(";") if s.strip()]
        with engine.begin() as conn:
            for stmt in stmts:
                conn.exec_driver_sql(stmt)
    logger.info("Init SQL executed: %s", sql_path)


@asynccontextmanager
async def lifespan(app: FastAPI):
    init_engine(config_path=str(CURRENT_WEB_CONFIG_PATH))
    create_all_tables()
    _run_init_sql()
    logger.info("Web app initialized.")
    yield
    logger.info("Web app shutdown.")


app = FastAPI(
    title="SpatialText2SQL Web API",
    version="1.0.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(user_router, prefix="/api")
app.include_router(auth_router, prefix="/api")
app.include_router(database_router, prefix="/api")


@app.get("/health")
def health():
    return {"status": "ok"}

def main() -> None:
    parser = argparse.ArgumentParser(description="Run SpatialText2SQL FastAPI server")
    parser.add_argument(
        "--config",
        type=str,
        default=str(DEFAULT_WEB_CONFIG_PATH),
        help="Path to web config yaml.",
    )
    args = parser.parse_args()

    host = str(get_config("server.host", "127.0.0.1"))
    port = int(get_config("server.port", 8888))

    logger.info("=" * 60)
    logger.info("启动Data-Agent服务")
    logger.info(f"服务地址: http://{host}:{port}")
    logger.info(f"Swagger文档: http://{host}:{port}/docs")
    logger.info(f"ReDoc文档: http://{host}:{port}/redoc")

    uvicorn.run(app, host=host, port=port, reload=False)


if __name__ == "__main__":
    main()
