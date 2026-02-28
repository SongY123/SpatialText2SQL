from __future__ import annotations

import argparse
import logging
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Dict, Optional

import uvicorn
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from web.api import admin_router, auth_router, chat_router, database_router, user_router
from web.db_migration_runner import SqlMigrationRunner
from web.entity.model import get_engine, init_engine
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


def _run_sql_migrations() -> None:
    sql_dir = _resolve_path(get_config("database.sql_dir"))
    if not sql_dir:
        logger.warning("database.sql_dir is empty, skip SQL migrations.")
        return
    engine = get_engine()
    SqlMigrationRunner(engine=engine, sql_dir=sql_dir).run()


@asynccontextmanager
async def lifespan(app: FastAPI):
    init_engine(config_path=str(CURRENT_WEB_CONFIG_PATH))
    _run_sql_migrations()
    logger.info("Web app initialized.")
    yield
    logger.info("Web app shutdown.")


app = FastAPI(
    title="SpatialSQL Web API",
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
app.include_router(chat_router, prefix="/api")
app.include_router(admin_router, prefix="/api")


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
    logger.info("Launch Spatial Multi-Agent Service")
    logger.info(f"Service Address: http://{host}:{port}")
    logger.info(f"Swagger Doc: http://{host}:{port}/docs")
    logger.info(f"ReDoc: http://{host}:{port}/redoc")

    uvicorn.run(app, host=host, port=port, reload=False)


if __name__ == "__main__":
    main()
