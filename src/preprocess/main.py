"""Preprocess 主入口：按配置自动执行数据库导入、向量化、关键词索引构建。"""
from __future__ import annotations

import argparse
import json
import logging
import re
import sqlite3
import sys
from urllib.parse import quote_plus
from pathlib import Path
from typing import Dict, List
from utils.logger import logger
from utils.config_loader import ConfigLoader, get_config

import yaml

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))


def _resolve_path(path_str: str | None, *, default: Path | None = None) -> Path:
    if path_str:
        p = Path(path_str)
        return p if p.is_absolute() else (PROJECT_ROOT / p)
    if default is None:
        raise ValueError("Path is required but missing in config.")
    return default


def _path_has_content(path: Path) -> bool:
    if not path.exists():
        return False
    if path.is_file():
        return path.stat().st_size > 0
    if path.is_dir():
        try:
            next(path.iterdir())
            return True
        except StopIteration:
            return False
    return False


def _quote_ident(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def _build_postgis_url(cfg: Dict) -> str:
    required = ["host", "port", "database", "user", "password", "schema"]
    missing = [k for k in required if not cfg.get(k)]
    if missing:
        raise ValueError(f"db_import.postgis missing required fields: {', '.join(missing)}")

    host = cfg["host"]
    port = cfg["port"]
    database = cfg["database"]
    user = quote_plus(str(cfg["user"]))
    password = quote_plus(str(cfg["password"]))
    return f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}"


def _ensure_postgis_ready(db_url: str, schema: str) -> None:
    try:
        from sqlalchemy import create_engine, text
    except ImportError as exc:
        raise RuntimeError("sqlalchemy is required for PostGIS import.") from exc

    engine = create_engine(db_url)
    with engine.begin() as conn:
        has_postgis = conn.execute(
            text("SELECT 1 FROM pg_extension WHERE extname = 'postgis' LIMIT 1")
        ).scalar()
        if not has_postgis:
            logger.info("PostGIS extension not found, installing...")
            conn.execute(text("CREATE EXTENSION IF NOT EXISTS postgis"))
            logger.info("PostGIS extension installed.")

        conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {_quote_ident(schema)}"))


def _is_spatialite_nonempty(sqlite_path: Path) -> bool:
    if not sqlite_path.exists() or sqlite_path.stat().st_size == 0:
        return False

    system_tables = {
        "geometry_columns",
        "spatial_ref_sys",
        "spatialite_history",
        "views_geometry_columns",
        "virts_geometry_columns",
        "sqlite_sequence",
    }
    with sqlite3.connect(str(sqlite_path)) as conn:
        cur = conn.execute("SELECT name FROM sqlite_master WHERE type='table'")
        names = [r[0] for r in cur.fetchall()]
    for n in names:
        if n.startswith("sqlite_"):
            continue
        if n in system_tables:
            continue
        return True
    return False


def run_db_import(config: Dict) -> None:
    input_path = _resolve_path(config.get("input_path"))
    target = str(config.get("target", "postgis")).lower()
    table_name = config.get("table_name")
    if_exists = config.get("if_exists", True)

    if target == "postgis":
        pg_cfg = config.get("postgis", {}) or {}
        schema = pg_cfg.get("schema")
        db_url = _build_postgis_url(pg_cfg)
        logger.info("Step 1/3: target=postgis, checking extension status...")
        _ensure_postgis_ready(db_url, schema)
    elif target in {"spatialite", "spatial"}:
        sl_cfg = config.get("spatialite", {}) or config.get("spatial", {}) or {}
        sqlite_path = _resolve_path(sl_cfg.get("path"))
        db_url = f"sqlite:///{sqlite_path}"
        schema = None
        if _is_spatialite_nonempty(sqlite_path):
            logger.info("Step 1/3 skipped: Spatialite DB is not empty (%s).", sqlite_path)
            return
    else:
        raise ValueError("db_import.target must be 'postgis' or 'spatialite' (or alias 'spatial').")

    try:
        from db_Importer import shp2db  # script mode
    except ImportError:
        try:
            from src.preprocess.db_Importer import shp2db  # package mode
        except ImportError as exc:
            raise RuntimeError(
                "Failed to import db_Importer. Please install geopandas/fiona/sqlalchemy first."
            ) from exc

    logger.info("Step 1/3: importing shp to DB. input=%s target=%s", input_path, target)
    shp2db(
        input_path=str(input_path),
        db_url=db_url,
        table_name=table_name,
        schema=schema,
        if_exists=if_exists,
    )
    logger.info("Step 1/3 completed.")


def _build_doc_text(doc: Dict) -> str:
    parts: List[str] = []
    function_id = doc.get("function_id", "")
    if function_id:
        parts.append(function_id)
    chapter_info = doc.get("chapter_info", "")
    if chapter_info:
        parts.append(chapter_info)
    description = doc.get("description", "")
    if description:
        parts.append(description)

    for fd in doc.get("function_definitions", []):
        if isinstance(fd, dict):
            fn = fd.get("function_name", "")
            sig = fd.get("signature_str", "")
            if fn:
                parts.append(fn)
            if sig:
                parts.append(sig)

    for ex in doc.get("examples", []):
        if not isinstance(ex, dict):
            continue
        name = ex.get("name", "")
        if name:
            parts.append(name)
        for step in ex.get("steps", []):
            if not isinstance(step, dict):
                continue
            q = step.get("question", "")
            sql = step.get("sql", "")
            if q:
                parts.append(q)
            if sql:
                parts.append(sql)

    return "\n".join(parts)


def _load_docs(doc_source: Path) -> List[Dict]:
    if not doc_source.exists():
        raise FileNotFoundError(f"Document file not found: {doc_source}")
    with open(doc_source, "r", encoding="utf-8") as f:
        data = json.load(f)
    if not isinstance(data, list):
        raise ValueError("postgis_extracted.json must be a JSON list.")
    return data


def _tokenize(text: str, min_len: int) -> List[str]:
    tokens = re.findall(r"[a-zA-Z_][a-zA-Z0-9_]*", text.lower())
    return [t for t in tokens if len(t) >= min_len]


def _mean_pool(last_hidden_state, attention_mask):
    mask = attention_mask.unsqueeze(-1).expand(last_hidden_state.size()).float()
    masked = last_hidden_state * mask
    summed = masked.sum(dim=1)
    counts = mask.sum(dim=1).clamp(min=1e-9)
    return summed / counts


def _encode_with_embedding_model(
    texts: List[str],
    model_name: str,
    batch_size: int,
    max_length: int,
    trust_remote_code: bool,
) -> List[List[float]]:
    try:
        import torch
        from transformers import AutoModel, AutoTokenizer
    except ImportError as exc:
        raise RuntimeError(
            "Vectorize step needs `transformers` and `torch`. Install from requirements.txt."
        ) from exc

    device = "cuda" if torch.cuda.is_available() else "cpu"
    logger.info("Loading embedding model: %s (device=%s)", model_name, device)

    tokenizer = AutoTokenizer.from_pretrained(model_name, trust_remote_code=trust_remote_code)
    model = AutoModel.from_pretrained(model_name, trust_remote_code=trust_remote_code).to(device)
    model.eval()

    vectors: List[List[float]] = []
    with torch.no_grad():
        for i in range(0, len(texts), batch_size):
            batch = texts[i : i + batch_size]
            inputs = tokenizer(
                batch,
                padding=True,
                truncation=True,
                max_length=max_length,
                return_tensors="pt",
            )
            inputs = {k: v.to(device) for k, v in inputs.items()}
            outputs = model(**inputs)
            if hasattr(outputs, "last_hidden_state"):
                hidden = outputs.last_hidden_state
            elif isinstance(outputs, tuple) and len(outputs) > 0:
                hidden = outputs[0]
            else:
                raise RuntimeError("Unexpected model output format, cannot extract embeddings.")

            emb = _mean_pool(hidden, inputs["attention_mask"])
            emb = torch.nn.functional.normalize(emb, p=2, dim=1)
            vectors.extend(emb.cpu().tolist())

    return vectors


def run_vectorization(config: Dict) -> None:
    doc_source = _resolve_path(config.get("doc_source"), default=PROJECT_ROOT / "data" / "postgis_extracted.json")
    output_path = _resolve_path(
        config.get("output_path"),
        default=PROJECT_ROOT / "data" / "indexes" / "vector" / "qwen3_embedding_0_6b.json",
    )
    if _path_has_content(output_path):
        logger.info("Step 2/3 skipped: vector storage path is not empty (%s).", output_path)
        return
    output_path.parent.mkdir(parents=True, exist_ok=True)

    docs = _load_docs(doc_source)
    model_name = str(config.get("model_name", "Qwen/Qwen3-Embedding-0.6B"))
    batch_size = int(config.get("batch_size", 8))
    max_length = int(config.get("max_length", 1024))
    trust_remote_code = bool(config.get("trust_remote_code", True))

    texts = [_build_doc_text(d) for d in docs]
    doc_ids = [d.get("function_id", f"doc_{i}") for i, d in enumerate(docs)]
    logger.info("Step 2/3: vectorizing postgis docs from %s with model=%s", doc_source, model_name)
    raw_vectors = _encode_with_embedding_model(
        texts=texts,
        model_name=model_name,
        batch_size=batch_size,
        max_length=max_length,
        trust_remote_code=trust_remote_code,
    )

    vectors: List[Dict] = []
    for i, emb in enumerate(raw_vectors):
        vectors.append({"doc_id": i, "function_id": doc_ids[i], "embedding": emb})

    payload = {
        "doc_source": str(doc_source),
        "model_name": model_name,
        "batch_size": batch_size,
        "max_length": max_length,
        "doc_count": len(doc_ids),
        "vectors": vectors,
    }
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False)

    logger.info("Step 2/3 completed. vector_file=%s docs=%d", output_path, len(doc_ids))


def run_keyword_index(config: Dict) -> None:
    doc_source = _resolve_path(config.get("doc_source"), default=PROJECT_ROOT / "data" / "postgis_extracted.json")
    output_path = _resolve_path(config.get("output_path"), default=PROJECT_ROOT / "data" / "indexes" / "keyword" / "keyword_index.json")
    if _path_has_content(output_path):
        logger.info("Step 3/3 skipped: keyword index path is not empty (%s).", output_path)
        return
    output_path.parent.mkdir(parents=True, exist_ok=True)
    min_token_len = int(config.get("min_token_length", 2))

    docs = _load_docs(doc_source)
    token_to_docs: Dict[str, List[int]] = {}
    doc_meta: List[Dict] = []

    logger.info("Step 3/3: building keyword index from %s", doc_source)
    for i, d in enumerate(docs):
        text = _build_doc_text(d)
        tokens = sorted(set(_tokenize(text, min_token_len)))
        for tok in tokens:
            token_to_docs.setdefault(tok, []).append(i)
        doc_meta.append(
            {
                "doc_id": i,
                "function_id": d.get("function_id", f"doc_{i}"),
                "chapter_info": d.get("chapter_info", ""),
            }
        )

    payload = {
        "doc_source": str(doc_source),
        "doc_count": len(docs),
        "token_count": len(token_to_docs),
        "token_to_docs": token_to_docs,
        "docs": doc_meta,
    }
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False)

    logger.info("Step 3/3 completed. keyword_index=%s tokens=%d", output_path, len(token_to_docs))


def run_all() -> None:
    run_db_import(get_config("db_import", {}))
    run_vectorization(get_config("vectorize", {}))
    run_keyword_index(get_config("keyword_search", {}))
    logger.info("All preprocess tasks completed.")


def main() -> None:
    parser = argparse.ArgumentParser(description="Preprocess pipeline (db import + vectorize + keyword index)")
    parser.add_argument(
        "--config",
        type=str,
        default=str(PROJECT_ROOT / "config" / "preprocess.yml"),
        help="Path to preprocess config yaml.",
    )
    args = parser.parse_args()

    ConfigLoader.load_config(args.config)
    run_all()


if __name__ == "__main__":
    main()
