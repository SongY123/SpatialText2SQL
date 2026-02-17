import json
import asyncio
import contextvars
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Dict, Optional, Tuple

import yaml
from agentscope.message import TextBlock
from agentscope.tool import ToolResponse, Toolkit

from tools import ChromaVectorStore, GoogleWebSearcher, JdbcDatabaseTool, JsonKeywordSearcher


_CTX_AGENT_NAME: contextvars.ContextVar[str] = contextvars.ContextVar("tool_ctx_agent_name", default="")
_CTX_ROUND: contextvars.ContextVar[int] = contextvars.ContextVar("tool_ctx_round", default=0)
_CTX_STAGE: contextvars.ContextVar[str] = contextvars.ContextVar("tool_ctx_stage", default="")


def _to_tool_response(payload: Dict) -> ToolResponse:
    return ToolResponse(
        content=[TextBlock(type="text", text=json.dumps(payload, ensure_ascii=False, default=str))],
        metadata=payload,
    )


def _ok(payload: Dict) -> ToolResponse:
    return _to_tool_response({"ok": True, **payload})


def _error(exc: Exception) -> ToolResponse:
    return _to_tool_response({"ok": False, "error": str(exc)})


@dataclass
class SpatialText2SQLToolRegistry:
    jdbc_url: Optional[str] = None
    keyword_index_path: Optional[str] = None
    keyword_min_token_length: int = 2
    chroma_path: Optional[str] = None
    vector_collection_name: str = "postgis_extracted"
    vector_model_name: str = "Qwen/Qwen3-Embedding-0.6B"
    vector_batch_size: int = 8
    vector_normalize_embeddings: bool = True
    vector_model_kwargs: Optional[Dict] = None
    vector_tokenizer_kwargs: Optional[Dict] = None

    _db_tool: Optional[JdbcDatabaseTool] = None
    _keyword_searcher: Optional[JsonKeywordSearcher] = None
    _vector_store: Optional[ChromaVectorStore] = None
    _web_searcher: Optional[GoogleWebSearcher] = None
    _tool_event_callback: Optional[Callable[[Dict[str, Any]], Any]] = None

    @classmethod
    def from_agent_config(
        cls,
        config_path: str = "src/web/resources/config.yaml",
        jdbc_url: Optional[str] = None,
    ) -> "SpatialText2SQLToolRegistry":
        root = Path(__file__).resolve().parents[3]
        cfg_path = Path(config_path)
        if not cfg_path.is_absolute():
            cfg_path = root / cfg_path
        cfg = {}
        if cfg_path.exists():
            with open(cfg_path, "r", encoding="utf-8") as f:
                cfg = yaml.safe_load(f) or {}

        kw_cfg = cfg.get("keyword_search", {}) or {}
        vec_cfg = cfg.get("vectorize", {}) or {}

        def resolve(p: Optional[str]) -> Optional[str]:
            if not p:
                return None
            path = Path(p)
            return str(path if path.is_absolute() else root / path)

        return cls(
            jdbc_url=jdbc_url,
            keyword_index_path=resolve(kw_cfg.get("output_path")),
            keyword_min_token_length=int(kw_cfg.get("min_token_length", 2)),
            chroma_path=resolve(vec_cfg.get("chroma_path")),
            vector_collection_name=str(vec_cfg.get("collection_name", "postgis_extracted")),
            vector_model_name=str(vec_cfg.get("model_name", "Qwen/Qwen3-Embedding-0.6B")),
            vector_batch_size=int(vec_cfg.get("batch_size", 8)),
            vector_normalize_embeddings=bool(vec_cfg.get("normalize_embeddings", True)),
            vector_model_kwargs=vec_cfg.get("model_kwargs") or {},
            vector_tokenizer_kwargs=vec_cfg.get("tokenizer_kwargs") or {},
        )

    def _get_db_tool(self) -> JdbcDatabaseTool:
        if self._db_tool is None:
            if not self.jdbc_url:
                raise ValueError("jdbc_url is required for DB tools.")
            self._db_tool = JdbcDatabaseTool(jdbc_url=self.jdbc_url)
        return self._db_tool

    def _get_keyword_searcher(self) -> JsonKeywordSearcher:
        if self._keyword_searcher is None:
            if not self.keyword_index_path:
                raise ValueError("keyword_index_path is required for keyword/postgis docs search.")
            self._keyword_searcher = JsonKeywordSearcher.load(
                index_path=self.keyword_index_path,
                min_token_length=self.keyword_min_token_length,
            )
        return self._keyword_searcher

    def _get_vector_store(self) -> ChromaVectorStore:
        if self._vector_store is None:
            if not self.chroma_path:
                raise ValueError("chroma_path is required for vector similarity search.")
            self._vector_store = ChromaVectorStore(
                chroma_path=self.chroma_path,
                collection_name=self.vector_collection_name,
                model_name=self.vector_model_name,
                batch_size=self.vector_batch_size,
                normalize_embeddings=self.vector_normalize_embeddings,
                model_kwargs=self.vector_model_kwargs or {},
                tokenizer_kwargs=self.vector_tokenizer_kwargs or {},
            )
        return self._vector_store

    def _get_web_searcher(self) -> GoogleWebSearcher:
        if self._web_searcher is None:
            self._web_searcher = GoogleWebSearcher()
        return self._web_searcher

    def set_tool_event_callback(self, callback: Optional[Callable[[Dict[str, Any]], Any]]) -> None:
        self._tool_event_callback = callback

    def push_tool_stream_context(self, agent_name: str, round_id: int, stage: str) -> Tuple[contextvars.Token, ...]:
        return (
            _CTX_AGENT_NAME.set(str(agent_name or "")),
            _CTX_ROUND.set(int(round_id or 0)),
            _CTX_STAGE.set(str(stage or "")),
        )

    def pop_tool_stream_context(self, tokens: Tuple[contextvars.Token, ...]) -> None:
        if not tokens:
            return
        if len(tokens) >= 1:
            _CTX_AGENT_NAME.reset(tokens[0])
        if len(tokens) >= 2:
            _CTX_ROUND.reset(tokens[1])
        if len(tokens) >= 3:
            _CTX_STAGE.reset(tokens[2])

    async def _emit_tool_event(self, tool_name: str, status: str, detail: Optional[Dict[str, Any]] = None) -> None:
        callback = self._tool_event_callback
        if callback is None:
            return
        payload: Dict[str, Any] = {
            "agent": _CTX_AGENT_NAME.get() or "",
            "round": _CTX_ROUND.get() or 0,
            "stage": _CTX_STAGE.get() or "",
            "tool_name": str(tool_name),
            "tool_status": str(status),
            "detail": detail or {},
        }
        try:
            maybe = callback(payload)
            if asyncio.iscoroutine(maybe):
                await maybe
        except Exception:
            return

    async def jdbc_introspect_catalog(
        self,
        schema_name: Optional[str] = None,
        include_views: bool = False,
    ) -> ToolResponse:
        """Introspect DB catalog (tables, columns, indexes, constraints).

        Args:
            schema_name (str | None):
                Optional schema name.
            include_views (bool):
                Whether to include views.
        """
        await self._emit_tool_event(
            "jdbc_introspect_catalog",
            "start",
            {"schema_name": schema_name, "include_views": bool(include_views)},
        )
        t0 = time.perf_counter()
        try:
            payload = await asyncio.to_thread(
                self._get_db_tool().introspect_catalog,
                schema=schema_name,
                include_views=include_views,
            )
            await self._emit_tool_event(
                "jdbc_introspect_catalog",
                "end",
                {
                    "elapsed_ms": int((time.perf_counter() - t0) * 1000),
                    "tables": len((payload or {}).get("tables", []) if isinstance(payload, dict) else []),
                },
            )
            return _ok({"result": payload})
        except Exception as exc:
            await self._emit_tool_event(
                "jdbc_introspect_catalog",
                "error",
                {"elapsed_ms": int((time.perf_counter() - t0) * 1000), "error": str(exc)},
            )
            return _error(exc)

    async def jdbc_estimate_rowcount(
        self,
        table: str,
        schema_name: Optional[str] = None,
    ) -> ToolResponse:
        """Estimate row count for one table.

        Args:
            table (str):
                Table name.
            schema_name (str | None):
                Optional schema name.
        """
        await self._emit_tool_event(
            "jdbc_estimate_rowcount",
            "start",
            {"table": table, "schema_name": schema_name},
        )
        t0 = time.perf_counter()
        try:
            payload = await asyncio.to_thread(
                self._get_db_tool().estimate_rowcount,
                table_name=table,
                schema=schema_name,
            )
            await self._emit_tool_event(
                "jdbc_estimate_rowcount",
                "end",
                {"elapsed_ms": int((time.perf_counter() - t0) * 1000)},
            )
            return _ok({"result": payload})
        except Exception as exc:
            await self._emit_tool_event(
                "jdbc_estimate_rowcount",
                "error",
                {"elapsed_ms": int((time.perf_counter() - t0) * 1000), "error": str(exc)},
            )
            return _error(exc)

    async def jdbc_topk_distinct(
        self,
        table: str,
        column: str,
        k: int = 10,
        schema_name: Optional[str] = None,
    ) -> ToolResponse:
        """Get top-k distinct values for one column.

        Args:
            table (str):
                Table name.
            column (str):
                Column name.
            k (int):
                Number of values.
            schema_name (str | None):
                Optional schema name.
        """
        await self._emit_tool_event(
            "jdbc_topk_distinct",
            "start",
            {"table": table, "column": column, "k": int(k), "schema_name": schema_name},
        )
        t0 = time.perf_counter()
        try:
            payload = await asyncio.to_thread(
                self._get_db_tool().topk_distinct,
                table_name=table,
                column_name=column,
                k=k,
                schema=schema_name,
            )
            await self._emit_tool_event(
                "jdbc_topk_distinct",
                "end",
                {"elapsed_ms": int((time.perf_counter() - t0) * 1000)},
            )
            return _ok({"result": payload})
        except Exception as exc:
            await self._emit_tool_event(
                "jdbc_topk_distinct",
                "error",
                {"elapsed_ms": int((time.perf_counter() - t0) * 1000), "error": str(exc)},
            )
            return _error(exc)

    async def jdbc_execute_readonly(
        self,
        sql: str,
        params: Optional[Dict] = None,
        timeout_ms: int = 8000,
        max_rows: int = 100,
    ) -> ToolResponse:
        """Execute read-only SQL with timeout and row limits.

        Args:
            sql (str):
                SQL text.
            params (dict | None):
                Optional bind parameters.
            timeout_ms (int):
                Statement timeout in milliseconds.
            max_rows (int):
                Maximum number of rows to return.
        """
        await self._emit_tool_event(
            "jdbc_execute_readonly",
            "start",
            {
                "timeout_ms": int(timeout_ms),
                "max_rows": int(max_rows),
                "sql_preview": str(sql or "")[:240],
            },
        )
        t0 = time.perf_counter()
        try:
            payload = await asyncio.to_thread(
                self._get_db_tool().execute_readonly,
                sql=sql,
                params=params,
                timeout_ms=timeout_ms,
                max_rows=max_rows,
            )
            result = (payload or {}) if isinstance(payload, dict) else {}
            await self._emit_tool_event(
                "jdbc_execute_readonly",
                "end",
                {
                    "elapsed_ms": int((time.perf_counter() - t0) * 1000),
                    "status": result.get("status"),
                    "row_count": result.get("row_count"),
                },
            )
            return _ok({"result": payload})
        except Exception as exc:
            await self._emit_tool_event(
                "jdbc_execute_readonly",
                "error",
                {"elapsed_ms": int((time.perf_counter() - t0) * 1000), "error": str(exc)},
            )
            return _error(exc)

    async def jdbc_explain(
        self,
        sql: str,
        params: Optional[Dict] = None,
    ) -> ToolResponse:
        """Run EXPLAIN for read-only SQL.

        Args:
            sql (str):
                SQL text.
            params (dict | None):
                Optional bind parameters.
        """
        await self._emit_tool_event(
            "jdbc_explain",
            "start",
            {"sql_preview": str(sql or "")[:240]},
        )
        t0 = time.perf_counter()
        try:
            payload = await asyncio.to_thread(self._get_db_tool().explain, sql=sql, params=params)
            await self._emit_tool_event(
                "jdbc_explain",
                "end",
                {"elapsed_ms": int((time.perf_counter() - t0) * 1000)},
            )
            return _ok({"result": payload})
        except Exception as exc:
            await self._emit_tool_event(
                "jdbc_explain",
                "error",
                {"elapsed_ms": int((time.perf_counter() - t0) * 1000), "error": str(exc)},
            )
            return _error(exc)

    async def keyword_search(self, query: str, k: int = 10) -> ToolResponse:
        """Search keyword index for relevant PostGIS docs.

        Args:
            query (str):
                Query text.
            k (int):
                Top-k results.
        """
        await self._emit_tool_event("keyword_search", "start", {"query": str(query or "")[:180], "k": int(k)})
        t0 = time.perf_counter()
        try:
            items = await asyncio.to_thread(self._get_keyword_searcher().search, query=query, top_k=k)
            await self._emit_tool_event(
                "keyword_search",
                "end",
                {"elapsed_ms": int((time.perf_counter() - t0) * 1000), "count": len(items)},
            )
            return _ok({"items": items, "count": len(items)})
        except Exception as exc:
            await self._emit_tool_event(
                "keyword_search",
                "error",
                {"elapsed_ms": int((time.perf_counter() - t0) * 1000), "error": str(exc)},
            )
            return _error(exc)

    async def vector_similarity_search(self, query: str, k: int = 10) -> ToolResponse:
        """Search vector store by semantic similarity.

        Args:
            query (str):
                Query text.
            k (int):
                Top-k results.
        """
        await self._emit_tool_event(
            "vector_similarity_search",
            "start",
            {"query": str(query or "")[:180], "k": int(k)},
        )
        t0 = time.perf_counter()
        try:
            items = await asyncio.to_thread(self._get_vector_store().search, query=query, top_k=k)
            await self._emit_tool_event(
                "vector_similarity_search",
                "end",
                {"elapsed_ms": int((time.perf_counter() - t0) * 1000), "count": len(items)},
            )
            return _ok({"items": items, "count": len(items)})
        except Exception as exc:
            await self._emit_tool_event(
                "vector_similarity_search",
                "error",
                {"elapsed_ms": int((time.perf_counter() - t0) * 1000), "error": str(exc)},
            )
            return _error(exc)

    async def postgis_docs_search(self, query: str, k: int = 8) -> ToolResponse:
        """Search PostGIS documentation with hybrid retrieval (vector + keyword).

        Args:
            query (str):
                Query text.
            k (int):
                Top-k for each retriever before merge.
        """
        await self._emit_tool_event("postgis_docs_search", "start", {"query": str(query or "")[:180], "k": int(k)})
        t0 = time.perf_counter()
        try:
            v_items = await asyncio.to_thread(self._get_vector_store().search, query=query, top_k=k)
            k_items = await asyncio.to_thread(self._get_keyword_searcher().search, query=query, top_k=k)

            merged: Dict[str, Dict] = {}
            for item in v_items:
                key = str(item.get("id"))
                merged[key] = {
                    "id": item.get("id"),
                    "document": item.get("document"),
                    "metadata": item.get("metadata"),
                    "vector_distance": item.get("distance"),
                    "keyword_score": None,
                }

            for item in k_items:
                key = str(item.get("metadata", {}).get("function_id") or item.get("doc_id"))
                current = merged.get(
                    key,
                    {
                        "id": key,
                        "document": item.get("document"),
                        "metadata": item.get("metadata"),
                        "vector_distance": None,
                        "keyword_score": None,
                    },
                )
                current["keyword_score"] = item.get("score")
                if not current.get("document"):
                    current["document"] = item.get("document")
                if not current.get("metadata"):
                    current["metadata"] = item.get("metadata")
                merged[key] = current

            items = list(merged.values())
            items.sort(
                key=lambda x: (
                    -float(x["keyword_score"]) if x.get("keyword_score") is not None else 0.0,
                    float(x["vector_distance"]) if x.get("vector_distance") is not None else float("inf"),
                ),
            )
            out_items = items[: max(1, int(k))]
            await self._emit_tool_event(
                "postgis_docs_search",
                "end",
                {"elapsed_ms": int((time.perf_counter() - t0) * 1000), "count": len(out_items)},
            )
            return _ok({"items": out_items, "count": len(items)})
        except Exception as exc:
            await self._emit_tool_event(
                "postgis_docs_search",
                "error",
                {"elapsed_ms": int((time.perf_counter() - t0) * 1000), "error": str(exc)},
            )
            return _error(exc)

    async def web_search(self, query: str, k: int = 5) -> ToolResponse:
        """Search web pages by Google Custom Search.

        Args:
            query (str):
                Query text.
            k (int):
                Top-k results.
        """
        await self._emit_tool_event("web_search", "start", {"query": str(query or "")[:180], "k": int(k)})
        t0 = time.perf_counter()
        try:
            items = await asyncio.to_thread(self._get_web_searcher().search, query=query, top_k=k)
            await self._emit_tool_event(
                "web_search",
                "end",
                {"elapsed_ms": int((time.perf_counter() - t0) * 1000), "count": len(items)},
            )
            return _ok({"items": items, "count": len(items)})
        except Exception as exc:
            await self._emit_tool_event(
                "web_search",
                "error",
                {"elapsed_ms": int((time.perf_counter() - t0) * 1000), "error": str(exc)},
            )
            return _error(exc)

    def register_db_context_tools(self, toolkit: Optional[Toolkit] = None) -> Toolkit:
        tk = toolkit or Toolkit()
        tk.register_tool_function(self.jdbc_introspect_catalog, func_name="jdbc_introspect_catalog")
        tk.register_tool_function(self.jdbc_estimate_rowcount, func_name="jdbc_estimate_rowcount")
        tk.register_tool_function(self.jdbc_topk_distinct, func_name="jdbc_topk_distinct")
        tk.register_tool_function(self.jdbc_execute_readonly, func_name="jdbc_execute_readonly")
        tk.register_tool_function(self.jdbc_explain, func_name="jdbc_explain")
        return tk

    def register_knowledge_tools(self, toolkit: Optional[Toolkit] = None) -> Toolkit:
        tk = toolkit or Toolkit()
        tk.register_tool_function(self.postgis_docs_search, func_name="postgis_docs_search")
        tk.register_tool_function(self.web_search, func_name="web_search")
        return tk

    def register_sql_builder_tools(self, toolkit: Optional[Toolkit] = None) -> Toolkit:
        tk = toolkit or Toolkit()
        tk.register_tool_function(self.jdbc_introspect_catalog, func_name="jdbc_introspect_catalog")
        tk.register_tool_function(self.jdbc_estimate_rowcount, func_name="jdbc_estimate_rowcount")
        tk.register_tool_function(self.jdbc_topk_distinct, func_name="jdbc_topk_distinct")
        tk.register_tool_function(self.jdbc_execute_readonly, func_name="jdbc_execute_readonly")
        tk.register_tool_function(self.jdbc_explain, func_name="jdbc_explain", namesake_strategy="override")
        return tk

    def close(self) -> None:
        if self._db_tool is not None:
            self._db_tool.close()


def build_role_toolkits(registry: SpatialText2SQLToolRegistry) -> Dict[str, Toolkit]:
    return {
        "orchestrator": Toolkit(),
        "db_context": registry.register_db_context_tools(Toolkit()),
        "knowledge": registry.register_knowledge_tools(Toolkit()),
        "sql_builder": registry.register_sql_builder_tools(Toolkit()),
        "sql_reviewer": Toolkit(),
    }
