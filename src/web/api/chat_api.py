from __future__ import annotations

import asyncio
import importlib
import json
from contextlib import suppress
from typing import Any, Dict

from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import StreamingResponse

from ..entity.request import ChatSSERequest
from ..service import ChatService, DatabaseService, get_global_chat_service
from utils.config_loader import get_config
from utils.auth_guard import assert_login as _assert_login
from utils.event_types import SSEEventType, event_timestamp


router = APIRouter(prefix="/chat", tags=["chat"])
_database_service = DatabaseService()
_chat_service: ChatService = get_global_chat_service()


def _format_sse(event: str, data: Dict[str, Any]) -> str:
    payload = json.dumps(data, ensure_ascii=False, default=str)
    return f"event: {event}\ndata: {payload}\n\n"


def _build_system_for_jdbc(jdbc_url: str):
    build_dashscope_system = None
    build_ollama_system = None
    build_openai_system = None

    for module_name in ("agent.system_factory", "src.agent.system_factory"):
        try:
            m = importlib.import_module(module_name)
            build_dashscope_system = getattr(m, "build_dashscope_system")
            build_ollama_system = getattr(m, "build_ollama_system")
            build_openai_system = getattr(m, "build_openai_system")
            break
        except Exception:
            continue
    if build_dashscope_system is None or build_ollama_system is None or build_openai_system is None:
        raise RuntimeError("failed to import agent system factory; check agentscope environment.")

    provider = str(get_config("model.provider", "dashscope")).strip().lower()
    max_rounds = int(get_config("agent.max_rounds", 3))
    web_config_path = str(get_config("web.config_path", "src/web/resources/config.yaml"))

    if provider == "ollama":
        model_name = str(get_config("model.ollama.model_name", "qwen3-coder:30b-a3b-fp16"))
        base_url = str(get_config("model.ollama.host", "") or "").strip()
        if base_url and not base_url.startswith("http://") and not base_url.startswith("https://"):
            base_url = f"http://{base_url}"
        return build_ollama_system(
            model_name=model_name,
            jdbc_url=jdbc_url,
            config_path=web_config_path,
            base_url=base_url or None,
            max_rounds=max_rounds,
        )

    if provider == "openai":
        model_name = str(get_config("model.openai.model_name", "gpt-4.1-mini"))
        api_key = get_config("model.openai.api_key")
        return build_openai_system(
            model_name=model_name,
            jdbc_url=jdbc_url,
            config_path=web_config_path,
            api_key=api_key,
            max_rounds=max_rounds,
        )

    model_name = str(get_config("model.dashscope.model_name", "qwen-max"))
    api_key = get_config("model.dashscope.api_key")
    return build_dashscope_system(
        model_name=model_name,
        api_key=api_key,
        jdbc_url=jdbc_url,
        config_path=web_config_path,
        max_rounds=max_rounds,
    )


@router.post("/sse")
async def chat_sse(body: ChatSSERequest, request: Request):
    current_user_id = _assert_login(request)
    chat_id = str(body.chat_id).strip()

    if body.context is not None:
        if hasattr(body.context, "model_dump"):
            context_patch = body.context.model_dump()
        else:
            context_patch = body.context.dict()
    else:
        context_patch = None
    chat_state = _chat_service.upsert_context(
        user_id=current_user_id,
        chat_id=chat_id,
        context=context_patch,
    )
    ctx = dict((chat_state.get("context") or {}))
    database_id = ctx.get("database_id")
    schema_name = str(ctx.get("schema_name") or "").strip()
    if database_id is None or not schema_name:
        raise HTTPException(
            status_code=400,
            detail="context.database_id and context.schema_name are required.",
        )

    db = _database_service.get_database(link_id=int(database_id))
    if db is None:
        raise HTTPException(status_code=404, detail=f"database link not found: link_id={database_id}")
    if int(db.get("user_id")) != int(current_user_id):
        raise HTTPException(status_code=403, detail="Can only access current user's database link.")

    jdbc_url = DatabaseService._patch_jdbc_auth(
        jdbc_url=str(db.get("url") or ""),
        db_type=str(db.get("type") or ""),
        username=db.get("db_username"),
        password=db.get("db_password"),
    )

    runtime_context = {
        "chat_id": chat_id,
        "database_id": int(database_id),
        "schema_name": schema_name,
        "table_list": list(ctx.get("table_list") or []),
        "view_list": list(ctx.get("view_list") or []),
    }
    history = _chat_service.get_history(user_id=current_user_id, chat_id=chat_id, limit=20)
    query_text = str(body.query or "").strip()

    async def event_stream():
        queue: asyncio.Queue = asyncio.Queue()
        system = None

        def emit(event: str, payload: Dict[str, Any]) -> None:
            event_payload = dict(payload or {})
            event_payload.setdefault("timestamp", event_timestamp())
            data = {"chat_id": chat_id, **event_payload}
            queue.put_nowait((event, data))

        async def runner() -> None:
            nonlocal system
            has_end_event = False
            try:
                _chat_service.append_message(
                    user_id=current_user_id,
                    chat_id=chat_id,
                    role="user",
                    content=query_text,
                )
                system = _build_system_for_jdbc(jdbc_url=jdbc_url)
                emit(
                    SSEEventType.START,
                    {
                        "status": "started",
                        "context": runtime_context,
                        "history_size": len(history),
                    },
                )
                answer_text = await system.run(
                    question=query_text,
                    context=runtime_context,
                    chat_history=history,
                    event_callback=emit,
                )
                _chat_service.append_message(
                    user_id=current_user_id,
                    chat_id=chat_id,
                    role="assistant",
                    content=answer_text,
                )
                emit(
                    SSEEventType.END,
                    {
                        "ok": True,
                        "status": "completed",
                        "result": answer_text,
                    },
                )
                has_end_event = True
            except Exception as exc:
                emit(SSEEventType.ERROR, {"message": str(exc)})
                emit(
                    SSEEventType.END,
                    {
                        "ok": False,
                        "status": "failed",
                        "error": str(exc),
                    },
                )
                has_end_event = True
            finally:
                if system is not None:
                    with suppress(Exception):
                        system.close()
                if not has_end_event:
                    emit(
                        SSEEventType.END,
                        {
                            "ok": False,
                            "status": "aborted",
                        },
                    )

        task = asyncio.create_task(runner())
        try:
            while True:
                event, data = await queue.get()
                yield _format_sse(event=event, data=data)
                if event == SSEEventType.END:
                    break
        finally:
            if not task.done():
                task.cancel()
                with suppress(asyncio.CancelledError):
                    await task

    return StreamingResponse(
        event_stream(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )
