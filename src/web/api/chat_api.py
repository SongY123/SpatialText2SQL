from __future__ import annotations

import asyncio
import importlib
import json
from contextlib import suppress
from typing import Any, Dict, List, Tuple

from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import StreamingResponse

from ..entity.request import ChatFeedbackRequest, ChatSSERequest
from ..service import ChatService, DatabaseService, get_global_chat_service
from utils.config_loader import get_config
from utils.auth_guard import assert_login as _assert_login
from utils.event_types import AgentEventType, AgentName, SSEEventType, event_timestamp


router = APIRouter(prefix="/chat", tags=["chat"])
_database_service = DatabaseService()
_chat_service: ChatService = get_global_chat_service()


def _ok(data=None, message: str = "ok"):
    return {
        "success": True,
        "message": message,
        "data": data,
    }


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
    chat_id = int(body.chat_id)

    if body.context is not None:
        if hasattr(body.context, "model_dump"):
            context_patch = body.context.model_dump()
        else:
            context_patch = body.context.dict()
    else:
        context_patch = None
    try:
        chat_state = _chat_service.resolve_context(
            user_id=current_user_id,
            chat_id=chat_id,
            context=context_patch,
        )
    except ValueError as exc:
        detail = str(exc)
        status = 404 if "not found" in detail.lower() else 400
        raise HTTPException(status_code=status, detail=detail)
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
    if ctx.get("geometry") is not None:
        runtime_context["geometry"] = ctx.get("geometry")
    history = _chat_service.get_history(user_id=current_user_id, chat_id=chat_id, limit=20)
    query_text = str(body.query or "").strip()

    async def event_stream():
        queue: asyncio.Queue = asyncio.Queue()
        system = None
        request_message_id: int | None = None
        agent_stream_buffers: Dict[Tuple[str, int, str], List[str]] = {}

        def _stream_key(agent_name: str, round_id: int, stage: str) -> Tuple[str, int, str]:
            return (str(agent_name or ""), int(round_id or 0), str(stage or ""))

        def _append_agent_stream_delta(event_payload: Dict[str, Any]) -> None:
            if event_payload.get("agent_event_type") != AgentEventType.PROGRESS:
                return
            if "delta" not in event_payload:
                return
            stage = str(event_payload.get("stage") or "")
            if not stage.endswith("_stream"):
                return
            base_stage = stage[: -len("_stream")]
            key = _stream_key(
                agent_name=str(event_payload.get("agent") or ""),
                round_id=int(event_payload.get("round") or 0),
                stage=base_stage,
            )
            delta = str(event_payload.get("delta") or "")
            if delta:
                agent_stream_buffers.setdefault(key, []).append(delta)

        def _build_agent_history_content(event_payload: Dict[str, Any], stream_text: str) -> str:
            content = str(stream_text or "")
            if content.strip():
                return content

            summary = str(event_payload.get("summary") or "").strip()
            if summary:
                return summary

            for text_key in ("result", "message"):
                value = event_payload.get(text_key)
                if isinstance(value, str) and value.strip():
                    return value.strip()

            payload_subset: Dict[str, Any] = {
                "stage": event_payload.get("stage"),
                "round": event_payload.get("round"),
            }
            for key in ("plan", "bundle", "output", "review_report", "result"):
                if key in event_payload and event_payload.get(key) is not None:
                    payload_subset[key] = event_payload.get(key)
            if len(payload_subset) <= 2:
                payload_subset = dict(event_payload)
            return json.dumps(payload_subset, ensure_ascii=False, default=str)

        def _persist_agent_end_if_needed(event_payload: Dict[str, Any]) -> None:
            if event_payload.get("agent_event_type") != AgentEventType.END:
                return

            agent_name = str(event_payload.get("agent") or "").strip()
            if not agent_name:
                return
            # Skip synthetic system lifecycle entries; final answer is already persisted separately.
            if agent_name == AgentName.SYSTEM:
                return

            stage = str(event_payload.get("stage") or "").strip()
            round_id = int(event_payload.get("round") or 0)
            key = _stream_key(agent_name=agent_name, round_id=round_id, stage=stage)
            stream_text = "".join(agent_stream_buffers.pop(key, []))
            content = _build_agent_history_content(event_payload=event_payload, stream_text=stream_text)
            if not str(content or "").strip():
                return

            try:
                row = _chat_service.append_message_with_meta(
                    user_id=current_user_id,
                    chat_id=chat_id,
                    role="assistant",
                    content=content,
                    request_id=request_message_id,
                    agent_name=agent_name,
                )
            except Exception:
                return

            if isinstance(row, dict):
                event_payload["agent_history_id"] = row.get("id")
                event_payload["chat_history_id"] = row.get("id")
                event_payload["request_id"] = row.get("request_id") or request_message_id
                event_payload["agent_name"] = row.get("agent_name") or agent_name

        def emit(event: str, payload: Dict[str, Any]) -> None:
            event_payload = dict(payload or {})
            event_payload.setdefault("timestamp", event_timestamp())
            if request_message_id is not None:
                event_payload.setdefault("request_id", int(request_message_id))
            if event == SSEEventType.PROGRESS:
                _append_agent_stream_delta(event_payload)
                _persist_agent_end_if_needed(event_payload)
            data = {"chat_id": chat_id, **event_payload}
            queue.put_nowait((event, data))

        async def runner() -> None:
            nonlocal system, request_message_id
            has_end_event = False
            try:
                user_msg_row = _chat_service.append_message(
                    user_id=current_user_id,
                    chat_id=chat_id,
                    role="user",
                    content=query_text,
                    context=ctx,
                )
                if isinstance(user_msg_row, dict) and user_msg_row.get("id") is not None:
                    request_message_id = int(user_msg_row["id"])
                system = _build_system_for_jdbc(jdbc_url=jdbc_url)
                emit(
                    SSEEventType.START,
                    {
                        "status": "started",
                        "context": runtime_context,
                        "history_size": len(history),
                        "user_message_id": (user_msg_row or {}).get("id") if isinstance(user_msg_row, dict) else None,
                    },
                )
                answer_text = await system.run(
                    question=query_text,
                    context=runtime_context,
                    chat_history=history,
                    event_callback=emit,
                )
                assistant_msg_row = _chat_service.append_message_with_meta(
                    user_id=current_user_id,
                    chat_id=chat_id,
                    role="assistant",
                    content=answer_text,
                    request_id=request_message_id,
                )
                emit(
                    SSEEventType.END,
                    {
                        "ok": True,
                        "status": "completed",
                        "result": answer_text,
                        "assistant_message_id": (
                            (assistant_msg_row or {}).get("id") if isinstance(assistant_msg_row, dict) else None
                        ),
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


@router.post("/new")
def create_new_chat(request: Request):
    current_user_id = _assert_login(request)
    try:
        row = _chat_service.create_chat(user_id=current_user_id)
        return _ok(data={"chat_id": row.get("chat_id")}, message="chat created")
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc))


@router.get("/{chat_id}/history")
def get_chat_history(chat_id: int, request: Request, limit: int = 100):
    current_user_id = _assert_login(request)
    try:
        rows = _chat_service.get_history_records(
            user_id=current_user_id,
            chat_id=int(chat_id),
            limit=limit,
        )
        return _ok(data=rows, message="chat history fetched")
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc))


@router.post("/feedback")
def set_chat_feedback(body: ChatFeedbackRequest, request: Request):
    current_user_id = _assert_login(request)
    try:
        row = _chat_service.set_message_feedback(
            user_id=current_user_id,
            chat_id=body.chat_id,
            message_id=body.message_id,
            feedback=body.feedback,
        )
        return _ok(data=row, message="chat feedback updated")
    except ValueError as exc:
        detail = str(exc)
        status = 404 if "not found" in detail.lower() else 400
        raise HTTPException(status_code=status, detail=detail)
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc))
