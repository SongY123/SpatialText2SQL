from __future__ import annotations

import asyncio
import json
import re
from contextlib import suppress
from datetime import date, datetime, time
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional
from uuid import uuid4

from agentscope.formatter import FormatterBase
from agentscope.message import Msg
from agentscope.model import ChatModelBase
from agentscope.pipeline import MsgHub

from agent.db_context_agent import DBContextAgent
from agent.knowledge_agent import KnowledgeAgent
from agent.orchestrator_agent import OrchestratorAgent
from agent.sql_builder_agent import SQLBuilderAgent
from agent.sql_reviewer_agent import SQLReviewerAgent
from agent.tools import SpatialText2SQLToolRegistry, build_role_toolkits
from utils.event_types import AgentEventType, AgentName, SSEEventType, event_timestamp


def _msg_to_text(msg: Msg) -> str:
    if isinstance(msg.content, str):
        return msg.content
    try:
        chunks: List[str] = []
        for block in msg.content:
            if isinstance(block, dict) and block.get("type") == "text":
                chunks.append(str(block.get("text", "")))
            else:
                chunks.append(str(block))
        return "\n".join(chunks)
    except Exception:
        return str(msg.content)


def _extract_first_json(text: str) -> Optional[Dict]:
    s = str(text or "").strip()
    if not s:
        return None

    def _try_parse(candidate: str) -> Optional[Dict]:
        if not candidate:
            return None
        try:
            obj = json.loads(candidate)
            return obj if isinstance(obj, dict) else None
        except Exception:
            return None

    whole = _try_parse(s)
    if whole is not None:
        return whole

    # Preferred: fenced JSON block for "summary + structured output" responses.
    fenced_blocks = re.findall(r"```(?:json)?\s*(.*?)```", s, flags=re.DOTALL | re.IGNORECASE)
    for block in reversed(fenced_blocks):
        parsed = _try_parse(block.strip())
        if parsed is not None:
            return parsed

    tagged_blocks = re.findall(
        r"<structured_json>\s*(.*?)\s*</structured_json>",
        s,
        flags=re.DOTALL | re.IGNORECASE,
    )
    for block in reversed(tagged_blocks):
        parsed = _try_parse(block.strip())
        if parsed is not None:
            return parsed

    # Fallback: scan the text and decode first valid JSON object.
    decoder = json.JSONDecoder()
    for idx, char in enumerate(s):
        if char != "{":
            continue
        try:
            obj, _ = decoder.raw_decode(s[idx:])
            if isinstance(obj, dict):
                return obj
        except Exception:
            continue
    return None


def _extract_summary_text(text: str, max_chars: int = 1200) -> str:
    s = str(text or "").strip()
    if not s:
        return ""
    s = re.sub(r"```(?:json)?\s*.*?```", "", s, flags=re.DOTALL | re.IGNORECASE)
    s = re.sub(r"<structured_json>\s*.*?\s*</structured_json>", "", s, flags=re.DOTALL | re.IGNORECASE)
    s = re.sub(r"\n{3,}", "\n\n", s).strip()
    if len(s) > max_chars:
        return s[: max_chars - 3].rstrip() + "..."
    return s


def _extract_sql_text(text: str) -> str:
    s = str(text or "").strip()
    if not s:
        return ""
    m = re.search(r"```(?:sql)?\s*(.*?)```", s, flags=re.DOTALL | re.IGNORECASE)
    if m:
        return m.group(1).strip()
    return s


def _looks_like_sql(text: str) -> bool:
    return bool(re.search(r"^\s*(WITH|SELECT)\b", str(text or "").strip(), flags=re.IGNORECASE))


def _to_sql_literal(value: Any) -> str:
    if value is None:
        return "NULL"
    if isinstance(value, bool):
        return "TRUE" if value else "FALSE"
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return str(value)
    if isinstance(value, (date, datetime, time)):
        text = value.isoformat().replace("'", "''")
        return f"'{text}'"
    if isinstance(value, (list, tuple, set)):
        return "(" + ", ".join(_to_sql_literal(v) for v in value) + ")"
    text = str(value).replace("'", "''")
    return f"'{text}'"


def _render_sql_with_params(sql_text: str, params: Optional[Dict[str, Any]]) -> str:
    sql = _extract_sql_text(sql_text)
    if not sql:
        return ""
    if not isinstance(params, dict) or not params:
        return sql

    pattern = re.compile(r"(?<!:):([A-Za-z_][A-Za-z0-9_]*)")

    def _replace(match: re.Match) -> str:
        key = str(match.group(1))
        if key in params:
            return _to_sql_literal(params[key])
        # Ensure returned SQL is still complete even when one bind is missing.
        return "NULL"

    return pattern.sub(_replace, sql)


def _dedupe_keep_order(values: List[str]) -> List[str]:
    out: List[str] = []
    seen = set()
    for value in values:
        key = str(value or "").strip().lower()
        if not key or key in seen:
            continue
        seen.add(key)
        out.append(str(value).strip())
    return out


def _contains_any_token(text: str, tokens: List[str]) -> bool:
    t = str(text or "").lower()
    return any(token in t for token in tokens)


_NEAREST_TOKENS = [
    "nearest",
    "closest",
    "closest to",
    "nearest to",
    "near",
]

_SPATIAL_TOKENS = [
    "within",
    "distance",
    "near",
    "intersects",
    "buffer",
    "area",
    "length",
    "dwithin",
    "closest",
    "nearest",
]

_COMMON_TARGET_CATEGORIES = [
    "restaurant",
    "cafe",
    "bar",
    "hotel",
    "hospital",
    "school",
    "bank",
    "pharmacy",
    "station",
    "park",
    "museum",
    "supermarket",
]

_ENTITY_STOPWORDS = {
    "Find",
    "List",
    "Show",
    "Give",
    "Get",
    "Where",
    "What",
    "Which",
    "Who",
    "When",
    "How",
}


def _extract_entity_candidates(question: str) -> List[str]:
    q = str(question or "").strip()
    if not q:
        return []

    candidates: List[str] = []

    for m in re.findall(r"['\"“”]([^'\"“”]{2,120})['\"“”]", q):
        candidates.append(m.strip())

    for m in re.finditer(r"\b([A-Z][\w&'/-]*(?:\s+[A-Z][\w&'/-]*){0,5})\b", q):
        phrase = m.group(1).strip()
        if not phrase:
            continue
        first_token = phrase.split()[0]
        if first_token in _ENTITY_STOPWORDS:
            continue
        candidates.append(phrase)

    return _dedupe_keep_order(candidates)[:5]


def _extract_target_categories(question: str) -> List[str]:
    q = str(question or "").lower()
    out: List[str] = []
    for category in _COMMON_TARGET_CATEGORIES:
        if re.search(rf"\b{re.escape(category)}s?\b", q):
            out.append(category)
    return _dedupe_keep_order(out)


def _to_string_list(items: Any) -> List[str]:
    if not isinstance(items, list):
        return []
    out: List[str] = []
    for item in items:
        text = str(item or "").strip()
        if not text:
            continue
        out.append(text)
    return out


def _normalize_runtime_context(context: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    ctx = dict(context or {})
    schema_name = str(ctx.get("schema_name") or "").strip()
    return {
        "chat_id": str(ctx.get("chat_id") or "").strip(),
        "database_id": ctx.get("database_id"),
        "schema_name": schema_name,
        "table_list": _to_string_list(ctx.get("table_list")),
        "view_list": _to_string_list(ctx.get("view_list")),
    }


def _default_db_context_request(question: str, context: Optional[Dict[str, Any]] = None) -> Dict:
    q = str(question or "")
    ctx = _normalize_runtime_context(context)
    q_lower = q.lower()
    signals = [token for token in _SPATIAL_TOKENS if token in q_lower]
    entity_candidates = _extract_entity_candidates(q)
    target_categories = _extract_target_categories(q)
    nearest_query = _contains_any_token(q, _NEAREST_TOKENS)

    keywords = q.split()[:10]
    keywords.extend(ctx.get("table_list", [])[:6])
    keywords.extend(ctx.get("view_list", [])[:6])

    expected_outputs: List[str] = []
    if nearest_query:
        expected_outputs.extend(["id", "name", "distance"])
    if target_categories:
        expected_outputs.append("category")

    likely_filters: List[str] = []
    if target_categories:
        likely_filters.extend(["fclass", "category", "type"])

    probe_hints: List[str] = []
    if entity_candidates:
        probe_hints.append(
            "Use jdbc_execute_readonly to locate exact/near-exact matches for entity_candidates in name-like columns."
        )
        probe_hints.append("Return matched table, text column, geometry column, and confidence in entity_resolution.")
    if target_categories:
        probe_hints.append(
            "Use tiny probes or jdbc_topk_distinct to confirm target category values in likely filter columns."
        )

    schema_name = str(ctx.get("schema_name") or "").strip()
    schema_whitelist = [schema_name] if schema_name else ["public"]
    return {
        "question": q,
        "focus": {
            "keywords": keywords,
            "expected_outputs": _dedupe_keep_order(expected_outputs),
            "likely_filters": _dedupe_keep_order(likely_filters),
            "spatial_signals": signals,
            "entity_candidates": entity_candidates,
            "target_categories": target_categories,
        },
        "constraints": {
            "schema_whitelist": schema_whitelist,
            "max_tables": 8,
            "max_columns_per_table": 12,
        },
        "probe_hints": probe_hints,
        "runtime_context": ctx,
    }


def _default_knowledge_request(
    question: str,
    error_text: Optional[str] = None,
    context: Optional[Dict[str, Any]] = None,
) -> Dict:
    ctx = _normalize_runtime_context(context)
    q = str(question or "")
    nearest_query = _contains_any_token(q, _NEAREST_TOKENS)
    topics = ["SRID alignment", "geometry vs geography", "distance units"]
    if nearest_query:
        topics.extend(["ST_Distance", "nearest-neighbor ordering", "anchor geometry resolution"])
    else:
        topics.extend(["ST_DWithin", "ST_Transform"])
    return {
        "question": question,
        "focus": {
            "postgis_topics": _dedupe_keep_order(topics),
            "need_websearch": bool(error_text),
            "error_text": error_text,
        },
        "runtime_context": ctx,
    }


@dataclass
class _AgentTextStreamState:
    stream_id: str
    chunk_index: int = 0
    emitted_chunks: int = 0
    emitted_chars: int = 0
    pending: str = ""
    cursor: str = ""
    has_emitted_content: bool = False
    input_closed: bool = False
    final_emitted: bool = False
    cumulative_mode: Optional[bool] = None


@dataclass
class SpatialText2SQLMultiAgentSystem:
    model: ChatModelBase
    formatter: FormatterBase
    tool_registry: SpatialText2SQLToolRegistry
    max_rounds: int = 3
    round_traces: List[Dict[str, Any]] = field(default_factory=list)

    def __post_init__(self) -> None:
        toolkits = build_role_toolkits(self.tool_registry)
        self.orchestrator = OrchestratorAgent(
            model=self.model,
            formatter=self.formatter,
            toolkit=toolkits["orchestrator"],
        )
        self.db_context_agent = DBContextAgent(
            model=self.model,
            formatter=self.formatter,
            toolkit=toolkits["db_context"],
        )
        self.knowledge_agent = KnowledgeAgent(
            model=self.model,
            formatter=self.formatter,
            toolkit=toolkits["knowledge"],
        )
        self.sql_builder_agent = SQLBuilderAgent(
            model=self.model,
            formatter=self.formatter,
            toolkit=toolkits["sql_builder"],
        )
        self.sql_reviewer_agent = SQLReviewerAgent(
            model=self.model,
            formatter=self.formatter,
            toolkit=toolkits["sql_reviewer"],
        )

    async def _emit_tool_progress(
        self,
        event_callback: Optional[Callable[[str, Dict[str, Any]], Any]],
        payload: Dict[str, Any],
    ) -> None:
        agent_name = str(payload.get("agent") or AgentName.SYSTEM)
        detail = payload.get("detail") or {}
        tool_name = str(payload.get("tool_name") or "")
        tool_status = str(payload.get("tool_status") or "")
        stage = str(payload.get("stage") or "")
        round_id = int(payload.get("round") or 0)
        await self._emit_agent(
            event_callback=event_callback,
            agent_name=agent_name,
            agent_event_type=AgentEventType.PROGRESS,
            payload={
                "round": round_id,
                "stage": stage,
                "tool_event": {
                    "name": tool_name,
                    "status": tool_status,
                    "detail": detail,
                },
            },
        )

    async def _run_agent_with_tool_context(
        self,
        agent,
        message: Msg,
        agent_name: str,
        round_id: int,
        stage: str,
        event_callback: Optional[Callable[[str, Dict[str, Any]], Any]] = None,
    ):
        tokens = self.tool_registry.push_tool_stream_context(
            agent_name=agent_name,
            round_id=round_id,
            stage=stage,
        )
        stream_stage = f"{stage}_stream"
        stream_state = _AgentTextStreamState(
            stream_id=f"{agent_name}:{stream_stage}:{round_id}:{uuid4().hex[:12]}",
        )
        has_stream_callback = False

        async def _on_agent_text(text: str) -> None:
            await self._append_agent_text_stream(
                event_callback=event_callback,
                agent_name=agent_name,
                round_id=round_id,
                stage=stream_stage,
                state=stream_state,
                text=text,
            )

        try:
            if hasattr(agent, "set_stream_text_callback"):
                with suppress(Exception):
                    agent.set_stream_text_callback(_on_agent_text)
                    has_stream_callback = True
            result = await agent(message)
            streamed = await self._finalize_agent_text_stream(
                event_callback=event_callback,
                agent_name=agent_name,
                round_id=round_id,
                stage=stream_stage,
                state=stream_state,
            )
            return result, streamed
        finally:
            if has_stream_callback:
                with suppress(Exception):
                    agent.set_stream_text_callback(None)
            self.tool_registry.pop_tool_stream_context(tokens)

    async def _emit(
        self,
        event_callback: Optional[Callable[[str, Dict[str, Any]], Any]],
        event: str,
        payload: Dict[str, Any],
    ) -> None:
        if event_callback is None:
            return
        try:
            event_payload = dict(payload or {})
            event_payload.setdefault("timestamp", event_timestamp())
            maybe = event_callback(event, event_payload)
            if asyncio.iscoroutine(maybe):
                await maybe
        except Exception:
            return

    async def _emit_agent(
        self,
        event_callback: Optional[Callable[[str, Dict[str, Any]], Any]],
        agent_name: str,
        agent_event_type: str,
        payload: Optional[Dict[str, Any]] = None,
    ) -> None:
        data: Dict[str, Any] = {
            "agent": agent_name,
            "agent_event_type": agent_event_type,
        }
        if payload:
            data.update(payload)
        await self._emit(
            event_callback=event_callback,
            event=SSEEventType.PROGRESS,
            payload=data,
        )

    async def _emit_agent_text_stream(
        self,
        event_callback: Optional[Callable[[str, Dict[str, Any]], Any]],
        agent_name: str,
        round_id: int,
        stage: str,
        text: str,
        max_chars: int = 12000,
        chunk_size: int = 200,
        max_chunks: int = 200,
    ) -> None:
        content = str(text or "")
        if not content.strip():
            return

        state = _AgentTextStreamState(
            stream_id=f"{agent_name}:{stage}:{round_id}:{uuid4().hex[:12]}",
        )
        await self._append_agent_text_stream(
            event_callback=event_callback,
            agent_name=agent_name,
            round_id=round_id,
            stage=stage,
            state=state,
            text=content,
            max_chars=max_chars,
            chunk_size=chunk_size,
            max_chunks=max_chunks,
            assume_delta=True,
        )
        await self._finalize_agent_text_stream(
            event_callback=event_callback,
            agent_name=agent_name,
            round_id=round_id,
            stage=stage,
            state=state,
            chunk_size=chunk_size,
            max_chunks=max_chunks,
        )

    @staticmethod
    def _normalize_stream_delta(state: _AgentTextStreamState, text: str) -> str:
        incoming = str(text or "")
        if not incoming:
            return ""
        previous = state.cursor

        if state.cumulative_mode is True:
            if previous and incoming.startswith(previous):
                state.cursor = incoming
                return incoming[len(previous) :]
            if previous and len(incoming) <= len(previous):
                return ""
            state.cursor = incoming
            return incoming

        if state.cumulative_mode is False:
            state.cursor = incoming
            return incoming

        # Unknown mode: detect whether callback emits cumulative text or deltas.
        if not previous:
            state.cursor = incoming
            return incoming
        if incoming == previous:
            return ""
        if len(incoming) > len(previous) and incoming.startswith(previous):
            state.cumulative_mode = True
            state.cursor = incoming
            return incoming[len(previous) :]
        if len(incoming) < len(previous) and previous.startswith(incoming):
            state.cumulative_mode = True
            return ""

        state.cumulative_mode = False
        state.cursor = incoming
        return incoming

    async def _emit_agent_stream_chunk(
        self,
        event_callback: Optional[Callable[[str, Dict[str, Any]], Any]],
        agent_name: str,
        round_id: int,
        stage: str,
        state: _AgentTextStreamState,
        delta: str,
        is_final_chunk: bool,
        max_chunks: int = 200,
    ) -> bool:
        if not is_final_chunk and max_chunks > 0 and state.emitted_chunks >= max_chunks:
            return False

        await self._emit_agent(
            event_callback=event_callback,
            agent_name=agent_name,
            agent_event_type=AgentEventType.PROGRESS,
            payload={
                "round": round_id,
                "stage": stage,
                "stream_id": state.stream_id,
                "chunk_index": state.chunk_index,
                "is_final_chunk": is_final_chunk,
                "delta": delta,
            },
        )
        state.chunk_index += 1
        if delta:
            state.has_emitted_content = True
            state.emitted_chunks += 1
            state.emitted_chars += len(delta)

        # Yield control so frontend can progressively receive chunks.
        await asyncio.sleep(0)
        return True

    async def _append_agent_text_stream(
        self,
        event_callback: Optional[Callable[[str, Dict[str, Any]], Any]],
        agent_name: str,
        round_id: int,
        stage: str,
        state: _AgentTextStreamState,
        text: str,
        max_chars: int = 12000,
        chunk_size: int = 200,
        max_chunks: int = 200,
        assume_delta: bool = False,
    ) -> bool:
        if state.input_closed:
            return state.has_emitted_content

        incoming = str(text or "")
        if not incoming:
            return state.has_emitted_content

        delta = incoming if assume_delta else self._normalize_stream_delta(state, incoming)
        if not delta:
            return state.has_emitted_content

        chunk_size = max(1, int(chunk_size or 1))
        max_chars = int(max_chars or 0)
        max_chunks = int(max_chunks or 0)

        if max_chars > 0:
            current_chars = state.emitted_chars + len(state.pending)
            remaining = max_chars - current_chars
            if remaining <= 0:
                state.input_closed = True
                return state.has_emitted_content
            if len(delta) > remaining:
                delta = delta[:remaining] + "\n...[stream truncated]"
                state.input_closed = True

        state.pending += delta
        while len(state.pending) >= chunk_size:
            piece = state.pending[:chunk_size]
            state.pending = state.pending[chunk_size:]
            emitted = await self._emit_agent_stream_chunk(
                event_callback=event_callback,
                agent_name=agent_name,
                round_id=round_id,
                stage=stage,
                state=state,
                delta=piece,
                is_final_chunk=False,
                max_chunks=max_chunks,
            )
            if not emitted:
                state.pending = ""
                state.input_closed = True
                break

        return state.has_emitted_content

    async def _finalize_agent_text_stream(
        self,
        event_callback: Optional[Callable[[str, Dict[str, Any]], Any]],
        agent_name: str,
        round_id: int,
        stage: str,
        state: _AgentTextStreamState,
        chunk_size: int = 200,
        max_chunks: int = 200,
    ) -> bool:
        if state.final_emitted:
            return state.has_emitted_content

        chunk_size = max(1, int(chunk_size or 1))
        max_chunks = int(max_chunks or 0)

        while state.pending:
            piece = state.pending[:chunk_size]
            state.pending = state.pending[chunk_size:]
            emitted = await self._emit_agent_stream_chunk(
                event_callback=event_callback,
                agent_name=agent_name,
                round_id=round_id,
                stage=stage,
                state=state,
                delta=piece,
                is_final_chunk=False,
                max_chunks=max_chunks,
            )
            if not emitted:
                state.pending = ""
                break

        if not state.has_emitted_content:
            state.final_emitted = True
            return False

        await self._emit_agent_stream_chunk(
            event_callback=event_callback,
            agent_name=agent_name,
            round_id=round_id,
            stage=stage,
            state=state,
            delta="",
            is_final_chunk=True,
            max_chunks=max_chunks,
        )
        state.input_closed = True
        state.final_emitted = True
        return True

    async def run(
        self,
        question: str,
        context: Optional[Dict[str, Any]] = None,
        chat_history: Optional[List[Dict[str, Any]]] = None,
        event_callback: Optional[Callable[[str, Dict[str, Any]], Any]] = None,
    ) -> str:
        q = str(question or "").strip()
        if not q:
            raise ValueError("question must not be empty.")

        runtime_context = _normalize_runtime_context(context)
        history = list(chat_history or [])

        await self._emit_agent(
            event_callback=event_callback,
            agent_name=AgentName.SYSTEM,
            agent_event_type=AgentEventType.START,
            payload={
                "stage": "run",
                "question": q,
                "context": runtime_context,
                "history_size": len(history),
                "max_rounds": self.max_rounds,
            },
        )

        participants = [
            self.orchestrator,
            self.db_context_agent,
            self.knowledge_agent,
            self.sql_builder_agent,
            self.sql_reviewer_agent,
        ]

        async def _tool_event_handler(payload: Dict[str, Any]) -> None:
            await self._emit_tool_progress(
                event_callback=event_callback,
                payload=payload,
            )

        self.tool_registry.set_tool_event_callback(_tool_event_handler)

        async def _run_pipeline() -> str:
            last_review: Dict[str, Any] = {}
            last_builder: Dict[str, Any] = {}
            self.round_traces = []

            for round_id in range(1, self.max_rounds + 1):
                await self._emit_agent(
                    event_callback=event_callback,
                    agent_name=AgentName.SYSTEM,
                    agent_event_type=AgentEventType.PROGRESS,
                    payload={
                        "stage": "round_start",
                        "round": round_id,
                        "max_rounds": self.max_rounds,
                    },
                )

                # Orchestrator planning step
                orchestration_input = {
                    "question": q,
                    "runtime_context": runtime_context,
                    "chat_history": history,
                    "round": round_id,
                    "max_rounds": self.max_rounds,
                    "previous_rounds": self.round_traces,
                    "last_review_report": last_review or None,
                    "instruction": (
                        "Produce a concise reasoning summary and a structured JSON plan "
                        "containing db_context_request and knowledge_request for this round."
                    ),
                }
                await self._emit_agent(
                    event_callback=event_callback,
                    agent_name=AgentName.ORCHESTRATOR,
                    agent_event_type=AgentEventType.START,
                    payload={
                        "round": round_id,
                        "stage": "planning",
                    },
                )
                await self._emit_agent(
                    event_callback=event_callback,
                    agent_name=AgentName.ORCHESTRATOR,
                    agent_event_type=AgentEventType.PROGRESS,
                    payload={
                        "round": round_id,
                        "stage": "planning_input_ready",
                        "question": q,
                    },
                )
                orchestrator_plan_msg, orchestrator_plan_streamed = await self._run_agent_with_tool_context(
                    agent=self.orchestrator,
                    message=Msg(
                        name="user",
                        role="user",
                        content=json.dumps(orchestration_input, ensure_ascii=False),
                    ),
                    agent_name=AgentName.ORCHESTRATOR,
                    round_id=round_id,
                    stage="planning",
                    event_callback=event_callback,
                )
                orchestrator_plan_text = _msg_to_text(orchestrator_plan_msg)
                if not orchestrator_plan_streamed:
                    await self._emit_agent_text_stream(
                        event_callback=event_callback,
                        agent_name=AgentName.ORCHESTRATOR,
                        round_id=round_id,
                        stage="planning_stream",
                        text=orchestrator_plan_text,
                    )
                plan = _extract_first_json(orchestrator_plan_text) or {}
                plan_summary = _extract_summary_text(orchestrator_plan_text)
                await self._emit_agent(
                    event_callback=event_callback,
                    agent_name=AgentName.ORCHESTRATOR,
                    agent_event_type=AgentEventType.END,
                    payload={
                        "round": round_id,
                        "stage": "planning",
                        "summary": plan_summary,
                        "plan": plan,
                    },
                )

                db_req = plan.get("db_context_request") or _default_db_context_request(
                    q,
                    context=runtime_context,
                )
                kb_req = plan.get("knowledge_request") or _default_knowledge_request(
                    q,
                    error_text=(last_builder.get("execution_result") or {}).get("error") if last_builder else None,
                    context=runtime_context,
                )
                if runtime_context.get("schema_name"):
                    constraints = db_req.setdefault("constraints", {})
                    constraints["schema_whitelist"] = [runtime_context["schema_name"]]
                db_req["runtime_context"] = runtime_context
                kb_req["runtime_context"] = runtime_context
                await self._emit_agent(
                    event_callback=event_callback,
                    agent_name=AgentName.DB_CONTEXT,
                    agent_event_type=AgentEventType.START,
                    payload={"round": round_id, "stage": "fanout"},
                )
                await self._emit_agent(
                    event_callback=event_callback,
                    agent_name=AgentName.KNOWLEDGE,
                    agent_event_type=AgentEventType.START,
                    payload={"round": round_id, "stage": "fanout"},
                )
                await self._emit_agent(
                    event_callback=event_callback,
                    agent_name=AgentName.DB_CONTEXT,
                    agent_event_type=AgentEventType.PROGRESS,
                    payload={
                        "round": round_id,
                        "stage": "fanout_request",
                        "request": db_req,
                    },
                )
                await self._emit_agent(
                    event_callback=event_callback,
                    agent_name=AgentName.KNOWLEDGE,
                    agent_event_type=AgentEventType.PROGRESS,
                    payload={
                        "round": round_id,
                        "stage": "fanout_request",
                        "request": kb_req,
                    },
                )

                # Fanout: DB context + knowledge in parallel
                db_task = asyncio.create_task(
                    self._run_agent_with_tool_context(
                        agent=self.db_context_agent,
                        message=Msg(name="orchestrator", role="user", content=json.dumps(db_req, ensure_ascii=False)),
                        agent_name=AgentName.DB_CONTEXT,
                        round_id=round_id,
                        stage="fanout",
                        event_callback=event_callback,
                    ),
                )
                kb_task = asyncio.create_task(
                    self._run_agent_with_tool_context(
                        agent=self.knowledge_agent,
                        message=Msg(name="orchestrator", role="user", content=json.dumps(kb_req, ensure_ascii=False)),
                        agent_name=AgentName.KNOWLEDGE,
                        round_id=round_id,
                        stage="fanout",
                        event_callback=event_callback,
                    ),
                )
                (db_msg, db_streamed), (kb_msg, kb_streamed) = await asyncio.gather(db_task, kb_task)
                db_text = _msg_to_text(db_msg)
                kb_text = _msg_to_text(kb_msg)
                if not db_streamed:
                    await self._emit_agent_text_stream(
                        event_callback=event_callback,
                        agent_name=AgentName.DB_CONTEXT,
                        round_id=round_id,
                        stage="fanout_stream",
                        text=db_text,
                    )
                if not kb_streamed:
                    await self._emit_agent_text_stream(
                        event_callback=event_callback,
                        agent_name=AgentName.KNOWLEDGE,
                        round_id=round_id,
                        stage="fanout_stream",
                        text=kb_text,
                    )
                db_bundle = _extract_first_json(db_text) or {}
                kb_bundle = _extract_first_json(kb_text) or {}
                db_summary = _extract_summary_text(db_text)
                kb_summary = _extract_summary_text(kb_text)
                await self._emit_agent(
                    event_callback=event_callback,
                    agent_name=AgentName.DB_CONTEXT,
                    agent_event_type=AgentEventType.END,
                    payload={
                        "round": round_id,
                        "stage": "fanout",
                        "summary": db_summary,
                        "bundle": db_bundle,
                    },
                )
                await self._emit_agent(
                    event_callback=event_callback,
                    agent_name=AgentName.KNOWLEDGE,
                    agent_event_type=AgentEventType.END,
                    payload={
                        "round": round_id,
                        "stage": "fanout",
                        "summary": kb_summary,
                        "bundle": kb_bundle,
                    },
                )

                # SQL builder: generate + execute
                builder_input = {
                    "question": q,
                    "runtime_context": runtime_context,
                    "chat_history": history,
                    "db_context_bundle": db_bundle,
                    "knowledge_bundle": kb_bundle,
                    "review_feedback": last_review or None,
                }
                await self._emit_agent(
                    event_callback=event_callback,
                    agent_name=AgentName.SQL_BUILDER,
                    agent_event_type=AgentEventType.START,
                    payload={
                        "round": round_id,
                        "stage": "build_and_execute",
                    },
                )
                await self._emit_agent(
                    event_callback=event_callback,
                    agent_name=AgentName.SQL_BUILDER,
                    agent_event_type=AgentEventType.PROGRESS,
                    payload={
                        "round": round_id,
                        "stage": "build_input_ready",
                    },
                )
                builder_msg, builder_streamed = await self._run_agent_with_tool_context(
                    agent=self.sql_builder_agent,
                    message=Msg(
                        name="orchestrator",
                        role="user",
                        content=json.dumps(builder_input, ensure_ascii=False),
                    ),
                    agent_name=AgentName.SQL_BUILDER,
                    round_id=round_id,
                    stage="build_and_execute",
                    event_callback=event_callback,
                )
                builder_text = _msg_to_text(builder_msg)
                if not builder_streamed:
                    await self._emit_agent_text_stream(
                        event_callback=event_callback,
                        agent_name=AgentName.SQL_BUILDER,
                        round_id=round_id,
                        stage="build_and_execute_stream",
                        text=builder_text,
                    )
                builder_out = _extract_first_json(builder_text) or {}
                builder_summary = _extract_summary_text(builder_text)
                execution_result = builder_out.get("execution_result", {}) or {}
                await self._emit_agent(
                    event_callback=event_callback,
                    agent_name=AgentName.SQL_BUILDER,
                    agent_event_type=AgentEventType.END,
                    payload={
                        "round": round_id,
                        "stage": "build_and_execute",
                        "summary": builder_summary,
                        "output": builder_out,
                    },
                )

                # SQL reviewer
                reviewer_input = {
                    "question": q,
                    "runtime_context": runtime_context,
                    "sql_draft": (builder_out.get("sql_draft") or {}).get("sql"),
                    "sql_bundle": builder_out,
                    "execution_result": execution_result,
                    "db_context_bundle": db_bundle,
                    "knowledge_bundle": kb_bundle,
                }
                await self._emit_agent(
                    event_callback=event_callback,
                    agent_name=AgentName.SQL_REVIEWER,
                    agent_event_type=AgentEventType.START,
                    payload={
                        "round": round_id,
                        "stage": "review",
                    },
                )
                await self._emit_agent(
                    event_callback=event_callback,
                    agent_name=AgentName.SQL_REVIEWER,
                    agent_event_type=AgentEventType.PROGRESS,
                    payload={
                        "round": round_id,
                        "stage": "review_input_ready",
                    },
                )
                reviewer_msg, reviewer_streamed = await self._run_agent_with_tool_context(
                    agent=self.sql_reviewer_agent,
                    message=Msg(
                        name="orchestrator",
                        role="user",
                        content=json.dumps(reviewer_input, ensure_ascii=False),
                    ),
                    agent_name=AgentName.SQL_REVIEWER,
                    round_id=round_id,
                    stage="review",
                    event_callback=event_callback,
                )
                reviewer_text = _msg_to_text(reviewer_msg)
                if not reviewer_streamed:
                    await self._emit_agent_text_stream(
                        event_callback=event_callback,
                        agent_name=AgentName.SQL_REVIEWER,
                        round_id=round_id,
                        stage="review_stream",
                        text=reviewer_text,
                    )
                review_out = _extract_first_json(reviewer_text) or {}
                reviewer_summary = _extract_summary_text(reviewer_text)
                await self._emit_agent(
                    event_callback=event_callback,
                    agent_name=AgentName.SQL_REVIEWER,
                    agent_event_type=AgentEventType.END,
                    payload={
                        "round": round_id,
                        "stage": "review",
                        "summary": reviewer_summary,
                        "review_report": review_out,
                    },
                )

                trace = {
                    "round": round_id,
                    "runtime_context": runtime_context,
                    "orchestrator_summary": plan_summary,
                    "orchestrator_plan": plan,
                    "db_context_request": db_req,
                    "knowledge_request": kb_req,
                    "db_context_summary": db_summary,
                    "db_context_bundle": db_bundle,
                    "knowledge_summary": kb_summary,
                    "knowledge_bundle": kb_bundle,
                    "sql_builder_summary": builder_summary,
                    "sql_builder_output": builder_out,
                    "sql_reviewer_summary": reviewer_summary,
                    "review_report": review_out,
                }
                self.round_traces.append(trace)
                last_review = review_out
                last_builder = builder_out

                verdict = str(review_out.get("verdict", "")).upper()
                status = str(execution_result.get("status", "")).upper()
                if verdict == "PASS" and status == "OK":
                    sql_draft = builder_out.get("sql_draft") or {}
                    builder_sql = _extract_sql_text(str(sql_draft.get("sql") or ""))
                    builder_params = sql_draft.get("params") if isinstance(sql_draft.get("params"), dict) else {}
                    builder_sql_full = _render_sql_with_params(builder_sql, builder_params)

                    # Final decision by orchestrator
                    await self._emit_agent(
                        event_callback=event_callback,
                        agent_name=AgentName.ORCHESTRATOR,
                        agent_event_type=AgentEventType.START,
                        payload={
                            "round": round_id,
                            "stage": "final_decision",
                        },
                    )
                    final_decision_msg, final_decision_streamed = await self._run_agent_with_tool_context(
                        agent=self.orchestrator,
                        message=Msg(
                            name="system",
                            role="user",
                            content=json.dumps(
                                {
                                    "question": q,
                                    "runtime_context": runtime_context,
                                    "chat_history": history,
                                    "validated_sql": builder_sql_full or builder_sql,
                                    "review_report": review_out,
                                    "instruction": (
                                        "Output final SQL string only. "
                                        "Do not use bind placeholders; return a complete executable SQL."
                                    ),
                                },
                                ensure_ascii=False,
                            ),
                        ),
                        agent_name=AgentName.ORCHESTRATOR,
                        round_id=round_id,
                        stage="final_decision",
                        event_callback=event_callback,
                    )
                    final_candidate = _extract_sql_text(_msg_to_text(final_decision_msg).strip())
                    if not final_decision_streamed:
                        await self._emit_agent_text_stream(
                            event_callback=event_callback,
                            agent_name=AgentName.ORCHESTRATOR,
                            round_id=round_id,
                            stage="final_decision_stream",
                            text=final_candidate,
                        )
                    if not _looks_like_sql(final_candidate):
                        final_candidate = builder_sql
                    final_text = _render_sql_with_params(final_candidate, builder_params)
                    if not final_text:
                        final_text = builder_sql_full or builder_sql
                    await self._emit_agent(
                        event_callback=event_callback,
                        agent_name=AgentName.ORCHESTRATOR,
                        agent_event_type=AgentEventType.END,
                        payload={
                            "round": round_id,
                            "stage": "final_decision",
                            "has_result": bool(final_text),
                        },
                    )
                    if final_text:
                        await self._emit_agent(
                            event_callback=event_callback,
                            agent_name=AgentName.SYSTEM,
                            agent_event_type=AgentEventType.END,
                            payload={
                                "stage": "run",
                                "result": final_text,
                                "source": "orchestrator",
                            },
                        )
                        return final_text
                    # fallback to builder SQL
                    fallback_sql = builder_sql_full or builder_sql
                    await self._emit_agent(
                        event_callback=event_callback,
                        agent_name=AgentName.SYSTEM,
                        agent_event_type=AgentEventType.END,
                        payload={
                            "stage": "run",
                            "result": fallback_sql,
                            "source": "builder_fallback",
                        },
                    )
                    return fallback_sql

            # max rounds exhausted -> ask minimal clarifying question
            await self._emit_agent(
                event_callback=event_callback,
                agent_name=AgentName.ORCHESTRATOR,
                agent_event_type=AgentEventType.START,
                payload={"stage": "clarification"},
            )
            clarify_msg, clarifying_streamed = await self._run_agent_with_tool_context(
                agent=self.orchestrator,
                message=Msg(
                    name="system",
                    role="user",
                    content=json.dumps(
                        {
                            "question": q,
                            "runtime_context": runtime_context,
                            "chat_history": history,
                            "round_traces": self.round_traces,
                            "instruction": "Max rounds reached. Ask minimal clarifying question(s), no SQL.",
                        },
                        ensure_ascii=False,
                    ),
                ),
                agent_name=AgentName.ORCHESTRATOR,
                round_id=self.max_rounds,
                stage="clarification",
                event_callback=event_callback,
            )
            clarifying_text = _msg_to_text(clarify_msg).strip()
            if not clarifying_streamed:
                await self._emit_agent_text_stream(
                    event_callback=event_callback,
                    agent_name=AgentName.ORCHESTRATOR,
                    round_id=self.max_rounds,
                    stage="clarification_stream",
                    text=clarifying_text,
                )
            await self._emit_agent(
                event_callback=event_callback,
                agent_name=AgentName.ORCHESTRATOR,
                agent_event_type=AgentEventType.END,
                payload={
                    "stage": "clarification",
                    "result": clarifying_text,
                },
            )
            await self._emit_agent(
                event_callback=event_callback,
                agent_name=AgentName.SYSTEM,
                agent_event_type=AgentEventType.END,
                payload={
                    "stage": "run",
                    "result": clarifying_text,
                    "source": "clarification",
                },
            )
            return clarifying_text

        hub = MsgHub(
            participants=participants,
            enable_auto_broadcast=False,
            name="spatial_text2sql_hub",
        )
        try:
            if hasattr(hub, "__aenter__") and hasattr(hub, "__aexit__"):
                async with hub:
                    return await _run_pipeline()
            if hasattr(hub, "__enter__") and hasattr(hub, "__exit__"):
                with hub:
                    return await _run_pipeline()
            return await _run_pipeline()
        finally:
            self.tool_registry.set_tool_event_callback(None)

    def get_round_traces(self) -> List[Dict[str, Any]]:
        return list(self.round_traces)

    def close(self) -> None:
        self.tool_registry.close()
