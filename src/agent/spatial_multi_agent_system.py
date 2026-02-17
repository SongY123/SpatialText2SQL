from __future__ import annotations

import asyncio
import json
import re
from datetime import date, datetime, time
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional

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
                orchestrator_plan_msg = await self.orchestrator(
                    Msg(
                        name="user",
                        role="user",
                        content=json.dumps(orchestration_input, ensure_ascii=False),
                    ),
                )
                orchestrator_plan_text = _msg_to_text(orchestrator_plan_msg)
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
                    self.db_context_agent(
                        Msg(name="orchestrator", role="user", content=json.dumps(db_req, ensure_ascii=False)),
                    ),
                )
                kb_task = asyncio.create_task(
                    self.knowledge_agent(
                        Msg(name="orchestrator", role="user", content=json.dumps(kb_req, ensure_ascii=False)),
                    ),
                )
                db_msg, kb_msg = await asyncio.gather(db_task, kb_task)
                db_text = _msg_to_text(db_msg)
                kb_text = _msg_to_text(kb_msg)
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
                builder_msg = await self.sql_builder_agent(
                    Msg(
                        name="orchestrator",
                        role="user",
                        content=json.dumps(builder_input, ensure_ascii=False),
                    ),
                )
                builder_text = _msg_to_text(builder_msg)
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
                reviewer_msg = await self.sql_reviewer_agent(
                    Msg(
                        name="orchestrator",
                        role="user",
                        content=json.dumps(reviewer_input, ensure_ascii=False),
                    ),
                )
                reviewer_text = _msg_to_text(reviewer_msg)
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
                    final_decision_msg = await self.orchestrator(
                        Msg(
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
                    )
                    final_candidate = _extract_sql_text(_msg_to_text(final_decision_msg).strip())
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
            clarify_msg = await self.orchestrator(
                Msg(
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
            )
            clarifying_text = _msg_to_text(clarify_msg).strip()
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
        if hasattr(hub, "__aenter__") and hasattr(hub, "__aexit__"):
            async with hub:
                return await _run_pipeline()
        if hasattr(hub, "__enter__") and hasattr(hub, "__exit__"):
            with hub:
                return await _run_pipeline()
        return await _run_pipeline()

    def get_round_traces(self) -> List[Dict[str, Any]]:
        return list(self.round_traces)

    def close(self) -> None:
        self.tool_registry.close()
