from __future__ import annotations

import asyncio
import json
import re
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional

from agentscope.formatter import FormatterBase
from agentscope.message import Msg
from agentscope.model import ChatModelBase
from agentscope.pipeline import MsgHub

from src.agent.db_context_agent import DBContextAgent
from src.agent.knowledge_agent import KnowledgeAgent
from src.agent.orchestrator_agent import OrchestratorAgent
from src.agent.sql_builder_agent import SQLBuilderAgent
from src.agent.sql_reviewer_agent import SQLReviewerAgent
from src.agent.tools import SpatialText2SQLToolRegistry, build_role_toolkits


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
    try:
        obj = json.loads(s)
        if isinstance(obj, dict):
            return obj
    except Exception:
        pass

    m = re.search(r"\{.*\}", s, flags=re.DOTALL)
    if not m:
        return None
    try:
        obj = json.loads(m.group(0))
        if isinstance(obj, dict):
            return obj
    except Exception:
        return None
    return None


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
    signals = []
    for token in ["within", "distance", "near", "intersects", "buffer", "area", "length", "dwithin"]:
        if token in q.lower():
            signals.append(token)

    keywords = q.split()[:10]
    keywords.extend(ctx.get("table_list", [])[:6])
    keywords.extend(ctx.get("view_list", [])[:6])

    schema_name = str(ctx.get("schema_name") or "").strip()
    schema_whitelist = [schema_name] if schema_name else ["public"]
    return {
        "question": q,
        "focus": {
            "keywords": keywords,
            "expected_outputs": [],
            "likely_filters": [],
            "spatial_signals": signals,
        },
        "constraints": {
            "schema_whitelist": schema_whitelist,
            "max_tables": 8,
            "max_columns_per_table": 12,
        },
        "runtime_context": ctx,
    }


def _default_knowledge_request(
    question: str,
    error_text: Optional[str] = None,
    context: Optional[Dict[str, Any]] = None,
) -> Dict:
    ctx = _normalize_runtime_context(context)
    return {
        "question": question,
        "focus": {
            "postgis_topics": ["ST_DWithin", "ST_Transform", "SRID", "geography vs geometry", "distance units"],
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
            maybe = event_callback(event, payload)
            if asyncio.iscoroutine(maybe):
                await maybe
        except Exception:
            return

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

        await self._emit(
            event_callback,
            "run_start",
            {
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

        last_review: Dict[str, Any] = {}
        last_builder: Dict[str, Any] = {}
        self.round_traces = []

        with MsgHub(participants=participants, enable_auto_broadcast=False, name="spatial_text2sql_hub"):
            for round_id in range(1, self.max_rounds + 1):
                await self._emit(
                    event_callback,
                    "round_start",
                    {"round": round_id, "max_rounds": self.max_rounds},
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
                    "instruction": "Produce db_context_request and knowledge_request JSON for this round.",
                }
                orchestrator_plan_msg = await self.orchestrator(
                    Msg(
                        name="user",
                        role="user",
                        content=json.dumps(orchestration_input, ensure_ascii=False),
                    ),
                )
                orchestrator_plan_text = _msg_to_text(orchestrator_plan_msg)
                plan = _extract_first_json(orchestrator_plan_text) or {}
                await self._emit(
                    event_callback,
                    "orchestrator_plan",
                    {"round": round_id, "plan": plan},
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
                await self._emit(
                    event_callback,
                    "fanout_input",
                    {
                        "round": round_id,
                        "db_context_request": db_req,
                        "knowledge_request": kb_req,
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
                db_bundle = _extract_first_json(_msg_to_text(db_msg)) or {}
                kb_bundle = _extract_first_json(_msg_to_text(kb_msg)) or {}
                await self._emit(
                    event_callback,
                    "fanout_output",
                    {
                        "round": round_id,
                        "db_context_bundle": db_bundle,
                        "knowledge_bundle": kb_bundle,
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
                builder_msg = await self.sql_builder_agent(
                    Msg(
                        name="orchestrator",
                        role="user",
                        content=json.dumps(builder_input, ensure_ascii=False),
                    ),
                )
                builder_out = _extract_first_json(_msg_to_text(builder_msg)) or {}
                execution_result = builder_out.get("execution_result", {}) or {}
                await self._emit(
                    event_callback,
                    "sql_builder_output",
                    {"round": round_id, "output": builder_out},
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
                reviewer_msg = await self.sql_reviewer_agent(
                    Msg(
                        name="orchestrator",
                        role="user",
                        content=json.dumps(reviewer_input, ensure_ascii=False),
                    ),
                )
                review_out = _extract_first_json(_msg_to_text(reviewer_msg)) or {}
                await self._emit(
                    event_callback,
                    "review_output",
                    {"round": round_id, "review_report": review_out},
                )

                trace = {
                    "round": round_id,
                    "runtime_context": runtime_context,
                    "orchestrator_plan": plan,
                    "db_context_request": db_req,
                    "knowledge_request": kb_req,
                    "db_context_bundle": db_bundle,
                    "knowledge_bundle": kb_bundle,
                    "sql_builder_output": builder_out,
                    "review_report": review_out,
                }
                self.round_traces.append(trace)
                last_review = review_out
                last_builder = builder_out

                verdict = str(review_out.get("verdict", "")).upper()
                status = str(execution_result.get("status", "")).upper()
                if verdict == "PASS" and status == "OK":
                    # Final decision by orchestrator
                    final_decision_msg = await self.orchestrator(
                        Msg(
                            name="system",
                            role="user",
                            content=json.dumps(
                                {
                                    "question": q,
                                    "runtime_context": runtime_context,
                                    "chat_history": history,
                                    "validated_sql": (builder_out.get("sql_draft") or {}).get("sql"),
                                    "review_report": review_out,
                                    "instruction": "Output final SQL string only.",
                                },
                                ensure_ascii=False,
                            ),
                        ),
                    )
                    final_text = _msg_to_text(final_decision_msg).strip()
                    if final_text:
                        await self._emit(
                            event_callback,
                            "run_end",
                            {"result": final_text, "source": "orchestrator"},
                        )
                        return final_text
                    # fallback to builder SQL
                    fallback_sql = str((builder_out.get("sql_draft") or {}).get("sql", "")).strip()
                    await self._emit(
                        event_callback,
                        "run_end",
                        {"result": fallback_sql, "source": "builder_fallback"},
                    )
                    return fallback_sql

            # max rounds exhausted -> ask minimal clarifying question
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
            await self._emit(
                event_callback,
                "run_end",
                {"result": clarifying_text, "source": "clarification"},
            )
            return clarifying_text

    def get_round_traces(self) -> List[Dict[str, Any]]:
        return list(self.round_traces)

    def close(self) -> None:
        self.tool_registry.close()
