from __future__ import annotations

import asyncio
import json
import re
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

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


def _default_db_context_request(question: str) -> Dict:
    q = str(question or "")
    signals = []
    for token in ["within", "distance", "near", "intersects", "buffer", "area", "length", "dwithin"]:
        if token in q.lower():
            signals.append(token)
    return {
        "question": q,
        "focus": {
            "keywords": q.split()[:10],
            "expected_outputs": [],
            "likely_filters": [],
            "spatial_signals": signals,
        },
        "constraints": {
            "schema_whitelist": ["public"],
            "max_tables": 8,
            "max_columns_per_table": 12,
        },
    }


def _default_knowledge_request(question: str, error_text: Optional[str] = None) -> Dict:
    return {
        "question": question,
        "focus": {
            "postgis_topics": ["ST_DWithin", "ST_Transform", "SRID", "geography vs geometry", "distance units"],
            "need_websearch": bool(error_text),
            "error_text": error_text,
        },
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

    async def run(self, question: str) -> str:
        q = str(question or "").strip()
        if not q:
            raise ValueError("question must not be empty.")

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
                # Orchestrator planning step
                orchestration_input = {
                    "question": q,
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

                db_req = plan.get("db_context_request") or _default_db_context_request(q)
                kb_req = plan.get("knowledge_request") or _default_knowledge_request(
                    q,
                    error_text=(last_builder.get("execution_result") or {}).get("error") if last_builder else None,
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

                # SQL builder: generate + execute
                builder_input = {
                    "question": q,
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

                # SQL reviewer
                reviewer_input = {
                    "question": q,
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

                trace = {
                    "round": round_id,
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
                        return final_text
                    # fallback to builder SQL
                    return str((builder_out.get("sql_draft") or {}).get("sql", "")).strip()

            # max rounds exhausted -> ask minimal clarifying question
            clarify_msg = await self.orchestrator(
                Msg(
                    name="system",
                    role="user",
                    content=json.dumps(
                        {
                            "question": q,
                            "round_traces": self.round_traces,
                            "instruction": "Max rounds reached. Ask minimal clarifying question(s), no SQL.",
                        },
                        ensure_ascii=False,
                    ),
                ),
            )
            return _msg_to_text(clarify_msg).strip()

    def get_round_traces(self) -> List[Dict[str, Any]]:
        return list(self.round_traces)

    def close(self) -> None:
        self.tool_registry.close()
