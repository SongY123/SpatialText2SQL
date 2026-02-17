from __future__ import annotations

from datetime import datetime, timezone


class SSEEventType:
    START = "start"
    PROGRESS = "progress"
    ERROR = "error"
    END = "end"


class AgentEventType:
    START = "agent_start"
    PROGRESS = "agent_progress"
    END = "agent_end"


class AgentName:
    SYSTEM = "system"
    ORCHESTRATOR = "orchestrator"
    DB_CONTEXT = "db_context"
    KNOWLEDGE = "knowledge"
    SQL_BUILDER = "sql_builder"
    SQL_REVIEWER = "sql_reviewer"


def event_timestamp() -> str:
    return datetime.now(timezone.utc).isoformat()
