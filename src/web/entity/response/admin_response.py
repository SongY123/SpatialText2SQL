from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict


@dataclass
class AdminOverviewResponse:
    user_count: int
    database_count: int
    session_count: int
    active_session_count: int
    message_count: int
    query_count: int
    success_rate: float
    avg_sql_latency: float

    @classmethod
    def from_dict(cls, payload: Dict[str, Any]) -> "AdminOverviewResponse":
        data = payload or {}
        return cls(
            user_count=int(data.get("user_count", 0)),
            database_count=int(data.get("database_count", 0)),
            session_count=int(data.get("session_count", 0)),
            active_session_count=int(data.get("active_session_count", 0)),
            message_count=int(data.get("message_count", 0)),
            query_count=int(data.get("query_count", 0)),
            success_rate=float(data.get("success_rate", 0.0)),
            avg_sql_latency=float(data.get("avg_sql_latency", 0.0)),
        )

    def to_dict(self) -> Dict[str, Any]:
        return {
            "user_count": int(self.user_count),
            "database_count": int(self.database_count),
            "session_count": int(self.session_count),
            "active_session_count": int(self.active_session_count),
            "message_count": int(self.message_count),
            "query_count": int(self.query_count),
            "success_rate": float(self.success_rate),
            "avg_sql_latency": float(self.avg_sql_latency),
        }
