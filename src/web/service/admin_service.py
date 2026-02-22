from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, Optional

from ..dao import ChatDAO, DatabaseLinkDAO, SqlExecutionLogDAO, UserDAO
from .session_service import SessionService, get_global_session_service


class AdminService:
    def __init__(
        self,
        user_dao: Optional[UserDAO] = None,
        database_link_dao: Optional[DatabaseLinkDAO] = None,
        chat_dao: Optional[ChatDAO] = None,
        sql_execution_log_dao: Optional[SqlExecutionLogDAO] = None,
        session_service: Optional[SessionService] = None,
    ) -> None:
        self.user_dao = user_dao or UserDAO()
        self.database_link_dao = database_link_dao or DatabaseLinkDAO()
        self.chat_dao = chat_dao or ChatDAO()
        self.sql_execution_log_dao = sql_execution_log_dao or SqlExecutionLogDAO()
        self.session_service = session_service or get_global_session_service()

    def get_overview_stats(self) -> Dict[str, Any]:
        sql_stats = self.sql_execution_log_dao.get_execution_summary_stats()
        return {
            "user_count": int(self.user_dao.count_users()),
            "database_count": int(self.database_link_dao.count_database_links()),
            "session_count": int(self.chat_dao.count_chat_sessions()),
            "active_session_count": int(self.session_service.count_sessions()),
            "message_count": int(self.chat_dao.count_chat_messages()),
            "query_count": int(self.chat_dao.count_chat_queries()),
            "success_rate": float(sql_stats.get("success_rate", 0.0)),
            "avg_sql_latency": float(sql_stats.get("avg_sql_latency", 0.0)),
        }

    def list_sql_execution_logs(
        self,
        user_id: Optional[int] = None,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
        query: Optional[str] = None,
        min_latency: int = 0,
        status: Optional[str] = None,
    ) -> Dict[str, Any]:
        if start_time is not None and end_time is not None and start_time > end_time:
            raise ValueError("start_time must be less than or equal to end_time")

        normalized_status = str(status or "").strip().lower()
        if normalized_status and normalized_status not in {"success", "failure"}:
            raise ValueError("status must be one of: success, failure")

        rows = self.sql_execution_log_dao.list_logs(
            user_id=(int(user_id) if user_id is not None else None),
            start_time=start_time,
            end_time=end_time,
            query=query,
            min_latency=int(min_latency or 0),
            status=(normalized_status or None),
        )
        return {
            "total_count": len(rows),
            "items": rows,
        }
