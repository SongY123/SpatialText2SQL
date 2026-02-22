from __future__ import annotations

from datetime import datetime
from typing import List, Optional

from sqlalchemy import case, func, select

from .base_dao import BaseDAO
from ..entity.model import ChatSession, DatabaseLink, SqlExecutionLog, User


class SqlExecutionLogDAO(BaseDAO):
    def insert_log(
        self,
        user_id: int,
        database_id: int,
        execute_status: str,
        execution_time_ms: int,
        row_count: int = 0,
        sql_text: Optional[str] = None,
        chat_id: Optional[int] = None,
    ) -> SqlExecutionLog:
        with self.session_scope() as session:
            user = session.get(User, int(user_id))
            if user is None:
                raise ValueError(f"user not found: user_id={user_id}")

            db_link = session.get(DatabaseLink, int(database_id))
            if db_link is None:
                raise ValueError(f"database link not found: link_id={database_id}")
            if int(db_link.user_id) != int(user_id):
                raise ValueError("database link does not belong to current user.")

            chat_fk = None
            if chat_id is not None:
                chat_session = session.get(ChatSession, int(chat_id))
                if chat_session is None:
                    raise ValueError(f"chat session not found: chat_id={chat_id}")
                if int(chat_session.user_id) != int(user_id):
                    raise ValueError("chat session does not belong to current user.")
                chat_fk = int(chat_id)

            row = SqlExecutionLog(
                user_id=int(user_id),
                chat_id=chat_fk,
                database_id=int(database_id),
                execute_status=execute_status,
                sql_text=(str(sql_text).strip() if sql_text is not None else None),
                execution_time_ms=int(execution_time_ms),
                row_count=int(row_count),
            )
            session.add(row)
            session.flush()
            session.refresh(row)
            return row

    def list_logs(
        self,
        user_id: Optional[int] = None,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
        query: Optional[str] = None,
        min_latency: int = 0,
        status: Optional[str] = None,
    ) -> List[dict]:
        with self.session_scope() as session:
            stmt = select(SqlExecutionLog).order_by(SqlExecutionLog.insert_time.desc(), SqlExecutionLog.id.desc())

            if user_id is not None:
                stmt = stmt.where(SqlExecutionLog.user_id == int(user_id))
            if start_time is not None:
                stmt = stmt.where(SqlExecutionLog.insert_time >= start_time)
            if end_time is not None:
                stmt = stmt.where(SqlExecutionLog.insert_time <= end_time)

            pattern = str(query or "")
            if pattern != "":
                stmt = stmt.where(SqlExecutionLog.sql_text.like(pattern))

            latency_threshold = int(min_latency or 0)
            if latency_threshold > 0:
                stmt = stmt.where(SqlExecutionLog.execution_time_ms >= latency_threshold)

            normalized_status = str(status or "").strip().lower()
            if normalized_status:
                stmt = stmt.where(SqlExecutionLog.execute_status == normalized_status)

            rows = list(session.execute(stmt).scalars().all())
            return [row.to_dict() for row in rows]

    def get_execution_summary_stats(self) -> dict:
        with self.session_scope() as session:
            stmt = select(
                func.count(SqlExecutionLog.id),
                func.sum(case((SqlExecutionLog.execute_status == "success", 1), else_=0)),
                func.sum(SqlExecutionLog.execution_time_ms),
            )
            total_count, success_count, total_latency_ms = session.execute(stmt).one()

            total = int(total_count or 0)
            success = int(success_count or 0)
            latency_sum = int(total_latency_ms or 0)

            if total <= 0:
                return {
                    "sql_execution_count": 0,
                    "sql_execution_success_count": 0,
                    "success_rate": 0.0,
                    "avg_sql_latency": 0.0,
                }

            return {
                "sql_execution_count": total,
                "sql_execution_success_count": success,
                "success_rate": float(success) / float(total),
                "avg_sql_latency": float(latency_sum) / float(total),
            }
