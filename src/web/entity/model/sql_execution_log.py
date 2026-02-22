from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, Optional

from sqlalchemy import CheckConstraint, Column, DateTime, ForeignKey, Integer, String, Text, text
from sqlalchemy.orm import validates

from .base import Base


def _utc_now() -> datetime:
    return datetime.utcnow()


SUPPORTED_SQL_EXEC_STATUSES = {"success", "failure"}


class SqlExecutionLog(Base):
    __tablename__ = "sql_execution_logs"
    __table_args__ = (
        CheckConstraint(
            "execute_status IN ('success', 'failure')",
            name="ck_sql_execution_logs_status",
        ),
        CheckConstraint(
            "execution_time_ms >= 0",
            name="ck_sql_execution_logs_execution_time_ms",
        ),
        CheckConstraint(
            "row_count >= 0",
            name="ck_sql_execution_logs_row_count",
        ),
    )

    id = Column(Integer, primary_key=True, autoincrement=True)
    user_id = Column(Integer, ForeignKey("users.id", ondelete="CASCADE"), nullable=False, index=True)
    chat_id = Column(Integer, ForeignKey("chat_sessions.id", ondelete="SET NULL"), nullable=True, index=True)
    database_id = Column(Integer, ForeignKey("database_links.id", ondelete="CASCADE"), nullable=False, index=True)
    execute_status = Column(String(16), nullable=False, index=True)
    sql_text = Column(Text, nullable=True, default=None)
    execution_time_ms = Column(Integer, nullable=False, default=0, server_default=text("0"))
    row_count = Column(Integer, nullable=False, default=0, server_default=text("0"))
    insert_time = Column(
        DateTime,
        nullable=False,
        default=_utc_now,
        server_default=text("CURRENT_TIMESTAMP"),
    )
    update_time = Column(
        DateTime,
        nullable=False,
        default=_utc_now,
        onupdate=_utc_now,
        server_default=text("CURRENT_TIMESTAMP"),
    )

    @validates("execute_status")
    def _validate_execute_status(self, key, value: str) -> str:
        raw = str(value or "").strip().lower()
        if raw not in SUPPORTED_SQL_EXEC_STATUSES:
            raise ValueError("execute_status must be one of: success, failure")
        return raw

    @validates("execution_time_ms")
    def _validate_execution_time_ms(self, key, value: Any) -> int:
        ms = int(value or 0)
        if ms < 0:
            raise ValueError("execution_time_ms must be >= 0")
        return ms

    @validates("row_count")
    def _validate_row_count(self, key, value: Any) -> int:
        cnt = int(value or 0)
        if cnt < 0:
            raise ValueError("row_count must be >= 0")
        return cnt

    @validates("sql_text")
    def _validate_sql_text(self, key, value: Optional[str]) -> Optional[str]:
        if value is None:
            return None
        text_value = str(value).strip()
        return text_value or None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "id": int(self.id),
            "user_id": int(self.user_id),
            "chat_id": int(self.chat_id) if self.chat_id is not None else None,
            "database_id": int(self.database_id),
            "execute_status": str(self.execute_status),
            "sql_text": self.sql_text,
            "execution_time_ms": int(self.execution_time_ms),
            "row_count": int(self.row_count),
            "insert_time": self.insert_time.isoformat() if self.insert_time else None,
            "update_time": self.update_time.isoformat() if self.update_time else None,
        }
