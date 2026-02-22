from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, Optional

from sqlalchemy import CheckConstraint, Column, DateTime, ForeignKey, Integer, String, Text, text
from sqlalchemy.orm import relationship, validates

from .base import Base


def _utc_now() -> datetime:
    return datetime.utcnow()


SUPPORTED_CHAT_ROLES = {"user", "assistant"}
SUPPORTED_FEEDBACK_VALUES = {"like", "dislike"}


class ChatSession(Base):
    __tablename__ = "chat_sessions"

    id = Column(Integer, primary_key=True, autoincrement=True)
    user_id = Column(Integer, ForeignKey("users.id", ondelete="CASCADE"), nullable=False, index=True)
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

    messages = relationship(
        "ChatHistory",
        back_populates="chat_session",
        cascade="all, delete-orphan",
        order_by="ChatHistory.id.asc()",
    )

    def to_dict(self) -> Dict[str, Any]:
        return {
            "id": self.id,
            "user_id": self.user_id,
            "chat_id": self.id,
            "insert_time": self.insert_time.isoformat() if self.insert_time else None,
            "update_time": self.update_time.isoformat() if self.update_time else None,
        }


class ChatHistory(Base):
    __tablename__ = "chat_history"
    __table_args__ = (
        CheckConstraint("role IN ('user', 'assistant')", name="ck_chat_history_role"),
        CheckConstraint(
            "feedback IS NULL OR feedback IN ('like', 'dislike')",
            name="ck_chat_history_feedback",
        ),
    )

    id = Column(Integer, primary_key=True, autoincrement=True)
    chat_id = Column(Integer, ForeignKey("chat_sessions.id", ondelete="CASCADE"), nullable=False, index=True)
    request_id = Column(Integer, nullable=True, index=True)
    role = Column(String(16), nullable=False)
    agent_name = Column(String(64), nullable=True, index=True)
    content = Column(Text, nullable=False)
    context_json = Column(Text, nullable=True, default=None)
    feedback = Column(String(16), nullable=True, default=None)
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

    chat_session = relationship("ChatSession", back_populates="messages")

    @validates("role")
    def _validate_role(self, key, value: str) -> str:
        role = str(value or "").strip().lower()
        if role not in SUPPORTED_CHAT_ROLES:
            raise ValueError("role must be 'user' or 'assistant'.")
        return role

    @validates("content")
    def _validate_content(self, key, value: str) -> str:
        text_value = str(value or "").strip()
        if not text_value:
            raise ValueError("content must not be empty.")
        return text_value

    @validates("feedback")
    def _validate_feedback(self, key, value: Optional[str]) -> Optional[str]:
        if value is None:
            return None
        raw = str(value).strip().lower()
        if raw == "":
            return None
        if raw not in SUPPORTED_FEEDBACK_VALUES:
            raise ValueError("feedback must be one of: like, dislike, null")
        return raw

    @validates("agent_name")
    def _validate_agent_name(self, key, value: Optional[str]) -> Optional[str]:
        if value is None:
            return None
        raw = str(value).strip()
        return raw or None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "id": self.id,
            "chat_id": self.chat_id,
            "request_id": self.request_id,
            "role": self.role,
            "agent_name": self.agent_name,
            "content": self.content,
            "context_json": self.context_json,
            "feedback": self.feedback,
            "insert_time": self.insert_time.isoformat() if self.insert_time else None,
            "update_time": self.update_time.isoformat() if self.update_time else None,
        }
