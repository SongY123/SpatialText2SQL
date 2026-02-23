from __future__ import annotations

import json
import uuid
from datetime import datetime
from typing import Any, Dict, List, Optional

from sqlalchemy import func, select, text

from .base_dao import BaseDAO
from ..entity.model import ChatHistory, ChatSession, User


class ChatDAO(BaseDAO):
    @staticmethod
    def _get_table_columns(session, table_name: str) -> set[str]:
        rows = session.execute(text(f"PRAGMA table_info({table_name})")).fetchall()
        return {str(r[1]) for r in rows}

    @staticmethod
    def _normalize_chat_session_id(chat_id: Any) -> int:
        try:
            session_id = int(chat_id)
        except (TypeError, ValueError):
            raise ValueError("chat_id must be a positive integer.")
        if session_id <= 0:
            raise ValueError("chat_id must be a positive integer.")
        return session_id

    @staticmethod
    def _default_context() -> Dict[str, Any]:
        return {
            "database_id": None,
            "schema_name": "",
            "table_list": [],
            "view_list": [],
            "geometry": None,
        }

    @staticmethod
    def _normalize_optional_geometry(value: Any) -> Any:
        if value is None:
            return None
        if isinstance(value, str):
            text = value.strip()
            return text or None
        if isinstance(value, (list, dict)):
            return value if len(value) > 0 else None
        return value

    @staticmethod
    def _normalize_context(raw: Optional[Dict[str, Any]]) -> Dict[str, Any]:
        base = ChatDAO._default_context()
        if not isinstance(raw, dict):
            return base

        if raw.get("database_id") is not None:
            base["database_id"] = int(raw.get("database_id"))
        if raw.get("schema_name") is not None:
            base["schema_name"] = str(raw.get("schema_name") or "").strip()

        for key in ("table_list", "view_list"):
            value = raw.get(key)
            if isinstance(value, list):
                out: List[str] = []
                for item in value:
                    text_value = str(item or "").strip()
                    if text_value:
                        out.append(text_value)
                base[key] = out
        if "geometry" in raw:
            base["geometry"] = ChatDAO._normalize_optional_geometry(raw.get("geometry"))
        return base

    @staticmethod
    def _parse_context_json(raw: Optional[str]) -> Dict[str, Any]:
        text_value = str(raw or "").strip()
        if not text_value:
            return ChatDAO._default_context()
        try:
            obj = json.loads(text_value)
        except Exception:
            return ChatDAO._default_context()
        return ChatDAO._normalize_context(obj if isinstance(obj, dict) else {})

    @staticmethod
    def _to_session_payload(chat_session: ChatSession) -> Dict[str, Any]:
        return {
            "id": int(chat_session.id),
            "user_id": int(chat_session.user_id),
            "chat_id": int(chat_session.id),
            "create_time": chat_session.insert_time.isoformat() if chat_session.insert_time else None,
            "last_update_time": chat_session.update_time.isoformat() if chat_session.update_time else None,
        }

    @staticmethod
    def _to_message_payload(message: ChatHistory) -> Dict[str, Any]:
        owner_user_id = None
        if getattr(message, "chat_session", None) is not None:
            try:
                owner_user_id = int(message.chat_session.user_id)
            except Exception:
                owner_user_id = None
        return {
            "id": int(message.id),
            "user_id": owner_user_id,
            "chat_id": int(message.chat_id),
            "request_id": int(message.request_id) if message.request_id is not None else None,
            "role": str(message.role),
            "agent_name": str(message.agent_name) if message.agent_name else None,
            "content": str(message.content),
            "context": ChatDAO._parse_context_json(message.context_json) if message.context_json else None,
            "feedback": message.feedback if message.feedback is not None else None,
            "time": message.insert_time.isoformat() if message.insert_time else None,
            "update_time": message.update_time.isoformat() if message.update_time else None,
        }

    def create_chat_session(self, user_id: int) -> Dict[str, Any]:
        with self.session_scope() as session:
            user = session.get(User, int(user_id))
            if user is None:
                raise ValueError(f"user not found: user_id={user_id}")

            dialect_name = ""
            if session.bind is not None and getattr(session.bind, "dialect", None) is not None:
                dialect_name = str(session.bind.dialect.name or "").lower()
            session_columns = self._get_table_columns(session, "chat_sessions") if dialect_name == "sqlite" else set()
            if "chat_id" in session_columns or "context_json" in session_columns:
                insert_cols = ["user_id"]
                params: Dict[str, Any] = {"user_id": int(user_id)}
                if "chat_id" in session_columns:
                    insert_cols.append("chat_id")
                    params["chat_id"] = f"legacy-{uuid.uuid4().hex}"
                if "context_json" in session_columns:
                    insert_cols.append("context_json")
                    params["context_json"] = json.dumps(self._default_context(), ensure_ascii=False)
                col_sql = ", ".join(insert_cols)
                val_sql = ", ".join(f":{c}" for c in insert_cols)
                session.execute(text(f"INSERT INTO chat_sessions ({col_sql}) VALUES ({val_sql})"), params)
                new_id = session.execute(text("SELECT last_insert_rowid()")).scalar()
                row = session.get(ChatSession, int(new_id))
                if row is None:
                    raise ValueError("failed to create chat session")
                return self._to_session_payload(row)

            row = ChatSession(user_id=int(user_id))
            session.add(row)
            session.flush()
            session.refresh(row)
            return self._to_session_payload(row)

    def get_chat_session(self, user_id: int, chat_id: Any) -> Optional[Dict[str, Any]]:
        with self.session_scope() as session:
            session_id = self._normalize_chat_session_id(chat_id)
            stmt = select(ChatSession).where(
                ChatSession.user_id == int(user_id),
                ChatSession.id == session_id,
            )
            row = session.execute(stmt).scalars().first()
            if row is None:
                return None
            return self._to_session_payload(row)

    def get_latest_chat_context(self, user_id: int, chat_id: Any) -> Dict[str, Any]:
        with self.session_scope() as session:
            session_id = self._normalize_chat_session_id(chat_id)
            chat_session = session.get(ChatSession, session_id)
            if chat_session is None or int(chat_session.user_id) != int(user_id):
                raise ValueError(f"chat session not found: chat_id={chat_id}")

            stmt = (
                select(ChatHistory)
                .where(
                    ChatHistory.chat_id == session_id,
                    ChatHistory.context_json.isnot(None),
                )
                .order_by(ChatHistory.id.desc())
                .limit(1)
            )
            row = session.execute(stmt).scalars().first()
            if row is None:
                return self._default_context()
            return self._parse_context_json(row.context_json)

    def insert_chat_message(
        self,
        user_id: int,
        chat_id: Any,
        role: str,
        content: str,
        request_id: Optional[int] = None,
        agent_name: Optional[str] = None,
        context: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        with self.session_scope() as session:
            session_id = self._normalize_chat_session_id(chat_id)
            chat_session = session.get(ChatSession, session_id)
            if chat_session is None or int(chat_session.user_id) != int(user_id):
                raise ValueError(f"chat session not found: chat_id={chat_id}")

            context_json: Optional[str] = None
            if context is not None:
                context_json = json.dumps(self._normalize_context(context), ensure_ascii=False)

            msg = ChatHistory(
                chat_id=session_id,
                request_id=int(request_id) if request_id is not None else None,
                role=role,
                agent_name=(str(agent_name).strip() if agent_name is not None else None),
                content=content,
                context_json=context_json,
            )
            session.add(msg)
            chat_session.update_time = datetime.utcnow()
            session.add(chat_session)
            session.flush()
            if msg.request_id is None and str(msg.role) == "user":
                # Use the user message row id as the request id for this turn.
                msg.request_id = int(msg.id)
                session.add(msg)
                session.flush()
            session.refresh(msg)
            return self._to_message_payload(msg)

    def list_chat_messages(
        self,
        user_id: int,
        chat_id: Any,
        limit: int = 20,
        include_agent_messages: bool = True,
    ) -> List[Dict[str, Any]]:
        with self.session_scope() as session:
            session_id = self._normalize_chat_session_id(chat_id)
            n = max(1, int(limit))
            chat_session = session.get(ChatSession, session_id)
            if chat_session is None or int(chat_session.user_id) != int(user_id):
                return []
            stmt = select(ChatHistory).where(
                ChatHistory.chat_id == session_id,
            )
            if not include_agent_messages:
                stmt = stmt.where(ChatHistory.agent_name.is_(None))
            stmt = stmt.order_by(ChatHistory.id.desc()).limit(n)
            rows = list(session.execute(stmt).scalars().all())
            rows.reverse()
            return [self._to_message_payload(row) for row in rows]

    def update_chat_message_feedback(
        self,
        user_id: int,
        chat_id: Any,
        message_id: int,
        feedback: Optional[str],
    ) -> Optional[Dict[str, Any]]:
        with self.session_scope() as session:
            session_id = self._normalize_chat_session_id(chat_id)
            chat_session = session.get(ChatSession, session_id)
            if chat_session is None or int(chat_session.user_id) != int(user_id):
                return None
            msg = session.get(ChatHistory, int(message_id))
            if msg is None:
                return None
            if int(msg.chat_id) != session_id:
                return None

            msg.feedback = feedback
            msg.update_time = datetime.utcnow()
            session.add(msg)
            session.flush()
            session.refresh(msg)
            return self._to_message_payload(msg)

    def count_chat_sessions(self) -> int:
        with self.session_scope() as session:
            stmt = select(func.count(ChatSession.id))
            value = session.execute(stmt).scalar()
            return int(value or 0)

    def count_chat_messages(self) -> int:
        with self.session_scope() as session:
            stmt = select(func.count(ChatHistory.id))
            value = session.execute(stmt).scalar()
            return int(value or 0)

    def count_chat_queries(self) -> int:
        with self.session_scope() as session:
            stmt = select(func.count(ChatHistory.id)).where(ChatHistory.role == "user")
            value = session.execute(stmt).scalar()
            return int(value or 0)
