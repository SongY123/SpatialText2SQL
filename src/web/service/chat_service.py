from __future__ import annotations

from typing import Any, Dict, List, Optional

from ..dao import ChatDAO


class ChatService:
    """Persistent chat context/history service backed by sqlite via SQLAlchemy."""

    def __init__(self, chat_dao: Optional[ChatDAO] = None) -> None:
        self.chat_dao = chat_dao or ChatDAO()

    @staticmethod
    def _normalize_context_patch(context: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
        if context is None:
            return None
        if not isinstance(context, dict):
            raise ValueError("context must be an object.")
        return dict(context)

    @staticmethod
    def _normalize_feedback(feedback: Optional[str]) -> Optional[str]:
        if feedback is None:
            return None
        raw = str(feedback).strip().lower()
        if raw in {"", "none", "null"}:
            return None
        if raw not in {"like", "dislike"}:
            raise ValueError("feedback must be one of: like, dislike, none")
        return raw

    def create_chat(self, user_id: int) -> Dict[str, Any]:
        session_row = self.chat_dao.create_chat_session(user_id=int(user_id))
        return {
            "chat_id": int(session_row["chat_id"]),
            "user_id": int(session_row["user_id"]),
            "create_time": session_row.get("create_time"),
            "last_update_time": session_row.get("last_update_time"),
        }

    def get_chat(self, user_id: int, chat_id: int) -> Optional[Dict[str, Any]]:
        session_row = self.chat_dao.get_chat_session(user_id=int(user_id), chat_id=chat_id)
        if session_row is None:
            return None
        messages = self.chat_dao.list_chat_messages(user_id=int(user_id), chat_id=chat_id, limit=1000)
        return {
            "user_id": int(session_row["user_id"]),
            "chat_id": int(session_row["chat_id"]),
            "context": self.chat_dao.get_latest_chat_context(user_id=int(user_id), chat_id=chat_id),
            "messages": messages,
            "create_time": session_row.get("create_time"),
            "last_update_time": session_row.get("last_update_time"),
        }

    def chat_exists(self, user_id: int, chat_id: int) -> bool:
        return self.chat_dao.get_chat_session(user_id=int(user_id), chat_id=chat_id) is not None

    def resolve_context(self, user_id: int, chat_id: int, context: Optional[Dict[str, Any]]) -> Dict[str, Any]:
        session_row = self.chat_dao.get_chat_session(user_id=int(user_id), chat_id=chat_id)
        if session_row is None:
            raise ValueError(f"chat session not found: chat_id={chat_id}")

        current_context = self.chat_dao.get_latest_chat_context(user_id=int(user_id), chat_id=chat_id)
        patch = self._normalize_context_patch(context)
        if patch is not None:
            normalized_patch = self.chat_dao._normalize_context(patch)
            if "database_id" in patch and patch.get("database_id") is not None:
                current_context["database_id"] = normalized_patch["database_id"]
            if "schema_name" in patch and patch.get("schema_name") is not None:
                current_context["schema_name"] = normalized_patch["schema_name"]
            if "table_list" in patch:
                current_context["table_list"] = normalized_patch["table_list"]
            if "view_list" in patch:
                current_context["view_list"] = normalized_patch["view_list"]
        return {
            "user_id": int(user_id),
            "chat_id": int(session_row["chat_id"]),
            "context": current_context,
            "messages": [],
            "create_time": session_row.get("create_time"),
            "last_update_time": session_row.get("last_update_time"),
        }

    def upsert_context(self, user_id: int, chat_id: int, context: Optional[Dict[str, Any]]) -> Dict[str, Any]:
        # Backward-compatible method name; context is no longer stored on chat_sessions.
        return self.resolve_context(user_id=user_id, chat_id=chat_id, context=context)

    def append_message(
        self,
        user_id: int,
        chat_id: int,
        role: str,
        content: str,
        context: Optional[Dict[str, Any]] = None,
    ) -> Optional[Dict[str, Any]]:
        return self.append_message_with_meta(
            user_id=user_id,
            chat_id=chat_id,
            role=role,
            content=content,
            request_id=None,
            agent_name=None,
            context=context,
        )

    def append_message_with_meta(
        self,
        user_id: int,
        chat_id: int,
        role: str,
        content: str,
        request_id: Optional[int] = None,
        agent_name: Optional[str] = None,
        context: Optional[Dict[str, Any]] = None,
    ) -> Optional[Dict[str, Any]]:
        msg_role = str(role or "").strip().lower()
        if msg_role not in {"user", "assistant"}:
            raise ValueError("role must be 'user' or 'assistant'.")
        text = str(content or "").strip()
        if not text:
            return None
        return self.chat_dao.insert_chat_message(
            user_id=int(user_id),
            chat_id=chat_id,
            role=msg_role,
            content=text,
            request_id=(int(request_id) if request_id is not None else None),
            agent_name=agent_name,
            context=context,
        )

    def get_history(self, user_id: int, chat_id: int, limit: int = 20) -> List[Dict[str, str]]:
        rows = self.chat_dao.list_chat_messages(
            user_id=int(user_id),
            chat_id=chat_id,
            limit=limit,
            include_agent_messages=False,
        )
        return [
            {
                "role": str(m.get("role", "")),
                "content": str(m.get("content", "")),
            }
            for m in rows
        ]

    def get_history_records(self, user_id: int, chat_id: int, limit: int = 100) -> List[Dict[str, Any]]:
        return self.chat_dao.list_chat_messages(user_id=int(user_id), chat_id=chat_id, limit=limit)

    def set_message_feedback(
        self,
        user_id: int,
        chat_id: int,
        message_id: int,
        feedback: Optional[str],
    ) -> Dict[str, Any]:
        feedback_value = self._normalize_feedback(feedback)
        row = self.chat_dao.update_chat_message_feedback(
            user_id=int(user_id),
            chat_id=chat_id,
            message_id=int(message_id),
            feedback=feedback_value,
        )
        if row is None:
            raise ValueError(f"chat history record not found: message_id={message_id}")
        return row


_GLOBAL_CHAT_SERVICE: Optional[ChatService] = None


def get_global_chat_service() -> ChatService:
    global _GLOBAL_CHAT_SERVICE
    if _GLOBAL_CHAT_SERVICE is None:
        _GLOBAL_CHAT_SERVICE = ChatService()
    return _GLOBAL_CHAT_SERVICE
