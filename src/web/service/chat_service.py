from __future__ import annotations

import threading
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple


def _now_iso() -> str:
    return datetime.utcnow().isoformat()


def _to_str_list(items: Any) -> List[str]:
    if not isinstance(items, list):
        return []
    out: List[str] = []
    for item in items:
        text = str(item or "").strip()
        if not text:
            continue
        out.append(text)
    return out


class ChatService:
    """In-memory chat context storage keyed by (user_id, chat_id)."""

    def __init__(self) -> None:
        self._chats: Dict[Tuple[int, str], Dict[str, Any]] = {}
        self._lock = threading.RLock()

    def _key(self, user_id: int, chat_id: str) -> Tuple[int, str]:
        uid = int(user_id)
        cid = str(chat_id or "").strip()
        if not cid:
            raise ValueError("chat_id must not be empty.")
        return uid, cid

    def get_chat(self, user_id: int, chat_id: str) -> Optional[Dict[str, Any]]:
        key = self._key(user_id=user_id, chat_id=chat_id)
        with self._lock:
            row = self._chats.get(key)
            return dict(row) if row is not None else None

    def upsert_context(self, user_id: int, chat_id: str, context: Optional[Dict[str, Any]]) -> Dict[str, Any]:
        key = self._key(user_id=user_id, chat_id=chat_id)
        with self._lock:
            row = self._chats.get(key)
            if row is None:
                row = {
                    "user_id": int(user_id),
                    "chat_id": key[1],
                    "context": {
                        "database_id": None,
                        "schema_name": "",
                        "table_list": [],
                        "view_list": [],
                    },
                    "messages": [],
                    "create_time": _now_iso(),
                    "last_update_time": _now_iso(),
                }
                self._chats[key] = row

            if context is not None:
                ctx = row["context"]
                if "database_id" in context and context.get("database_id") is not None:
                    ctx["database_id"] = int(context["database_id"])
                if "schema_name" in context and context.get("schema_name") is not None:
                    ctx["schema_name"] = str(context["schema_name"]).strip()
                if "table_list" in context:
                    ctx["table_list"] = _to_str_list(context.get("table_list"))
                if "view_list" in context:
                    ctx["view_list"] = _to_str_list(context.get("view_list"))

            row["last_update_time"] = _now_iso()
            return dict(row)

    def append_message(self, user_id: int, chat_id: str, role: str, content: str) -> None:
        key = self._key(user_id=user_id, chat_id=chat_id)
        msg_role = str(role or "").strip().lower()
        if msg_role not in {"user", "assistant"}:
            raise ValueError("role must be 'user' or 'assistant'.")
        text = str(content or "").strip()
        if not text:
            return

        with self._lock:
            row = self._chats.get(key)
            if row is None:
                row = self.upsert_context(user_id=user_id, chat_id=chat_id, context=None)
                row = self._chats[key]
            row["messages"].append(
                {
                    "role": msg_role,
                    "content": text,
                    "time": _now_iso(),
                },
            )
            row["last_update_time"] = _now_iso()

    def get_history(self, user_id: int, chat_id: str, limit: int = 20) -> List[Dict[str, str]]:
        key = self._key(user_id=user_id, chat_id=chat_id)
        with self._lock:
            row = self._chats.get(key)
            if row is None:
                return []
            messages = row.get("messages") or []
            n = max(1, int(limit))
            out = messages[-n:]
            return [
                {
                    "role": str(m.get("role", "")),
                    "content": str(m.get("content", "")),
                }
                for m in out
            ]


_GLOBAL_CHAT_SERVICE: Optional[ChatService] = None


def get_global_chat_service() -> ChatService:
    global _GLOBAL_CHAT_SERVICE
    if _GLOBAL_CHAT_SERVICE is None:
        _GLOBAL_CHAT_SERVICE = ChatService()
    return _GLOBAL_CHAT_SERVICE

