from __future__ import annotations

import threading
import uuid
from datetime import datetime
from typing import Any, Dict, List, Optional


def _now_iso() -> str:
    return datetime.utcnow().isoformat()


class SessionService:
    """In-memory session storage for logged-in users."""

    def __init__(self) -> None:
        self._sessions: Dict[str, Dict[str, Any]] = {}
        self._lock = threading.RLock()

    def create_session(self, payload: Dict[str, Any]) -> str:
        session_id = uuid.uuid4().hex
        with self._lock:
            self._sessions[session_id] = {
                **payload,
                "session_id": session_id,
                "login_time": _now_iso(),
                "last_update_time": _now_iso(),
            }
        return session_id

    def get_session(self, session_id: str) -> Optional[Dict[str, Any]]:
        sid = str(session_id or "").strip()
        if not sid:
            return None
        with self._lock:
            data = self._sessions.get(sid)
            return dict(data) if data is not None else None

    def update_session(self, session_id: str, patch: Dict[str, Any]) -> bool:
        sid = str(session_id or "").strip()
        if not sid:
            return False
        with self._lock:
            if sid not in self._sessions:
                return False
            self._sessions[sid].update(patch or {})
            self._sessions[sid]["last_update_time"] = _now_iso()
            return True

    def delete_session(self, session_id: str) -> bool:
        sid = str(session_id or "").strip()
        if not sid:
            return False
        with self._lock:
            return self._sessions.pop(sid, None) is not None

    def delete_sessions_for_user(self, user_id: int) -> int:
        uid = int(user_id)
        with self._lock:
            to_delete: List[str] = []
            for sid, payload in self._sessions.items():
                user = payload.get("user") or {}
                if int(user.get("id", -1)) == uid:
                    to_delete.append(sid)

            for sid in to_delete:
                self._sessions.pop(sid, None)
            return len(to_delete)

    def refresh_user_context(self, user_payload: Dict[str, Any], database_links: List[Dict[str, Any]]) -> int:
        uid = int((user_payload or {}).get("id", -1))
        if uid < 0:
            return 0

        updated = 0
        with self._lock:
            for sid, payload in self._sessions.items():
                user = payload.get("user") or {}
                if int(user.get("id", -1)) != uid:
                    continue
                payload["user"] = dict(user_payload)
                payload["database_links"] = list(database_links or [])
                payload["last_update_time"] = _now_iso()
                self._sessions[sid] = payload
                updated += 1
        return updated


_GLOBAL_SESSION_SERVICE: Optional[SessionService] = None


def get_global_session_service() -> SessionService:
    global _GLOBAL_SESSION_SERVICE
    if _GLOBAL_SESSION_SERVICE is None:
        _GLOBAL_SESSION_SERVICE = SessionService()
    return _GLOBAL_SESSION_SERVICE
