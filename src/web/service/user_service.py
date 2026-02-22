from __future__ import annotations

from typing import Dict, List, Optional

from ..dao import DatabaseLinkDAO, UserDAO
from .session_service import SessionService, get_global_session_service


class UserService:
    def __init__(
        self,
        user_dao: Optional[UserDAO] = None,
        database_link_dao: Optional[DatabaseLinkDAO] = None,
        session_service: Optional[SessionService] = None,
    ) -> None:
        self.user_dao = user_dao or UserDAO()
        self.database_link_dao = database_link_dao or DatabaseLinkDAO()
        self.session_service = session_service or get_global_session_service()

    @staticmethod
    def _normalize_status_filter(status: Optional[str]) -> Optional[str]:
        raw = str(status or "").strip().lower()
        if raw == "":
            return None
        if raw not in {"active", "disabled"}:
            raise ValueError("status must be active or disabled")
        return raw

    @staticmethod
    def _to_public_user_payload(user_payload: Dict) -> Dict:
        data = dict(user_payload or {})
        data.pop("password", None)
        return data

    def insert_user(self, username: str, password: str, role: str = "user", status: str = "active") -> Dict:
        user = self.user_dao.insert_user(username=username, password=password, role=role, status=status)
        return user.to_dict()

    def update_user(
        self,
        user_id: int,
        username: Optional[str] = None,
        password: Optional[str] = None,
        role: Optional[str] = None,
        status: Optional[str] = None,
    ) -> Dict:
        user = self.user_dao.update_user(
            user_id=user_id,
            username=username,
            password=password,
            role=role,
            status=status,
        )
        if user is None:
            raise ValueError(f"user not found: user_id={user_id}")

        links = self.database_link_dao.list_database_links(user_id=user.id)
        self.session_service.refresh_user_context(
            user_payload=user.to_dict(),
            database_links=[x.to_dict() for x in links],
        )
        return user.to_dict()

    def delete_user(self, user_id: int) -> bool:
        deleted = self.user_dao.delete_user(user_id=user_id)
        if deleted:
            self.session_service.delete_sessions_for_user(user_id=user_id)
        return deleted

    def list_users(self, status: Optional[str] = None) -> List[Dict]:
        normalized_status = self._normalize_status_filter(status)
        rows = self.user_dao.list_users(status=normalized_status)
        return [self._to_public_user_payload(x.to_dict()) for x in rows]

    def login(self, username: str, password: str) -> Dict:
        user = self.user_dao.get_user_by_username(username=username)
        if user is None:
            raise ValueError("invalid username or password")
        if user.password != str(password):
            raise ValueError("invalid username or password")
        if str(getattr(user, "status", "active") or "active").strip().lower() != "active":
            raise ValueError("user is disabled")

        touched_user = self.user_dao.touch_last_login(user_id=int(user.id))
        if touched_user is not None:
            user = touched_user

        db_links = self.database_link_dao.list_database_links(user_id=user.id)
        user_payload = user.to_dict()
        role = str(user_payload.get("role") or "user")
        links_payload = [x.to_dict() for x in db_links]
        session_id = self.session_service.create_session(
            {
                "user": user_payload,
                "database_links": links_payload,
            }
        )
        return {
            "session_id": session_id,
            "role": role,
            "user": {
                "id": user_payload.get("id"),
                "username": user_payload.get("username"),
                "role": role,
                "status": user_payload.get("status"),
                "last_login": user_payload.get("last_login"),
            },
            "database_links": links_payload,
        }

    def logout(self, session_id: str) -> bool:
        return self.session_service.delete_session(session_id=session_id)

    def get_session(self, session_id: str) -> Optional[Dict]:
        raw = self.session_service.get_session(session_id=session_id)
        if raw is None:
            return None

        user = raw.get("user") or {}
        return {
            "session_id": raw.get("session_id"),
            "login_time": raw.get("login_time"),
            "last_update_time": raw.get("last_update_time"),
            "user": {
                "id": user.get("id"),
                "username": user.get("username"),
                "role": user.get("role"),
                "status": user.get("status"),
                "last_login": user.get("last_login"),
            },
        }
