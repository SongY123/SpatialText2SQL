from __future__ import annotations

from typing import Dict, Optional

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

    def insert_user(self, username: str, password: str) -> Dict:
        user = self.user_dao.insert_user(username=username, password=password)
        return user.to_dict()

    def update_user(
        self,
        user_id: int,
        username: Optional[str] = None,
        password: Optional[str] = None,
    ) -> Dict:
        user = self.user_dao.update_user(user_id=user_id, username=username, password=password)
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

    def login(self, username: str, password: str) -> Dict:
        user = self.user_dao.get_user_by_username(username=username)
        if user is None:
            raise ValueError("invalid username or password")
        if user.password != str(password):
            raise ValueError("invalid username or password")

        db_links = self.database_link_dao.list_database_links(user_id=user.id)
        user_payload = user.to_dict()
        links_payload = [x.to_dict() for x in db_links]
        session_id = self.session_service.create_session(
            {
                "user": user_payload,
                "database_links": links_payload,
            }
        )
        return {
            "session_id": session_id,
            "user": user_payload,
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
            },
        }
