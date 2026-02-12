from __future__ import annotations

from typing import Dict, List, Optional

from ..dao import DatabaseLinkDAO, UserDAO
from .session_service import SessionService, get_global_session_service


class DatabaseService:
    def __init__(
        self,
        database_link_dao: Optional[DatabaseLinkDAO] = None,
        user_dao: Optional[UserDAO] = None,
        session_service: Optional[SessionService] = None,
    ) -> None:
        self.database_link_dao = database_link_dao or DatabaseLinkDAO()
        self.user_dao = user_dao or UserDAO()
        self.session_service = session_service or get_global_session_service()

    def insert_database(
        self,
        user_id: int,
        name: str,
        db_type: str,
        url: str,
        schema: Optional[List[str]] = None,
        db_username: Optional[str] = None,
        db_password: Optional[str] = None,
    ) -> Dict:
        db_link = self.database_link_dao.insert_database_link(
            user_id=user_id,
            name=name,
            db_type=db_type,
            url=url,
            schema=schema or [],
            db_username=db_username,
            db_password=db_password,
        )
        self._refresh_user_sessions(user_id=int(user_id))
        return db_link.to_dict()

    def update_database(
        self,
        link_id: int,
        name: Optional[str] = None,
        db_type: Optional[str] = None,
        url: Optional[str] = None,
        schema: Optional[List[str]] = None,
        db_username: Optional[str] = None,
        db_password: Optional[str] = None,
    ) -> Dict:
        db_link = self.database_link_dao.update_database_link(
            link_id=link_id,
            name=name,
            db_type=db_type,
            url=url,
            schema=schema,
            db_username=db_username,
            db_password=db_password,
        )
        if db_link is None:
            raise ValueError(f"database link not found: link_id={link_id}")

        self._refresh_user_sessions(user_id=int(db_link.user_id))
        return db_link.to_dict()

    def delete_database(self, link_id: int) -> bool:
        db_link = self.database_link_dao.get_database_link_by_id(link_id=link_id)
        if db_link is None:
            return False

        deleted = self.database_link_dao.delete_database_link(link_id=link_id)
        if deleted:
            self._refresh_user_sessions(user_id=int(db_link.user_id))
        return deleted

    def list_databases(self, user_id: Optional[int] = None) -> List[Dict]:
        rows = self.database_link_dao.list_database_links(user_id=user_id)
        return [x.to_dict() for x in rows]

    def _refresh_user_sessions(self, user_id: int) -> None:
        user = self.user_dao.get_user_by_id(user_id=user_id)
        if user is None:
            return
        links = self.database_link_dao.list_database_links(user_id=user_id)
        self.session_service.refresh_user_context(
            user_payload=user.to_dict(),
            database_links=[x.to_dict() for x in links],
        )
