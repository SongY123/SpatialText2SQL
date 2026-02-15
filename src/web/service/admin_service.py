from __future__ import annotations

from typing import Dict, Optional

from ..dao import DatabaseLinkDAO, UserDAO
from .session_service import SessionService, get_global_session_service


class AdminService:
    def __init__(
        self,
        user_dao: Optional[UserDAO] = None,
        database_link_dao: Optional[DatabaseLinkDAO] = None,
        session_service: Optional[SessionService] = None,
    ) -> None:
        self.user_dao = user_dao or UserDAO()
        self.database_link_dao = database_link_dao or DatabaseLinkDAO()
        self.session_service = session_service or get_global_session_service()

    def get_overview_stats(self) -> Dict[str, int]:
        return {
            "user_count": int(self.user_dao.count_users()),
            "session_count": int(self.session_service.count_sessions()),
            "database_count": int(self.database_link_dao.count_database_links()),
        }

