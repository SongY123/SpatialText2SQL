from __future__ import annotations

from datetime import datetime
from typing import List, Optional

from sqlalchemy import select

from .base_dao import BaseDAO
from ..entity.model import DatabaseLink, User


class DatabaseLinkDAO(BaseDAO):
    def insert_database_link(
        self,
        user_id: int,
        name: str,
        db_type: str,
        url: str,
        schema: List[str],
        db_username: Optional[str] = None,
        db_password: Optional[str] = None,
    ) -> DatabaseLink:
        with self.session_scope() as session:
            user = session.get(User, int(user_id))
            if user is None:
                raise ValueError(f"user not found: user_id={user_id}")

            db_link = DatabaseLink(
                user_id=int(user_id),
                name=name,
                type=db_type,
                url=url,
                db_username=db_username,
                db_password=db_password,
                schema=schema,
            )
            session.add(db_link)
            session.flush()
            session.refresh(db_link)
            return db_link

    def get_database_link_by_id(self, link_id: int) -> Optional[DatabaseLink]:
        with self.session_scope() as session:
            return session.get(DatabaseLink, int(link_id))

    def list_database_links(self, user_id: Optional[int] = None) -> List[DatabaseLink]:
        with self.session_scope() as session:
            stmt = select(DatabaseLink)
            if user_id is not None:
                stmt = stmt.where(DatabaseLink.user_id == int(user_id))
            stmt = stmt.order_by(DatabaseLink.id.asc())
            return list(session.execute(stmt).scalars().all())

    def update_database_link(
        self,
        link_id: int,
        name: Optional[str] = None,
        db_type: Optional[str] = None,
        url: Optional[str] = None,
        schema: Optional[List[str]] = None,
        db_username: Optional[str] = None,
        db_password: Optional[str] = None,
    ) -> Optional[DatabaseLink]:
        with self.session_scope() as session:
            db_link = session.get(DatabaseLink, int(link_id))
            if db_link is None:
                return None

            if name is not None:
                db_link.name = name
            if db_type is not None:
                db_link.type = db_type
            if url is not None:
                db_link.url = url
            if schema is not None:
                db_link.schema = schema
            if db_username is not None:
                db_link.db_username = db_username
            if db_password is not None:
                db_link.db_password = db_password

            db_link.update_time = datetime.utcnow()
            session.add(db_link)
            session.flush()
            session.refresh(db_link)
            return db_link

    def delete_database_link(self, link_id: int) -> bool:
        with self.session_scope() as session:
            db_link = session.get(DatabaseLink, int(link_id))
            if db_link is None:
                return False
            session.delete(db_link)
            session.flush()
            return True
