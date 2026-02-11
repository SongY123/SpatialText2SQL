from __future__ import annotations

from datetime import datetime
from typing import List, Optional

from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import selectinload

from .base_dao import BaseDAO
from ..entity.model import User


class UserDAO(BaseDAO):
    def insert_user(self, username: str, password: str) -> User:
        with self.session_scope() as session:
            user = User(username=username, password=password)
            session.add(user)
            try:
                session.flush()
            except IntegrityError as exc:
                raise ValueError(f"User insert failed: {exc}") from exc
            session.refresh(user)
            return user

    def get_user_by_id(self, user_id: int) -> Optional[User]:
        with self.session_scope() as session:
            return session.get(User, int(user_id))

    def get_user_by_username(self, username: str) -> Optional[User]:
        with self.session_scope() as session:
            stmt = select(User).where(User.username == str(username).strip())
            return session.execute(stmt).scalars().first()

    def list_users(self) -> List[User]:
        with self.session_scope() as session:
            stmt = select(User).order_by(User.id.asc())
            return list(session.execute(stmt).scalars().all())

    def get_user_with_database_links(self, user_id: int) -> Optional[User]:
        with self.session_scope() as session:
            stmt = (
                select(User)
                .options(selectinload(User.db_links))
                .where(User.id == int(user_id))
            )
            return session.execute(stmt).scalars().first()

    def update_user(
        self,
        user_id: int,
        username: Optional[str] = None,
        password: Optional[str] = None,
    ) -> Optional[User]:
        with self.session_scope() as session:
            user = session.get(User, int(user_id))
            if user is None:
                return None

            if username is not None:
                user.username = username
            if password is not None:
                user.password = password

            user.update_time = datetime.utcnow()
            session.add(user)
            try:
                session.flush()
            except IntegrityError as exc:
                raise ValueError(f"User update failed: {exc}") from exc
            session.refresh(user)
            return user

    def delete_user(self, user_id: int) -> bool:
        with self.session_scope() as session:
            user = session.get(User, int(user_id))
            if user is None:
                return False
            session.delete(user)
            session.flush()
            return True
