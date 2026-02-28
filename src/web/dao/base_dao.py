from __future__ import annotations

from contextlib import contextmanager
from typing import Optional

from sqlalchemy.orm import Session

from ..entity.model import get_db_session


class BaseDAO:
    def __init__(self, session: Optional[Session] = None) -> None:
        self._external_session = session

    @contextmanager
    def session_scope(self):
        if self._external_session is not None:
            yield self._external_session
            return

        session = get_db_session()
        try:
            yield session
            session.commit()
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()
