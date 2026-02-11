from __future__ import annotations

from datetime import datetime
from typing import Dict

from sqlalchemy import Column, DateTime, Integer, String, text
from sqlalchemy.orm import relationship, validates

from .base import Base


def _utc_now() -> datetime:
    return datetime.utcnow()


class User(Base):
    __tablename__ = "users"

    id = Column(Integer, primary_key=True, autoincrement=True)
    username = Column(String(128), nullable=False, unique=True, index=True)
    password = Column(String(256), nullable=False)  # plain text by requirement
    insert_time = Column(
        DateTime,
        nullable=False,
        default=_utc_now,
        server_default=text("CURRENT_TIMESTAMP"),
    )
    update_time = Column(
        DateTime,
        nullable=False,
        default=_utc_now,
        onupdate=_utc_now,
        server_default=text("CURRENT_TIMESTAMP"),
    )

    db_links = relationship(
        "DatabaseLink",
        back_populates="user",
        cascade="all, delete-orphan",
    )

    @validates("username")
    def _validate_username(self, key, value: str) -> str:
        name = str(value or "").strip()
        if not name:
            raise ValueError("username must not be empty.")
        return name

    @validates("password")
    def _validate_password(self, key, value: str) -> str:
        raw = str(value or "")
        if not raw:
            raise ValueError("password must not be empty.")
        return raw

    def to_dict(self, include_db_links: bool = False) -> Dict:
        payload = {
            "id": self.id,
            "username": self.username,
            "password": self.password,
            "insert_time": self.insert_time.isoformat() if self.insert_time else None,
            "update_time": self.update_time.isoformat() if self.update_time else None,
        }
        if include_db_links:
            payload["db_links"] = [link.to_dict() for link in self.db_links]
        return payload

