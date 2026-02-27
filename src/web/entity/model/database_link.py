from __future__ import annotations

import json
from datetime import datetime
from typing import Dict, List

from sqlalchemy import JSON, Column, DateTime, ForeignKey, Integer, String, text
from sqlalchemy.orm import relationship, validates

from .base import Base


SUPPORTED_DB_TYPES = {"Spatialite", "Postgis", "Sedona", "MySQL"}


def _utc_now() -> datetime:
    return datetime.utcnow()


def _normalize_db_type(raw_value: str) -> str:
    value = str(raw_value or "").strip().lower()
    if value == "spatialite":
        return "Spatialite"
    if value == "postgis":
        return "Postgis"
    if value == "sedona":
        return "Sedona"
    if value == "mysql":
        return "MySQL"
    raise ValueError("type must be one of: Spatialite, Postgis, Sedona, MySQL")


class DatabaseLink(Base):
    __tablename__ = "database_links"

    id = Column(Integer, primary_key=True, autoincrement=True)
    user_id = Column(Integer, ForeignKey("users.id", ondelete="CASCADE"), nullable=False, index=True)
    name = Column(String(256), nullable=False)
    type = Column(String(16), nullable=False)
    url = Column(String(1024), nullable=False)
    db_username = Column(String(256), nullable=True)
    db_password = Column(String(256), nullable=True)  # plain text by requirement
    schema = Column(JSON, nullable=False, default=list)
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

    user = relationship("User", back_populates="db_links")

    @validates("type")
    def _validate_type(self, key, value: str) -> str:
        return _normalize_db_type(value)

    @validates("name")
    def _validate_name(self, key, value: str) -> str:
        name = str(value or "").strip()
        if not name:
            raise ValueError("name must not be empty.")
        return name

    @validates("url")
    def _validate_url(self, key, value: str) -> str:
        jdbc = str(value or "").strip()
        if not jdbc:
            raise ValueError("url must not be empty.")
        if not jdbc.startswith("jdbc:"):
            raise ValueError("url must be a JDBC url, e.g. jdbc:postgresql://...")
        return jdbc

    @validates("db_username")
    def _validate_db_username(self, key, value):
        if value is None:
            return None
        text_value = str(value).strip()
        return text_value or None

    @validates("db_password")
    def _validate_db_password(self, key, value):
        if value is None:
            return None
        text_value = str(value)
        return text_value if text_value != "" else None

    @validates("schema")
    def _validate_schema(self, key, value) -> List[str]:
        if value is None:
            return []
        if isinstance(value, str):
            raw_text = value.strip()
            if not raw_text:
                return []
            try:
                value = json.loads(raw_text)
            except json.JSONDecodeError:
                value = [x.strip() for x in raw_text.split(",") if x.strip()]

        if not isinstance(value, list):
            raise ValueError("schema must be a list of schema names.")

        cleaned: List[str] = []
        seen = set()
        for item in value:
            schema_name = str(item or "").strip()
            if not schema_name:
                continue
            if schema_name in seen:
                continue
            cleaned.append(schema_name)
            seen.add(schema_name)
        return cleaned

    def to_dict(self) -> Dict:
        try:
            db_type = _normalize_db_type(self.type)
        except Exception:
            db_type = str(self.type or "")
        return {
            "id": self.id,
            "user_id": self.user_id,
            "name": self.name,
            "type": db_type,
            "url": self.url,
            "db_username": self.db_username,
            "db_password": self.db_password,
            "schema": list(self.schema or []),
            "insert_time": self.insert_time.isoformat() if self.insert_time else None,
            "update_time": self.update_time.isoformat() if self.update_time else None,
        }
