from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Optional


@dataclass
class DatabasePublicResponse:
    id: int
    user_id: int
    name: str
    type: str
    url: str
    schema: List[str]
    insert_time: Optional[str] = None
    update_time: Optional[str] = None

    @classmethod
    def from_dict(cls, payload: Dict[str, Any]) -> "DatabasePublicResponse":
        data = payload or {}
        return cls(
            id=int(data.get("id", 0)),
            user_id=int(data.get("user_id", 0)),
            name=str(data.get("name", "")),
            type=str(data.get("type", "")),
            url=str(data.get("url", "")),
            schema=[str(x) for x in (data.get("schema") or [])],
            insert_time=data.get("insert_time"),
            update_time=data.get("update_time"),
        )

    def to_dict(self) -> Dict[str, Any]:
        return {
            "id": self.id,
            "user_id": self.user_id,
            "name": self.name,
            "type": self.type,
            "url": self.url,
            "schema": list(self.schema),
            "insert_time": self.insert_time,
            "update_time": self.update_time,
        }

