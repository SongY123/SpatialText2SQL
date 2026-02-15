from __future__ import annotations

from typing import List, Optional

from pydantic import BaseModel, Field, root_validator


class ChatContextRequest(BaseModel):
    database_id: Optional[int] = Field(default=None, ge=1)
    schema_name: Optional[str] = Field(default=None, min_length=1, max_length=128)
    table_list: List[str] = Field(default_factory=list)
    view_list: List[str] = Field(default_factory=list)


class ChatSSERequest(BaseModel):
    chat_id: str = Field(..., min_length=1, max_length=128)
    query: str = Field(..., min_length=1, max_length=20000)
    context: Optional[ChatContextRequest] = None

    @root_validator(skip_on_failure=True)
    def _validate_context_if_present(cls, values):
        ctx = values.get("context")
        if ctx is None:
            return values

        schema_name = str(getattr(ctx, "schema_name", "") or "").strip()
        database_id = getattr(ctx, "database_id", None)
        if (database_id is None) ^ (schema_name == ""):
            raise ValueError("context.database_id and context.schema_name must be provided together.")
        return values

