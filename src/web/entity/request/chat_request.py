from __future__ import annotations

from typing import List, Optional

from pydantic import BaseModel, Field, field_validator, model_validator


class ChatContextRequest(BaseModel):
    database_id: Optional[int] = Field(default=None, ge=1)
    schema_name: Optional[str] = Field(default=None, min_length=1, max_length=128)
    table_list: List[str] = Field(default_factory=list)
    view_list: List[str] = Field(default_factory=list)


class ChatSSERequest(BaseModel):
    chat_id: int = Field(..., ge=1)
    query: str = Field(..., min_length=1, max_length=20000)
    context: Optional[ChatContextRequest] = None

    @model_validator(mode="after")
    def _validate_context_if_present(self):
        ctx = self.context
        if ctx is None:
            return self

        schema_name = str(getattr(ctx, "schema_name", "") or "").strip()
        database_id = getattr(ctx, "database_id", None)
        if (database_id is None) ^ (schema_name == ""):
            raise ValueError("context.database_id and context.schema_name must be provided together.")
        return self


class ChatFeedbackRequest(BaseModel):
    chat_id: int = Field(..., ge=1)
    message_id: int = Field(..., ge=1)
    feedback: Optional[str] = Field(
        default=None,
        description="like / dislike / none(null means clear)",
    )

    @field_validator("feedback", mode="before")
    @classmethod
    def _normalize_feedback(cls, value):
        if value is None:
            return None
        raw = str(value).strip().lower()
        if raw in {"", "none", "null"}:
            return None
        if raw not in {"like", "dislike"}:
            raise ValueError("feedback must be one of: like, dislike, none")
        return raw
