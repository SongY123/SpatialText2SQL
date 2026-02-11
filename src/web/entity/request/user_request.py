from __future__ import annotations

from typing import Optional

from pydantic import BaseModel, Field, root_validator


class UserCreateRequest(BaseModel):
    username: str = Field(..., min_length=1, max_length=128)
    password: str = Field(..., min_length=1, max_length=256)


class UserUpdateRequest(BaseModel):
    username: Optional[str] = Field(default=None, min_length=1, max_length=128)
    password: Optional[str] = Field(default=None, min_length=1, max_length=256)

    @root_validator(skip_on_failure=True)
    def _validate_any_field(cls, values):
        if values.get("username") is None and values.get("password") is None:
            raise ValueError("At least one of username or password must be provided.")
        return values

