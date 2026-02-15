from __future__ import annotations

from typing import Optional

from pydantic import BaseModel, Field, root_validator, validator


class UserCreateRequest(BaseModel):
    username: str = Field(..., min_length=1, max_length=128)
    password: str = Field(..., min_length=1, max_length=256)
    role: str = Field(default="user", min_length=1, max_length=16)

    @validator("role")
    def _validate_role(cls, value: str) -> str:
        role = str(value or "").strip().lower()
        if role not in {"user", "admin"}:
            raise ValueError("role must be user or admin")
        return role


class UserUpdateRequest(BaseModel):
    username: Optional[str] = Field(default=None, min_length=1, max_length=128)
    password: Optional[str] = Field(default=None, min_length=1, max_length=256)
    role: Optional[str] = Field(default=None, min_length=1, max_length=16)

    @validator("role")
    def _validate_role(cls, value: Optional[str]) -> Optional[str]:
        if value is None:
            return None
        role = str(value or "").strip().lower()
        if role not in {"user", "admin"}:
            raise ValueError("role must be user or admin")
        return role

    @root_validator(skip_on_failure=True)
    def _validate_any_field(cls, values):
        if (
            values.get("username") is None
            and values.get("password") is None
            and values.get("role") is None
        ):
            raise ValueError("At least one of username, password or role must be provided.")
        return values
