from __future__ import annotations

from typing import Optional

from pydantic import BaseModel, Field, field_validator, model_validator


class UserCreateRequest(BaseModel):
    username: str = Field(..., min_length=1, max_length=128)
    password: str = Field(..., min_length=1, max_length=256)
    role: str = Field(default="user", min_length=1, max_length=16)
    status: str = Field(default="active", min_length=1, max_length=16)

    @field_validator("role")
    @classmethod
    def _validate_role(cls, value: str) -> str:
        role = str(value or "").strip().lower()
        if role not in {"user", "admin"}:
            raise ValueError("role must be user or admin")
        return role

    @field_validator("status")
    @classmethod
    def _validate_status(cls, value: str) -> str:
        status = str(value or "").strip().lower()
        if status not in {"active", "disabled"}:
            raise ValueError("status must be active or disabled")
        return status


class UserUpdateRequest(BaseModel):
    username: Optional[str] = Field(default=None, min_length=1, max_length=128)
    password: Optional[str] = Field(default=None, min_length=1, max_length=256)
    role: Optional[str] = Field(default=None, min_length=1, max_length=16)
    status: Optional[str] = Field(default=None, min_length=1, max_length=16)

    @field_validator("role")
    @classmethod
    def _validate_role(cls, value: Optional[str]) -> Optional[str]:
        if value is None:
            return None
        role = str(value or "").strip().lower()
        if role not in {"user", "admin"}:
            raise ValueError("role must be user or admin")
        return role

    @field_validator("status")
    @classmethod
    def _validate_status(cls, value: Optional[str]) -> Optional[str]:
        if value is None:
            return None
        status = str(value or "").strip().lower()
        if status not in {"active", "disabled"}:
            raise ValueError("status must be active or disabled")
        return status

    @model_validator(mode="after")
    def _validate_any_field(self):
        if self.username is None and self.password is None and self.role is None and self.status is None:
            raise ValueError("At least one of username, password, role or status must be provided.")
        return self
