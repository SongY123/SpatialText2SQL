from __future__ import annotations

from typing import List, Optional

from pydantic import BaseModel, Field, root_validator, validator

try:
    from pydantic import ConfigDict
except ImportError:  # pydantic v1
    ConfigDict = None


class DatabaseCreateRequest(BaseModel):
    user_id: int = Field(..., ge=1)
    type: str = Field(..., min_length=1, max_length=16)
    url: str = Field(..., min_length=1, max_length=1024)
    schema_list: List[str] = Field(default_factory=list, alias="schema")
    db_username: Optional[str] = Field(default=None, max_length=256)
    db_password: Optional[str] = Field(default=None, max_length=256)

    if ConfigDict is not None:
        model_config = ConfigDict(populate_by_name=True)
    else:
        class Config:
            allow_population_by_field_name = True

    @validator("type")
    def _validate_type(cls, value: str) -> str:
        low = value.strip().lower()
        if low not in {"postgis", "spatial"}:
            raise ValueError("type must be Spatial or Postgis")
        return "Postgis" if low == "postgis" else "Spatial"


class DatabaseUpdateRequest(BaseModel):
    type: Optional[str] = Field(default=None, min_length=1, max_length=16)
    url: Optional[str] = Field(default=None, min_length=1, max_length=1024)
    schema_list: Optional[List[str]] = Field(default=None, alias="schema")
    db_username: Optional[str] = Field(default=None, max_length=256)
    db_password: Optional[str] = Field(default=None, max_length=256)

    if ConfigDict is not None:
        model_config = ConfigDict(populate_by_name=True)
    else:
        class Config:
            allow_population_by_field_name = True

    @validator("type")
    def _validate_type(cls, value: Optional[str]) -> Optional[str]:
        if value is None:
            return None
        low = value.strip().lower()
        if low not in {"postgis", "spatial"}:
            raise ValueError("type must be Spatial or Postgis")
        return "Postgis" if low == "postgis" else "Spatial"

    @root_validator(skip_on_failure=True)
    def _validate_any_field(cls, values):
        if (
            values.get("type") is None
            and values.get("url") is None
            and values.get("schema_list") is None
            and values.get("db_username") is None
            and values.get("db_password") is None
        ):
            raise ValueError("At least one field must be provided for update.")
        return values
