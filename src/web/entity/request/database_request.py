from __future__ import annotations

from typing import List, Optional

from pydantic import BaseModel, Field, field_validator, model_validator

try:
    from pydantic import ConfigDict
except ImportError:  # pydantic v1
    ConfigDict = None


class DatabaseCreateRequest(BaseModel):
    name: str = Field(..., min_length=1, max_length=256)
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

    @field_validator("type")
    @classmethod
    def _validate_type(cls, value: str) -> str:
        low = value.strip().lower()
        if low not in {"postgis", "spatial"}:
            raise ValueError("type must be Spatial or Postgis")
        return "Postgis" if low == "postgis" else "Spatial"


class DatabaseUpdateRequest(BaseModel):
    name: Optional[str] = Field(default=None, min_length=1, max_length=256)
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

    @field_validator("type")
    @classmethod
    def _validate_type(cls, value: Optional[str]) -> Optional[str]:
        if value is None:
            return None
        low = value.strip().lower()
        if low not in {"postgis", "spatial"}:
            raise ValueError("type must be Spatial or Postgis")
        return "Postgis" if low == "postgis" else "Spatial"

    @model_validator(mode="after")
    def _validate_any_field(self):
        if (
            self.name is None
            and self.type is None
            and self.url is None
            and self.schema_list is None
            and self.db_username is None
            and self.db_password is None
        ):
            raise ValueError("At least one field must be provided for update.")
        return self


class DatabaseSchemaProbeRequest(BaseModel):
    type: str = Field(..., min_length=1, max_length=16)
    jdbc_url: str = Field(..., min_length=1, max_length=2048, alias="jdbcurl")
    username: Optional[str] = Field(default=None, max_length=256)
    password: Optional[str] = Field(default=None, max_length=256)

    if ConfigDict is not None:
        model_config = ConfigDict(populate_by_name=True)
    else:
        class Config:
            allow_population_by_field_name = True

    @field_validator("type")
    @classmethod
    def _validate_type(cls, value: str) -> str:
        low = value.strip().lower()
        if low not in {"postgis", "spatial"}:
            raise ValueError("type must be Spatial or Postgis")
        return "Postgis" if low == "postgis" else "Spatial"


class DatabaseSqlExecuteRequest(BaseModel):
    chat_id: Optional[int] = Field(default=None, ge=1)
    schema_name: str = Field(..., min_length=1, max_length=128, alias="schema")
    sql: str = Field(..., min_length=1, max_length=200000)
    page: int = Field(default=1, ge=1)
    page_size: int = Field(default=20, ge=1, le=500)

    if ConfigDict is not None:
        model_config = ConfigDict(populate_by_name=True)
    else:
        class Config:
            allow_population_by_field_name = True
