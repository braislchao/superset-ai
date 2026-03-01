"""Common schema types used across Superset API payloads."""

from datetime import datetime

from pydantic import BaseModel, Field


class BaseSchema(BaseModel):
    """Base model with common configuration."""

    model_config = {"extra": "ignore", "populate_by_name": True, "serialize_by_alias": True}


class ColumnInfo(BaseSchema):
    """Information about a dataset column."""

    id: int | None = None
    column_name: str
    type: str | None = None
    type_generic: int | None = None
    is_dttm: bool = False
    filterable: bool = True
    groupby: bool = True
    verbose_name: str | None = None
    description: str | None = None
    expression: str | None = None


class MetricInfo(BaseSchema):
    """Information about a dataset metric."""

    id: int | None = None
    metric_name: str
    expression: str
    metric_type: str | None = None
    verbose_name: str | None = None
    description: str | None = None
    d3format: str | None = None
    warning_text: str | None = None


class DatabaseInfo(BaseSchema):
    """Information about a database connection."""

    id: int
    database_name: str
    backend: str | None = None
    expose_in_sqllab: bool = True
    allow_ctas: bool = False
    allow_cvas: bool = False
    allow_dml: bool = False
    allow_run_async: bool = False


class TableInfo(BaseSchema):
    """Information about a database table."""

    name: str
    schema_: str | None = Field(default=None, alias="schema")
    type: str = "table"  # table or view


class OwnerInfo(BaseSchema):
    """Information about a resource owner."""

    id: int | None = None
    first_name: str | None = None
    last_name: str | None = None
    username: str | None = None


class TimestampMixin(BaseModel):
    """Mixin for created/modified timestamps."""

    model_config = {"extra": "ignore"}

    created_on: datetime | None = None
    changed_on: datetime | None = None
    created_by: OwnerInfo | None = None
    changed_by: OwnerInfo | None = None
