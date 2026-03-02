"""Pydantic schemas for Superset Dataset API."""

from typing import Any

from pydantic import Field, field_validator

from superset_ai.schemas.common import (
    BaseSchema,
    ColumnInfo,
    MetricInfo,
    OwnerInfo,
    TimestampMixin,
)


class DatasetInfo(TimestampMixin, BaseSchema):
    """
    Dataset information returned from list endpoint.
    GET /api/v1/dataset/
    """

    id: int
    table_name: str
    schema_: str | None = Field(default=None, alias="schema")
    database_id: int | None = Field(default=None, alias="database")
    database_name: str | None = None
    sql: str | None = None
    kind: str | None = None  # "physical" or "virtual"
    is_sqllab_view: bool = False
    explore_url: str | None = None
    owners: list[OwnerInfo] = Field(default_factory=list)

    @field_validator("database_id", mode="before")
    @classmethod
    def extract_database_id(cls, v: Any) -> int | None:
        """Extract database ID from nested object if present."""
        if v is None:
            return None
        if isinstance(v, int):
            return v
        if isinstance(v, dict) and "id" in v:
            return v["id"]
        return None


class DatasetDetail(TimestampMixin, BaseSchema):
    """
    Detailed dataset information.
    GET /api/v1/dataset/{id}
    """

    id: int
    table_name: str
    schema_: str | None = Field(default=None, alias="schema")
    database_id: int | None = Field(default=None, alias="database")
    sql: str | None = None
    kind: str | None = None
    is_sqllab_view: bool = False

    # Column and metric details
    columns: list[ColumnInfo] = Field(default_factory=list)
    metrics: list[MetricInfo] = Field(default_factory=list)

    # Additional metadata
    main_dttm_col: str | None = None
    default_endpoint: str | None = None
    offset: int = 0
    cache_timeout: int | None = None
    params: str | None = None
    filter_select_enabled: bool = True
    fetch_values_predicate: str | None = None
    extra: str | None = None
    normalize_columns: bool = False
    always_filter_main_dttm: bool = False

    owners: list[OwnerInfo] = Field(default_factory=list)

    @field_validator("database_id", mode="before")
    @classmethod
    def extract_database_id(cls, v: Any) -> int | None:
        """Extract database ID from nested object if present."""
        if v is None:
            return None
        if isinstance(v, int):
            return v
        if isinstance(v, dict) and "id" in v:
            return v["id"]
        return None


class DatasetCreate(BaseSchema):
    """
    Schema for creating a new dataset.
    POST /api/v1/dataset/
    """

    table_name: str = Field(..., description="Name of the table or virtual dataset")
    database: int = Field(..., description="Database ID")
    schema_: str | None = Field(default=None, alias="schema", description="Database schema name")
    sql: str | None = Field(
        default=None,
        description="SQL query for virtual datasets",
    )
    owners: list[int] = Field(
        default_factory=list,
        description="List of owner user IDs",
    )


class DatasetUpdate(BaseSchema):
    """
    Schema for updating a dataset.
    PUT /api/v1/dataset/{id}
    """

    table_name: str | None = None
    schema_: str | None = Field(default=None, alias="schema")
    sql: str | None = None
    main_dttm_col: str | None = None
    columns: list[dict[str, Any]] | None = None
    metrics: list[dict[str, Any]] | None = None
    owners: list[int] | None = None
    cache_timeout: int | None = None


class DatasetListParams(BaseSchema):
    """Query parameters for listing datasets."""

    page: int = 0
    page_size: int = 100
    q: str | None = Field(
        default=None,
        description="JSON-encoded filter/sort parameters",
    )
