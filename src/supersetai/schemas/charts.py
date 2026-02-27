"""Pydantic schemas for Superset Chart API."""

import json
from typing import Any, Literal

from pydantic import Field, field_serializer, model_validator

from supersetai.schemas.common import BaseSchema, OwnerInfo, TimestampMixin


# Supported chart types for MVP
ChartType = Literal[
    "dist_bar",  # Distribution bar (no time column required)
    "line",      # Line chart (requires time column)
    "pie",
    "table",
    "big_number_total",
]

# Mapping from natural language to Superset viz_type
CHART_TYPE_MAP: dict[str, ChartType] = {
    "bar": "dist_bar",
    "bar chart": "dist_bar",
    "bar_chart": "dist_bar",
    "legacy bar": "dist_bar",
    "distribution bar": "dist_bar",
    "line": "line",
    "line chart": "line",
    "line_chart": "line",
    "timeseries": "line",
    "pie": "pie",
    "pie chart": "pie",
    "pie_chart": "pie",
    "table": "table",
    "data table": "table",
    "metric": "big_number_total",
    "number": "big_number_total",
    "big number": "big_number_total",
    "kpi": "big_number_total",
}


class ChartInfo(TimestampMixin, BaseSchema):
    """
    Chart information returned from list endpoint.
    GET /api/v1/chart/
    """

    id: int
    slice_name: str
    viz_type: str
    datasource_id: int | None = None
    datasource_type: str | None = None
    datasource_name_text: str | None = None
    description: str | None = None
    certified_by: str | None = None
    certification_details: str | None = None
    owners: list[OwnerInfo] = Field(default_factory=list)


class ChartDetail(TimestampMixin, BaseSchema):
    """
    Detailed chart information.
    GET /api/v1/chart/{id}
    """

    id: int
    slice_name: str
    viz_type: str
    datasource_id: int | None = None  # May not be returned directly by API
    datasource_type: str | None = None  # May not be returned directly by API
    datasource_name: str | None = None
    description: str | None = None
    params: str | None = None  # JSON string
    query_context: str | None = None  # JSON string
    cache_timeout: int | None = None
    owners: list[OwnerInfo] = Field(default_factory=list)
    dashboards: list[dict[str, Any]] = Field(default_factory=list)  # Dashboard associations
    url: str | None = None
    thumbnail_url: str | None = None

    @model_validator(mode="after")
    def extract_datasource_from_params(self) -> "ChartDetail":
        """Extract datasource_id and datasource_type from params if not set."""
        if self.datasource_id is None and self.params:
            try:
                params_dict = json.loads(self.params)
                datasource = params_dict.get("datasource", "")
                if "__" in datasource:
                    ds_id, ds_type = datasource.split("__", 1)
                    self.datasource_id = int(ds_id)
                    self.datasource_type = ds_type
            except (json.JSONDecodeError, ValueError):
                pass
        return self

    def get_params(self) -> dict[str, Any]:
        """Parse params JSON string to dict."""
        if self.params:
            return json.loads(self.params)
        return {}


class AdhocMetric(BaseSchema):
    """
    Adhoc metric definition for chart params.
    
    Used when creating metrics inline rather than referencing
    pre-defined dataset metrics.
    """

    expressionType: Literal["SIMPLE", "SQL"] = "SIMPLE"
    column: dict[str, Any] | None = None
    aggregate: str | None = None  # COUNT, SUM, AVG, MAX, MIN
    sqlExpression: str | None = None
    label: str | None = None
    optionName: str | None = None


class AdhocFilter(BaseSchema):
    """
    Adhoc filter definition for chart params.
    """

    expressionType: Literal["SIMPLE", "SQL"] = "SIMPLE"
    subject: str | None = None  # Column name
    operator: str | None = None  # ==, !=, >, <, IN, NOT IN, etc.
    comparator: Any = None  # Value to compare
    clause: Literal["WHERE", "HAVING"] = "WHERE"
    sqlExpression: str | None = None


class ChartParams(BaseSchema):
    """
    Internal model for chart visualization parameters.
    
    This gets serialized to JSON string for the `params` field.
    Note: Structure varies significantly by viz_type.
    """

    viz_type: str
    datasource: str  # Format: "{id}__table"
    
    # Metrics and dimensions
    metrics: list[str | dict[str, Any]] = Field(default_factory=list)
    groupby: list[str] = Field(default_factory=list)
    columns: list[str] = Field(default_factory=list)
    
    # Time settings
    time_range: str = "No filter"
    granularity_sqla: str | None = None
    time_grain_sqla: str | None = None
    
    # Filters
    adhoc_filters: list[dict[str, Any]] = Field(default_factory=list)
    
    # Display settings
    row_limit: int = 1000
    order_desc: bool = True
    
    # Chart-specific options (varies by viz_type)
    show_legend: bool = True
    legendType: str = "scroll"
    legendOrientation: str = "top"
    x_axis_title: str | None = None
    y_axis_title: str | None = None
    
    # Bar chart specific
    rich_tooltip: bool = True
    show_bar_value: bool = False
    bar_stacked: bool = False
    
    # Table-specific
    all_columns: list[str] = Field(default_factory=list)
    percent_metrics: list[str] = Field(default_factory=list)
    
    # Additional params
    extra_form_data: dict[str, Any] = Field(default_factory=dict)

    def to_json(self) -> str:
        """Serialize to JSON string for API."""
        return json.dumps(self.model_dump(exclude_none=True))


class ChartCreate(BaseSchema):
    """
    Schema for creating a new chart.
    POST /api/v1/chart/
    """

    slice_name: str = Field(..., description="Chart title")
    viz_type: str = Field(..., description="Visualization type")
    datasource_id: int = Field(..., description="Dataset ID")
    datasource_type: str = Field(default="table", description="Datasource type")
    params: str = Field(..., description="JSON-encoded visualization parameters")
    query_context: str | None = Field(
        default=None,
        description="JSON-encoded query context",
    )
    description: str | None = None
    cache_timeout: int | None = None
    owners: list[int] = Field(default_factory=list)
    dashboards: list[int] = Field(default_factory=list)

    @field_serializer("params")
    def serialize_params(self, v: str) -> str:
        """Ensure params is a valid JSON string."""
        # Validate it's valid JSON
        json.loads(v)
        return v


class ChartUpdate(BaseSchema):
    """
    Schema for updating a chart.
    PUT /api/v1/chart/{id}
    """

    slice_name: str | None = None
    viz_type: str | None = None
    params: str | None = None
    query_context: str | None = None
    description: str | None = None
    cache_timeout: int | None = None
    owners: list[int] | None = None
    dashboards: list[int] | None = None


class ChartListParams(BaseSchema):
    """Query parameters for listing charts."""

    page: int = 0
    page_size: int = 100
    q: str | None = Field(
        default=None,
        description="JSON-encoded filter/sort parameters",
    )


# =============================================================================
# Chart Params Builders
# =============================================================================


def build_bar_chart_params(
    datasource_id: int,
    metrics: list[str],
    groupby: list[str],
    *,
    time_column: str | None = None,
    time_range: str = "No filter",
    row_limit: int = 1000,
) -> ChartParams:
    """Build params for distribution bar chart visualization (no time column required)."""
    return ChartParams(
        viz_type="dist_bar",
        datasource=f"{datasource_id}__table",
        metrics=metrics,
        groupby=groupby,
        granularity_sqla=time_column,
        time_range=time_range,
        row_limit=row_limit,
        order_desc=True,
        show_legend=True,
        rich_tooltip=True,
        show_bar_value=True,
        bar_stacked=False,
    )


def build_line_chart_params(
    datasource_id: int,
    metrics: list[str],
    time_column: str,
    *,
    groupby: list[str] | None = None,
    time_grain: str = "P1D",  # Daily
    time_range: str = "Last 30 days",
    row_limit: int = 10000,
) -> ChartParams:
    """Build params for line chart visualization."""
    return ChartParams(
        viz_type="line",
        datasource=f"{datasource_id}__table",
        metrics=metrics,
        groupby=groupby or [],
        granularity_sqla=time_column,
        time_grain_sqla=time_grain,
        time_range=time_range,
        row_limit=row_limit,
        show_legend=True,
    )


def build_pie_chart_params(
    datasource_id: int,
    metric: str,
    groupby: str,
    *,
    time_range: str = "No filter",
    row_limit: int = 100,
) -> ChartParams:
    """Build params for pie chart visualization."""
    return ChartParams(
        viz_type="pie",
        datasource=f"{datasource_id}__table",
        metrics=[metric],
        groupby=[groupby],
        time_range=time_range,
        row_limit=row_limit,
        show_legend=True,
        legendType="scroll",
        legendOrientation="top",
    )


def build_table_params(
    datasource_id: int,
    columns: list[str],
    *,
    metrics: list[str] | None = None,
    groupby: list[str] | None = None,
    time_range: str = "No filter",
    row_limit: int = 1000,
) -> ChartParams:
    """Build params for table visualization."""
    params = ChartParams(
        viz_type="table",
        datasource=f"{datasource_id}__table",
        time_range=time_range,
        row_limit=row_limit,
    )
    
    if metrics and groupby:
        # Aggregated table
        params.metrics = metrics
        params.groupby = groupby
    else:
        # Raw data table
        params.all_columns = columns
    
    return params


def build_big_number_params(
    datasource_id: int,
    metric: str,
    *,
    time_column: str | None = None,
    time_range: str = "No filter",
) -> ChartParams:
    """Build params for big number/KPI visualization."""
    return ChartParams(
        viz_type="big_number_total",
        datasource=f"{datasource_id}__table",
        metrics=[metric],
        granularity_sqla=time_column,
        time_range=time_range,
    )


def build_adhoc_metric(
    column_name: str,
    aggregate: str = "COUNT",
    label: str | None = None,
) -> dict[str, Any]:
    """
    Build an adhoc metric definition.
    
    Args:
        column_name: Name of the column to aggregate (use "*" for COUNT(*))
        aggregate: Aggregation function (COUNT, SUM, AVG, MAX, MIN)
        label: Display label for the metric
    
    Returns:
        Dict suitable for inclusion in metrics array
    """
    # For COUNT(*), use SQL expression type since SIMPLE type doesn't accept "*"
    if column_name == "*" and aggregate.upper() == "COUNT":
        return {
            "expressionType": "SQL",
            "sqlExpression": "COUNT(*)",
            "label": label or "COUNT(*)",
            "optionName": "metric_count_star",
        }
    
    return {
        "expressionType": "SIMPLE",
        "column": {"column_name": column_name},
        "aggregate": aggregate.upper(),
        "label": label or f"{aggregate}({column_name})",
        "optionName": f"metric_{column_name}_{aggregate.lower()}",
    }
