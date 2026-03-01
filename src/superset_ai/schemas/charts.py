"""Pydantic schemas for Superset Chart API."""

import json
from typing import Any, Literal

from pydantic import Field, field_serializer, model_validator

from superset_ai.schemas.common import BaseSchema, OwnerInfo, TimestampMixin


# Supported chart types
ChartType = Literal[
    # Original types
    "dist_bar",              # Distribution bar (no time column required)
    "line",                  # Line chart (requires time column)
    "pie",
    "table",
    "big_number_total",      # Big number without trendline
    # New types
    "area",                  # Area chart (filled line)
    "big_number",            # Big number with trendline
    "echarts_timeseries_bar",  # ECharts bar chart over time
    "bubble",                # Bubble chart (scatter with size)
    "funnel",                # Funnel chart
    "gauge_chart",           # Gauge / speedometer
    "treemap_v2",            # Treemap
    "histogram",             # Histogram (value distribution)
    "box_plot",              # Box plot (statistical distribution)
    "heatmap",               # Heatmap (2D color grid)
]

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
    
    # Ordering
    row_limit: int = 1000
    order_desc: bool | None = None
    timeseries_limit_metric: str | dict[str, Any] | None = None
    
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

    # Area chart specific
    stacked_style: str | None = None  # "stack", "stream", "expand"

    # Big number with trendline specific
    header_font_size: float | None = None
    subheader_font_size: float | None = None

    # Bubble chart specific
    series: str | None = None  # Series column (grouping for bubbles)
    entity: str | None = None  # Entity column (label for each bubble)
    x: dict[str, Any] | None = None  # X-axis metric
    y: dict[str, Any] | None = None  # Y-axis metric
    size: dict[str, Any] | None = None  # Bubble size metric
    max_bubble_size: int | None = None

    # Funnel chart specific
    sort_by_metric: bool = True

    # Gauge chart specific
    min_val: float | None = None
    max_val: float | None = None
    start_angle: int | None = None
    end_angle: int | None = None
    show_pointer: bool = True
    show_axis_tick: bool = False
    show_split_line: bool = False
    show_progress: bool = True
    overlap: bool = True
    animation: bool = True

    # Treemap specific
    # uses standard metrics + groupby

    # Histogram specific
    link_length: int | None = None  # bin size / number of bins
    x_axis_label: str | None = None
    y_axis_label: str | None = None
    normalized: bool = False

    # Box plot specific
    whisker_options: str | None = None  # "Tukey", "Min/max (no outliers)", "2/98 percentiles", "9/91 percentiles"

    # Heatmap specific
    all_columns_x: str | None = None
    all_columns_y: str | None = None
    linear_color_scheme: str | None = None
    xscale_interval: int | None = None
    yscale_interval: int | None = None
    canvas_image_rendering: str | None = None  # "pixelated", "auto"
    show_perc: bool = True
    show_values: bool = False
    normalize_across: str | None = None  # "heatmap", "x", "y"

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
    metrics: list[str | dict[str, Any]],
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
        timeseries_limit_metric=metrics[0] if metrics else None,
        show_legend=True,
        rich_tooltip=True,
        show_bar_value=True,
        bar_stacked=False,
    )


def build_line_chart_params(
    datasource_id: int,
    metrics: list[str | dict[str, Any]],
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
        timeseries_limit_metric=metrics[0] if metrics else None,
        show_legend=True,
    )


def build_pie_chart_params(
    datasource_id: int,
    metric: str | dict[str, Any],
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
        timeseries_limit_metric=metric,
        show_legend=True,
        legendType="scroll",
        legendOrientation="top",
    )


def build_table_params(
    datasource_id: int,
    columns: list[str],
    *,
    metrics: list[str | dict[str, Any]] | None = None,
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
    metric: str | dict[str, Any],
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


# =============================================================================
# New Chart Params Builders
# =============================================================================


def build_area_chart_params(
    datasource_id: int,
    metrics: list[str | dict[str, Any]],
    time_column: str,
    *,
    groupby: list[str] | None = None,
    time_grain: str = "P1D",
    time_range: str = "Last 30 days",
    stacked: bool = True,
    row_limit: int = 10000,
) -> ChartParams:
    """Build params for area chart visualization (filled line chart).

    Args:
        datasource_id: Dataset ID.
        metrics: Metric expressions.
        time_column: Time column for x-axis.
        groupby: Optional grouping for stacked areas.
        time_grain: Time granularity.
        time_range: Time filter.
        stacked: If *True* areas are stacked.
        row_limit: Max data points.
    """
    return ChartParams(
        viz_type="area",
        datasource=f"{datasource_id}__table",
        metrics=metrics,
        groupby=groupby or [],
        granularity_sqla=time_column,
        time_grain_sqla=time_grain,
        time_range=time_range,
        row_limit=row_limit,
        timeseries_limit_metric=metrics[0] if metrics else None,
        show_legend=True,
        stacked_style="stack" if stacked else None,
    )


def build_big_number_with_trendline_params(
    datasource_id: int,
    metric: str | dict[str, Any],
    time_column: str,
    *,
    time_grain: str = "P1D",
    time_range: str = "Last 30 days",
) -> ChartParams:
    """Build params for big number with trendline / sparkline.

    Unlike ``big_number_total``, this variant requires a time column and
    renders a small line chart below the metric value.
    """
    return ChartParams(
        viz_type="big_number",
        datasource=f"{datasource_id}__table",
        metrics=[metric],
        granularity_sqla=time_column,
        time_grain_sqla=time_grain,
        time_range=time_range,
    )


def build_timeseries_bar_chart_params(
    datasource_id: int,
    metrics: list[str | dict[str, Any]],
    time_column: str,
    *,
    groupby: list[str] | None = None,
    time_grain: str = "P1D",
    time_range: str = "Last 30 days",
    stacked: bool = False,
    row_limit: int = 10000,
) -> ChartParams:
    """Build params for ECharts timeseries bar chart.

    Like ``dist_bar`` but plotted over a time axis.
    """
    return ChartParams(
        viz_type="echarts_timeseries_bar",
        datasource=f"{datasource_id}__table",
        metrics=metrics,
        groupby=groupby or [],
        granularity_sqla=time_column,
        time_grain_sqla=time_grain,
        time_range=time_range,
        row_limit=row_limit,
        timeseries_limit_metric=metrics[0] if metrics else None,
        show_legend=True,
        rich_tooltip=True,
        bar_stacked=stacked,
    )


def build_bubble_chart_params(
    datasource_id: int,
    x_metric: str | dict[str, Any],
    y_metric: str | dict[str, Any],
    size_metric: str | dict[str, Any],
    series_column: str,
    *,
    entity_column: str | None = None,
    time_range: str = "No filter",
    max_bubble_size: int = 25,
    row_limit: int = 500,
) -> ChartParams:
    """Build params for bubble chart visualization.

    Bubble charts use three metrics mapped to x-position, y-position,
    and bubble size, plus a series column for colouring.
    """
    return ChartParams(
        viz_type="bubble",
        datasource=f"{datasource_id}__table",
        # Bubble uses special x/y/size fields rather than metrics list
        x=_metric_to_dict(x_metric),
        y=_metric_to_dict(y_metric),
        size=_metric_to_dict(size_metric),
        series=series_column,
        entity=entity_column or series_column,
        time_range=time_range,
        max_bubble_size=max_bubble_size,
        row_limit=row_limit,
        show_legend=True,
    )


def build_funnel_chart_params(
    datasource_id: int,
    metric: str | dict[str, Any],
    groupby: str,
    *,
    time_range: str = "No filter",
    sort_by_metric: bool = True,
    row_limit: int = 100,
) -> ChartParams:
    """Build params for funnel chart visualization.

    Funnels show sequential stages with decreasing values.
    """
    return ChartParams(
        viz_type="funnel",
        datasource=f"{datasource_id}__table",
        metrics=[metric],
        groupby=[groupby],
        time_range=time_range,
        sort_by_metric=sort_by_metric,
        row_limit=row_limit,
        show_legend=True,
    )


def build_gauge_chart_params(
    datasource_id: int,
    metric: str | dict[str, Any],
    *,
    min_val: float = 0,
    max_val: float = 100,
    time_range: str = "No filter",
    start_angle: int = 225,
    end_angle: int = -45,
) -> ChartParams:
    """Build params for gauge / speedometer chart.

    Displays a single metric as a position on an arc.
    """
    return ChartParams(
        viz_type="gauge_chart",
        datasource=f"{datasource_id}__table",
        metrics=[metric],
        time_range=time_range,
        min_val=min_val,
        max_val=max_val,
        start_angle=start_angle,
        end_angle=end_angle,
        show_pointer=True,
        show_progress=True,
        animation=True,
        overlap=True,
    )


def build_treemap_params(
    datasource_id: int,
    metric: str | dict[str, Any],
    groupby: list[str],
    *,
    time_range: str = "No filter",
    row_limit: int = 500,
) -> ChartParams:
    """Build params for treemap visualization.

    Treemaps display hierarchical data as nested rectangles whose area
    is proportional to the metric value.
    """
    return ChartParams(
        viz_type="treemap_v2",
        datasource=f"{datasource_id}__table",
        metrics=[metric],
        groupby=groupby,
        time_range=time_range,
        row_limit=row_limit,
        show_legend=False,
    )


def build_histogram_params(
    datasource_id: int,
    column: str,
    *,
    groupby: list[str] | None = None,
    link_length: int = 10,
    time_range: str = "No filter",
    row_limit: int = 10000,
    normalized: bool = False,
) -> ChartParams:
    """Build params for histogram visualization.

    Histograms show the distribution of a single numeric column.

    Args:
        datasource_id: Dataset ID.
        column: The numeric column whose distribution to plot.
        groupby: Optional grouping for overlaid histograms.
        link_length: Number of bins.
        time_range: Time filter.
        row_limit: Max rows to fetch.
        normalized: Whether to normalise the histogram.
    """
    return ChartParams(
        viz_type="histogram",
        datasource=f"{datasource_id}__table",
        all_columns=[column],
        groupby=groupby or [],
        link_length=link_length,
        time_range=time_range,
        row_limit=row_limit,
        normalized=normalized,
    )


def build_box_plot_params(
    datasource_id: int,
    metrics: list[str | dict[str, Any]],
    groupby: list[str],
    *,
    time_column: str | None = None,
    time_range: str = "No filter",
    whisker_options: str = "Tukey",
) -> ChartParams:
    """Build params for box plot visualization.

    Box plots display the statistical distribution (median, quartiles,
    outliers) of one or more metrics, grouped by dimensions.

    Args:
        datasource_id: Dataset ID.
        metrics: Metric expressions to plot.
        groupby: Dimension columns for grouping.
        time_column: Optional time column for filtering.
        time_range: Time filter.
        whisker_options: Whisker calculation method.
    """
    return ChartParams(
        viz_type="box_plot",
        datasource=f"{datasource_id}__table",
        metrics=metrics,
        groupby=groupby,
        granularity_sqla=time_column,
        time_range=time_range,
        whisker_options=whisker_options,
    )


def build_heatmap_params(
    datasource_id: int,
    metric: str | dict[str, Any],
    x_column: str,
    y_column: str,
    *,
    time_range: str = "No filter",
    linear_color_scheme: str = "blue_white_yellow",
    normalize_across: str | None = None,
    show_values: bool = False,
) -> ChartParams:
    """Build params for heatmap visualization.

    Heatmaps display a 2D grid coloured by a metric value at each
    (x, y) intersection.

    Args:
        datasource_id: Dataset ID.
        metric: Metric expression for cell colour intensity.
        x_column: Column for x-axis.
        y_column: Column for y-axis.
        time_range: Time filter.
        linear_color_scheme: Colour scheme.
        normalize_across: Normalisation axis (``None``, ``"heatmap"``, ``"x"``, ``"y"``).
        show_values: Whether to render values inside cells.
    """
    return ChartParams(
        viz_type="heatmap",
        datasource=f"{datasource_id}__table",
        metrics=[metric],
        all_columns_x=x_column,
        all_columns_y=y_column,
        time_range=time_range,
        linear_color_scheme=linear_color_scheme,
        normalize_across=normalize_across,
        show_values=show_values,
        show_legend=True,
        canvas_image_rendering="pixelated",
    )


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _metric_to_dict(metric: str | dict[str, Any]) -> dict[str, Any]:
    """Ensure a metric spec is a dict (for bubble x/y/size fields)."""
    if isinstance(metric, dict):
        return metric
    # Treat as a pre-defined metric name reference
    return {"label": metric, "expressionType": "SIMPLE", "column": {"column_name": metric}, "aggregate": "SUM"}
