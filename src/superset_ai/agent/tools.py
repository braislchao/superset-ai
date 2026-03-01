"""Agent tools for Superset operations.

Thin LangChain @tool wrappers that delegate to the shared operations layer.
Each tool resolves its services from the per-task ContextVar, calls the
corresponding operation, and applies agent-specific side-effects (session
caching, asset tracking).
"""

import contextvars
import logging
from typing import Any, Literal

from langchain_core.tools import tool

from superset_ai.operations import charts as chart_ops
from superset_ai.operations import dashboards as dashboard_ops
from superset_ai.operations import datasets as dataset_ops
from superset_ai.operations import discovery as discovery_ops

logger = logging.getLogger(__name__)

# Context variable for per-task tool context isolation.
# Unlike a module-level global, ContextVar is safe for concurrent async tasks —
# each asyncio Task gets its own copy of the value.
_tool_context_var: contextvars.ContextVar[Any] = contextvars.ContextVar(
    "_tool_context_var",
)


def set_tool_context(context: Any) -> None:
    """Set the tool context for the current execution."""
    _tool_context_var.set(context)


def get_tool_context() -> Any:
    """Get the current tool context."""
    try:
        return _tool_context_var.get()
    except LookupError:
        raise RuntimeError("Tool context not set. Call set_tool_context first.")


# =============================================================================
# Discovery Tools
# =============================================================================


@tool
async def list_databases() -> list[dict[str, Any]]:
    """
    List all available database connections in Superset.

    Returns a list of databases with their IDs and names.
    Use this first to discover what data sources are available.
    """
    ctx = get_tool_context()
    result = await discovery_ops.list_databases(ctx.databases)
    # Cache in session context
    ctx.session.superset_context.databases = result
    return result


@tool
async def list_schemas(database_id: int) -> list[str]:
    """
    List all schemas in a specific database.

    Args:
        database_id: The ID of the database to list schemas from

    Returns a list of schema names. For SQLite, this is typically ["main"].
    For PostgreSQL/MySQL, this may include schemas like "public", "information_schema", etc.
    """
    ctx = get_tool_context()
    return await discovery_ops.list_schemas(ctx.databases, database_id)


@tool
async def list_tables(database_id: int, schema_name: str | None = None) -> list[dict[str, Any]]:
    """
    List all tables in a specific database schema.

    Args:
        database_id: The ID of the database to list tables from
        schema_name: Optional schema name. If not provided, uses the first available schema.

    Returns a list of table names available in the database.
    """
    ctx = get_tool_context()
    result = await discovery_ops.list_tables(ctx.databases, database_id, schema_name)
    # Cache table names
    ctx.session.superset_context.discovered_tables[database_id] = [
        t["name"] for t in result
    ]
    return result


@tool
async def get_dataset_columns(dataset_id: int) -> dict[str, Any]:
    """
    Get column information for a dataset.

    Args:
        dataset_id: The ID of the dataset

    Returns column names, types, and which are suitable for time/metrics.
    """
    ctx = get_tool_context()
    result = await discovery_ops.get_dataset_columns(ctx.datasets, dataset_id)
    # Cache
    ctx.session.superset_context.discovered_columns[dataset_id] = [
        c["name"] for c in result["columns"]
    ]
    return result


@tool
async def list_existing_datasets(database_id: int | None = None) -> list[dict[str, Any]]:
    """
    List existing datasets in Superset.

    Args:
        database_id: Optional database ID to filter by

    Returns list of datasets that can be reused for charts.
    """
    ctx = get_tool_context()
    return await discovery_ops.list_existing_datasets(ctx.datasets, database_id)


@tool
async def execute_sql(
    database_id: int,
    sql: str,
    limit: int = 100,
) -> dict[str, Any]:
    """
    Execute a SQL query against a Superset database.

    Use this to explore data, validate column names, check cardinality,
    or run ad-hoc analysis before creating charts.

    Args:
        database_id: The database ID to run the query against
        sql: The SQL query string
        limit: Maximum number of rows to return (default 100)

    Returns columns, data rows, row count, and whether the result was truncated.
    """
    ctx = get_tool_context()
    return await discovery_ops.execute_sql(ctx.databases, database_id, sql, limit)


@tool
async def profile_dataset(
    dataset_id: int,
    sample_size: int = 5,
) -> dict[str, Any]:
    """
    Profile a dataset to understand its data shape before creating charts.

    Runs exploratory SQL queries to gather row count, per-column cardinality,
    null counts, and sample values. Use this to make informed decisions about
    which chart type and columns to use.

    Args:
        dataset_id: The dataset to profile
        sample_size: Number of sample values to retrieve per column (default 5)

    Returns dataset metadata, row count, and per-column statistics.
    """
    ctx = get_tool_context()
    return await discovery_ops.profile_dataset(
        ctx.databases, ctx.datasets, dataset_id, sample_size
    )


# =============================================================================
# Dataset Tools
# =============================================================================


@tool
async def find_or_create_dataset(
    database_id: int,
    table_name: str,
    schema_name: str | None = None,
) -> dict[str, Any]:
    """
    Find an existing dataset or create a new one for a table.

    Args:
        database_id: The database ID containing the table
        table_name: Name of the table
        schema_name: Optional database schema name

    Returns the dataset information including ID and columns.
    """
    ctx = get_tool_context()
    result = await dataset_ops.find_or_create_dataset(
        ctx.datasets, database_id, table_name, schema_name
    )
    ctx.session.add_asset("dataset", result["id"], result["table_name"])
    return result


# =============================================================================
# Chart Tools
# =============================================================================


@tool
async def create_bar_chart(
    title: str,
    dataset_id: int,
    metrics: list[str],
    dimensions: list[str],
    time_range: str = "No filter",
) -> dict[str, Any]:
    """
    Create a bar chart visualization.

    Args:
        title: Chart title
        dataset_id: ID of the dataset to use
        metrics: List of metrics (e.g., ["COUNT(*)", "SUM(amount)"])
        dimensions: List of dimension columns to group by
        time_range: Time filter (e.g., "Last 7 days", "No filter")

    Returns the created chart information with ID and URL.
    """
    ctx = get_tool_context()
    result = await chart_ops.create_bar_chart(
        ctx.charts, title, dataset_id, metrics, dimensions, time_range
    )
    ctx.session.add_asset("chart", result["id"], result["title"])
    return result


@tool
async def create_line_chart(
    title: str,
    dataset_id: int,
    metrics: list[str],
    time_column: str,
    dimensions: list[str] | None = None,
    time_grain: str = "P1D",
    time_range: str = "Last 30 days",
) -> dict[str, Any]:
    """
    Create a line/timeseries chart.

    Args:
        title: Chart title
        dataset_id: ID of the dataset to use
        metrics: List of metrics to plot
        time_column: Column to use for x-axis time
        dimensions: Optional grouping columns for multiple lines
        time_grain: Time granularity (P1D=daily, P1W=weekly, P1M=monthly)
        time_range: Time filter

    Returns the created chart information.
    """
    ctx = get_tool_context()
    result = await chart_ops.create_line_chart(
        ctx.charts, title, dataset_id, metrics, time_column,
        dimensions, time_grain, time_range,
    )
    ctx.session.add_asset("chart", result["id"], result["title"])
    return result


@tool
async def create_pie_chart(
    title: str,
    dataset_id: int,
    metric: str,
    dimension: str,
    time_range: str = "No filter",
) -> dict[str, Any]:
    """
    Create a pie chart visualization.

    Args:
        title: Chart title
        dataset_id: ID of the dataset to use
        metric: Single metric for slice sizes
        dimension: Column for pie slices
        time_range: Time filter

    Returns the created chart information.
    """
    ctx = get_tool_context()
    result = await chart_ops.create_pie_chart(
        ctx.charts, title, dataset_id, metric, dimension, time_range
    )
    ctx.session.add_asset("chart", result["id"], result["title"])
    return result


@tool
async def create_table_chart(
    title: str,
    dataset_id: int,
    columns: list[str],
    metrics: list[str] | None = None,
    dimensions: list[str] | None = None,
    row_limit: int = 1000,
) -> dict[str, Any]:
    """
    Create a table visualization.

    Args:
        title: Chart title
        dataset_id: ID of the dataset to use
        columns: Columns to display (for raw data)
        metrics: Optional metrics for aggregated table
        dimensions: Optional grouping for aggregated table
        row_limit: Maximum rows to show

    Returns the created chart information.
    """
    ctx = get_tool_context()
    result = await chart_ops.create_table_chart(
        ctx.charts, title, dataset_id, columns, metrics, dimensions, row_limit
    )
    ctx.session.add_asset("chart", result["id"], result["title"])
    return result


@tool
async def create_metric_chart(
    title: str,
    dataset_id: int,
    metric: str,
    time_range: str = "No filter",
) -> dict[str, Any]:
    """
    Create a big number/KPI metric visualization.

    Args:
        title: Chart title
        dataset_id: ID of the dataset to use
        metric: The metric to display (e.g., "COUNT(*)", "SUM(revenue)")
        time_range: Time filter

    Returns the created chart information.
    """
    ctx = get_tool_context()
    result = await chart_ops.create_metric_chart(
        ctx.charts, title, dataset_id, metric, time_range
    )
    ctx.session.add_asset("chart", result["id"], result["title"])
    return result


@tool
async def create_area_chart(
    title: str,
    dataset_id: int,
    metrics: list[str],
    time_column: str,
    dimensions: list[str] | None = None,
    time_grain: str = "P1D",
    time_range: str = "Last 30 days",
    stacked: bool = True,
) -> dict[str, Any]:
    """
    Create an area chart (filled line chart).

    Args:
        title: Chart title
        dataset_id: ID of the dataset to use
        metrics: List of metrics to plot
        time_column: Column to use for x-axis time
        dimensions: Optional grouping columns for stacked areas
        time_grain: Time granularity (P1D=daily, P1W=weekly, P1M=monthly)
        time_range: Time filter
        stacked: Whether to stack the areas (default True)

    Returns the created chart information.
    """
    ctx = get_tool_context()
    result = await chart_ops.create_area_chart(
        ctx.charts, title, dataset_id, metrics, time_column,
        dimensions, time_grain, time_range, stacked,
    )
    ctx.session.add_asset("chart", result["id"], result["title"])
    return result


@tool
async def create_big_number_trendline_chart(
    title: str,
    dataset_id: int,
    metric: str,
    time_column: str,
    time_grain: str = "P1D",
    time_range: str = "Last 30 days",
) -> dict[str, Any]:
    """
    Create a big number KPI with a trendline / sparkline.

    Unlike the plain big number chart, this variant requires a time column
    and renders a small trend line below the headline metric.

    Args:
        title: Chart title
        dataset_id: ID of the dataset to use
        metric: The metric to display (e.g., "COUNT(*)", "SUM(revenue)")
        time_column: Time column for the trendline
        time_grain: Time granularity (P1D=daily, P1W=weekly, P1M=monthly)
        time_range: Time filter

    Returns the created chart information.
    """
    ctx = get_tool_context()
    result = await chart_ops.create_big_number_trendline_chart(
        ctx.charts, title, dataset_id, metric, time_column,
        time_grain, time_range,
    )
    ctx.session.add_asset("chart", result["id"], result["title"])
    return result


@tool
async def create_timeseries_bar_chart(
    title: str,
    dataset_id: int,
    metrics: list[str],
    time_column: str,
    dimensions: list[str] | None = None,
    time_grain: str = "P1D",
    time_range: str = "Last 30 days",
    stacked: bool = False,
) -> dict[str, Any]:
    """
    Create a timeseries bar chart (ECharts).

    Like a regular bar chart but plotted over a time axis. Supports stacking.

    Args:
        title: Chart title
        dataset_id: ID of the dataset to use
        metrics: List of metrics to plot
        time_column: Time column for the x-axis
        dimensions: Optional grouping columns for stacked bars
        time_grain: Time granularity (P1D=daily, P1W=weekly, P1M=monthly)
        time_range: Time filter
        stacked: Whether to stack the bars (default False)

    Returns the created chart information.
    """
    ctx = get_tool_context()
    result = await chart_ops.create_timeseries_bar_chart(
        ctx.charts, title, dataset_id, metrics, time_column,
        dimensions, time_grain, time_range, stacked,
    )
    ctx.session.add_asset("chart", result["id"], result["title"])
    return result


@tool
async def create_bubble_chart(
    title: str,
    dataset_id: int,
    x_metric: str,
    y_metric: str,
    size_metric: str,
    series_column: str,
    entity_column: str | None = None,
    time_range: str = "No filter",
    max_bubble_size: int = 25,
) -> dict[str, Any]:
    """
    Create a bubble chart visualization.

    Three metrics are mapped to x-position, y-position, and bubble size.

    Args:
        title: Chart title
        dataset_id: ID of the dataset to use
        x_metric: Metric for x-axis position
        y_metric: Metric for y-axis position
        size_metric: Metric for bubble size
        series_column: Column for colouring / grouping bubbles
        entity_column: Column for bubble labels (defaults to series_column)
        time_range: Time filter
        max_bubble_size: Maximum bubble diameter in pixels

    Returns the created chart information.
    """
    ctx = get_tool_context()
    result = await chart_ops.create_bubble_chart(
        ctx.charts, title, dataset_id, x_metric, y_metric,
        size_metric, series_column, entity_column, time_range,
        max_bubble_size,
    )
    ctx.session.add_asset("chart", result["id"], result["title"])
    return result


@tool
async def create_funnel_chart(
    title: str,
    dataset_id: int,
    metric: str,
    dimension: str,
    time_range: str = "No filter",
    sort_by_metric: bool = True,
) -> dict[str, Any]:
    """
    Create a funnel chart visualization.

    Funnels show sequential stages with decreasing values.

    Args:
        title: Chart title
        dataset_id: ID of the dataset to use
        metric: Single metric for funnel stage values
        dimension: Column representing funnel stages
        time_range: Time filter
        sort_by_metric: Whether to sort stages by metric value

    Returns the created chart information.
    """
    ctx = get_tool_context()
    result = await chart_ops.create_funnel_chart(
        ctx.charts, title, dataset_id, metric, dimension,
        time_range, sort_by_metric,
    )
    ctx.session.add_asset("chart", result["id"], result["title"])
    return result


@tool
async def create_gauge_chart(
    title: str,
    dataset_id: int,
    metric: str,
    min_val: float = 0,
    max_val: float = 100,
    time_range: str = "No filter",
) -> dict[str, Any]:
    """
    Create a gauge / speedometer chart.

    Displays a single metric as a position on an arc between min and max.

    Args:
        title: Chart title
        dataset_id: ID of the dataset to use
        metric: The metric to display
        min_val: Minimum value on the gauge scale
        max_val: Maximum value on the gauge scale
        time_range: Time filter

    Returns the created chart information.
    """
    ctx = get_tool_context()
    result = await chart_ops.create_gauge_chart(
        ctx.charts, title, dataset_id, metric, min_val, max_val, time_range,
    )
    ctx.session.add_asset("chart", result["id"], result["title"])
    return result


@tool
async def create_treemap_chart(
    title: str,
    dataset_id: int,
    metric: str,
    dimensions: list[str],
    time_range: str = "No filter",
) -> dict[str, Any]:
    """
    Create a treemap visualization.

    Treemaps display hierarchical data as nested rectangles whose area
    is proportional to the metric value.

    Args:
        title: Chart title
        dataset_id: ID of the dataset to use
        metric: Metric for rectangle area sizing
        dimensions: Dimension columns for hierarchy levels
        time_range: Time filter

    Returns the created chart information.
    """
    ctx = get_tool_context()
    result = await chart_ops.create_treemap_chart(
        ctx.charts, title, dataset_id, metric, dimensions, time_range,
    )
    ctx.session.add_asset("chart", result["id"], result["title"])
    return result


@tool
async def create_histogram_chart(
    title: str,
    dataset_id: int,
    column: str,
    dimensions: list[str] | None = None,
    num_bins: int = 10,
    normalized: bool = False,
    time_range: str = "No filter",
) -> dict[str, Any]:
    """
    Create a histogram visualization.

    Histograms show the distribution of a single numeric column.

    Args:
        title: Chart title
        dataset_id: ID of the dataset to use
        column: Numeric column whose distribution to plot
        dimensions: Optional grouping for overlaid histograms
        num_bins: Number of bins
        normalized: Whether to normalize the histogram
        time_range: Time filter

    Returns the created chart information.
    """
    ctx = get_tool_context()
    result = await chart_ops.create_histogram_chart(
        ctx.charts, title, dataset_id, column, dimensions,
        num_bins, normalized, time_range,
    )
    ctx.session.add_asset("chart", result["id"], result["title"])
    return result


@tool
async def create_box_plot_chart(
    title: str,
    dataset_id: int,
    metrics: list[str],
    dimensions: list[str],
    time_range: str = "No filter",
    whisker_options: str = "Tukey",
) -> dict[str, Any]:
    """
    Create a box plot visualization.

    Box plots display the statistical distribution (median, quartiles,
    outliers) of one or more metrics, grouped by dimensions.

    Args:
        title: Chart title
        dataset_id: ID of the dataset to use
        metrics: Metric expressions to plot
        dimensions: Dimension columns for grouping
        time_range: Time filter
        whisker_options: Whisker calculation method (Tukey, Min/max, etc.)

    Returns the created chart information.
    """
    ctx = get_tool_context()
    result = await chart_ops.create_box_plot_chart(
        ctx.charts, title, dataset_id, metrics, dimensions,
        time_range, whisker_options,
    )
    ctx.session.add_asset("chart", result["id"], result["title"])
    return result


@tool
async def create_heatmap_chart(
    title: str,
    dataset_id: int,
    metric: str,
    x_column: str,
    y_column: str,
    time_range: str = "No filter",
    linear_color_scheme: str = "blue_white_yellow",
    normalize_across: str | None = None,
    show_values: bool = False,
) -> dict[str, Any]:
    """
    Create a heatmap visualization.

    Heatmaps display a 2D grid coloured by a metric value at each
    (x, y) intersection.

    Args:
        title: Chart title
        dataset_id: ID of the dataset to use
        metric: Metric for cell colour intensity
        x_column: Column for x-axis
        y_column: Column for y-axis
        time_range: Time filter
        linear_color_scheme: Colour scheme name
        normalize_across: Normalisation axis (None, "heatmap", "x", "y")
        show_values: Whether to display values in cells

    Returns the created chart information.
    """
    ctx = get_tool_context()
    result = await chart_ops.create_heatmap_chart(
        ctx.charts, title, dataset_id, metric, x_column, y_column,
        time_range, linear_color_scheme, normalize_across, show_values,
    )
    ctx.session.add_asset("chart", result["id"], result["title"])
    return result


# =============================================================================
# Chart Management Tools
# =============================================================================


@tool
async def list_all_charts() -> list[dict[str, Any]]:
    """
    List all charts in Superset.

    Returns a list of all charts with their IDs, titles, and types.
    Use this to find charts that need to be modified or deleted.
    """
    ctx = get_tool_context()
    return await chart_ops.list_all_charts(ctx.charts)


@tool
async def get_chart(chart_id: int) -> dict[str, Any]:
    """
    Get detailed information about a single chart.

    Args:
        chart_id: The ID of the chart

    Returns chart details including title, type, datasource, params,
    and associated dashboards.
    """
    ctx = get_tool_context()
    return await chart_ops.get_chart(ctx.charts, chart_id)


@tool
async def update_chart(
    chart_id: int,
    title: str | None = None,
    description: str | None = None,
    cache_timeout: int | None = None,
    owners: list[int] | None = None,
    dashboards: list[int] | None = None,
) -> dict[str, Any]:
    """
    Update an existing chart's metadata.

    Only the provided fields are updated; omitted fields are left unchanged.

    Args:
        chart_id: The ID of the chart to update
        title: New chart title
        description: New chart description
        cache_timeout: Cache timeout in seconds
        owners: List of owner user IDs
        dashboards: List of dashboard IDs to associate

    Returns updated chart information.
    """
    ctx = get_tool_context()
    result = await chart_ops.update_chart(
        ctx.charts, chart_id,
        title=title,
        description=description,
        cache_timeout=cache_timeout,
        owners=owners,
        dashboards=dashboards,
    )
    if title:
        ctx.session.add_asset("chart", result["id"], result["title"])
    return result


@tool
async def delete_chart(chart_id: int) -> dict[str, Any]:
    """
    Delete a chart from Superset.

    Args:
        chart_id: The ID of the chart to delete

    Returns confirmation of deletion or error details.
    """
    ctx = get_tool_context()
    return await chart_ops.delete_chart(ctx.charts, chart_id)


# =============================================================================
# Dashboard Tools
# =============================================================================


@tool
async def list_all_dashboards() -> list[dict[str, Any]]:
    """
    List all dashboards in Superset.

    Returns a list of all dashboards with their IDs and titles.
    Use this to find dashboards that need to be modified or deleted.
    """
    ctx = get_tool_context()
    return await dashboard_ops.list_all_dashboards(ctx.dashboards)


@tool
async def get_dashboard(dashboard_id: int) -> dict[str, Any]:
    """
    Get detailed information about a single dashboard.

    Args:
        dashboard_id: The ID of the dashboard

    Returns dashboard details including title, charts, published status,
    and layout information.
    """
    ctx = get_tool_context()
    return await dashboard_ops.get_dashboard(ctx.dashboards, dashboard_id)


@tool
async def update_dashboard(
    dashboard_id: int,
    title: str | None = None,
    slug: str | None = None,
    css: str | None = None,
    published: bool | None = None,
    owners: list[int] | None = None,
    color_scheme: str | None = None,
) -> dict[str, Any]:
    """
    Update an existing dashboard's metadata.

    Only the provided fields are updated; omitted fields are left unchanged.

    Args:
        dashboard_id: The ID of the dashboard to update
        title: New dashboard title
        slug: New URL-friendly slug
        css: Custom CSS
        published: Whether the dashboard is published
        owners: List of owner user IDs
        color_scheme: Color scheme name. Options: supersetColors,
            d3Category10, d3Category20, d3Category20b, d3Category20c,
            googleCategory10c, googleCategory20c, bnbColors, lyftColors,
            echarts4Colors, echarts5Colors, presetColors.

    Returns updated dashboard information.
    """
    ctx = get_tool_context()
    result = await dashboard_ops.update_dashboard(
        ctx.dashboards, dashboard_id,
        title=title,
        slug=slug,
        css=css,
        published=published,
        owners=owners,
        color_scheme=color_scheme,
    )
    if title:
        ctx.session.add_asset("dashboard", result["id"], result["title"])
    return result


@tool
async def remove_chart_from_dashboard(
    dashboard_id: int,
    chart_id: int,
) -> dict[str, Any]:
    """
    Remove a chart from a dashboard.

    The chart is NOT deleted — it is only removed from the dashboard layout.

    Args:
        dashboard_id: ID of the dashboard to update
        chart_id: ID of the chart to remove

    Returns updated dashboard information.
    """
    ctx = get_tool_context()
    return await dashboard_ops.remove_chart_from_dashboard(
        ctx.dashboards, dashboard_id, chart_id
    )


@tool
async def delete_dashboard(dashboard_id: int) -> dict[str, Any]:
    """
    Delete a dashboard from Superset.

    Note: This only deletes the dashboard, not the charts it contains.
    Charts will remain and can be reused in other dashboards.

    Args:
        dashboard_id: The ID of the dashboard to delete

    Returns confirmation of deletion or error details.
    """
    ctx = get_tool_context()
    return await dashboard_ops.delete_dashboard(ctx.dashboards, dashboard_id)


@tool
async def delete_all_charts_and_dashboards() -> dict[str, Any]:
    """
    Delete ALL charts and dashboards from Superset.

    This tool deletes dashboards FIRST (to remove chart associations),
    then deletes all charts. Use with caution - this is destructive!

    Returns a summary of what was deleted.
    """
    ctx = get_tool_context()
    return await dashboard_ops.delete_all_charts_and_dashboards(
        ctx.charts, ctx.dashboards
    )


@tool
async def create_dashboard(
    title: str,
    chart_ids: list[int],
    layout: Literal["vertical", "grid"] = "vertical",
    color_scheme: str = "supersetColors",
) -> dict[str, Any]:
    """
    Create a dashboard containing multiple charts.

    Args:
        title: Dashboard title
        chart_ids: List of chart IDs to include
        layout: Layout type ("vertical" or "grid")
        color_scheme: Color scheme name. Options: supersetColors (default),
            d3Category10, d3Category20, d3Category20b, d3Category20c,
            googleCategory10c, googleCategory20c, bnbColors, lyftColors,
            echarts4Colors, echarts5Colors, presetColors.

    Returns the created dashboard information with URL.
    """
    ctx = get_tool_context()
    result = await dashboard_ops.create_dashboard(
        ctx.dashboards, title, chart_ids, layout, color_scheme
    )
    ctx.session.add_asset("dashboard", result["id"], result["title"])
    ctx.session.active_dashboard_id = result["id"]
    ctx.session.active_dashboard_title = result["title"]
    return result


@tool
async def create_tabbed_dashboard(
    title: str,
    tabs: dict[str, list[int]],
    color_scheme: str = "supersetColors",
) -> dict[str, Any]:
    """
    Create a dashboard with a tabbed layout.

    Charts are organized into named tabs. Each tab shows its charts
    stacked vertically.

    Args:
        title: Dashboard title
        tabs: Mapping of tab label to list of chart IDs.
              Example: {"Overview": [1, 2], "Details": [3, 4, 5]}
        color_scheme: Color scheme name. Options: supersetColors (default),
            d3Category10, d3Category20, d3Category20b, d3Category20c,
            googleCategory10c, googleCategory20c, bnbColors, lyftColors,
            echarts4Colors, echarts5Colors, presetColors.

    Returns the created dashboard information with URL.
    """
    ctx = get_tool_context()
    result = await dashboard_ops.create_tabbed_dashboard(
        ctx.dashboards, title, tabs, color_scheme
    )
    ctx.session.add_asset("dashboard", result["id"], result["title"])
    ctx.session.active_dashboard_id = result["id"]
    ctx.session.active_dashboard_title = result["title"]
    return result


@tool
async def add_chart_to_dashboard(
    dashboard_id: int,
    chart_ids: list[int],
    tab_label: str | None = None,
) -> dict[str, Any]:
    """
    Add charts to an existing dashboard.

    If the dashboard uses tabs, the charts are added to the specified tab.
    If no tab_label is provided, charts are added to the first tab.
    If the dashboard has no tabs, charts are appended as new rows.

    Args:
        dashboard_id: ID of the dashboard to update
        chart_ids: List of chart IDs to add
        tab_label: Optional tab label to add the charts to. If the tab
            doesn't exist, a new tab is created with this name.

    Returns updated dashboard information.
    """
    ctx = get_tool_context()
    return await dashboard_ops.add_chart_to_dashboard(
        ctx.dashboards, dashboard_id, chart_ids, tab_label=tab_label
    )


# =============================================================================
# Dashboard Filter Tools
# =============================================================================


@tool
async def add_filter_to_dashboard(
    dashboard_id: int,
    name: str,
    filter_type: str = "filter_select",
    dataset_id: int | None = None,
    column: str | None = None,
    exclude_chart_ids: list[int] | None = None,
    multi_select: bool = True,
    default_to_first_item: bool = False,
    description: str = "",
) -> dict[str, Any]:
    """
    Add a native filter to a dashboard.

    Filters let dashboard viewers interactively filter the displayed data.

    Args:
        dashboard_id: ID of the dashboard
        name: Display name for the filter
        filter_type: One of "filter_select" (dropdown), "filter_range"
            (numeric slider), "filter_time" (time range picker),
            "filter_timecolumn" (temporal column selector),
            "filter_timegrain" (time grain selector)
        dataset_id: Dataset ID. Required for all types except "filter_time"
        column: Column name. Required for all types except "filter_time"
        exclude_chart_ids: Chart IDs to exclude from the filter scope
        multi_select: Allow multiple values (filter_select only)
        default_to_first_item: Pre-select the first value
        description: Optional description

    Returns the filter ID and confirmation message.
    """
    ctx = get_tool_context()
    return await dashboard_ops.add_filter_to_dashboard(
        ctx.dashboards, dashboard_id,
        name=name,
        filter_type=filter_type,
        dataset_id=dataset_id,
        column=column,
        exclude_chart_ids=exclude_chart_ids,
        multi_select=multi_select,
        default_to_first_item=default_to_first_item,
        description=description,
    )


@tool
async def remove_filter_from_dashboard(
    dashboard_id: int,
    filter_id: str,
) -> dict[str, Any]:
    """
    Remove a native filter from a dashboard.

    Args:
        dashboard_id: ID of the dashboard
        filter_id: The filter ID (e.g. "NATIVE_FILTER-abc12345").
            Use list_dashboard_filters to find filter IDs.

    Returns confirmation of removal.
    """
    ctx = get_tool_context()
    return await dashboard_ops.remove_filter_from_dashboard(
        ctx.dashboards, dashboard_id, filter_id
    )


@tool
async def list_dashboard_filters(
    dashboard_id: int,
) -> list[dict[str, Any]]:
    """
    List all native filters on a dashboard.

    Args:
        dashboard_id: ID of the dashboard

    Returns a list of filters with IDs, names, types, columns, and dataset IDs.
    """
    ctx = get_tool_context()
    return await dashboard_ops.list_dashboard_filters(ctx.dashboards, dashboard_id)


# =============================================================================
# Tool Registry
# =============================================================================

ALL_TOOLS = [
    # Discovery
    list_databases,
    list_schemas,
    list_tables,
    get_dataset_columns,
    list_existing_datasets,
    execute_sql,
    profile_dataset,
    # Datasets
    find_or_create_dataset,
    # Charts — original types
    list_all_charts,
    get_chart,
    create_bar_chart,
    create_line_chart,
    create_pie_chart,
    create_table_chart,
    create_metric_chart,
    # Charts — new types
    create_area_chart,
    create_big_number_trendline_chart,
    create_timeseries_bar_chart,
    create_bubble_chart,
    create_funnel_chart,
    create_gauge_chart,
    create_treemap_chart,
    create_histogram_chart,
    create_box_plot_chart,
    create_heatmap_chart,
    update_chart,
    delete_chart,
    # Dashboards
    list_all_dashboards,
    get_dashboard,
    create_dashboard,
    create_tabbed_dashboard,
    add_chart_to_dashboard,
    remove_chart_from_dashboard,
    update_dashboard,
    delete_dashboard,
    # Dashboard filters
    add_filter_to_dashboard,
    remove_filter_from_dashboard,
    list_dashboard_filters,
    # Bulk operations
    delete_all_charts_and_dashboards,
]
