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
) -> dict[str, Any]:
    """
    Create a dashboard containing multiple charts.

    Args:
        title: Dashboard title
        chart_ids: List of chart IDs to include
        layout: Layout type ("vertical" or "grid")

    Returns the created dashboard information with URL.
    """
    ctx = get_tool_context()
    result = await dashboard_ops.create_dashboard(
        ctx.dashboards, title, chart_ids, layout
    )
    ctx.session.add_asset("dashboard", result["id"], result["title"])
    ctx.session.active_dashboard_id = result["id"]
    ctx.session.active_dashboard_title = result["title"]
    return result


@tool
async def add_chart_to_dashboard(
    dashboard_id: int,
    chart_ids: list[int],
) -> dict[str, Any]:
    """
    Add charts to an existing dashboard.

    Args:
        dashboard_id: ID of the dashboard to update
        chart_ids: List of chart IDs to add

    Returns updated dashboard information.
    """
    ctx = get_tool_context()
    return await dashboard_ops.add_chart_to_dashboard(
        ctx.dashboards, dashboard_id, chart_ids
    )


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
    # Datasets
    find_or_create_dataset,
    # Charts
    list_all_charts,
    create_bar_chart,
    create_line_chart,
    create_pie_chart,
    create_table_chart,
    create_metric_chart,
    delete_chart,
    # Dashboards
    list_all_dashboards,
    create_dashboard,
    add_chart_to_dashboard,
    delete_dashboard,
    # Bulk operations
    delete_all_charts_and_dashboards,
]
