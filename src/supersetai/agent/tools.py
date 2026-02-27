"""Agent tools for Superset operations."""

import logging
from typing import Any, Literal

from langchain_core.tools import tool

from supersetai.schemas.charts import CHART_TYPE_MAP

logger = logging.getLogger(__name__)

# Global context holder - will be set by the agent
_tool_context = None


def set_tool_context(context: Any) -> None:
    """Set the tool context for the current execution."""
    global _tool_context
    _tool_context = context


def get_tool_context() -> Any:
    """Get the current tool context."""
    if _tool_context is None:
        raise RuntimeError("Tool context not set. Call set_tool_context first.")
    return _tool_context


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
    databases = await ctx.databases.list_databases()
    
    result = [
        {
            "id": db.id,
            "name": db.database_name,
            "backend": db.backend,
        }
        for db in databases
    ]
    
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
    schemas = await ctx.databases.list_schemas(database_id)
    return schemas


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
    tables = await ctx.databases.list_tables(database_id, schema=schema_name)
    
    result = [
        {
            "name": t.name,
            "schema": t.schema_,
            "type": t.type,
        }
        for t in tables
    ]
    
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
    dataset = await ctx.datasets.get_dataset(dataset_id)
    
    columns = []
    time_columns = []
    numeric_columns = []
    
    for col in dataset.columns:
        col_info = {
            "name": col.column_name,
            "type": col.type,
            "is_time": col.is_dttm,
        }
        columns.append(col_info)
        
        if col.is_dttm:
            time_columns.append(col.column_name)
        
        # Check for numeric types
        if col.type_generic in (0, 1):  # INT, FLOAT
            numeric_columns.append(col.column_name)
    
    # Cache
    ctx.session.superset_context.discovered_columns[dataset_id] = [
        c["name"] for c in columns
    ]
    
    return {
        "dataset_id": dataset_id,
        "table_name": dataset.table_name,
        "columns": columns,
        "time_columns": time_columns,
        "numeric_columns": numeric_columns,
    }


@tool
async def list_existing_datasets(database_id: int | None = None) -> list[dict[str, Any]]:
    """
    List existing datasets in Superset.
    
    Args:
        database_id: Optional database ID to filter by
    
    Returns list of datasets that can be reused for charts.
    """
    ctx = get_tool_context()
    datasets = await ctx.datasets.list_datasets(database_id=database_id)
    
    return [
        {
            "id": ds.id,
            "table_name": ds.table_name,
            "database_id": ds.database_id,
            "schema": ds.schema_,
        }
        for ds in datasets
    ]


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
    
    dataset = await ctx.datasets.find_or_create(
        table_name=table_name,
        database_id=database_id,
        schema=schema_name,
    )
    
    # Record if newly created
    ctx.session.add_asset("dataset", dataset.id, dataset.table_name)
    
    return {
        "id": dataset.id,
        "table_name": dataset.table_name,
        "columns": [c.column_name for c in dataset.columns],
        "time_columns": [c.column_name for c in dataset.columns if c.is_dttm],
    }


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
    
    chart = await ctx.charts.create_bar_chart(
        title=title,
        datasource_id=dataset_id,
        metrics=metrics,
        groupby=dimensions,
        time_range=time_range,
    )
    
    ctx.session.add_asset("chart", chart.id, chart.slice_name)
    
    return {
        "id": chart.id,
        "title": chart.slice_name,
        "type": chart.viz_type,
        "url": f"/explore/?slice_id={chart.id}",
    }


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
    
    chart = await ctx.charts.create_line_chart(
        title=title,
        datasource_id=dataset_id,
        metrics=metrics,
        time_column=time_column,
        groupby=dimensions,
        time_grain=time_grain,
        time_range=time_range,
    )
    
    ctx.session.add_asset("chart", chart.id, chart.slice_name)
    
    return {
        "id": chart.id,
        "title": chart.slice_name,
        "type": chart.viz_type,
        "url": f"/explore/?slice_id={chart.id}",
    }


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
    
    chart = await ctx.charts.create_pie_chart(
        title=title,
        datasource_id=dataset_id,
        metric=metric,
        groupby=dimension,
        time_range=time_range,
    )
    
    ctx.session.add_asset("chart", chart.id, chart.slice_name)
    
    return {
        "id": chart.id,
        "title": chart.slice_name,
        "type": chart.viz_type,
        "url": f"/explore/?slice_id={chart.id}",
    }


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
    
    chart = await ctx.charts.create_table(
        title=title,
        datasource_id=dataset_id,
        columns=columns,
        metrics=metrics,
        groupby=dimensions,
        row_limit=row_limit,
    )
    
    ctx.session.add_asset("chart", chart.id, chart.slice_name)
    
    return {
        "id": chart.id,
        "title": chart.slice_name,
        "type": chart.viz_type,
        "url": f"/explore/?slice_id={chart.id}",
    }


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
    
    chart = await ctx.charts.create_big_number(
        title=title,
        datasource_id=dataset_id,
        metric=metric,
        time_range=time_range,
    )
    
    ctx.session.add_asset("chart", chart.id, chart.slice_name)
    
    return {
        "id": chart.id,
        "title": chart.slice_name,
        "type": chart.viz_type,
        "url": f"/explore/?slice_id={chart.id}",
    }


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
    charts = await ctx.charts.list_charts()
    
    return [
        {
            "id": chart.id,
            "title": chart.slice_name,
            "type": chart.viz_type,
        }
        for chart in charts
    ]


@tool
async def delete_chart(chart_id: int) -> dict[str, Any]:
    """
    Delete a chart from Superset.
    
    Args:
        chart_id: The ID of the chart to delete
    
    Returns confirmation of deletion or error details.
    """
    ctx = get_tool_context()
    
    # Get chart info before deleting for confirmation message
    try:
        chart = await ctx.charts.get_chart(chart_id)
        chart_name = chart.slice_name
    except Exception:
        chart_name = f"Chart {chart_id}"
    
    try:
        await ctx.charts.delete_chart(chart_id)
        return {
            "deleted": True,
            "chart_id": chart_id,
            "chart_name": chart_name,
            "message": f"Successfully deleted chart '{chart_name}' (ID: {chart_id})",
        }
    except Exception as e:
        return {
            "deleted": False,
            "chart_id": chart_id,
            "chart_name": chart_name,
            "error": str(e),
            "message": f"Failed to delete chart '{chart_name}' (ID: {chart_id}): {e}",
        }


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
    dashboards = await ctx.dashboards.list_dashboards()
    
    return [
        {
            "id": dashboard.id,
            "title": dashboard.dashboard_title,
            "published": dashboard.published,
        }
        for dashboard in dashboards
    ]


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
    
    # Get dashboard info before deleting for confirmation message
    try:
        dashboard = await ctx.dashboards.get_dashboard(dashboard_id)
        dashboard_name = dashboard.dashboard_title
    except Exception:
        dashboard_name = f"Dashboard {dashboard_id}"
    
    try:
        await ctx.dashboards.delete_dashboard(dashboard_id)
        return {
            "deleted": True,
            "dashboard_id": dashboard_id,
            "dashboard_name": dashboard_name,
            "message": f"Successfully deleted dashboard '{dashboard_name}' (ID: {dashboard_id})",
        }
    except Exception as e:
        return {
            "deleted": False,
            "dashboard_id": dashboard_id,
            "dashboard_name": dashboard_name,
            "error": str(e),
            "message": f"Failed to delete dashboard '{dashboard_name}' (ID: {dashboard_id}): {e}",
        }


@tool
async def delete_all_charts_and_dashboards() -> dict[str, Any]:
    """
    Delete ALL charts and dashboards from Superset.
    
    This tool deletes dashboards FIRST (to remove chart associations),
    then deletes all charts. Use with caution - this is destructive!
    
    Returns a summary of what was deleted.
    """
    ctx = get_tool_context()
    
    results = {
        "dashboards_deleted": [],
        "dashboards_failed": [],
        "charts_deleted": [],
        "charts_failed": [],
    }
    
    # Step 1: Delete all dashboards first (to free up chart associations)
    dashboards = await ctx.dashboards.list_dashboards()
    for dashboard in dashboards:
        try:
            await ctx.dashboards.delete_dashboard(dashboard.id)
            results["dashboards_deleted"].append({
                "id": dashboard.id,
                "title": dashboard.dashboard_title,
            })
        except Exception as e:
            results["dashboards_failed"].append({
                "id": dashboard.id,
                "title": dashboard.dashboard_title,
                "error": str(e),
            })
    
    # Step 2: Delete all charts
    charts = await ctx.charts.list_charts()
    for chart in charts:
        try:
            await ctx.charts.delete_chart(chart.id)
            results["charts_deleted"].append({
                "id": chart.id,
                "title": chart.slice_name,
            })
        except Exception as e:
            results["charts_failed"].append({
                "id": chart.id,
                "title": chart.slice_name,
                "error": str(e),
            })
    
    return {
        "success": len(results["dashboards_failed"]) == 0 and len(results["charts_failed"]) == 0,
        "dashboards_deleted_count": len(results["dashboards_deleted"]),
        "charts_deleted_count": len(results["charts_deleted"]),
        "dashboards_failed_count": len(results["dashboards_failed"]),
        "charts_failed_count": len(results["charts_failed"]),
        "details": results,
        "message": f"Deleted {len(results['dashboards_deleted'])} dashboards and {len(results['charts_deleted'])} charts.",
    }


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
    
    dashboard = await ctx.dashboards.create_dashboard_with_charts(
        title=title,
        chart_ids=chart_ids,
        layout=layout,
    )
    
    ctx.session.add_asset("dashboard", dashboard.id, dashboard.dashboard_title)
    ctx.session.active_dashboard_id = dashboard.id
    ctx.session.active_dashboard_title = dashboard.dashboard_title
    
    return {
        "id": dashboard.id,
        "title": dashboard.dashboard_title,
        "url": f"/superset/dashboard/{dashboard.id}/",
        "charts_included": chart_ids,
    }


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
    
    dashboard = await ctx.dashboards.add_charts_to_dashboard(
        dashboard_id=dashboard_id,
        chart_ids=chart_ids,
    )
    
    return {
        "id": dashboard.id,
        "title": dashboard.dashboard_title,
        "url": f"/superset/dashboard/{dashboard.id}/",
        "message": f"Added {len(chart_ids)} chart(s) to dashboard",
    }


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
