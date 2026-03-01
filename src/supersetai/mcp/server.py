"""MCP server exposing Superset operations as tools.

This module creates a FastMCP server that wraps the supersetai API client,
making all Superset operations available via the Model Context Protocol.

The server manages a single SupersetClient instance per session, lazily
initialized on the first tool call. Configuration is read from environment
variables with the SUPERSETAI_ prefix (see supersetai.core.config).

Tools are organized into categories:
- Discovery: list databases, schemas, tables, datasets, columns
- Datasets: find or create datasets
- Charts: create bar, line, pie, table, and metric charts; list/delete charts
- Dashboards: create dashboards, add charts, list/delete dashboards
- Bulk: delete all charts and dashboards
"""

from __future__ import annotations

import logging
from typing import Any, Literal

from fastmcp import FastMCP

logger = logging.getLogger(__name__)

mcp = FastMCP(
    name="SupersetAI",
    instructions=(
        "You are a Superset dashboard assistant. Use these tools to explore "
        "databases, create datasets, build charts, and assemble dashboards. "
        "Start by listing databases and datasets to understand what data is "
        "available before creating visualizations."
    ),
)

# ---------------------------------------------------------------------------
# Lazy client management
# ---------------------------------------------------------------------------

_client = None
_chart_svc = None
_dashboard_svc = None
_dataset_svc = None
_database_svc = None


async def _get_services():
    """Lazily initialize SupersetClient and all service instances."""
    global _client, _chart_svc, _dashboard_svc, _dataset_svc, _database_svc

    if _client is None:
        from supersetai.api.charts import ChartService
        from supersetai.api.client import SupersetClient
        from supersetai.api.dashboards import DashboardService
        from supersetai.api.databases import DatabaseService
        from supersetai.api.datasets import DatasetService
        from supersetai.core.config import SupersetConfig

        config = SupersetConfig()
        _client = SupersetClient(config)
        # Authenticate eagerly so first tool call doesn't silently fail
        await _client.auth.get_valid_session()

        _chart_svc = ChartService(_client)
        _dashboard_svc = DashboardService(_client)
        _dataset_svc = DatasetService(_client)
        _database_svc = DatabaseService(_client)

        logger.info(
            "SupersetAI MCP: connected to %s", config.superset_base_url
        )

    return _chart_svc, _dashboard_svc, _dataset_svc, _database_svc


# ---------------------------------------------------------------------------
# Discovery tools
# ---------------------------------------------------------------------------


@mcp.tool(
    annotations={"readOnlyHint": True},
    tags={"discovery"},
)
async def list_databases() -> list[dict[str, Any]]:
    """List all available database connections in Superset.

    Returns a list of databases with their IDs, names, and backends.
    Use this first to discover what data sources are available.
    """
    _, _, _, db_svc = await _get_services()
    databases = await db_svc.list_databases()
    return [
        {"id": db.id, "name": db.database_name, "backend": db.backend}
        for db in databases
    ]


@mcp.tool(
    annotations={"readOnlyHint": True},
    tags={"discovery"},
)
async def list_schemas(database_id: int) -> list[str]:
    """List all schemas in a specific database.

    Args:
        database_id: The ID of the database to list schemas from.

    Returns a list of schema names (e.g. "public", "main").
    """
    _, _, _, db_svc = await _get_services()
    return await db_svc.list_schemas(database_id)


@mcp.tool(
    annotations={"readOnlyHint": True},
    tags={"discovery"},
)
async def list_tables(
    database_id: int,
    schema_name: str | None = None,
) -> list[dict[str, Any]]:
    """List all tables in a specific database schema.

    Args:
        database_id: The ID of the database.
        schema_name: Schema to list tables from. If omitted, uses the first
            available schema.
    """
    _, _, _, db_svc = await _get_services()
    tables = await db_svc.list_tables(database_id, schema=schema_name)
    return [
        {"name": t.name, "schema": t.schema_, "type": t.type}
        for t in tables
    ]


@mcp.tool(
    annotations={"readOnlyHint": True},
    tags={"discovery"},
)
async def get_dataset_columns(dataset_id: int) -> dict[str, Any]:
    """Get column information for a dataset.

    Args:
        dataset_id: The ID of the dataset.

    Returns column names, types, and which are suitable for time axes or
    numeric metrics.
    """
    _, _, ds_svc, _ = await _get_services()
    dataset = await ds_svc.get_dataset(dataset_id)

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
        if col.type_generic in (0, 1):  # INT, FLOAT
            numeric_columns.append(col.column_name)

    return {
        "dataset_id": dataset_id,
        "table_name": dataset.table_name,
        "columns": columns,
        "time_columns": time_columns,
        "numeric_columns": numeric_columns,
    }


@mcp.tool(
    annotations={"readOnlyHint": True},
    tags={"discovery"},
)
async def list_existing_datasets(
    database_id: int | None = None,
) -> list[dict[str, Any]]:
    """List existing datasets (registered tables) in Superset.

    Args:
        database_id: Optional database ID to filter by.
    """
    _, _, ds_svc, _ = await _get_services()
    datasets = await ds_svc.list_datasets(database_id=database_id)
    return [
        {
            "id": ds.id,
            "table_name": ds.table_name,
            "database_id": ds.database_id,
            "schema": ds.schema_,
        }
        for ds in datasets
    ]


# ---------------------------------------------------------------------------
# Dataset tools
# ---------------------------------------------------------------------------


@mcp.tool(tags={"datasets"})
async def find_or_create_dataset(
    database_id: int,
    table_name: str,
    schema_name: str | None = None,
) -> dict[str, Any]:
    """Find an existing dataset or create a new one for a table.

    Args:
        database_id: The database ID containing the table.
        table_name: Name of the table to register.
        schema_name: Optional database schema name.

    Returns the dataset ID, table name, and available columns.
    """
    _, _, ds_svc, _ = await _get_services()
    dataset = await ds_svc.find_or_create(
        table_name=table_name,
        database_id=database_id,
        schema=schema_name,
    )
    return {
        "id": dataset.id,
        "table_name": dataset.table_name,
        "columns": [c.column_name for c in dataset.columns],
        "time_columns": [
            c.column_name for c in dataset.columns if c.is_dttm
        ],
    }


# ---------------------------------------------------------------------------
# Chart creation tools
# ---------------------------------------------------------------------------


@mcp.tool(tags={"charts"})
async def create_bar_chart(
    title: str,
    dataset_id: int,
    metrics: list[str],
    dimensions: list[str],
    time_range: str = "No filter",
) -> dict[str, Any]:
    """Create a bar chart visualization.

    Args:
        title: Chart title.
        dataset_id: ID of the dataset to use.
        metrics: List of metrics (e.g. ["COUNT(*)", "SUM(amount)"]).
        dimensions: List of dimension columns to group by.
        time_range: Time filter (e.g. "Last 7 days", "No filter").
    """
    chart_svc, _, _, _ = await _get_services()
    chart = await chart_svc.create_bar_chart(
        title=title,
        datasource_id=dataset_id,
        metrics=metrics,
        groupby=dimensions,
        time_range=time_range,
    )
    return {
        "id": chart.id,
        "title": chart.slice_name,
        "type": chart.viz_type,
        "url": f"/explore/?slice_id={chart.id}",
    }


@mcp.tool(tags={"charts"})
async def create_line_chart(
    title: str,
    dataset_id: int,
    metrics: list[str],
    time_column: str,
    dimensions: list[str] | None = None,
    time_grain: str = "P1D",
    time_range: str = "Last 30 days",
) -> dict[str, Any]:
    """Create a line / timeseries chart.

    Args:
        title: Chart title.
        dataset_id: ID of the dataset to use.
        metrics: List of metrics to plot.
        time_column: Column to use for the x-axis.
        dimensions: Optional grouping columns for multiple lines.
        time_grain: Time granularity (P1D=daily, P1W=weekly, P1M=monthly).
        time_range: Time filter.
    """
    chart_svc, _, _, _ = await _get_services()
    chart = await chart_svc.create_line_chart(
        title=title,
        datasource_id=dataset_id,
        metrics=metrics,
        time_column=time_column,
        groupby=dimensions,
        time_grain=time_grain,
        time_range=time_range,
    )
    return {
        "id": chart.id,
        "title": chart.slice_name,
        "type": chart.viz_type,
        "url": f"/explore/?slice_id={chart.id}",
    }


@mcp.tool(tags={"charts"})
async def create_pie_chart(
    title: str,
    dataset_id: int,
    metric: str,
    dimension: str,
    time_range: str = "No filter",
) -> dict[str, Any]:
    """Create a pie chart visualization.

    Args:
        title: Chart title.
        dataset_id: ID of the dataset to use.
        metric: Single metric for slice sizes (e.g. "COUNT(*)").
        dimension: Column for pie slices.
        time_range: Time filter.
    """
    chart_svc, _, _, _ = await _get_services()
    chart = await chart_svc.create_pie_chart(
        title=title,
        datasource_id=dataset_id,
        metric=metric,
        groupby=dimension,
        time_range=time_range,
    )
    return {
        "id": chart.id,
        "title": chart.slice_name,
        "type": chart.viz_type,
        "url": f"/explore/?slice_id={chart.id}",
    }


@mcp.tool(tags={"charts"})
async def create_table_chart(
    title: str,
    dataset_id: int,
    columns: list[str],
    metrics: list[str] | None = None,
    dimensions: list[str] | None = None,
    row_limit: int = 1000,
) -> dict[str, Any]:
    """Create a table visualization.

    Args:
        title: Chart title.
        dataset_id: ID of the dataset to use.
        columns: Columns to display (for raw data view).
        metrics: Optional metrics for an aggregated table.
        dimensions: Optional grouping for an aggregated table.
        row_limit: Maximum rows to show.
    """
    chart_svc, _, _, _ = await _get_services()
    chart = await chart_svc.create_table(
        title=title,
        datasource_id=dataset_id,
        columns=columns,
        metrics=metrics,
        groupby=dimensions,
        row_limit=row_limit,
    )
    return {
        "id": chart.id,
        "title": chart.slice_name,
        "type": chart.viz_type,
        "url": f"/explore/?slice_id={chart.id}",
    }


@mcp.tool(tags={"charts"})
async def create_metric_chart(
    title: str,
    dataset_id: int,
    metric: str,
    time_range: str = "No filter",
) -> dict[str, Any]:
    """Create a big number / KPI metric visualization.

    Args:
        title: Chart title.
        dataset_id: ID of the dataset to use.
        metric: The metric to display (e.g. "COUNT(*)", "SUM(revenue)").
        time_range: Time filter.
    """
    chart_svc, _, _, _ = await _get_services()
    chart = await chart_svc.create_big_number(
        title=title,
        datasource_id=dataset_id,
        metric=metric,
        time_range=time_range,
    )
    return {
        "id": chart.id,
        "title": chart.slice_name,
        "type": chart.viz_type,
        "url": f"/explore/?slice_id={chart.id}",
    }


# ---------------------------------------------------------------------------
# Chart management tools
# ---------------------------------------------------------------------------


@mcp.tool(
    annotations={"readOnlyHint": True},
    tags={"charts"},
)
async def list_all_charts() -> list[dict[str, Any]]:
    """List all charts in Superset.

    Returns a list of all charts with their IDs, titles, and types.
    """
    chart_svc, _, _, _ = await _get_services()
    charts = await chart_svc.list_charts()
    return [
        {"id": c.id, "title": c.slice_name, "type": c.viz_type}
        for c in charts
    ]


@mcp.tool(
    annotations={"destructiveHint": True},
    tags={"charts"},
)
async def delete_chart(chart_id: int) -> dict[str, Any]:
    """Delete a chart from Superset.

    Args:
        chart_id: The ID of the chart to delete.
    """
    chart_svc, _, _, _ = await _get_services()
    try:
        chart = await chart_svc.get_chart(chart_id)
        chart_name = chart.slice_name
    except Exception:
        chart_name = f"Chart {chart_id}"

    try:
        await chart_svc.delete_chart(chart_id)
        return {
            "deleted": True,
            "chart_id": chart_id,
            "chart_name": chart_name,
            "message": f"Deleted chart '{chart_name}' (ID: {chart_id})",
        }
    except Exception as e:
        return {
            "deleted": False,
            "chart_id": chart_id,
            "error": str(e),
        }


# ---------------------------------------------------------------------------
# Dashboard tools
# ---------------------------------------------------------------------------


@mcp.tool(
    annotations={"readOnlyHint": True},
    tags={"dashboards"},
)
async def list_all_dashboards() -> list[dict[str, Any]]:
    """List all dashboards in Superset.

    Returns a list of all dashboards with their IDs and titles.
    """
    _, dash_svc, _, _ = await _get_services()
    dashboards = await dash_svc.list_dashboards()
    return [
        {
            "id": d.id,
            "title": d.dashboard_title,
            "published": d.published,
        }
        for d in dashboards
    ]


@mcp.tool(tags={"dashboards"})
async def create_dashboard(
    title: str,
    chart_ids: list[int],
    layout: Literal["vertical", "grid"] = "vertical",
) -> dict[str, Any]:
    """Create a dashboard containing multiple charts.

    Args:
        title: Dashboard title.
        chart_ids: List of chart IDs to include.
        layout: Layout type ("vertical" stacks charts, "grid" uses columns).
    """
    _, dash_svc, _, _ = await _get_services()
    dashboard = await dash_svc.create_dashboard_with_charts(
        title=title,
        chart_ids=chart_ids,
        layout=layout,
    )
    return {
        "id": dashboard.id,
        "title": dashboard.dashboard_title,
        "url": f"/superset/dashboard/{dashboard.id}/",
        "charts_included": chart_ids,
    }


@mcp.tool(tags={"dashboards"})
async def add_chart_to_dashboard(
    dashboard_id: int,
    chart_ids: list[int],
) -> dict[str, Any]:
    """Add charts to an existing dashboard.

    Args:
        dashboard_id: ID of the dashboard to update.
        chart_ids: List of chart IDs to add.
    """
    _, dash_svc, _, _ = await _get_services()
    dashboard = await dash_svc.add_charts_to_dashboard(
        dashboard_id=dashboard_id,
        chart_ids=chart_ids,
    )
    return {
        "id": dashboard.id,
        "title": dashboard.dashboard_title,
        "url": f"/superset/dashboard/{dashboard.id}/",
        "message": f"Added {len(chart_ids)} chart(s) to dashboard",
    }


@mcp.tool(
    annotations={"destructiveHint": True},
    tags={"dashboards"},
)
async def delete_dashboard(dashboard_id: int) -> dict[str, Any]:
    """Delete a dashboard from Superset.

    Charts are NOT deleted — only the dashboard container is removed.

    Args:
        dashboard_id: The ID of the dashboard to delete.
    """
    _, dash_svc, _, _ = await _get_services()
    try:
        dashboard = await dash_svc.get_dashboard(dashboard_id)
        dashboard_name = dashboard.dashboard_title
    except Exception:
        dashboard_name = f"Dashboard {dashboard_id}"

    try:
        await dash_svc.delete_dashboard(dashboard_id)
        return {
            "deleted": True,
            "dashboard_id": dashboard_id,
            "dashboard_name": dashboard_name,
            "message": f"Deleted dashboard '{dashboard_name}' (ID: {dashboard_id})",
        }
    except Exception as e:
        return {
            "deleted": False,
            "dashboard_id": dashboard_id,
            "error": str(e),
        }


# ---------------------------------------------------------------------------
# Bulk operations
# ---------------------------------------------------------------------------


@mcp.tool(
    annotations={"destructiveHint": True},
    tags={"bulk"},
)
async def delete_all_charts_and_dashboards() -> dict[str, Any]:
    """Delete ALL charts and dashboards from Superset.

    Dashboards are deleted first (to release chart associations), then all
    charts are deleted. This is destructive and cannot be undone.
    """
    chart_svc, dash_svc, _, _ = await _get_services()

    results: dict[str, list] = {
        "dashboards_deleted": [],
        "dashboards_failed": [],
        "charts_deleted": [],
        "charts_failed": [],
    }

    # 1. Delete dashboards first
    dashboards = await dash_svc.list_dashboards()
    for d in dashboards:
        try:
            await dash_svc.delete_dashboard(d.id)
            results["dashboards_deleted"].append(
                {"id": d.id, "title": d.dashboard_title}
            )
        except Exception as e:
            results["dashboards_failed"].append(
                {"id": d.id, "title": d.dashboard_title, "error": str(e)}
            )

    # 2. Delete charts
    charts = await chart_svc.list_charts()
    for c in charts:
        try:
            await chart_svc.delete_chart(c.id)
            results["charts_deleted"].append(
                {"id": c.id, "title": c.slice_name}
            )
        except Exception as e:
            results["charts_failed"].append(
                {"id": c.id, "title": c.slice_name, "error": str(e)}
            )

    return {
        "success": (
            len(results["dashboards_failed"]) == 0
            and len(results["charts_failed"]) == 0
        ),
        "dashboards_deleted_count": len(results["dashboards_deleted"]),
        "charts_deleted_count": len(results["charts_deleted"]),
        "message": (
            f"Deleted {len(results['dashboards_deleted'])} dashboards "
            f"and {len(results['charts_deleted'])} charts."
        ),
        "details": results,
    }


# ---------------------------------------------------------------------------
# Entry point for `python -m supersetai.mcp.server`
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    mcp.run()
