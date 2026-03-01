"""MCP server exposing Superset operations as tools.

This module creates a FastMCP server that wraps the superset_ai API client,
making all Superset operations available via the Model Context Protocol.

The server manages a single SupersetClient instance per session, lazily
initialized on the first tool call. Configuration is read from environment
variables with the SUPERSET_AI_ prefix (see superset_ai.core.config).

Tools are organized into categories:
- Discovery: list databases, schemas, tables, datasets, columns
- Datasets: find or create datasets
- Charts: create bar, line, pie, table, and metric charts; list/delete charts
- Dashboards: create dashboards, add charts, list/delete dashboards
- Bulk: delete all charts and dashboards
"""

from __future__ import annotations

import functools
import logging
from typing import Any, Literal

from fastmcp import FastMCP

from superset_ai.core.exceptions import (
    AuthenticationError,
    PermissionDeniedError,
    ResourceNotFoundError,
    SupersetAIError,
    ValidationError,
)
from superset_ai.operations import charts as chart_ops
from superset_ai.operations import dashboards as dashboard_ops
from superset_ai.operations import datasets as dataset_ops
from superset_ai.operations import discovery as discovery_ops

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
        from superset_ai.api.charts import ChartService
        from superset_ai.api.client import SupersetClient
        from superset_ai.api.dashboards import DashboardService
        from superset_ai.api.databases import DatabaseService
        from superset_ai.api.datasets import DatasetService
        from superset_ai.core.config import SupersetConfig

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
# Error handling
# ---------------------------------------------------------------------------


def _error_response(error: str, error_type: str, **extra: Any) -> dict[str, Any]:
    """Build a structured error dict for MCP tool responses."""
    result: dict[str, Any] = {"error": error, "error_type": error_type}
    result.update(extra)
    return result


def _handle_errors(fn):
    """Decorator that catches SupersetAI exceptions and returns structured errors.

    MCP tool functions that raise raw exceptions produce unhelpful tracebacks
    for the LLM consumer. This wrapper catches known exception types and
    converts them into structured error dicts with actionable messages.
    """

    @functools.wraps(fn)
    async def wrapper(*args: Any, **kwargs: Any) -> Any:
        try:
            return await fn(*args, **kwargs)
        except ResourceNotFoundError as e:
            return _error_response(
                error=str(e),
                error_type="not_found",
                resource_type=e.resource_type,
                resource_id=e.resource_id,
            )
        except ValidationError as e:
            return _error_response(
                error=str(e),
                error_type="validation_error",
                status_code=e.status_code,
            )
        except AuthenticationError as e:
            return _error_response(
                error=str(e),
                error_type="authentication_error",
                hint="Check SUPERSET_AI_SUPERSET_USERNAME and SUPERSET_AI_SUPERSET_PASSWORD env vars.",
            )
        except PermissionDeniedError as e:
            return _error_response(
                error=str(e),
                error_type="permission_denied",
            )
        except SupersetAIError as e:
            return _error_response(
                error=str(e),
                error_type="superset_error",
            )
        except Exception as e:
            logger.exception("Unexpected error in MCP tool %s", fn.__name__)
            return _error_response(
                error=f"Unexpected error: {e}",
                error_type="internal_error",
            )

    return wrapper


# ---------------------------------------------------------------------------
# Discovery tools
# ---------------------------------------------------------------------------


@mcp.tool(
    annotations={"readOnlyHint": True},
    tags={"discovery"},
)
@_handle_errors
async def list_databases() -> list[dict[str, Any]]:
    """List all available database connections in Superset.

    Returns a list of databases with their IDs, names, and backends.
    Use this first to discover what data sources are available.
    """
    _, _, _, db_svc = await _get_services()
    return await discovery_ops.list_databases(db_svc)


@mcp.tool(
    annotations={"readOnlyHint": True},
    tags={"discovery"},
)
@_handle_errors
async def list_schemas(database_id: int) -> list[str]:
    """List all schemas in a specific database.

    Args:
        database_id: The ID of the database to list schemas from.

    Returns a list of schema names (e.g. "public", "main").
    """
    _, _, _, db_svc = await _get_services()
    return await discovery_ops.list_schemas(db_svc, database_id)


@mcp.tool(
    annotations={"readOnlyHint": True},
    tags={"discovery"},
)
@_handle_errors
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
    return await discovery_ops.list_tables(db_svc, database_id, schema_name)


@mcp.tool(
    annotations={"readOnlyHint": True},
    tags={"discovery"},
)
@_handle_errors
async def get_dataset_columns(dataset_id: int) -> dict[str, Any]:
    """Get column information for a dataset.

    Args:
        dataset_id: The ID of the dataset.

    Returns column names, types, and which are suitable for time axes or
    numeric metrics.
    """
    _, _, ds_svc, _ = await _get_services()
    return await discovery_ops.get_dataset_columns(ds_svc, dataset_id)


@mcp.tool(
    annotations={"readOnlyHint": True},
    tags={"discovery"},
)
@_handle_errors
async def list_existing_datasets(
    database_id: int | None = None,
) -> list[dict[str, Any]]:
    """List existing datasets (registered tables) in Superset.

    Args:
        database_id: Optional database ID to filter by.
    """
    _, _, ds_svc, _ = await _get_services()
    return await discovery_ops.list_existing_datasets(ds_svc, database_id)


# ---------------------------------------------------------------------------
# Dataset tools
# ---------------------------------------------------------------------------


@mcp.tool(tags={"datasets"})
@_handle_errors
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
    return await dataset_ops.find_or_create_dataset(
        ds_svc, database_id, table_name, schema_name
    )


# ---------------------------------------------------------------------------
# Chart creation tools
# ---------------------------------------------------------------------------


@mcp.tool(tags={"charts"})
@_handle_errors
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
    return await chart_ops.create_bar_chart(
        chart_svc, title, dataset_id, metrics, dimensions, time_range
    )


@mcp.tool(tags={"charts"})
@_handle_errors
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
    return await chart_ops.create_line_chart(
        chart_svc, title, dataset_id, metrics, time_column,
        dimensions, time_grain, time_range,
    )


@mcp.tool(tags={"charts"})
@_handle_errors
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
    return await chart_ops.create_pie_chart(
        chart_svc, title, dataset_id, metric, dimension, time_range
    )


@mcp.tool(tags={"charts"})
@_handle_errors
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
    return await chart_ops.create_table_chart(
        chart_svc, title, dataset_id, columns, metrics, dimensions, row_limit
    )


@mcp.tool(tags={"charts"})
@_handle_errors
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
    return await chart_ops.create_metric_chart(
        chart_svc, title, dataset_id, metric, time_range
    )


# ---------------------------------------------------------------------------
# Chart management tools
# ---------------------------------------------------------------------------


@mcp.tool(
    annotations={"readOnlyHint": True},
    tags={"charts"},
)
@_handle_errors
async def list_all_charts() -> list[dict[str, Any]]:
    """List all charts in Superset.

    Returns a list of all charts with their IDs, titles, and types.
    """
    chart_svc, _, _, _ = await _get_services()
    return await chart_ops.list_all_charts(chart_svc)


@mcp.tool(
    annotations={"destructiveHint": True},
    tags={"charts"},
)
@_handle_errors
async def delete_chart(chart_id: int) -> dict[str, Any]:
    """Delete a chart from Superset.

    Args:
        chart_id: The ID of the chart to delete.
    """
    chart_svc, _, _, _ = await _get_services()
    return await chart_ops.delete_chart(chart_svc, chart_id)


# ---------------------------------------------------------------------------
# Dashboard tools
# ---------------------------------------------------------------------------


@mcp.tool(
    annotations={"readOnlyHint": True},
    tags={"dashboards"},
)
@_handle_errors
async def list_all_dashboards() -> list[dict[str, Any]]:
    """List all dashboards in Superset.

    Returns a list of all dashboards with their IDs and titles.
    """
    _, dash_svc, _, _ = await _get_services()
    return await dashboard_ops.list_all_dashboards(dash_svc)


@mcp.tool(tags={"dashboards"})
@_handle_errors
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
    return await dashboard_ops.create_dashboard(
        dash_svc, title, chart_ids, layout
    )


@mcp.tool(tags={"dashboards"})
@_handle_errors
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
    return await dashboard_ops.add_chart_to_dashboard(
        dash_svc, dashboard_id, chart_ids
    )


@mcp.tool(
    annotations={"destructiveHint": True},
    tags={"dashboards"},
)
@_handle_errors
async def delete_dashboard(dashboard_id: int) -> dict[str, Any]:
    """Delete a dashboard from Superset.

    Charts are NOT deleted — only the dashboard container is removed.

    Args:
        dashboard_id: The ID of the dashboard to delete.
    """
    _, dash_svc, _, _ = await _get_services()
    return await dashboard_ops.delete_dashboard(dash_svc, dashboard_id)


# ---------------------------------------------------------------------------
# Bulk operations
# ---------------------------------------------------------------------------


@mcp.tool(
    annotations={"destructiveHint": True},
    tags={"bulk"},
)
@_handle_errors
async def delete_all_charts_and_dashboards() -> dict[str, Any]:
    """Delete ALL charts and dashboards from Superset.

    Dashboards are deleted first (to release chart associations), then all
    charts are deleted. This is destructive and cannot be undone.
    """
    chart_svc, dash_svc, _, _ = await _get_services()
    return await dashboard_ops.delete_all_charts_and_dashboards(
        chart_svc, dash_svc
    )


# ---------------------------------------------------------------------------
# Entry point for `python -m superset_ai.mcp.server`
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    mcp.run()
