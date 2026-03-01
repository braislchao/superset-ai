"""Chart operations — create, list, and delete charts."""

from __future__ import annotations

from typing import Any

from superset_ai.api.charts import ChartService


async def create_bar_chart(
    chart_svc: ChartService,
    title: str,
    dataset_id: int,
    metrics: list[str],
    dimensions: list[str],
    time_range: str = "No filter",
) -> dict[str, Any]:
    """Create a bar chart visualization.

    Returns:
        Dict with ``id``, ``title``, ``type``, and ``url``.
    """
    chart = await chart_svc.create_bar_chart(
        title=title,
        datasource_id=dataset_id,
        metrics=metrics,
        groupby=dimensions,
        time_range=time_range,
    )
    return _chart_result(chart)


async def create_line_chart(
    chart_svc: ChartService,
    title: str,
    dataset_id: int,
    metrics: list[str],
    time_column: str,
    dimensions: list[str] | None = None,
    time_grain: str = "P1D",
    time_range: str = "Last 30 days",
) -> dict[str, Any]:
    """Create a line / timeseries chart.

    Returns:
        Dict with ``id``, ``title``, ``type``, and ``url``.
    """
    chart = await chart_svc.create_line_chart(
        title=title,
        datasource_id=dataset_id,
        metrics=metrics,
        time_column=time_column,
        groupby=dimensions,
        time_grain=time_grain,
        time_range=time_range,
    )
    return _chart_result(chart)


async def create_pie_chart(
    chart_svc: ChartService,
    title: str,
    dataset_id: int,
    metric: str,
    dimension: str,
    time_range: str = "No filter",
) -> dict[str, Any]:
    """Create a pie chart visualization.

    Returns:
        Dict with ``id``, ``title``, ``type``, and ``url``.
    """
    chart = await chart_svc.create_pie_chart(
        title=title,
        datasource_id=dataset_id,
        metric=metric,
        groupby=dimension,
        time_range=time_range,
    )
    return _chart_result(chart)


async def create_table_chart(
    chart_svc: ChartService,
    title: str,
    dataset_id: int,
    columns: list[str],
    metrics: list[str] | None = None,
    dimensions: list[str] | None = None,
    row_limit: int = 1000,
) -> dict[str, Any]:
    """Create a table visualization.

    Returns:
        Dict with ``id``, ``title``, ``type``, and ``url``.
    """
    chart = await chart_svc.create_table(
        title=title,
        datasource_id=dataset_id,
        columns=columns,
        metrics=metrics,
        groupby=dimensions,
        row_limit=row_limit,
    )
    return _chart_result(chart)


async def create_metric_chart(
    chart_svc: ChartService,
    title: str,
    dataset_id: int,
    metric: str,
    time_range: str = "No filter",
) -> dict[str, Any]:
    """Create a big number / KPI metric visualization.

    Returns:
        Dict with ``id``, ``title``, ``type``, and ``url``.
    """
    chart = await chart_svc.create_big_number(
        title=title,
        datasource_id=dataset_id,
        metric=metric,
        time_range=time_range,
    )
    return _chart_result(chart)


async def list_all_charts(chart_svc: ChartService) -> list[dict[str, Any]]:
    """List all charts in Superset.

    Returns:
        List of dicts with ``id``, ``title``, and ``type``.
    """
    charts = await chart_svc.list_charts()
    return [
        {"id": c.id, "title": c.slice_name, "type": c.viz_type}
        for c in charts
    ]


async def delete_chart(
    chart_svc: ChartService,
    chart_id: int,
) -> dict[str, Any]:
    """Delete a chart from Superset.

    Returns:
        Dict with ``deleted`` (bool), ``chart_id``, ``chart_name``, and ``message``.
    """
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
            "chart_name": chart_name,
            "error": str(e),
            "message": f"Failed to delete chart '{chart_name}' (ID: {chart_id}): {e}",
        }


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _chart_result(chart: Any) -> dict[str, Any]:
    """Build a standard chart result dict."""
    return {
        "id": chart.id,
        "title": chart.slice_name,
        "type": chart.viz_type,
        "url": f"/explore/?slice_id={chart.id}",
    }
