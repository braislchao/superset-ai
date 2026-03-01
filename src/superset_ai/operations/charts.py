"""Chart operations — create, list, get, update, and delete charts."""

from __future__ import annotations

from typing import Any

from superset_ai.api.charts import ChartService
from superset_ai.schemas.charts import ChartUpdate


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


async def create_area_chart(
    chart_svc: ChartService,
    title: str,
    dataset_id: int,
    metrics: list[str],
    time_column: str,
    dimensions: list[str] | None = None,
    time_grain: str = "P1D",
    time_range: str = "Last 30 days",
    stacked: bool = True,
) -> dict[str, Any]:
    """Create an area chart (filled line chart).

    Returns:
        Dict with ``id``, ``title``, ``type``, and ``url``.
    """
    chart = await chart_svc.create_area_chart(
        title=title,
        datasource_id=dataset_id,
        metrics=metrics,
        time_column=time_column,
        groupby=dimensions,
        time_grain=time_grain,
        time_range=time_range,
        stacked=stacked,
    )
    return _chart_result(chart)


async def create_big_number_trendline_chart(
    chart_svc: ChartService,
    title: str,
    dataset_id: int,
    metric: str,
    time_column: str,
    time_grain: str = "P1D",
    time_range: str = "Last 30 days",
) -> dict[str, Any]:
    """Create a big number with trendline / sparkline.

    Returns:
        Dict with ``id``, ``title``, ``type``, and ``url``.
    """
    chart = await chart_svc.create_big_number_with_trendline(
        title=title,
        datasource_id=dataset_id,
        metric=metric,
        time_column=time_column,
        time_grain=time_grain,
        time_range=time_range,
    )
    return _chart_result(chart)


async def create_timeseries_bar_chart(
    chart_svc: ChartService,
    title: str,
    dataset_id: int,
    metrics: list[str],
    time_column: str,
    dimensions: list[str] | None = None,
    time_grain: str = "P1D",
    time_range: str = "Last 30 days",
    stacked: bool = False,
) -> dict[str, Any]:
    """Create an ECharts timeseries bar chart.

    Returns:
        Dict with ``id``, ``title``, ``type``, and ``url``.
    """
    chart = await chart_svc.create_timeseries_bar_chart(
        title=title,
        datasource_id=dataset_id,
        metrics=metrics,
        time_column=time_column,
        groupby=dimensions,
        time_grain=time_grain,
        time_range=time_range,
        stacked=stacked,
    )
    return _chart_result(chart)


async def create_bubble_chart(
    chart_svc: ChartService,
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
    """Create a bubble chart visualization.

    Returns:
        Dict with ``id``, ``title``, ``type``, and ``url``.
    """
    chart = await chart_svc.create_bubble_chart(
        title=title,
        datasource_id=dataset_id,
        x_metric=x_metric,
        y_metric=y_metric,
        size_metric=size_metric,
        series_column=series_column,
        entity_column=entity_column,
        time_range=time_range,
        max_bubble_size=max_bubble_size,
    )
    return _chart_result(chart)


async def create_funnel_chart(
    chart_svc: ChartService,
    title: str,
    dataset_id: int,
    metric: str,
    dimension: str,
    time_range: str = "No filter",
    sort_by_metric: bool = True,
) -> dict[str, Any]:
    """Create a funnel chart visualization.

    Returns:
        Dict with ``id``, ``title``, ``type``, and ``url``.
    """
    chart = await chart_svc.create_funnel_chart(
        title=title,
        datasource_id=dataset_id,
        metric=metric,
        groupby=dimension,
        time_range=time_range,
        sort_by_metric=sort_by_metric,
    )
    return _chart_result(chart)


async def create_gauge_chart(
    chart_svc: ChartService,
    title: str,
    dataset_id: int,
    metric: str,
    min_val: float = 0,
    max_val: float = 100,
    time_range: str = "No filter",
) -> dict[str, Any]:
    """Create a gauge / speedometer chart.

    Returns:
        Dict with ``id``, ``title``, ``type``, and ``url``.
    """
    chart = await chart_svc.create_gauge_chart(
        title=title,
        datasource_id=dataset_id,
        metric=metric,
        min_val=min_val,
        max_val=max_val,
        time_range=time_range,
    )
    return _chart_result(chart)


async def create_treemap_chart(
    chart_svc: ChartService,
    title: str,
    dataset_id: int,
    metric: str,
    dimensions: list[str],
    time_range: str = "No filter",
) -> dict[str, Any]:
    """Create a treemap visualization.

    Returns:
        Dict with ``id``, ``title``, ``type``, and ``url``.
    """
    chart = await chart_svc.create_treemap(
        title=title,
        datasource_id=dataset_id,
        metric=metric,
        groupby=dimensions,
        time_range=time_range,
    )
    return _chart_result(chart)


async def create_histogram_chart(
    chart_svc: ChartService,
    title: str,
    dataset_id: int,
    column: str,
    dimensions: list[str] | None = None,
    num_bins: int = 10,
    normalized: bool = False,
    time_range: str = "No filter",
) -> dict[str, Any]:
    """Create a histogram visualization.

    Returns:
        Dict with ``id``, ``title``, ``type``, and ``url``.
    """
    chart = await chart_svc.create_histogram(
        title=title,
        datasource_id=dataset_id,
        column=column,
        groupby=dimensions,
        num_bins=num_bins,
        normalized=normalized,
        time_range=time_range,
    )
    return _chart_result(chart)


async def create_box_plot_chart(
    chart_svc: ChartService,
    title: str,
    dataset_id: int,
    metrics: list[str],
    dimensions: list[str],
    time_range: str = "No filter",
    whisker_options: str = "Tukey",
) -> dict[str, Any]:
    """Create a box plot visualization.

    Returns:
        Dict with ``id``, ``title``, ``type``, and ``url``.
    """
    chart = await chart_svc.create_box_plot(
        title=title,
        datasource_id=dataset_id,
        metrics=metrics,
        groupby=dimensions,
        time_range=time_range,
        whisker_options=whisker_options,
    )
    return _chart_result(chart)


async def create_heatmap_chart(
    chart_svc: ChartService,
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
    """Create a heatmap visualization.

    Returns:
        Dict with ``id``, ``title``, ``type``, and ``url``.
    """
    chart = await chart_svc.create_heatmap(
        title=title,
        datasource_id=dataset_id,
        metric=metric,
        x_column=x_column,
        y_column=y_column,
        time_range=time_range,
        linear_color_scheme=linear_color_scheme,
        normalize_across=normalize_across,
        show_values=show_values,
    )
    return _chart_result(chart)


async def get_chart(
    chart_svc: ChartService,
    chart_id: int,
) -> dict[str, Any]:
    """Get detailed information about a single chart.

    Returns:
        Dict with ``id``, ``title``, ``type``, ``url``, ``description``,
        ``datasource_id``, ``dashboards``, and ``params``.
    """
    chart = await chart_svc.get_chart(chart_id)
    result = _chart_result(chart)
    result["description"] = chart.description
    result["datasource_id"] = chart.datasource_id
    result["dashboards"] = chart.dashboards
    result["params"] = chart.get_params()
    return result


async def update_chart(
    chart_svc: ChartService,
    chart_id: int,
    *,
    title: str | None = None,
    description: str | None = None,
    cache_timeout: int | None = None,
    owners: list[int] | None = None,
    dashboards: list[int] | None = None,
) -> dict[str, Any]:
    """Update an existing chart's metadata.

    Only the provided fields are updated; ``None`` values are skipped.

    Returns:
        Dict with ``id``, ``title``, ``type``, ``url``, and ``message``.
    """
    spec = ChartUpdate(
        slice_name=title,
        description=description,
        cache_timeout=cache_timeout,
        owners=owners,
        dashboards=dashboards,
    )
    chart = await chart_svc.update_chart(chart_id, spec)
    result = _chart_result(chart)
    result["message"] = f"Updated chart '{chart.slice_name}' (ID: {chart.id})"
    return result


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
