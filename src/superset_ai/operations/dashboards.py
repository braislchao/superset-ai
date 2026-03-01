"""Dashboard operations — create, list, get, update, delete dashboards and bulk ops."""

from __future__ import annotations

import json
from typing import Any, Literal

from superset_ai.api.charts import ChartService
from superset_ai.api.dashboards import DashboardService
from superset_ai.schemas.dashboards import (
    DashboardUpdate,
    _has_tabs,
    build_native_filter,
)


async def list_all_dashboards(
    dash_svc: DashboardService,
) -> list[dict[str, Any]]:
    """List all dashboards in Superset.

    Returns:
        List of dicts with ``id``, ``title``, and ``published``.
    """
    dashboards = await dash_svc.list_dashboards()
    return [
        {
            "id": d.id,
            "title": d.dashboard_title,
            "published": d.published,
        }
        for d in dashboards
    ]


async def create_dashboard(
    dash_svc: DashboardService,
    title: str,
    chart_ids: list[int],
    layout: Literal["vertical", "grid"] = "vertical",
    color_scheme: str = "supersetColors",
) -> dict[str, Any]:
    """Create a dashboard containing multiple charts.

    Returns:
        Dict with ``id``, ``title``, ``url``, ``charts_included``,
        and ``color_scheme``.
    """
    dashboard = await dash_svc.create_dashboard_with_charts(
        title=title,
        chart_ids=chart_ids,
        layout=layout,
        color_scheme=color_scheme,
    )
    return {
        "id": dashboard.id,
        "title": dashboard.dashboard_title,
        "url": f"/superset/dashboard/{dashboard.id}/",
        "charts_included": chart_ids,
        "color_scheme": color_scheme,
    }


async def create_tabbed_dashboard(
    dash_svc: DashboardService,
    title: str,
    tabs: dict[str, list[int]],
    color_scheme: str = "supersetColors",
) -> dict[str, Any]:
    """Create a dashboard with a tabbed layout.

    Args:
        dash_svc: Dashboard service instance.
        title: Dashboard title.
        tabs: Mapping of tab label to list of chart IDs.
              Example: ``{"Overview": [1, 2], "Details": [3, 4, 5]}``
        color_scheme: Color scheme name.

    Returns:
        Dict with ``id``, ``title``, ``url``, ``tabs``, and ``color_scheme``.
    """
    dashboard = await dash_svc.create_tabbed_dashboard(
        title=title,
        tabs=tabs,
        color_scheme=color_scheme,
    )
    return {
        "id": dashboard.id,
        "title": dashboard.dashboard_title,
        "url": f"/superset/dashboard/{dashboard.id}/",
        "tabs": {label: ids for label, ids in tabs.items()},
        "color_scheme": color_scheme,
    }


async def add_chart_to_dashboard(
    dash_svc: DashboardService,
    dashboard_id: int,
    chart_ids: list[int],
    *,
    tab_label: str | None = None,
) -> dict[str, Any]:
    """Add charts to an existing dashboard.

    If the dashboard uses a tabbed layout, ``tab_label`` determines which
    tab the charts are added to.  Defaults to the first tab when omitted.

    Returns:
        Dict with ``id``, ``title``, ``url``, and ``message``.
    """
    dashboard = await dash_svc.add_charts_to_dashboard(
        dashboard_id=dashboard_id,
        chart_ids=chart_ids,
        tab_label=tab_label,
    )
    return {
        "id": dashboard.id,
        "title": dashboard.dashboard_title,
        "url": f"/superset/dashboard/{dashboard.id}/",
        "message": f"Added {len(chart_ids)} chart(s) to dashboard",
    }


async def get_dashboard(
    dash_svc: DashboardService,
    dashboard_id: int,
) -> dict[str, Any]:
    """Get detailed information about a single dashboard.

    Returns:
        Dict with ``id``, ``title``, ``url``, ``published``, ``charts``,
        ``css``, ``slug``, ``color_scheme``, and optionally ``tabs``.
    """
    dashboard = await dash_svc.get_dashboard(dashboard_id)
    # Extract chart IDs from position_json
    position = dashboard.get_position()
    chart_ids: list[int] = []
    for key, value in position.items():
        if isinstance(value, dict) and value.get("type") == "CHART":
            meta = value.get("meta", {})
            chart_id = meta.get("chartId")
            if chart_id:
                chart_ids.append(chart_id)

    # Extract color scheme from metadata
    metadata = dashboard.get_metadata()
    color_scheme = metadata.get("color_scheme", "supersetColors")

    result: dict[str, Any] = {
        "id": dashboard.id,
        "title": dashboard.dashboard_title,
        "url": f"/superset/dashboard/{dashboard.id}/",
        "published": dashboard.published,
        "slug": dashboard.slug,
        "css": dashboard.css,
        "chart_ids": chart_ids,
        "charts": dashboard.charts,
        "color_scheme": color_scheme,
    }

    # Add tab information if the dashboard uses tabs
    if _has_tabs(position):
        tabs_info: dict[str, list[int]] = {}
        for comp in position.values():
            if isinstance(comp, dict) and comp.get("type") == "TAB":
                tab_label = comp.get("meta", {}).get("text", "Untitled")
                tab_chart_ids: list[int] = []
                for child_id in comp.get("children", []):
                    row = position.get(child_id, {})
                    if isinstance(row, dict) and row.get("type") == "ROW":
                        for chart_key in row.get("children", []):
                            chart_comp = position.get(chart_key, {})
                            if isinstance(chart_comp, dict) and chart_comp.get("type") == "CHART":
                                cid = chart_comp.get("meta", {}).get("chartId")
                                if cid is not None:
                                    tab_chart_ids.append(cid)
                tabs_info[tab_label] = tab_chart_ids
        result["tabs"] = tabs_info

    return result


async def update_dashboard(
    dash_svc: DashboardService,
    dashboard_id: int,
    *,
    title: str | None = None,
    slug: str | None = None,
    css: str | None = None,
    published: bool | None = None,
    owners: list[int] | None = None,
    color_scheme: str | None = None,
) -> dict[str, Any]:
    """Update an existing dashboard's metadata.

    Only the provided fields are updated; ``None`` values are skipped.

    Returns:
        Dict with ``id``, ``title``, ``url``, and ``message``.
    """
    # If color_scheme is provided, update json_metadata
    json_metadata_str: str | None = None
    if color_scheme is not None:
        dashboard = await dash_svc.get_dashboard(dashboard_id)
        metadata = dashboard.get_metadata()
        metadata["color_scheme"] = color_scheme
        json_metadata_str = json.dumps(metadata)

    spec = DashboardUpdate(
        dashboard_title=title,
        slug=slug,
        css=css,
        published=published,
        owners=owners,
        json_metadata=json_metadata_str,
    )
    dashboard_result = await dash_svc.update_dashboard(dashboard_id, spec)
    return {
        "id": dashboard_result.id,
        "title": dashboard_result.dashboard_title,
        "url": f"/superset/dashboard/{dashboard_result.id}/",
        "message": f"Updated dashboard '{dashboard_result.dashboard_title}' (ID: {dashboard_result.id})",
    }


async def remove_chart_from_dashboard(
    dash_svc: DashboardService,
    dashboard_id: int,
    chart_id: int,
) -> dict[str, Any]:
    """Remove a chart from a dashboard.

    Updates both the dashboard layout and chart-dashboard associations.
    Preserves the existing layout structure including tabs.

    Returns:
        Dict with ``id``, ``title``, ``url``, and ``message``.
    """
    dashboard = await dash_svc.remove_chart_from_dashboard(
        dashboard_id=dashboard_id,
        chart_id=chart_id,
    )
    return {
        "id": dashboard.id,
        "title": dashboard.dashboard_title,
        "url": f"/superset/dashboard/{dashboard.id}/",
        "message": f"Removed chart {chart_id} from dashboard '{dashboard.dashboard_title}'",
    }


async def delete_dashboard(
    dash_svc: DashboardService,
    dashboard_id: int,
) -> dict[str, Any]:
    """Delete a dashboard from Superset.

    Charts are NOT deleted — only the dashboard container is removed.

    Returns:
        Dict with ``deleted`` (bool), ``dashboard_id``, ``dashboard_name``,
        and ``message``.
    """
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
            "dashboard_name": dashboard_name,
            "error": str(e),
            "message": f"Failed to delete dashboard '{dashboard_name}' (ID: {dashboard_id}): {e}",
        }


async def delete_all_charts_and_dashboards(
    chart_svc: ChartService,
    dash_svc: DashboardService,
) -> dict[str, Any]:
    """Delete ALL charts and dashboards from Superset.

    Dashboards are deleted first (to release chart associations), then all
    charts are deleted. This is destructive and cannot be undone.

    Returns:
        Dict with success flag, counts, message, and details.
    """
    results: dict[str, list[dict[str, Any]]] = {
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


# =============================================================================
# Native filter operations
# =============================================================================


async def add_filter_to_dashboard(
    dash_svc: DashboardService,
    dashboard_id: int,
    *,
    name: str,
    filter_type: str = "filter_select",
    dataset_id: int | None = None,
    column: str | None = None,
    exclude_chart_ids: list[int] | None = None,
    multi_select: bool = True,
    default_to_first_item: bool = False,
    description: str = "",
) -> dict[str, Any]:
    """Add a native filter to a dashboard.

    Args:
        dash_svc: Dashboard service instance.
        dashboard_id: ID of the dashboard.
        name: Display name for the filter.
        filter_type: One of ``filter_select``, ``filter_range``,
            ``filter_time``, ``filter_timecolumn``, ``filter_timegrain``.
        dataset_id: Dataset ID (required for all types except ``filter_time``).
        column: Column name (required for all types except ``filter_time``).
        exclude_chart_ids: Chart IDs to exclude from the filter scope.
        multi_select: Allow multiple values (``filter_select`` only).
        default_to_first_item: Pre-select the first value.
        description: Optional description text.

    Returns:
        Dict with ``filter_id``, ``name``, ``filter_type``, ``dashboard_id``,
        and ``message``.
    """
    filter_config = build_native_filter(
        name=name,
        filter_type=filter_type,
        dataset_id=dataset_id,
        column=column,
        exclude_chart_ids=exclude_chart_ids,
        multi_select=multi_select,
        default_to_first_item=default_to_first_item,
        description=description,
    )

    await dash_svc.add_native_filter(dashboard_id, filter_config)

    return {
        "filter_id": filter_config["id"],
        "name": name,
        "filter_type": filter_type,
        "dashboard_id": dashboard_id,
        "message": f"Added filter '{name}' ({filter_type}) to dashboard {dashboard_id}",
    }


async def remove_filter_from_dashboard(
    dash_svc: DashboardService,
    dashboard_id: int,
    filter_id: str,
) -> dict[str, Any]:
    """Remove a native filter from a dashboard.

    Args:
        dash_svc: Dashboard service instance.
        dashboard_id: ID of the dashboard.
        filter_id: The filter ID (e.g. ``NATIVE_FILTER-abc12345``).

    Returns:
        Dict with ``dashboard_id``, ``filter_id``, and ``message``.
    """
    await dash_svc.remove_native_filter(dashboard_id, filter_id)

    return {
        "dashboard_id": dashboard_id,
        "filter_id": filter_id,
        "message": f"Removed filter '{filter_id}' from dashboard {dashboard_id}",
    }


async def list_dashboard_filters(
    dash_svc: DashboardService,
    dashboard_id: int,
) -> list[dict[str, Any]]:
    """List all native filters on a dashboard.

    Returns:
        List of dicts with ``filter_id``, ``name``, ``filter_type``,
        ``column``, and ``dataset_id``.
    """
    raw_filters = await dash_svc.list_native_filters(dashboard_id)

    result: list[dict[str, Any]] = []
    for f in raw_filters:
        targets = f.get("targets", [{}])
        target = targets[0] if targets else {}
        col_info = target.get("column", {})
        result.append({
            "filter_id": f.get("id"),
            "name": f.get("name"),
            "filter_type": f.get("filterType"),
            "column": col_info.get("name") if col_info else None,
            "dataset_id": target.get("datasetId"),
        })

    return result
