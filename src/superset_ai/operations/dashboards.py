"""Dashboard operations — create, list, delete dashboards and bulk ops."""

from __future__ import annotations

from typing import Any, Literal

from superset_ai.api.charts import ChartService
from superset_ai.api.dashboards import DashboardService


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
) -> dict[str, Any]:
    """Create a dashboard containing multiple charts.

    Returns:
        Dict with ``id``, ``title``, ``url``, and ``charts_included``.
    """
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


async def add_chart_to_dashboard(
    dash_svc: DashboardService,
    dashboard_id: int,
    chart_ids: list[int],
) -> dict[str, Any]:
    """Add charts to an existing dashboard.

    Returns:
        Dict with ``id``, ``title``, ``url``, and ``message``.
    """
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
