"""Dashboard service for Superset API operations."""

import json
import logging
from typing import TYPE_CHECKING, Any

from superset_ai.schemas.dashboards import (
    DashboardCreate,
    DashboardDetail,
    DashboardInfo,
    DashboardUpdate,
    _add_charts_to_position,
    _remove_chart_from_position,
    generate_dashboard_metadata,
    generate_grid_layout,
    generate_position_json,
    generate_tabbed_layout,
)

if TYPE_CHECKING:
    from superset_ai.api.client import SupersetClient

logger = logging.getLogger(__name__)


class DashboardService:
    """
    Service for managing Superset dashboards.
    
    Wraps /api/v1/dashboard/ endpoints with typed interfaces.
    """

    def __init__(self, client: "SupersetClient") -> None:
        self.client = client

    async def list_dashboards(
        self,
        *,
        page: int = 0,
        page_size: int = 100,
    ) -> list[DashboardInfo]:
        """
        List all dashboards.
        
        GET /api/v1/dashboard/
        """
        params = {
            "page": page,
            "page_size": page_size,
        }

        response = await self.client.get("/dashboard/", params=params)
        
        result = response.get("result", [])
        return [DashboardInfo.model_validate(item) for item in result]

    async def get_dashboard(self, dashboard_id: int) -> DashboardDetail:
        """
        Get detailed information about a dashboard.
        
        GET /api/v1/dashboard/{id}
        """
        response = await self.client.get(f"/dashboard/{dashboard_id}")
        result = response.get("result", {})
        return DashboardDetail.model_validate(result)

    async def create_dashboard(self, spec: DashboardCreate) -> DashboardDetail:
        """
        Create a new dashboard.
        
        POST /api/v1/dashboard/
        """
        payload = spec.model_dump(exclude_none=True)
        
        logger.info("Creating dashboard: %s", spec.dashboard_title)
        response = await self.client.post("/dashboard/", json=payload)
        
        dashboard_id = response.get("id")
        if dashboard_id:
            return await self.get_dashboard(dashboard_id)
        
        result = response.get("result", response)
        return DashboardDetail.model_validate(result)

    async def update_dashboard(
        self,
        dashboard_id: int,
        spec: DashboardUpdate,
    ) -> DashboardDetail:
        """
        Update an existing dashboard.
        
        PUT /api/v1/dashboard/{id}
        """
        payload = spec.model_dump(exclude_none=True)
        
        logger.info("Updating dashboard %d", dashboard_id)
        await self.client.put(f"/dashboard/{dashboard_id}", json=payload)
        
        return await self.get_dashboard(dashboard_id)

    async def delete_dashboard(self, dashboard_id: int) -> None:
        """
        Delete a dashboard.
        
        DELETE /api/v1/dashboard/{id}
        """
        logger.info("Deleting dashboard %d", dashboard_id)
        await self.client.delete(f"/dashboard/{dashboard_id}")

    # =========================================================================
    # High-level dashboard creation methods
    # =========================================================================

    async def create_dashboard_with_charts(
        self,
        *,
        title: str,
        chart_ids: list[int],
        layout: str = "vertical",
        columns: int = 2,
        published: bool = False,
        color_scheme: str = "supersetColors",
    ) -> DashboardDetail:
        """
        Create a dashboard with charts arranged in a layout.
        
        Args:
            title: Dashboard title
            chart_ids: List of chart IDs to include
            layout: Layout type ("vertical" or "grid")
            columns: Number of columns for grid layout
            published: Whether to publish the dashboard
            color_scheme: Color scheme name (e.g. "supersetColors", "d3Category10")
        
        Returns:
            Created dashboard details
        """
        # Generate layout
        if layout == "grid":
            position_json = generate_grid_layout(chart_ids, columns=columns)
        else:
            position_json = generate_position_json(chart_ids)
        
        # Generate metadata
        json_metadata = generate_dashboard_metadata(
            chart_ids, color_scheme=color_scheme
        )
        
        spec = DashboardCreate(
            dashboard_title=title,
            position_json=position_json,
            json_metadata=json_metadata,
            published=published,
        )
        
        dashboard = await self.create_dashboard(spec)
        
        # Associate each chart with the new dashboard
        for chart_id in chart_ids:
            await self._associate_chart_with_dashboard(chart_id, dashboard.id)
        
        return dashboard

    async def create_tabbed_dashboard(
        self,
        *,
        title: str,
        tabs: dict[str, list[int]],
        published: bool = False,
        color_scheme: str = "supersetColors",
    ) -> DashboardDetail:
        """
        Create a dashboard with a tabbed layout.

        Args:
            title: Dashboard title
            tabs: Mapping of tab label to list of chart IDs.
                  Example: ``{"Overview": [1, 2], "Details": [3]}``
            published: Whether to publish the dashboard
            color_scheme: Color scheme name

        Returns:
            Created dashboard details
        """
        position_json = generate_tabbed_layout(tabs)

        # Collect all chart IDs across tabs
        all_chart_ids: list[int] = []
        for chart_ids in tabs.values():
            all_chart_ids.extend(chart_ids)

        json_metadata = generate_dashboard_metadata(
            all_chart_ids, color_scheme=color_scheme
        )

        spec = DashboardCreate(
            dashboard_title=title,
            position_json=position_json,
            json_metadata=json_metadata,
            published=published,
        )

        dashboard = await self.create_dashboard(spec)

        # Associate each chart with the new dashboard
        for chart_id in all_chart_ids:
            await self._associate_chart_with_dashboard(chart_id, dashboard.id)

        return dashboard

    async def add_charts_to_dashboard(
        self,
        dashboard_id: int,
        chart_ids: list[int],
        *,
        tab_label: str | None = None,
    ) -> DashboardDetail:
        """
        Add charts to an existing dashboard.
        
        Preserves the existing layout structure (including tabs) and appends
        new charts.  If the dashboard uses tabs, ``tab_label`` determines
        which tab receives the new charts (defaults to the first tab).
        """
        # Get current dashboard
        dashboard = await self.get_dashboard(dashboard_id)
        
        # Work with the existing position in-place (tab-aware)
        existing_position = dashboard.get_position()
        _add_charts_to_position(
            existing_position, chart_ids, tab_label=tab_label
        )
        
        position_json = json.dumps(existing_position)
        
        spec = DashboardUpdate(
            position_json=position_json,
        )
        
        # Update dashboard layout
        result = await self.update_dashboard(dashboard_id, spec)
        
        # Also update each chart to associate it with this dashboard
        for chart_id in chart_ids:
            await self._associate_chart_with_dashboard(chart_id, dashboard_id)
        
        return result

    async def _associate_chart_with_dashboard(
        self,
        chart_id: int,
        dashboard_id: int,
    ) -> None:
        """
        Associate a chart with a dashboard by updating the chart's dashboards field.
        
        This is necessary because Superset maintains the chart-dashboard relationship
        on the chart side, not just via position_json.
        """
        # Get current chart to preserve existing dashboard associations
        response = await self.client.get(f"/chart/{chart_id}")
        chart_data = response.get("result", {})
        
        existing_dashboards = chart_data.get("dashboards", [])
        existing_dashboard_ids = [d.get("id") for d in existing_dashboards if d.get("id")]
        
        # Add new dashboard if not already present
        if dashboard_id not in existing_dashboard_ids:
            all_dashboard_ids = existing_dashboard_ids + [dashboard_id]
            logger.info("Associating chart %d with dashboard %d", chart_id, dashboard_id)
            await self.client.put(f"/chart/{chart_id}", json={"dashboards": all_dashboard_ids})

    async def remove_chart_from_dashboard(
        self,
        dashboard_id: int,
        chart_id: int,
    ) -> DashboardDetail:
        """
        Remove a chart from a dashboard.
        
        Preserves the existing layout structure (including tabs) and removes
        only the specified chart. Updates both the dashboard layout and the
        chart's dashboard associations.
        """
        # Get current dashboard
        dashboard = await self.get_dashboard(dashboard_id)
        
        # Work with the existing position in-place (tab-aware)
        existing_position = dashboard.get_position()
        _remove_chart_from_position(existing_position, chart_id)
        
        position_json = json.dumps(existing_position)
        
        spec = DashboardUpdate(
            position_json=position_json,
        )
        
        result = await self.update_dashboard(dashboard_id, spec)
        
        # Also remove the dashboard association from the chart
        await self._disassociate_chart_from_dashboard(chart_id, dashboard_id)
        
        return result

    async def _disassociate_chart_from_dashboard(
        self,
        chart_id: int,
        dashboard_id: int,
    ) -> None:
        """
        Remove a dashboard association from a chart.
        """
        # Get current chart to find existing dashboard associations
        response = await self.client.get(f"/chart/{chart_id}")
        chart_data = response.get("result", {})
        
        existing_dashboards = chart_data.get("dashboards", [])
        existing_dashboard_ids = [d.get("id") for d in existing_dashboards if d.get("id")]
        
        # Remove the dashboard if present
        if dashboard_id in existing_dashboard_ids:
            remaining_dashboard_ids = [d_id for d_id in existing_dashboard_ids if d_id != dashboard_id]
            logger.info("Disassociating chart %d from dashboard %d", chart_id, dashboard_id)
            await self.client.put(f"/chart/{chart_id}", json={"dashboards": remaining_dashboard_ids})

    # =========================================================================
    # Native filter methods
    # =========================================================================

    async def add_native_filter(
        self,
        dashboard_id: int,
        filter_config: dict[str, Any],
    ) -> DashboardDetail:
        """
        Add a native filter to a dashboard.

        The filter configuration dict (from ``build_native_filter``) is
        appended to ``json_metadata.native_filter_configuration``.
        """
        dashboard = await self.get_dashboard(dashboard_id)
        metadata = dashboard.get_metadata()

        filters = metadata.get("native_filter_configuration", [])
        filters.append(filter_config)
        metadata["native_filter_configuration"] = filters

        spec = DashboardUpdate(json_metadata=json.dumps(metadata))
        return await self.update_dashboard(dashboard_id, spec)

    async def remove_native_filter(
        self,
        dashboard_id: int,
        filter_id: str,
    ) -> DashboardDetail:
        """
        Remove a native filter from a dashboard by its filter ID.
        """
        dashboard = await self.get_dashboard(dashboard_id)
        metadata = dashboard.get_metadata()

        filters = metadata.get("native_filter_configuration", [])
        metadata["native_filter_configuration"] = [
            f for f in filters if f.get("id") != filter_id
        ]

        spec = DashboardUpdate(json_metadata=json.dumps(metadata))
        return await self.update_dashboard(dashboard_id, spec)

    async def list_native_filters(
        self,
        dashboard_id: int,
    ) -> list[dict[str, Any]]:
        """
        List all native filters on a dashboard.
        """
        dashboard = await self.get_dashboard(dashboard_id)
        metadata = dashboard.get_metadata()
        return metadata.get("native_filter_configuration", [])

    async def find_by_title(self, title: str) -> DashboardInfo | None:
        """
        Find a dashboard by title.
        
        Returns None if not found.
        """
        filters = [{"col": "dashboard_title", "opr": "eq", "value": title}]
        params = {"q": json.dumps({"filters": filters})}
        
        response = await self.client.get("/dashboard/", params=params)
        result = response.get("result", [])
        
        if result:
            return DashboardInfo.model_validate(result[0])
        return None

    # =========================================================================
    # Helper methods
    # =========================================================================

    def _extract_chart_ids(self, position: dict) -> list[int]:
        """
        Extract chart IDs from position_json structure.
        """
        chart_ids = []
        
        for key, value in position.items():
            if isinstance(value, dict) and value.get("type") == "CHART":
                meta = value.get("meta", {})
                chart_id = meta.get("chartId")
                if chart_id:
                    chart_ids.append(chart_id)
        
        return chart_ids
