"""Chart service for Superset API operations."""

import json
import logging
from typing import TYPE_CHECKING, Any

from superset_ai.schemas.charts import (
    ChartCreate,
    ChartDetail,
    ChartInfo,
    ChartParams,
    ChartUpdate,
    build_adhoc_metric,
    build_bar_chart_params,
    build_big_number_params,
    build_line_chart_params,
    build_pie_chart_params,
    build_table_params,
)

if TYPE_CHECKING:
    from superset_ai.api.client import SupersetClient

logger = logging.getLogger(__name__)


class ChartService:
    """
    Service for managing Superset charts.
    
    Wraps /api/v1/chart/ endpoints with typed interfaces
    and provides builders for chart params.
    """

    def __init__(self, client: "SupersetClient") -> None:
        self.client = client

    async def list_charts(
        self,
        *,
        datasource_id: int | None = None,
        page: int = 0,
        page_size: int = 100,
    ) -> list[ChartInfo]:
        """
        List all charts, optionally filtered by datasource.
        
        GET /api/v1/chart/
        """
        params: dict = {
            "page": page,
            "page_size": page_size,
        }

        if datasource_id is not None:
            filters = [{"col": "datasource_id", "opr": "eq", "value": datasource_id}]
            params["q"] = json.dumps({"filters": filters})

        response = await self.client.get("/chart/", params=params)
        
        result = response.get("result", [])
        return [ChartInfo.model_validate(item) for item in result]

    async def get_chart(self, chart_id: int) -> ChartDetail:
        """
        Get detailed information about a chart.
        
        GET /api/v1/chart/{id}
        """
        response = await self.client.get(f"/chart/{chart_id}")
        result = response.get("result", {})
        return ChartDetail.model_validate(result)

    async def create_chart(self, spec: ChartCreate) -> ChartDetail:
        """
        Create a new chart.
        
        POST /api/v1/chart/
        """
        payload = spec.model_dump(exclude_none=True)
        
        logger.info(f"Creating chart: {spec.slice_name} ({spec.viz_type})")
        response = await self.client.post("/chart/", json=payload)
        
        chart_id = response.get("id")
        if chart_id:
            return await self.get_chart(chart_id)
        
        result = response.get("result", response)
        return ChartDetail.model_validate(result)

    async def update_chart(
        self,
        chart_id: int,
        spec: ChartUpdate,
    ) -> ChartDetail:
        """
        Update an existing chart.
        
        PUT /api/v1/chart/{id}
        """
        payload = spec.model_dump(exclude_none=True)
        
        logger.info(f"Updating chart {chart_id}")
        await self.client.put(f"/chart/{chart_id}", json=payload)
        
        return await self.get_chart(chart_id)

    async def delete_chart(self, chart_id: int) -> None:
        """
        Delete a chart.
        
        DELETE /api/v1/chart/{id}
        """
        logger.info(f"Deleting chart {chart_id}")
        await self.client.delete(f"/chart/{chart_id}")

    async def add_to_dashboards(
        self,
        chart_id: int,
        dashboard_ids: list[int],
    ) -> ChartDetail:
        """
        Associate a chart with dashboards.
        
        This updates the chart's dashboards field to include the specified
        dashboard IDs, preserving any existing associations.
        
        Args:
            chart_id: ID of the chart to update
            dashboard_ids: List of dashboard IDs to associate
        
        Returns:
            Updated chart details
        """
        # Get current chart to preserve existing dashboard associations
        chart = await self.get_chart(chart_id)
        existing_dashboard_ids = [d.get("id") for d in chart.dashboards if d.get("id")]
        
        # Merge with new dashboard IDs (avoid duplicates)
        all_dashboard_ids = list(existing_dashboard_ids)
        for dashboard_id in dashboard_ids:
            if dashboard_id not in all_dashboard_ids:
                all_dashboard_ids.append(dashboard_id)
        
        # Update the chart
        logger.info(f"Adding chart {chart_id} to dashboards: {dashboard_ids}")
        await self.client.put(f"/chart/{chart_id}", json={"dashboards": all_dashboard_ids})
        
        return await self.get_chart(chart_id)

    # =========================================================================
    # High-level chart creation methods
    # =========================================================================

    async def create_bar_chart(
        self,
        *,
        title: str,
        datasource_id: int,
        metrics: list[str],
        groupby: list[str],
        time_column: str | None = None,
        time_range: str = "No filter",
        description: str | None = None,
    ) -> ChartDetail:
        """
        Create a bar chart with simplified parameters.
        
        Args:
            title: Chart title
            datasource_id: Dataset ID
            metrics: List of metric expressions or column names
            groupby: List of dimension columns
            time_column: Optional time column for filtering
            time_range: Time range filter
            description: Optional chart description
        """
        params = build_bar_chart_params(
            datasource_id=datasource_id,
            metrics=self._normalize_metrics(metrics),
            groupby=groupby,
            time_column=time_column,
            time_range=time_range,
        )
        
        spec = ChartCreate(
            slice_name=title,
            viz_type="dist_bar",
            datasource_id=datasource_id,
            params=params.to_json(),
            description=description,
        )
        
        return await self.create_chart(spec)

    async def create_line_chart(
        self,
        *,
        title: str,
        datasource_id: int,
        metrics: list[str],
        time_column: str,
        groupby: list[str] | None = None,
        time_grain: str = "P1D",
        time_range: str = "Last 30 days",
        description: str | None = None,
    ) -> ChartDetail:
        """
        Create a line/timeseries chart.
        
        Args:
            title: Chart title
            datasource_id: Dataset ID
            metrics: List of metric expressions
            time_column: Time column for x-axis
            groupby: Optional grouping columns for multiple lines
            time_grain: Time granularity (P1D=daily, P1W=weekly, etc.)
            time_range: Time range filter
            description: Optional chart description
        """
        params = build_line_chart_params(
            datasource_id=datasource_id,
            metrics=self._normalize_metrics(metrics),
            time_column=time_column,
            groupby=groupby,
            time_grain=time_grain,
            time_range=time_range,
        )
        
        spec = ChartCreate(
            slice_name=title,
            viz_type="line",
            datasource_id=datasource_id,
            params=params.to_json(),
            description=description,
        )
        
        return await self.create_chart(spec)

    async def create_pie_chart(
        self,
        *,
        title: str,
        datasource_id: int,
        metric: str,
        groupby: str,
        time_range: str = "No filter",
        description: str | None = None,
    ) -> ChartDetail:
        """
        Create a pie chart.
        
        Args:
            title: Chart title
            datasource_id: Dataset ID
            metric: Single metric expression
            groupby: Single dimension column for slices
            time_range: Time range filter
            description: Optional chart description
        """
        params = build_pie_chart_params(
            datasource_id=datasource_id,
            metric=self._normalize_single_metric(metric),
            groupby=groupby,
            time_range=time_range,
        )
        
        spec = ChartCreate(
            slice_name=title,
            viz_type="pie",
            datasource_id=datasource_id,
            params=params.to_json(),
            description=description,
        )
        
        return await self.create_chart(spec)

    async def create_table(
        self,
        *,
        title: str,
        datasource_id: int,
        columns: list[str],
        metrics: list[str] | None = None,
        groupby: list[str] | None = None,
        time_range: str = "No filter",
        row_limit: int = 1000,
        description: str | None = None,
    ) -> ChartDetail:
        """
        Create a table visualization.
        
        Args:
            title: Chart title
            datasource_id: Dataset ID
            columns: Columns to display (for raw data)
            metrics: Optional metrics (for aggregated table)
            groupby: Optional grouping (for aggregated table)
            time_range: Time range filter
            row_limit: Maximum rows to display
            description: Optional chart description
        """
        params = build_table_params(
            datasource_id=datasource_id,
            columns=columns,
            metrics=self._normalize_metrics(metrics) if metrics else None,
            groupby=groupby,
            time_range=time_range,
            row_limit=row_limit,
        )
        
        spec = ChartCreate(
            slice_name=title,
            viz_type="table",
            datasource_id=datasource_id,
            params=params.to_json(),
            description=description,
        )
        
        return await self.create_chart(spec)

    async def create_big_number(
        self,
        *,
        title: str,
        datasource_id: int,
        metric: str,
        time_column: str | None = None,
        time_range: str = "No filter",
        description: str | None = None,
    ) -> ChartDetail:
        """
        Create a big number/KPI visualization.
        
        Args:
            title: Chart title
            datasource_id: Dataset ID
            metric: Single metric expression
            time_column: Optional time column for filtering
            time_range: Time range filter
            description: Optional chart description
        """
        params = build_big_number_params(
            datasource_id=datasource_id,
            metric=self._normalize_single_metric(metric),
            time_column=time_column,
            time_range=time_range,
        )
        
        spec = ChartCreate(
            slice_name=title,
            viz_type="big_number_total",
            datasource_id=datasource_id,
            params=params.to_json(),
            description=description,
        )
        
        return await self.create_chart(spec)

    # =========================================================================
    # Helper methods
    # =========================================================================

    def _normalize_metrics(self, metrics: list[str]) -> list[str | dict[str, Any]]:
        """
        Normalize metric specifications.
        
        Handles both simple column references and aggregation expressions.
        """
        normalized = []
        for metric in metrics:
            normalized.append(self._normalize_single_metric(metric))
        return normalized

    def _normalize_single_metric(self, metric: str) -> str | dict[str, Any]:
        """
        Normalize a single metric specification.
        
        Supported formats:
        - "column_name" → COUNT(column_name)
        - "COUNT(column)" → adhoc metric
        - "SUM(column)" → adhoc metric
        - "count" → COUNT(*)
        """
        metric_upper = metric.upper().strip()
        
        # Handle COUNT(*)
        if metric_upper in ("COUNT", "COUNT(*)"):
            return build_adhoc_metric("*", "COUNT", "COUNT(*)")
        
        # Handle aggregation expressions like SUM(column)
        for agg in ("COUNT", "SUM", "AVG", "MAX", "MIN"):
            if metric_upper.startswith(f"{agg}(") and metric_upper.endswith(")"):
                col = metric[len(agg)+1:-1].strip()
                return build_adhoc_metric(col, agg, metric)
        
        # Assume it's a pre-defined metric name
        return metric

    async def find_similar_chart(
        self,
        *,
        datasource_id: int,
        viz_type: str,
        metrics: list[str],
    ) -> ChartInfo | None:
        """
        Find an existing chart with similar configuration.
        
        Useful for implementing reuse strategy.
        """
        charts = await self.list_charts(datasource_id=datasource_id)
        
        for chart in charts:
            if chart.viz_type == viz_type:
                # TODO: Could add more sophisticated matching
                # by parsing params and comparing metrics/dimensions
                return chart
        
        return None
