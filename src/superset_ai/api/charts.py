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
    build_area_chart_params,
    build_bar_chart_params,
    build_big_number_params,
    build_big_number_with_trendline_params,
    build_box_plot_params,
    build_bubble_chart_params,
    build_funnel_chart_params,
    build_gauge_chart_params,
    build_heatmap_params,
    build_histogram_params,
    build_line_chart_params,
    build_pie_chart_params,
    build_table_params,
    build_timeseries_bar_chart_params,
    build_treemap_params,
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
        
        logger.info("Creating chart: %s (%s)", spec.slice_name, spec.viz_type)
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
        
        logger.info("Updating chart %d", chart_id)
        await self.client.put(f"/chart/{chart_id}", json=payload)
        
        return await self.get_chart(chart_id)

    async def delete_chart(self, chart_id: int) -> None:
        """
        Delete a chart.
        
        DELETE /api/v1/chart/{id}
        """
        logger.info("Deleting chart %d", chart_id)
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
        logger.info("Adding chart %d to dashboards: %s", chart_id, dashboard_ids)
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
    # New chart creation methods
    # =========================================================================

    async def create_area_chart(
        self,
        *,
        title: str,
        datasource_id: int,
        metrics: list[str],
        time_column: str,
        groupby: list[str] | None = None,
        time_grain: str = "P1D",
        time_range: str = "Last 30 days",
        stacked: bool = True,
        description: str | None = None,
    ) -> ChartDetail:
        """Create an area chart (filled line chart).

        Args:
            title: Chart title
            datasource_id: Dataset ID
            metrics: List of metric expressions
            time_column: Time column for x-axis
            groupby: Optional grouping columns for stacked areas
            time_grain: Time granularity
            time_range: Time range filter
            stacked: Whether to stack the areas
            description: Optional chart description
        """
        params = build_area_chart_params(
            datasource_id=datasource_id,
            metrics=self._normalize_metrics(metrics),
            time_column=time_column,
            groupby=groupby,
            time_grain=time_grain,
            time_range=time_range,
            stacked=stacked,
        )

        spec = ChartCreate(
            slice_name=title,
            viz_type="area",
            datasource_id=datasource_id,
            params=params.to_json(),
            description=description,
        )

        return await self.create_chart(spec)

    async def create_big_number_with_trendline(
        self,
        *,
        title: str,
        datasource_id: int,
        metric: str,
        time_column: str,
        time_grain: str = "P1D",
        time_range: str = "Last 30 days",
        description: str | None = None,
    ) -> ChartDetail:
        """Create a big number with trendline / sparkline.

        Args:
            title: Chart title
            datasource_id: Dataset ID
            metric: Single metric expression
            time_column: Time column for the trendline
            time_grain: Time granularity for the trendline
            time_range: Time range filter
            description: Optional chart description
        """
        params = build_big_number_with_trendline_params(
            datasource_id=datasource_id,
            metric=self._normalize_single_metric(metric),
            time_column=time_column,
            time_grain=time_grain,
            time_range=time_range,
        )

        spec = ChartCreate(
            slice_name=title,
            viz_type="big_number",
            datasource_id=datasource_id,
            params=params.to_json(),
            description=description,
        )

        return await self.create_chart(spec)

    async def create_timeseries_bar_chart(
        self,
        *,
        title: str,
        datasource_id: int,
        metrics: list[str],
        time_column: str,
        groupby: list[str] | None = None,
        time_grain: str = "P1D",
        time_range: str = "Last 30 days",
        stacked: bool = False,
        description: str | None = None,
    ) -> ChartDetail:
        """Create an ECharts timeseries bar chart.

        Args:
            title: Chart title
            datasource_id: Dataset ID
            metrics: List of metric expressions
            time_column: Time column for x-axis
            groupby: Optional grouping columns
            time_grain: Time granularity
            time_range: Time range filter
            stacked: Whether to stack bars
            description: Optional chart description
        """
        params = build_timeseries_bar_chart_params(
            datasource_id=datasource_id,
            metrics=self._normalize_metrics(metrics),
            time_column=time_column,
            groupby=groupby,
            time_grain=time_grain,
            time_range=time_range,
            stacked=stacked,
        )

        spec = ChartCreate(
            slice_name=title,
            viz_type="echarts_timeseries_bar",
            datasource_id=datasource_id,
            params=params.to_json(),
            description=description,
        )

        return await self.create_chart(spec)

    async def create_bubble_chart(
        self,
        *,
        title: str,
        datasource_id: int,
        x_metric: str,
        y_metric: str,
        size_metric: str,
        series_column: str,
        entity_column: str | None = None,
        time_range: str = "No filter",
        max_bubble_size: int = 25,
        description: str | None = None,
    ) -> ChartDetail:
        """Create a bubble chart.

        Args:
            title: Chart title
            datasource_id: Dataset ID
            x_metric: Metric for x-axis position
            y_metric: Metric for y-axis position
            size_metric: Metric for bubble size
            series_column: Column for colouring / grouping
            entity_column: Column for bubble labels
            time_range: Time range filter
            max_bubble_size: Maximum bubble size in pixels
            description: Optional chart description
        """
        params = build_bubble_chart_params(
            datasource_id=datasource_id,
            x_metric=self._normalize_single_metric(x_metric),
            y_metric=self._normalize_single_metric(y_metric),
            size_metric=self._normalize_single_metric(size_metric),
            series_column=series_column,
            entity_column=entity_column,
            time_range=time_range,
            max_bubble_size=max_bubble_size,
        )

        spec = ChartCreate(
            slice_name=title,
            viz_type="bubble",
            datasource_id=datasource_id,
            params=params.to_json(),
            description=description,
        )

        return await self.create_chart(spec)

    async def create_funnel_chart(
        self,
        *,
        title: str,
        datasource_id: int,
        metric: str,
        groupby: str,
        time_range: str = "No filter",
        sort_by_metric: bool = True,
        description: str | None = None,
    ) -> ChartDetail:
        """Create a funnel chart.

        Args:
            title: Chart title
            datasource_id: Dataset ID
            metric: Single metric for funnel stage values
            groupby: Column representing funnel stages
            time_range: Time range filter
            sort_by_metric: Whether to sort stages by metric value
            description: Optional chart description
        """
        params = build_funnel_chart_params(
            datasource_id=datasource_id,
            metric=self._normalize_single_metric(metric),
            groupby=groupby,
            time_range=time_range,
            sort_by_metric=sort_by_metric,
        )

        spec = ChartCreate(
            slice_name=title,
            viz_type="funnel",
            datasource_id=datasource_id,
            params=params.to_json(),
            description=description,
        )

        return await self.create_chart(spec)

    async def create_gauge_chart(
        self,
        *,
        title: str,
        datasource_id: int,
        metric: str,
        min_val: float = 0,
        max_val: float = 100,
        time_range: str = "No filter",
        description: str | None = None,
    ) -> ChartDetail:
        """Create a gauge / speedometer chart.

        Args:
            title: Chart title
            datasource_id: Dataset ID
            metric: Single metric to display on the gauge
            min_val: Minimum value on the gauge scale
            max_val: Maximum value on the gauge scale
            time_range: Time range filter
            description: Optional chart description
        """
        params = build_gauge_chart_params(
            datasource_id=datasource_id,
            metric=self._normalize_single_metric(metric),
            min_val=min_val,
            max_val=max_val,
            time_range=time_range,
        )

        spec = ChartCreate(
            slice_name=title,
            viz_type="gauge_chart",
            datasource_id=datasource_id,
            params=params.to_json(),
            description=description,
        )

        return await self.create_chart(spec)

    async def create_treemap(
        self,
        *,
        title: str,
        datasource_id: int,
        metric: str,
        groupby: list[str],
        time_range: str = "No filter",
        description: str | None = None,
    ) -> ChartDetail:
        """Create a treemap visualization.

        Args:
            title: Chart title
            datasource_id: Dataset ID
            metric: Metric for rectangle area sizing
            groupby: Dimension columns for hierarchy levels
            time_range: Time range filter
            description: Optional chart description
        """
        params = build_treemap_params(
            datasource_id=datasource_id,
            metric=self._normalize_single_metric(metric),
            groupby=groupby,
            time_range=time_range,
        )

        spec = ChartCreate(
            slice_name=title,
            viz_type="treemap_v2",
            datasource_id=datasource_id,
            params=params.to_json(),
            description=description,
        )

        return await self.create_chart(spec)

    async def create_histogram(
        self,
        *,
        title: str,
        datasource_id: int,
        column: str,
        groupby: list[str] | None = None,
        num_bins: int = 10,
        normalized: bool = False,
        time_range: str = "No filter",
        description: str | None = None,
    ) -> ChartDetail:
        """Create a histogram visualization.

        Args:
            title: Chart title
            datasource_id: Dataset ID
            column: Numeric column whose distribution to plot
            groupby: Optional grouping for overlaid histograms
            num_bins: Number of bins
            normalized: Whether to normalize the histogram
            time_range: Time range filter
            description: Optional chart description
        """
        params = build_histogram_params(
            datasource_id=datasource_id,
            column=column,
            groupby=groupby,
            link_length=num_bins,
            time_range=time_range,
            normalized=normalized,
        )

        spec = ChartCreate(
            slice_name=title,
            viz_type="histogram",
            datasource_id=datasource_id,
            params=params.to_json(),
            description=description,
        )

        return await self.create_chart(spec)

    async def create_box_plot(
        self,
        *,
        title: str,
        datasource_id: int,
        metrics: list[str],
        groupby: list[str],
        time_column: str | None = None,
        time_range: str = "No filter",
        whisker_options: str = "Tukey",
        description: str | None = None,
    ) -> ChartDetail:
        """Create a box plot visualization.

        Args:
            title: Chart title
            datasource_id: Dataset ID
            metrics: Metric expressions to plot
            groupby: Dimension columns for grouping
            time_column: Optional time column for filtering
            time_range: Time range filter
            whisker_options: Whisker calculation method
            description: Optional chart description
        """
        params = build_box_plot_params(
            datasource_id=datasource_id,
            metrics=self._normalize_metrics(metrics),
            groupby=groupby,
            time_column=time_column,
            time_range=time_range,
            whisker_options=whisker_options,
        )

        spec = ChartCreate(
            slice_name=title,
            viz_type="box_plot",
            datasource_id=datasource_id,
            params=params.to_json(),
            description=description,
        )

        return await self.create_chart(spec)

    async def create_heatmap(
        self,
        *,
        title: str,
        datasource_id: int,
        metric: str,
        x_column: str,
        y_column: str,
        time_range: str = "No filter",
        linear_color_scheme: str = "blue_white_yellow",
        normalize_across: str | None = None,
        show_values: bool = False,
        description: str | None = None,
    ) -> ChartDetail:
        """Create a heatmap visualization.

        Args:
            title: Chart title
            datasource_id: Dataset ID
            metric: Metric for cell colour intensity
            x_column: Column for x-axis
            y_column: Column for y-axis
            time_range: Time range filter
            linear_color_scheme: Colour scheme name
            normalize_across: Normalisation axis (None, "heatmap", "x", "y")
            show_values: Whether to display values in cells
            description: Optional chart description
        """
        params = build_heatmap_params(
            datasource_id=datasource_id,
            metric=self._normalize_single_metric(metric),
            x_column=x_column,
            y_column=y_column,
            time_range=time_range,
            linear_color_scheme=linear_color_scheme,
            normalize_across=normalize_across,
            show_values=show_values,
        )

        spec = ChartCreate(
            slice_name=title,
            viz_type="heatmap",
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
                # Basic match by viz_type only; could be extended to
                # parse params and compare metrics/dimensions for a
                # more precise match.
                return chart
        
        return None
