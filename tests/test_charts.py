"""Tests for chart parameter builders."""

import json

from superset_ai.schemas.charts import (
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


class TestBuildAdhocMetric:
    """Tests for adhoc metric builder."""
    
    def test_builds_count_metric(self):
        """Should build COUNT metric correctly."""
        metric = build_adhoc_metric("user_id", "COUNT", "User Count")
        
        assert metric["expressionType"] == "SIMPLE"
        assert metric["column"]["column_name"] == "user_id"
        assert metric["aggregate"] == "COUNT"
        assert metric["label"] == "User Count"
    
    def test_builds_sum_metric(self):
        """Should build SUM metric correctly."""
        metric = build_adhoc_metric("amount", "SUM")
        
        assert metric["aggregate"] == "SUM"
        assert metric["label"] == "SUM(amount)"
    
    def test_normalizes_aggregate_to_uppercase(self):
        """Should normalize aggregate to uppercase."""
        metric = build_adhoc_metric("value", "avg")
        
        assert metric["aggregate"] == "AVG"


class TestBuildBarChartParams:
    """Tests for bar chart parameter builder."""
    
    def test_builds_basic_bar_params(self):
        """Should build basic bar chart params."""
        params = build_bar_chart_params(
            datasource_id=1,
            metrics=["COUNT(*)"],
            groupby=["region"],
        )
        
        assert params.viz_type == "dist_bar"
        assert params.datasource == "1__table"
        assert params.metrics == ["COUNT(*)"]
        assert params.groupby == ["region"]
    
    def test_includes_time_settings(self):
        """Should include time settings when provided."""
        params = build_bar_chart_params(
            datasource_id=1,
            metrics=["SUM(sales)"],
            groupby=["category"],
            time_column="created_at",
            time_range="Last 7 days",
        )
        
        assert params.granularity_sqla == "created_at"
        assert params.time_range == "Last 7 days"
    
    def test_to_json_produces_valid_json(self):
        """Should serialize to valid JSON."""
        params = build_bar_chart_params(
            datasource_id=1,
            metrics=["COUNT(*)"],
            groupby=["status"],
        )
        
        json_str = params.to_json()
        parsed = json.loads(json_str)
        
        assert parsed["viz_type"] == "dist_bar"
        assert "datasource" in parsed


class TestBuildLineChartParams:
    """Tests for line chart parameter builder."""
    
    def test_builds_basic_line_params(self):
        """Should build basic line chart params."""
        params = build_line_chart_params(
            datasource_id=2,
            metrics=["AVG(response_time)"],
            time_column="timestamp",
        )
        
        assert params.viz_type == "line"
        assert params.granularity_sqla == "timestamp"
        assert params.time_range == "Last 30 days"  # Default
    
    def test_includes_groupby_for_multi_line(self):
        """Should include groupby for multiple lines."""
        params = build_line_chart_params(
            datasource_id=2,
            metrics=["COUNT(*)"],
            time_column="date",
            groupby=["status", "region"],
        )
        
        assert params.groupby == ["status", "region"]


class TestBuildPieChartParams:
    """Tests for pie chart parameter builder."""
    
    def test_builds_basic_pie_params(self):
        """Should build basic pie chart params."""
        params = build_pie_chart_params(
            datasource_id=3,
            metric="SUM(revenue)",
            groupby="product_category",
        )
        
        assert params.viz_type == "pie"
        assert params.metrics == ["SUM(revenue)"]
        assert params.groupby == ["product_category"]


class TestBuildTableParams:
    """Tests for table parameter builder."""
    
    def test_builds_raw_data_table(self):
        """Should build raw data table with columns."""
        params = build_table_params(
            datasource_id=4,
            columns=["name", "email", "created_at"],
        )
        
        assert params.viz_type == "table"
        assert params.all_columns == ["name", "email", "created_at"]
    
    def test_builds_aggregated_table(self):
        """Should build aggregated table with metrics."""
        params = build_table_params(
            datasource_id=4,
            columns=["status"],
            metrics=["COUNT(*)", "AVG(amount)"],
            groupby=["status"],
        )
        
        assert params.metrics == ["COUNT(*)", "AVG(amount)"]
        assert params.groupby == ["status"]


class TestBuildBigNumberParams:
    """Tests for big number parameter builder."""
    
    def test_builds_basic_big_number(self):
        """Should build basic big number params."""
        params = build_big_number_params(
            datasource_id=5,
            metric="COUNT(*)",
        )
        
        assert params.viz_type == "big_number_total"
        assert params.metrics == ["COUNT(*)"]


# =============================================================================
# Tests for new chart type builders
# =============================================================================


class TestBuildAreaChartParams:
    """Tests for area chart parameter builder."""

    def test_builds_basic_area_params(self):
        """Should build basic area chart params."""
        params = build_area_chart_params(
            datasource_id=1,
            metrics=["SUM(revenue)"],
            time_column="created_at",
        )

        assert params.viz_type == "area"
        assert params.datasource == "1__table"
        assert params.metrics == ["SUM(revenue)"]
        assert params.granularity_sqla == "created_at"
        assert params.stacked_style == "stack"

    def test_unstacked_area(self):
        """Should not set stacked_style when stacked=False."""
        params = build_area_chart_params(
            datasource_id=1,
            metrics=["COUNT(*)"],
            time_column="ts",
            stacked=False,
        )

        assert params.stacked_style is None

    def test_includes_groupby(self):
        """Should include groupby for stacked areas."""
        params = build_area_chart_params(
            datasource_id=1,
            metrics=["SUM(sales)"],
            time_column="order_date",
            groupby=["region"],
        )

        assert params.groupby == ["region"]

    def test_to_json_produces_valid_json(self):
        """Should serialize to valid JSON."""
        params = build_area_chart_params(
            datasource_id=1,
            metrics=["COUNT(*)"],
            time_column="ts",
        )
        parsed = json.loads(params.to_json())
        assert parsed["viz_type"] == "area"


class TestBuildBigNumberWithTrendlineParams:
    """Tests for big number with trendline builder."""

    def test_builds_basic_params(self):
        """Should build big number with trendline params."""
        params = build_big_number_with_trendline_params(
            datasource_id=2,
            metric="SUM(revenue)",
            time_column="created_at",
        )

        assert params.viz_type == "big_number"
        assert params.metrics == ["SUM(revenue)"]
        assert params.granularity_sqla == "created_at"
        assert params.time_grain_sqla == "P1D"

    def test_custom_time_range(self):
        """Should respect custom time range."""
        params = build_big_number_with_trendline_params(
            datasource_id=2,
            metric="COUNT(*)",
            time_column="ts",
            time_range="Last 7 days",
            time_grain="P1W",
        )

        assert params.time_range == "Last 7 days"
        assert params.time_grain_sqla == "P1W"


class TestBuildTimeseriesBarChartParams:
    """Tests for timeseries bar chart builder."""

    def test_builds_basic_params(self):
        """Should build basic timeseries bar params."""
        params = build_timeseries_bar_chart_params(
            datasource_id=3,
            metrics=["COUNT(*)"],
            time_column="order_date",
        )

        assert params.viz_type == "echarts_timeseries_bar"
        assert params.datasource == "3__table"
        assert params.granularity_sqla == "order_date"
        assert params.bar_stacked is False

    def test_stacked_bars(self):
        """Should set bar_stacked when stacked=True."""
        params = build_timeseries_bar_chart_params(
            datasource_id=3,
            metrics=["SUM(amount)"],
            time_column="ts",
            stacked=True,
        )

        assert params.bar_stacked is True

    def test_includes_groupby(self):
        """Should include groupby for stacked bars."""
        params = build_timeseries_bar_chart_params(
            datasource_id=3,
            metrics=["COUNT(*)"],
            time_column="ts",
            groupby=["category"],
        )

        assert params.groupby == ["category"]


class TestBuildBubbleChartParams:
    """Tests for bubble chart builder."""

    def test_builds_basic_bubble_params(self):
        """Should build bubble chart with x, y, size metrics."""
        params = build_bubble_chart_params(
            datasource_id=4,
            x_metric="SUM(revenue)",
            y_metric="COUNT(*)",
            size_metric="AVG(rating)",
            series_column="region",
        )

        assert params.viz_type == "bubble"
        assert params.series == "region"
        assert params.entity == "region"  # Defaults to series
        assert params.x is not None
        assert params.y is not None
        assert params.size is not None
        assert params.max_bubble_size == 25

    def test_custom_entity_column(self):
        """Should use custom entity column when provided."""
        params = build_bubble_chart_params(
            datasource_id=4,
            x_metric="SUM(a)",
            y_metric="SUM(b)",
            size_metric="SUM(c)",
            series_column="region",
            entity_column="country",
        )

        assert params.entity == "country"


class TestBuildFunnelChartParams:
    """Tests for funnel chart builder."""

    def test_builds_basic_funnel_params(self):
        """Should build funnel chart params."""
        params = build_funnel_chart_params(
            datasource_id=5,
            metric="COUNT(*)",
            groupby="stage",
        )

        assert params.viz_type == "funnel"
        assert params.metrics == ["COUNT(*)"]
        assert params.groupby == ["stage"]
        assert params.sort_by_metric is True

    def test_unsorted_funnel(self):
        """Should allow disabling metric sorting."""
        params = build_funnel_chart_params(
            datasource_id=5,
            metric="SUM(users)",
            groupby="step",
            sort_by_metric=False,
        )

        assert params.sort_by_metric is False


class TestBuildGaugeChartParams:
    """Tests for gauge chart builder."""

    def test_builds_basic_gauge_params(self):
        """Should build gauge chart params."""
        params = build_gauge_chart_params(
            datasource_id=6,
            metric="AVG(score)",
        )

        assert params.viz_type == "gauge_chart"
        assert params.metrics == ["AVG(score)"]
        assert params.min_val == 0
        assert params.max_val == 100
        assert params.show_pointer is True
        assert params.show_progress is True

    def test_custom_min_max(self):
        """Should respect custom min/max values."""
        params = build_gauge_chart_params(
            datasource_id=6,
            metric="SUM(completion)",
            min_val=-50,
            max_val=200,
        )

        assert params.min_val == -50
        assert params.max_val == 200


class TestBuildTreemapParams:
    """Tests for treemap builder."""

    def test_builds_basic_treemap_params(self):
        """Should build treemap params."""
        params = build_treemap_params(
            datasource_id=7,
            metric="SUM(revenue)",
            groupby=["category", "subcategory"],
        )

        assert params.viz_type == "treemap_v2"
        assert params.metrics == ["SUM(revenue)"]
        assert params.groupby == ["category", "subcategory"]
        assert params.show_legend is False


class TestBuildHistogramParams:
    """Tests for histogram builder."""

    def test_builds_basic_histogram_params(self):
        """Should build histogram params."""
        params = build_histogram_params(
            datasource_id=8,
            column="price",
        )

        assert params.viz_type == "histogram"
        assert params.all_columns == ["price"]
        assert params.link_length == 10
        assert params.normalized is False

    def test_custom_bins_and_normalized(self):
        """Should allow custom bins and normalization."""
        params = build_histogram_params(
            datasource_id=8,
            column="age",
            link_length=20,
            normalized=True,
        )

        assert params.link_length == 20
        assert params.normalized is True

    def test_includes_groupby(self):
        """Should include groupby for overlaid histograms."""
        params = build_histogram_params(
            datasource_id=8,
            column="score",
            groupby=["gender"],
        )

        assert params.groupby == ["gender"]


class TestBuildBoxPlotParams:
    """Tests for box plot builder."""

    def test_builds_basic_box_plot_params(self):
        """Should build box plot params."""
        params = build_box_plot_params(
            datasource_id=9,
            metrics=["SUM(sales)"],
            groupby=["region"],
        )

        assert params.viz_type == "box_plot"
        assert params.metrics == ["SUM(sales)"]
        assert params.groupby == ["region"]
        assert params.whisker_options == "Tukey"

    def test_custom_whisker_options(self):
        """Should respect custom whisker options."""
        params = build_box_plot_params(
            datasource_id=9,
            metrics=["AVG(score)"],
            groupby=["department"],
            whisker_options="Min/max (no outliers)",
        )

        assert params.whisker_options == "Min/max (no outliers)"


class TestBuildHeatmapParams:
    """Tests for heatmap builder."""

    def test_builds_basic_heatmap_params(self):
        """Should build heatmap params."""
        params = build_heatmap_params(
            datasource_id=10,
            metric="COUNT(*)",
            x_column="day_of_week",
            y_column="hour_of_day",
        )

        assert params.viz_type == "heatmap"
        assert params.metrics == ["COUNT(*)"]
        assert params.all_columns_x == "day_of_week"
        assert params.all_columns_y == "hour_of_day"
        assert params.linear_color_scheme == "blue_white_yellow"
        assert params.show_values is False
        assert params.canvas_image_rendering == "pixelated"

    def test_custom_color_and_normalization(self):
        """Should respect custom colour scheme and normalization."""
        params = build_heatmap_params(
            datasource_id=10,
            metric="SUM(amount)",
            x_column="month",
            y_column="product",
            linear_color_scheme="fire",
            normalize_across="x",
            show_values=True,
        )

        assert params.linear_color_scheme == "fire"
        assert params.normalize_across == "x"
        assert params.show_values is True
