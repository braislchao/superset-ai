"""Tests for chart parameter builders."""

import json

from superset_ai.schemas.charts import (
    build_adhoc_metric,
    build_bar_chart_params,
    build_big_number_params,
    build_line_chart_params,
    build_pie_chart_params,
    build_table_params,
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
