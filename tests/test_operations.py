"""Tests for the operations layer (discovery, datasets, charts, dashboards)."""

from __future__ import annotations

import json
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from superset_ai.operations import charts as chart_ops
from superset_ai.operations import dashboards as dashboard_ops
from superset_ai.operations import datasets as dataset_ops
from superset_ai.operations import discovery as discovery_ops
from superset_ai.schemas.charts import ChartUpdate


# ---------------------------------------------------------------------------
# Helpers – mock object factories
# ---------------------------------------------------------------------------


def _make_db(id: int = 1, database_name: str = "main", backend: str = "postgresql"):
    m = MagicMock()
    m.id = id
    m.database_name = database_name
    m.backend = backend
    return m


def _make_table(name: str = "users", schema_: str = "public", type: str = "TABLE"):
    m = MagicMock()
    m.name = name
    m.schema_ = schema_
    m.type = type
    return m


def _make_column(
    column_name: str = "id",
    type: str = "INTEGER",
    is_dttm: bool = False,
    type_generic: int = 0,
):
    m = MagicMock()
    m.column_name = column_name
    m.type = type
    m.is_dttm = is_dttm
    m.type_generic = type_generic
    return m


def _make_dataset_info(
    id: int = 10,
    table_name: str = "orders",
    database_id: int = 1,
    schema_: str = "public",
):
    m = MagicMock()
    m.id = id
    m.table_name = table_name
    m.database_id = database_id
    m.schema_ = schema_
    return m


def _make_dataset_detail(
    id: int = 10,
    table_name: str = "orders",
    columns: list | None = None,
):
    m = MagicMock()
    m.id = id
    m.table_name = table_name
    m.columns = columns or []
    return m


def _make_chart(
    id: int = 1,
    slice_name: str = "My Chart",
    viz_type: str = "echarts_timeseries_bar",
    datasource_id: int = 10,
    description: str = "",
    dashboards: list | None = None,
    params: dict | None = None,
):
    m = MagicMock()
    m.id = id
    m.slice_name = slice_name
    m.viz_type = viz_type
    m.datasource_id = datasource_id
    m.description = description
    m.dashboards = dashboards or []
    m.get_params.return_value = params or {}
    return m


def _make_dashboard(
    id: int = 1,
    dashboard_title: str = "My Dashboard",
    published: bool = True,
    slug: str | None = None,
    css: str | None = None,
    charts: list | None = None,
    position: dict | None = None,
    metadata: dict | None = None,
):
    m = MagicMock()
    m.id = id
    m.dashboard_title = dashboard_title
    m.published = published
    m.slug = slug
    m.css = css
    m.charts = charts or []
    m.get_position.return_value = position or {}
    m.get_metadata.return_value = metadata or {}
    return m


# ===========================================================================
# Discovery operations
# ===========================================================================


class TestDiscoveryOps:
    async def test_list_databases(self):
        db_svc = AsyncMock()
        db_svc.list_databases.return_value = [
            _make_db(1, "main", "sqlite"),
            _make_db(2, "analytics", "postgresql"),
        ]

        result = await discovery_ops.list_databases(db_svc)

        assert result == [
            {"id": 1, "database_name": "main", "backend": "sqlite"},
            {"id": 2, "database_name": "analytics", "backend": "postgresql"},
        ]
        db_svc.list_databases.assert_called_once()

    async def test_list_databases_empty(self):
        db_svc = AsyncMock()
        db_svc.list_databases.return_value = []

        result = await discovery_ops.list_databases(db_svc)
        assert result == []

    async def test_list_schemas(self):
        db_svc = AsyncMock()
        db_svc.list_schemas.return_value = ["public", "staging"]

        result = await discovery_ops.list_schemas(db_svc, database_id=1)

        assert result == ["public", "staging"]
        db_svc.list_schemas.assert_called_once_with(1)

    async def test_list_tables(self):
        db_svc = AsyncMock()
        db_svc.list_tables.return_value = [
            _make_table("users", "public", "TABLE"),
            _make_table("orders", "public", "TABLE"),
        ]

        result = await discovery_ops.list_tables(db_svc, database_id=1, schema_name="public")

        assert result == [
            {"name": "users", "schema": "public", "type": "TABLE"},
            {"name": "orders", "schema": "public", "type": "TABLE"},
        ]
        db_svc.list_tables.assert_called_once_with(1, schema="public")

    async def test_list_tables_no_schema(self):
        db_svc = AsyncMock()
        db_svc.list_tables.return_value = [_make_table("t1")]

        await discovery_ops.list_tables(db_svc, database_id=5)
        db_svc.list_tables.assert_called_once_with(5, schema=None)

    async def test_list_existing_datasets(self):
        ds_svc = AsyncMock()
        ds_svc.list_datasets.return_value = [
            _make_dataset_info(10, "orders", 1, "public"),
            _make_dataset_info(11, "users", 1, "public"),
        ]

        result = await discovery_ops.list_existing_datasets(ds_svc, database_id=1)

        assert result == [
            {"id": 10, "table_name": "orders", "database_id": 1, "schema": "public"},
            {"id": 11, "table_name": "users", "database_id": 1, "schema": "public"},
        ]
        ds_svc.list_datasets.assert_called_once_with(database_id=1)

    async def test_list_existing_datasets_no_filter(self):
        ds_svc = AsyncMock()
        ds_svc.list_datasets.return_value = []

        await discovery_ops.list_existing_datasets(ds_svc)
        ds_svc.list_datasets.assert_called_once_with(database_id=None)

    async def test_get_dataset_columns(self):
        ds_svc = AsyncMock()
        cols = [
            _make_column("id", "INTEGER", is_dttm=False, type_generic=0),
            _make_column("created_at", "TIMESTAMP", is_dttm=True, type_generic=2),
            _make_column("amount", "FLOAT", is_dttm=False, type_generic=1),
            _make_column("name", "VARCHAR", is_dttm=False, type_generic=3),
        ]
        ds_svc.get_dataset.return_value = _make_dataset_detail(10, "orders", cols)

        result = await discovery_ops.get_dataset_columns(ds_svc, dataset_id=10)

        assert result["dataset_id"] == 10
        assert result["table_name"] == "orders"
        assert len(result["columns"]) == 4

        assert result["columns"][0] == {"name": "id", "type": "INTEGER", "is_time": False}
        assert result["columns"][1] == {"name": "created_at", "type": "TIMESTAMP", "is_time": True}

        assert result["time_columns"] == ["created_at"]
        # INT (0) and FLOAT (1) are numeric
        assert result["numeric_columns"] == ["id", "amount"]
        ds_svc.get_dataset.assert_called_once_with(10)

    async def test_get_dataset_columns_no_numeric_or_time(self):
        ds_svc = AsyncMock()
        cols = [
            _make_column("name", "VARCHAR", is_dttm=False, type_generic=3),
        ]
        ds_svc.get_dataset.return_value = _make_dataset_detail(5, "tags", cols)

        result = await discovery_ops.get_dataset_columns(ds_svc, dataset_id=5)
        assert result["time_columns"] == []
        assert result["numeric_columns"] == []


# ===========================================================================
# Dataset operations
# ===========================================================================


class TestDatasetOps:
    async def test_find_or_create_dataset(self):
        ds_svc = AsyncMock()
        cols = [
            _make_column("id", "INT", is_dttm=False),
            _make_column("created_at", "TIMESTAMP", is_dttm=True),
            _make_column("name", "VARCHAR", is_dttm=False),
        ]
        ds_svc.find_or_create.return_value = _make_dataset_detail(20, "products", cols)

        result = await dataset_ops.find_or_create_dataset(
            ds_svc,
            database_id=1,
            table_name="products",
            schema_name="public",
        )

        assert result == {
            "id": 20,
            "table_name": "products",
            "columns": ["id", "created_at", "name"],
            "time_columns": ["created_at"],
        }
        ds_svc.find_or_create.assert_called_once_with(
            table_name="products",
            database_id=1,
            schema="public",
        )

    async def test_find_or_create_dataset_no_schema(self):
        ds_svc = AsyncMock()
        ds_svc.find_or_create.return_value = _make_dataset_detail(21, "logs", [])

        result = await dataset_ops.find_or_create_dataset(
            ds_svc,
            database_id=2,
            table_name="logs",
        )

        assert result["id"] == 21
        assert result["columns"] == []
        assert result["time_columns"] == []
        ds_svc.find_or_create.assert_called_once_with(
            table_name="logs",
            database_id=2,
            schema=None,
        )


# ===========================================================================
# Chart operations
# ===========================================================================


class TestChartOps:
    """Test chart creation, retrieval, update, listing, and deletion."""

    # -- Chart creation (representative samples) --

    async def test_create_bar_chart(self):
        chart_svc = AsyncMock()
        chart_svc.create_bar_chart.return_value = _make_chart(
            id=1, slice_name="Revenue by Region", viz_type="echarts_timeseries_bar",
        )

        result = await chart_ops.create_bar_chart(
            chart_svc,
            title="Revenue by Region",
            dataset_id=10,
            metrics=["SUM(revenue)"],
            dimensions=["region"],
            time_range="Last 7 days",
        )

        assert result == {
            "id": 1,
            "title": "Revenue by Region",
            "type": "echarts_timeseries_bar",
            "url": "/explore/?slice_id=1",
        }
        chart_svc.create_bar_chart.assert_called_once_with(
            title="Revenue by Region",
            datasource_id=10,
            metrics=["SUM(revenue)"],
            groupby=["region"],
            time_range="Last 7 days",
        )

    async def test_create_line_chart(self):
        chart_svc = AsyncMock()
        chart_svc.create_line_chart.return_value = _make_chart(
            id=2, slice_name="Sales Trend", viz_type="echarts_timeseries_line",
        )

        result = await chart_ops.create_line_chart(
            chart_svc,
            title="Sales Trend",
            dataset_id=10,
            metrics=["COUNT(*)"],
            time_column="ds",
            dimensions=["category"],
            time_grain="P1M",
            time_range="Last year",
        )

        assert result["id"] == 2
        assert result["url"] == "/explore/?slice_id=2"
        chart_svc.create_line_chart.assert_called_once_with(
            title="Sales Trend",
            datasource_id=10,
            metrics=["COUNT(*)"],
            time_column="ds",
            groupby=["category"],
            time_grain="P1M",
            time_range="Last year",
        )

    async def test_create_pie_chart(self):
        chart_svc = AsyncMock()
        chart_svc.create_pie_chart.return_value = _make_chart(
            id=3, slice_name="Market Share", viz_type="pie",
        )

        result = await chart_ops.create_pie_chart(
            chart_svc,
            title="Market Share",
            dataset_id=10,
            metric="SUM(sales)",
            dimension="company",
        )

        assert result["id"] == 3
        assert result["type"] == "pie"
        chart_svc.create_pie_chart.assert_called_once_with(
            title="Market Share",
            datasource_id=10,
            metric="SUM(sales)",
            groupby="company",
            time_range="No filter",
        )

    async def test_create_table_chart(self):
        chart_svc = AsyncMock()
        chart_svc.create_table.return_value = _make_chart(
            id=4, slice_name="User Table", viz_type="table",
        )

        result = await chart_ops.create_table_chart(
            chart_svc,
            title="User Table",
            dataset_id=10,
            columns=["name", "email"],
            metrics=["COUNT(*)"],
            dimensions=["status"],
            row_limit=500,
        )

        assert result["id"] == 4
        chart_svc.create_table.assert_called_once_with(
            title="User Table",
            datasource_id=10,
            columns=["name", "email"],
            metrics=["COUNT(*)"],
            groupby=["status"],
            row_limit=500,
        )

    async def test_create_bubble_chart(self):
        chart_svc = AsyncMock()
        chart_svc.create_bubble_chart.return_value = _make_chart(
            id=5, slice_name="Bubble", viz_type="bubble",
        )

        result = await chart_ops.create_bubble_chart(
            chart_svc,
            title="Bubble",
            dataset_id=10,
            x_metric="AVG(price)",
            y_metric="SUM(quantity)",
            size_metric="COUNT(*)",
            series_column="region",
            entity_column="product",
            time_range="Last month",
            max_bubble_size=50,
        )

        assert result["id"] == 5
        chart_svc.create_bubble_chart.assert_called_once_with(
            title="Bubble",
            datasource_id=10,
            x_metric="AVG(price)",
            y_metric="SUM(quantity)",
            size_metric="COUNT(*)",
            series_column="region",
            entity_column="product",
            time_range="Last month",
            max_bubble_size=50,
        )

    async def test_create_heatmap_chart(self):
        chart_svc = AsyncMock()
        chart_svc.create_heatmap.return_value = _make_chart(
            id=6, slice_name="Heatmap", viz_type="heatmap",
        )

        result = await chart_ops.create_heatmap_chart(
            chart_svc,
            title="Heatmap",
            dataset_id=10,
            metric="AVG(score)",
            x_column="weekday",
            y_column="hour",
            linear_color_scheme="fire",
            normalize_across="x",
            show_values=True,
        )

        assert result["id"] == 6
        chart_svc.create_heatmap.assert_called_once_with(
            title="Heatmap",
            datasource_id=10,
            metric="AVG(score)",
            x_column="weekday",
            y_column="hour",
            time_range="No filter",
            linear_color_scheme="fire",
            normalize_across="x",
            show_values=True,
        )

    # -- get_chart --

    async def test_get_chart(self):
        chart_svc = AsyncMock()
        chart_svc.get_chart.return_value = _make_chart(
            id=42,
            slice_name="KPI",
            viz_type="big_number",
            datasource_id=10,
            description="Total revenue metric",
            dashboards=[1, 2],
            params={"metric": "SUM(revenue)", "time_range": "Last 30 days"},
        )

        result = await chart_ops.get_chart(chart_svc, chart_id=42)

        assert result["id"] == 42
        assert result["title"] == "KPI"
        assert result["type"] == "big_number"
        assert result["url"] == "/explore/?slice_id=42"
        assert result["description"] == "Total revenue metric"
        assert result["datasource_id"] == 10
        assert result["dashboards"] == [1, 2]
        assert result["params"] == {"metric": "SUM(revenue)", "time_range": "Last 30 days"}
        chart_svc.get_chart.assert_called_once_with(42)

    # -- update_chart --

    async def test_update_chart(self):
        chart_svc = AsyncMock()
        chart_svc.update_chart.return_value = _make_chart(
            id=42, slice_name="Updated KPI", viz_type="big_number",
        )

        result = await chart_ops.update_chart(
            chart_svc,
            chart_id=42,
            title="Updated KPI",
            description="New description",
        )

        assert result["id"] == 42
        assert result["title"] == "Updated KPI"
        assert result["message"] == "Updated chart 'Updated KPI' (ID: 42)"

        call_args = chart_svc.update_chart.call_args
        assert call_args[0][0] == 42
        spec = call_args[0][1]
        assert isinstance(spec, ChartUpdate)
        assert spec.slice_name == "Updated KPI"
        assert spec.description == "New description"
        assert spec.cache_timeout is None
        assert spec.owners is None
        assert spec.dashboards is None

    async def test_update_chart_with_all_fields(self):
        chart_svc = AsyncMock()
        chart_svc.update_chart.return_value = _make_chart(
            id=10, slice_name="Full Update", viz_type="pie",
        )

        await chart_ops.update_chart(
            chart_svc,
            chart_id=10,
            title="Full Update",
            description="desc",
            cache_timeout=300,
            owners=[1, 2],
            dashboards=[5],
        )

        spec = chart_svc.update_chart.call_args[0][1]
        assert spec.slice_name == "Full Update"
        assert spec.description == "desc"
        assert spec.cache_timeout == 300
        assert spec.owners == [1, 2]
        assert spec.dashboards == [5]

    # -- list_all_charts --

    async def test_list_all_charts(self):
        chart_svc = AsyncMock()
        chart_svc.list_charts.return_value = [
            _make_chart(id=1, slice_name="A", viz_type="bar"),
            _make_chart(id=2, slice_name="B", viz_type="line"),
        ]

        result = await chart_ops.list_all_charts(chart_svc)

        assert result == [
            {"id": 1, "title": "A", "type": "bar"},
            {"id": 2, "title": "B", "type": "line"},
        ]
        chart_svc.list_charts.assert_called_once()

    async def test_list_all_charts_empty(self):
        chart_svc = AsyncMock()
        chart_svc.list_charts.return_value = []

        result = await chart_ops.list_all_charts(chart_svc)
        assert result == []

    # -- delete_chart --

    async def test_delete_chart_success(self):
        chart_svc = AsyncMock()
        chart_svc.get_chart.return_value = _make_chart(
            id=42, slice_name="Doomed Chart",
        )

        result = await chart_ops.delete_chart(chart_svc, chart_id=42)

        assert result["deleted"] is True
        assert result["chart_id"] == 42
        assert result["chart_name"] == "Doomed Chart"
        assert "error" not in result
        assert "Deleted chart 'Doomed Chart' (ID: 42)" in result["message"]
        chart_svc.delete_chart.assert_called_once_with(42)

    async def test_delete_chart_failure(self):
        chart_svc = AsyncMock()
        chart_svc.get_chart.return_value = _make_chart(
            id=42, slice_name="Problematic Chart",
        )
        chart_svc.delete_chart.side_effect = Exception("Permission denied")

        result = await chart_ops.delete_chart(chart_svc, chart_id=42)

        assert result["deleted"] is False
        assert result["chart_id"] == 42
        assert result["chart_name"] == "Problematic Chart"
        assert result["error"] == "Permission denied"
        assert "Failed to delete" in result["message"]

    async def test_delete_chart_get_fails_then_delete_succeeds(self):
        """When get_chart fails, we should still attempt the delete
        and use a fallback name."""
        chart_svc = AsyncMock()
        chart_svc.get_chart.side_effect = Exception("Not found")

        result = await chart_ops.delete_chart(chart_svc, chart_id=99)

        assert result["deleted"] is True
        assert result["chart_name"] == "Chart 99"
        chart_svc.delete_chart.assert_called_once_with(99)

    async def test_delete_chart_get_fails_and_delete_fails(self):
        """When both get and delete fail, report failure with fallback name."""
        chart_svc = AsyncMock()
        chart_svc.get_chart.side_effect = Exception("Not found")
        chart_svc.delete_chart.side_effect = Exception("Server error")

        result = await chart_ops.delete_chart(chart_svc, chart_id=99)

        assert result["deleted"] is False
        assert result["chart_name"] == "Chart 99"
        assert result["error"] == "Server error"


# ===========================================================================
# Dashboard operations
# ===========================================================================


class TestDashboardOps:
    # -- list_all_dashboards --

    async def test_list_all_dashboards(self):
        dash_svc = AsyncMock()
        dash_svc.list_dashboards.return_value = [
            _make_dashboard(id=1, dashboard_title="Sales", published=True),
            _make_dashboard(id=2, dashboard_title="Ops", published=False),
        ]

        result = await dashboard_ops.list_all_dashboards(dash_svc)

        assert result == [
            {"id": 1, "title": "Sales", "published": True},
            {"id": 2, "title": "Ops", "published": False},
        ]
        dash_svc.list_dashboards.assert_called_once()

    async def test_list_all_dashboards_empty(self):
        dash_svc = AsyncMock()
        dash_svc.list_dashboards.return_value = []

        result = await dashboard_ops.list_all_dashboards(dash_svc)
        assert result == []

    # -- create_dashboard --

    async def test_create_dashboard(self):
        dash_svc = AsyncMock()
        dash_svc.create_dashboard_with_charts.return_value = _make_dashboard(
            id=10, dashboard_title="Sales Dashboard",
        )

        result = await dashboard_ops.create_dashboard(
            dash_svc,
            title="Sales Dashboard",
            chart_ids=[1, 2, 3],
            layout="grid",
            color_scheme="d3Category10",
        )

        assert result == {
            "id": 10,
            "title": "Sales Dashboard",
            "url": "/superset/dashboard/10/",
            "charts_included": [1, 2, 3],
            "color_scheme": "d3Category10",
        }
        dash_svc.create_dashboard_with_charts.assert_called_once_with(
            title="Sales Dashboard",
            chart_ids=[1, 2, 3],
            layout="grid",
            color_scheme="d3Category10",
        )

    async def test_create_dashboard_defaults(self):
        dash_svc = AsyncMock()
        dash_svc.create_dashboard_with_charts.return_value = _make_dashboard(id=11)

        result = await dashboard_ops.create_dashboard(
            dash_svc,
            title="Simple",
            chart_ids=[1],
        )

        assert result["color_scheme"] == "supersetColors"
        dash_svc.create_dashboard_with_charts.assert_called_once_with(
            title="Simple",
            chart_ids=[1],
            layout="vertical",
            color_scheme="supersetColors",
        )

    # -- create_tabbed_dashboard --

    async def test_create_tabbed_dashboard(self):
        dash_svc = AsyncMock()
        dash_svc.create_tabbed_dashboard.return_value = _make_dashboard(
            id=20, dashboard_title="Tabbed Dash",
        )

        tabs = {"Overview": [1, 2], "Details": [3, 4]}
        result = await dashboard_ops.create_tabbed_dashboard(
            dash_svc,
            title="Tabbed Dash",
            tabs=tabs,
            color_scheme="bnbColors",
        )

        assert result == {
            "id": 20,
            "title": "Tabbed Dash",
            "url": "/superset/dashboard/20/",
            "tabs": {"Overview": [1, 2], "Details": [3, 4]},
            "color_scheme": "bnbColors",
        }
        dash_svc.create_tabbed_dashboard.assert_called_once_with(
            title="Tabbed Dash",
            tabs=tabs,
            color_scheme="bnbColors",
        )

    # -- add_chart_to_dashboard --

    async def test_add_chart_to_dashboard(self):
        dash_svc = AsyncMock()
        dash_svc.add_charts_to_dashboard.return_value = _make_dashboard(
            id=10, dashboard_title="Sales Dashboard",
        )

        result = await dashboard_ops.add_chart_to_dashboard(
            dash_svc,
            dashboard_id=10,
            chart_ids=[5, 6],
            tab_label="Revenue",
        )

        assert result["id"] == 10
        assert result["message"] == "Added 2 chart(s) to dashboard"
        assert result["url"] == "/superset/dashboard/10/"
        dash_svc.add_charts_to_dashboard.assert_called_once_with(
            dashboard_id=10,
            chart_ids=[5, 6],
            tab_label="Revenue",
        )

    async def test_add_chart_to_dashboard_no_tab(self):
        dash_svc = AsyncMock()
        dash_svc.add_charts_to_dashboard.return_value = _make_dashboard(id=10)

        await dashboard_ops.add_chart_to_dashboard(
            dash_svc,
            dashboard_id=10,
            chart_ids=[7],
        )

        dash_svc.add_charts_to_dashboard.assert_called_once_with(
            dashboard_id=10,
            chart_ids=[7],
            tab_label=None,
        )

    # -- get_dashboard (non-tabbed) --

    async def test_get_dashboard_non_tabbed(self):
        position = {
            "CHART-abc": {
                "type": "CHART",
                "meta": {"chartId": 1},
            },
            "CHART-def": {
                "type": "CHART",
                "meta": {"chartId": 2},
            },
            "ROW-1": {
                "type": "ROW",
                "children": ["CHART-abc"],
            },
        }
        metadata = {"color_scheme": "bnbColors"}

        dash_svc = AsyncMock()
        dash_svc.get_dashboard.return_value = _make_dashboard(
            id=10,
            dashboard_title="Sales",
            published=True,
            slug="sales",
            css=".custom {}",
            charts=[{"id": 1}, {"id": 2}],
            position=position,
            metadata=metadata,
        )

        result = await dashboard_ops.get_dashboard(dash_svc, dashboard_id=10)

        assert result["id"] == 10
        assert result["title"] == "Sales"
        assert result["published"] is True
        assert result["slug"] == "sales"
        assert result["css"] == ".custom {}"
        assert set(result["chart_ids"]) == {1, 2}
        assert result["color_scheme"] == "bnbColors"
        # Non-tabbed layout should not have "tabs" key
        assert "tabs" not in result

    async def test_get_dashboard_default_color_scheme(self):
        """When metadata has no color_scheme, default to supersetColors."""
        dash_svc = AsyncMock()
        dash_svc.get_dashboard.return_value = _make_dashboard(
            id=5,
            position={},
            metadata={},  # no color_scheme
        )

        result = await dashboard_ops.get_dashboard(dash_svc, dashboard_id=5)
        assert result["color_scheme"] == "supersetColors"

    # -- get_dashboard (tabbed) --

    async def test_get_dashboard_tabbed(self):
        position = {
            "TABS-root": {"type": "TABS", "children": ["TAB-1", "TAB-2"]},
            "TAB-1": {
                "type": "TAB",
                "meta": {"text": "Overview"},
                "children": ["ROW-1"],
            },
            "TAB-2": {
                "type": "TAB",
                "meta": {"text": "Details"},
                "children": ["ROW-2"],
            },
            "ROW-1": {
                "type": "ROW",
                "children": ["CHART-a"],
            },
            "ROW-2": {
                "type": "ROW",
                "children": ["CHART-b", "CHART-c"],
            },
            "CHART-a": {
                "type": "CHART",
                "meta": {"chartId": 10},
            },
            "CHART-b": {
                "type": "CHART",
                "meta": {"chartId": 20},
            },
            "CHART-c": {
                "type": "CHART",
                "meta": {"chartId": 30},
            },
        }
        metadata = {"color_scheme": "d3Category10"}

        dash_svc = AsyncMock()
        dash_svc.get_dashboard.return_value = _make_dashboard(
            id=15,
            dashboard_title="Tabbed",
            position=position,
            metadata=metadata,
        )

        result = await dashboard_ops.get_dashboard(dash_svc, dashboard_id=15)

        assert result["id"] == 15
        assert set(result["chart_ids"]) == {10, 20, 30}
        assert "tabs" in result
        assert result["tabs"]["Overview"] == [10]
        assert set(result["tabs"]["Details"]) == {20, 30}

    # -- update_dashboard --

    async def test_update_dashboard_title_only(self):
        dash_svc = AsyncMock()
        dash_svc.update_dashboard.return_value = _make_dashboard(
            id=10, dashboard_title="New Title",
        )

        result = await dashboard_ops.update_dashboard(
            dash_svc,
            dashboard_id=10,
            title="New Title",
        )

        assert result["id"] == 10
        assert result["title"] == "New Title"
        assert "Updated dashboard 'New Title' (ID: 10)" in result["message"]

        # Should NOT have fetched dashboard since no color_scheme
        dash_svc.get_dashboard.assert_not_called()

        from superset_ai.schemas.dashboards import DashboardUpdate

        call_args = dash_svc.update_dashboard.call_args
        spec = call_args[0][1]
        assert isinstance(spec, DashboardUpdate)
        assert spec.dashboard_title == "New Title"
        assert spec.json_metadata is None

    async def test_update_dashboard_with_color_scheme(self):
        """When color_scheme is set, it should read existing metadata and merge."""
        existing_metadata = {"existing_key": "value", "color_scheme": "old"}

        dash_svc = AsyncMock()
        dash_svc.get_dashboard.return_value = _make_dashboard(
            id=10,
            dashboard_title="Dash",
            metadata=existing_metadata,
        )
        dash_svc.update_dashboard.return_value = _make_dashboard(
            id=10, dashboard_title="Dash",
        )

        await dashboard_ops.update_dashboard(
            dash_svc,
            dashboard_id=10,
            color_scheme="googleCategory20c",
        )

        # Should have fetched the dashboard first
        dash_svc.get_dashboard.assert_called_once_with(10)

        spec = dash_svc.update_dashboard.call_args[0][1]
        parsed = json.loads(spec.json_metadata)
        # Existing key preserved
        assert parsed["existing_key"] == "value"
        # Color scheme updated
        assert parsed["color_scheme"] == "googleCategory20c"

    async def test_update_dashboard_all_fields(self):
        dash_svc = AsyncMock()
        dash_svc.update_dashboard.return_value = _make_dashboard(
            id=10, dashboard_title="Full",
        )

        await dashboard_ops.update_dashboard(
            dash_svc,
            dashboard_id=10,
            title="Full",
            slug="full",
            css=".x {}",
            published=True,
            owners=[1],
        )

        from superset_ai.schemas.dashboards import DashboardUpdate

        spec = dash_svc.update_dashboard.call_args[0][1]
        assert spec.dashboard_title == "Full"
        assert spec.slug == "full"
        assert spec.css == ".x {}"
        assert spec.published is True
        assert spec.owners == [1]
        assert spec.json_metadata is None  # no color_scheme

    # -- remove_chart_from_dashboard --

    async def test_remove_chart_from_dashboard(self):
        dash_svc = AsyncMock()
        dash_svc.remove_chart_from_dashboard.return_value = _make_dashboard(
            id=10, dashboard_title="Sales Dashboard",
        )

        result = await dashboard_ops.remove_chart_from_dashboard(
            dash_svc, dashboard_id=10, chart_id=5,
        )

        assert result["id"] == 10
        assert result["message"] == "Removed chart 5 from dashboard 'Sales Dashboard'"
        dash_svc.remove_chart_from_dashboard.assert_called_once_with(
            dashboard_id=10, chart_id=5,
        )

    # -- delete_dashboard --

    async def test_delete_dashboard_success(self):
        dash_svc = AsyncMock()
        dash_svc.get_dashboard.return_value = _make_dashboard(
            id=10, dashboard_title="Old Dashboard",
        )

        result = await dashboard_ops.delete_dashboard(dash_svc, dashboard_id=10)

        assert result["deleted"] is True
        assert result["dashboard_id"] == 10
        assert result["dashboard_name"] == "Old Dashboard"
        assert "Deleted dashboard" in result["message"]
        assert "error" not in result
        dash_svc.delete_dashboard.assert_called_once_with(10)

    async def test_delete_dashboard_failure(self):
        dash_svc = AsyncMock()
        dash_svc.get_dashboard.return_value = _make_dashboard(
            id=10, dashboard_title="Protected",
        )
        dash_svc.delete_dashboard.side_effect = Exception("Forbidden")

        result = await dashboard_ops.delete_dashboard(dash_svc, dashboard_id=10)

        assert result["deleted"] is False
        assert result["dashboard_name"] == "Protected"
        assert result["error"] == "Forbidden"
        assert "Failed to delete" in result["message"]

    async def test_delete_dashboard_get_fails_then_delete_succeeds(self):
        dash_svc = AsyncMock()
        dash_svc.get_dashboard.side_effect = Exception("Not found")

        result = await dashboard_ops.delete_dashboard(dash_svc, dashboard_id=99)

        assert result["deleted"] is True
        assert result["dashboard_name"] == "Dashboard 99"
        dash_svc.delete_dashboard.assert_called_once_with(99)

    async def test_delete_dashboard_get_fails_and_delete_fails(self):
        dash_svc = AsyncMock()
        dash_svc.get_dashboard.side_effect = Exception("Not found")
        dash_svc.delete_dashboard.side_effect = Exception("Server error")

        result = await dashboard_ops.delete_dashboard(dash_svc, dashboard_id=99)

        assert result["deleted"] is False
        assert result["dashboard_name"] == "Dashboard 99"
        assert result["error"] == "Server error"

    # -- delete_all_charts_and_dashboards --

    async def test_delete_all_charts_and_dashboards_all_succeed(self):
        chart_svc = AsyncMock()
        dash_svc = AsyncMock()

        dash_svc.list_dashboards.return_value = [
            _make_dashboard(id=1, dashboard_title="D1"),
            _make_dashboard(id=2, dashboard_title="D2"),
        ]
        chart_svc.list_charts.return_value = [
            _make_chart(id=10, slice_name="C1"),
            _make_chart(id=11, slice_name="C2"),
        ]

        result = await dashboard_ops.delete_all_charts_and_dashboards(
            chart_svc, dash_svc,
        )

        assert result["success"] is True
        assert result["dashboards_deleted_count"] == 2
        assert result["charts_deleted_count"] == 2
        assert "2 dashboards" in result["message"]
        assert "2 charts" in result["message"]

        details = result["details"]
        assert len(details["dashboards_deleted"]) == 2
        assert len(details["charts_deleted"]) == 2
        assert details["dashboards_failed"] == []
        assert details["charts_failed"] == []

    async def test_delete_all_charts_and_dashboards_with_failures(self):
        """Mix of successes and failures should report success=False."""
        chart_svc = AsyncMock()
        dash_svc = AsyncMock()

        dash_svc.list_dashboards.return_value = [
            _make_dashboard(id=1, dashboard_title="D1"),
            _make_dashboard(id=2, dashboard_title="D2"),
        ]
        # First dashboard deletes fine, second fails
        dash_svc.delete_dashboard.side_effect = [None, Exception("locked")]

        chart_svc.list_charts.return_value = [
            _make_chart(id=10, slice_name="C1"),
            _make_chart(id=11, slice_name="C2"),
            _make_chart(id=12, slice_name="C3"),
        ]
        # First and third chart delete fine, second fails
        chart_svc.delete_chart.side_effect = [None, Exception("in use"), None]

        result = await dashboard_ops.delete_all_charts_and_dashboards(
            chart_svc, dash_svc,
        )

        assert result["success"] is False
        assert result["dashboards_deleted_count"] == 1
        assert result["charts_deleted_count"] == 2

        details = result["details"]
        assert len(details["dashboards_deleted"]) == 1
        assert details["dashboards_deleted"][0] == {"id": 1, "title": "D1"}

        assert len(details["dashboards_failed"]) == 1
        assert details["dashboards_failed"][0]["id"] == 2
        assert details["dashboards_failed"][0]["error"] == "locked"

        assert len(details["charts_failed"]) == 1
        assert details["charts_failed"][0]["id"] == 11
        assert details["charts_failed"][0]["error"] == "in use"

    async def test_delete_all_charts_and_dashboards_empty(self):
        chart_svc = AsyncMock()
        dash_svc = AsyncMock()

        dash_svc.list_dashboards.return_value = []
        chart_svc.list_charts.return_value = []

        result = await dashboard_ops.delete_all_charts_and_dashboards(
            chart_svc, dash_svc,
        )

        assert result["success"] is True
        assert result["dashboards_deleted_count"] == 0
        assert result["charts_deleted_count"] == 0

    # -- add_filter_to_dashboard --

    async def test_add_filter_to_dashboard(self):
        dash_svc = AsyncMock()

        with patch("superset_ai.operations.dashboards.build_native_filter") as mock_build:
            mock_build.return_value = {
                "id": "NATIVE_FILTER-abc12345",
                "name": "Region",
                "filterType": "filter_select",
            }

            result = await dashboard_ops.add_filter_to_dashboard(
                dash_svc,
                dashboard_id=10,
                name="Region",
                filter_type="filter_select",
                dataset_id=5,
                column="region",
                exclude_chart_ids=[1],
                multi_select=True,
                default_to_first_item=False,
                description="Filter by region",
            )

        assert result["filter_id"] == "NATIVE_FILTER-abc12345"
        assert result["name"] == "Region"
        assert result["filter_type"] == "filter_select"
        assert result["dashboard_id"] == 10
        assert "Added filter" in result["message"]

        mock_build.assert_called_once_with(
            name="Region",
            filter_type="filter_select",
            dataset_id=5,
            column="region",
            exclude_chart_ids=[1],
            multi_select=True,
            default_to_first_item=False,
            description="Filter by region",
        )
        dash_svc.add_native_filter.assert_called_once_with(
            10,
            {"id": "NATIVE_FILTER-abc12345", "name": "Region", "filterType": "filter_select"},
        )

    async def test_add_filter_to_dashboard_defaults(self):
        dash_svc = AsyncMock()

        with patch("superset_ai.operations.dashboards.build_native_filter") as mock_build:
            mock_build.return_value = {"id": "NATIVE_FILTER-xyz"}

            await dashboard_ops.add_filter_to_dashboard(
                dash_svc,
                dashboard_id=10,
                name="Status",
            )

        mock_build.assert_called_once_with(
            name="Status",
            filter_type="filter_select",
            dataset_id=None,
            column=None,
            exclude_chart_ids=None,
            multi_select=True,
            default_to_first_item=False,
            description="",
        )

    # -- remove_filter_from_dashboard --

    async def test_remove_filter_from_dashboard(self):
        dash_svc = AsyncMock()

        result = await dashboard_ops.remove_filter_from_dashboard(
            dash_svc,
            dashboard_id=10,
            filter_id="NATIVE_FILTER-abc12345",
        )

        assert result["dashboard_id"] == 10
        assert result["filter_id"] == "NATIVE_FILTER-abc12345"
        assert "Removed filter" in result["message"]
        dash_svc.remove_native_filter.assert_called_once_with(
            10, "NATIVE_FILTER-abc12345",
        )

    # -- list_dashboard_filters --

    async def test_list_dashboard_filters(self):
        dash_svc = AsyncMock()
        dash_svc.list_native_filters.return_value = [
            {
                "id": "NATIVE_FILTER-aaa",
                "name": "Region",
                "filterType": "filter_select",
                "targets": [
                    {"datasetId": 5, "column": {"name": "region"}},
                ],
            },
            {
                "id": "NATIVE_FILTER-bbb",
                "name": "Time Range",
                "filterType": "filter_time",
                "targets": [{}],
            },
        ]

        result = await dashboard_ops.list_dashboard_filters(dash_svc, dashboard_id=10)

        assert len(result) == 2
        assert result[0] == {
            "filter_id": "NATIVE_FILTER-aaa",
            "name": "Region",
            "filter_type": "filter_select",
            "column": "region",
            "dataset_id": 5,
        }
        assert result[1] == {
            "filter_id": "NATIVE_FILTER-bbb",
            "name": "Time Range",
            "filter_type": "filter_time",
            "column": None,
            "dataset_id": None,
        }

    async def test_list_dashboard_filters_empty(self):
        dash_svc = AsyncMock()
        dash_svc.list_native_filters.return_value = []

        result = await dashboard_ops.list_dashboard_filters(dash_svc, dashboard_id=10)
        assert result == []

    async def test_list_dashboard_filters_no_targets(self):
        """Filter with empty targets list should handle gracefully."""
        dash_svc = AsyncMock()
        dash_svc.list_native_filters.return_value = [
            {
                "id": "NATIVE_FILTER-ccc",
                "name": "Broken",
                "filterType": "filter_select",
                "targets": [],
            },
        ]

        result = await dashboard_ops.list_dashboard_filters(dash_svc, dashboard_id=10)

        assert result[0]["column"] is None
        assert result[0]["dataset_id"] is None
