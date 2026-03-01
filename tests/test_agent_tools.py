"""Tests for the agent tools layer.

Verifies context management, the ALL_TOOLS registry, and wiring for
representative tools (correct delegation, session caching, asset tracking).
"""

from __future__ import annotations

import contextvars
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from superset_ai.agent.tools import (
    ALL_TOOLS,
    _tool_context_var,
    get_tool_context,
    set_tool_context,
    # Discovery tools
    list_databases,
    list_schemas,
    list_tables,
    get_dataset_columns,
    list_existing_datasets,
    execute_sql,
    profile_dataset,
    # Dataset tools
    find_or_create_dataset,
    # Chart creation tools
    create_bar_chart,
    create_line_chart,
    create_pie_chart,
    create_dashboard,
    # Chart management tools
    list_all_charts,
    get_chart,
    update_chart,
    delete_chart,
    # Dashboard tools
    list_all_dashboards,
    create_tabbed_dashboard,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def make_mock_context():
    """Create a mock ToolContext with all required service attributes."""
    ctx = MagicMock()
    ctx.charts = AsyncMock()
    ctx.dashboards = AsyncMock()
    ctx.datasets = AsyncMock()
    ctx.databases = AsyncMock()
    ctx.session = MagicMock()
    ctx.session.superset_context = MagicMock()
    ctx.session.superset_context.databases = None
    ctx.session.superset_context.discovered_tables = {}
    ctx.session.superset_context.discovered_columns = {}
    return ctx


@pytest.fixture(autouse=True)
def _reset_tool_context():
    """Reset the ContextVar after each test to avoid cross-test leakage."""
    token = _tool_context_var.set(None)
    yield
    _tool_context_var.reset(token)


# =========================================================================
# A. Context management
# =========================================================================

class TestContextManagement:
    """Tests for set_tool_context / get_tool_context."""

    def test_roundtrip(self):
        """set_tool_context followed by get_tool_context returns same object."""
        ctx = make_mock_context()
        set_tool_context(ctx)
        assert get_tool_context() is ctx

    def test_get_raises_when_not_set(self):
        """get_tool_context raises RuntimeError when nothing has been set."""
        # Replace the module-level ContextVar with a fresh one that has
        # never been set, so .get() raises LookupError.
        fresh_var = contextvars.ContextVar("_fresh")
        with patch(f"{OPS_BASE}._tool_context_var", fresh_var):
            with pytest.raises(RuntimeError, match="Tool context not set"):
                get_tool_context()

    def test_overwrite_context(self):
        """Setting context twice replaces the previous value."""
        ctx1 = make_mock_context()
        ctx2 = make_mock_context()
        set_tool_context(ctx1)
        set_tool_context(ctx2)
        assert get_tool_context() is ctx2


# =========================================================================
# B. ALL_TOOLS registry
# =========================================================================

class TestAllToolsList:
    """Tests for the ALL_TOOLS registry."""

    def test_expected_count(self):
        """ALL_TOOLS should contain exactly 39 tools."""
        assert len(ALL_TOOLS) == 39

    def test_all_entries_are_callable(self):
        """Every entry in ALL_TOOLS must be callable."""
        for t in ALL_TOOLS:
            assert callable(t), f"{t!r} is not callable"

    def test_no_duplicates(self):
        """ALL_TOOLS must not contain duplicate entries."""
        seen_ids = set()
        for t in ALL_TOOLS:
            assert id(t) not in seen_ids, f"Duplicate tool: {t!r}"
            seen_ids.add(id(t))

    def test_unique_names(self):
        """Every tool should have a unique name."""
        names = [t.name for t in ALL_TOOLS]
        assert len(names) == len(set(names)), f"Duplicate names: {names}"


# =========================================================================
# C. Tool wiring tests
# =========================================================================

OPS_BASE = "superset_ai.agent.tools"


class TestDiscoveryToolWiring:
    """Wiring tests for discovery tools."""

    async def test_list_databases_delegates_and_caches(self):
        """list_databases calls discovery_ops.list_databases and caches result."""
        ctx = make_mock_context()
        set_tool_context(ctx)

        fake_result = [{"id": 1, "database_name": "main", "backend": "sqlite"}]

        with patch(
            f"{OPS_BASE}.discovery_ops.list_databases",
            new_callable=AsyncMock,
            return_value=fake_result,
        ) as mock_op:
            result = await list_databases.ainvoke({})

        mock_op.assert_awaited_once_with(ctx.databases)
        assert result == fake_result
        assert ctx.session.superset_context.databases == fake_result

    async def test_list_schemas_delegates(self):
        """list_schemas calls discovery_ops.list_schemas with correct args."""
        ctx = make_mock_context()
        set_tool_context(ctx)

        fake_result = ["public", "information_schema"]

        with patch(
            f"{OPS_BASE}.discovery_ops.list_schemas",
            new_callable=AsyncMock,
            return_value=fake_result,
        ) as mock_op:
            result = await list_schemas.ainvoke({"database_id": 5})

        mock_op.assert_awaited_once_with(ctx.databases, 5)
        assert result == fake_result

    async def test_list_tables_delegates_and_caches(self):
        """list_tables caches table names in session context."""
        ctx = make_mock_context()
        set_tool_context(ctx)

        fake_result = [
            {"name": "orders", "schema": "public", "type": "table"},
            {"name": "users", "schema": "public", "type": "table"},
        ]

        with patch(
            f"{OPS_BASE}.discovery_ops.list_tables",
            new_callable=AsyncMock,
            return_value=fake_result,
        ) as mock_op:
            result = await list_tables.ainvoke({"database_id": 3, "schema_name": "public"})

        mock_op.assert_awaited_once_with(ctx.databases, 3, "public")
        assert result == fake_result
        assert ctx.session.superset_context.discovered_tables[3] == ["orders", "users"]

    async def test_get_dataset_columns_delegates_and_caches(self):
        """get_dataset_columns caches column names in session context."""
        ctx = make_mock_context()
        set_tool_context(ctx)

        fake_result = {
            "columns": [
                {"name": "id", "type": "INTEGER"},
                {"name": "amount", "type": "FLOAT"},
            ]
        }

        with patch(
            f"{OPS_BASE}.discovery_ops.get_dataset_columns",
            new_callable=AsyncMock,
            return_value=fake_result,
        ) as mock_op:
            result = await get_dataset_columns.ainvoke({"dataset_id": 42})

        mock_op.assert_awaited_once_with(ctx.datasets, 42)
        assert result == fake_result
        assert ctx.session.superset_context.discovered_columns[42] == ["id", "amount"]

    async def test_list_existing_datasets_delegates(self):
        """list_existing_datasets delegates to discovery_ops correctly."""
        ctx = make_mock_context()
        set_tool_context(ctx)

        fake_result = [{"id": 10, "table_name": "sales"}]

        with patch(
            f"{OPS_BASE}.discovery_ops.list_existing_datasets",
            new_callable=AsyncMock,
            return_value=fake_result,
        ) as mock_op:
            result = await list_existing_datasets.ainvoke({"database_id": 2})

        mock_op.assert_awaited_once_with(ctx.datasets, 2)
        assert result == fake_result

    async def test_execute_sql_delegates(self):
        """execute_sql delegates to discovery_ops.execute_sql with correct args."""
        ctx = make_mock_context()
        set_tool_context(ctx)

        fake_result = {"columns": ["x"], "data": [[1]], "row_count": 1, "truncated": False}

        with patch(
            f"{OPS_BASE}.discovery_ops.execute_sql",
            new_callable=AsyncMock,
            return_value=fake_result,
        ) as mock_op:
            result = await execute_sql.ainvoke({
                "database_id": 1,
                "sql": "SELECT 1 AS x",
                "limit": 50,
            })

        mock_op.assert_awaited_once_with(ctx.databases, 1, "SELECT 1 AS x", 50)
        assert result == fake_result

    async def test_profile_dataset_delegates(self):
        """profile_dataset delegates to discovery_ops.profile_dataset with correct args."""
        ctx = make_mock_context()
        set_tool_context(ctx)

        fake_result = {
            "dataset_id": 10,
            "table_name": "orders",
            "row_count": 100,
            "columns": [{"name": "id", "cardinality": 100}],
        }

        with patch(
            f"{OPS_BASE}.discovery_ops.profile_dataset",
            new_callable=AsyncMock,
            return_value=fake_result,
        ) as mock_op:
            result = await profile_dataset.ainvoke({
                "dataset_id": 10,
                "sample_size": 3,
            })

        mock_op.assert_awaited_once_with(ctx.databases, ctx.datasets, 10, 3)
        assert result == fake_result


class TestDatasetToolWiring:
    """Wiring tests for dataset tools."""

    async def test_find_or_create_dataset_delegates_and_tracks(self):
        """find_or_create_dataset delegates and calls add_asset."""
        ctx = make_mock_context()
        set_tool_context(ctx)

        fake_result = {
            "id": 7,
            "table_name": "events",
            "columns": ["id", "ts", "type"],
            "time_columns": ["ts"],
        }

        with patch(
            f"{OPS_BASE}.dataset_ops.find_or_create_dataset",
            new_callable=AsyncMock,
            return_value=fake_result,
        ) as mock_op:
            result = await find_or_create_dataset.ainvoke(
                {"database_id": 1, "table_name": "events", "schema_name": "public"}
            )

        mock_op.assert_awaited_once_with(ctx.datasets, 1, "events", "public")
        assert result == fake_result
        ctx.session.add_asset.assert_called_once_with("dataset", 7, "events")


class TestChartToolWiring:
    """Wiring tests for chart creation tools."""

    async def test_create_bar_chart_delegates_and_tracks(self):
        """create_bar_chart delegates to chart_ops and calls add_asset."""
        ctx = make_mock_context()
        set_tool_context(ctx)

        fake_result = {"id": 100, "title": "Revenue by Region", "type": "echarts_bar", "url": "/chart/100/"}

        with patch(
            f"{OPS_BASE}.chart_ops.create_bar_chart",
            new_callable=AsyncMock,
            return_value=fake_result,
        ) as mock_op:
            result = await create_bar_chart.ainvoke({
                "title": "Revenue by Region",
                "dataset_id": 5,
                "metrics": ["SUM(revenue)"],
                "dimensions": ["region"],
            })

        mock_op.assert_awaited_once_with(
            ctx.charts,
            "Revenue by Region",
            5,
            ["SUM(revenue)"],
            ["region"],
            "No filter",
        )
        assert result == fake_result
        ctx.session.add_asset.assert_called_once_with("chart", 100, "Revenue by Region")

    async def test_create_line_chart_delegates_and_tracks(self):
        """create_line_chart delegates to chart_ops and calls add_asset."""
        ctx = make_mock_context()
        set_tool_context(ctx)

        fake_result = {"id": 101, "title": "Sales Trend", "type": "echarts_timeseries_line", "url": "/chart/101/"}

        with patch(
            f"{OPS_BASE}.chart_ops.create_line_chart",
            new_callable=AsyncMock,
            return_value=fake_result,
        ) as mock_op:
            result = await create_line_chart.ainvoke({
                "title": "Sales Trend",
                "dataset_id": 5,
                "metrics": ["SUM(amount)"],
                "time_column": "order_date",
            })

        mock_op.assert_awaited_once_with(
            ctx.charts,
            "Sales Trend",
            5,
            ["SUM(amount)"],
            "order_date",
            None,
            "P1D",
            "Last 30 days",
        )
        assert result == fake_result
        ctx.session.add_asset.assert_called_once_with("chart", 101, "Sales Trend")

    async def test_create_pie_chart_delegates_and_tracks(self):
        """create_pie_chart delegates to chart_ops and calls add_asset."""
        ctx = make_mock_context()
        set_tool_context(ctx)

        fake_result = {"id": 102, "title": "Market Share", "type": "pie", "url": "/chart/102/"}

        with patch(
            f"{OPS_BASE}.chart_ops.create_pie_chart",
            new_callable=AsyncMock,
            return_value=fake_result,
        ) as mock_op:
            result = await create_pie_chart.ainvoke({
                "title": "Market Share",
                "dataset_id": 5,
                "metric": "SUM(revenue)",
                "dimension": "company",
            })

        mock_op.assert_awaited_once_with(
            ctx.charts,
            "Market Share",
            5,
            "SUM(revenue)",
            "company",
            "No filter",
        )
        assert result == fake_result
        ctx.session.add_asset.assert_called_once_with("chart", 102, "Market Share")


class TestChartManagementToolWiring:
    """Wiring tests for chart management tools (list, get, update, delete)."""

    async def test_list_all_charts_delegates(self):
        """list_all_charts delegates to chart_ops.list_all_charts."""
        ctx = make_mock_context()
        set_tool_context(ctx)

        fake_result = [{"id": 1, "title": "C1"}, {"id": 2, "title": "C2"}]

        with patch(
            f"{OPS_BASE}.chart_ops.list_all_charts",
            new_callable=AsyncMock,
            return_value=fake_result,
        ) as mock_op:
            result = await list_all_charts.ainvoke({})

        mock_op.assert_awaited_once_with(ctx.charts)
        assert result == fake_result

    async def test_get_chart_delegates(self):
        """get_chart delegates to chart_ops.get_chart."""
        ctx = make_mock_context()
        set_tool_context(ctx)

        fake_result = {"id": 55, "title": "My Chart", "type": "bar"}

        with patch(
            f"{OPS_BASE}.chart_ops.get_chart",
            new_callable=AsyncMock,
            return_value=fake_result,
        ) as mock_op:
            result = await get_chart.ainvoke({"chart_id": 55})

        mock_op.assert_awaited_once_with(ctx.charts, 55)
        assert result == fake_result

    async def test_update_chart_with_title_tracks_asset(self):
        """update_chart with title calls add_asset."""
        ctx = make_mock_context()
        set_tool_context(ctx)

        fake_result = {"id": 55, "title": "Renamed Chart"}

        with patch(
            f"{OPS_BASE}.chart_ops.update_chart",
            new_callable=AsyncMock,
            return_value=fake_result,
        ) as mock_op:
            result = await update_chart.ainvoke({
                "chart_id": 55,
                "title": "Renamed Chart",
            })

        mock_op.assert_awaited_once_with(
            ctx.charts, 55,
            title="Renamed Chart",
            description=None,
            cache_timeout=None,
            owners=None,
            dashboards=None,
        )
        assert result == fake_result
        ctx.session.add_asset.assert_called_once_with("chart", 55, "Renamed Chart")

    async def test_update_chart_without_title_no_asset_tracking(self):
        """update_chart without title does NOT call add_asset."""
        ctx = make_mock_context()
        set_tool_context(ctx)

        fake_result = {"id": 55, "title": "Same Title"}

        with patch(
            f"{OPS_BASE}.chart_ops.update_chart",
            new_callable=AsyncMock,
            return_value=fake_result,
        ):
            await update_chart.ainvoke({"chart_id": 55, "description": "new desc"})

        ctx.session.add_asset.assert_not_called()

    async def test_delete_chart_delegates(self):
        """delete_chart delegates to chart_ops.delete_chart."""
        ctx = make_mock_context()
        set_tool_context(ctx)

        fake_result = {"message": "OK"}

        with patch(
            f"{OPS_BASE}.chart_ops.delete_chart",
            new_callable=AsyncMock,
            return_value=fake_result,
        ) as mock_op:
            result = await delete_chart.ainvoke({"chart_id": 99})

        mock_op.assert_awaited_once_with(ctx.charts, 99)
        assert result == fake_result


class TestDashboardToolWiring:
    """Wiring tests for dashboard tools."""

    async def test_create_dashboard_delegates_and_tracks(self):
        """create_dashboard delegates, calls add_asset, and sets active dashboard."""
        ctx = make_mock_context()
        set_tool_context(ctx)

        fake_result = {
            "id": 200,
            "title": "Sales Dashboard",
            "url": "/superset/dashboard/200/",
            "charts_included": [100, 101],
            "color_scheme": "supersetColors",
        }

        with patch(
            f"{OPS_BASE}.dashboard_ops.create_dashboard",
            new_callable=AsyncMock,
            return_value=fake_result,
        ) as mock_op:
            result = await create_dashboard.ainvoke({
                "title": "Sales Dashboard",
                "chart_ids": [100, 101],
            })

        mock_op.assert_awaited_once_with(
            ctx.dashboards,
            "Sales Dashboard",
            [100, 101],
            "vertical",
            "supersetColors",
        )
        assert result == fake_result
        ctx.session.add_asset.assert_called_once_with("dashboard", 200, "Sales Dashboard")
        assert ctx.session.active_dashboard_id == 200
        assert ctx.session.active_dashboard_title == "Sales Dashboard"

    async def test_create_tabbed_dashboard_delegates_and_tracks(self):
        """create_tabbed_dashboard delegates, tracks asset, and sets active dashboard."""
        ctx = make_mock_context()
        set_tool_context(ctx)

        fake_result = {
            "id": 201,
            "title": "Tabbed Dash",
            "url": "/superset/dashboard/201/",
        }

        tabs = {"Overview": [1, 2], "Details": [3]}

        with patch(
            f"{OPS_BASE}.dashboard_ops.create_tabbed_dashboard",
            new_callable=AsyncMock,
            return_value=fake_result,
        ) as mock_op:
            result = await create_tabbed_dashboard.ainvoke({
                "title": "Tabbed Dash",
                "tabs": tabs,
            })

        mock_op.assert_awaited_once_with(
            ctx.dashboards,
            "Tabbed Dash",
            tabs,
            "supersetColors",
        )
        assert result == fake_result
        ctx.session.add_asset.assert_called_once_with("dashboard", 201, "Tabbed Dash")
        assert ctx.session.active_dashboard_id == 201
        assert ctx.session.active_dashboard_title == "Tabbed Dash"

    async def test_list_all_dashboards_delegates(self):
        """list_all_dashboards delegates to dashboard_ops.list_all_dashboards."""
        ctx = make_mock_context()
        set_tool_context(ctx)

        fake_result = [{"id": 1, "title": "D1", "published": True}]

        with patch(
            f"{OPS_BASE}.dashboard_ops.list_all_dashboards",
            new_callable=AsyncMock,
            return_value=fake_result,
        ) as mock_op:
            result = await list_all_dashboards.ainvoke({})

        mock_op.assert_awaited_once_with(ctx.dashboards)
        assert result == fake_result
