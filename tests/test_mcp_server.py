"""Tests for the MCP server layer.

Covers:
A. The _handle_errors decorator — verifies each exception type maps to the
   correct structured error dict.
B. Tool wiring — representative tools correctly delegate to operation
   functions with the right service instances.
C. Tool registration — the FastMCP server has the expected number of tools.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, patch

import pytest

from superset_ai.core.exceptions import (
    AuthenticationError,
    PermissionDeniedError,
    ResourceNotFoundError,
    SupersetAIError,
    ValidationError,
)
from superset_ai.mcp.server import _error_response, _handle_errors, mcp


# =========================================================================
# A. Error handling decorator tests
# =========================================================================


class TestErrorResponse:
    """Tests for the _error_response helper."""

    def test_basic_error_response(self):
        result = _error_response("something broke", "some_type")
        assert result == {"error": "something broke", "error_type": "some_type"}

    def test_error_response_with_extras(self):
        result = _error_response("oops", "test", foo="bar", count=3)
        assert result == {
            "error": "oops",
            "error_type": "test",
            "foo": "bar",
            "count": 3,
        }


class TestHandleErrors:
    """Tests for the _handle_errors decorator."""

    async def test_success_passes_through(self):
        """When the wrapped function succeeds, its return value is passed through."""

        @_handle_errors
        async def good_fn():
            return {"ok": True}

        result = await good_fn()
        assert result == {"ok": True}

    async def test_resource_not_found_error(self):
        @_handle_errors
        async def fn():
            raise ResourceNotFoundError(resource_type="chart", resource_id=42)

        result = await fn()
        assert result["error_type"] == "not_found"
        assert result["resource_type"] == "chart"
        assert result["resource_id"] == 42
        assert "chart" in result["error"]
        assert "42" in result["error"]

    async def test_validation_error(self):
        @_handle_errors
        async def fn():
            raise ValidationError(message="bad data", status_code=400)

        result = await fn()
        assert result["error_type"] == "validation_error"
        assert result["status_code"] == 400
        assert "bad data" in result["error"]

    async def test_validation_error_with_422(self):
        @_handle_errors
        async def fn():
            raise ValidationError(message="invalid payload", status_code=422)

        result = await fn()
        assert result["error_type"] == "validation_error"
        assert result["status_code"] == 422

    async def test_authentication_error(self):
        @_handle_errors
        async def fn():
            raise AuthenticationError("bad creds")

        result = await fn()
        assert result["error_type"] == "authentication_error"
        assert "bad creds" in result["error"]
        assert "hint" in result
        assert "SUPERSET_AI_SUPERSET_USERNAME" in result["hint"]

    async def test_authentication_error_default_message(self):
        @_handle_errors
        async def fn():
            raise AuthenticationError()

        result = await fn()
        assert result["error_type"] == "authentication_error"
        assert "Authentication failed" in result["error"]

    async def test_permission_denied_error(self):
        @_handle_errors
        async def fn():
            raise PermissionDeniedError("no access")

        result = await fn()
        assert result["error_type"] == "permission_denied"
        assert "no access" in result["error"]
        # Should NOT have a hint key (unlike auth errors)
        assert "hint" not in result

    async def test_superset_ai_error(self):
        @_handle_errors
        async def fn():
            raise SupersetAIError("generic")

        result = await fn()
        assert result["error_type"] == "superset_error"
        assert "generic" in result["error"]

    async def test_unexpected_exception(self):
        @_handle_errors
        async def fn():
            raise RuntimeError("unexpected")

        result = await fn()
        assert result["error_type"] == "internal_error"
        assert "unexpected" in result["error"]

    async def test_preserves_function_name(self):
        """functools.wraps should preserve the original function's metadata."""

        @_handle_errors
        async def my_special_function():
            pass

        assert my_special_function.__name__ == "my_special_function"

    async def test_args_forwarded(self):
        """Arguments and keyword arguments are forwarded to the wrapped function."""

        @_handle_errors
        async def fn(a, b, key=None):
            return {"a": a, "b": b, "key": key}

        result = await fn(1, 2, key="hello")
        assert result == {"a": 1, "b": 2, "key": "hello"}

    async def test_exception_ordering_resource_not_found_before_superset(self):
        """ResourceNotFoundError is a subclass of SupersetAIError;
        the decorator must catch it specifically, not as generic SupersetAIError."""

        @_handle_errors
        async def fn():
            raise ResourceNotFoundError(resource_type="dataset", resource_id=99)

        result = await fn()
        # Must be "not_found", not "superset_error"
        assert result["error_type"] == "not_found"

    async def test_exception_ordering_validation_before_superset(self):
        """ValidationError is also a subclass of SupersetAIError."""

        @_handle_errors
        async def fn():
            raise ValidationError(message="bad")

        result = await fn()
        assert result["error_type"] == "validation_error"


# =========================================================================
# B. Tool wiring tests
# =========================================================================


class TestToolWiring:
    """Verify that MCP tool functions delegate to the correct operations."""

    async def test_list_databases_tool(self):
        mock_db_svc = AsyncMock()
        mock_chart_svc = AsyncMock()
        mock_dash_svc = AsyncMock()
        mock_ds_svc = AsyncMock()

        with patch(
            "superset_ai.mcp.server._get_services",
            new_callable=AsyncMock,
            return_value=(mock_chart_svc, mock_dash_svc, mock_ds_svc, mock_db_svc),
        ):
            with patch(
                "superset_ai.mcp.server.discovery_ops.list_databases",
                new_callable=AsyncMock,
                return_value=[{"id": 1, "name": "examples"}],
            ) as mock_op:
                from superset_ai.mcp.server import list_databases

                result = await list_databases()
                mock_op.assert_called_once_with(mock_db_svc)
                assert result == [{"id": 1, "name": "examples"}]

    async def test_create_bar_chart_tool(self):
        mock_chart_svc = AsyncMock()
        mock_dash_svc = AsyncMock()
        mock_ds_svc = AsyncMock()
        mock_db_svc = AsyncMock()

        with patch(
            "superset_ai.mcp.server._get_services",
            new_callable=AsyncMock,
            return_value=(mock_chart_svc, mock_dash_svc, mock_ds_svc, mock_db_svc),
        ):
            with patch(
                "superset_ai.mcp.server.chart_ops.create_bar_chart",
                new_callable=AsyncMock,
                return_value={"id": 10, "title": "Sales"},
            ) as mock_op:
                from superset_ai.mcp.server import create_bar_chart

                result = await create_bar_chart(
                    title="Sales",
                    dataset_id=1,
                    metrics=["COUNT(*)"],
                    dimensions=["region"],
                    time_range="No filter",
                )
                mock_op.assert_called_once_with(
                    mock_chart_svc,
                    "Sales",
                    1,
                    ["COUNT(*)"],
                    ["region"],
                    "No filter",
                )
                assert result == {"id": 10, "title": "Sales"}

    async def test_create_dashboard_tool(self):
        mock_chart_svc = AsyncMock()
        mock_dash_svc = AsyncMock()
        mock_ds_svc = AsyncMock()
        mock_db_svc = AsyncMock()

        with patch(
            "superset_ai.mcp.server._get_services",
            new_callable=AsyncMock,
            return_value=(mock_chart_svc, mock_dash_svc, mock_ds_svc, mock_db_svc),
        ):
            with patch(
                "superset_ai.mcp.server.dashboard_ops.create_dashboard",
                new_callable=AsyncMock,
                return_value={"id": 5, "title": "My Dashboard"},
            ) as mock_op:
                from superset_ai.mcp.server import create_dashboard

                result = await create_dashboard(
                    title="My Dashboard",
                    chart_ids=[1, 2, 3],
                    layout="grid",
                    color_scheme="d3Category10",
                )
                mock_op.assert_called_once_with(
                    mock_dash_svc,
                    "My Dashboard",
                    [1, 2, 3],
                    "grid",
                    "d3Category10",
                )
                assert result == {"id": 5, "title": "My Dashboard"}

    async def test_add_filter_to_dashboard_tool(self):
        mock_chart_svc = AsyncMock()
        mock_dash_svc = AsyncMock()
        mock_ds_svc = AsyncMock()
        mock_db_svc = AsyncMock()

        with patch(
            "superset_ai.mcp.server._get_services",
            new_callable=AsyncMock,
            return_value=(mock_chart_svc, mock_dash_svc, mock_ds_svc, mock_db_svc),
        ):
            with patch(
                "superset_ai.mcp.server.dashboard_ops.add_filter_to_dashboard",
                new_callable=AsyncMock,
                return_value={"status": "ok", "filter_id": "NATIVE_FILTER-abc"},
            ) as mock_op:
                from superset_ai.mcp.server import add_filter_to_dashboard

                result = await add_filter_to_dashboard(
                    dashboard_id=5,
                    name="Region Filter",
                    filter_type="filter_select",
                    dataset_id=1,
                    column="region",
                )
                mock_op.assert_called_once_with(
                    mock_dash_svc,
                    5,
                    name="Region Filter",
                    filter_type="filter_select",
                    dataset_id=1,
                    column="region",
                    exclude_chart_ids=None,
                    multi_select=True,
                    default_to_first_item=False,
                    description="",
                )
                assert result["filter_id"] == "NATIVE_FILTER-abc"

    async def test_tool_propagates_error_via_decorator(self):
        """When _get_services raises, the _handle_errors decorator catches it."""
        with patch(
            "superset_ai.mcp.server._get_services",
            new_callable=AsyncMock,
            side_effect=AuthenticationError("bad token"),
        ):
            from superset_ai.mcp.server import list_databases

            result = await list_databases()
            assert result["error_type"] == "authentication_error"


# =========================================================================
# C. Tool registration test
# =========================================================================


class TestToolRegistration:
    """Verify that the FastMCP server has the expected tools registered."""

    async def test_expected_tool_count(self):
        tools = await mcp.list_tools()
        tool_names = [t.name for t in tools]
        assert len(tools) == 37, (
            f"Expected 37 registered tools, got {len(tools)}. "
            f"Registered: {sorted(tool_names)}"
        )

    async def test_known_tools_are_registered(self):
        tools = await mcp.list_tools()
        registered_names = {t.name for t in tools}
        expected_names = {
            "list_databases",
            "list_schemas",
            "list_tables",
            "get_dataset_columns",
            "list_existing_datasets",
            "find_or_create_dataset",
            "create_bar_chart",
            "create_line_chart",
            "create_pie_chart",
            "create_table_chart",
            "create_metric_chart",
            "create_area_chart",
            "create_big_number_trendline_chart",
            "create_timeseries_bar_chart",
            "create_bubble_chart",
            "create_funnel_chart",
            "create_gauge_chart",
            "create_treemap_chart",
            "create_histogram_chart",
            "create_box_plot_chart",
            "create_heatmap_chart",
            "list_all_charts",
            "get_chart",
            "update_chart",
            "delete_chart",
            "list_all_dashboards",
            "get_dashboard",
            "update_dashboard",
            "create_dashboard",
            "create_tabbed_dashboard",
            "add_chart_to_dashboard",
            "remove_chart_from_dashboard",
            "add_filter_to_dashboard",
            "remove_filter_from_dashboard",
            "list_dashboard_filters",
            "delete_dashboard",
            "delete_all_charts_and_dashboards",
        }
        missing = expected_names - registered_names
        extra = registered_names - expected_names
        assert not missing, f"Missing tools: {missing}"
        assert not extra, f"Unexpected tools: {extra}"
