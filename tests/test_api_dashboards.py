"""Tests for DashboardService — API service layer for Superset dashboards."""

from __future__ import annotations

import json
from unittest.mock import AsyncMock

import pytest

from superset_ai.api.dashboards import DashboardService
from superset_ai.schemas.dashboards import (
    DashboardCreate,
    DashboardUpdate,
    generate_position_json,
    generate_tabbed_layout,
)

# ---------------------------------------------------------------------------
# Helpers: realistic API response dicts
# ---------------------------------------------------------------------------


def _dashboard_info_dict(
    *,
    id: int = 1,
    dashboard_title: str = "Sales Overview",
    published: bool = False,
) -> dict:
    return {
        "id": id,
        "dashboard_title": dashboard_title,
        "published": published,
        "owners": [],
    }


def _dashboard_detail_dict(
    *,
    id: int = 1,
    dashboard_title: str = "Sales Overview",
    published: bool = False,
    position_json: str | None = None,
    json_metadata: str | None = None,
    charts: list | None = None,
) -> dict:
    return {
        "id": id,
        "dashboard_title": dashboard_title,
        "published": published,
        "owners": [],
        "position_json": position_json,
        "json_metadata": json_metadata,
        "charts": charts or [],
    }


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture()
def client() -> AsyncMock:
    return AsyncMock()


@pytest.fixture()
def service(client: AsyncMock) -> DashboardService:
    return DashboardService(client)


# =========================================================================
# list_dashboards
# =========================================================================


class TestListDashboards:
    """Tests for DashboardService.list_dashboards."""

    @pytest.mark.asyncio
    async def test_returns_list_of_dashboard_info(self, service, client):
        """Should parse response into a list of DashboardInfo."""
        client.get.return_value = {
            "result": [
                _dashboard_info_dict(id=1, dashboard_title="A"),
                _dashboard_info_dict(id=2, dashboard_title="B"),
            ]
        }

        dashboards = await service.list_dashboards()

        assert len(dashboards) == 2
        assert dashboards[0].id == 1
        assert dashboards[0].dashboard_title == "A"
        assert dashboards[1].id == 2

    @pytest.mark.asyncio
    async def test_passes_pagination_params(self, service, client):
        """Should forward page/page_size to the API."""
        client.get.return_value = {"result": []}

        await service.list_dashboards(page=3, page_size=25)

        client.get.assert_called_once_with("/dashboard/", params={"page": 3, "page_size": 25})

    @pytest.mark.asyncio
    async def test_returns_empty_list_when_no_results(self, service, client):
        """Should return an empty list when API returns no results."""
        client.get.return_value = {"result": []}

        dashboards = await service.list_dashboards()

        assert dashboards == []


# =========================================================================
# get_dashboard
# =========================================================================


class TestGetDashboard:
    """Tests for DashboardService.get_dashboard."""

    @pytest.mark.asyncio
    async def test_returns_dashboard_detail(self, service, client):
        """Should parse response into DashboardDetail."""
        client.get.return_value = {
            "result": _dashboard_detail_dict(id=7, dashboard_title="My Dash")
        }

        dashboard = await service.get_dashboard(7)

        assert dashboard.id == 7
        assert dashboard.dashboard_title == "My Dash"
        client.get.assert_called_once_with("/dashboard/7")

    @pytest.mark.asyncio
    async def test_parses_position_json(self, service, client):
        """Should be able to parse position_json from the detail."""
        pos = generate_position_json([1, 2])
        client.get.return_value = {"result": _dashboard_detail_dict(id=7, position_json=pos)}

        dashboard = await service.get_dashboard(7)
        position = dashboard.get_position()

        assert "CHART-1" in position
        assert "CHART-2" in position


# =========================================================================
# create_dashboard
# =========================================================================


class TestCreateDashboard:
    """Tests for DashboardService.create_dashboard."""

    @pytest.mark.asyncio
    async def test_posts_and_fetches_detail(self, service, client):
        """Should POST, then GET the created dashboard."""
        client.post.return_value = {"id": 50}
        client.get.return_value = {
            "result": _dashboard_detail_dict(id=50, dashboard_title="New Dash")
        }

        spec = DashboardCreate(dashboard_title="New Dash")
        dashboard = await service.create_dashboard(spec)

        assert dashboard.id == 50
        client.post.assert_called_once()
        assert client.post.call_args[0][0] == "/dashboard/"

    @pytest.mark.asyncio
    async def test_fallback_when_no_id_returned(self, service, client):
        """Should validate from result when no id is returned."""
        raw = _dashboard_detail_dict(id=3)
        client.post.return_value = {"result": raw}

        spec = DashboardCreate(dashboard_title="Fallback")
        dashboard = await service.create_dashboard(spec)

        assert dashboard.id == 3


# =========================================================================
# update_dashboard
# =========================================================================


class TestUpdateDashboard:
    """Tests for DashboardService.update_dashboard."""

    @pytest.mark.asyncio
    async def test_puts_and_fetches_detail(self, service, client):
        """Should PUT the update, then GET the updated dashboard."""
        client.put.return_value = {}
        client.get.return_value = {
            "result": _dashboard_detail_dict(id=10, dashboard_title="Updated")
        }

        spec = DashboardUpdate(dashboard_title="Updated")
        dashboard = await service.update_dashboard(10, spec)

        assert dashboard.dashboard_title == "Updated"
        client.put.assert_called_once()
        assert client.put.call_args[0][0] == "/dashboard/10"

    @pytest.mark.asyncio
    async def test_excludes_none_fields_from_payload(self, service, client):
        """Should not include None fields in the PUT payload."""
        client.put.return_value = {}
        client.get.return_value = {"result": _dashboard_detail_dict(id=10)}

        spec = DashboardUpdate(dashboard_title="Only Title")
        await service.update_dashboard(10, spec)

        put_payload = client.put.call_args[1]["json"]
        assert "dashboard_title" in put_payload
        assert "slug" not in put_payload


# =========================================================================
# delete_dashboard
# =========================================================================


class TestDeleteDashboard:
    """Tests for DashboardService.delete_dashboard."""

    @pytest.mark.asyncio
    async def test_calls_delete_endpoint(self, service, client):
        """Should call DELETE on the correct endpoint."""
        client.delete.return_value = {}

        await service.delete_dashboard(42)

        client.delete.assert_called_once_with("/dashboard/42")


# =========================================================================
# create_dashboard_with_charts
# =========================================================================


class TestCreateDashboardWithCharts:
    """Tests for DashboardService.create_dashboard_with_charts."""

    @pytest.mark.asyncio
    async def test_creates_dashboard_and_associates_charts(self, service, client):
        """Should create dashboard, then associate each chart."""
        # create_dashboard -> POST returns id, then GET returns detail
        client.post.return_value = {"id": 20}

        detail = _dashboard_detail_dict(id=20, dashboard_title="With Charts")
        # get is called: once for create, twice for _associate (per chart)
        # Each _associate also calls get(/chart/N) and then put(/chart/N)
        chart_1_resp = {"result": {"dashboards": []}}
        chart_2_resp = {"result": {"dashboards": []}}

        client.get.side_effect = [
            {"result": detail},  # get_dashboard after create
            chart_1_resp,  # _associate_chart: get chart 1
            chart_2_resp,  # _associate_chart: get chart 2
        ]
        client.put.return_value = {}

        dashboard = await service.create_dashboard_with_charts(
            title="With Charts",
            chart_ids=[1, 2],
        )

        assert dashboard.id == 20
        # Should have called put to associate charts with dashboard
        put_calls = client.put.call_args_list
        # Two put calls: one for chart 1, one for chart 2
        assert len(put_calls) == 2
        assert put_calls[0][0][0] == "/chart/1"
        assert put_calls[1][0][0] == "/chart/2"

    @pytest.mark.asyncio
    async def test_grid_layout_option(self, service, client):
        """Should use grid layout when specified."""
        client.post.return_value = {"id": 21}
        detail = _dashboard_detail_dict(id=21)
        client.get.side_effect = [
            {"result": detail},
            {"result": {"dashboards": []}},
            {"result": {"dashboards": []}},
        ]
        client.put.return_value = {}

        await service.create_dashboard_with_charts(
            title="Grid",
            chart_ids=[1, 2],
            layout="grid",
            columns=2,
        )

        post_payload = client.post.call_args[1]["json"]
        position = json.loads(post_payload["position_json"])
        # Grid layout with 2 columns => chart width = 6
        assert position["CHART-1"]["meta"]["width"] == 6


# =========================================================================
# add_charts_to_dashboard
# =========================================================================


class TestAddChartsToDashboard:
    """Tests for DashboardService.add_charts_to_dashboard."""

    @pytest.mark.asyncio
    async def test_adds_charts_to_existing_dashboard(self, service, client):
        """Should add charts to the existing position layout."""
        existing_pos = generate_position_json([1])
        detail = _dashboard_detail_dict(id=10, position_json=existing_pos)

        # get calls: get_dashboard, then update_dashboard->get_dashboard, then _associate
        updated_detail = _dashboard_detail_dict(id=10)
        client.get.side_effect = [
            {"result": detail},  # get_dashboard (initial)
            {"result": updated_detail},  # get_dashboard (after update)
            {"result": {"dashboards": []}},  # _associate_chart: get chart 5
        ]
        client.put.side_effect = [{}, {}]  # update_dashboard, associate chart

        result = await service.add_charts_to_dashboard(10, [5])

        assert result.id == 10
        # First put is the dashboard position update
        put_calls = client.put.call_args_list
        dashboard_put = put_calls[0]
        assert dashboard_put[0][0] == "/dashboard/10"
        position_payload = json.loads(dashboard_put[1]["json"]["position_json"])
        assert "CHART-5" in position_payload
        assert "CHART-1" in position_payload  # original preserved

    @pytest.mark.asyncio
    async def test_preserves_tabbed_layout(self, service, client):
        """Should add charts to tabs when dashboard uses tabbed layout."""
        tabbed_pos = generate_tabbed_layout({"Overview": [1], "Details": [2]})
        detail = _dashboard_detail_dict(id=10, position_json=tabbed_pos)

        updated_detail = _dashboard_detail_dict(id=10)
        client.get.side_effect = [
            {"result": detail},
            {"result": updated_detail},
            {"result": {"dashboards": []}},
        ]
        client.put.side_effect = [{}, {}]

        await service.add_charts_to_dashboard(10, [99], tab_label="Details")

        dashboard_put = client.put.call_args_list[0]
        position_payload = json.loads(dashboard_put[1]["json"]["position_json"])
        assert "CHART-99" in position_payload


# =========================================================================
# find_by_title
# =========================================================================


class TestFindByTitle:
    """Tests for DashboardService.find_by_title."""

    @pytest.mark.asyncio
    async def test_returns_dashboard_when_found(self, service, client):
        """Should return DashboardInfo when a match exists."""
        client.get.return_value = {"result": [_dashboard_info_dict(id=5, dashboard_title="Sales")]}

        result = await service.find_by_title("Sales")

        assert result is not None
        assert result.id == 5
        # Verify the filter was passed
        call_params = client.get.call_args[1]["params"]
        q_value = json.loads(call_params["q"])
        assert q_value["filters"][0]["col"] == "dashboard_title"
        assert q_value["filters"][0]["value"] == "Sales"

    @pytest.mark.asyncio
    async def test_returns_none_when_not_found(self, service, client):
        """Should return None when no match exists."""
        client.get.return_value = {"result": []}

        result = await service.find_by_title("Nonexistent")

        assert result is None


# =========================================================================
# remove_chart_from_dashboard
# =========================================================================


class TestRemoveChartFromDashboard:
    """Tests for DashboardService.remove_chart_from_dashboard."""

    @pytest.mark.asyncio
    async def test_removes_chart_from_layout(self, service, client):
        """Should remove the chart from position_json."""
        existing_pos = generate_position_json([1, 2, 3])
        detail = _dashboard_detail_dict(id=10, position_json=existing_pos)

        updated_detail = _dashboard_detail_dict(id=10)
        chart_resp = {"result": {"dashboards": [{"id": 10}]}}
        client.get.side_effect = [
            {"result": detail},  # get_dashboard (initial)
            {"result": updated_detail},  # get_dashboard (after update)
            chart_resp,  # _disassociate: get chart
        ]
        client.put.side_effect = [{}, {}]

        await service.remove_chart_from_dashboard(10, 2)

        # First put is the dashboard position update
        dashboard_put = client.put.call_args_list[0]
        position_payload = json.loads(dashboard_put[1]["json"]["position_json"])
        assert "CHART-2" not in position_payload
        assert "CHART-1" in position_payload
        assert "CHART-3" in position_payload


# =========================================================================
# Native filter methods
# =========================================================================


class TestNativeFilterMethods:
    """Tests for native filter management methods."""

    @pytest.mark.asyncio
    async def test_add_native_filter(self, service, client):
        """Should append a filter to json_metadata."""
        metadata = json.dumps({"native_filter_configuration": []})
        detail = _dashboard_detail_dict(id=10, json_metadata=metadata)

        updated_detail = _dashboard_detail_dict(id=10)
        client.get.side_effect = [
            {"result": detail},
            {"result": updated_detail},
        ]
        client.put.return_value = {}

        filter_config = {"id": "NATIVE_FILTER-abc", "name": "Country"}
        await service.add_native_filter(10, filter_config)

        put_payload = client.put.call_args[1]["json"]
        new_metadata = json.loads(put_payload["json_metadata"])
        assert len(new_metadata["native_filter_configuration"]) == 1
        assert new_metadata["native_filter_configuration"][0]["name"] == "Country"

    @pytest.mark.asyncio
    async def test_remove_native_filter(self, service, client):
        """Should remove a filter by ID from json_metadata."""
        filters = [
            {"id": "NATIVE_FILTER-aaa", "name": "A"},
            {"id": "NATIVE_FILTER-bbb", "name": "B"},
        ]
        metadata = json.dumps({"native_filter_configuration": filters})
        detail = _dashboard_detail_dict(id=10, json_metadata=metadata)

        updated_detail = _dashboard_detail_dict(id=10)
        client.get.side_effect = [
            {"result": detail},
            {"result": updated_detail},
        ]
        client.put.return_value = {}

        await service.remove_native_filter(10, "NATIVE_FILTER-aaa")

        put_payload = client.put.call_args[1]["json"]
        new_metadata = json.loads(put_payload["json_metadata"])
        remaining = new_metadata["native_filter_configuration"]
        assert len(remaining) == 1
        assert remaining[0]["id"] == "NATIVE_FILTER-bbb"

    @pytest.mark.asyncio
    async def test_list_native_filters(self, service, client):
        """Should return the list of native filter configs."""
        filters = [{"id": "f1", "name": "X"}, {"id": "f2", "name": "Y"}]
        metadata = json.dumps({"native_filter_configuration": filters})
        detail = _dashboard_detail_dict(id=10, json_metadata=metadata)
        client.get.return_value = {"result": detail}

        result = await service.list_native_filters(10)

        assert len(result) == 2
        assert result[0]["name"] == "X"
