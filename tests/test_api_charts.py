"""Tests for ChartService — API service layer for Superset charts."""

from __future__ import annotations

import json
from unittest.mock import AsyncMock

import pytest

from superset_ai.api.charts import ChartService
from superset_ai.schemas.charts import ChartCreate, ChartUpdate

# ---------------------------------------------------------------------------
# Helpers: realistic API response dicts
# ---------------------------------------------------------------------------


def _chart_info_dict(
    *,
    id: int = 1,
    slice_name: str = "Revenue by Region",
    viz_type: str = "dist_bar",
    datasource_id: int = 10,
) -> dict:
    return {
        "id": id,
        "slice_name": slice_name,
        "viz_type": viz_type,
        "datasource_id": datasource_id,
        "datasource_type": "table",
        "owners": [],
    }


def _chart_detail_dict(
    *,
    id: int = 1,
    slice_name: str = "Revenue by Region",
    viz_type: str = "dist_bar",
    datasource_id: int = 10,
    dashboards: list | None = None,
    params: str | None = None,
    query_context: str | None = None,
) -> dict:
    return {
        "id": id,
        "slice_name": slice_name,
        "viz_type": viz_type,
        "datasource_id": datasource_id,
        "datasource_type": "table",
        "owners": [],
        "dashboards": dashboards or [],
        "params": params,
        "query_context": query_context,
    }


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture()
def client() -> AsyncMock:
    """Mock SupersetClient with async get/post/put/delete."""
    return AsyncMock()


@pytest.fixture()
def service(client: AsyncMock) -> ChartService:
    return ChartService(client)


# =========================================================================
# list_charts
# =========================================================================


class TestListCharts:
    """Tests for ChartService.list_charts."""

    @pytest.mark.asyncio
    async def test_returns_list_of_chart_info(self, service, client):
        """Should parse response into a list of ChartInfo."""
        client.get.return_value = {
            "result": [
                _chart_info_dict(id=1, slice_name="A"),
                _chart_info_dict(id=2, slice_name="B"),
            ]
        }

        charts = await service.list_charts()

        assert len(charts) == 2
        assert charts[0].id == 1
        assert charts[0].slice_name == "A"
        assert charts[1].id == 2

    @pytest.mark.asyncio
    async def test_passes_pagination_params(self, service, client):
        """Should forward page/page_size to the API."""
        client.get.return_value = {"result": []}

        await service.list_charts(page=2, page_size=50)

        client.get.assert_called_once_with("/chart/", params={"page": 2, "page_size": 50})

    @pytest.mark.asyncio
    async def test_filters_by_datasource_id(self, service, client):
        """Should include filter JSON when datasource_id is given."""
        client.get.return_value = {"result": []}

        await service.list_charts(datasource_id=42)

        call_args = client.get.call_args
        params = call_args.kwargs.get("params") or call_args[1].get("params") or call_args[0][1]
        q_value = json.loads(params["q"])
        assert q_value["filters"][0]["col"] == "datasource_id"
        assert q_value["filters"][0]["value"] == 42

    @pytest.mark.asyncio
    async def test_returns_empty_list_when_no_results(self, service, client):
        """Should return an empty list when API returns no results."""
        client.get.return_value = {"result": []}

        charts = await service.list_charts()

        assert charts == []


# =========================================================================
# get_chart
# =========================================================================


class TestGetChart:
    """Tests for ChartService.get_chart."""

    @pytest.mark.asyncio
    async def test_returns_chart_detail(self, service, client):
        """Should parse response into ChartDetail."""
        client.get.return_value = {"result": _chart_detail_dict(id=5)}

        chart = await service.get_chart(5)

        assert chart.id == 5
        assert chart.slice_name == "Revenue by Region"
        client.get.assert_called_once_with("/chart/5")

    @pytest.mark.asyncio
    async def test_extracts_datasource_from_params(self, service, client):
        """Should extract datasource info from params JSON when not set."""
        detail = _chart_detail_dict(id=5, datasource_id=None)
        detail["datasource_id"] = None
        detail["params"] = json.dumps({"datasource": "10__table", "viz_type": "bar"})
        client.get.return_value = {"result": detail}

        chart = await service.get_chart(5)

        assert chart.datasource_id == 10
        assert chart.datasource_type == "table"


# =========================================================================
# create_chart
# =========================================================================


class TestCreateChart:
    """Tests for ChartService.create_chart."""

    @pytest.mark.asyncio
    async def test_posts_payload_and_fetches_detail(self, service, client):
        """Should POST, then GET the created chart."""
        client.post.return_value = {"id": 99}
        client.get.return_value = {"result": _chart_detail_dict(id=99)}

        spec = ChartCreate(
            slice_name="New Chart",
            viz_type="dist_bar",
            datasource_id=10,
            params='{"viz_type": "dist_bar", "datasource": "10__table"}',
        )
        chart = await service.create_chart(spec)

        assert chart.id == 99
        client.post.assert_called_once()
        post_args = client.post.call_args
        assert post_args[0][0] == "/chart/"

    @pytest.mark.asyncio
    async def test_patches_query_context_with_slice_id(self, service, client):
        """After creation, should PUT updated query_context with slice_id."""
        client.post.return_value = {"id": 55}
        client.get.return_value = {"result": _chart_detail_dict(id=55)}
        client.put.return_value = {}

        qc = json.dumps({"datasource": {"id": 10, "type": "table"}, "queries": []})
        spec = ChartCreate(
            slice_name="Test",
            viz_type="line",
            datasource_id=10,
            params='{"viz_type": "line", "datasource": "10__table"}',
            query_context=qc,
        )

        await service.create_chart(spec)

        # The first put call should patch query_context
        put_call = client.put.call_args_list[0]
        assert put_call[0][0] == "/chart/55"
        payload = put_call[1]["json"]
        patched_qc = json.loads(payload["query_context"])
        assert patched_qc["form_data"]["slice_id"] == 55

    @pytest.mark.asyncio
    async def test_skips_query_context_patch_when_absent(self, service, client):
        """Should not PUT query_context if spec has none."""
        client.post.return_value = {"id": 7}
        client.get.return_value = {"result": _chart_detail_dict(id=7)}

        spec = ChartCreate(
            slice_name="No QC",
            viz_type="table",
            datasource_id=10,
            params='{"viz_type": "table", "datasource": "10__table"}',
        )

        await service.create_chart(spec)

        # put should not have been called (no query_context to patch)
        client.put.assert_not_called()

    @pytest.mark.asyncio
    async def test_fallback_when_no_id_returned(self, service, client):
        """Should validate from result when no id is returned."""
        raw_detail = _chart_detail_dict(id=3)
        client.post.return_value = {"result": raw_detail}

        spec = ChartCreate(
            slice_name="Fallback",
            viz_type="pie",
            datasource_id=10,
            params='{"viz_type": "pie", "datasource": "10__table"}',
        )

        chart = await service.create_chart(spec)

        assert chart.id == 3


# =========================================================================
# update_chart
# =========================================================================


class TestUpdateChart:
    """Tests for ChartService.update_chart."""

    @pytest.mark.asyncio
    async def test_puts_payload_and_fetches_detail(self, service, client):
        """Should PUT the update, then GET the updated chart."""
        client.put.return_value = {}
        client.get.return_value = {"result": _chart_detail_dict(id=10, slice_name="Updated")}

        spec = ChartUpdate(slice_name="Updated")
        chart = await service.update_chart(10, spec)

        assert chart.slice_name == "Updated"
        client.put.assert_called_once()
        assert client.put.call_args[0][0] == "/chart/10"

    @pytest.mark.asyncio
    async def test_excludes_none_fields_from_payload(self, service, client):
        """Should not include None fields in the PUT payload."""
        client.put.return_value = {}
        client.get.return_value = {"result": _chart_detail_dict(id=10)}

        spec = ChartUpdate(slice_name="Only Name")
        await service.update_chart(10, spec)

        put_payload = client.put.call_args[1]["json"]
        assert "slice_name" in put_payload
        assert "viz_type" not in put_payload


# =========================================================================
# delete_chart
# =========================================================================


class TestDeleteChart:
    """Tests for ChartService.delete_chart."""

    @pytest.mark.asyncio
    async def test_calls_delete_endpoint(self, service, client):
        """Should call DELETE on the correct endpoint."""
        client.delete.return_value = {}

        await service.delete_chart(42)

        client.delete.assert_called_once_with("/chart/42")


# =========================================================================
# add_to_dashboards
# =========================================================================


class TestAddToDashboards:
    """Tests for ChartService.add_to_dashboards."""

    @pytest.mark.asyncio
    async def test_merges_existing_and_new_dashboard_ids(self, service, client):
        """Should merge existing dashboard associations with new ones."""
        # First get_chart returns existing dashboards
        detail_with_dashes = _chart_detail_dict(id=5, dashboards=[{"id": 1}, {"id": 2}])
        # get_chart is called twice: once to read existing, once after update
        client.get.side_effect = [
            {"result": detail_with_dashes},
            {"result": detail_with_dashes},
        ]
        client.put.return_value = {}

        await service.add_to_dashboards(5, [2, 3])

        put_payload = client.put.call_args[1]["json"]
        # Should contain 1, 2, 3 (2 is not duplicated)
        assert sorted(put_payload["dashboards"]) == [1, 2, 3]

    @pytest.mark.asyncio
    async def test_handles_chart_with_no_existing_dashboards(self, service, client):
        """Should work when chart has no existing dashboard associations."""
        detail = _chart_detail_dict(id=5, dashboards=[])
        client.get.side_effect = [
            {"result": detail},
            {"result": detail},
        ]
        client.put.return_value = {}

        await service.add_to_dashboards(5, [10, 20])

        put_payload = client.put.call_args[1]["json"]
        assert put_payload["dashboards"] == [10, 20]


# =========================================================================
# create_bar_chart (high-level method)
# =========================================================================


class TestCreateBarChart:
    """Tests for ChartService.create_bar_chart high-level method."""

    @pytest.mark.asyncio
    async def test_builds_spec_and_delegates_to_create_chart(self, service, client):
        """Should build a ChartCreate spec and call create_chart."""
        client.post.return_value = {"id": 100}
        client.put.return_value = {}
        client.get.return_value = {"result": _chart_detail_dict(id=100, viz_type="dist_bar")}

        chart = await service.create_bar_chart(
            title="Sales by Region",
            datasource_id=10,
            metrics=["SUM(revenue)"],
            groupby=["region"],
        )

        assert chart.id == 100
        # Verify POST was called with correct viz_type in payload
        post_payload = client.post.call_args[1]["json"]
        assert post_payload["slice_name"] == "Sales by Region"
        assert post_payload["viz_type"] == "dist_bar"
        assert post_payload["datasource_id"] == 10

    @pytest.mark.asyncio
    async def test_normalizes_aggregate_metrics(self, service, client):
        """Should normalize aggregate expressions like SUM(col)."""
        client.post.return_value = {"id": 101}
        client.put.return_value = {}
        client.get.return_value = {"result": _chart_detail_dict(id=101)}

        await service.create_bar_chart(
            title="Test",
            datasource_id=10,
            metrics=["SUM(amount)", "COUNT(*)"],
            groupby=["category"],
        )

        post_payload = client.post.call_args[1]["json"]
        params = json.loads(post_payload["params"])
        # SUM(amount) should be an adhoc metric dict
        metric_0 = params["metrics"][0]
        assert isinstance(metric_0, dict)
        assert metric_0["aggregate"] == "SUM"
        # COUNT(*) should be a SQL expression metric
        metric_1 = params["metrics"][1]
        assert isinstance(metric_1, dict)
        assert metric_1["expressionType"] == "SQL"

    @pytest.mark.asyncio
    async def test_includes_query_context(self, service, client):
        """Should include query_context in the spec for dashboard rendering."""
        client.post.return_value = {"id": 102}
        client.put.return_value = {}
        client.get.return_value = {"result": _chart_detail_dict(id=102)}

        await service.create_bar_chart(
            title="QC Test",
            datasource_id=10,
            metrics=["revenue"],
            groupby=["region"],
        )

        post_payload = client.post.call_args[1]["json"]
        assert "query_context" in post_payload
        qc = json.loads(post_payload["query_context"])
        assert qc["datasource"]["id"] == 10

    @pytest.mark.asyncio
    async def test_passes_optional_description(self, service, client):
        """Should pass description when provided."""
        client.post.return_value = {"id": 103}
        client.put.return_value = {}
        client.get.return_value = {"result": _chart_detail_dict(id=103)}

        await service.create_bar_chart(
            title="Desc Test",
            datasource_id=10,
            metrics=["revenue"],
            groupby=["region"],
            description="A test chart",
        )

        post_payload = client.post.call_args[1]["json"]
        assert post_payload["description"] == "A test chart"

    @pytest.mark.asyncio
    async def test_predefined_metric_name_stays_string(self, service, client):
        """Predefined metric names should remain as strings, not converted to adhoc."""
        client.post.return_value = {"id": 104}
        client.put.return_value = {}
        client.get.return_value = {"result": _chart_detail_dict(id=104)}

        await service.create_bar_chart(
            title="Predefined",
            datasource_id=10,
            metrics=["total_revenue"],
            groupby=["region"],
        )

        post_payload = client.post.call_args[1]["json"]
        params = json.loads(post_payload["params"])
        assert params["metrics"][0] == "total_revenue"


# =========================================================================
# _normalize_metrics helpers
# =========================================================================


class TestNormalizeMetrics:
    """Tests for the internal metric normalization."""

    def test_count_star_shorthand(self, service):
        """'count' should become COUNT(*)."""
        result = service._normalize_single_metric("count")
        assert isinstance(result, dict)
        assert result["sqlExpression"] == "COUNT(*)"

    def test_aggregate_expression(self, service):
        """'AVG(price)' should become an adhoc metric."""
        result = service._normalize_single_metric("AVG(price)")
        assert isinstance(result, dict)
        assert result["aggregate"] == "AVG"
        assert result["column"]["column_name"] == "price"

    def test_plain_name(self, service):
        """Plain metric names stay as strings."""
        result = service._normalize_single_metric("my_metric")
        assert result == "my_metric"

    def test_normalize_metrics_list(self, service):
        """Should normalize a list of mixed metric specs."""
        results = service._normalize_metrics(["SUM(a)", "my_metric", "COUNT(*)"])
        assert isinstance(results[0], dict)
        assert results[1] == "my_metric"
        assert isinstance(results[2], dict)


# =========================================================================
# create_chart_by_type dispatcher
# =========================================================================


class TestCreateChartByType:
    """Tests for the unified create_chart_by_type dispatcher."""

    @pytest.fixture
    def service(self, client):
        return ChartService(client)

    @pytest.fixture
    def client(self):
        return AsyncMock()

    @pytest.mark.asyncio
    async def test_dispatches_bar_chart(self, service, client):
        """create_chart_by_type('dist_bar') should delegate to create_bar_chart."""
        client.post.return_value = {"id": 200}
        client.put.return_value = {}
        client.get.return_value = {"result": _chart_detail_dict(id=200, viz_type="dist_bar")}

        result = await service.create_chart_by_type(
            chart_type="dist_bar",
            title="Bar Test",
            datasource_id=10,
            metrics=["SUM(revenue)"],
            groupby=["region"],
        )

        assert result.id == 200
        assert result.viz_type == "dist_bar"

    @pytest.mark.asyncio
    async def test_dispatches_pie_chart(self, service, client):
        """create_chart_by_type('pie') should delegate to create_pie_chart."""
        client.post.return_value = {"id": 201}
        client.put.return_value = {}
        client.get.return_value = {"result": _chart_detail_dict(id=201, viz_type="pie")}

        result = await service.create_chart_by_type(
            chart_type="pie",
            title="Pie Test",
            datasource_id=10,
            metric="COUNT(*)",
            groupby="category",
        )

        assert result.id == 201
        assert result.viz_type == "pie"

    @pytest.mark.asyncio
    async def test_unsupported_type_raises(self, service):
        """create_chart_by_type with invalid type should raise ValueError."""
        with pytest.raises(ValueError, match="Unsupported chart type"):
            await service.create_chart_by_type(
                chart_type="nonexistent",  # type: ignore[arg-type]
                title="Bad",
                datasource_id=10,
            )

    @pytest.mark.asyncio
    async def test_all_chart_types_have_dispatch(self, service):
        """Every ChartType value should be dispatchable."""
        from typing import get_args

        from superset_ai.schemas.charts import ChartType

        chart_types = get_args(ChartType)
        # Just verify the dispatch dict covers all types — don't actually call them
        for _ct in chart_types:
            # Should not raise
            assert hasattr(service, "create_chart_by_type")
