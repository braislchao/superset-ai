"""Tests for DatasetService — API service layer for Superset datasets."""

from __future__ import annotations

import json
from unittest.mock import AsyncMock

import pytest

from superset_ai.api.datasets import DatasetService
from superset_ai.schemas.datasets import DatasetCreate

# ---------------------------------------------------------------------------
# Helpers: realistic API response dicts
# ---------------------------------------------------------------------------


def _dataset_info_dict(
    *,
    id: int = 10,
    table_name: str = "orders",
    database_id: int = 1,
    schema: str | None = "public",
) -> dict:
    return {
        "id": id,
        "table_name": table_name,
        "database": database_id,
        "schema": schema,
        "owners": [],
    }


def _dataset_detail_dict(
    *,
    id: int = 10,
    table_name: str = "orders",
    database_id: int = 1,
    schema: str | None = "public",
    columns: list | None = None,
    metrics: list | None = None,
) -> dict:
    return {
        "id": id,
        "table_name": table_name,
        "database": database_id,
        "schema": schema,
        "columns": columns or [],
        "metrics": metrics or [],
        "owners": [],
    }


def _column_dict(
    *,
    id: int = 1,
    column_name: str = "id",
    type: str = "INTEGER",
    is_dttm: bool = False,
    type_generic: int = 0,
) -> dict:
    return {
        "id": id,
        "column_name": column_name,
        "type": type,
        "is_dttm": is_dttm,
        "type_generic": type_generic,
    }


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture()
def client() -> AsyncMock:
    return AsyncMock()


@pytest.fixture()
def service(client: AsyncMock) -> DatasetService:
    return DatasetService(client)


# =========================================================================
# list_datasets
# =========================================================================


class TestListDatasets:
    """Tests for DatasetService.list_datasets."""

    @pytest.mark.asyncio
    async def test_returns_list_of_dataset_info(self, service, client):
        """Should parse response into a list of DatasetInfo."""
        client.get.return_value = {
            "result": [
                _dataset_info_dict(id=10, table_name="orders"),
                _dataset_info_dict(id=11, table_name="users"),
            ]
        }

        datasets = await service.list_datasets()

        assert len(datasets) == 2
        assert datasets[0].id == 10
        assert datasets[0].table_name == "orders"
        assert datasets[1].id == 11

    @pytest.mark.asyncio
    async def test_passes_pagination_params(self, service, client):
        """Should forward page/page_size to the API."""
        client.get.return_value = {"result": [_dataset_info_dict()]}

        await service.list_datasets(page=2, page_size=50)

        client.get.assert_called_once_with("/dataset/", params={"page": 2, "page_size": 50})

    @pytest.mark.asyncio
    async def test_filters_by_database_id(self, service, client):
        """Should include filter JSON when database_id is given."""
        client.get.return_value = {"result": [_dataset_info_dict()]}

        await service.list_datasets(database_id=5)

        call_params = client.get.call_args[1].get("params") or client.get.call_args[0][1]
        q_value = json.loads(call_params["q"])
        assert q_value["filters"][0]["col"] == "database"
        assert q_value["filters"][0]["value"] == 5

    @pytest.mark.asyncio
    async def test_fallback_when_api_returns_empty(self, service, client):
        """Should try fallback when API returns empty results."""
        # Primary endpoint returns empty
        # Fallback: /sqllab/ returns databases, then tables, then dataset filter
        client.get.side_effect = [
            {"result": []},  # /dataset/ returns empty
            {"result": {"databases": {"1": {"database_name": "main"}}}},  # /sqllab/
            {"result": [{"value": "orders"}]},  # /database/1/tables/
            {"result": [_dataset_info_dict(id=10, table_name="orders")]},  # /dataset/ filter
        ]

        datasets = await service.list_datasets()

        assert len(datasets) == 1
        assert datasets[0].table_name == "orders"

    @pytest.mark.asyncio
    async def test_fallback_returns_empty_on_failure(self, service, client):
        """Should return empty list when fallback also fails."""
        client.get.side_effect = [
            {"result": []},  # /dataset/ empty
            Exception("Connection error"),  # /sqllab/ fails
        ]

        datasets = await service.list_datasets()

        assert datasets == []

    @pytest.mark.asyncio
    async def test_database_id_field_extracted_from_nested(self, service, client):
        """Should extract database_id from nested dict format."""
        client.get.return_value = {
            "result": [
                {
                    "id": 10,
                    "table_name": "orders",
                    "database": {"id": 5, "database_name": "prod"},
                    "schema": "public",
                    "owners": [],
                }
            ]
        }

        datasets = await service.list_datasets()

        assert datasets[0].database_id == 5


# =========================================================================
# get_dataset
# =========================================================================


class TestGetDataset:
    """Tests for DatasetService.get_dataset."""

    @pytest.mark.asyncio
    async def test_returns_dataset_detail(self, service, client):
        """Should parse response into DatasetDetail."""
        client.get.return_value = {"result": _dataset_detail_dict(id=10, table_name="orders")}

        dataset = await service.get_dataset(10)

        assert dataset.id == 10
        assert dataset.table_name == "orders"
        client.get.assert_called_once_with("/dataset/10")

    @pytest.mark.asyncio
    async def test_parses_columns(self, service, client):
        """Should parse column information."""
        client.get.return_value = {
            "result": _dataset_detail_dict(
                id=10,
                columns=[
                    _column_dict(column_name="id", type="INTEGER", type_generic=0),
                    _column_dict(
                        column_name="created_at", type="TIMESTAMP", is_dttm=True, type_generic=2
                    ),
                ],
            )
        }

        dataset = await service.get_dataset(10)

        assert len(dataset.columns) == 2
        assert dataset.columns[0].column_name == "id"
        assert dataset.columns[1].is_dttm is True

    @pytest.mark.asyncio
    async def test_parses_metrics(self, service, client):
        """Should parse metric information."""
        client.get.return_value = {
            "result": _dataset_detail_dict(
                id=10,
                metrics=[
                    {
                        "id": 1,
                        "metric_name": "count",
                        "expression": "COUNT(*)",
                    }
                ],
            )
        }

        dataset = await service.get_dataset(10)

        assert len(dataset.metrics) == 1
        assert dataset.metrics[0].metric_name == "count"


# =========================================================================
# create_dataset
# =========================================================================


class TestCreateDataset:
    """Tests for DatasetService.create_dataset."""

    @pytest.mark.asyncio
    async def test_posts_and_fetches_detail(self, service, client):
        """Should POST, then GET the created dataset."""
        client.post.return_value = {"id": 20}
        client.get.return_value = {"result": _dataset_detail_dict(id=20, table_name="new_table")}

        spec = DatasetCreate(table_name="new_table", database=1)
        dataset = await service.create_dataset(spec)

        assert dataset.id == 20
        assert dataset.table_name == "new_table"
        client.post.assert_called_once()
        assert client.post.call_args[0][0] == "/dataset/"

    @pytest.mark.asyncio
    async def test_posts_correct_payload(self, service, client):
        """Should include table_name, database, and schema in payload."""
        client.post.return_value = {"id": 21}
        client.get.return_value = {"result": _dataset_detail_dict(id=21)}

        spec = DatasetCreate(table_name="events", database=3, schema="analytics")
        await service.create_dataset(spec)

        post_payload = client.post.call_args[1]["json"]
        assert post_payload["table_name"] == "events"
        assert post_payload["database"] == 3
        assert post_payload["schema"] == "analytics"

    @pytest.mark.asyncio
    async def test_fallback_when_no_id_returned(self, service, client):
        """Should validate from result when no id is returned."""
        raw = _dataset_detail_dict(id=5)
        client.post.return_value = {"result": raw}

        spec = DatasetCreate(table_name="fallback", database=1)
        dataset = await service.create_dataset(spec)

        assert dataset.id == 5


# =========================================================================
# find_by_table_name
# =========================================================================


class TestFindByTableName:
    """Tests for DatasetService.find_by_table_name."""

    @pytest.mark.asyncio
    async def test_returns_dataset_when_found(self, service, client):
        """Should return DatasetInfo when a match exists."""
        client.get.return_value = {"result": [_dataset_info_dict(id=10, table_name="orders")]}

        result = await service.find_by_table_name("orders", database_id=1)

        assert result is not None
        assert result.id == 10
        # Verify filters
        call_params = client.get.call_args[1]["params"]
        q_value = json.loads(call_params["q"])
        filters = q_value["filters"]
        assert any(f["col"] == "table_name" and f["value"] == "orders" for f in filters)
        assert any(f["col"] == "database" and f["value"] == 1 for f in filters)

    @pytest.mark.asyncio
    async def test_returns_none_when_not_found(self, service, client):
        """Should return None when no match exists."""
        client.get.return_value = {"result": []}

        result = await service.find_by_table_name("nonexistent", database_id=1)

        assert result is None

    @pytest.mark.asyncio
    async def test_includes_schema_filter_when_provided(self, service, client):
        """Should add schema filter when specified."""
        client.get.return_value = {"result": []}

        await service.find_by_table_name("orders", database_id=1, schema="analytics")

        call_params = client.get.call_args[1]["params"]
        q_value = json.loads(call_params["q"])
        filters = q_value["filters"]
        assert any(f["col"] == "schema" and f["value"] == "analytics" for f in filters)


# =========================================================================
# find_or_create
# =========================================================================


class TestFindOrCreate:
    """Tests for DatasetService.find_or_create."""

    @pytest.mark.asyncio
    async def test_returns_existing_dataset(self, service, client):
        """Should return existing dataset when found."""
        # find_by_table_name returns a match
        client.get.side_effect = [
            {"result": [_dataset_info_dict(id=10, table_name="orders")]},  # find
            {"result": _dataset_detail_dict(id=10, table_name="orders")},  # get_dataset
        ]

        dataset = await service.find_or_create("orders", database_id=1)

        assert dataset.id == 10
        # Should not POST (no creation)
        client.post.assert_not_called()

    @pytest.mark.asyncio
    async def test_creates_new_dataset_when_not_found(self, service, client):
        """Should create a new dataset when not found."""
        client.get.side_effect = [
            {"result": []},  # find returns nothing
            {"result": _dataset_detail_dict(id=30, table_name="new_table")},  # get after create
        ]
        client.post.return_value = {"id": 30}

        dataset = await service.find_or_create("new_table", database_id=1)

        assert dataset.id == 30
        client.post.assert_called_once()
        post_payload = client.post.call_args[1]["json"]
        assert post_payload["table_name"] == "new_table"
        assert post_payload["database"] == 1

    @pytest.mark.asyncio
    async def test_passes_schema_through(self, service, client):
        """Should pass schema to both find and create."""
        client.get.side_effect = [
            {"result": []},  # find returns nothing
            {"result": _dataset_detail_dict(id=31)},  # get after create
        ]
        client.post.return_value = {"id": 31}

        await service.find_or_create("table", database_id=1, schema="analytics")

        # Verify find included schema filter
        find_params = client.get.call_args_list[0][1]["params"]
        q_value = json.loads(find_params["q"])
        assert any(f["col"] == "schema" and f["value"] == "analytics" for f in q_value["filters"])

        # Verify create included schema
        post_payload = client.post.call_args[1]["json"]
        assert post_payload["schema"] == "analytics"


# =========================================================================
# get_column_names
# =========================================================================


class TestGetColumnNames:
    """Tests for DatasetService.get_column_names."""

    @pytest.mark.asyncio
    async def test_returns_column_names(self, service, client):
        """Should return a list of column name strings."""
        client.get.return_value = {
            "result": _dataset_detail_dict(
                id=10,
                columns=[
                    _column_dict(column_name="id"),
                    _column_dict(column_name="name"),
                    _column_dict(column_name="email"),
                ],
            )
        }

        names = await service.get_column_names(10)

        assert names == ["id", "name", "email"]

    @pytest.mark.asyncio
    async def test_returns_empty_for_no_columns(self, service, client):
        """Should return empty list when dataset has no columns."""
        client.get.return_value = {"result": _dataset_detail_dict(id=10, columns=[])}

        names = await service.get_column_names(10)

        assert names == []


# =========================================================================
# get_time_columns
# =========================================================================


class TestGetTimeColumns:
    """Tests for DatasetService.get_time_columns."""

    @pytest.mark.asyncio
    async def test_returns_only_datetime_columns(self, service, client):
        """Should filter to columns where is_dttm is True."""
        client.get.return_value = {
            "result": _dataset_detail_dict(
                id=10,
                columns=[
                    _column_dict(column_name="id", is_dttm=False),
                    _column_dict(column_name="created_at", is_dttm=True, type="TIMESTAMP"),
                    _column_dict(column_name="name", is_dttm=False),
                    _column_dict(column_name="updated_at", is_dttm=True, type="DATETIME"),
                ],
            )
        }

        time_cols = await service.get_time_columns(10)

        assert time_cols == ["created_at", "updated_at"]

    @pytest.mark.asyncio
    async def test_returns_empty_when_no_datetime_columns(self, service, client):
        """Should return empty list when no datetime columns exist."""
        client.get.return_value = {
            "result": _dataset_detail_dict(
                id=10,
                columns=[
                    _column_dict(column_name="id", is_dttm=False),
                    _column_dict(column_name="name", is_dttm=False),
                ],
            )
        }

        time_cols = await service.get_time_columns(10)

        assert time_cols == []


# =========================================================================
# get_numeric_columns
# =========================================================================


class TestGetNumericColumns:
    """Tests for DatasetService.get_numeric_columns."""

    @pytest.mark.asyncio
    async def test_returns_only_numeric_columns(self, service, client):
        """Should filter to columns with type_generic in {0, 1} (INT, FLOAT)."""
        client.get.return_value = {
            "result": _dataset_detail_dict(
                id=10,
                columns=[
                    _column_dict(column_name="id", type_generic=0),  # INT
                    _column_dict(column_name="price", type_generic=1),  # FLOAT
                    _column_dict(column_name="name", type_generic=2),  # STRING
                    _column_dict(column_name="created_at", type_generic=3),  # DATETIME
                    _column_dict(column_name="amount", type_generic=0),  # INT
                ],
            )
        }

        numeric_cols = await service.get_numeric_columns(10)

        assert numeric_cols == ["id", "price", "amount"]

    @pytest.mark.asyncio
    async def test_returns_empty_when_no_numeric_columns(self, service, client):
        """Should return empty list when no numeric columns exist."""
        client.get.return_value = {
            "result": _dataset_detail_dict(
                id=10,
                columns=[
                    _column_dict(column_name="name", type_generic=2),
                    _column_dict(column_name="created_at", type_generic=3),
                ],
            )
        }

        numeric_cols = await service.get_numeric_columns(10)

        assert numeric_cols == []

    @pytest.mark.asyncio
    async def test_handles_none_type_generic(self, service, client):
        """Should exclude columns with None type_generic."""
        client.get.return_value = {
            "result": _dataset_detail_dict(
                id=10,
                columns=[
                    _column_dict(column_name="id", type_generic=0),
                    {"id": 2, "column_name": "unknown", "type": "UNKNOWN"},
                ],
            )
        }

        numeric_cols = await service.get_numeric_columns(10)

        assert numeric_cols == ["id"]
