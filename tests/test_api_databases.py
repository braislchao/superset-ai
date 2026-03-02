"""Tests for DatabaseService — API service layer for Superset databases."""

from __future__ import annotations

import json
from unittest.mock import AsyncMock

import pytest

from superset_ai.api.databases import DatabaseService

# ---------------------------------------------------------------------------
# Helpers: realistic API response dicts
# ---------------------------------------------------------------------------


def _database_info_dict(
    *,
    id: int = 1,
    database_name: str = "main",
    backend: str = "postgresql",
) -> dict:
    return {
        "id": id,
        "database_name": database_name,
        "backend": backend,
    }


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture()
def client() -> AsyncMock:
    return AsyncMock()


@pytest.fixture()
def service(client: AsyncMock) -> DatabaseService:
    return DatabaseService(client)


# =========================================================================
# list_databases
# =========================================================================


class TestListDatabases:
    """Tests for DatabaseService.list_databases."""

    @pytest.mark.asyncio
    async def test_returns_list_of_database_info(self, service, client):
        """Should parse response into a list of DatabaseInfo."""
        client.get.return_value = {
            "result": [
                _database_info_dict(id=1, database_name="prod"),
                _database_info_dict(id=2, database_name="staging"),
            ]
        }

        databases = await service.list_databases()

        assert len(databases) == 2
        assert databases[0].id == 1
        assert databases[0].database_name == "prod"
        assert databases[1].id == 2

    @pytest.mark.asyncio
    async def test_passes_pagination_params(self, service, client):
        """Should forward page/page_size to the API."""
        client.get.return_value = {"result": [_database_info_dict()]}

        await service.list_databases(page=1, page_size=10)

        client.get.assert_called_once_with("/database/", params={"page": 1, "page_size": 10})

    @pytest.mark.asyncio
    async def test_fallback_to_sqllab_when_empty(self, service, client):
        """Should fall back to /sqllab/ when primary endpoint returns empty."""
        # First call to /database/ returns empty
        # Second call to /sqllab/ returns databases
        client.get.side_effect = [
            {"result": []},  # /database/
            {
                "result": {
                    "databases": {
                        "1": {"id": 1, "database_name": "fallback_db", "backend": "sqlite"}
                    }
                }
            },  # /sqllab/
        ]

        databases = await service.list_databases()

        assert len(databases) == 1
        assert databases[0].database_name == "fallback_db"
        # Should have called both endpoints
        assert client.get.call_count == 2

    @pytest.mark.asyncio
    async def test_fallback_handles_exception_gracefully(self, service, client):
        """Should return empty list when fallback also fails."""
        client.get.side_effect = [
            {"result": []},  # /database/
            Exception("Connection error"),  # /sqllab/ fails
        ]

        databases = await service.list_databases()

        assert databases == []

    @pytest.mark.asyncio
    async def test_no_fallback_when_primary_has_results(self, service, client):
        """Should not call fallback when primary endpoint has results."""
        client.get.return_value = {"result": [_database_info_dict(id=1, database_name="main")]}

        databases = await service.list_databases()

        assert len(databases) == 1
        # Only one GET call (no fallback)
        client.get.assert_called_once()


# =========================================================================
# get_database
# =========================================================================


class TestGetDatabase:
    """Tests for DatabaseService.get_database."""

    @pytest.mark.asyncio
    async def test_returns_database_info(self, service, client):
        """Should parse response into DatabaseInfo."""
        client.get.return_value = {"result": _database_info_dict(id=5, database_name="analytics")}

        db = await service.get_database(5)

        assert db.id == 5
        assert db.database_name == "analytics"
        client.get.assert_called_once_with("/database/5")


# =========================================================================
# list_tables
# =========================================================================


class TestListTables:
    """Tests for DatabaseService.list_tables."""

    @pytest.mark.asyncio
    async def test_returns_tables_with_schema(self, service, client):
        """Should list tables with explicit schema."""
        client.get.return_value = {
            "result": [
                {"value": "users", "type": "table", "schema": "public"},
                {"value": "orders", "type": "table", "schema": "public"},
            ]
        }

        tables = await service.list_tables(1, schema="public")

        assert len(tables) == 2
        assert tables[0].name == "users"
        assert tables[1].name == "orders"
        # Should have called with Rison format
        client.get.assert_called_once_with(
            "/database/1/tables/",
            params={"q": "(schema_name:public)"},
        )

    @pytest.mark.asyncio
    async def test_fetches_default_schema_when_none(self, service, client):
        """Should get first schema when none provided."""
        # First call: list_schemas
        # Second call: list tables
        client.get.side_effect = [
            {"result": ["public", "analytics"]},  # schemas
            {"result": [{"value": "t1", "type": "table"}]},  # tables
        ]

        tables = await service.list_tables(1)

        assert len(tables) == 1
        assert tables[0].name == "t1"
        # Second call should use first schema
        table_call = client.get.call_args_list[1]
        assert table_call[1]["params"]["q"] == "(schema_name:public)"

    @pytest.mark.asyncio
    async def test_handles_string_items_in_result(self, service, client):
        """Should handle response items that are plain strings."""
        client.get.return_value = {"result": ["simple_table"]}

        tables = await service.list_tables(1, schema="main")

        assert len(tables) == 1
        assert tables[0].name == "simple_table"
        assert tables[0].type == "table"

    @pytest.mark.asyncio
    async def test_handles_name_key_fallback(self, service, client):
        """Should fall back to 'name' key when 'value' is missing."""
        client.get.return_value = {"result": [{"name": "alt_table", "type": "view"}]}

        tables = await service.list_tables(1, schema="public")

        assert tables[0].name == "alt_table"
        assert tables[0].type == "view"

    @pytest.mark.asyncio
    async def test_uses_main_schema_when_no_schemas_found(self, service, client):
        """Should use 'main' schema when list_schemas returns empty."""
        client.get.side_effect = [
            {"result": []},  # empty schemas
            {"result": []},  # no tables
        ]

        tables = await service.list_tables(1)

        assert tables == []
        table_call = client.get.call_args_list[1]
        assert table_call[1]["params"]["q"] == "(schema_name:main)"


# =========================================================================
# list_schemas
# =========================================================================


class TestListSchemas:
    """Tests for DatabaseService.list_schemas."""

    @pytest.mark.asyncio
    async def test_returns_list_of_strings(self, service, client):
        """Should return a list of schema name strings."""
        client.get.return_value = {"result": ["public", "analytics", "raw"]}

        schemas = await service.list_schemas(1)

        assert schemas == ["public", "analytics", "raw"]
        client.get.assert_called_once_with("/database/1/schemas/")

    @pytest.mark.asyncio
    async def test_returns_empty_list(self, service, client):
        """Should return empty list when no schemas exist."""
        client.get.return_value = {"result": []}

        schemas = await service.list_schemas(1)

        assert schemas == []


# =========================================================================
# find_by_name
# =========================================================================


class TestFindByName:
    """Tests for DatabaseService.find_by_name."""

    @pytest.mark.asyncio
    async def test_returns_database_when_found(self, service, client):
        """Should return DatabaseInfo when a match exists."""
        client.get.return_value = {"result": [_database_info_dict(id=3, database_name="analytics")]}

        result = await service.find_by_name("analytics")

        assert result is not None
        assert result.id == 3
        # Verify filter query
        call_params = client.get.call_args[1]["params"]
        q_value = json.loads(call_params["q"])
        assert q_value["filters"][0]["col"] == "database_name"
        assert q_value["filters"][0]["value"] == "analytics"

    @pytest.mark.asyncio
    async def test_returns_none_when_not_found(self, service, client):
        """Should return None when no match exists."""
        client.get.return_value = {"result": []}

        result = await service.find_by_name("nonexistent")

        assert result is None


# =========================================================================
# execute_sql
# =========================================================================


class TestExecuteSQL:
    """Tests for DatabaseService.execute_sql."""

    @pytest.mark.asyncio
    async def test_posts_sql_and_returns_raw_response(self, service, client):
        """Should POST to /sqllab/execute/ and return raw response."""
        expected = {
            "columns": [{"name": "id"}, {"name": "name"}],
            "data": [{"id": 1, "name": "Alice"}],
        }
        client.post.return_value = expected

        result = await service.execute_sql(1, "SELECT * FROM users")

        assert result == expected
        client.post.assert_called_once_with(
            "/sqllab/execute/",
            json={
                "database_id": 1,
                "sql": "SELECT * FROM users",
                "queryLimit": 100,
            },
        )

    @pytest.mark.asyncio
    async def test_passes_custom_limit(self, service, client):
        """Should pass custom limit to the API."""
        client.post.return_value = {"columns": [], "data": []}

        await service.execute_sql(1, "SELECT 1", limit=500)

        payload = client.post.call_args[1]["json"]
        assert payload["queryLimit"] == 500

    @pytest.mark.asyncio
    async def test_passes_schema_when_provided(self, service, client):
        """Should include schema in the payload when specified."""
        client.post.return_value = {"columns": [], "data": []}

        await service.execute_sql(1, "SELECT 1", schema="analytics")

        payload = client.post.call_args[1]["json"]
        assert payload["schema"] == "analytics"

    @pytest.mark.asyncio
    async def test_omits_schema_when_not_provided(self, service, client):
        """Should not include schema in the payload when not specified."""
        client.post.return_value = {"columns": [], "data": []}

        await service.execute_sql(1, "SELECT 1")

        payload = client.post.call_args[1]["json"]
        assert "schema" not in payload
