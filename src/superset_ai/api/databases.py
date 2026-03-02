"""Database service for Superset API operations."""

import json
import logging
from typing import TYPE_CHECKING

from superset_ai.schemas.common import DatabaseInfo, TableInfo

if TYPE_CHECKING:
    from superset_ai.api.client import SupersetClient

logger = logging.getLogger(__name__)


class DatabaseService:
    """
    Service for interacting with Superset database connections.

    Wraps /api/v1/database/ endpoints.
    """

    def __init__(self, client: "SupersetClient") -> None:
        self.client = client

    async def list_databases(
        self,
        *,
        page: int = 0,
        page_size: int = 100,
    ) -> list[DatabaseInfo]:
        """
        List all database connections.

        GET /api/v1/database/

        Falls back to /api/v1/sqllab/ endpoint if the primary endpoint
        returns empty results (workaround for Superset 3.1.0 permissions bug).
        """
        params = {
            "page": page,
            "page_size": page_size,
        }

        response = await self.client.get("/database/", params=params)

        result = response.get("result", [])

        # Fallback: If no results, try the /sqllab/ endpoint
        # which includes databases in its response (Superset 3.1.0 workaround)
        if not result:
            logger.debug("Primary /database/ endpoint returned empty, trying /sqllab/ fallback")
            try:
                sqllab_response = await self.client.get("/sqllab/")
                databases_dict = sqllab_response.get("result", {}).get("databases", {})
                if databases_dict:
                    result = list(databases_dict.values())
                    logger.debug("Found %d databases via /sqllab/ fallback", len(result))
            except Exception as e:
                logger.warning("Fallback to /sqllab/ failed: %s", e)

        return [DatabaseInfo.model_validate(item) for item in result]

    async def get_database(self, database_id: int) -> DatabaseInfo:
        """
        Get information about a specific database.

        GET /api/v1/database/{id}
        """
        response = await self.client.get(f"/database/{database_id}")
        result = response.get("result", {})
        return DatabaseInfo.model_validate(result)

    async def list_tables(
        self,
        database_id: int,
        *,
        schema: str | None = None,
    ) -> list[TableInfo]:
        """
        List tables in a database.

        GET /api/v1/database/{id}/tables/

        Note: Superset 3.1.0 requires schema_name. If not provided,
        we'll try to get the first available schema.
        """
        # Superset 3.1.0 requires schema_name in Rison format
        # If no schema provided, try to get default
        if schema is None:
            schemas = await self.list_schemas(database_id)
            schema = schemas[0] if schemas else "main"

        # Use Rison encoding for the q parameter
        params = {"q": f"(schema_name:{schema})"}

        response = await self.client.get(
            f"/database/{database_id}/tables/",
            params=params,
        )

        # Response format: {"result": [{"value": "table_name", "type": "table", ...}]}
        result = response.get("result", [])

        tables = []
        for item in result:
            # Handle different response formats
            if isinstance(item, dict):
                name = item.get("value") or item.get("name", "")
                table_type = item.get("type", "table")
                table_schema = item.get("schema", schema)
            else:
                name = str(item)
                table_type = "table"
                table_schema = schema

            tables.append(
                TableInfo(
                    name=name,
                    schema=table_schema,
                    type=table_type,
                )
            )

        return tables

    async def list_schemas(self, database_id: int) -> list[str]:
        """
        List schemas in a database.

        GET /api/v1/database/{id}/schemas/
        """
        response = await self.client.get(f"/database/{database_id}/schemas/")
        result = response.get("result", [])
        return [str(s) for s in result]

    async def find_by_name(self, name: str) -> DatabaseInfo | None:
        """
        Find a database by name.

        Returns None if not found.
        """
        filters = [{"col": "database_name", "opr": "eq", "value": name}]
        params = {"q": json.dumps({"filters": filters})}

        response = await self.client.get("/database/", params=params)
        result = response.get("result", [])

        if result:
            return DatabaseInfo.model_validate(result[0])
        return None

    async def execute_sql(
        self,
        database_id: int,
        sql: str,
        *,
        limit: int = 100,
        schema: str | None = None,
    ) -> dict:
        """
        Execute a SQL query via Superset SQL Lab.

        POST /api/v1/sqllab/execute/

        Args:
            database_id: The database to run the query against.
            sql: The SQL query string.
            limit: Maximum number of rows to return (default 100).
            schema: Optional schema context for the query.

        Returns:
            Raw response dict from Superset containing ``columns`` and ``data``.
        """
        payload: dict = {
            "database_id": database_id,
            "sql": sql,
            "queryLimit": limit,
        }
        if schema is not None:
            payload["schema"] = schema

        logger.info("Executing SQL on database %d (limit=%d)", database_id, limit)
        response = await self.client.post("/sqllab/execute/", json=payload)
        return response
