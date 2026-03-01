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
            if schemas:
                schema = schemas[0]  # Use first schema (e.g., "main" for SQLite)
            else:
                schema = "main"  # Fallback default
        
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
            
            tables.append(TableInfo(
                name=name,
                schema=table_schema,
                type=table_type,
            ))
        
        return tables

    async def list_schemas(self, database_id: int) -> list[str]:
        """
        List schemas in a database.
        
        GET /api/v1/database/{id}/schemas/
        """
        response = await self.client.get(f"/database/{database_id}/schemas/")
        result = response.get("result", [])
        return [str(s) for s in result]

    async def test_connection(self, database_id: int) -> bool:
        """
        Test database connection.
        
        GET /api/v1/database/{id}/connection
        """
        try:
            await self.client.get(f"/database/{database_id}/connection")
            return True
        except Exception as e:
            logger.warning("Database connection test failed: %s", e)
            return False

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

    async def get_table_columns(
        self,
        database_id: int,
        table_name: str,
        *,
        schema: str | None = None,
    ) -> list[dict]:
        """
        Get column information for a table.
        
        This uses the dataset metadata endpoint.
        Note: May require creating a temporary dataset.
        """
        # This is a complex operation that typically requires:
        # 1. Creating a temporary virtual dataset with SELECT * FROM table LIMIT 0
        # 2. Reading the columns from the dataset
        # 3. Optionally cleaning up
        #
        # For now, we'll use the simpler approach of querying via SQL Lab API
        # or require the user to have an existing dataset
        
        logger.warning(
            "get_table_columns for %s not fully implemented. "
            "Use DatasetService.get_dataset() with an existing dataset instead.",
            table_name,
        )
        return []
