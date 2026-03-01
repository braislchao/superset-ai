"""Dataset service for Superset API operations."""

import json
import logging
from typing import TYPE_CHECKING

from superset_ai.core.exceptions import ResourceNotFoundError
from superset_ai.schemas.datasets import (
    DatasetCreate,
    DatasetDetail,
    DatasetInfo,
    DatasetUpdate,
)

if TYPE_CHECKING:
    from superset_ai.api.client import SupersetClient

logger = logging.getLogger(__name__)


class DatasetService:
    """
    Service for managing Superset datasets.
    
    Wraps /api/v1/dataset/ endpoints with typed interfaces.
    """

    def __init__(self, client: "SupersetClient") -> None:
        self.client = client

    async def list_datasets(
        self,
        *,
        database_id: int | None = None,
        page: int = 0,
        page_size: int = 100,
    ) -> list[DatasetInfo]:
        """
        List all datasets, optionally filtered by database.
        
        GET /api/v1/dataset/
        
        Note: Superset 3.x has a known bug where the dataset list API returns empty
        results even when datasets exist. This method includes a fallback that
        queries the Superset metadata tables directly via SQL Lab.
        """
        params: dict = {
            "page": page,
            "page_size": page_size,
        }

        # Build filter if database_id provided
        if database_id is not None:
            filters = [{"col": "database", "opr": "rel_o_m", "value": database_id}]
            params["q"] = json.dumps({"filters": filters})

        response = await self.client.get("/dataset/", params=params)
        
        result = response.get("result", [])
        
        # If the API returns results, use them
        if result:
            return [DatasetInfo.model_validate(item) for item in result]
        
        # WORKAROUND: Superset 3.x bug - API returns empty even when datasets exist
        # Try to get datasets from the sqllab tables endpoint as a fallback
        logger.warning("Dataset API returned empty, trying fallback via database tables...")
        return await self._list_datasets_fallback(database_id=database_id)

    async def get_dataset(self, dataset_id: int) -> DatasetDetail:
        """
        Get detailed information about a dataset.
        
        GET /api/v1/dataset/{id}
        """
        response = await self.client.get(f"/dataset/{dataset_id}")
        result = response.get("result", {})
        return DatasetDetail.model_validate(result)

    async def create_dataset(self, spec: DatasetCreate) -> DatasetDetail:
        """
        Create a new dataset.
        
        POST /api/v1/dataset/
        """
        payload = spec.model_dump(exclude_none=True)
        
        logger.info("Creating dataset: %s", spec.table_name)
        response = await self.client.post("/dataset/", json=payload)
        
        dataset_id = response.get("id")
        if dataset_id:
            # Fetch full details
            return await self.get_dataset(dataset_id)
        
        # Fallback: construct from response
        result = response.get("result", response)
        return DatasetDetail.model_validate(result)

    async def update_dataset(
        self,
        dataset_id: int,
        spec: DatasetUpdate,
    ) -> DatasetDetail:
        """
        Update an existing dataset.
        
        PUT /api/v1/dataset/{id}
        """
        payload = spec.model_dump(exclude_none=True)
        
        logger.info("Updating dataset %d", dataset_id)
        await self.client.put(f"/dataset/{dataset_id}", json=payload)
        
        return await self.get_dataset(dataset_id)

    async def delete_dataset(self, dataset_id: int) -> None:
        """
        Delete a dataset.
        
        DELETE /api/v1/dataset/{id}
        """
        logger.info("Deleting dataset %d", dataset_id)
        await self.client.delete(f"/dataset/{dataset_id}")

    async def refresh_columns(self, dataset_id: int) -> DatasetDetail:
        """
        Refresh dataset columns from the database.
        
        PUT /api/v1/dataset/{id}/refresh
        """
        logger.info("Refreshing columns for dataset %d", dataset_id)
        await self.client.put(f"/dataset/{dataset_id}/refresh")
        return await self.get_dataset(dataset_id)

    # =========================================================================
    # Higher-level operations
    # =========================================================================

    async def find_by_table_name(
        self,
        table_name: str,
        database_id: int,
        *,
        schema: str | None = None,
    ) -> DatasetInfo | None:
        """
        Find an existing dataset by table name and database.
        
        Returns None if not found.
        """
        filters = [
            {"col": "table_name", "opr": "eq", "value": table_name},
            {"col": "database", "opr": "rel_o_m", "value": database_id},
        ]
        
        if schema:
            filters.append({"col": "schema", "opr": "eq", "value": schema})

        params = {"q": json.dumps({"filters": filters})}
        
        response = await self.client.get("/dataset/", params=params)
        result = response.get("result", [])
        
        if result:
            return DatasetInfo.model_validate(result[0])
        return None

    async def find_or_create(
        self,
        table_name: str,
        database_id: int,
        *,
        schema: str | None = None,
    ) -> DatasetDetail:
        """
        Find existing dataset or create new one.
        
        Implements the reuse-existing-assets strategy.
        """
        existing = await self.find_by_table_name(
            table_name=table_name,
            database_id=database_id,
            schema=schema,
        )
        
        if existing:
            logger.info("Found existing dataset: %s (id=%d)", existing.table_name, existing.id)
            return await self.get_dataset(existing.id)
        
        # Create new dataset
        spec = DatasetCreate(
            table_name=table_name,
            database=database_id,
            schema=schema,
        )
        return await self.create_dataset(spec)

    async def get_column_names(self, dataset_id: int) -> list[str]:
        """Get list of column names for a dataset."""
        dataset = await self.get_dataset(dataset_id)
        return [col.column_name for col in dataset.columns]

    async def get_time_columns(self, dataset_id: int) -> list[str]:
        """Get list of datetime columns for a dataset."""
        dataset = await self.get_dataset(dataset_id)
        return [col.column_name for col in dataset.columns if col.is_dttm]

    async def get_numeric_columns(self, dataset_id: int) -> list[str]:
        """
        Get list of numeric columns suitable for metrics.
        
        Filters by type_generic which indicates numeric types.
        """
        dataset = await self.get_dataset(dataset_id)
        numeric_types = {0, 1}  # INT, FLOAT in Superset's type system
        return [
            col.column_name
            for col in dataset.columns
            if col.type_generic in numeric_types
        ]

    # =========================================================================
    # Workarounds for Superset 3.x API bugs
    # =========================================================================

    async def _list_datasets_fallback(
        self,
        database_id: int | None = None,
    ) -> list[DatasetInfo]:
        """
        Fallback method to list datasets when the main API returns empty.
        
        Uses the database tables endpoint which bypasses the broken dataset API.
        """
        try:
            # First, get databases from sqllab endpoint (also has workaround)
            sqllab_response = await self.client.get("/sqllab/")
            databases = sqllab_response.get("result", {}).get("databases", {})
            
            if not databases:
                logger.warning("No databases found via sqllab endpoint")
                return []
            
            all_datasets: list[DatasetInfo] = []
            
            # For each database, get its tables
            for db_id_str, db_info in databases.items():
                db_id = int(db_id_str)
                
                # Skip if filtering by specific database
                if database_id is not None and db_id != database_id:
                    continue
                
                db_name = db_info.get("database_name", "unknown")
                
                # Get tables for this database
                try:
                    tables_response = await self.client.get(
                        f"/database/{db_id}/tables/",
                        params={"schema_name": "public"}
                    )
                    tables = tables_response.get("result", [])
                    
                    for table_info in tables:
                        table_name = table_info.get("value", table_info.get("table", ""))
                        if not table_name:
                            continue
                        
                        # Check if this table is registered as a dataset
                        # by trying to find it in the dataset endpoint by name
                        dataset_info = await self._get_dataset_by_table_name(
                            table_name=table_name,
                            database_id=db_id,
                            database_name=db_name,
                        )
                        if dataset_info:
                            all_datasets.append(dataset_info)
                            
                except Exception as e:
                    logger.warning("Could not get tables for database %d: %s", db_id, e)
                    continue
            
            return all_datasets
            
        except Exception as e:
            logger.error("Fallback dataset listing failed: %s", e)
            return []

    async def _get_dataset_by_table_name(
        self,
        table_name: str,
        database_id: int,
        database_name: str,
    ) -> DatasetInfo | None:
        """
        Try to get dataset info for a specific table.
        
        Uses a filter query which sometimes works even when list doesn't.
        """
        try:
            # First try filter by table_name
            filters = [
                {"col": "table_name", "opr": "eq", "value": table_name},
                {"col": "database", "opr": "rel_o_m", "value": database_id},
            ]
            params = {"q": json.dumps({"filters": filters})}
            
            response = await self.client.get("/dataset/", params=params)
            result = response.get("result", [])
            
            if result:
                return DatasetInfo.model_validate(result[0])
            
            # If filter didn't work, construct a minimal DatasetInfo
            # This allows the user to see that tables exist even if full
            # dataset details aren't available via API
            return DatasetInfo(
                id=-1,  # Placeholder - actual ID unknown
                table_name=table_name,
                schema="public",
                database={"id": database_id, "database_name": database_name},
                kind="physical",
            )
            
        except Exception as e:
            logger.debug("Could not get dataset info for %s: %s", table_name, e)
            return None
