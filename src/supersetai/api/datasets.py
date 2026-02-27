"""Dataset service for Superset API operations."""

import json
import logging
from typing import TYPE_CHECKING

from supersetai.core.exceptions import ResourceNotFoundError
from supersetai.schemas.datasets import (
    DatasetCreate,
    DatasetDetail,
    DatasetInfo,
    DatasetUpdate,
)

if TYPE_CHECKING:
    from supersetai.api.client import SupersetClient

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
        return [DatasetInfo.model_validate(item) for item in result]

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
        
        logger.info(f"Creating dataset: {spec.table_name}")
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
        
        logger.info(f"Updating dataset {dataset_id}")
        await self.client.put(f"/dataset/{dataset_id}", json=payload)
        
        return await self.get_dataset(dataset_id)

    async def delete_dataset(self, dataset_id: int) -> None:
        """
        Delete a dataset.
        
        DELETE /api/v1/dataset/{id}
        """
        logger.info(f"Deleting dataset {dataset_id}")
        await self.client.delete(f"/dataset/{dataset_id}")

    async def refresh_columns(self, dataset_id: int) -> DatasetDetail:
        """
        Refresh dataset columns from the database.
        
        PUT /api/v1/dataset/{id}/refresh
        """
        logger.info(f"Refreshing columns for dataset {dataset_id}")
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
            logger.info(f"Found existing dataset: {existing.table_name} (id={existing.id})")
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
