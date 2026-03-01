"""Discovery operations — list databases, schemas, tables, datasets, columns."""

from __future__ import annotations

from typing import Any

from superset_ai.api.databases import DatabaseService
from superset_ai.api.datasets import DatasetService


async def list_databases(db_svc: DatabaseService) -> list[dict[str, Any]]:
    """List all available database connections in Superset.

    Returns:
        List of dicts with ``id``, ``database_name``, and ``backend`` keys.
    """
    databases = await db_svc.list_databases()
    return [
        {
            "id": db.id,
            "database_name": db.database_name,
            "backend": db.backend,
        }
        for db in databases
    ]


async def list_schemas(db_svc: DatabaseService, database_id: int) -> list[str]:
    """List all schemas in a specific database.

    Returns:
        List of schema name strings.
    """
    return await db_svc.list_schemas(database_id)


async def list_tables(
    db_svc: DatabaseService,
    database_id: int,
    schema_name: str | None = None,
) -> list[dict[str, Any]]:
    """List all tables in a specific database schema.

    Returns:
        List of dicts with ``name``, ``schema``, and ``type`` keys.
    """
    tables = await db_svc.list_tables(database_id, schema=schema_name)
    return [
        {
            "name": t.name,
            "schema": t.schema_,
            "type": t.type,
        }
        for t in tables
    ]


async def list_existing_datasets(
    ds_svc: DatasetService,
    database_id: int | None = None,
) -> list[dict[str, Any]]:
    """List existing datasets (registered tables) in Superset.

    Returns:
        List of dicts with ``id``, ``table_name``, ``database_id``, ``schema``.
    """
    datasets = await ds_svc.list_datasets(database_id=database_id)
    return [
        {
            "id": ds.id,
            "table_name": ds.table_name,
            "database_id": ds.database_id,
            "schema": ds.schema_,
        }
        for ds in datasets
    ]


async def get_dataset_columns(
    ds_svc: DatasetService,
    dataset_id: int,
) -> dict[str, Any]:
    """Get column information for a dataset.

    Returns:
        Dict with ``dataset_id``, ``table_name``, ``columns``,
        ``time_columns``, and ``numeric_columns``.
    """
    dataset = await ds_svc.get_dataset(dataset_id)

    columns = []
    time_columns: list[str] = []
    numeric_columns: list[str] = []

    for col in dataset.columns:
        col_info = {
            "name": col.column_name,
            "type": col.type,
            "is_time": col.is_dttm,
        }
        columns.append(col_info)

        if col.is_dttm:
            time_columns.append(col.column_name)
        if col.type_generic in (0, 1):  # INT, FLOAT
            numeric_columns.append(col.column_name)

    return {
        "dataset_id": dataset_id,
        "table_name": dataset.table_name,
        "columns": columns,
        "time_columns": time_columns,
        "numeric_columns": numeric_columns,
    }
