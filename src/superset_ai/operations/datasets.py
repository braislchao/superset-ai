"""Dataset operations — find or create datasets."""

from __future__ import annotations

from typing import Any

from superset_ai.api.datasets import DatasetService


async def find_or_create_dataset(
    ds_svc: DatasetService,
    database_id: int,
    table_name: str,
    schema_name: str | None = None,
) -> dict[str, Any]:
    """Find an existing dataset or create a new one for a table.

    Returns:
        Dict with ``id``, ``table_name``, ``columns``, and ``time_columns``.
    """
    dataset = await ds_svc.find_or_create(
        table_name=table_name,
        database_id=database_id,
        schema=schema_name,
    )
    return {
        "id": dataset.id,
        "table_name": dataset.table_name,
        "columns": [c.column_name for c in dataset.columns],
        "time_columns": [c.column_name for c in dataset.columns if c.is_dttm],
    }
