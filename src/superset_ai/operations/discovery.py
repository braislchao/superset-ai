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
            "filterable": col.filterable,
            "groupby": col.groupby,
            "type_generic": col.type_generic,
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


async def execute_sql(
    db_svc: DatabaseService,
    database_id: int,
    sql: str,
    limit: int = 100,
) -> dict[str, Any]:
    """Execute a SQL query and return structured results.

    Args:
        db_svc: Database service instance.
        database_id: The database to run the query against.
        sql: The SQL query string.
        limit: Maximum rows to return.

    Returns:
        Dict with ``columns`` (list of column name strings),
        ``data`` (list of row lists), ``row_count``, and ``truncated``.
    """
    response = await db_svc.execute_sql(database_id, sql, limit=limit)

    # Superset returns columns as list of dicts with "name" (and sometimes
    # "type"), and data as list of dicts keyed by column name.
    raw_columns = response.get("columns", [])
    column_names = [
        c["name"] if isinstance(c, dict) else str(c) for c in raw_columns
    ]

    raw_data = response.get("data", [])
    # Normalise rows: convert list-of-dicts to list-of-lists in column order
    if raw_data and isinstance(raw_data[0], dict):
        data = [[row.get(col) for col in column_names] for row in raw_data]
    else:
        data = [list(row) for row in raw_data]

    row_count = len(data)
    truncated = row_count >= limit

    return {
        "columns": column_names,
        "data": data,
        "row_count": row_count,
        "truncated": truncated,
    }


async def profile_dataset(
    db_svc: DatabaseService,
    ds_svc: DatasetService,
    dataset_id: int,
    sample_size: int = 5,
) -> dict[str, Any]:
    """Profile a dataset to understand its data shape.

    Runs exploratory SQL queries against the dataset's underlying table to
    gather row count, per-column cardinality, null counts, and sample values.

    Args:
        db_svc: Database service instance.
        ds_svc: Dataset service instance.
        dataset_id: The dataset to profile.
        sample_size: Number of sample values to retrieve per column.

    Returns:
        Dict with ``dataset_id``, ``table_name``, ``row_count``, and
        ``columns`` (list of column profiles).
    """
    # 1. Fetch dataset metadata
    dataset = await ds_svc.get_dataset(dataset_id)
    table_name = dataset.table_name
    database_id = dataset.database_id

    if database_id is None:
        return {
            "dataset_id": dataset_id,
            "table_name": table_name,
            "error": "Dataset has no associated database_id",
        }

    # Qualify table name with schema if available
    schema = dataset.schema_
    qualified_table = f"{schema}.{table_name}" if schema else table_name

    col_names = [col.column_name for col in dataset.columns]

    if not col_names:
        return {
            "dataset_id": dataset_id,
            "table_name": table_name,
            "row_count": 0,
            "columns": [],
        }

    # 2. Row count
    count_result = await execute_sql(
        db_svc, database_id, f"SELECT COUNT(*) AS cnt FROM {qualified_table}"
    )
    row_count = (
        count_result["data"][0][0] if count_result["data"] else 0
    )

    # 3. Cardinality + null counts in a single query
    parts = []
    for col in col_names:
        parts.append(f"COUNT(DISTINCT \"{col}\") AS \"{col}__cardinality\"")
        parts.append(f"SUM(CASE WHEN \"{col}\" IS NULL THEN 1 ELSE 0 END) AS \"{col}__nulls\"")

    stats_sql = f"SELECT {', '.join(parts)} FROM {qualified_table}"
    stats_result = await execute_sql(db_svc, database_id, stats_sql)

    # Build lookup from the single-row result
    stats_row: dict[str, Any] = {}
    if stats_result["data"]:
        for i, col_name in enumerate(stats_result["columns"]):
            stats_row[col_name] = stats_result["data"][0][i]

    # 4. Sample values
    sample_sql = f"SELECT * FROM {qualified_table} LIMIT {sample_size}"
    sample_result = await execute_sql(
        db_svc, database_id, sample_sql, limit=sample_size
    )

    # Build per-column sample values
    sample_by_col: dict[str, list[Any]] = {col: [] for col in col_names}
    for row in sample_result["data"]:
        for i, col in enumerate(sample_result["columns"]):
            if col in sample_by_col and i < len(row):
                sample_by_col[col].append(row[i])

    # 5. Assemble column profiles
    column_profiles = []
    for col_obj in dataset.columns:
        name = col_obj.column_name
        column_profiles.append({
            "name": name,
            "type": col_obj.type,
            "is_time": col_obj.is_dttm,
            "cardinality": stats_row.get(f"{name}__cardinality"),
            "null_count": stats_row.get(f"{name}__nulls"),
            "sample_values": sample_by_col.get(name, []),
        })

    return {
        "dataset_id": dataset_id,
        "table_name": table_name,
        "row_count": row_count,
        "columns": column_profiles,
    }
