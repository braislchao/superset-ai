"""Discovery operations — list databases, schemas, tables, datasets, columns."""

from __future__ import annotations

import re
from typing import Any

from superset_ai.api.databases import DatabaseService
from superset_ai.api.datasets import DatasetService

# Backends that use backtick quoting; everything else uses double-quote (ANSI SQL).
_BACKTICK_BACKENDS = {"mysql", "sqlite"}

# Pattern for valid SQL identifiers — rejects anything suspicious.
_SAFE_IDENTIFIER_RE = re.compile(r"^[\w. ]+$", re.UNICODE)


def quote_identifier(name: str, backend: str | None = None) -> str:
    """Quote a SQL identifier (table or column name) for the given backend.

    Uses backticks for MySQL/SQLite and double-quotes for everything else
    (PostgreSQL, Presto, Trino, etc. — ANSI SQL standard).

    Raises ``ValueError`` if *name* contains the quote character for the
    target backend, preventing SQL injection via crafted identifiers.
    """
    if not _SAFE_IDENTIFIER_RE.match(name):
        raise ValueError(f"Unsafe SQL identifier rejected: {name!r}")

    if backend and backend.lower() in _BACKTICK_BACKENDS:
        if "`" in name:
            raise ValueError(f"Identifier contains backtick, cannot safely quote: {name!r}")
        return f"`{name}`"

    if '"' in name:
        raise ValueError(f"Identifier contains double-quote, cannot safely quote: {name!r}")
    return f'"{name}"'


def quote_table(table_name: str, schema: str | None, backend: str | None = None) -> str:
    """Build a fully-qualified, safely-quoted table reference."""
    quoted_table = quote_identifier(table_name, backend)
    if schema:
        return f"{quote_identifier(schema, backend)}.{quoted_table}"
    return quoted_table


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
    column_names = [c["name"] if isinstance(c, dict) else str(c) for c in raw_columns]

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

    # Resolve the DB backend for correct identifier quoting
    db_info = await db_svc.get_database(database_id)
    backend = db_info.backend

    # Qualify table name with schema if available
    schema = dataset.schema_
    qualified_table = quote_table(table_name, schema, backend)

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
    row_count = count_result["data"][0][0] if count_result["data"] else 0

    # 3. Cardinality + null counts in a single query
    parts = []
    for col in col_names:
        qcol = quote_identifier(col, backend)
        alias_card = quote_identifier(f"{col}__cardinality", backend)
        alias_nulls = quote_identifier(f"{col}__nulls", backend)
        parts.append(f"COUNT(DISTINCT {qcol}) AS {alias_card}")
        parts.append(f"SUM(CASE WHEN {qcol} IS NULL THEN 1 ELSE 0 END) AS {alias_nulls}")

    stats_sql = f"SELECT {', '.join(parts)} FROM {qualified_table}"
    stats_result = await execute_sql(db_svc, database_id, stats_sql)

    # Build lookup from the single-row result
    stats_row: dict[str, Any] = {}
    if stats_result["data"]:
        for i, col_name in enumerate(stats_result["columns"]):
            stats_row[col_name] = stats_result["data"][0][i]

    # 4. Sample values
    sample_sql = f"SELECT * FROM {qualified_table} LIMIT {sample_size}"
    sample_result = await execute_sql(db_svc, database_id, sample_sql, limit=sample_size)

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
        column_profiles.append(
            {
                "name": name,
                "type": col_obj.type,
                "is_time": col_obj.is_dttm,
                "cardinality": stats_row.get(f"{name}__cardinality"),
                "null_count": stats_row.get(f"{name}__nulls"),
                "sample_values": sample_by_col.get(name, []),
            }
        )

    return {
        "dataset_id": dataset_id,
        "table_name": table_name,
        "row_count": row_count,
        "columns": column_profiles,
    }


# ---------------------------------------------------------------------------
# Chart type suggestion
# ---------------------------------------------------------------------------


def suggest_chart_type(
    columns: list[dict[str, Any]],
    row_count: int = 0,
) -> list[dict[str, Any]]:
    """Suggest chart types based on column profiles.

    Analyzes column types, cardinality, and null counts to recommend
    appropriate chart types ordered by relevance.

    Args:
        columns: Column profiles as returned by ``profile_dataset``.
            Each dict has ``name``, ``type``, ``is_time``, ``cardinality``,
            and ``null_count``.
        row_count: Total rows in the dataset.

    Returns:
        List of recommendation dicts, each with ``chart_type``,
        ``reason``, and ``suggested_params`` (example parameter hints).
    """
    time_cols = [c for c in columns if c.get("is_time")]
    numeric_cols = _numeric_columns(columns)
    categorical_cols = _categorical_columns(columns)

    # Track low- and high-cardinality categoricals
    low_card_cats = [c for c in categorical_cols if (c.get("cardinality") or 0) <= 7]
    high_card_cats = [c for c in categorical_cols if (c.get("cardinality") or 0) > 7]

    recommendations: list[dict[str, Any]] = []

    # ---- Time-series charts ------------------------------------------------
    if time_cols and numeric_cols:
        tc = time_cols[0]["name"]
        m = _metric_expr(numeric_cols[0])

        recommendations.append(
            {
                "chart_type": "line",
                "reason": (
                    f"Time column '{tc}' + numeric column(s) available — "
                    "ideal for showing trends over time."
                ),
                "suggested_params": {
                    "metrics": [m],
                    "time_column": tc,
                },
            }
        )

        recommendations.append(
            {
                "chart_type": "area",
                "reason": (
                    f"Time column '{tc}' + numeric columns — area chart shows cumulative trends."
                ),
                "suggested_params": {
                    "metrics": [m],
                    "time_column": tc,
                    "stacked": True,
                },
            }
        )

        if categorical_cols:
            recommendations.append(
                {
                    "chart_type": "echarts_timeseries_bar",
                    "reason": (
                        f"Time column '{tc}' + numeric + categorical columns — "
                        "bar chart over time axis."
                    ),
                    "suggested_params": {
                        "metrics": [m],
                        "time_column": tc,
                        "dimensions": [categorical_cols[0]["name"]],
                    },
                }
            )

        recommendations.append(
            {
                "chart_type": "big_number",
                "reason": (f"Single KPI with trendline using time column '{tc}'."),
                "suggested_params": {
                    "metric": m,
                    "time_column": tc,
                },
            }
        )

    # ---- Category-based charts ---------------------------------------------
    if numeric_cols and categorical_cols:
        m = _metric_expr(numeric_cols[0])
        cat = categorical_cols[0]["name"]

        recommendations.append(
            {
                "chart_type": "dist_bar",
                "reason": (
                    f"Numeric + categorical columns — bar chart compares '{m}' across '{cat}'."
                ),
                "suggested_params": {
                    "metrics": [m],
                    "dimensions": [cat],
                },
            }
        )

        if low_card_cats:
            lc = low_card_cats[0]["name"]
            card = low_card_cats[0].get("cardinality", "?")
            recommendations.append(
                {
                    "chart_type": "pie",
                    "reason": (
                        f"Low-cardinality column '{lc}' ({card} values) — "
                        "good for part-of-whole comparison."
                    ),
                    "suggested_params": {
                        "metric": m,
                        "dimension": lc,
                    },
                }
            )

        if high_card_cats:
            hc = high_card_cats[0]["name"]
            card = high_card_cats[0].get("cardinality", "?")
            recommendations.append(
                {
                    "chart_type": "treemap_v2",
                    "reason": (
                        f"High-cardinality column '{hc}' ({card} values) — "
                        "treemap handles many categories better than pie."
                    ),
                    "suggested_params": {
                        "metric": m,
                        "dimensions": [hc],
                    },
                }
            )

    # ---- Two-categorical + numeric → heatmap --------------------------------
    if len(categorical_cols) >= 2 and numeric_cols:
        m = _metric_expr(numeric_cols[0])
        recommendations.append(
            {
                "chart_type": "heatmap",
                "reason": (
                    f"Two categorical columns + numeric — heatmap shows "
                    f"'{m}' across '{categorical_cols[0]['name']}' × "
                    f"'{categorical_cols[1]['name']}'."
                ),
                "suggested_params": {
                    "metric": m,
                    "x_column": categorical_cols[0]["name"],
                    "y_column": categorical_cols[1]["name"],
                },
            }
        )

    # ---- Three numeric → bubble ---------------------------------------------
    if len(numeric_cols) >= 3 and categorical_cols:
        recommendations.append(
            {
                "chart_type": "bubble",
                "reason": ("3+ numeric columns — bubble chart encodes x, y, and size."),
                "suggested_params": {
                    "x_metric": _metric_expr(numeric_cols[0]),
                    "y_metric": _metric_expr(numeric_cols[1]),
                    "size_metric": _metric_expr(numeric_cols[2]),
                    "series_column": categorical_cols[0]["name"],
                },
            }
        )

    # ---- Single numeric → histogram / gauge / big_number_total ---------------
    if numeric_cols:
        m = _metric_expr(numeric_cols[0])

        recommendations.append(
            {
                "chart_type": "histogram",
                "reason": (
                    f"Numeric column '{numeric_cols[0]['name']}' — "
                    "histogram shows value distribution."
                ),
                "suggested_params": {
                    "column": numeric_cols[0]["name"],
                },
            }
        )

        recommendations.append(
            {
                "chart_type": "big_number_total",
                "reason": "Single headline KPI metric.",
                "suggested_params": {
                    "metric": m,
                },
            }
        )

        recommendations.append(
            {
                "chart_type": "gauge_chart",
                "reason": ("Single metric on a scale — good for KPIs with known min/max bounds."),
                "suggested_params": {
                    "metric": m,
                    "min_val": 0,
                    "max_val": 100,
                },
            }
        )

    # ---- Numeric + categorical → box plot ------------------------------------
    if numeric_cols and categorical_cols:
        recommendations.append(
            {
                "chart_type": "box_plot",
                "reason": (
                    f"Compare distribution of '{numeric_cols[0]['name']}' "
                    f"across '{categorical_cols[0]['name']}' groups."
                ),
                "suggested_params": {
                    "metrics": [_metric_expr(numeric_cols[0])],
                    "dimensions": [categorical_cols[0]["name"]],
                },
            }
        )

    # ---- Table (always available) -------------------------------------------
    all_names = [c["name"] for c in columns[:10]]  # cap at 10
    recommendations.append(
        {
            "chart_type": "table",
            "reason": "Raw data table — always applicable.",
            "suggested_params": {
                "columns": all_names,
            },
        }
    )

    # De-duplicate by chart_type, keeping first occurrence (highest priority)
    seen: set[str] = set()
    unique: list[dict[str, Any]] = []
    for rec in recommendations:
        ct: str = rec["chart_type"]
        if ct not in seen:
            seen.add(ct)
            unique.append(rec)

    return unique


# ---------------------------------------------------------------------------
# Internal helpers for suggest_chart_type
# ---------------------------------------------------------------------------


def _numeric_columns(columns: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Filter columns that are numeric (INT or FLOAT generic types, or by type string)."""
    numeric_type_strings = {
        "INT",
        "INTEGER",
        "BIGINT",
        "SMALLINT",
        "FLOAT",
        "DOUBLE",
        "DECIMAL",
        "NUMERIC",
        "REAL",
        "NUMBER",
    }
    result = []
    for c in columns:
        if c.get("is_time"):
            continue
        # type_generic 0=INT, 1=FLOAT in Superset
        tg = c.get("type_generic")
        if tg in (0, 1) or c.get("type", "").upper().split("(")[0].strip() in numeric_type_strings:
            result.append(c)
    return result


def _categorical_columns(columns: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Filter columns that are categorical (non-numeric, non-time)."""
    numeric_type_strings = {
        "INT",
        "INTEGER",
        "BIGINT",
        "SMALLINT",
        "FLOAT",
        "DOUBLE",
        "DECIMAL",
        "NUMERIC",
        "REAL",
        "NUMBER",
    }
    result = []
    for c in columns:
        if c.get("is_time"):
            continue
        tg = c.get("type_generic")
        if tg in (0, 1):
            continue
        if c.get("type", "").upper().split("(")[0].strip() in numeric_type_strings:
            continue
        result.append(c)
    return result


def _metric_expr(col: dict[str, Any]) -> str:
    """Build a default metric expression for a numeric column."""
    return f"SUM({col['name']})"
