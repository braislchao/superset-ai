"""Pytest configuration and shared test fixtures."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

# ---------------------------------------------------------------------------
# Mock object factories — shared across all test modules
# ---------------------------------------------------------------------------


def make_mock_db(
    id: int = 1, database_name: str = "main", backend: str = "postgresql"
) -> MagicMock:
    m = MagicMock()
    m.id = id
    m.database_name = database_name
    m.backend = backend
    return m


def make_mock_table(name: str = "users", schema_: str = "public", type: str = "TABLE") -> MagicMock:
    m = MagicMock()
    m.name = name
    m.schema_ = schema_
    m.type = type
    return m


def make_mock_column(
    column_name: str = "id",
    type: str = "INTEGER",
    is_dttm: bool = False,
    type_generic: int = 0,
    filterable: bool = True,
    groupby: bool = True,
) -> MagicMock:
    m = MagicMock()
    m.column_name = column_name
    m.type = type
    m.is_dttm = is_dttm
    m.type_generic = type_generic
    m.filterable = filterable
    m.groupby = groupby
    return m


def make_mock_dataset_info(
    id: int = 10,
    table_name: str = "orders",
    database_id: int = 1,
    schema_: str = "public",
) -> MagicMock:
    m = MagicMock()
    m.id = id
    m.table_name = table_name
    m.database_id = database_id
    m.schema_ = schema_
    return m


def make_mock_dataset_detail(
    id: int = 10,
    table_name: str = "orders",
    columns: list | None = None,
) -> MagicMock:
    m = MagicMock()
    m.id = id
    m.table_name = table_name
    m.columns = columns or []
    return m


def make_mock_chart(
    id: int = 1,
    slice_name: str = "My Chart",
    viz_type: str = "echarts_timeseries_bar",
    datasource_id: int = 10,
    description: str = "",
    dashboards: list | None = None,
    params: dict | None = None,
) -> MagicMock:
    m = MagicMock()
    m.id = id
    m.slice_name = slice_name
    m.viz_type = viz_type
    m.datasource_id = datasource_id
    m.description = description
    m.dashboards = dashboards or []
    m.get_params.return_value = params or {}
    return m


def make_mock_dashboard(
    id: int = 1,
    dashboard_title: str = "My Dashboard",
    published: bool = True,
    slug: str | None = None,
    css: str | None = None,
    charts: list | None = None,
    position: dict | None = None,
    metadata: dict | None = None,
) -> MagicMock:
    m = MagicMock()
    m.id = id
    m.dashboard_title = dashboard_title
    m.published = published
    m.slug = slug
    m.css = css
    m.charts = charts or []
    m.get_position.return_value = position or {}
    m.get_metadata.return_value = metadata or {}
    return m


def make_mock_tool_context() -> MagicMock:
    """Create a mock ToolContext with all required service attributes."""
    ctx = MagicMock()
    ctx.charts = AsyncMock()
    ctx.dashboards = AsyncMock()
    ctx.datasets = AsyncMock()
    ctx.databases = AsyncMock()
    ctx.session = MagicMock()
    ctx.session.superset_context = MagicMock()
    ctx.session.superset_context.databases = None
    ctx.session.superset_context.discovered_tables = {}
    ctx.session.superset_context.discovered_columns = {}
    return ctx
