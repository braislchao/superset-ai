"""Pydantic schemas for Superset Dashboard API."""

import json
from typing import Any

from pydantic import Field, field_serializer

from supersetai.schemas.common import BaseSchema, OwnerInfo, TimestampMixin


class DashboardInfo(TimestampMixin, BaseSchema):
    """
    Dashboard information returned from list endpoint.
    GET /api/v1/dashboard/
    """

    id: int
    dashboard_title: str
    slug: str | None = None
    url: str | None = None
    status: str | None = None
    published: bool = False
    certified_by: str | None = None
    certification_details: str | None = None
    owners: list[OwnerInfo] = Field(default_factory=list)


class DashboardDetail(TimestampMixin, BaseSchema):
    """
    Detailed dashboard information.
    GET /api/v1/dashboard/{id}
    """

    id: int
    dashboard_title: str
    slug: str | None = None
    position_json: str | None = None  # JSON string
    css: str | None = None
    json_metadata: str | None = None  # JSON string
    published: bool = False
    certified_by: str | None = None
    certification_details: str | None = None
    owners: list[OwnerInfo] = Field(default_factory=list)
    # Charts can be returned as strings (names) or dicts depending on API response
    charts: list[str | dict[str, Any]] = Field(default_factory=list)

    def get_position(self) -> dict[str, Any]:
        """Parse position_json to dict."""
        if self.position_json:
            return json.loads(self.position_json)
        return {}

    def get_metadata(self) -> dict[str, Any]:
        """Parse json_metadata to dict."""
        if self.json_metadata:
            return json.loads(self.json_metadata)
        return {}


class DashboardCreate(BaseSchema):
    """
    Schema for creating a new dashboard.
    POST /api/v1/dashboard/
    """

    dashboard_title: str = Field(..., description="Dashboard title")
    slug: str | None = Field(
        default=None,
        description="URL-friendly identifier",
    )
    position_json: str | None = Field(
        default=None,
        description="JSON-encoded layout position data",
    )
    json_metadata: str | None = Field(
        default=None,
        description="JSON-encoded dashboard metadata",
    )
    css: str | None = Field(
        default=None,
        description="Custom CSS for the dashboard",
    )
    published: bool = False
    owners: list[int] = Field(default_factory=list)

    @field_serializer("position_json", "json_metadata")
    def serialize_json_fields(self, v: str | None) -> str | None:
        """Validate JSON fields."""
        if v is not None:
            json.loads(v)  # Validate
        return v


class DashboardUpdate(BaseSchema):
    """
    Schema for updating a dashboard.
    PUT /api/v1/dashboard/{id}
    """

    dashboard_title: str | None = None
    slug: str | None = None
    position_json: str | None = None
    json_metadata: str | None = None
    css: str | None = None
    published: bool | None = None
    owners: list[int] | None = None


class DashboardListParams(BaseSchema):
    """Query parameters for listing dashboards."""

    page: int = 0
    page_size: int = 100
    q: str | None = Field(
        default=None,
        description="JSON-encoded filter/sort parameters",
    )


# =============================================================================
# Position JSON Builders
# =============================================================================


def generate_position_json(chart_ids: list[int]) -> str:
    """
    Generate a simple vertical stack layout for charts.
    
    Each chart gets a full-width row in a 12-column grid.
    This is a safe, simple layout that works reliably.
    
    Args:
        chart_ids: List of chart IDs to include
    
    Returns:
        JSON string for position_json field
    """
    position: dict[str, Any] = {
        "DASHBOARD_VERSION_KEY": "v2",
        "ROOT_ID": {
            "type": "ROOT",
            "id": "ROOT_ID",
            "children": ["GRID_ID"],
        },
        "GRID_ID": {
            "type": "GRID",
            "id": "GRID_ID",
            "children": [],
            "parents": ["ROOT_ID"],
        },
        "HEADER_ID": {
            "type": "HEADER",
            "id": "HEADER_ID",
            "meta": {"text": "Dashboard"},
        },
    }

    for i, chart_id in enumerate(chart_ids):
        row_id = f"ROW-{i}"
        chart_key = f"CHART-{chart_id}"

        # Add row to grid
        position["GRID_ID"]["children"].append(row_id)

        # Create row
        position[row_id] = {
            "type": "ROW",
            "id": row_id,
            "children": [chart_key],
            "parents": ["GRID_ID"],
            "meta": {"background": "BACKGROUND_TRANSPARENT"},
        }

        # Create chart component
        position[chart_key] = {
            "type": "CHART",
            "id": chart_key,
            "children": [],
            "parents": [row_id],
            "meta": {
                "width": 12,  # Full width (12-column grid)
                "height": 50,  # Default height in grid units
                "chartId": chart_id,
                "sliceName": f"Chart {chart_id}",
            },
        }

    return json.dumps(position)


def generate_grid_layout(
    chart_ids: list[int],
    columns: int = 2,
) -> str:
    """
    Generate a grid layout with multiple charts per row.
    
    Args:
        chart_ids: List of chart IDs to include
        columns: Number of columns (1, 2, 3, 4, or 6 for even division of 12)
    
    Returns:
        JSON string for position_json field
    """
    if columns not in (1, 2, 3, 4, 6):
        columns = 2

    width = 12 // columns

    position: dict[str, Any] = {
        "DASHBOARD_VERSION_KEY": "v2",
        "ROOT_ID": {
            "type": "ROOT",
            "id": "ROOT_ID",
            "children": ["GRID_ID"],
        },
        "GRID_ID": {
            "type": "GRID",
            "id": "GRID_ID",
            "children": [],
            "parents": ["ROOT_ID"],
        },
        "HEADER_ID": {
            "type": "HEADER",
            "id": "HEADER_ID",
            "meta": {"text": "Dashboard"},
        },
    }

    # Group charts into rows
    rows: list[list[int]] = []
    current_row: list[int] = []

    for chart_id in chart_ids:
        current_row.append(chart_id)
        if len(current_row) >= columns:
            rows.append(current_row)
            current_row = []

    if current_row:
        rows.append(current_row)

    # Build position for each row
    for row_idx, row_charts in enumerate(rows):
        row_id = f"ROW-{row_idx}"
        position["GRID_ID"]["children"].append(row_id)

        row_children = []
        for chart_id in row_charts:
            chart_key = f"CHART-{chart_id}"
            row_children.append(chart_key)

            position[chart_key] = {
                "type": "CHART",
                "id": chart_key,
                "children": [],
                "parents": [row_id],
                "meta": {
                    "width": width,
                    "height": 50,
                    "chartId": chart_id,
                    "sliceName": f"Chart {chart_id}",
                },
            }

        position[row_id] = {
            "type": "ROW",
            "id": row_id,
            "children": row_children,
            "parents": ["GRID_ID"],
            "meta": {"background": "BACKGROUND_TRANSPARENT"},
        }

    return json.dumps(position)


def generate_dashboard_metadata(
    chart_ids: list[int],
    *,
    refresh_frequency: int = 0,
    color_scheme: str = "supersetColors",
) -> str:
    """
    Generate dashboard metadata JSON.
    
    Args:
        chart_ids: List of chart IDs in the dashboard
        refresh_frequency: Auto-refresh interval in seconds (0 = disabled)
        color_scheme: Color scheme name
    
    Returns:
        JSON string for json_metadata field
    
    Note:
        Do NOT include a 'positions' key here - Superset uses that to override
        position_json, which would cause layout data to be lost.
    """
    metadata = {
        "timed_refresh_immune_slices": [],
        "expanded_slices": {},
        "refresh_frequency": refresh_frequency,
        "default_filters": "{}",
        "color_scheme": color_scheme,
        "label_colors": {},
        "shared_label_colors": {},
        "color_scheme_domain": [],
        "cross_filters_enabled": True,
        "chart_configuration": {},
    }

    return json.dumps(metadata)
