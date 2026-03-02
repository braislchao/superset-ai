"""Pydantic schemas for Superset Dashboard API."""

import json
import uuid
from typing import Any

from pydantic import Field, field_serializer

from superset_ai.schemas.common import BaseSchema, OwnerInfo, TimestampMixin


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


def generate_tabbed_layout(
    tabs: dict[str, list[int]],
) -> str:
    """
    Generate a tabbed layout for a dashboard.

    Each key in ``tabs`` is a tab label, and each value is a list of chart IDs
    to place in that tab. Charts within each tab are stacked vertically
    (full-width rows).

    Args:
        tabs: Mapping of tab label to list of chart IDs.
              Example: ``{"Overview": [1, 2], "Details": [3, 4, 5]}``

    Returns:
        JSON string for position_json field
    """
    tabs_id = f"TABS-{uuid.uuid4().hex[:8]}"

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
            "children": [tabs_id],
            "parents": ["ROOT_ID"],
        },
        "HEADER_ID": {
            "type": "HEADER",
            "id": "HEADER_ID",
            "meta": {"text": "Dashboard"},
        },
    }

    # Create the TABS container
    tab_children: list[str] = []

    row_counter = 0

    for tab_label, chart_ids in tabs.items():
        tab_id = f"TAB-{uuid.uuid4().hex[:8]}"
        tab_children.append(tab_id)

        tab_row_ids: list[str] = []

        for chart_id in chart_ids:
            row_id = f"ROW-{row_counter}"
            chart_key = f"CHART-{chart_id}"
            tab_row_ids.append(row_id)

            position[row_id] = {
                "type": "ROW",
                "id": row_id,
                "children": [chart_key],
                "parents": [tabs_id, tab_id],
                "meta": {"background": "BACKGROUND_TRANSPARENT"},
            }

            position[chart_key] = {
                "type": "CHART",
                "id": chart_key,
                "children": [],
                "parents": [row_id],
                "meta": {
                    "width": 12,
                    "height": 50,
                    "chartId": chart_id,
                    "sliceName": f"Chart {chart_id}",
                },
            }

            row_counter += 1

        position[tab_id] = {
            "type": "TAB",
            "id": tab_id,
            "children": tab_row_ids,
            "parents": [tabs_id],
            "meta": {"text": tab_label},
        }

    position[tabs_id] = {
        "type": "TABS",
        "id": tabs_id,
        "children": tab_children,
        "parents": ["GRID_ID"],
    }

    return json.dumps(position)


def _has_tabs(position: dict[str, Any]) -> bool:
    """Check whether a position dict contains a tabbed layout."""
    for value in position.values():
        if isinstance(value, dict) and value.get("type") == "TABS":
            return True
    return False


def _add_charts_to_position(
    position: dict[str, Any],
    chart_ids: list[int],
    *,
    tab_label: str | None = None,
) -> dict[str, Any]:
    """Add charts to an existing position layout (tab-aware).

    If the layout is tabbed and ``tab_label`` is provided, charts are added
    to that tab (creating a new tab if it doesn't exist).  If the layout is
    tabbed and ``tab_label`` is ``None``, charts are added to the **first**
    tab.  If the layout is not tabbed, charts are appended as new rows under
    ``GRID_ID``.

    Returns:
        The mutated position dict (same object, for convenience).
    """
    # Determine the highest ROW-N index already present
    existing_row_indices = [
        int(k.split("-")[1]) for k in position if k.startswith("ROW-") and k.split("-")[1].isdigit()
    ]
    next_row = (max(existing_row_indices) + 1) if existing_row_indices else 0

    # Filter out charts that already exist in the layout
    existing_chart_ids = _extract_chart_ids_from_position(position)
    new_chart_ids = [cid for cid in chart_ids if cid not in existing_chart_ids]
    if not new_chart_ids:
        return position

    if _has_tabs(position):
        # Find the TABS container
        tabs_id: str | None = None
        for key, value in position.items():
            if isinstance(value, dict) and value.get("type") == "TABS":
                tabs_id = key
                break
        assert tabs_id is not None

        # Find or create the target tab
        target_tab_id: str | None = None
        if tab_label is not None:
            # Search for matching tab
            for child_id in position[tabs_id]["children"]:
                tab_comp = position.get(child_id, {})
                if tab_comp.get("meta", {}).get("text") == tab_label:
                    target_tab_id = child_id
                    break
            # Create new tab if not found
            if target_tab_id is None:
                target_tab_id = f"TAB-{uuid.uuid4().hex[:8]}"
                position[target_tab_id] = {
                    "type": "TAB",
                    "id": target_tab_id,
                    "children": [],
                    "parents": [tabs_id],
                    "meta": {"text": tab_label},
                }
                position[tabs_id]["children"].append(target_tab_id)
        else:
            # Default to first tab
            target_tab_id = position[tabs_id]["children"][0]

        assert target_tab_id is not None
        parent_chain = [tabs_id, target_tab_id]

        for chart_id in new_chart_ids:
            row_id = f"ROW-{next_row}"
            chart_key = f"CHART-{chart_id}"

            position[target_tab_id]["children"].append(row_id)

            position[row_id] = {
                "type": "ROW",
                "id": row_id,
                "children": [chart_key],
                "parents": parent_chain,
                "meta": {"background": "BACKGROUND_TRANSPARENT"},
            }
            position[chart_key] = {
                "type": "CHART",
                "id": chart_key,
                "children": [],
                "parents": [row_id],
                "meta": {
                    "width": 12,
                    "height": 50,
                    "chartId": chart_id,
                    "sliceName": f"Chart {chart_id}",
                },
            }
            next_row += 1
    else:
        # Non-tabbed layout: append rows to GRID_ID
        for chart_id in new_chart_ids:
            row_id = f"ROW-{next_row}"
            chart_key = f"CHART-{chart_id}"

            position["GRID_ID"]["children"].append(row_id)

            position[row_id] = {
                "type": "ROW",
                "id": row_id,
                "children": [chart_key],
                "parents": ["GRID_ID"],
                "meta": {"background": "BACKGROUND_TRANSPARENT"},
            }
            position[chart_key] = {
                "type": "CHART",
                "id": chart_key,
                "children": [],
                "parents": [row_id],
                "meta": {
                    "width": 12,
                    "height": 50,
                    "chartId": chart_id,
                    "sliceName": f"Chart {chart_id}",
                },
            }
            next_row += 1

    return position


def _remove_chart_from_position(
    position: dict[str, Any],
    chart_id: int,
) -> dict[str, Any]:
    """Remove a chart from a position layout (tab-aware).

    Removes the CHART component and its parent ROW if the row becomes empty.
    Works for both tabbed and non-tabbed layouts.

    Returns:
        The mutated position dict.
    """
    chart_key = f"CHART-{chart_id}"
    if chart_key not in position:
        return position

    # Find the parent row
    chart_comp = position[chart_key]
    parent_ids = chart_comp.get("parents", [])

    # Remove chart component
    del position[chart_key]

    # Clean up parent row
    for parent_id in parent_ids:
        if parent_id in position and position[parent_id].get("type") == "ROW":
            row = position[parent_id]
            if chart_key in row["children"]:
                row["children"].remove(chart_key)
            # Remove empty row
            if not row["children"]:
                # Remove row from its parent's children list
                for row_parent_id in row.get("parents", []):
                    if row_parent_id in position:
                        parent_comp = position[row_parent_id]
                        if isinstance(parent_comp, dict) and parent_id in parent_comp.get(
                            "children", []
                        ):
                            parent_comp["children"].remove(parent_id)
                del position[parent_id]

    return position


def _extract_chart_ids_from_position(position: dict[str, Any]) -> list[int]:
    """Extract all chart IDs from a position dict."""
    chart_ids: list[int] = []
    for value in position.values():
        if isinstance(value, dict) and value.get("type") == "CHART":
            meta = value.get("meta", {})
            chart_id = meta.get("chartId")
            if chart_id is not None:
                chart_ids.append(chart_id)
    return chart_ids


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


# =============================================================================
# Native Filter Builders
# =============================================================================

# Valid filter types in Superset 3.x
VALID_FILTER_TYPES = frozenset(
    {
        "filter_select",
        "filter_range",
        "filter_time",
        "filter_timecolumn",
        "filter_timegrain",
    }
)


def build_native_filter(
    *,
    name: str,
    filter_type: str = "filter_select",
    dataset_id: int | None = None,
    column: str | None = None,
    scope_chart_ids: list[int] | None = None,
    exclude_chart_ids: list[int] | None = None,
    cascade_parent_ids: list[str] | None = None,
    multi_select: bool = True,
    default_to_first_item: bool = False,
    sort_ascending: bool = True,
    enable_empty_filter: bool = False,
    search_all_options: bool = False,
    inverse_selection: bool = False,
    description: str = "",
) -> dict[str, Any]:
    """
    Build a native filter configuration dict for Superset 3.x dashboards.

    Args:
        name: Display name for the filter.
        filter_type: One of ``filter_select``, ``filter_range``,
            ``filter_time``, ``filter_timecolumn``, ``filter_timegrain``.
        dataset_id: Dataset ID for column-based filters.  Required for all
            types except ``filter_time``.
        column: Column name.  Required for all types except ``filter_time``.
        scope_chart_ids: If set, limits the filter scope to these chart IDs.
            By default, the filter applies to all charts.
        exclude_chart_ids: Chart IDs to exclude from the filter scope.
        cascade_parent_ids: List of parent filter IDs for cascading filters.
        multi_select: Allow multiple values (``filter_select`` only).
        default_to_first_item: Pre-select the first value.
        sort_ascending: Sort filter values ascending.
        enable_empty_filter: Whether an empty filter selection filters out all data.
        search_all_options: Enable search across all filter options.
        inverse_selection: Invert the filter selection.
        description: Optional description text.

    Returns:
        A dict suitable for appending to ``json_metadata.native_filter_configuration``.
    """
    if filter_type not in VALID_FILTER_TYPES:
        raise ValueError(
            f"Invalid filter_type '{filter_type}'. "
            f"Must be one of: {', '.join(sorted(VALID_FILTER_TYPES))}"
        )

    filter_id = f"NATIVE_FILTER-{uuid.uuid4().hex[:8]}"

    # Build targets
    if filter_type == "filter_time":
        targets: list[dict[str, Any]] = [{}]
    else:
        if dataset_id is None or column is None:
            raise ValueError(f"filter_type '{filter_type}' requires both dataset_id and column.")
        targets = [{"datasetId": dataset_id, "column": {"name": column}}]

    # Build scope
    scope: dict[str, Any] = {
        "rootPath": ["ROOT_ID"],
        "excluded": exclude_chart_ids or [],
    }

    return {
        "id": filter_id,
        "name": name,
        "type": "NATIVE_FILTER",
        "filterType": filter_type,
        "targets": targets,
        "scope": scope,
        "defaultDataMask": {
            "filterState": {"value": None},
            "extraFormData": {},
        },
        "cascadeParentIds": cascade_parent_ids or [],
        "controlValues": {
            "enableEmptyFilter": enable_empty_filter,
            "defaultToFirstItem": default_to_first_item,
            "multiSelect": multi_select,
            "searchAllOptions": search_all_options,
            "inverseSelection": inverse_selection,
            "sortAscending": sort_ascending,
        },
        "description": description,
    }
