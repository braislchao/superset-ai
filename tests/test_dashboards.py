"""Tests for dashboard position JSON generation."""

import json
import pytest

from superset_ai.schemas.dashboards import (
    _add_charts_to_position,
    _extract_chart_ids_from_position,
    _has_tabs,
    _remove_chart_from_position,
    build_native_filter,
    generate_dashboard_metadata,
    generate_grid_layout,
    generate_position_json,
    generate_tabbed_layout,
)


class TestGeneratePositionJson:
    """Tests for vertical position JSON generation."""
    
    def test_generates_valid_json(self):
        """Should generate valid JSON."""
        position = generate_position_json([1, 2, 3])
        parsed = json.loads(position)
        
        assert isinstance(parsed, dict)
    
    def test_includes_version_key(self):
        """Should include dashboard version key."""
        position = generate_position_json([1])
        parsed = json.loads(position)
        
        assert parsed["DASHBOARD_VERSION_KEY"] == "v2"
    
    def test_includes_root_and_grid(self):
        """Should include ROOT and GRID components."""
        position = generate_position_json([1])
        parsed = json.loads(position)
        
        assert "ROOT_ID" in parsed
        assert "GRID_ID" in parsed
        assert parsed["ROOT_ID"]["type"] == "ROOT"
        assert parsed["GRID_ID"]["type"] == "GRID"
    
    def test_creates_row_per_chart(self):
        """Should create one row per chart."""
        chart_ids = [10, 20, 30]
        position = generate_position_json(chart_ids)
        parsed = json.loads(position)
        
        # Check rows exist
        assert "ROW-0" in parsed
        assert "ROW-1" in parsed
        assert "ROW-2" in parsed
    
    def test_creates_chart_components(self):
        """Should create chart components with correct metadata."""
        chart_ids = [42, 43]
        position = generate_position_json(chart_ids)
        parsed = json.loads(position)
        
        assert "CHART-42" in parsed
        assert "CHART-43" in parsed
        
        chart_42 = parsed["CHART-42"]
        assert chart_42["type"] == "CHART"
        assert chart_42["meta"]["chartId"] == 42
        assert chart_42["meta"]["width"] == 12  # Full width
    
    def test_handles_empty_list(self):
        """Should handle empty chart list."""
        position = generate_position_json([])
        parsed = json.loads(position)
        
        assert parsed["GRID_ID"]["children"] == []


class TestGenerateGridLayout:
    """Tests for grid layout generation."""
    
    def test_creates_two_column_layout(self):
        """Should create 2-column layout by default."""
        chart_ids = [1, 2, 3, 4]
        position = generate_grid_layout(chart_ids, columns=2)
        parsed = json.loads(position)
        
        # Should have 2 rows (4 charts / 2 columns)
        assert "ROW-0" in parsed
        assert "ROW-1" in parsed
        
        # Each chart should have width 6 (12 / 2)
        assert parsed["CHART-1"]["meta"]["width"] == 6
    
    def test_creates_three_column_layout(self):
        """Should create 3-column layout."""
        chart_ids = [1, 2, 3]
        position = generate_grid_layout(chart_ids, columns=3)
        parsed = json.loads(position)
        
        # Should have 1 row
        assert "ROW-0" in parsed
        assert len(parsed["ROW-0"]["children"]) == 3
        
        # Each chart should have width 4 (12 / 3)
        assert parsed["CHART-1"]["meta"]["width"] == 4
    
    def test_handles_partial_row(self):
        """Should handle partial final row."""
        chart_ids = [1, 2, 3]  # 3 charts in 2-column layout
        position = generate_grid_layout(chart_ids, columns=2)
        parsed = json.loads(position)
        
        # Should have 2 rows
        assert "ROW-0" in parsed
        assert "ROW-1" in parsed
        
        # First row has 2 charts, second has 1
        assert len(parsed["ROW-0"]["children"]) == 2
        assert len(parsed["ROW-1"]["children"]) == 1


class TestGenerateDashboardMetadata:
    """Tests for dashboard metadata generation."""
    
    def test_generates_valid_json(self):
        """Should generate valid JSON."""
        metadata = generate_dashboard_metadata([1, 2, 3])
        parsed = json.loads(metadata)
        
        assert isinstance(parsed, dict)
    
    def test_includes_default_color_scheme(self):
        """Should include default color scheme."""
        metadata = generate_dashboard_metadata([1])
        parsed = json.loads(metadata)
        
        assert parsed["color_scheme"] == "supersetColors"
    
    def test_includes_refresh_frequency(self):
        """Should include refresh frequency setting."""
        metadata = generate_dashboard_metadata([1], refresh_frequency=60)
        parsed = json.loads(metadata)
        
        assert parsed["refresh_frequency"] == 60
    
    def test_cross_filters_enabled(self):
        """Should enable cross filters by default."""
        metadata = generate_dashboard_metadata([1])
        parsed = json.loads(metadata)
        
        assert parsed["cross_filters_enabled"] is True

    def test_custom_color_scheme(self):
        """Should accept a custom color scheme."""
        metadata = generate_dashboard_metadata([1], color_scheme="d3Category10")
        parsed = json.loads(metadata)

        assert parsed["color_scheme"] == "d3Category10"


# =============================================================================
# Tabbed layout tests
# =============================================================================


class TestGenerateTabbedLayout:
    """Tests for tabbed layout generation."""

    def test_generates_valid_json(self):
        """Should generate valid JSON."""
        result = generate_tabbed_layout({"Tab A": [1], "Tab B": [2]})
        parsed = json.loads(result)
        assert isinstance(parsed, dict)

    def test_includes_version_key(self):
        """Should include dashboard version key."""
        parsed = json.loads(generate_tabbed_layout({"Tab A": [1]}))
        assert parsed["DASHBOARD_VERSION_KEY"] == "v2"

    def test_creates_tabs_container(self):
        """Should create a TABS container under GRID_ID."""
        parsed = json.loads(generate_tabbed_layout({"A": [1], "B": [2]}))
        grid_children = parsed["GRID_ID"]["children"]
        assert len(grid_children) == 1
        tabs_id = grid_children[0]
        assert tabs_id.startswith("TABS-")
        assert parsed[tabs_id]["type"] == "TABS"

    def test_creates_tab_per_key(self):
        """Should create one TAB component per dict key."""
        parsed = json.loads(generate_tabbed_layout({"Sales": [1], "Marketing": [2, 3]}))
        tabs_id = parsed["GRID_ID"]["children"][0]
        tab_ids = parsed[tabs_id]["children"]
        assert len(tab_ids) == 2

        tab_labels = {parsed[tid]["meta"]["text"] for tid in tab_ids}
        assert tab_labels == {"Sales", "Marketing"}

    def test_creates_chart_components_in_tabs(self):
        """Should create CHART components inside their respective tabs."""
        parsed = json.loads(generate_tabbed_layout({"A": [10, 20], "B": [30]}))
        assert "CHART-10" in parsed
        assert "CHART-20" in parsed
        assert "CHART-30" in parsed
        assert parsed["CHART-10"]["meta"]["chartId"] == 10

    def test_charts_have_correct_parents(self):
        """Chart parents should trace through ROW."""
        parsed = json.loads(generate_tabbed_layout({"A": [1]}))
        chart = parsed["CHART-1"]
        row_id = chart["parents"][0]
        assert parsed[row_id]["type"] == "ROW"

    def test_empty_tabs(self):
        """Should handle tabs with empty chart lists."""
        parsed = json.loads(generate_tabbed_layout({"Empty": []}))
        tabs_id = parsed["GRID_ID"]["children"][0]
        tab_ids = parsed[tabs_id]["children"]
        assert len(tab_ids) == 1
        assert parsed[tab_ids[0]]["children"] == []


class TestHasTabs:
    """Tests for _has_tabs helper."""

    def test_detects_tabbed_layout(self):
        parsed = json.loads(generate_tabbed_layout({"A": [1]}))
        assert _has_tabs(parsed) is True

    def test_detects_non_tabbed_layout(self):
        parsed = json.loads(generate_position_json([1, 2]))
        assert _has_tabs(parsed) is False


class TestAddChartsToPosition:
    """Tests for tab-aware chart addition."""

    def test_adds_to_non_tabbed_layout(self):
        """Should append rows to GRID_ID for non-tabbed layouts."""
        position = json.loads(generate_position_json([1]))
        _add_charts_to_position(position, [2, 3])

        assert "CHART-2" in position
        assert "CHART-3" in position
        assert "CHART-1" in position  # original still there
        assert len(position["GRID_ID"]["children"]) == 3

    def test_skips_duplicate_charts(self):
        """Should not add charts that already exist."""
        position = json.loads(generate_position_json([1, 2]))
        _add_charts_to_position(position, [2, 3])

        assert "CHART-3" in position
        # CHART-2 should only appear once
        chart2_count = sum(
            1 for v in position.values()
            if isinstance(v, dict) and v.get("type") == "CHART"
            and v.get("meta", {}).get("chartId") == 2
        )
        assert chart2_count == 1

    def test_adds_to_first_tab_by_default(self):
        """Should add to the first tab when no tab_label given."""
        position = json.loads(generate_tabbed_layout({"A": [1], "B": [2]}))
        tabs_id = position["GRID_ID"]["children"][0]
        first_tab_id = position[tabs_id]["children"][0]

        _add_charts_to_position(position, [10])

        assert "CHART-10" in position
        # New chart's row should be a child of the first tab
        chart_row = position["CHART-10"]["parents"][0]
        assert chart_row in position[first_tab_id]["children"]

    def test_adds_to_named_tab(self):
        """Should add to a specific named tab."""
        position = json.loads(generate_tabbed_layout({"A": [1], "B": [2]}))
        tabs_id = position["GRID_ID"]["children"][0]
        # Find tab B
        tab_b_id = None
        for tid in position[tabs_id]["children"]:
            if position[tid]["meta"]["text"] == "B":
                tab_b_id = tid
                break
        assert tab_b_id is not None

        _add_charts_to_position(position, [10], tab_label="B")

        chart_row = position["CHART-10"]["parents"][0]
        assert chart_row in position[tab_b_id]["children"]

    def test_creates_new_tab_if_missing(self):
        """Should create a new tab if tab_label doesn't exist."""
        position = json.loads(generate_tabbed_layout({"A": [1]}))
        tabs_id = position["GRID_ID"]["children"][0]
        original_tab_count = len(position[tabs_id]["children"])

        _add_charts_to_position(position, [10], tab_label="New Tab")

        assert len(position[tabs_id]["children"]) == original_tab_count + 1
        assert "CHART-10" in position


class TestRemoveChartFromPosition:
    """Tests for tab-aware chart removal."""

    def test_removes_from_non_tabbed_layout(self):
        """Should remove chart and its empty row."""
        position = json.loads(generate_position_json([1, 2, 3]))
        _remove_chart_from_position(position, 2)

        assert "CHART-2" not in position
        assert "CHART-1" in position
        assert "CHART-3" in position

    def test_removes_from_tabbed_layout(self):
        """Should remove chart from a tabbed layout."""
        position = json.loads(generate_tabbed_layout({"A": [1, 2], "B": [3]}))
        _remove_chart_from_position(position, 1)

        assert "CHART-1" not in position
        assert "CHART-2" in position
        assert "CHART-3" in position

    def test_noop_for_missing_chart(self):
        """Should do nothing if chart doesn't exist."""
        position = json.loads(generate_position_json([1]))
        original = json.dumps(position, sort_keys=True)
        _remove_chart_from_position(position, 999)
        assert json.dumps(position, sort_keys=True) == original

    def test_removes_empty_row(self):
        """Should remove the parent row when it becomes empty."""
        position = json.loads(generate_position_json([1, 2]))
        _remove_chart_from_position(position, 1)

        # ROW-0 was the parent of CHART-1 and should be gone
        assert "ROW-0" not in position
        # ROW-1 still exists for CHART-2
        assert "ROW-1" in position


class TestExtractChartIdsFromPosition:
    """Tests for _extract_chart_ids_from_position helper."""

    def test_extracts_from_flat_layout(self):
        position = json.loads(generate_position_json([10, 20, 30]))
        ids = _extract_chart_ids_from_position(position)
        assert sorted(ids) == [10, 20, 30]

    def test_extracts_from_tabbed_layout(self):
        position = json.loads(generate_tabbed_layout({"A": [1, 2], "B": [3]}))
        ids = _extract_chart_ids_from_position(position)
        assert sorted(ids) == [1, 2, 3]


# =============================================================================
# Native filter tests
# =============================================================================


class TestBuildNativeFilter:
    """Tests for build_native_filter."""

    def test_builds_select_filter(self):
        """Should build a valid filter_select configuration."""
        f = build_native_filter(
            name="Country",
            filter_type="filter_select",
            dataset_id=42,
            column="country_name",
        )
        assert f["name"] == "Country"
        assert f["filterType"] == "filter_select"
        assert f["id"].startswith("NATIVE_FILTER-")
        assert f["targets"][0]["datasetId"] == 42
        assert f["targets"][0]["column"]["name"] == "country_name"
        assert f["controlValues"]["multiSelect"] is True

    def test_builds_range_filter(self):
        """Should build a valid filter_range configuration."""
        f = build_native_filter(
            name="Price Range",
            filter_type="filter_range",
            dataset_id=10,
            column="price",
        )
        assert f["filterType"] == "filter_range"
        assert f["targets"][0]["datasetId"] == 10

    def test_builds_time_filter(self):
        """Should build a filter_time without dataset/column."""
        f = build_native_filter(
            name="Time",
            filter_type="filter_time",
        )
        assert f["filterType"] == "filter_time"
        assert f["targets"] == [{}]

    def test_rejects_invalid_filter_type(self):
        """Should raise ValueError for unknown filter types."""
        with pytest.raises(ValueError, match="Invalid filter_type"):
            build_native_filter(
                name="Bad",
                filter_type="filter_invalid",
                dataset_id=1,
                column="x",
            )

    def test_requires_dataset_for_non_time_filters(self):
        """Should raise ValueError when dataset_id missing for select filter."""
        with pytest.raises(ValueError, match="requires both dataset_id and column"):
            build_native_filter(
                name="Bad",
                filter_type="filter_select",
            )

    def test_requires_column_for_non_time_filters(self):
        """Should raise ValueError when column missing for select filter."""
        with pytest.raises(ValueError, match="requires both dataset_id and column"):
            build_native_filter(
                name="Bad",
                filter_type="filter_select",
                dataset_id=1,
            )

    def test_default_scope_all_charts(self):
        """Default scope should target all charts."""
        f = build_native_filter(
            name="X",
            filter_type="filter_select",
            dataset_id=1,
            column="x",
        )
        assert f["scope"]["rootPath"] == ["ROOT_ID"]
        assert f["scope"]["excluded"] == []

    def test_exclude_chart_ids(self):
        """Should set excluded chart IDs in scope."""
        f = build_native_filter(
            name="X",
            filter_type="filter_select",
            dataset_id=1,
            column="x",
            exclude_chart_ids=[10, 20],
        )
        assert f["scope"]["excluded"] == [10, 20]

    def test_control_values(self):
        """Should respect control value parameters."""
        f = build_native_filter(
            name="X",
            filter_type="filter_select",
            dataset_id=1,
            column="x",
            multi_select=False,
            default_to_first_item=True,
            sort_ascending=False,
        )
        assert f["controlValues"]["multiSelect"] is False
        assert f["controlValues"]["defaultToFirstItem"] is True
        assert f["controlValues"]["sortAscending"] is False

    def test_unique_filter_ids(self):
        """Each call should produce a unique filter ID."""
        ids = set()
        for _ in range(10):
            f = build_native_filter(
                name="X",
                filter_type="filter_select",
                dataset_id=1,
                column="x",
            )
            ids.add(f["id"])
        assert len(ids) == 10
