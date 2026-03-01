"""Tests for dashboard position JSON generation."""

import json
import pytest

from superset_ai.schemas.dashboards import (
    generate_dashboard_metadata,
    generate_grid_layout,
    generate_position_json,
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
