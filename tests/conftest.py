"""Pytest configuration and fixtures."""

import pytest


@pytest.fixture
def mock_superset_response():
    """Factory for creating mock Superset API responses."""
    def _create_response(result=None, id=None, count=None):
        response = {}
        if result is not None:
            response["result"] = result
        if id is not None:
            response["id"] = id
        if count is not None:
            response["count"] = count
        return response
    return _create_response
