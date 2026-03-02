"""Tests for SupersetClient HTTP client."""

import time
from unittest.mock import AsyncMock, patch

import httpx
import pytest
import respx

from superset_ai.api.auth import AuthSession
from superset_ai.api.client import SupersetClient
from superset_ai.core.config import SupersetConfig
from superset_ai.core.exceptions import (
    AuthenticationError,
    PermissionDeniedError,
    RateLimitError,
    ResourceNotFoundError,
    ServerError,
    SupersetAPIError,
    ValidationError,
)

BASE_URL = "http://test:8088"
API_URL = f"{BASE_URL}/api/v1"


@pytest.fixture
def config():
    """Create test configuration with minimal retries for fast tests."""
    return SupersetConfig(
        superset_base_url=BASE_URL,
        superset_username="admin",
        superset_password="admin",
        openai_api_key="test-key",
        max_retries=2,
    )


def _make_session(*, session_based: bool = False) -> AuthSession:
    """Create a valid AuthSession for testing."""
    return AuthSession(
        access_token="test-token",
        csrf_token="test-csrf",
        refresh_token="test-refresh",
        expires_at=time.time() + 3600,
        session_based=session_based,
    )


@pytest.fixture
def client(config):
    """Create a SupersetClient with mocked auth."""
    c = SupersetClient(config)
    c.auth = AsyncMock()
    c.auth.get_valid_session = AsyncMock(return_value=_make_session())
    c.auth.invalidate = AsyncMock()
    c.auth.session_cookies = {}
    return c


# =============================================================================
# Happy-path CRUD
# =============================================================================


class TestGetPostPutDelete:
    """Test successful GET/POST/PUT/DELETE requests."""

    @respx.mock
    async def test_get_success(self, client):
        route = respx.get(f"{API_URL}/chart/").respond(200, json={"result": [{"id": 1}]})

        result = await client.get("/chart/")

        assert route.called
        assert result == {"result": [{"id": 1}]}

    @respx.mock
    async def test_get_with_params(self, client):
        route = respx.get(f"{API_URL}/chart/").respond(200, json={"result": [], "count": 0})

        result = await client.get("/chart/", params={"q": "filter"})

        assert route.called
        request = route.calls[0].request
        assert b"q=filter" in request.url.raw_path
        assert result["count"] == 0

    @respx.mock
    async def test_post_success(self, client):
        route = respx.post(f"{API_URL}/chart/").respond(
            201, json={"id": 42, "result": {"slice_name": "New Chart"}}
        )

        result = await client.post("/chart/", json={"slice_name": "New Chart"})

        assert route.called
        assert result["id"] == 42

    @respx.mock
    async def test_put_success(self, client):
        route = respx.put(f"{API_URL}/chart/42").respond(
            200, json={"id": 42, "result": {"slice_name": "Updated"}}
        )

        result = await client.put("/chart/42", json={"slice_name": "Updated"})

        assert route.called
        assert result["result"]["slice_name"] == "Updated"

    @respx.mock
    async def test_delete_success(self, client):
        route = respx.delete(f"{API_URL}/chart/42").respond(200, json={"message": "Deleted"})

        result = await client.delete("/chart/42")

        assert route.called
        assert result["message"] == "Deleted"

    @respx.mock
    async def test_delete_204_no_content(self, client):
        respx.delete(f"{API_URL}/chart/42").respond(204)

        result = await client.delete("/chart/42")

        assert result == {}


# =============================================================================
# Auth headers
# =============================================================================


class TestAuthHeaders:
    """Test that correct auth headers are sent."""

    @respx.mock
    async def test_jwt_auth_sends_bearer_header(self, client):
        route = respx.get(f"{API_URL}/chart/").respond(200, json={})

        await client.get("/chart/")

        request = route.calls[0].request
        assert request.headers["authorization"] == "Bearer test-token"
        assert request.headers["x-csrftoken"] == "test-csrf"

    @respx.mock
    async def test_session_auth_omits_bearer_header(self, client):
        client.auth.get_valid_session = AsyncMock(return_value=_make_session(session_based=True))
        client.auth.session_cookies = {"session": "abc123"}
        route = respx.get(f"{API_URL}/chart/").respond(200, json={})

        await client.get("/chart/")

        request = route.calls[0].request
        assert "authorization" not in request.headers

    @respx.mock
    async def test_endpoint_without_leading_slash(self, client):
        route = respx.get(f"{API_URL}/chart/").respond(200, json={})

        await client.get("chart/")

        assert route.called


# =============================================================================
# 401 auto-refresh
# =============================================================================


class TestAutoRefreshOn401:
    """Test that 401 triggers auth invalidation and retry."""

    @respx.mock
    async def test_401_triggers_invalidation_and_retry(self, client):
        """First call returns 401, client invalidates auth, retries, second succeeds."""
        route = respx.get(f"{API_URL}/chart/").mock(
            side_effect=[
                httpx.Response(401, json={"message": "Token expired"}),
                httpx.Response(200, json={"result": "ok"}),
            ]
        )

        result = await client.get("/chart/")

        assert client.auth.invalidate.await_count == 1
        assert route.call_count == 2
        assert result == {"result": "ok"}

    @respx.mock
    async def test_401_on_retry_raises_authentication_error(self, client):
        """If the retry also returns 401, raise AuthenticationError."""
        respx.get(f"{API_URL}/chart/").mock(
            side_effect=[
                httpx.Response(401, json={"message": "Bad token"}),
                httpx.Response(401, json={"message": "Still bad"}),
            ]
        )

        with pytest.raises(AuthenticationError):
            await client.get("/chart/")


# =============================================================================
# Error classification
# =============================================================================


class TestErrorClassification:
    """Test that HTTP status codes are mapped to correct exception types."""

    @respx.mock
    async def test_404_raises_resource_not_found(self, client):
        respx.get(f"{API_URL}/chart/999").respond(404, json={"message": "Not found"})

        with pytest.raises(ResourceNotFoundError) as exc_info:
            await client.get("/chart/999")

        assert exc_info.value.resource_type == "chart"
        assert exc_info.value.resource_id == "999"

    @respx.mock
    async def test_403_raises_permission_denied(self, client):
        respx.get(f"{API_URL}/chart/").respond(403, json={"message": "Forbidden"})

        with pytest.raises(PermissionDeniedError):
            await client.get("/chart/")

    @respx.mock
    async def test_429_raises_rate_limit_error(self, client):
        respx.get(f"{API_URL}/chart/").mock(
            side_effect=[
                httpx.Response(
                    429,
                    json={"message": "Rate limited"},
                    headers={"Retry-After": "30"},
                ),
                httpx.Response(
                    429,
                    json={"message": "Rate limited"},
                    headers={"Retry-After": "30"},
                ),
            ]
        )

        with pytest.raises(RateLimitError) as exc_info:
            await client.get("/chart/")

        assert exc_info.value.retry_after == 30

    @respx.mock
    async def test_500_raises_server_error(self, client):
        respx.get(f"{API_URL}/chart/").respond(500, json={"message": "Internal error"})

        with pytest.raises(ServerError) as exc_info:
            await client.get("/chart/")

        assert exc_info.value.status_code == 500

    @respx.mock
    async def test_502_raises_server_error(self, client):
        respx.get(f"{API_URL}/chart/").respond(502, json={"message": "Bad gateway"})

        with pytest.raises(ServerError) as exc_info:
            await client.get("/chart/")

        assert exc_info.value.status_code == 502

    @respx.mock
    async def test_400_raises_validation_error(self, client):
        respx.post(f"{API_URL}/chart/").respond(400, json={"message": "Invalid payload"})

        with pytest.raises(ValidationError) as exc_info:
            await client.post("/chart/", json={"bad": "data"})

        assert exc_info.value.status_code == 400

    @respx.mock
    async def test_422_raises_validation_error(self, client):
        respx.post(f"{API_URL}/chart/").respond(422, json={"errors": ["field X is required"]})

        with pytest.raises(ValidationError) as exc_info:
            await client.post("/chart/", json={"incomplete": True})

        assert exc_info.value.status_code == 422

    @respx.mock
    async def test_unknown_error_raises_superset_api_error(self, client):
        respx.get(f"{API_URL}/chart/").respond(418, json={"message": "I'm a teapot"})

        with pytest.raises(SupersetAPIError) as exc_info:
            await client.get("/chart/")

        assert exc_info.value.status_code == 418


# =============================================================================
# _parse_response fallback
# =============================================================================


class TestParseResponse:
    """Test response parsing with JSON and non-JSON bodies."""

    @respx.mock
    async def test_json_response_is_parsed(self, client):
        respx.get(f"{API_URL}/chart/").respond(200, json={"result": "data"})

        result = await client.get("/chart/")

        assert result == {"result": "data"}

    @respx.mock
    async def test_non_json_response_returns_fallback(self, client):
        respx.get(f"{API_URL}/chart/").respond(
            200,
            content=b"<html>Not JSON</html>",
            headers={"content-type": "text/html"},
        )

        result = await client.get("/chart/")

        assert "raw_response" in result
        assert result["status_code"] == 200
        assert "Not JSON" in result["raw_response"]

    @respx.mock
    async def test_204_response_returns_empty_dict(self, client):
        respx.delete(f"{API_URL}/chart/1").respond(204)

        result = await client.delete("/chart/1")

        assert result == {}


# =============================================================================
# _extract_error_message heuristics
# =============================================================================


class TestExtractErrorMessage:
    """Test error message extraction from various Superset response formats."""

    def test_message_key(self, client):
        assert client._extract_error_message({"message": "Something went wrong"}) == (
            "Something went wrong"
        )

    def test_msg_key(self, client):
        assert client._extract_error_message({"msg": "Token expired"}) == "Token expired"

    def test_error_key(self, client):
        assert client._extract_error_message({"error": "Bad request"}) == "Bad request"

    def test_errors_list(self, client):
        result = client._extract_error_message({"errors": ["First error", "Second"]})
        assert result == "First error"

    def test_errors_dict(self, client):
        result = client._extract_error_message({"error": {"code": 42, "detail": "oops"}})
        assert "42" in result
        assert "oops" in result

    def test_string_body(self, client):
        assert client._extract_error_message("plain text error") == "plain text error"

    def test_empty_string_body(self, client):
        assert client._extract_error_message("") is None

    def test_empty_dict(self, client):
        assert client._extract_error_message({}) is None

    def test_empty_errors_list(self, client):
        assert client._extract_error_message({"errors": []}) is None


# =============================================================================
# Retry on RateLimitError and httpx.TimeoutException
# =============================================================================


class TestRetryBehavior:
    """Test tenacity retry logic on transient errors."""

    @respx.mock
    async def test_retry_on_rate_limit_then_success(self, client):
        """RateLimitError triggers retry; second attempt succeeds."""
        route = respx.get(f"{API_URL}/chart/").mock(
            side_effect=[
                httpx.Response(429, json={"message": "Rate limited"}),
                httpx.Response(200, json={"result": "ok"}),
            ]
        )

        result = await client.get("/chart/")

        assert route.call_count == 2
        assert result == {"result": "ok"}

    @respx.mock
    async def test_retry_on_timeout_then_success(self, client):
        """httpx.TimeoutException triggers retry; second attempt succeeds."""
        call_count = 0

        def side_effect(request):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise httpx.ReadTimeout("timed out")
            return httpx.Response(200, json={"result": "recovered"})

        respx.get(f"{API_URL}/chart/").mock(side_effect=side_effect)

        result = await client.get("/chart/")

        assert call_count == 2
        assert result == {"result": "recovered"}

    @respx.mock
    async def test_exhausted_retries_on_rate_limit(self, client):
        """All retries exhausted on persistent 429 raises RateLimitError."""
        respx.get(f"{API_URL}/chart/").mock(
            side_effect=[
                httpx.Response(429, json={"message": "Rate limited"}),
                httpx.Response(429, json={"message": "Rate limited"}),
            ]
        )

        with pytest.raises(RateLimitError):
            await client.get("/chart/")

    @respx.mock
    async def test_exhausted_retries_on_timeout(self, client):
        """All retries exhausted on persistent timeout raises TimeoutException."""
        respx.get(f"{API_URL}/chart/").mock(side_effect=httpx.ReadTimeout("timed out"))

        with pytest.raises(httpx.TimeoutException):
            await client.get("/chart/")


# =============================================================================
# Connection errors
# =============================================================================


class TestConnectionErrors:
    """Test that httpx.RequestError is wrapped in SupersetAPIError."""

    @respx.mock
    async def test_request_error_raises_superset_api_error(self, client):
        respx.get(f"{API_URL}/chart/").mock(side_effect=httpx.ConnectError("Connection refused"))

        with pytest.raises(SupersetAPIError, match="Request failed"):
            await client.get("/chart/")


# =============================================================================
# Context manager
# =============================================================================


class TestContextManager:
    """Test async context manager protocol."""

    async def test_async_context_manager(self, config):
        with patch.object(SupersetClient, "close", new_callable=AsyncMock) as mock_close:
            async with SupersetClient(config) as client:
                assert isinstance(client, SupersetClient)

            mock_close.assert_awaited_once()
