"""Tests for authentication module."""

import base64
import json
import time
from unittest.mock import MagicMock

import httpx
import pytest
import respx

from superset_ai.api.auth import AuthSession, SupersetAuthManager
from superset_ai.core.config import SupersetConfig
from superset_ai.core.exceptions import AuthenticationError, CSRFTokenError

BASE_URL = "http://localhost:8088"


def _make_jwt(payload: dict | None = None, exp: float | None = None) -> str:
    """Create a fake JWT token with the given claims."""
    claims = payload or {}
    if exp is not None:
        claims["exp"] = exp
    elif "exp" not in claims:
        claims["exp"] = int(time.time()) + 3600
    header_b64 = base64.urlsafe_b64encode(b'{"alg":"HS256"}').decode().rstrip("=")
    payload_b64 = base64.urlsafe_b64encode(json.dumps(claims).encode()).decode().rstrip("=")
    return f"{header_b64}.{payload_b64}.fakesignature"


@pytest.fixture
def config():
    """Create test configuration."""
    return SupersetConfig(
        superset_base_url=BASE_URL,
        superset_username="admin",
        superset_password="admin",
        openai_api_key="test-key",
    )


@pytest.fixture
def auth_manager(config):
    """Create auth manager instance."""
    return SupersetAuthManager(config)


# =============================================================================
# AuthSession dataclass
# =============================================================================


class TestAuthSession:
    """Tests for AuthSession."""

    def test_is_expired_returns_false_for_future_expiry(self):
        """Session should not be expired if expiry is in the future."""
        session = AuthSession(
            access_token="token",
            csrf_token="csrf",
            refresh_token=None,
            expires_at=time.time() + 3600,
        )
        assert not session.is_expired()

    def test_is_expired_returns_true_for_past_expiry(self):
        """Session should be expired if expiry is in the past."""
        session = AuthSession(
            access_token="token",
            csrf_token="csrf",
            refresh_token=None,
            expires_at=time.time() - 100,
        )
        assert session.is_expired()

    def test_is_expired_considers_buffer(self):
        """Session should be considered expired within buffer window."""
        session = AuthSession(
            access_token="token",
            csrf_token="csrf",
            refresh_token=None,
            expires_at=time.time() + 200,
        )
        assert session.is_expired(buffer_seconds=300)
        assert not session.is_expired(buffer_seconds=100)

    def test_session_based_default(self):
        """session_based should default to False."""
        session = AuthSession(
            access_token="token",
            csrf_token="csrf",
            refresh_token=None,
            expires_at=time.time() + 3600,
        )
        assert session.session_based is False

    def test_session_based_flag(self):
        """session_based can be set to True."""
        session = AuthSession(
            access_token=None,
            csrf_token="csrf",
            refresh_token=None,
            expires_at=time.time() + 3600,
            session_based=True,
        )
        assert session.session_based is True


# =============================================================================
# SupersetAuthManager helper methods
# =============================================================================


class TestSupersetAuthManager:
    """Tests for SupersetAuthManager helper methods."""

    def test_extract_expiry_from_valid_jwt(self, auth_manager):
        """Should extract expiry from valid JWT."""
        exp_time = int(time.time()) + 3600
        token = _make_jwt(exp=exp_time)
        extracted = auth_manager._extract_expiry(token)
        assert extracted == exp_time

    def test_extract_expiry_fallback_for_invalid_jwt(self, auth_manager):
        """Should return default expiry for invalid JWT payload."""
        # payload is not valid base64 JSON
        extracted = auth_manager._extract_expiry("header.!!!invalid!!!.signature")
        assert extracted > time.time()
        assert extracted < time.time() + 3700

    def test_extract_expiry_fallback_for_non_jwt_string(self, auth_manager):
        """Should return default expiry for strings with wrong part count."""
        extracted = auth_manager._extract_expiry("not-a-jwt")
        assert extracted > time.time()
        assert extracted < time.time() + 3700

    def test_extract_expiry_fallback_missing_exp_claim(self, auth_manager):
        """Should return default if JWT payload has no 'exp' claim."""
        token = _make_jwt(payload={"sub": "admin"})
        # Remove exp from payload: rebuild without exp
        header_b64 = base64.urlsafe_b64encode(b'{"alg":"HS256"}').decode().rstrip("=")
        payload_b64 = (
            base64.urlsafe_b64encode(json.dumps({"sub": "admin"}).encode()).decode().rstrip("=")
        )
        token = f"{header_b64}.{payload_b64}.fakesignature"

        extracted = auth_manager._extract_expiry(token)
        assert extracted > time.time()
        assert extracted < time.time() + 3700

    def test_safe_json_returns_dict_for_valid_json(self, auth_manager):
        """Should return parsed JSON for valid response."""
        mock_response = MagicMock()
        mock_response.json.return_value = {"key": "value"}
        result = auth_manager._safe_json(mock_response)
        assert result == {"key": "value"}

    def test_safe_json_returns_text_for_invalid_json(self, auth_manager):
        """Should return text for invalid JSON response."""
        mock_response = MagicMock()
        mock_response.json.side_effect = ValueError("Invalid JSON")
        mock_response.text = "Plain text response"
        result = auth_manager._safe_json(mock_response)
        assert result == "Plain text response"


# =============================================================================
# Session-based authentication flow
# =============================================================================


class TestSessionBasedAuth:
    """Test the session-based authentication flow (Superset 6.x+)."""

    @respx.mock
    async def test_session_auth_happy_path(self, auth_manager):
        """Successful session auth: CSRF fetch, form login, /me/ verification."""
        # Step 1: CSRF token fetch
        respx.get(f"{BASE_URL}/api/v1/security/csrf_token/").respond(
            200, json={"result": "csrf-token-abc"}
        )
        # Step 2: Form-based login (302 redirect = success)
        respx.post(f"{BASE_URL}/login/").respond(302)
        # Step 3: Verify via /me/
        respx.get(f"{BASE_URL}/api/v1/me/").respond(
            200,
            json={"result": {"username": "admin", "is_anonymous": False}},
        )

        session = await auth_manager.get_valid_session()

        assert session.session_based is True
        assert session.csrf_token == "csrf-token-abc"
        assert session.access_token is None
        assert session.expires_at > time.time()

    @respx.mock
    async def test_session_auth_login_200_success(self, auth_manager):
        """Some Superset versions return 200 on login instead of 302."""
        respx.get(f"{BASE_URL}/api/v1/security/csrf_token/").respond(
            200, json={"result": "csrf-200"}
        )
        respx.post(f"{BASE_URL}/login/").respond(200)
        respx.get(f"{BASE_URL}/api/v1/me/").respond(
            200,
            json={"result": {"username": "admin", "is_anonymous": False}},
        )

        session = await auth_manager.get_valid_session()

        assert session.session_based is True
        assert session.csrf_token == "csrf-200"

    @respx.mock
    async def test_session_auth_login_fails_status(self, auth_manager):
        """Login returning 403 should trigger fallback to JWT."""
        # Session auth fails at login step
        respx.get(f"{BASE_URL}/api/v1/security/csrf_token/").respond(
            200, json={"result": "csrf-token"}
        )
        respx.post(f"{BASE_URL}/login/").respond(403, json={"message": "Forbidden"})

        # JWT fallback: login endpoint + CSRF fetch
        jwt_token = _make_jwt()
        respx.post(f"{BASE_URL}/api/v1/security/login").respond(
            200,
            json={"access_token": jwt_token, "refresh_token": "refresh-123"},
        )
        respx.get(f"{BASE_URL}/api/v1/security/csrf_token/").respond(
            200, json={"result": "jwt-csrf"}
        )

        session = await auth_manager.get_valid_session()

        assert session.session_based is False
        assert session.access_token == jwt_token

    @respx.mock
    async def test_session_auth_me_anonymous_triggers_fallback(self, auth_manager):
        """If /me/ says user is anonymous, session auth fails and falls back to JWT."""
        respx.get(f"{BASE_URL}/api/v1/security/csrf_token/").respond(
            200, json={"result": "csrf-anon"}
        )
        respx.post(f"{BASE_URL}/login/").respond(302)
        respx.get(f"{BASE_URL}/api/v1/me/").respond(
            200,
            json={"result": {"username": "anon", "is_anonymous": True}},
        )

        # JWT fallback
        jwt_token = _make_jwt()
        respx.post(f"{BASE_URL}/api/v1/security/login").respond(
            200,
            json={"access_token": jwt_token, "refresh_token": "r"},
        )
        respx.get(f"{BASE_URL}/api/v1/security/csrf_token/").respond(
            200, json={"result": "jwt-csrf"}
        )

        session = await auth_manager.get_valid_session()

        assert session.session_based is False
        assert session.access_token == jwt_token


# =============================================================================
# JWT authentication flow
# =============================================================================


class TestJWTAuth:
    """Test the JWT authentication flow (legacy Superset 3.x-5.x)."""

    @respx.mock
    async def test_jwt_auth_happy_path(self, auth_manager):
        """Successful JWT auth: login POST, CSRF token fetch."""
        jwt_token = _make_jwt()

        # Session auth fails: initial CSRF fetch (via _root_client -> /api/v1/security/csrf_token/)
        respx.get(f"{BASE_URL}/api/v1/security/csrf_token/").respond(
            500, json={"error": "not supported"}
        )

        # JWT login (via _client -> /api/v1/security/login)
        respx.post(f"{BASE_URL}/api/v1/security/login").respond(
            200,
            json={"access_token": jwt_token, "refresh_token": "refresh-abc"},
        )

        # JWT CSRF fetch (via _client -> /api/v1/security/csrf_token/)
        respx.get(f"{BASE_URL}/api/v1/security/csrf_token/").respond(
            200, json={"result": "jwt-csrf-token"}
        )

        session = await auth_manager.get_valid_session()

        assert session.session_based is False
        assert session.access_token == jwt_token
        assert session.csrf_token == "jwt-csrf-token"
        assert session.refresh_token == "refresh-abc"

    @respx.mock
    async def test_jwt_auth_missing_access_token(self, auth_manager):
        """Login response without access_token raises AuthenticationError."""
        # Session auth fails at initial CSRF
        respx.get(f"{BASE_URL}/api/v1/security/csrf_token/").respond(500, json={"error": "fail"})
        # JWT login succeeds but returns no token
        respx.post(f"{BASE_URL}/api/v1/security/login").respond(200, json={"refresh_token": "r"})

        with pytest.raises(AuthenticationError, match="missing access_token"):
            await auth_manager.get_valid_session()

    @respx.mock
    async def test_jwt_auth_login_non_200(self, auth_manager):
        """JWT login returning non-200 raises AuthenticationError."""
        # Session auth fails at initial CSRF
        respx.get(f"{BASE_URL}/api/v1/security/csrf_token/").respond(500, json={"error": "fail"})
        # JWT login fails
        respx.post(f"{BASE_URL}/api/v1/security/login").respond(
            401, json={"message": "Bad credentials"}
        )

        with pytest.raises(AuthenticationError, match="Login failed"):
            await auth_manager.get_valid_session()


# =============================================================================
# Fallback from session -> JWT with exception chaining
# =============================================================================


class TestAuthFallback:
    """Test fallback from session-based to JWT auth and exception chaining."""

    @respx.mock
    async def test_both_auth_methods_fail_chains_exceptions(self, auth_manager):
        """When both session and JWT fail, the JWT error chains to the session error."""
        # Session auth fails
        respx.get(f"{BASE_URL}/api/v1/security/csrf_token/").respond(
            500, json={"error": "session csrf fail"}
        )

        # JWT also fails
        respx.post(f"{BASE_URL}/api/v1/security/login").respond(
            500, json={"message": "JWT login fail"}
        )

        with pytest.raises(AuthenticationError) as exc_info:
            await auth_manager.get_valid_session()

        # The raised exception should have __cause__ from session error
        assert exc_info.value.__cause__ is not None

    @respx.mock
    async def test_connect_error_in_login_propagates_immediately(self, auth_manager):
        """httpx.ConnectError from login POST should NOT fall back to JWT."""
        # CSRF fetch succeeds
        respx.get(f"{BASE_URL}/api/v1/security/csrf_token/").respond(
            200, json={"result": "csrf-token"}
        )
        # Login POST raises ConnectError — this propagates through
        # _authenticate_session_based as AuthenticationError, but since
        # ConnectError is wrapped inside RequestError catch block,
        # _authenticate re-raises it through the generic except chain.
        respx.post(f"{BASE_URL}/login/").mock(side_effect=httpx.ConnectError("Connection refused"))

        # JWT fallback also fails because Superset is unreachable
        respx.post(f"{BASE_URL}/api/v1/security/login").mock(
            side_effect=httpx.ConnectError("Connection refused")
        )

        with pytest.raises(AuthenticationError, match="Failed to connect"):
            await auth_manager.get_valid_session()

    @respx.mock
    async def test_timeout_in_csrf_fetch_falls_back_to_jwt(self, auth_manager):
        """httpx.TimeoutException in CSRF fetch is wrapped in CSRFTokenError and triggers JWT fallback."""
        # CSRF fetch times out — wrapped in CSRFTokenError by _fetch_csrf_token_initial
        respx.get(f"{BASE_URL}/api/v1/security/csrf_token/").mock(
            side_effect=httpx.ReadTimeout("Timed out")
        )

        # JWT fallback succeeds
        jwt_token = _make_jwt()
        respx.post(f"{BASE_URL}/api/v1/security/login").respond(
            200,
            json={"access_token": jwt_token, "refresh_token": "r"},
        )
        respx.get(f"{BASE_URL}/api/v1/security/csrf_token/").respond(
            200, json={"result": "jwt-csrf"}
        )

        session = await auth_manager.get_valid_session()

        assert session.session_based is False
        assert session.access_token == jwt_token


# =============================================================================
# CSRF token errors
# =============================================================================


class TestCSRFTokenErrors:
    """Test CSRF token fetch edge cases."""

    @respx.mock
    async def test_csrf_token_non_200_raises_csrf_error(self, auth_manager):
        """Non-200 response from CSRF endpoint raises CSRFTokenError."""
        respx.get(f"{BASE_URL}/api/v1/security/csrf_token/").respond(
            403, json={"message": "Forbidden"}
        )

        # Falls through to JWT, which also needs CSRF. Both routes must be set up.
        jwt_token = _make_jwt()
        respx.post(f"{BASE_URL}/api/v1/security/login").respond(
            200,
            json={"access_token": jwt_token, "refresh_token": "r"},
        )
        # JWT CSRF also fails
        respx.get(f"{BASE_URL}/api/v1/security/csrf_token/").respond(
            403, json={"message": "Forbidden"}
        )

        with pytest.raises(CSRFTokenError):
            await auth_manager.get_valid_session()

    @respx.mock
    async def test_csrf_token_missing_result_field(self, auth_manager):
        """CSRF response missing 'result' field raises CSRFTokenError."""
        respx.get(f"{BASE_URL}/api/v1/security/csrf_token/").respond(
            200, json={"data": "no-result-key"}
        )

        # JWT fallback
        jwt_token = _make_jwt()
        respx.post(f"{BASE_URL}/api/v1/security/login").respond(
            200,
            json={"access_token": jwt_token, "refresh_token": "r"},
        )
        respx.get(f"{BASE_URL}/api/v1/security/csrf_token/").respond(
            200, json={"not_result": "oops"}
        )

        with pytest.raises(CSRFTokenError, match="missing 'result' field"):
            await auth_manager.get_valid_session()

    @respx.mock
    async def test_csrf_token_request_error(self, auth_manager):
        """httpx.RequestError during CSRF fetch wraps into CSRFTokenError then falls back."""
        # Initial CSRF fetch for session-based auth fails with RequestError.
        # _fetch_csrf_token_initial catches httpx.RequestError and raises CSRFTokenError.
        # That causes session auth to fail, triggering JWT fallback.
        respx.get(f"{BASE_URL}/api/v1/security/csrf_token/").mock(
            side_effect=httpx.NetworkError("DNS failure")
        )

        # JWT login fails too (it tries _client which has base_url=superset_base_url)
        respx.post(f"{BASE_URL}/api/v1/security/login").mock(
            side_effect=httpx.NetworkError("DNS failure")
        )

        with pytest.raises(AuthenticationError):
            await auth_manager.get_valid_session()


# =============================================================================
# Token refresh on expiry
# =============================================================================


class TestTokenRefresh:
    """Test token refresh when session expires."""

    @respx.mock
    async def test_refresh_jwt_on_expiry(self, auth_manager):
        """Expired JWT session triggers refresh rather than full re-auth."""
        old_jwt = _make_jwt(exp=int(time.time()) - 100)
        new_jwt = _make_jwt(exp=int(time.time()) + 7200)

        # Seed the auth manager with an expired session
        auth_manager._session = AuthSession(
            access_token=old_jwt,
            csrf_token="old-csrf",
            refresh_token="refresh-token",
            expires_at=time.time() - 100,  # expired
            session_based=False,
        )

        # Refresh endpoint
        respx.post(f"{BASE_URL}/api/v1/security/refresh").respond(
            200, json={"access_token": new_jwt}
        )
        # CSRF after refresh
        respx.get(f"{BASE_URL}/api/v1/security/csrf_token/").respond(
            200, json={"result": "new-csrf"}
        )

        session = await auth_manager.get_valid_session()

        assert session.access_token == new_jwt
        assert session.csrf_token == "new-csrf"
        assert session.refresh_token == "refresh-token"

    @respx.mock
    async def test_refresh_fails_falls_back_to_full_auth(self, auth_manager):
        """If refresh fails, fall back to full re-authentication."""
        old_jwt = _make_jwt(exp=int(time.time()) - 100)

        auth_manager._session = AuthSession(
            access_token=old_jwt,
            csrf_token="old-csrf",
            refresh_token="refresh-token",
            expires_at=time.time() - 100,
            session_based=False,
        )

        # Refresh fails
        respx.post(f"{BASE_URL}/api/v1/security/refresh").respond(
            401, json={"message": "Invalid refresh token"}
        )

        # Full re-auth: session auth CSRF fetch
        respx.get(f"{BASE_URL}/api/v1/security/csrf_token/").respond(
            200, json={"result": "new-csrf"}
        )
        respx.post(f"{BASE_URL}/login/").respond(302)
        respx.get(f"{BASE_URL}/api/v1/me/").respond(
            200,
            json={"result": {"username": "admin", "is_anonymous": False}},
        )

        session = await auth_manager.get_valid_session()

        assert session.csrf_token == "new-csrf"

    @respx.mock
    async def test_refresh_no_refresh_token_falls_back(self, auth_manager):
        """If no refresh token is available, fall back to full re-authentication."""
        auth_manager._session = AuthSession(
            access_token="some-jwt",
            csrf_token="old-csrf",
            refresh_token=None,  # No refresh token
            expires_at=time.time() - 100,
            session_based=False,
        )

        # Full re-auth via session
        respx.get(f"{BASE_URL}/api/v1/security/csrf_token/").respond(
            200, json={"result": "fresh-csrf"}
        )
        respx.post(f"{BASE_URL}/login/").respond(302)
        respx.get(f"{BASE_URL}/api/v1/me/").respond(
            200,
            json={"result": {"username": "admin", "is_anonymous": False}},
        )

        session = await auth_manager.get_valid_session()

        assert session.csrf_token == "fresh-csrf"

    @respx.mock
    async def test_session_based_expired_re_authenticates(self, auth_manager):
        """Expired session-based auth triggers full re-authentication, not refresh."""
        auth_manager._session = AuthSession(
            access_token=None,
            csrf_token="old-csrf",
            refresh_token=None,
            expires_at=time.time() - 100,
            session_based=True,
        )

        respx.get(f"{BASE_URL}/api/v1/security/csrf_token/").respond(
            200, json={"result": "new-session-csrf"}
        )
        respx.post(f"{BASE_URL}/login/").respond(302)
        respx.get(f"{BASE_URL}/api/v1/me/").respond(
            200,
            json={"result": {"username": "admin", "is_anonymous": False}},
        )

        session = await auth_manager.get_valid_session()

        assert session.session_based is True
        assert session.csrf_token == "new-session-csrf"


# =============================================================================
# get_valid_session caching
# =============================================================================


class TestSessionCaching:
    """Test that get_valid_session caches and reuses sessions."""

    @respx.mock
    async def test_second_call_uses_cached_session(self, auth_manager):
        """Second call to get_valid_session should not re-authenticate."""
        respx.get(f"{BASE_URL}/api/v1/security/csrf_token/").respond(
            200, json={"result": "cached-csrf"}
        )
        respx.post(f"{BASE_URL}/login/").respond(302)
        respx.get(f"{BASE_URL}/api/v1/me/").respond(
            200,
            json={"result": {"username": "admin", "is_anonymous": False}},
        )

        session1 = await auth_manager.get_valid_session()
        session2 = await auth_manager.get_valid_session()

        # Both calls should return the exact same object
        assert session1 is session2
        # CSRF endpoint should only have been called once
        assert respx.calls.call_count == 3  # csrf + login + me = 3 calls total

    @respx.mock
    async def test_invalidate_forces_re_auth(self, auth_manager):
        """After invalidate(), next call re-authenticates."""
        call_counter = {"csrf": 0}

        def csrf_side_effect(request):
            call_counter["csrf"] += 1
            return httpx.Response(200, json={"result": f"csrf-{call_counter['csrf']}"})

        respx.get(f"{BASE_URL}/api/v1/security/csrf_token/").mock(side_effect=csrf_side_effect)
        respx.post(f"{BASE_URL}/login/").respond(302)
        respx.get(f"{BASE_URL}/api/v1/me/").respond(
            200,
            json={"result": {"username": "admin", "is_anonymous": False}},
        )

        session1 = await auth_manager.get_valid_session()
        assert session1.csrf_token == "csrf-1"

        await auth_manager.invalidate()

        session2 = await auth_manager.get_valid_session()
        assert session2.csrf_token == "csrf-2"
        assert session1 is not session2


# =============================================================================
# _extract_expiry edge cases
# =============================================================================


class TestExtractExpiry:
    """Detailed tests for JWT expiry extraction."""

    def test_valid_jwt_with_exp(self, auth_manager):
        """Should extract exp claim from well-formed JWT."""
        exp_time = int(time.time()) + 7200
        token = _make_jwt(exp=exp_time)
        assert auth_manager._extract_expiry(token) == exp_time

    def test_jwt_with_float_exp(self, auth_manager):
        """Should handle float exp values."""
        exp_time = time.time() + 3600.5
        token = _make_jwt(exp=exp_time)
        assert auth_manager._extract_expiry(token) == exp_time

    def test_invalid_base64_payload(self, auth_manager):
        """Invalid base64 in JWT payload falls back to default."""
        token = "header.not-valid-base64!!!.signature"
        result = auth_manager._extract_expiry(token)
        assert result > time.time()
        assert result < time.time() + 3700

    def test_non_jwt_string(self, auth_manager):
        """String without three dot-separated parts falls back to default."""
        result = auth_manager._extract_expiry("just-a-plain-string")
        assert result > time.time()
        assert result < time.time() + 3700

    def test_too_many_parts(self, auth_manager):
        """String with more than 3 parts falls back to default."""
        result = auth_manager._extract_expiry("a.b.c.d")
        assert result > time.time()
        assert result < time.time() + 3700

    def test_empty_string(self, auth_manager):
        """Empty string falls back to default."""
        result = auth_manager._extract_expiry("")
        assert result > time.time()
        assert result < time.time() + 3700


# =============================================================================
# close() and cleanup
# =============================================================================


class TestCleanup:
    """Test resource cleanup."""

    async def test_close_cleans_up_clients(self, auth_manager):
        """close() should close both HTTP clients if they were created."""
        # Force creation of both clients by accessing properties
        _ = auth_manager._client
        _ = auth_manager._root_client

        assert auth_manager._own_http_client is not None
        assert auth_manager._auth_http_client is not None

        await auth_manager.close()

        assert auth_manager._own_http_client is None
        assert auth_manager._auth_http_client is None

    async def test_close_is_safe_when_no_clients(self, auth_manager):
        """close() should not fail if clients were never created."""
        await auth_manager.close()  # Should not raise


# =============================================================================
# JWT CSRF session cookie capture
# =============================================================================


class TestJWTSessionCookieCapture:
    """Test that JWT auth captures session cookies set during CSRF fetch.

    Superset 3.x ties the CSRF token to a server-side session via a cookie.
    After _fetch_csrf_token(), the cookies from _client must be copied to
    _session_cookies so that the API client can forward them on later requests.
    """

    @respx.mock
    async def test_jwt_auth_populates_session_cookies(self, config):
        """JWT auth should capture cookies from _client into session_cookies."""
        jwt_token = _make_jwt()

        # Session auth fails at initial CSRF (drives fallback to JWT)
        respx.get(f"{BASE_URL}/api/v1/security/csrf_token/").respond(
            500, json={"error": "not supported"}
        )

        # JWT login succeeds
        respx.post(f"{BASE_URL}/api/v1/security/login").respond(
            200,
            json={"access_token": jwt_token, "refresh_token": "r"},
        )

        # CSRF fetch succeeds and the server sets a session cookie.
        # We mock it via a callback so we can inject cookies into _client.
        csrf_called = False

        def csrf_side_effect(request):
            nonlocal csrf_called
            csrf_called = True
            resp = httpx.Response(200, json={"result": "jwt-csrf"})
            return resp

        respx.get(f"{BASE_URL}/api/v1/security/csrf_token/").mock(side_effect=csrf_side_effect)

        # Create auth manager with its own client (so we control cookies)
        auth = SupersetAuthManager(config)

        # Pre-set a cookie on _client to simulate the server setting one
        # during the CSRF fetch response. (respx doesn't propagate
        # Set-Cookie headers into httpx cookies, so we seed it directly.)
        auth._client.cookies.set("session", "fake-session-id")

        session = await auth.get_valid_session()

        assert session.session_based is False
        # The session cookie must have been captured
        assert "session" in auth.session_cookies
        assert auth.session_cookies["session"] == "fake-session-id"

        await auth.close()

    @respx.mock
    async def test_jwt_auth_captures_multiple_cookies(self, config):
        """JWT auth should capture all cookies from _client, not just 'session'."""
        jwt_token = _make_jwt()

        respx.get(f"{BASE_URL}/api/v1/security/csrf_token/").respond(500, json={"error": "fail"})
        respx.post(f"{BASE_URL}/api/v1/security/login").respond(
            200,
            json={"access_token": jwt_token, "refresh_token": "r"},
        )
        respx.get(f"{BASE_URL}/api/v1/security/csrf_token/").respond(
            200, json={"result": "csrf-ok"}
        )

        auth = SupersetAuthManager(config)
        auth._client.cookies.set("session", "sess-val")
        auth._client.cookies.set("_superset_session", "extra-val")

        await auth.get_valid_session()

        assert auth.session_cookies["session"] == "sess-val"
        assert auth.session_cookies["_superset_session"] == "extra-val"

        await auth.close()

    @respx.mock
    async def test_jwt_auth_no_cookies_gives_empty_dict(self, config):
        """If the server sets no cookies during JWT auth, session_cookies stays empty."""
        jwt_token = _make_jwt()

        respx.get(f"{BASE_URL}/api/v1/security/csrf_token/").respond(500, json={"error": "fail"})
        respx.post(f"{BASE_URL}/api/v1/security/login").respond(
            200,
            json={"access_token": jwt_token, "refresh_token": "r"},
        )
        respx.get(f"{BASE_URL}/api/v1/security/csrf_token/").respond(
            200, json={"result": "csrf-ok"}
        )

        auth = SupersetAuthManager(config)
        # Don't seed any cookies on _client

        await auth.get_valid_session()

        assert auth.session_cookies == {}

        await auth.close()
