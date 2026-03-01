"""Authentication manager for Superset API."""

import asyncio
import logging
import time
from dataclasses import dataclass
from typing import TYPE_CHECKING, Callable

import httpx

from superset_ai.core.exceptions import AuthenticationError, CSRFTokenError

if TYPE_CHECKING:
    from superset_ai.core.config import SupersetConfig

logger = logging.getLogger(__name__)


@dataclass
class AuthSession:
    """Holds authentication session data."""

    access_token: str | None  # May be None for session-based auth
    csrf_token: str
    refresh_token: str | None
    expires_at: float  # Unix timestamp
    session_based: bool = False  # True if using session cookies instead of JWT

    def is_expired(self, buffer_seconds: int = 300) -> bool:
        """Check if token is expired or about to expire."""
        return time.time() >= (self.expires_at - buffer_seconds)


class SupersetAuthManager:
    """
    Manages Superset authentication including:
    - Session-based authentication (Superset 6.x+)
    - JWT authentication (legacy fallback)
    - CSRF token retrieval for mutating operations
    - Token refresh when expired
    
    Superset 6.x Authentication Flow (Session-based):
    1. GET /api/v1/security/csrf_token/ → get CSRF token (establishes session cookie)
    2. POST /login/ → form-based login with CSRF token
    3. Use session cookies for all subsequent requests
    
    Legacy JWT Flow (Superset 3.x-5.x):
    1. POST /api/v1/security/login → get access_token (JWT)
    2. GET /api/v1/security/csrf_token/ → get CSRF token
    3. Include both in subsequent requests:
       - Authorization: Bearer {access_token}
       - X-CSRFToken: {csrf_token}
    """

    def __init__(
        self,
        config: "SupersetConfig",
        client_getter: Callable[[], httpx.AsyncClient] | None = None,
    ) -> None:
        self.config = config
        self._session: AuthSession | None = None
        self._lock = asyncio.Lock()
        self._client_getter = client_getter
        self._own_http_client: httpx.AsyncClient | None = None
        # Separate client for auth operations using root base URL (not /api/v1)
        self._auth_http_client: httpx.AsyncClient | None = None
        # Store session cookies for session-based auth
        self._session_cookies: dict[str, str] = {}

    @property
    def _client(self) -> httpx.AsyncClient:
        """
        Get HTTP client for API requests.
        
        Uses shared client if provided (for cookie persistence), otherwise
        creates own client.
        """
        if self._client_getter is not None:
            return self._client_getter()
        
        # Fallback to own client
        if self._own_http_client is None:
            self._own_http_client = httpx.AsyncClient(
                base_url=self.config.superset_base_url,
                timeout=self.config.request_timeout,
            )
        return self._own_http_client

    @property
    def _root_client(self) -> httpx.AsyncClient:
        """
        Get HTTP client for root-level requests (login, csrf token).
        
        Uses the base URL without /api/v1 prefix.
        """
        if self._auth_http_client is None:
            self._auth_http_client = httpx.AsyncClient(
                base_url=self.config.superset_base_url,
                timeout=self.config.request_timeout,
            )
        return self._auth_http_client

    async def close(self) -> None:
        """Close the HTTP clients (only if we own them)."""
        if self._own_http_client is not None:
            await self._own_http_client.aclose()
            self._own_http_client = None
        if self._auth_http_client is not None:
            await self._auth_http_client.aclose()
            self._auth_http_client = None

    @property
    def session_cookies(self) -> dict[str, str]:
        """Get the session cookies for session-based auth."""
        return self._session_cookies

    async def get_valid_session(self) -> AuthSession:
        """
        Get a valid authentication session.
        
        Will authenticate if no session exists, or refresh if expired.
        Thread-safe via asyncio lock.
        """
        async with self._lock:
            if self._session is None:
                self._session = await self._authenticate()
            elif self._session.is_expired():
                try:
                    if self._session.session_based:
                        # For session-based auth, re-login
                        self._session = await self._authenticate()
                    else:
                        self._session = await self._refresh_token()
                except Exception:
                    # If refresh fails, try full re-authentication
                    self._session = await self._authenticate()
            return self._session

    async def invalidate(self) -> None:
        """Invalidate the current session, forcing re-authentication."""
        async with self._lock:
            self._session = None

    async def _authenticate(self) -> AuthSession:
        """
        Perform authentication to Superset.
        
        Tries session-based auth first (Superset 6.x), falls back to JWT (legacy).
        """
        try:
            # Try session-based authentication first (Superset 6.x+)
            return await self._authenticate_session_based()
        except Exception as e:
            logger.debug("Session-based auth failed, trying JWT: %s", e)
            # Fall back to JWT authentication
            return await self._authenticate_jwt()

    async def _authenticate_session_based(self) -> AuthSession:
        """
        Perform session-based authentication (Superset 6.x+).
        
        Steps:
        1. GET CSRF token (this creates a session cookie)
        2. POST to /login/ with form data (like browser would)
        3. Verify login succeeded via /api/v1/me/
        """
        # Step 1: Get CSRF token (this sets the session cookie)
        csrf_token = await self._fetch_csrf_token_initial()
        
        # Step 2: Login via form POST using the ROOT client (not API client)
        login_url = "/login/"
        
        login_data = {
            "username": self.config.superset_username,
            "password": self.config.superset_password.get_secret_value(),
            "csrf_token": csrf_token,
        }
        
        try:
            # Use follow_redirects=False to check if login succeeded (302 = success)
            response = await self._root_client.post(
                login_url,
                data=login_data,  # Form data, not JSON
                headers={
                    "Content-Type": "application/x-www-form-urlencoded",
                    "X-CSRFToken": csrf_token,
                },
                follow_redirects=False,
            )
        except httpx.RequestError as e:
            raise AuthenticationError(
                f"Failed to connect to Superset: {e}",
                details={"url": self.config.superset_base_url},
            ) from e

        # 302 redirect typically means successful login
        if response.status_code not in (200, 302):
            error_body = self._safe_json(response)
            raise AuthenticationError(
                f"Session login failed with status {response.status_code}",
                details={
                    "status_code": response.status_code,
                    "response": error_body,
                },
            )

        # Store session cookies from the root client for later use
        for name, value in self._root_client.cookies.items():
            self._session_cookies[name] = value
        
        # Step 3: Verify login by checking /api/v1/me/ using the root client
        me_response = await self._root_client.get(
            "/api/v1/me/",
            headers={"X-CSRFToken": csrf_token},
        )
        
        if me_response.status_code == 200:
            me_data = me_response.json()
            result = me_data.get("result", {})
            if not result.get("is_anonymous", True):
                logger.info("Session-based auth successful for user: %s", result.get('username'))
                # Update cookies again after verification
                for name, value in self._root_client.cookies.items():
                    self._session_cookies[name] = value
                return AuthSession(
                    access_token=None,  # Not using JWT
                    csrf_token=csrf_token,
                    refresh_token=None,
                    expires_at=time.time() + 86400,  # Session typically lasts 24 hours
                    session_based=True,
                )
        
        raise AuthenticationError(
            "Session login succeeded but user verification failed",
            details={"me_response": self._safe_json(me_response)},
        )

    async def _authenticate_jwt(self) -> AuthSession:
        """
        Perform JWT authentication (legacy Superset 3.x-5.x).
        
        Steps:
        1. Login with username/password to get JWT
        2. Fetch CSRF token
        3. Calculate token expiry
        """
        # Step 1: Login
        login_payload = {
            "username": self.config.superset_username,
            "password": self.config.superset_password.get_secret_value(),
            "provider": "db",
            "refresh": True,
        }

        try:
            login_response = await self._client.post(
                "/security/login",
                json=login_payload,
            )
        except httpx.RequestError as e:
            raise AuthenticationError(
                f"Failed to connect to Superset: {e}",
                details={"url": self.config.superset_base_url},
            ) from e

        if login_response.status_code != 200:
            error_body = self._safe_json(login_response)
            raise AuthenticationError(
                f"Login failed with status {login_response.status_code}",
                details={
                    "status_code": login_response.status_code,
                    "response": error_body,
                },
            )

        login_data = login_response.json()
        access_token = login_data.get("access_token")
        refresh_token = login_data.get("refresh_token")

        if not access_token:
            raise AuthenticationError(
                "Login response missing access_token",
                details={"response": login_data},
            )

        # Step 2: Get CSRF token
        csrf_token = await self._fetch_csrf_token(access_token)

        # Step 3: Calculate expiry
        expires_at = self._extract_expiry(access_token)

        logger.info("JWT-based auth successful")
        return AuthSession(
            access_token=access_token,
            csrf_token=csrf_token,
            refresh_token=refresh_token,
            expires_at=expires_at,
            session_based=False,
        )

    async def _fetch_csrf_token_initial(self) -> str:
        """
        Fetch CSRF token without access token (for session-based auth).
        
        Uses the root client to ensure cookies are shared with the login request.
        """
        try:
            # Use root client so session cookie is shared with login request
            response = await self._root_client.get("/api/v1/security/csrf_token/")
        except httpx.RequestError as e:
            raise CSRFTokenError(f"Failed to fetch CSRF token: {e}") from e

        if response.status_code != 200:
            raise CSRFTokenError(
                f"CSRF token request failed with status {response.status_code}"
            )

        data = response.json()
        csrf_token = data.get("result")

        if not csrf_token:
            raise CSRFTokenError("CSRF token response missing 'result' field")

        return csrf_token

    async def _fetch_csrf_token(self, access_token: str) -> str:
        """Fetch CSRF token using the access token."""
        try:
            response = await self._client.get(
                "/security/csrf_token/",
                headers={"Authorization": f"Bearer {access_token}"},
            )
        except httpx.RequestError as e:
            raise CSRFTokenError(f"Failed to fetch CSRF token: {e}") from e

        if response.status_code != 200:
            raise CSRFTokenError(
                f"CSRF token request failed with status {response.status_code}"
            )

        data = response.json()
        csrf_token = data.get("result")

        if not csrf_token:
            raise CSRFTokenError("CSRF token response missing 'result' field")

        return csrf_token

    async def _refresh_token(self) -> AuthSession:
        """
        Refresh the access token using the refresh token.
        
        If refresh token is not available, raises AuthenticationError.
        """
        if self._session is None or not self._session.refresh_token:
            raise AuthenticationError("No refresh token available")

        try:
            response = await self._client.post(
                "/security/refresh",
                headers={"Authorization": f"Bearer {self._session.refresh_token}"},
            )
        except httpx.RequestError as e:
            raise AuthenticationError(f"Token refresh failed: {e}") from e

        if response.status_code != 200:
            raise AuthenticationError(
                f"Token refresh failed with status {response.status_code}",
                details={"response": self._safe_json(response)},
            )

        data = response.json()
        new_access_token = data.get("access_token")

        if not new_access_token:
            raise AuthenticationError("Refresh response missing access_token")

        # Fetch new CSRF token
        csrf_token = await self._fetch_csrf_token(new_access_token)

        return AuthSession(
            access_token=new_access_token,
            csrf_token=csrf_token,
            refresh_token=self._session.refresh_token,
            expires_at=self._extract_expiry(new_access_token),
            session_based=False,
        )

    def _extract_expiry(self, token: str) -> float:
        """
        Extract expiry time from JWT token.
        
        Falls back to 1 hour from now if parsing fails.
        """
        import base64
        import json

        try:
            # JWT format: header.payload.signature
            parts = token.split(".")
            if len(parts) != 3:
                return time.time() + 3600

            # Decode payload (add padding if needed)
            payload = parts[1]
            padding = 4 - len(payload) % 4
            if padding != 4:
                payload += "=" * padding

            decoded = base64.urlsafe_b64decode(payload)
            claims = json.loads(decoded)
            
            exp = claims.get("exp")
            if exp and isinstance(exp, (int, float)):
                return float(exp)
        except Exception:
            pass

        # Default: 1 hour from now
        return time.time() + 3600

    @staticmethod
    def _safe_json(response: httpx.Response) -> dict | str:
        """Safely extract JSON from response, returning text on failure."""
        try:
            return response.json()
        except Exception:
            return response.text
