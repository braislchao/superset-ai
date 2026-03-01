"""Base HTTP client for Superset API with retry logic and error handling."""

import logging
from typing import Any

import httpx
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential_jitter,
)

from superset_ai.api.auth import SupersetAuthManager
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

logger = logging.getLogger(__name__)


class SupersetClient:
    """
    Central HTTP client for Superset API interactions.
    
    Handles:
    - Authentication (JWT + CSRF tokens)
    - Automatic token refresh on 401
    - Retry logic with exponential backoff
    - Error classification and exception mapping
    """

    def __init__(self, config: SupersetConfig | None = None) -> None:
        from superset_ai.core.config import get_config

        self.config = config or get_config()
        self._http_client: httpx.AsyncClient | None = None
        # Create auth manager first (without client_getter to avoid circular dependency)
        self.auth = SupersetAuthManager(self.config)

    @property
    def _client(self) -> httpx.AsyncClient:
        """Lazy-initialize HTTP client."""
        if self._http_client is None:
            self._http_client = httpx.AsyncClient(
                base_url=self.config.api_base_url,
                timeout=self.config.request_timeout,
            )
        return self._http_client

    async def close(self) -> None:
        """Close all HTTP connections."""
        if self._http_client is not None:
            await self._http_client.aclose()
            self._http_client = None
        await self.auth.close()

    async def __aenter__(self) -> "SupersetClient":
        """Async context manager entry."""
        return self

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        """Async context manager exit."""
        await self.close()

    # =========================================================================
    # Public API Methods
    # =========================================================================

    async def get(
        self,
        endpoint: str,
        *,
        params: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Perform GET request."""
        return await self._request("GET", endpoint, params=params)

    async def post(
        self,
        endpoint: str,
        *,
        json: dict[str, Any] | None = None,
        params: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Perform POST request."""
        return await self._request("POST", endpoint, json=json, params=params)

    async def put(
        self,
        endpoint: str,
        *,
        json: dict[str, Any] | None = None,
        params: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Perform PUT request."""
        return await self._request("PUT", endpoint, json=json, params=params)

    async def delete(
        self,
        endpoint: str,
        *,
        params: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Perform DELETE request."""
        return await self._request("DELETE", endpoint, params=params)

    # =========================================================================
    # Internal Methods
    # =========================================================================

    async def _request(
        self,
        method: str,
        endpoint: str,
        *,
        json: dict[str, Any] | None = None,
        params: dict[str, Any] | None = None,
        _retry_auth: bool = True,
    ) -> dict[str, Any]:
        """
        Make authenticated request to Superset API.
        
        Handles:
        - Adding auth headers (JWT + CSRF)
        - Auto-retry on 401 (token expired)
        - Retry with exponential backoff on rate limits and timeouts
        - Error classification
        """

        @retry(
            stop=stop_after_attempt(self.config.max_retries),
            wait=wait_exponential_jitter(initial=1, max=10),
            retry=retry_if_exception_type((RateLimitError, httpx.TimeoutException)),
            reraise=True,
        )
        async def _do_request() -> dict[str, Any]:
            return await self._execute_request(
                method, endpoint, json=json, params=params, _retry_auth=_retry_auth
            )

        return await _do_request()

    async def _execute_request(
        self,
        method: str,
        endpoint: str,
        *,
        json: dict[str, Any] | None = None,
        params: dict[str, Any] | None = None,
        _retry_auth: bool = True,
    ) -> dict[str, Any]:
        """Execute a single authenticated request (called by _request with retry)."""
        session = await self.auth.get_valid_session()

        headers = {
            "X-CSRFToken": session.csrf_token,
            "Content-Type": "application/json",
            "Accept": "application/json",
        }
        
        # Only add Bearer token for JWT-based auth (not session-based)
        if not session.session_based and session.access_token:
            headers["Authorization"] = f"Bearer {session.access_token}"
        
        # For session-based auth, apply session cookies to the client
        if session.session_based and self.auth.session_cookies:
            for name, value in self.auth.session_cookies.items():
                self._client.cookies.set(name, value)

        # Ensure endpoint starts with /
        if not endpoint.startswith("/"):
            endpoint = f"/{endpoint}"

        logger.debug("%s %s", method, endpoint)

        try:
            response = await self._client.request(
                method,
                endpoint,
                json=json,
                params=params,
                headers=headers,
            )
        except httpx.TimeoutException:
            logger.warning("Request timeout: %s %s", method, endpoint)
            raise
        except httpx.RequestError as e:
            raise SupersetAPIError(f"Request failed: {e}") from e

        # Handle 401 - try to refresh and retry once
        if response.status_code == 401 and _retry_auth:
            logger.info("Received 401, attempting token refresh")
            await self.auth.invalidate()
            return await self._execute_request(
                method, endpoint, json=json, params=params, _retry_auth=False
            )

        # Classify errors
        self._raise_for_status(response, endpoint)

        # Parse response
        return self._parse_response(response)

    def _raise_for_status(self, response: httpx.Response, endpoint: str) -> None:
        """Classify HTTP errors and raise appropriate exceptions."""
        if response.is_success:
            return

        status = response.status_code
        body = self._safe_json(response)

        # Extract error message from Superset response
        message = self._extract_error_message(body) or f"Request failed: {endpoint}"

        if status == 400:
            raise ValidationError(
                message=message,
                status_code=status,
                response_body=body if isinstance(body, dict) else {"raw": body},
            )

        if status == 401:
            raise AuthenticationError(
                message=message,
                details={"response": body},
            )

        if status == 403:
            raise PermissionDeniedError(
                message=message,
                response_body=body if isinstance(body, dict) else {"raw": body},
            )

        if status == 404:
            # Try to extract resource info from endpoint
            parts = endpoint.strip("/").split("/")
            resource_type = parts[0] if parts else "resource"
            resource_id = parts[1] if len(parts) > 1 else "unknown"
            raise ResourceNotFoundError(
                resource_type=resource_type,
                resource_id=resource_id,
                response_body=body if isinstance(body, dict) else {"raw": body},
            )

        if status == 422:
            raise ValidationError(
                message=message,
                status_code=status,
                response_body=body if isinstance(body, dict) else {"raw": body},
            )

        if status == 429:
            retry_after = response.headers.get("Retry-After")
            raise RateLimitError(
                retry_after=int(retry_after) if retry_after else None,
                response_body=body if isinstance(body, dict) else {"raw": body},
            )

        if status >= 500:
            raise ServerError(
                message=message,
                status_code=status,
                response_body=body if isinstance(body, dict) else {"raw": body},
            )

        # Generic error for other status codes
        raise SupersetAPIError(
            message=message,
            status_code=status,
            response_body=body if isinstance(body, dict) else {"raw": body},
        )

    def _parse_response(self, response: httpx.Response) -> dict[str, Any]:
        """Parse JSON response body."""
        if response.status_code == 204:
            return {}

        try:
            return response.json()
        except Exception as e:
            logger.warning("Failed to parse JSON response: %s", e)
            return {"raw_response": response.text}

    def _extract_error_message(self, body: dict | str) -> str | None:
        """Extract error message from Superset error response."""
        if isinstance(body, str):
            return body if body else None

        # Superset uses various error formats
        for key in ["message", "msg", "error", "errors"]:
            if key in body:
                value = body[key]
                if isinstance(value, str):
                    return value
                if isinstance(value, list) and value:
                    return str(value[0])
                if isinstance(value, dict):
                    return str(value)

        return None

    @staticmethod
    def _safe_json(response: httpx.Response) -> dict | str:
        """Safely extract JSON from response."""
        try:
            return response.json()
        except Exception:
            return response.text
