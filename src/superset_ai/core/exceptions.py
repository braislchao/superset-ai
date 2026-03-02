"""Custom exceptions for SupersetAI."""

from typing import Any


class SupersetAIError(Exception):
    """Base exception for all SupersetAI errors."""

    def __init__(self, message: str, details: dict[str, Any] | None = None) -> None:
        self.message = message
        self.details = details or {}
        super().__init__(message)


# =============================================================================
# Authentication Errors
# =============================================================================


class AuthenticationError(SupersetAIError):
    """Raised when authentication fails."""

    def __init__(
        self,
        message: str = "Authentication failed",
        details: dict[str, Any] | None = None,
    ) -> None:
        super().__init__(message, details)


class CSRFTokenError(AuthenticationError):
    """Raised when CSRF token retrieval or validation fails."""

    def __init__(self, message: str = "Failed to obtain CSRF token") -> None:
        super().__init__(message)


# =============================================================================
# API Errors
# =============================================================================


class SupersetAPIError(SupersetAIError):
    """Base exception for Superset API errors."""

    def __init__(
        self,
        message: str,
        status_code: int | None = None,
        response_body: dict[str, Any] | None = None,
    ) -> None:
        details = {
            "status_code": status_code,
            "response_body": response_body,
        }
        super().__init__(message, details)
        self.status_code = status_code
        self.response_body = response_body


class ResourceNotFoundError(SupersetAPIError):
    """Raised when a requested resource does not exist (404)."""

    def __init__(
        self,
        resource_type: str,
        resource_id: int | str,
        response_body: dict[str, Any] | None = None,
    ) -> None:
        message = f"{resource_type} with ID '{resource_id}' not found"
        super().__init__(message, status_code=404, response_body=response_body)
        self.resource_type = resource_type
        self.resource_id = resource_id


class ValidationError(SupersetAPIError):
    """Raised when the API rejects a payload due to validation errors (400/422)."""

    def __init__(
        self,
        message: str = "Validation error",
        status_code: int = 400,
        response_body: dict[str, Any] | None = None,
        validation_errors: list[dict[str, Any]] | None = None,
    ) -> None:
        super().__init__(message, status_code, response_body)
        self.validation_errors = validation_errors or []


class PermissionDeniedError(SupersetAPIError):
    """Raised when the user lacks permission for an operation (403)."""

    def __init__(
        self,
        message: str = "Permission denied",
        response_body: dict[str, Any] | None = None,
    ) -> None:
        super().__init__(message, status_code=403, response_body=response_body)


class RateLimitError(SupersetAPIError):
    """Raised when rate limit is exceeded (429)."""

    def __init__(
        self,
        retry_after: int | None = None,
        response_body: dict[str, Any] | None = None,
    ) -> None:
        message = "Rate limit exceeded"
        if retry_after:
            message += f", retry after {retry_after} seconds"
        super().__init__(message, status_code=429, response_body=response_body)
        self.retry_after = retry_after


class ServerError(SupersetAPIError):
    """Raised for server-side errors (5xx)."""

    def __init__(
        self,
        message: str = "Superset server error",
        status_code: int = 500,
        response_body: dict[str, Any] | None = None,
    ) -> None:
        super().__init__(message, status_code, response_body)


# =============================================================================
# Agent Errors
# =============================================================================


class AgentError(SupersetAIError):
    """Base exception for agent-related errors."""

    pass
