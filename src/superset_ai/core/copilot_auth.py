"""GitHub Copilot OAuth Device Flow authentication.

This implements the same OAuth flow that GitHub Copilot CLI uses:
1. Request device code from GitHub
2. User opens browser and enters the code
3. Poll for access token
4. Exchange GitHub token for Copilot token
"""

import json
import time
import webbrowser
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import httpx

# GitHub Copilot OAuth Client ID (public, used by VS Code Copilot)
COPILOT_CLIENT_ID = "Iv1.b507a08c87ecfe98"

# Token storage locations
TOKEN_CACHE_PATH = Path.home() / ".superset-ai" / "copilot_token.json"
GITHUB_TOKEN_CACHE_PATH = Path.home() / ".superset-ai" / "github_token.json"


@dataclass
class CopilotToken:
    """Copilot API access token."""

    access_token: str
    expires_at: datetime

    def is_expired(self, buffer_seconds: int = 300) -> bool:
        """Check if token is expired or about to expire."""
        return datetime.now(tz=UTC) >= (self.expires_at - timedelta(seconds=buffer_seconds))

    def to_dict(self) -> dict[str, Any]:
        """Serialize to dict for caching."""
        return {
            "access_token": self.access_token,
            "expires_at": self.expires_at.isoformat(),
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "CopilotToken":
        """Deserialize from dict."""
        return cls(
            access_token=data["access_token"],
            expires_at=datetime.fromisoformat(data["expires_at"]),
        )


class CopilotAuthError(Exception):
    """Error during Copilot authentication."""

    pass


def load_cached_token() -> CopilotToken | None:
    """Load token from cache if valid."""
    if not TOKEN_CACHE_PATH.exists():
        return None

    try:
        with open(TOKEN_CACHE_PATH) as f:
            data = json.load(f)
        token = CopilotToken.from_dict(data)
        if not token.is_expired():
            return token
    except (json.JSONDecodeError, KeyError, ValueError):
        pass

    return None


def save_token_to_cache(token: CopilotToken) -> None:
    """Save token to cache."""
    TOKEN_CACHE_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(TOKEN_CACHE_PATH, "w") as f:
        json.dump(token.to_dict(), f)
    # Restrict permissions
    TOKEN_CACHE_PATH.chmod(0o600)


def clear_cached_token() -> None:
    """Clear cached token."""
    if TOKEN_CACHE_PATH.exists():
        TOKEN_CACHE_PATH.unlink()
    if GITHUB_TOKEN_CACHE_PATH.exists():
        GITHUB_TOKEN_CACHE_PATH.unlink()


def save_github_token(token: str) -> None:
    """Save GitHub OAuth token to cache for debugging/fallback."""
    GITHUB_TOKEN_CACHE_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(GITHUB_TOKEN_CACHE_PATH, "w") as f:
        json.dump({"github_token": token}, f)
    GITHUB_TOKEN_CACHE_PATH.chmod(0o600)


def load_github_token() -> str | None:
    """Load cached GitHub token."""
    if not GITHUB_TOKEN_CACHE_PATH.exists():
        return None
    try:
        with open(GITHUB_TOKEN_CACHE_PATH) as f:
            data = json.load(f)
        return data.get("github_token")
    except (json.JSONDecodeError, KeyError):
        return None


def request_device_code() -> dict[str, Any]:
    """
    Request a device code from GitHub.

    Returns dict with:
    - device_code: Code to poll with
    - user_code: Code user enters in browser
    - verification_uri: URL to open
    - expires_in: Seconds until code expires
    - interval: Polling interval in seconds
    """
    with httpx.Client() as client:
        response = client.post(
            "https://github.com/login/device/code",
            data={
                "client_id": COPILOT_CLIENT_ID,
                # No scope - let the OAuth app define its default scopes
            },
            headers={"Accept": "application/json"},
        )
        response.raise_for_status()
        return response.json()


def poll_for_github_token(device_code: str, interval: int, timeout: int = 900) -> str:
    """
    Poll GitHub for access token after user authorizes.

    Args:
        device_code: The device code from request_device_code
        interval: Polling interval in seconds
        timeout: Maximum time to wait in seconds

    Returns:
        GitHub access token

    Raises:
        CopilotAuthError: If authentication fails or times out
    """
    start_time = time.time()

    with httpx.Client() as client:
        while time.time() - start_time < timeout:
            response = client.post(
                "https://github.com/login/oauth/access_token",
                data={
                    "client_id": COPILOT_CLIENT_ID,
                    "device_code": device_code,
                    "grant_type": "urn:ietf:params:oauth:grant-type:device_code",
                },
                headers={"Accept": "application/json"},
            )

            data = response.json()

            if "access_token" in data:
                return data["access_token"]

            error = data.get("error")
            if error == "authorization_pending":
                # User hasn't authorized yet, keep polling
                time.sleep(interval)
            elif error == "slow_down":
                # GitHub wants us to slow down
                interval += 5
                time.sleep(interval)
            elif error == "expired_token":
                raise CopilotAuthError("Device code expired. Please try again.")
            elif error == "access_denied":
                raise CopilotAuthError("Access denied by user.")
            else:
                raise CopilotAuthError(f"Authentication failed: {error}")

    raise CopilotAuthError("Authentication timed out. Please try again.")


def exchange_for_copilot_token(github_token: str) -> CopilotToken:
    """
    Exchange GitHub token for Copilot API token.

    Args:
        github_token: GitHub OAuth access token

    Returns:
        CopilotToken for API calls
    """
    with httpx.Client() as client:
        response = client.get(
            "https://api.github.com/copilot_internal/v2/token",
            headers={
                "Authorization": f"token {github_token}",
                "Accept": "application/json",
                "Editor-Version": "vscode/1.95.0",
                "Editor-Plugin-Version": "copilot/1.0.0",
                "User-Agent": "GitHubCopilotChat/1.0.0",
            },
        )

        if response.status_code == 401:
            raise CopilotAuthError(
                "GitHub token not authorized for Copilot. "
                "Make sure you have an active GitHub Copilot subscription."
            )

        if response.status_code == 403:
            raise CopilotAuthError(
                "Access forbidden. Make sure you have an active GitHub Copilot subscription "
                "and that your account has access to Copilot API features."
            )

        if response.status_code == 404:
            raise CopilotAuthError(
                "Copilot token endpoint not found. This may indicate "
                "the token lacks required permissions."
            )

        response.raise_for_status()
        data = response.json()

        return CopilotToken(
            access_token=data["token"],
            expires_at=datetime.fromtimestamp(data["expires_at"], tz=UTC),
        )


def authenticate_copilot(
    open_browser: bool = True,
    print_fn: Callable[..., Any] = print,
) -> CopilotToken:
    """
    Full Copilot authentication flow.

    1. Check for cached valid token
    2. If not, start device flow
    3. Open browser for user to authorize
    4. Poll for GitHub token
    5. Exchange for Copilot token
    6. Cache and return

    Args:
        open_browser: Whether to automatically open the browser
        print_fn: Function to use for printing messages

    Returns:
        CopilotToken for API calls
    """
    # Check cache first
    cached = load_cached_token()
    if cached:
        print_fn("Using cached Copilot token")
        return cached

    # Start device flow
    print_fn("Authenticating with GitHub Copilot...")
    device_data = request_device_code()

    user_code = device_data["user_code"]
    verification_uri = device_data["verification_uri"]
    device_code = device_data["device_code"]
    interval = device_data.get("interval", 5)

    # Show instructions
    print_fn(f"\nPlease visit: {verification_uri}")
    print_fn(f"And enter code: {user_code}\n")

    # Open browser
    if open_browser:
        webbrowser.open(verification_uri)

    # Poll for token
    print_fn("Waiting for authorization...")
    github_token = poll_for_github_token(device_code, interval)
    print_fn("GitHub authorization successful!")

    # Save GitHub token for debugging/fallback
    save_github_token(github_token)

    # Exchange for Copilot token
    print_fn("Getting Copilot token...")
    copilot_token = exchange_for_copilot_token(github_token)

    # Cache token
    save_token_to_cache(copilot_token)
    print_fn("Authentication complete!\n")

    return copilot_token


def get_copilot_token() -> str:
    """
    Get a valid Copilot API token.

    Checks cache first, then authenticates if needed.

    Returns:
        Copilot API access token string
    """
    # Check cache
    cached = load_cached_token()
    if cached:
        return cached.access_token

    # Need to authenticate - this will raise if not in interactive mode
    token = authenticate_copilot()
    return token.access_token
