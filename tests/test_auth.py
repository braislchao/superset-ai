"""Tests for authentication module."""

import pytest
from unittest.mock import AsyncMock, MagicMock, patch

from supersetai.api.auth import SupersetAuthManager, AuthSession
from supersetai.core.config import SupersetConfig
from supersetai.core.exceptions import AuthenticationError


@pytest.fixture
def config():
    """Create test configuration."""
    return SupersetConfig(
        superset_base_url="http://localhost:8088",
        superset_username="admin",
        superset_password="admin",
        openai_api_key="test-key",
    )


@pytest.fixture
def auth_manager(config):
    """Create auth manager instance."""
    return SupersetAuthManager(config)


class TestAuthSession:
    """Tests for AuthSession."""
    
    def test_is_expired_returns_false_for_future_expiry(self):
        """Session should not be expired if expiry is in the future."""
        import time
        session = AuthSession(
            access_token="token",
            csrf_token="csrf",
            refresh_token=None,
            expires_at=time.time() + 3600,  # 1 hour from now
        )
        assert not session.is_expired()
    
    def test_is_expired_returns_true_for_past_expiry(self):
        """Session should be expired if expiry is in the past."""
        import time
        session = AuthSession(
            access_token="token",
            csrf_token="csrf",
            refresh_token=None,
            expires_at=time.time() - 100,  # 100 seconds ago
        )
        assert session.is_expired()
    
    def test_is_expired_considers_buffer(self):
        """Session should be considered expired within buffer window."""
        import time
        session = AuthSession(
            access_token="token",
            csrf_token="csrf",
            refresh_token=None,
            expires_at=time.time() + 200,  # 200 seconds from now
        )
        # With default 300 second buffer, should be "expired"
        assert session.is_expired(buffer_seconds=300)
        # With smaller buffer, should not be expired
        assert not session.is_expired(buffer_seconds=100)


class TestSupersetAuthManager:
    """Tests for SupersetAuthManager."""
    
    @pytest.mark.asyncio
    async def test_extract_expiry_from_jwt(self, auth_manager):
        """Should extract expiry from valid JWT."""
        import base64
        import json
        import time
        
        # Create a mock JWT with expiry
        exp_time = int(time.time()) + 3600
        payload = {"exp": exp_time, "sub": "admin"}
        payload_b64 = base64.urlsafe_b64encode(
            json.dumps(payload).encode()
        ).decode().rstrip("=")
        
        mock_jwt = f"header.{payload_b64}.signature"
        
        extracted = auth_manager._extract_expiry(mock_jwt)
        assert extracted == exp_time
    
    @pytest.mark.asyncio
    async def test_extract_expiry_fallback_for_invalid_jwt(self, auth_manager):
        """Should return default expiry for invalid JWT."""
        import time
        
        invalid_jwt = "not.a.valid.jwt"
        extracted = auth_manager._extract_expiry(invalid_jwt)
        
        # Should be approximately 1 hour from now
        assert extracted > time.time()
        assert extracted < time.time() + 3700

    @pytest.mark.asyncio
    async def test_safe_json_returns_dict_for_valid_json(self, auth_manager):
        """Should return parsed JSON for valid response."""
        mock_response = MagicMock()
        mock_response.json.return_value = {"key": "value"}
        
        result = auth_manager._safe_json(mock_response)
        assert result == {"key": "value"}
    
    @pytest.mark.asyncio
    async def test_safe_json_returns_text_for_invalid_json(self, auth_manager):
        """Should return text for invalid JSON response."""
        mock_response = MagicMock()
        mock_response.json.side_effect = ValueError("Invalid JSON")
        mock_response.text = "Plain text response"
        
        result = auth_manager._safe_json(mock_response)
        assert result == "Plain text response"
