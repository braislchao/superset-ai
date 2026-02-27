"""Tests for configuration module."""

import os
import pytest
from unittest.mock import patch

from pydantic import SecretStr


class TestSupersetConfig:
    """Tests for SupersetConfig."""
    
    def test_loads_defaults(self):
        """Should load default values."""
        from supersetai.core.config import SupersetConfig
        
        with patch.dict(os.environ, {"SUPERSETAI_OPENAI_API_KEY": "test-key"}):
            config = SupersetConfig()
            
            assert config.superset_base_url == "http://localhost:8088"
            assert config.superset_username == "admin"
            assert config.request_timeout == 30
            assert config.max_retries == 3
    
    def test_loads_from_env(self):
        """Should load values from environment variables."""
        from supersetai.core.config import SupersetConfig
        
        env_vars = {
            "SUPERSETAI_SUPERSET_BASE_URL": "http://custom:9000",
            "SUPERSETAI_SUPERSET_USERNAME": "custom_user",
            "SUPERSETAI_SUPERSET_PASSWORD": "custom_pass",
            "SUPERSETAI_OPENAI_API_KEY": "sk-test-key",
            "SUPERSETAI_REQUEST_TIMEOUT": "60",
        }
        
        with patch.dict(os.environ, env_vars, clear=False):
            config = SupersetConfig()
            
            assert config.superset_base_url == "http://custom:9000"
            assert config.superset_username == "custom_user"
            assert config.superset_password.get_secret_value() == "custom_pass"
            assert config.openai_api_key.get_secret_value() == "sk-test-key"
            assert config.request_timeout == 60
    
    def test_api_base_url_property(self):
        """Should construct API base URL correctly."""
        from supersetai.core.config import SupersetConfig
        
        with patch.dict(os.environ, {"SUPERSETAI_OPENAI_API_KEY": "test-key"}):
            config = SupersetConfig(superset_base_url="http://localhost:8088/")
            
            assert config.api_base_url == "http://localhost:8088/api/v1"
    
    def test_api_base_url_handles_trailing_slash(self):
        """Should handle trailing slash in base URL."""
        from supersetai.core.config import SupersetConfig
        
        with patch.dict(os.environ, {"SUPERSETAI_OPENAI_API_KEY": "test-key"}):
            config1 = SupersetConfig(superset_base_url="http://localhost:8088")
            config2 = SupersetConfig(superset_base_url="http://localhost:8088/")
            
            assert config1.api_base_url == config2.api_base_url
    
    def test_openai_api_key_is_optional(self):
        """OpenAI API key should be optional for non-chat commands."""
        from supersetai.core.config import SupersetConfig
        
        # Clear any existing env var
        with patch.dict(os.environ, {}, clear=True):
            config = SupersetConfig()
            assert config.openai_api_key is None
