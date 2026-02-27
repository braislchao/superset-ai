"""Configuration management using pydantic-settings."""

from functools import lru_cache
from typing import Literal

from pydantic import Field, SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict


class SupersetConfig(BaseSettings):
    """Configuration for Superset connection and API settings."""

    model_config = SettingsConfigDict(
        env_prefix="SUPERSETAI_",
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    # Superset connection
    superset_base_url: str = Field(
        default="http://localhost:8088",
        description="Base URL of the Superset instance",
    )
    superset_username: str = Field(
        default="admin",
        description="Superset username for authentication",
    )
    superset_password: SecretStr = Field(
        default=SecretStr("admin"),
        description="Superset password for authentication",
    )

    # LLM Provider configuration
    llm_provider: Literal["openai", "copilot"] = Field(
        default="copilot",
        description="LLM provider to use: 'openai' or 'copilot' (GitHub Copilot)",
    )
    
    # OpenAI configuration (used when llm_provider='openai')
    openai_api_key: SecretStr | None = Field(
        default=None,
        description="OpenAI API key for LLM integration (not needed for copilot)",
    )
    openai_model: str = Field(
        default="gpt-4o",
        description="OpenAI model to use for the agent",
    )
    
    # GitHub Copilot configuration (used when llm_provider='copilot')
    # NOTE: Some models like gpt-5.2 return incomplete token usage metadata that
    # breaks langchain-openai v1.1.10. Known working models: gpt-4o, claude-sonnet-4.6
    copilot_model: str = Field(
        default="gpt-4o",
        description="Model to use via GitHub Copilot (gpt-4o, claude-sonnet-4.6, gpt-4.1)",
    )

    # HTTP client settings
    request_timeout: int = Field(
        default=30,
        ge=5,
        le=300,
        description="Request timeout in seconds",
    )
    max_retries: int = Field(
        default=3,
        ge=1,
        le=10,
        description="Maximum number of retry attempts",
    )

    # Logging
    log_level: Literal["DEBUG", "INFO", "WARNING", "ERROR"] = Field(
        default="INFO",
        description="Logging level",
    )

    @property
    def api_base_url(self) -> str:
        """Return the base URL for API endpoints."""
        return f"{self.superset_base_url.rstrip('/')}/api/v1"
    
    def get_llm_api_key(self) -> str:
        """Get the API key for the configured LLM provider."""
        if self.llm_provider == "openai":
            if not self.openai_api_key:
                raise ValueError("OpenAI API key is required when llm_provider='openai'")
            return self.openai_api_key.get_secret_value()
        else:  # copilot
            from supersetai.core.copilot_auth import get_copilot_token
            return get_copilot_token()
    
    def get_llm_base_url(self) -> str | None:
        """Get the base URL for the LLM API."""
        if self.llm_provider == "copilot":
            return "https://api.githubcopilot.com"
        return None  # Use default OpenAI URL
    
    def get_llm_model(self) -> str:
        """Get the model name for the configured provider."""
        if self.llm_provider == "copilot":
            return self.copilot_model
        return self.openai_model


@lru_cache
def get_config() -> SupersetConfig:
    """Get cached configuration instance."""
    return SupersetConfig()
