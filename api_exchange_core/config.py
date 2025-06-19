"""
Centralized configuration management for the API Exchange framework.

This module provides a unified configuration system with support for:
- Environment variables
- Feature flags
- Runtime configuration
- Validation using Pydantic
"""

import os
from typing import Any, Dict, Optional

from pydantic import BaseModel, Field, field_validator

from .constants import EnvironmentVariable, LogLevel


class DatabaseConfig(BaseModel):
    """Database connection configuration."""

    connection_string: str = Field(
        default_factory=lambda: os.getenv(
            EnvironmentVariable.DATABASE_URL.value, "sqlite:///./api_exchange.db"
        ),
        description="Database connection string",
    )
    pool_size: int = Field(default=5, description="Connection pool size")
    max_overflow: int = Field(default=10, description="Maximum overflow connections")
    pool_timeout: int = Field(default=30, description="Pool timeout in seconds")
    echo: bool = Field(default=False, description="Echo SQL statements")


class QueueConfig(BaseModel):
    """Queue configuration for Azure Storage Queues."""

    connection_string: str = Field(
        default_factory=lambda: os.getenv(EnvironmentVariable.AZURE_STORAGE_CONNECTION.value, ""),
        description="Azure Storage connection string",
    )
    metrics_queue_name: str = Field(default="metrics-queue", description="Metrics queue name")
    error_queue_name: str = Field(default="error-queue", description="Error queue name")
    default_visibility_timeout: int = Field(
        default=30, description="Default message visibility timeout in seconds"
    )


class LoggingConfig(BaseModel):
    """Logging configuration."""

    level: str = Field(
        default_factory=lambda: os.getenv(EnvironmentVariable.LOG_LEVEL.value, LogLevel.INFO.value),
        description="Logging level",
    )
    format: str = Field(
        default="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        description="Log format string",
    )
    enable_json_logs: bool = Field(default=False, description="Enable JSON structured logging")

    @field_validator("level")
    def validate_log_level(cls, v: str) -> str:
        """Validate log level is valid."""
        valid_levels = {level.value for level in LogLevel}
        if v.upper() not in valid_levels:
            raise ValueError(f"Invalid log level: {v}. Must be one of {valid_levels}")
        return v.upper()


class FeatureFlags(BaseModel):
    """Feature flags for controlling framework behavior."""

    enable_metrics: bool = Field(default=True, description="Enable metrics collection")
    enable_state_tracking: bool = Field(default=True, description="Enable automatic state tracking")
    enable_content_hashing: bool = Field(
        default=True, description="Enable content hashing for entities"
    )
    enable_operation_context: bool = Field(
        default=True, description="Enable operation context tracking"
    )
    enable_tenant_isolation: bool = Field(default=True, description="Enable multi-tenant isolation")
    enable_auto_retry: bool = Field(
        default=True, description="Enable automatic retry for recoverable errors"
    )
    enable_dead_letter_queue: bool = Field(
        default=True, description="Enable dead letter queue for failed messages"
    )


class ProcessingConfig(BaseModel):
    """Configuration for processing behavior."""

    max_retry_attempts: int = Field(
        default=3, description="Maximum retry attempts for recoverable errors"
    )
    retry_backoff_base: int = Field(default=2, description="Base for exponential backoff (seconds)")
    retry_backoff_max: int = Field(default=300, description="Maximum backoff time (seconds)")
    processing_timeout: int = Field(default=300, description="Processing timeout (seconds)")
    batch_size: int = Field(default=100, description="Default batch size for bulk operations")


class SecurityConfig(BaseModel):
    """Security-related configuration."""

    enable_encryption: bool = Field(
        default=False, description="Enable encryption for sensitive data"
    )
    encryption_key: Optional[str] = Field(
        default=None, description="Encryption key (base64 encoded)"
    )
    enable_audit_logging: bool = Field(default=True, description="Enable audit logging")
    allowed_origins: list[str] = Field(default_factory=list, description="Allowed CORS origins")


class AppConfig(BaseModel):
    """Main application configuration."""

    environment: str = Field(
        default_factory=lambda: os.getenv(EnvironmentVariable.APP_ENV.value, "development"),
        description="Application environment",
    )
    debug: bool = Field(
        default_factory=lambda: os.getenv("DEBUG", "false").lower() == "true",
        description="Debug mode",
    )

    # Sub-configurations
    database: DatabaseConfig = Field(
        default_factory=DatabaseConfig, description="Database configuration"
    )
    queue: QueueConfig = Field(default_factory=QueueConfig, description="Queue configuration")
    logging: LoggingConfig = Field(
        default_factory=LoggingConfig, description="Logging configuration"
    )
    features: FeatureFlags = Field(default_factory=FeatureFlags, description="Feature flags")
    processing: ProcessingConfig = Field(
        default_factory=ProcessingConfig, description="Processing configuration"
    )
    security: SecurityConfig = Field(
        default_factory=SecurityConfig, description="Security configuration"
    )

    # Custom configuration
    custom: Dict[str, Any] = Field(default_factory=dict, description="Custom configuration values")

    @classmethod
    def from_env(cls) -> "AppConfig":
        """Create configuration from environment variables."""
        return cls()

    def get_custom(self, key: str, default: Any = None) -> Any:
        """Get a custom configuration value."""
        return self.custom.get(key, default)

    def set_custom(self, key: str, value: Any) -> None:
        """Set a custom configuration value."""
        self.custom[key] = value


# Global configuration instance
_config: Optional[AppConfig] = None


def get_config() -> AppConfig:
    """Get the global configuration instance."""
    global _config
    if _config is None:
        _config = AppConfig.from_env()
    return _config


def set_config(config: AppConfig) -> None:
    """Set the global configuration instance."""
    global _config
    _config = config


def reset_config() -> None:
    """Reset the global configuration instance."""
    global _config
    _config = None
