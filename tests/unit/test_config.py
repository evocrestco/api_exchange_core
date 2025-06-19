"""Tests for centralized configuration module."""

import os
from unittest.mock import patch

import pytest

from api_exchange_core.config import (
    AppConfig,
    DatabaseConfig,
    FeatureFlags,
    LoggingConfig,
    ProcessingConfig,
    QueueConfig,
    SecurityConfig,
    get_config,
    reset_config,
    set_config,
)
from api_exchange_core.constants import LogLevel


class TestDatabaseConfig:
    """Test DatabaseConfig model."""

    def test_default_values(self):
        """Test default database configuration values."""
        config = DatabaseConfig()
        assert config.connection_string == "sqlite:///./api_exchange.db"
        assert config.pool_size == 5
        assert config.max_overflow == 10
        assert config.pool_timeout == 30
        assert config.echo is False

    def test_from_env(self):
        """Test loading database config from environment."""
        with patch.dict(os.environ, {"DATABASE_URL": "postgresql://test:pass@localhost/db"}):
            config = DatabaseConfig()
            assert config.connection_string == "postgresql://test:pass@localhost/db"


class TestQueueConfig:
    """Test QueueConfig model."""

    def test_default_values(self):
        """Test default queue configuration values."""
        config = QueueConfig()
        assert config.connection_string == ""
        assert config.metrics_queue_name == "metrics-queue"
        assert config.error_queue_name == "error-queue"
        assert config.default_visibility_timeout == 30

    def test_from_env(self):
        """Test loading queue config from environment."""
        with patch.dict(os.environ, {"AzureWebJobsStorage": "DefaultEndpointsProtocol=https;..."}):
            config = QueueConfig()
            assert config.connection_string == "DefaultEndpointsProtocol=https;..."


class TestLoggingConfig:
    """Test LoggingConfig model."""

    def test_default_values(self):
        """Test default logging configuration values."""
        config = LoggingConfig()
        assert config.level == LogLevel.INFO.value
        assert config.format == "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        assert config.enable_json_logs is False

    def test_from_env(self):
        """Test loading logging config from environment."""
        with patch.dict(os.environ, {"LOG_LEVEL": "DEBUG"}):
            config = LoggingConfig()
            assert config.level == "DEBUG"

    def test_validate_log_level(self):
        """Test log level validation."""
        # Valid log level
        config = LoggingConfig(level="debug")
        assert config.level == "DEBUG"

        # Invalid log level
        with pytest.raises(ValueError, match="Invalid log level"):
            LoggingConfig(level="INVALID")


class TestFeatureFlags:
    """Test FeatureFlags model."""

    def test_default_values(self):
        """Test default feature flag values."""
        flags = FeatureFlags()
        assert flags.enable_metrics is True
        assert flags.enable_state_tracking is True
        assert flags.enable_content_hashing is True
        assert flags.enable_operation_context is True
        assert flags.enable_tenant_isolation is True
        assert flags.enable_auto_retry is True
        assert flags.enable_dead_letter_queue is True


class TestProcessingConfig:
    """Test ProcessingConfig model."""

    def test_default_values(self):
        """Test default processing configuration values."""
        config = ProcessingConfig()
        assert config.max_retry_attempts == 3
        assert config.retry_backoff_base == 2
        assert config.retry_backoff_max == 300
        assert config.processing_timeout == 300
        assert config.batch_size == 100


class TestSecurityConfig:
    """Test SecurityConfig model."""

    def test_default_values(self):
        """Test default security configuration values."""
        config = SecurityConfig()
        assert config.enable_encryption is False
        assert config.encryption_key is None
        assert config.enable_audit_logging is True
        assert config.allowed_origins == []


class TestAppConfig:
    """Test main AppConfig model."""

    def test_default_values(self):
        """Test default app configuration values."""
        config = AppConfig()
        assert config.environment == "development"
        assert config.debug is False
        assert isinstance(config.database, DatabaseConfig)
        assert isinstance(config.queue, QueueConfig)
        assert isinstance(config.logging, LoggingConfig)
        assert isinstance(config.features, FeatureFlags)
        assert isinstance(config.processing, ProcessingConfig)
        assert isinstance(config.security, SecurityConfig)
        assert config.custom == {}

    def test_from_env(self):
        """Test loading app config from environment."""
        env_vars = {
            "APP_ENV": "production",
            "DEBUG": "true",
            "DATABASE_URL": "postgresql://prod:pass@db/app",
            "LOG_LEVEL": "ERROR",
        }
        with patch.dict(os.environ, env_vars):
            config = AppConfig.from_env()
            assert config.environment == "production"
            assert config.debug is True
            assert config.database.connection_string == "postgresql://prod:pass@db/app"
            assert config.logging.level == "ERROR"

    def test_custom_config(self):
        """Test custom configuration get/set."""
        config = AppConfig()

        # Test get with default
        assert config.get_custom("missing_key", "default") == "default"

        # Test set and get
        config.set_custom("api_key", "secret123")
        assert config.get_custom("api_key") == "secret123"
        assert config.custom["api_key"] == "secret123"


class TestConfigManagement:
    """Test global configuration management functions."""

    def teardown_method(self):
        """Reset config after each test."""
        reset_config()

    def test_get_config_singleton(self):
        """Test get_config returns singleton instance."""
        config1 = get_config()
        config2 = get_config()
        assert config1 is config2

    def test_set_config(self):
        """Test setting custom configuration."""
        custom_config = AppConfig(environment="test")
        set_config(custom_config)

        config = get_config()
        assert config is custom_config
        assert config.environment == "test"

    def test_reset_config(self):
        """Test resetting configuration."""
        # Set custom config
        custom_config = AppConfig(environment="custom")
        set_config(custom_config)
        assert get_config().environment == "custom"

        # Reset and verify new instance
        reset_config()
        new_config = get_config()
        assert new_config is not custom_config
        assert new_config.environment == "development"


class TestConfigIntegration:
    """Test configuration integration scenarios."""

    def test_production_config(self):
        """Test production configuration setup."""
        env_vars = {
            "APP_ENV": "production",
            "DATABASE_URL": "postgresql://user:pass@prod-db:5432/app",
            "AzureWebJobsStorage": "DefaultEndpointsProtocol=https;AccountName=prod;...",
            "LOG_LEVEL": "WARNING",
        }
        with patch.dict(os.environ, env_vars, clear=True):
            reset_config()
            config = get_config()

            assert config.environment == "production"
            assert "postgresql://" in config.database.connection_string
            assert "AccountName=prod" in config.queue.connection_string
            assert config.logging.level == "WARNING"

    def test_development_config(self):
        """Test development configuration setup."""
        with patch.dict(os.environ, {}, clear=True):
            reset_config()
            config = get_config()

            assert config.environment == "development"
            assert "sqlite:///" in config.database.connection_string
            assert config.queue.connection_string == ""
            assert config.logging.level == "INFO"
