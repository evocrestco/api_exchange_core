"""
Unit tests for output handler configuration management.

Tests the configuration loading, validation, and management functionality
for all output handler types.
"""

import os
import json
import tempfile
from unittest.mock import patch
from dataclasses import dataclass
import pytest

from src.processors.v2.output_handlers.config import (
    OutputHandlerConfigBase,
    QueueOutputHandlerConfig,
    ServiceBusOutputHandlerConfig,
    FileOutputHandlerConfig,
    NoOpOutputHandlerConfig,
    OutputHandlerConfigFactory,
    OutputHandlerConfigManager
)


class TestOutputHandlerConfigBase:
    """Test the base configuration class."""
    
    def test_base_config_creation(self):
        """Test creating a base configuration."""
        config = OutputHandlerConfigBase(
            handler_type="test",
            destination="test-dest",
            enabled=True,
            max_retries=5,
            retry_backoff_seconds=2,
            timeout_seconds=60
        )
        
        assert config.handler_type == "test"
        assert config.destination == "test-dest"
        assert config.enabled is True
        assert config.max_retries == 5
        assert config.retry_backoff_seconds == 2
        assert config.timeout_seconds == 60
    
    def test_base_config_from_dict(self):
        """Test creating configuration from dictionary."""
        config_dict = {
            "handler_type": "test",
            "destination": "test-dest",
            "enabled": False,
            "max_retries": 10
        }
        
        config = OutputHandlerConfigBase.from_dict(config_dict)
        
        assert config.handler_type == "test"
        assert config.destination == "test-dest"
        assert config.enabled is False
        assert config.max_retries == 10
        # Check defaults
        assert config.retry_backoff_seconds == 1
        assert config.timeout_seconds == 30
    
    def test_base_config_to_dict(self):
        """Test converting configuration to dictionary."""
        config = OutputHandlerConfigBase(
            handler_type="test",
            destination="test-dest"
        )
        
        config_dict = config.to_dict()
        
        assert config_dict == {
            "handler_type": "test",
            "destination": "test-dest",
            "enabled": True,
            "max_retries": 3,
            "retry_backoff_seconds": 1,
            "timeout_seconds": 30
        }


class TestQueueOutputHandlerConfig:
    """Test Queue output handler configuration."""
    
    def test_queue_config_defaults(self):
        """Test queue configuration with defaults."""
        config = QueueOutputHandlerConfig(destination="test-queue")
        
        assert config.handler_type == "queue"
        assert config.destination == "test-queue"
        assert config.connection_string is None  # Will be loaded from env in __post_init__
        assert config.auto_create_queue is True
        assert config.message_ttl_seconds == 604800
        assert config.visibility_timeout_seconds == 30
    
    @patch.dict(os.environ, {"AZURE_STORAGE_CONNECTION_STRING": "DefaultEndpointsProtocol=https;..."})
    def test_queue_config_env_loading(self):
        """Test loading connection string from environment."""
        config = QueueOutputHandlerConfig(destination="test-queue")
        
        assert config.connection_string == "DefaultEndpointsProtocol=https;..."
    
    @patch.dict(os.environ, {"AzureWebJobsStorage": "UseDevelopmentStorage=true"})
    def test_queue_config_alt_env_loading(self):
        """Test loading connection string from alternative environment variable."""
        config = QueueOutputHandlerConfig(destination="test-queue")
        
        assert config.connection_string == "UseDevelopmentStorage=true"
    
    def test_queue_config_explicit_connection_string(self):
        """Test explicit connection string takes precedence."""
        config = QueueOutputHandlerConfig(
            destination="test-queue",
            connection_string="explicit-connection"
        )
        
        assert config.connection_string == "explicit-connection"
    
    def test_queue_config_to_dict(self):
        """Test converting queue configuration to dictionary."""
        config = QueueOutputHandlerConfig(
            destination="test-queue",
            connection_string="test-connection",
            auto_create_queue=False,
            message_ttl_seconds=3600
        )
        
        config_dict = config.to_dict()
        
        assert config_dict["handler_type"] == "queue"
        assert config_dict["destination"] == "test-queue"
        assert config_dict["connection_string"] == "test-connection"
        assert config_dict["auto_create_queue"] is False
        assert config_dict["message_ttl_seconds"] == 3600


class TestServiceBusOutputHandlerConfig:
    """Test Service Bus output handler configuration."""
    
    def test_service_bus_config_defaults(self):
        """Test service bus configuration with defaults."""
        config = ServiceBusOutputHandlerConfig(destination="test-topic")
        
        assert config.handler_type == "service_bus"
        assert config.destination == "test-topic"
        assert config.connection_string is None
        assert config.create_queue_if_not_exists is True
        assert config.message_time_to_live is None
        assert config.scheduled_enqueue_time_utc is None
        assert config.session_id is None
    
    @patch.dict(os.environ, {"AZURE_SERVICE_BUS_CONNECTION_STRING": "Endpoint=sb://..."})
    def test_service_bus_config_env_loading(self):
        """Test loading connection string from environment."""
        config = ServiceBusOutputHandlerConfig(destination="test-topic")
        
        assert config.connection_string == "Endpoint=sb://..."
    
    def test_service_bus_config_with_options(self):
        """Test service bus configuration with all options."""
        config = ServiceBusOutputHandlerConfig(
            destination="test-topic",
            connection_string="test-connection",
            create_queue_if_not_exists=False,
            message_time_to_live=300,
            scheduled_enqueue_time_utc="2024-01-01T12:00:00Z",
            session_id="session-123"
        )
        
        assert config.connection_string == "test-connection"
        assert config.create_queue_if_not_exists is False
        assert config.message_time_to_live == 300
        assert config.scheduled_enqueue_time_utc == "2024-01-01T12:00:00Z"
        assert config.session_id == "session-123"


class TestFileOutputHandlerConfig:
    """Test File output handler configuration."""
    
    def test_file_config_defaults(self):
        """Test file configuration with defaults."""
        config = FileOutputHandlerConfig(destination="output.json")
        
        assert config.handler_type == "file"
        assert config.destination == "output.json"
        assert config.base_path == "./output"  # Default from __post_init__
        assert config.create_directories is True
        assert config.output_format == "json"
        assert config.append_mode is True
        assert config.file_permissions == 0o644
    
    @patch.dict(os.environ, {"FILE_OUTPUT_BASE_PATH": "/var/data/output"})
    def test_file_config_env_loading(self):
        """Test loading base path from environment."""
        config = FileOutputHandlerConfig(destination="data.json")
        
        assert config.base_path == "/var/data/output"
    
    def test_file_config_with_options(self):
        """Test file configuration with all options."""
        config = FileOutputHandlerConfig(
            destination="output.csv",
            base_path="/custom/path",
            create_directories=False,
            output_format="csv",
            append_mode=True,
            file_permissions=0o600
        )
        
        assert config.base_path == "/custom/path"
        assert config.create_directories is False
        assert config.output_format == "csv"
        assert config.append_mode is True
        assert config.file_permissions == 0o600


class TestNoOpOutputHandlerConfig:
    """Test NoOp output handler configuration."""
    
    def test_noop_config_defaults(self):
        """Test no-op configuration with defaults."""
        config = NoOpOutputHandlerConfig(destination="noop")
        
        assert config.handler_type == "noop"
        assert config.destination == "noop"
        assert config.log_level == "INFO"
        assert config.include_metrics is True
    
    def test_noop_config_with_options(self):
        """Test no-op configuration with options."""
        config = NoOpOutputHandlerConfig(
            destination="test-complete",
            log_level="DEBUG",
            include_metrics=False
        )
        
        assert config.log_level == "DEBUG"
        assert config.include_metrics is False


class TestOutputHandlerConfigFactory:
    """Test the configuration factory."""
    
    def test_create_queue_config(self):
        """Test creating queue configuration through factory."""
        config = OutputHandlerConfigFactory.create_config(
            "queue",
            "test-queue",
            {"connection_string": "test-conn", "auto_create_queue": False}
        )
        
        assert isinstance(config, QueueOutputHandlerConfig)
        assert config.destination == "test-queue"
        assert config.connection_string == "test-conn"
        assert config.auto_create_queue is False
    
    def test_create_service_bus_config(self):
        """Test creating service bus configuration through factory."""
        config = OutputHandlerConfigFactory.create_config(
            "service_bus",
            "test-topic"
        )
        
        assert isinstance(config, ServiceBusOutputHandlerConfig)
        assert config.destination == "test-topic"
    
    def test_create_file_config(self):
        """Test creating file configuration through factory."""
        config = OutputHandlerConfigFactory.create_config(
            "file",
            "output.json",
            {"base_path": "/data", "output_format": "json"}
        )
        
        assert isinstance(config, FileOutputHandlerConfig)
        assert config.destination == "output.json"
        assert config.base_path == "/data"
        assert config.output_format == "json"
    
    def test_create_noop_config(self):
        """Test creating no-op configuration through factory."""
        config = OutputHandlerConfigFactory.create_config(
            "noop",
            "test-complete"
        )
        
        assert isinstance(config, NoOpOutputHandlerConfig)
        assert config.destination == "test-complete"
    
    def test_unsupported_handler_type(self):
        """Test error on unsupported handler type."""
        with pytest.raises(ValueError) as exc_info:
            OutputHandlerConfigFactory.create_config(
                "unsupported",
                "test"
            )
        
        assert "Unsupported handler type: unsupported" in str(exc_info.value)
    
    def test_return_existing_config_object(self):
        """Test that existing config objects are returned as-is."""
        existing_config = QueueOutputHandlerConfig(
            destination="test-queue",
            connection_string="test-conn"
        )
        
        config = OutputHandlerConfigFactory.create_config(
            "queue",
            "ignored",
            existing_config
        )
        
        assert config is existing_config
    
    @patch.dict(os.environ, {
        "TEST_QUEUE_CONNECTION_STRING": "test-connection",
        "TEST_QUEUE_AUTO_CREATE_QUEUE": "false",
        "TEST_QUEUE_MESSAGE_TTL_SECONDS": "3600",
        "TEST_QUEUE_ENABLED": "true",
        "TEST_QUEUE_MAX_RETRIES": "5"
    })
    def test_from_env_prefix(self):
        """Test creating configuration from environment variables with prefix."""
        config = OutputHandlerConfigFactory.from_env_prefix(
            "queue",
            "test-queue",
            "TEST_QUEUE_"
        )
        
        assert isinstance(config, QueueOutputHandlerConfig)
        assert config.connection_string == "test-connection"
        assert config.auto_create_queue is False
        assert config.message_ttl_seconds == 3600
        assert config.enabled is True
        assert config.max_retries == 5
    
    @patch.dict(os.environ, {
        "APP_FILE_BASE_PATH": "/app/data",
        "APP_FILE_CREATE_DIRECTORIES": "yes",
        "APP_FILE_OUTPUT_FORMAT": "csv",
        "APP_FILE_APPEND_MODE": "1",
        "APP_FILE_FILE_PERMISSIONS": "0600"
    })
    def test_from_env_prefix_file_handler(self):
        """Test creating file handler configuration from environment."""
        config = OutputHandlerConfigFactory.from_env_prefix(
            "file",
            "output.csv",
            "APP_FILE_"
        )
        
        assert isinstance(config, FileOutputHandlerConfig)
        assert config.base_path == "/app/data"
        assert config.create_directories is True
        assert config.output_format == "csv"
        assert config.append_mode is True
        assert config.file_permissions == 0o600
    
    def test_register_custom_handler_config(self):
        """Test registering a custom handler configuration."""
        @dataclass
        class CustomOutputHandlerConfig(OutputHandlerConfigBase):
            custom_field: str = "default"
            
            def __post_init__(self):
                super().__post_init__() if hasattr(super(), '__post_init__') else None
                self.handler_type = "custom"
        
        OutputHandlerConfigFactory.register_handler_config(
            "custom",
            CustomOutputHandlerConfig
        )
        
        config = OutputHandlerConfigFactory.create_config(
            "custom",
            "custom-dest",
            {"custom_field": "test-value"}
        )
        
        assert isinstance(config, CustomOutputHandlerConfig)
        assert config.custom_field == "test-value"
    
    def test_register_invalid_config_class(self):
        """Test error when registering invalid config class."""
        class NotAConfig:
            pass
        
        with pytest.raises(ValueError) as exc_info:
            OutputHandlerConfigFactory.register_handler_config(
                "invalid",
                NotAConfig
            )
        
        assert "must inherit from OutputHandlerConfigBase" in str(exc_info.value)


class TestOutputHandlerConfigManager:
    """Test the configuration manager."""
    
    def test_add_and_get_config(self):
        """Test adding and retrieving configurations."""
        manager = OutputHandlerConfigManager()
        
        manager.add_config(
            "primary_queue",
            "queue",
            "output-queue",
            {"connection_string": "test-conn"}
        )
        
        config = manager.get_config("primary_queue")
        assert isinstance(config, QueueOutputHandlerConfig)
        assert config.destination == "output-queue"
        assert config.connection_string == "test-conn"
    
    def test_remove_config(self):
        """Test removing configurations."""
        manager = OutputHandlerConfigManager()
        
        manager.add_config("test", "noop", "test-dest")
        assert manager.get_config("test") is not None
        
        assert manager.remove_config("test") is True
        assert manager.get_config("test") is None
        
        # Removing non-existent config returns False
        assert manager.remove_config("non-existent") is False
    
    def test_list_configs(self):
        """Test listing all configurations."""
        manager = OutputHandlerConfigManager()
        
        manager.add_config("queue1", "queue", "queue-1")
        manager.add_config("file1", "file", "output.json")
        manager.add_config("noop1", "noop", "test")
        
        configs = manager.list_configs()
        assert len(configs) == 3
        assert "queue1" in configs
        assert "file1" in configs
        assert "noop1" in configs
    
    def test_load_from_dict(self):
        """Test loading configurations from dictionary."""
        manager = OutputHandlerConfigManager()
        
        configs_dict = {
            "primary_queue": {
                "handler_type": "queue",
                "destination": "output-queue",
                "connection_string": "test-conn",
                "auto_create_queue": True
            },
            "backup_file": {
                "handler_type": "file",
                "destination": "backup.json",
                "base_path": "/backup",
                "output_format": "json"
            },
            "audit_log": {
                "handler_type": "noop",
                "destination": "audit"
            }
        }
        
        manager.load_from_dict(configs_dict)
        
        assert len(manager.list_configs()) == 3
        
        queue_config = manager.get_config("primary_queue")
        assert isinstance(queue_config, QueueOutputHandlerConfig)
        assert queue_config.connection_string == "test-conn"
        
        file_config = manager.get_config("backup_file")
        assert isinstance(file_config, FileOutputHandlerConfig)
        assert file_config.base_path == "/backup"
    
    def test_load_from_dict_skip_invalid(self):
        """Test that invalid configurations are skipped."""
        manager = OutputHandlerConfigManager()
        
        configs_dict = {
            "valid": {
                "handler_type": "queue",
                "destination": "test-queue"
            },
            "missing_type": {
                "destination": "test"
            },
            "missing_destination": {
                "handler_type": "queue"
            }
        }
        
        manager.load_from_dict(configs_dict)
        
        # Only valid config should be loaded
        assert len(manager.list_configs()) == 1
        assert manager.get_config("valid") is not None
    
    def test_load_from_json_file(self):
        """Test loading configurations from JSON file."""
        manager = OutputHandlerConfigManager()
        
        # Create temporary JSON file
        configs_data = {
            "test_queue": {
                "handler_type": "queue",
                "destination": "test-queue",
                "message_ttl_seconds": 3600
            }
        }
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            json.dump(configs_data, f)
            temp_file = f.name
        
        try:
            manager.load_from_json_file(temp_file)
            
            config = manager.get_config("test_queue")
            assert config is not None
            assert config.message_ttl_seconds == 3600
            
        finally:
            os.unlink(temp_file)
    
    def test_to_dict(self):
        """Test exporting configurations to dictionary."""
        manager = OutputHandlerConfigManager()
        
        manager.add_config("queue1", "queue", "test-queue")
        manager.add_config("file1", "file", "output.json", {"output_format": "csv"})
        
        export_dict = manager.to_dict()
        
        assert len(export_dict) == 2
        assert export_dict["queue1"]["handler_type"] == "queue"
        assert export_dict["queue1"]["destination"] == "test-queue"
        assert export_dict["file1"]["output_format"] == "csv"
    
    def test_validate_all(self):
        """Test validation of all configurations."""
        manager = OutputHandlerConfigManager()
        
        # Add valid configurations
        manager.add_config(
            "valid_queue",
            "queue",
            "test-queue",
            {"connection_string": "test-conn"}
        )
        
        manager.add_config(
            "valid_file",
            "file",
            "output.json",
            {"base_path": "/data"}
        )
        
        # Add invalid configuration (manually create to bypass validation)
        invalid_queue = QueueOutputHandlerConfig(destination="")
        invalid_queue.connection_string = None
        manager._configs["invalid_queue"] = invalid_queue
        
        validation_results = manager.validate_all()
        
        assert validation_results["valid_queue"] is True
        assert validation_results["valid_file"] is True
        assert validation_results["invalid_queue"] is False
    
    def test_validate_missing_connection_string(self):
        """Test validation catches missing connection strings."""
        manager = OutputHandlerConfigManager()
        
        # Create configs without connection strings
        queue_config = QueueOutputHandlerConfig(destination="test-queue")
        queue_config.connection_string = None  # Force None
        manager._configs["queue"] = queue_config
        
        sb_config = ServiceBusOutputHandlerConfig(destination="test-topic")
        sb_config.connection_string = None  # Force None
        manager._configs["service_bus"] = sb_config
        
        validation_results = manager.validate_all()
        
        assert validation_results["queue"] is False
        assert validation_results["service_bus"] is False
    
    def test_validate_missing_base_path(self):
        """Test validation catches missing base path for file handler."""
        manager = OutputHandlerConfigManager()
        
        file_config = FileOutputHandlerConfig(destination="output.json")
        file_config.base_path = None  # Force None
        manager._configs["file"] = file_config
        
        validation_results = manager.validate_all()
        
        assert validation_results["file"] is False