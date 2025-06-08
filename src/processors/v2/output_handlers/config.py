"""
Configuration management for output handlers.

This module provides a centralized configuration system for output handlers,
supporting loading from environment variables, configuration dictionaries,
and providing sensible defaults.
"""

import json
import os
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any, Dict, Optional, Union

from src.utils.logger import get_logger

logger = get_logger()


@dataclass
class OutputHandlerConfigBase:
    """Base configuration for all output handlers."""

    handler_type: str = ""
    destination: str = ""
    enabled: bool = True
    max_retries: int = 3
    retry_backoff_seconds: int = 1
    timeout_seconds: int = 30

    @classmethod
    def from_dict(cls, config_dict: Dict[str, Any]) -> "OutputHandlerConfigBase":
        """Create configuration from dictionary."""
        # Filter config_dict to only include fields that the class accepts
        import inspect

        sig = inspect.signature(cls)
        valid_params = set(sig.parameters.keys())
        filtered_config = {k: v for k, v in config_dict.items() if k in valid_params}
        return cls(**filtered_config)

    def to_dict(self) -> Dict[str, Any]:
        """Convert configuration to dictionary."""
        return {
            "handler_type": self.handler_type,
            "destination": self.destination,
            "enabled": self.enabled,
            "max_retries": self.max_retries,
            "retry_backoff_seconds": self.retry_backoff_seconds,
            "timeout_seconds": self.timeout_seconds,
        }


@dataclass
class QueueOutputHandlerConfig(OutputHandlerConfigBase):
    """Configuration for Azure Storage Queue output handler."""

    connection_string: Optional[str] = None
    auto_create_queue: bool = True
    message_ttl_seconds: int = 604800  # 7 days
    visibility_timeout_seconds: int = 30

    def __post_init__(self):
        """Set handler type and load connection string from environment if not provided."""
        if not hasattr(self, "handler_type") or not self.handler_type:
            self.handler_type = "queue"

        if not self.connection_string:
            # Try multiple environment variable names
            for env_var in ["AZURE_STORAGE_CONNECTION_STRING", "AzureWebJobsStorage"]:
                conn_str = os.getenv(env_var)
                if conn_str:
                    self.connection_string = conn_str
                    logger.debug(f"Loaded connection string from {env_var}")
                    break

    def to_dict(self) -> Dict[str, Any]:
        """Convert configuration to dictionary."""
        config = super().to_dict()
        config.update(
            {
                "connection_string": self.connection_string,
                "auto_create_queue": self.auto_create_queue,
                "message_ttl_seconds": self.message_ttl_seconds,
                "visibility_timeout_seconds": self.visibility_timeout_seconds,
            }
        )
        return config


@dataclass
class ServiceBusOutputHandlerConfig(OutputHandlerConfigBase):
    """Configuration for Azure Service Bus output handler."""

    connection_string: Optional[str] = None
    create_queue_if_not_exists: bool = True
    message_time_to_live: Optional[int] = None
    scheduled_enqueue_time_utc: Optional[str] = None
    session_id: Optional[str] = None

    def __post_init__(self):
        """Set handler type and load connection string from environment if not provided."""
        if not hasattr(self, "handler_type") or not self.handler_type:
            self.handler_type = "service_bus"

        if not self.connection_string:
            # Try multiple environment variable names
            for env_var in [
                "AZURE_SERVICE_BUS_CONNECTION_STRING",
                "AZURE_SERVICEBUS_CONNECTION_STRING",
                "ServiceBusConnectionString",
            ]:
                conn_str = os.getenv(env_var)
                if conn_str:
                    self.connection_string = conn_str
                    logger.debug(f"Loaded connection string from {env_var}")
                    break

    def to_dict(self) -> Dict[str, Any]:
        """Convert configuration to dictionary."""
        config = super().to_dict()
        config.update(
            {
                "connection_string": self.connection_string,
                "create_queue_if_not_exists": self.create_queue_if_not_exists,
                "message_time_to_live": self.message_time_to_live,
                "scheduled_enqueue_time_utc": self.scheduled_enqueue_time_utc,
                "session_id": self.session_id,
            }
        )
        return config


@dataclass
class FileOutputHandlerConfig(OutputHandlerConfigBase):
    """Configuration for file system output handler."""

    base_path: Optional[str] = None
    create_directories: bool = True
    output_format: str = "json"  # json, text, csv
    append_mode: bool = True
    file_permissions: int = 0o644

    def __post_init__(self):
        """Set handler type and default base path if not provided."""
        if not hasattr(self, "handler_type") or not self.handler_type:
            self.handler_type = "file"

        if not self.base_path:
            # Use environment variable or default to current directory
            self.base_path = os.getenv("FILE_OUTPUT_BASE_PATH", "./output")
            logger.debug(f"Using base path: {self.base_path}")

    def to_dict(self) -> Dict[str, Any]:
        """Convert configuration to dictionary."""
        config = super().to_dict()
        config.update(
            {
                "base_path": self.base_path,
                "create_directories": self.create_directories,
                "output_format": self.output_format,
                "append_mode": self.append_mode,
                "file_permissions": self.file_permissions,
            }
        )
        return config


@dataclass
class NoOpOutputHandlerConfig(OutputHandlerConfigBase):
    """Configuration for no-op output handler."""

    log_level: str = "INFO"  # Logging level for no-op operations
    include_metrics: bool = True

    def __post_init__(self):
        """Set handler type."""
        if not hasattr(self, "handler_type") or not self.handler_type:
            self.handler_type = "noop"

    def to_dict(self) -> Dict[str, Any]:
        """Convert configuration to dictionary."""
        config = super().to_dict()
        config.update({"log_level": self.log_level, "include_metrics": self.include_metrics})
        return config


class OutputHandlerConfigFactory:
    """Factory for creating output handler configurations."""

    _handler_configs = {
        "queue": QueueOutputHandlerConfig,
        "service_bus": ServiceBusOutputHandlerConfig,
        "file": FileOutputHandlerConfig,
        "noop": NoOpOutputHandlerConfig,
    }

    @classmethod
    def create_config(
        cls,
        handler_type: str,
        destination: str,
        config: Optional[Union[Dict[str, Any], OutputHandlerConfigBase]] = None,
    ) -> OutputHandlerConfigBase:
        """
        Create an output handler configuration.

        Args:
            handler_type: Type of output handler (queue, service_bus, file, noop)
            destination: Destination for the output (queue name, file path, etc.)
            config: Optional configuration dict or existing config object

        Returns:
            Appropriate configuration object for the handler type

        Raises:
            ValueError: If handler_type is not supported
        """
        if handler_type not in cls._handler_configs:
            raise ValueError(
                f"Unsupported handler type: {handler_type}. "
                f"Supported types: {list(cls._handler_configs.keys())}"
            )

        config_class = cls._handler_configs[handler_type]

        # If config is already a configuration object, return it
        if isinstance(config, OutputHandlerConfigBase):
            return config

        # Create configuration from dict or defaults
        if config:
            # Merge with defaults
            config_dict = {"handler_type": handler_type, "destination": destination, **config}
            # Filter config_dict to only include fields that the class accepts
            import inspect

            sig = inspect.signature(config_class)
            valid_params = set(sig.parameters.keys())
            filtered_config = {k: v for k, v in config_dict.items() if k in valid_params}
            return config_class(**filtered_config)
        else:
            # Use defaults
            return config_class(destination=destination)

    @classmethod
    def from_env_prefix(
        cls, handler_type: str, destination: str, env_prefix: str
    ) -> OutputHandlerConfigBase:
        """
        Create configuration from environment variables with a prefix.

        For example, with prefix "QUEUE_OUTPUT_":
        - QUEUE_OUTPUT_CONNECTION_STRING
        - QUEUE_OUTPUT_AUTO_CREATE_QUEUE
        - QUEUE_OUTPUT_MESSAGE_TTL_SECONDS

        Args:
            handler_type: Type of output handler
            destination: Destination for the output
            env_prefix: Prefix for environment variables

        Returns:
            Configuration object with values from environment
        """
        config_dict = {"destination": destination}

        # Map of config field names to environment variable suffixes
        env_mappings = {
            # Common fields
            "enabled": "ENABLED",
            "max_retries": "MAX_RETRIES",
            "retry_backoff_seconds": "RETRY_BACKOFF_SECONDS",
            "timeout_seconds": "TIMEOUT_SECONDS",
            # Queue specific
            "connection_string": "CONNECTION_STRING",
            "auto_create_queue": "AUTO_CREATE_QUEUE",
            "message_ttl_seconds": "MESSAGE_TTL_SECONDS",
            "visibility_timeout_seconds": "VISIBILITY_TIMEOUT_SECONDS",
            # Service Bus specific
            "create_queue_if_not_exists": "CREATE_QUEUE_IF_NOT_EXISTS",
            "message_time_to_live": "MESSAGE_TIME_TO_LIVE",
            "scheduled_enqueue_time_utc": "SCHEDULED_ENQUEUE_TIME_UTC",
            "session_id": "SESSION_ID",
            # File specific
            "base_path": "BASE_PATH",
            "create_directories": "CREATE_DIRECTORIES",
            "output_format": "OUTPUT_FORMAT",
            "append_mode": "APPEND_MODE",
            "file_permissions": "FILE_PERMISSIONS",
            # NoOp specific
            "log_level": "LOG_LEVEL",
            "include_metrics": "INCLUDE_METRICS",
        }

        # Load values from environment
        for field_name, env_suffix in env_mappings.items():
            env_var = f"{env_prefix}{env_suffix}"
            value = os.getenv(env_var)

            if value is not None:
                # Convert string values to appropriate types
                if field_name in [
                    "enabled",
                    "auto_create_queue",
                    "create_queue_if_not_exists",
                    "create_directories",
                    "append_mode",
                    "include_metrics",
                ]:
                    # Boolean fields
                    config_dict[field_name] = value.lower() in ["true", "1", "yes", "on"]
                elif field_name in [
                    "max_retries",
                    "retry_backoff_seconds",
                    "timeout_seconds",
                    "message_ttl_seconds",
                    "visibility_timeout_seconds",
                    "message_time_to_live",
                ]:
                    # Integer fields
                    try:
                        config_dict[field_name] = int(value)
                    except ValueError:
                        logger.warning(f"Invalid integer value for {env_var}: {value}")
                elif field_name == "file_permissions":
                    # Octal permissions field
                    try:
                        # Handle both octal (0644) and decimal (644) formats
                        if value.startswith("0") and len(value) > 1:
                            config_dict[field_name] = int(value, 8)  # Parse as octal
                        else:
                            config_dict[field_name] = int(value)
                    except ValueError:
                        logger.warning(f"Invalid octal value for {env_var}: {value}")
                else:
                    # String fields
                    config_dict[field_name] = value

        return cls.create_config(handler_type, destination, config_dict)

    @classmethod
    def register_handler_config(cls, handler_type: str, config_class: type):
        """
        Register a custom output handler configuration class.

        Args:
            handler_type: Type identifier for the handler
            config_class: Configuration class (must inherit from OutputHandlerConfigBase)
        """
        if not issubclass(config_class, OutputHandlerConfigBase):
            raise ValueError(
                f"Config class must inherit from OutputHandlerConfigBase, got {config_class}"
            )

        cls._handler_configs[handler_type] = config_class
        logger.info(f"Registered configuration for handler type: {handler_type}")


class OutputHandlerConfigManager:
    """
    Manages output handler configurations for processors.

    This class provides a centralized way to manage multiple output handler
    configurations, supporting loading from various sources and providing
    validation.
    """

    def __init__(self):
        """Initialize the configuration manager."""
        self._configs: Dict[str, OutputHandlerConfigBase] = {}

    def add_config(
        self,
        name: str,
        handler_type: str,
        destination: str,
        config: Optional[Union[Dict[str, Any], OutputHandlerConfigBase]] = None,
    ) -> None:
        """
        Add an output handler configuration.

        Args:
            name: Unique name for this configuration
            handler_type: Type of output handler
            destination: Destination for the output
            config: Optional configuration
        """
        handler_config = OutputHandlerConfigFactory.create_config(handler_type, destination, config)
        self._configs[name] = handler_config

        logger.debug(f"Added output handler config '{name}': " f"{handler_type} -> {destination}")

    def get_config(self, name: str) -> Optional[OutputHandlerConfigBase]:
        """Get a configuration by name."""
        return self._configs.get(name)

    def remove_config(self, name: str) -> bool:
        """Remove a configuration by name."""
        if name in self._configs:
            del self._configs[name]
            return True
        return False

    def list_configs(self) -> Dict[str, OutputHandlerConfigBase]:
        """Get all configurations."""
        return self._configs.copy()

    def load_from_dict(self, configs_dict: Dict[str, Dict[str, Any]]) -> None:
        """
        Load multiple configurations from a dictionary.

        Expected format:
        {
            "primary_queue": {
                "handler_type": "queue",
                "destination": "output-queue",
                "connection_string": "...",
                "auto_create_queue": true
            },
            "backup_file": {
                "handler_type": "file",
                "destination": "/backup",
                "output_format": "json"
            }
        }
        """
        for name, config_data in configs_dict.items():
            if "handler_type" not in config_data or "destination" not in config_data:
                logger.warning(f"Skipping config '{name}': missing handler_type or destination")
                continue

            handler_type = config_data.pop("handler_type")
            destination = config_data.pop("destination")

            self.add_config(name, handler_type, destination, config_data)

    def load_from_json_file(self, file_path: str) -> None:
        """Load configurations from a JSON file."""
        try:
            with open(file_path, "r") as f:
                configs_dict = json.load(f)

            self.load_from_dict(configs_dict)
            logger.info(f"Loaded configurations from {file_path}")

        except Exception as e:
            logger.error(f"Failed to load configurations from {file_path}: {e}")
            raise

    def to_dict(self) -> Dict[str, Dict[str, Any]]:
        """Export all configurations to a dictionary."""
        result = {}
        for name, config in self._configs.items():
            result[name] = config.to_dict()
        return result

    def validate_all(self) -> Dict[str, bool]:
        """
        Validate all configurations.

        Returns:
            Dictionary mapping configuration names to validation status
        """
        results = {}

        for name, config in self._configs.items():
            # Basic validation
            is_valid = True

            # Check required fields
            if not config.destination:
                logger.warning(f"Config '{name}': missing destination")
                is_valid = False

            # Handler-specific validation
            if isinstance(config, (QueueOutputHandlerConfig, ServiceBusOutputHandlerConfig)):
                if not config.connection_string:
                    logger.warning(f"Config '{name}': missing connection string")
                    is_valid = False

            elif isinstance(config, FileOutputHandlerConfig):
                if not config.base_path:
                    logger.warning(f"Config '{name}': missing base path")
                    is_valid = False

            results[name] = is_valid

        return results
