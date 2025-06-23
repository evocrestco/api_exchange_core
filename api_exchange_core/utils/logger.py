"""
Simplified logging solution that fixes both console and queue logging.

This module provides a clean implementation that:
1. Uses ContextAwareLogger for console logs (with pipe-delimited extras)
2. Uses AzureQueueHandler for structured queue logs
"""

import logging
import os
import sys
import traceback
from datetime import datetime
from typing import Any, Dict, List, Optional, Union

from azure.storage.queue import QueueClient, QueueServiceClient

from ..config import get_config
from .json_utils import dumps

_function_logger = None


class ContextAwareLogger:
    """
    Logger wrapper that formats extra attributes in message while preserving them.

    This ensures extras appear in console output even when Azure Functions
    overrides the formatters.
    """

    def __init__(self, logger):
        """Initialize with an existing logger."""
        self.logger = logger

    def _log_with_formatted_extra(self, level, msg, **kwargs):
        """
        Log with extra data formatted into the message.

        Args:
            level: Logging level method to use
            msg: Log message
            **kwargs: Additional arguments including 'extra'
        """
        # Extract extra if present
        extra = kwargs.pop("extra", {})

        # Format extra as string
        if extra:
            extra_str = " | ".join([f"{k}={v}" for k, v in extra.items()])
            full_msg = f"{msg} | {extra_str}"
        else:
            full_msg = msg

        # Call the underlying logger method with proper parameters
        log_method = getattr(self.logger, level)
        log_method(full_msg, extra=extra)

    def set_level(self, level):
        """Set the logging level of the underlying logger."""
        self.logger.setLevel(level)

    def info(self, msg, **kwargs):
        """Log at INFO level with formatted extra."""
        self._log_with_formatted_extra("info", msg, **kwargs)

    def error(self, msg, **kwargs):
        """Log at ERROR level with formatted extra."""
        self._log_with_formatted_extra("error", msg, **kwargs)

    def warning(self, msg, **kwargs):
        """Log at WARNING level with formatted extra."""
        self._log_with_formatted_extra("warning", msg, **kwargs)

    def debug(self, msg, **kwargs):
        """Log at DEBUG level with formatted extra."""
        self._log_with_formatted_extra("debug", msg, **kwargs)

    def exception(self, msg, **kwargs):
        """Log exception with formatted extra."""
        self._log_with_formatted_extra("exception", msg, **kwargs)


class TenantContextFilter(logging.Filter):
    """
    Logging filter that adds tenant context information to log records.
    """

    def filter(self, record):
        """
        Add tenant_id to the log record if available in the current context.

        Args:
            record: LogRecord to modify

        Returns:
            True to include the record in the log output
        """
        # Lazy import to avoid circular dependency
        from ..context.tenant_context import TenantContext
        
        tenant_id = TenantContext.get_current_tenant_id()
        if tenant_id:
            record.tenant_id = tenant_id

        return True


class AzureQueueHandler(logging.Handler):
    """
    Custom logging handler that sends logs to Azure Storage Queue.

    This handler captures log entries, converts them to LogEntry models,
    and sends them to an Azure Storage Queue.
    """

    def __init__(
        self,
        queue_name: str = "logs-queue",
        connection_string: Optional[str] = None,
        batch_size: int = 10,
    ):
        """
        Initialize the Azure Queue handler.

        Args:
            queue_name: Name of the queue to send logs to
            connection_string: Azure Storage connection string
            batch_size: Number of logs to batch before sending
        """
        super().__init__()
        self.queue_name = queue_name
        self.connection_string = connection_string or os.getenv("AzureWebJobsStorage")
        self.batch_size = batch_size
        self.log_buffer: List[Dict[str, Any]] = []

        # Check connection string
        if not self.connection_string:
            sys.stderr.write("Azure Storage connection string not provided\n")

        # Try to ensure queue exists
        try:
            self._ensure_queue_exists()
        except Exception as e:
            sys.stderr.write(f"Failed to ensure queue exists: {str(e)}\n")

    def _ensure_queue_exists(self) -> bool:
        """
        Ensure the specified queue exists, creating it if necessary.

        Returns:
            True if the queue exists or was created successfully
        """
        if not self.connection_string:
            return False

        try:
            # Create queue service client
            queue_service = QueueServiceClient.from_connection_string(self.connection_string)

            # Check if queue exists
            queues = queue_service.list_queues()
            queue_exists = any(queue.name == self.queue_name for queue in queues)

            if not queue_exists:
                sys.stderr.write(f"Queue '{self.queue_name}' does not exist. Creating...\n")
                queue_service.create_queue(self.queue_name)
                sys.stderr.write(f"Queue '{self.queue_name}' created successfully\n")

            return True

        except Exception as e:
            sys.stderr.write(f"Failed to ensure queue exists: {str(e)}\n")
            return False

    def emit(self, record: logging.LogRecord) -> None:
        """
        Queue a log record for sending to Azure Queue.

        Args:
            record: LogRecord to send
        """
        try:
            # Create a dictionary representation of the log entry
            log_entry = {
                "timestamp": datetime.fromtimestamp(record.created).isoformat(),
                "level": record.levelname,
                "logger": record.name,
                "message": record.getMessage(),  # Use the raw message
                "module": record.module,
                "function": record.funcName,
                "line": record.lineno,
            }

            # Add common fields if present
            for field in ["tenant_id", "entity_id"]:
                if hasattr(record, field):
                    log_entry[field] = getattr(record, field)

            # Add context with all extra fields
            context = {}
            for key, value in record.__dict__.items():
                if (
                    key.startswith("_")
                    and not key.startswith("__")
                    and not callable(value)
                    and value is not None
                ):
                    # Custom fields with underscore
                    context[key[1:]] = value
                elif (
                    key
                    not in [
                        "name",
                        "msg",
                        "args",
                        "levelname",
                        "levelno",
                        "pathname",
                        "filename",
                        "module",
                        "exc_info",
                        "exc_text",
                        "stack_info",
                        "lineno",
                        "funcName",
                        "created",
                        "msecs",
                        "relativeCreated",
                        "thread",
                        "threadName",
                        "processName",
                        "process",
                        "tenant_id",
                        "entity_id",
                    ]
                    and not key.startswith("__")
                    and not callable(value)
                ):
                    # Other non-standard fields
                    context[key] = value

            if context:
                log_entry["context"] = context

            # Add exception info if present
            if record.exc_info and record.exc_info[0]:
                log_entry["exception"] = {
                    "type": record.exc_info[0].__name__,
                    "message": str(record.exc_info[1]),
                    "traceback": [
                        line.rstrip() for line in traceback.format_exception(*record.exc_info)
                    ],
                }

            # Add to buffer
            self.log_buffer.append(log_entry)

            # Send if buffer is full
            if len(self.log_buffer) >= self.batch_size:
                self.flush()

        except Exception:
            self.handleError(record)

    def flush(self) -> None:
        """Send any buffered log records to the queue."""
        if not self.log_buffer:
            return

        if not self.connection_string:
            return

        try:
            # Create queue client
            queue_client = QueueClient.from_connection_string(
                conn_str=self.connection_string, queue_name=self.queue_name
            )

            # Send individual log entries (like metrics) to avoid base64 encoding bug
            for log_entry in self.log_buffer:
                try:
                    json_data = dumps(log_entry)
                    queue_client.send_message(json_data)
                except Exception as log_error:
                    sys.stderr.write(f"Error sending individual log entry: {str(log_error)}\n")

            # Clear buffer
            self.log_buffer.clear()

        except Exception as e:
            sys.stderr.write(f"Error sending logs to Azure Queue: {str(e)}\n")

    def close(self) -> None:
        """Flush any remaining logs before closing."""
        self.flush()
        super().close()


def configure_logging(
    function_name: str,
    log_level: Optional[Union[int, str]] = None,
    enable_queue: Optional[bool] = None,
    queue_name: Optional[str] = None,
    queue_batch_size: int = 10,
    connection_string: Optional[str] = None,
) -> "ContextAwareLogger":
    """
    Configure logging with console and optional queue output.

    Args:
        function_name: Name of the function
        log_level: Logging level (default: INFO)
        enable_queue: Whether to enable Azure Queue logging (default: from config.features.enable_logs_queue)
        queue_name: Name of the queue to send logs to (default: "logs-queue")
        queue_batch_size: Number of logs to batch before sending (default: 10)
        connection_string: Azure Storage connection string (default: from env)

    Returns:
        The configured logger wrapped with ContextAwareLogger
    """
    global _function_logger

    # Get configuration
    app_config = get_config()

    # Use provided values or fall back to config
    if log_level is None:
        log_level = app_config.logging.level
    if enable_queue is None:
        enable_queue = app_config.features.enable_logs_queue
    if connection_string is None:
        connection_string = app_config.queue.connection_string

    # Convert string log level to integer if necessary
    if isinstance(log_level, str):
        log_level = getattr(logging, log_level.upper(), logging.INFO)

    # Create function logger
    logger_name = f"function.{function_name}"
    logger = logging.getLogger(logger_name)
    logger.setLevel(log_level)

    # Remove existing handlers
    for handler in logger.handlers[:]:
        logger.removeHandler(handler)

    # Create console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(log_level)
    console_handler.setFormatter(logging.Formatter("%(message)s"))  # Simple format

    # Add tenant context filter
    tenant_filter = TenantContextFilter()
    console_handler.addFilter(tenant_filter)

    # Add handler to function logger
    logger.addHandler(console_handler)

    # Add Azure Queue handler if enabled
    if enable_queue:
        queue_name = queue_name or "logs-queue"
        queue_handler = AzureQueueHandler(
            queue_name=queue_name, connection_string=connection_string, batch_size=queue_batch_size
        )
        queue_handler.setLevel(log_level)
        queue_handler.addFilter(tenant_filter)
        logger.addHandler(queue_handler)

    # Wrap with context-aware logger
    wrapped_logger = ContextAwareLogger(logger)

    # Log initialization
    wrapped_logger.info(
        "Function logger configured",
        extra={
            "function_name": function_name,
            "queue_logging": enable_queue,
            "queue_name": queue_name if enable_queue else None,
        },
    )
    _function_logger = wrapped_logger
    return wrapped_logger


def get_logger(
    log_level: Optional[Union[int, str]] = None,
) -> Union[logging.Logger, "ContextAwareLogger"]:
    """
    Get the function logger.

    Args:
        log_level: Optional log level to set

    Returns:
        Logger instance
    """
    if _function_logger is not None:
        return _function_logger

    # Get the root logger as fallback
    logger = logging.getLogger()

    # Use centralized config if no log level provided
    if log_level is None:
        app_config = get_config()
        log_level = app_config.logging.level

    # Set log level if specified
    if log_level is not None:
        if isinstance(log_level, str):
            log_level = getattr(logging, log_level.upper(), logging.INFO)
        logger.setLevel(log_level)

    # Wrap with ContextAwareLogger if not already wrapped
    if not isinstance(logger, ContextAwareLogger):
        return ContextAwareLogger(logger)

    # Already wrapped, return as is
    return logger
