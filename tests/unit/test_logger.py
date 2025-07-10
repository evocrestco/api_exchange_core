"""
Unit tests for logger utilities.

Tests the logging infrastructure including ContextAwareLogger and AzureQueueHandler.
"""

import json
import logging
import os
import sys
from datetime import datetime, timezone
from io import StringIO
from unittest.mock import Mock, patch, MagicMock, call
from typing import Dict, Any

import pytest
from azure.storage.queue import QueueClient, QueueServiceClient

from api_exchange_core.utils.logger import (
    ContextAwareLogger,
    AzureQueueHandler,
    configure_logging,
    get_logger,
    _function_logger
)


class TestContextAwareLogger:
    """Test ContextAwareLogger functionality."""

    def setup_method(self):
        """Set up test fixtures."""
        # Create a mock logger for testing
        self.mock_logger = Mock(spec=logging.Logger)
        self.context_logger = ContextAwareLogger(self.mock_logger)

    def test_init(self):
        """Test ContextAwareLogger initialization."""
        assert self.context_logger.logger == self.mock_logger

    def test_set_level(self):
        """Test setting log level."""
        self.context_logger.set_level(logging.DEBUG)
        self.mock_logger.setLevel.assert_called_once_with(logging.DEBUG)

    def test_log_with_formatted_extra_no_extras(self):
        """Test logging without extra data."""
        # Configure mock methods
        self.mock_logger.info = Mock()
        
        # Test logging without extras
        self.context_logger._log_with_formatted_extra("info", "Test message")
        
        # Verify logger was called with original message and empty extra
        self.mock_logger.info.assert_called_once_with("Test message", extra={})

    def test_log_with_formatted_extra_with_extras(self):
        """Test logging with extra data."""
        # Configure mock methods
        self.mock_logger.error = Mock()
        
        # Test logging with extras
        extra_data = {"user_id": "123", "action": "login", "ip": "192.168.1.1"}
        self.context_logger._log_with_formatted_extra(
            "error", "User login failed", extra=extra_data
        )
        
        # Verify logger was called with formatted message and structured extra
        expected_message = "User login failed | user_id=123 | action=login | ip=192.168.1.1"
        self.mock_logger.error.assert_called_once_with(expected_message, extra=extra_data)

    def test_log_with_formatted_extra_complex_values(self):
        """Test logging with complex extra values."""
        # Configure mock methods
        self.mock_logger.warning = Mock()
        
        # Test with complex values
        extra_data = {
            "count": 42,
            "active": True,
            "data": {"nested": "value"},
            "empty": None
        }
        self.context_logger._log_with_formatted_extra(
            "warning", "Complex data", extra=extra_data
        )
        
        # Verify formatted message includes all values
        args, kwargs = self.mock_logger.warning.call_args
        message = args[0]
        assert "count=42" in message
        assert "active=True" in message
        assert "data={'nested': 'value'}" in message
        assert "empty=None" in message
        assert kwargs["extra"] == extra_data

    def test_info_method(self):
        """Test info logging method."""
        self.mock_logger.info = Mock()
        
        self.context_logger.info("Info message", extra={"level": "info"})
        
        expected_message = "Info message | level=info"
        self.mock_logger.info.assert_called_once_with(
            expected_message, extra={"level": "info"}
        )

    def test_error_method(self):
        """Test error logging method."""
        self.mock_logger.error = Mock()
        
        self.context_logger.error("Error message", extra={"error_code": "E001"})
        
        expected_message = "Error message | error_code=E001"
        self.mock_logger.error.assert_called_once_with(
            expected_message, extra={"error_code": "E001"}
        )

    def test_warning_method(self):
        """Test warning logging method."""
        self.mock_logger.warning = Mock()
        
        self.context_logger.warning("Warning message")
        
        self.mock_logger.warning.assert_called_once_with("Warning message", extra={})

    def test_debug_method(self):
        """Test debug logging method."""
        self.mock_logger.debug = Mock()
        
        self.context_logger.debug("Debug message", extra={"debug": True})
        
        expected_message = "Debug message | debug=True"
        self.mock_logger.debug.assert_called_once_with(
            expected_message, extra={"debug": True}
        )

    def test_exception_method(self):
        """Test exception logging method."""
        self.mock_logger.exception = Mock()
        
        self.context_logger.exception("Exception occurred", extra={"trace": "stack"})
        
        expected_message = "Exception occurred | trace=stack"
        self.mock_logger.exception.assert_called_once_with(
            expected_message, extra={"trace": "stack"}
        )

    def test_additional_kwargs_passed_through(self):
        """Test that additional kwargs are passed through to logger."""
        self.mock_logger.info = Mock()
        
        self.context_logger.info(
            "Message with kwargs", 
            extra={"test": "data"},
            exc_info=True,
            stack_info=False
        )
        
        args, kwargs = self.mock_logger.info.call_args
        assert "exc_info" in kwargs
        assert "stack_info" in kwargs
        assert kwargs["exc_info"] is True
        assert kwargs["stack_info"] is False


class TestAzureQueueHandler:
    """Test AzureQueueHandler functionality."""

    def setup_method(self):
        """Set up test fixtures."""
        self.connection_string = "DefaultEndpointsProtocol=https;AccountName=test;AccountKey=key==;EndpointSuffix=core.windows.net"
        self.queue_name = "test-logs-queue"

    def test_init_with_connection_string(self):
        """Test handler initialization with connection string."""
        with patch.object(AzureQueueHandler, '_ensure_queue_exists', return_value=True):
            handler = AzureQueueHandler(
                queue_name=self.queue_name,
                connection_string=self.connection_string,
                batch_size=5
            )
            
            assert handler.queue_name == self.queue_name
            assert handler.connection_string == self.connection_string
            assert handler.batch_size == 5
            assert handler.log_buffer == []

    def test_init_without_connection_string(self):
        """Test handler initialization without connection string."""
        with patch.object(AzureQueueHandler, '_ensure_queue_exists', return_value=False), \
             patch('sys.stderr', new_callable=StringIO) as mock_stderr:
            
            handler = AzureQueueHandler(queue_name=self.queue_name)
            
            assert handler.connection_string is None
            assert "Azure Storage connection string not provided" in mock_stderr.getvalue()

    def test_init_with_env_connection_string(self):
        """Test handler initialization with connection string from environment."""
        with patch.dict(os.environ, {'AzureWebJobsStorage': self.connection_string}), \
             patch.object(AzureQueueHandler, '_ensure_queue_exists', return_value=True):
            
            handler = AzureQueueHandler(queue_name=self.queue_name)
            
            assert handler.connection_string == self.connection_string

    def test_init_ensure_queue_fails(self):
        """Test handler initialization when queue creation fails."""
        with patch.object(AzureQueueHandler, '_ensure_queue_exists', side_effect=Exception("Queue error")), \
             patch('sys.stderr', new_callable=StringIO) as mock_stderr:
            
            handler = AzureQueueHandler(
                queue_name=self.queue_name,
                connection_string=self.connection_string
            )
            
            assert "Failed to ensure queue exists: Queue error" in mock_stderr.getvalue()

    @patch('api_exchange_core.utils.logger.QueueServiceClient')
    def test_ensure_queue_exists_success_existing_queue(self, mock_queue_service_class):
        """Test _ensure_queue_exists when queue already exists."""
        # Setup mock
        mock_queue_service = Mock()
        mock_queue_service_class.from_connection_string.return_value = mock_queue_service
        
        # Mock existing queue
        mock_queue = Mock()
        mock_queue.name = self.queue_name
        mock_queue_service.list_queues.return_value = [mock_queue]
        
        # Test
        handler = AzureQueueHandler.__new__(AzureQueueHandler)
        handler.connection_string = self.connection_string
        handler.queue_name = self.queue_name
        
        result = handler._ensure_queue_exists()
        
        assert result is True
        mock_queue_service.list_queues.assert_called_once()
        mock_queue_service.create_queue.assert_not_called()

    @patch('api_exchange_core.utils.logger.QueueServiceClient')
    def test_ensure_queue_exists_success_create_queue(self, mock_queue_service_class):
        """Test _ensure_queue_exists when queue needs to be created."""
        # Setup mock
        mock_queue_service = Mock()
        mock_queue_service_class.from_connection_string.return_value = mock_queue_service
        
        # Mock no existing queues
        mock_queue_service.list_queues.return_value = []
        
        # Test
        with patch('sys.stderr', new_callable=StringIO) as mock_stderr:
            handler = AzureQueueHandler.__new__(AzureQueueHandler)
            handler.connection_string = self.connection_string
            handler.queue_name = self.queue_name
            
            result = handler._ensure_queue_exists()
            
            assert result is True
            mock_queue_service.list_queues.assert_called_once()
            mock_queue_service.create_queue.assert_called_once_with(self.queue_name)
            assert f"Queue '{self.queue_name}' does not exist. Creating..." in mock_stderr.getvalue()
            assert f"Queue '{self.queue_name}' created successfully" in mock_stderr.getvalue()

    def test_ensure_queue_exists_no_connection_string(self):
        """Test _ensure_queue_exists without connection string."""
        handler = AzureQueueHandler.__new__(AzureQueueHandler)
        handler.connection_string = None
        
        result = handler._ensure_queue_exists()
        
        assert result is False

    @patch('api_exchange_core.utils.logger.QueueServiceClient')
    def test_ensure_queue_exists_failure(self, mock_queue_service_class):
        """Test _ensure_queue_exists when Azure operation fails."""
        # Setup mock to raise exception
        mock_queue_service_class.from_connection_string.side_effect = Exception("Azure error")
        
        # Test
        with patch('sys.stderr', new_callable=StringIO) as mock_stderr:
            handler = AzureQueueHandler.__new__(AzureQueueHandler)
            handler.connection_string = self.connection_string
            handler.queue_name = self.queue_name
            
            result = handler._ensure_queue_exists()
            
            assert result is False
            assert "Failed to ensure queue exists: Azure error" in mock_stderr.getvalue()

    @patch('api_exchange_core.exceptions.get_correlation_id')
    def test_emit_basic_log_record(self, mock_get_correlation_id):
        """Test emitting a basic log record."""
        mock_get_correlation_id.return_value = "corr-123"
        
        # Create handler
        with patch.object(AzureQueueHandler, '_ensure_queue_exists'):
            handler = AzureQueueHandler(
                queue_name=self.queue_name,
                connection_string=self.connection_string,
                batch_size=10
            )
        
        # Create log record
        record = logging.LogRecord(
            name="test.logger",
            level=logging.INFO,
            pathname="/test/file.py",
            lineno=42,
            msg="Test message",
            args=(),
            exc_info=None
        )
        record.created = datetime.now(timezone.utc).timestamp()
        
        # Test emit
        handler.emit(record)
        
        # Verify log was added to buffer
        assert len(handler.log_buffer) == 1
        log_entry = handler.log_buffer[0]
        
        assert log_entry["message"] == "Test message"
        assert log_entry["level"] == "INFO"
        assert log_entry["logger"] == "test.logger"
        assert log_entry["module"] == "file"
        assert log_entry["function"] is None
        assert log_entry["line"] == 42
        assert log_entry["correlation_id"] == "corr-123"

    @patch('api_exchange_core.exceptions.get_correlation_id')
    def test_emit_with_exception_info(self, mock_get_correlation_id):
        """Test emitting log record with exception information."""
        mock_get_correlation_id.return_value = "corr-456"
        
        # Create handler
        with patch.object(AzureQueueHandler, '_ensure_queue_exists'):
            handler = AzureQueueHandler(
                queue_name=self.queue_name,
                connection_string=self.connection_string
            )
        
        # Create exception
        try:
            1 / 0
        except ZeroDivisionError:
            exc_info = sys.exc_info()
        
        # Create log record with exception
        record = logging.LogRecord(
            name="test.logger",
            level=logging.ERROR,
            pathname="/test/file.py",
            lineno=50,
            msg="Error occurred",
            args=(),
            exc_info=exc_info
        )
        record.created = datetime.now(timezone.utc).timestamp()
        
        # Test emit
        handler.emit(record)
        
        # Verify exception info was captured
        log_entry = handler.log_buffer[0]
        assert "exception" in log_entry
        assert log_entry["exception"]["type"] == "ZeroDivisionError"
        assert "division by zero" in log_entry["exception"]["message"]
        assert len(log_entry["exception"]["traceback"]) > 0

    @patch('api_exchange_core.exceptions.get_correlation_id')
    def test_emit_with_custom_fields(self, mock_get_correlation_id):
        """Test emitting log record with custom fields."""
        mock_get_correlation_id.return_value = "corr-789"
        
        # Create handler
        with patch.object(AzureQueueHandler, '_ensure_queue_exists'):
            handler = AzureQueueHandler(
                queue_name=self.queue_name,
                connection_string=self.connection_string
            )
        
        # Create log record with custom fields
        record = logging.LogRecord(
            name="test.logger",
            level=logging.WARNING,
            pathname="/test/file.py",
            lineno=60,
            msg="Warning message",
            args=(),
            exc_info=None
        )
        record.created = datetime.now(timezone.utc).timestamp()
        
        # Add custom fields
        record.user_id = "user123"
        record.operation_id = "op456"
        record._tenant_id = "tenant789"  # Underscore prefix should be stripped
        record.custom_data = {"key": "value"}
        
        # Test emit
        handler.emit(record)
        
        # Verify custom fields were included
        log_entry = handler.log_buffer[0]
        assert log_entry["user_id"] == "user123"
        assert log_entry["operation_id"] == "op456"
        assert log_entry["tenant_id"] == "tenant789"  # Underscore stripped
        assert log_entry["custom_data"] == {"key": "value"}

    def test_emit_triggers_flush_when_buffer_full(self):
        """Test that emit triggers flush when buffer reaches batch size."""
        with patch.object(AzureQueueHandler, '_ensure_queue_exists'), \
             patch.object(AzureQueueHandler, 'flush') as mock_flush:
            
            handler = AzureQueueHandler(
                queue_name=self.queue_name,
                connection_string=self.connection_string,
                batch_size=2
            )
            
            # Create log records
            record1 = logging.LogRecord("test", logging.INFO, "", 1, "Message 1", (), None)
            record1.created = datetime.now(timezone.utc).timestamp()
            
            record2 = logging.LogRecord("test", logging.INFO, "", 2, "Message 2", (), None)
            record2.created = datetime.now(timezone.utc).timestamp()
            
            # First emit should not trigger flush
            handler.emit(record1)
            mock_flush.assert_not_called()
            
            # Second emit should trigger flush
            handler.emit(record2)
            mock_flush.assert_called_once()

    def test_emit_handles_exception(self):
        """Test that emit handles exceptions gracefully."""
        with patch.object(AzureQueueHandler, '_ensure_queue_exists'), \
             patch.object(AzureQueueHandler, 'handleError') as mock_handle_error:
            
            handler = AzureQueueHandler(
                queue_name=self.queue_name,
                connection_string=self.connection_string
            )
            
            # Patch get_correlation_id to raise exception
            with patch('api_exchange_core.exceptions.get_correlation_id', side_effect=Exception("Import error")):
                record = logging.LogRecord("test", logging.INFO, "", 1, "Test", (), None)
                
                handler.emit(record)
                
                mock_handle_error.assert_called_once_with(record)

    @patch('api_exchange_core.utils.logger.QueueClient')
    def test_flush_success(self, mock_queue_client_class):
        """Test successful log flushing."""
        # Setup mock
        mock_queue_client = Mock()
        mock_queue_client_class.from_connection_string.return_value = mock_queue_client
        
        # Create handler with logs in buffer
        with patch.object(AzureQueueHandler, '_ensure_queue_exists'):
            handler = AzureQueueHandler(
                queue_name=self.queue_name,
                connection_string=self.connection_string
            )
            
            # Add test logs to buffer
            handler.log_buffer = [
                {"message": "Log 1", "level": "INFO"},
                {"message": "Log 2", "level": "ERROR"}
            ]
            
            # Test flush
            handler.flush()
            
            # Verify queue client was created and messages sent
            mock_queue_client_class.from_connection_string.assert_called_once_with(
                conn_str=self.connection_string,
                queue_name=self.queue_name
            )
            assert mock_queue_client.send_message.call_count == 2
            
            # Verify buffer was cleared
            assert handler.log_buffer == []

    def test_flush_empty_buffer(self):
        """Test flushing when buffer is empty."""
        with patch.object(AzureQueueHandler, '_ensure_queue_exists'):
            handler = AzureQueueHandler(
                queue_name=self.queue_name,
                connection_string=self.connection_string
            )
            
            # Test flush with empty buffer
            handler.flush()
            
            # Should return early without doing anything
            assert handler.log_buffer == []

    def test_flush_no_connection_string(self):
        """Test flushing without connection string."""
        with patch.object(AzureQueueHandler, '_ensure_queue_exists'):
            handler = AzureQueueHandler(queue_name=self.queue_name)
            handler.connection_string = None
            handler.log_buffer = [{"message": "Test"}]
            
            # Test flush
            handler.flush()
            
            # Buffer should remain unchanged
            assert len(handler.log_buffer) == 1

    @patch('api_exchange_core.utils.logger.QueueClient')
    def test_flush_send_failure(self, mock_queue_client_class):
        """Test flush behavior when individual message send fails."""
        # Setup mock to fail on send_message
        mock_queue_client = Mock()
        mock_queue_client.send_message.side_effect = Exception("Send failed")
        mock_queue_client_class.from_connection_string.return_value = mock_queue_client
        
        with patch.object(AzureQueueHandler, '_ensure_queue_exists'), \
             patch('sys.stderr', new_callable=StringIO) as mock_stderr:
            
            handler = AzureQueueHandler(
                queue_name=self.queue_name,
                connection_string=self.connection_string
            )
            handler.log_buffer = [{"message": "Test log"}]
            
            # Test flush
            handler.flush()
            
            # Verify error was logged to stderr
            assert "Error sending individual log entry: Send failed" in mock_stderr.getvalue()
            
            # Buffer should still be cleared
            assert handler.log_buffer == []

    @patch('api_exchange_core.utils.logger.QueueClient')
    def test_flush_client_creation_failure(self, mock_queue_client_class):
        """Test flush behavior when queue client creation fails."""
        # Setup mock to fail on client creation
        mock_queue_client_class.from_connection_string.side_effect = Exception("Client creation failed")
        
        with patch.object(AzureQueueHandler, '_ensure_queue_exists'), \
             patch('sys.stderr', new_callable=StringIO) as mock_stderr:
            
            handler = AzureQueueHandler(
                queue_name=self.queue_name,
                connection_string=self.connection_string
            )
            handler.log_buffer = [{"message": "Test log"}]
            
            # Test flush
            handler.flush()
            
            # Verify error was logged to stderr
            assert "Error sending logs to Azure Queue: Client creation failed" in mock_stderr.getvalue()

    def test_close_calls_flush(self):
        """Test that close method calls flush."""
        with patch.object(AzureQueueHandler, '_ensure_queue_exists'), \
             patch.object(AzureQueueHandler, 'flush') as mock_flush:
            
            handler = AzureQueueHandler(
                queue_name=self.queue_name,
                connection_string=self.connection_string
            )
            
            # Test close
            handler.close()
            
            mock_flush.assert_called_once()


class TestConfigureLogging:
    """Test configure_logging function."""

    def setup_method(self):
        """Set up test fixtures."""
        # Reset global function logger
        global _function_logger
        _function_logger = None

    def teardown_method(self):
        """Clean up after tests."""
        global _function_logger
        _function_logger = None

    @patch('api_exchange_core.utils.logger.get_config')
    def test_configure_logging_default_params(self, mock_get_config):
        """Test configure_logging with default parameters."""
        # Setup mock config
        mock_config = Mock()
        mock_config.logging.level = "INFO"
        mock_config.features.enable_logs_queue = False
        mock_config.queue.connection_string = "test-connection"
        mock_get_config.return_value = mock_config
        
        # Test
        logger = configure_logging("test-function")
        
        assert isinstance(logger, ContextAwareLogger)
        assert logger.logger.name == "function.test-function"

    @patch('api_exchange_core.utils.logger.get_config')
    def test_configure_logging_custom_params(self, mock_get_config):
        """Test configure_logging with custom parameters."""
        # Setup mock config
        mock_config = Mock()
        mock_get_config.return_value = mock_config
        
        # Test with custom parameters
        logger = configure_logging(
            function_name="custom-function",
            log_level="DEBUG",
            enable_queue=True,
            queue_name="custom-queue",
            queue_batch_size=20,
            connection_string="custom-connection"
        )
        
        assert isinstance(logger, ContextAwareLogger)
        assert logger.logger.name == "function.custom-function"

    @patch('api_exchange_core.utils.logger.get_config')
    def test_configure_logging_string_log_level(self, mock_get_config):
        """Test configure_logging with string log level."""
        # Setup mock config
        mock_config = Mock()
        mock_config.logging.level = "WARNING"
        mock_config.features.enable_logs_queue = False
        mock_config.queue.connection_string = "test-connection"
        mock_get_config.return_value = mock_config
        
        # Test with string log level
        logger = configure_logging("test-function", log_level="ERROR")
        
        # Verify log level was converted and set
        assert logger.logger.level == logging.ERROR

    @patch('api_exchange_core.utils.logger.get_config')
    def test_configure_logging_queue_enabled(self, mock_get_config):
        """Test configure_logging with queue logging enabled."""
        # Setup mock config
        mock_config = Mock()
        mock_config.logging.level = "INFO"
        mock_config.features.enable_logs_queue = True
        mock_config.queue.connection_string = "test-connection"
        mock_get_config.return_value = mock_config
        
        with patch.object(AzureQueueHandler, '_ensure_queue_exists'):
            logger = configure_logging("test-function", enable_queue=True)
            
            # Verify queue handler was added
            queue_handlers = [
                h for h in logger.logger.handlers 
                if isinstance(h, AzureQueueHandler)
            ]
            assert len(queue_handlers) == 1

    @patch('api_exchange_core.utils.logger.get_config')
    def test_configure_logging_removes_existing_handlers(self, mock_get_config):
        """Test that configure_logging removes existing handlers."""
        # Setup mock config
        mock_config = Mock()
        mock_config.logging.level = "INFO"
        mock_config.features.enable_logs_queue = False
        mock_config.queue.connection_string = "test-connection"
        mock_get_config.return_value = mock_config
        
        # Test
        logger = configure_logging("test-function")
        
        # Add a handler manually
        test_handler = logging.StreamHandler()
        logger.logger.addHandler(test_handler)
        
        # Configure again
        logger2 = configure_logging("test-function")
        
        # Original handler should be removed
        assert test_handler not in logger2.logger.handlers

    @patch('api_exchange_core.utils.logger.get_config')
    def test_configure_logging_sets_global_logger(self, mock_get_config):
        """Test that configure_logging sets the global function logger."""
        # Setup mock config
        mock_config = Mock()
        mock_config.logging.level = "INFO"
        mock_config.features.enable_logs_queue = False
        mock_config.queue.connection_string = "test-connection"
        mock_get_config.return_value = mock_config
        
        # Test
        logger = configure_logging("test-function")
        
        # Verify global logger was set
        from api_exchange_core.utils.logger import _function_logger
        assert _function_logger == logger


class TestGetLogger:
    """Test get_logger function."""

    def setup_method(self):
        """Set up test fixtures."""
        # Reset global function logger
        global _function_logger
        _function_logger = None

    def teardown_method(self):
        """Clean up after tests."""
        global _function_logger
        _function_logger = None

    def test_get_logger_returns_configured_function_logger(self):
        """Test get_logger returns existing function logger."""
        # Setup existing function logger
        mock_base_logger = Mock(spec=logging.Logger)
        context_logger = ContextAwareLogger(mock_base_logger)
        
        # Import and set the global logger directly
        import api_exchange_core.utils.logger as logger_module
        logger_module._function_logger = context_logger
        
        # Test
        result = get_logger()
        
        assert result == context_logger
        
        # Clean up
        logger_module._function_logger = None

    @patch('api_exchange_core.utils.logger.get_config')
    def test_get_logger_fallback_to_root(self, mock_get_config):
        """Test get_logger fallback to root logger."""
        # Setup mock config
        mock_config = Mock()
        mock_config.logging.level = "DEBUG"
        mock_get_config.return_value = mock_config
        
        # Test
        result = get_logger()
        
        assert isinstance(result, ContextAwareLogger)
        assert isinstance(result.logger, logging.Logger)

    @patch('api_exchange_core.utils.logger.get_config')
    def test_get_logger_with_custom_log_level(self, mock_get_config):
        """Test get_logger with custom log level."""
        mock_get_config.return_value = Mock()
        
        # Test with custom log level
        result = get_logger(log_level="WARNING")
        
        assert isinstance(result, ContextAwareLogger)
        assert result.logger.level == logging.WARNING

    @patch('api_exchange_core.utils.logger.get_config')
    def test_get_logger_string_log_level_conversion(self, mock_get_config):
        """Test get_logger converts string log levels."""
        mock_get_config.return_value = Mock()
        
        # Test with string log level
        result = get_logger(log_level="ERROR")
        
        assert result.logger.level == logging.ERROR

    def test_get_logger_returns_already_wrapped_logger(self):
        """Test get_logger returns already wrapped ContextAwareLogger."""
        # Create a ContextAwareLogger and set as global
        mock_base_logger = Mock(spec=logging.Logger)
        wrapped_logger = ContextAwareLogger(mock_base_logger)
        
        # Import and set the global logger directly
        import api_exchange_core.utils.logger as logger_module
        logger_module._function_logger = wrapped_logger
        
        # Test
        result = get_logger()
        
        # Should return the same wrapped logger, not double-wrap
        assert result == wrapped_logger
        assert isinstance(result, ContextAwareLogger)
        
        # Clean up
        logger_module._function_logger = None