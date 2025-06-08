"""
Unit tests for base OutputHandler class and related components.

Tests the abstract base class functionality, helper methods, and
common result/error classes used by all output handlers.
"""

from datetime import datetime, UTC
from typing import Any, Dict
from unittest.mock import MagicMock

import pytest

from src.processors.v2.output_handlers.base import (
    OutputHandler,
    OutputHandlerError,
    OutputHandlerResult,
    OutputHandlerStatus,
)
from src.processors.v2.message import Message
from src.processors.processing_result import ProcessingResult


class ConcreteOutputHandler(OutputHandler):
    """Concrete implementation for testing abstract base class."""
    
    def __init__(self, destination: str, config: Dict[str, Any] = None):
        super().__init__(destination, config)
        self.handle_called = False
        self.last_message = None
        self.last_result = None
        self.should_fail = False
        self.error_message = "Test error"
    
    def handle(self, message: Message, result: ProcessingResult) -> OutputHandlerResult:
        """Test implementation that can be configured to succeed or fail."""
        self.handle_called = True
        self.last_message = message
        self.last_result = result
        
        if self.should_fail:
            raise OutputHandlerError(
                message=self.error_message,
                error_code="TEST_ERROR",
                can_retry=True,
                retry_after_seconds=60
            )
        
        return self._create_success_result(
            execution_duration_ms=10.5,
            metadata={"test": "success"}
        )


class TestOutputHandlerStatus:
    """Test OutputHandlerStatus enum values."""
    
    def test_status_values(self):
        """Verify all expected status values exist."""
        assert OutputHandlerStatus.SUCCESS == "success"
        assert OutputHandlerStatus.FAILED == "failed"
        assert OutputHandlerStatus.RETRYABLE_ERROR == "retryable_error"
        assert OutputHandlerStatus.SKIPPED == "skipped"
        assert OutputHandlerStatus.PARTIAL_SUCCESS == "partial_success"


class TestOutputHandlerError:
    """Test OutputHandlerError exception class."""
    
    def test_basic_error_creation(self):
        """Test creating a basic error with message only."""
        error = OutputHandlerError("Something went wrong")
        
        assert str(error) == "Something went wrong"
        assert error.message == "Something went wrong"
        assert error.error_code == "OUTPUT_HANDLER_ERROR"
        assert error.can_retry is False
        assert error.retry_after_seconds is None
        assert error.error_details == {}
        assert error.original_exception is None
    
    def test_full_error_creation(self):
        """Test creating an error with all parameters."""
        original_exc = ValueError("Original error")
        error = OutputHandlerError(
            message="Failed to send message",
            error_code="QUEUE_SEND_FAILED",
            can_retry=True,
            retry_after_seconds=30,
            error_details={"queue": "test-queue", "attempt": 1},
            original_exception=original_exc
        )
        
        assert error.message == "Failed to send message"
        assert error.error_code == "QUEUE_SEND_FAILED"
        assert error.can_retry is True
        assert error.retry_after_seconds == 30
        assert error.error_details == {"queue": "test-queue", "attempt": 1}
        assert error.original_exception is original_exc


class TestOutputHandlerResult:
    """Test OutputHandlerResult model class."""
    
    def test_success_result_creation(self):
        """Test creating a successful result using factory method."""
        result = OutputHandlerResult.create_success(
            handler_name="TestHandler",
            destination="test-destination",
            execution_duration_ms=25.3,
            metadata={"records_processed": 5}
        )
        
        assert result.status == OutputHandlerStatus.SUCCESS
        assert result.success is True
        assert result.handler_name == "TestHandler"
        assert result.destination == "test-destination"
        assert result.execution_duration_ms == 25.3
        assert result.metadata == {"records_processed": 5}
        assert result.error_message is None
        assert result.error_code is None
        assert result.can_retry is False
        assert isinstance(result.completed_at, datetime)
    
    def test_failure_result_creation(self):
        """Test creating a failure result using factory method."""
        result = OutputHandlerResult.create_failure(
            handler_name="TestHandler",
            destination="test-destination",
            execution_duration_ms=100.0,
            error_message="Connection timeout",
            error_code="TIMEOUT",
            can_retry=True,
            retry_after_seconds=60,
            error_details={"timeout_ms": 30000},
            metadata={"attempt": 1}
        )
        
        assert result.status == OutputHandlerStatus.RETRYABLE_ERROR
        assert result.success is False
        assert result.handler_name == "TestHandler"
        assert result.destination == "test-destination"
        assert result.execution_duration_ms == 100.0
        assert result.error_message == "Connection timeout"
        assert result.error_code == "TIMEOUT"
        assert result.can_retry is True
        assert result.retry_after_seconds == 60
        assert result.error_details == {"timeout_ms": 30000}
        assert result.metadata == {"attempt": 1}
    
    def test_failure_result_non_retryable(self):
        """Test creating a non-retryable failure result."""
        result = OutputHandlerResult.create_failure(
            handler_name="TestHandler",
            destination="test-destination",
            execution_duration_ms=50.0,
            error_message="Invalid configuration",
            error_code="CONFIG_ERROR",
            can_retry=False
        )
        
        assert result.status == OutputHandlerStatus.FAILED
        assert result.success is False
        assert result.can_retry is False
    
    def test_skipped_result_creation(self):
        """Test creating a skipped result using factory method."""
        result = OutputHandlerResult.create_skipped(
            handler_name="TestHandler",
            destination="test-destination",
            execution_duration_ms=5.0,
            reason="No output required for this message type",
            metadata={"message_type": "internal"}
        )
        
        assert result.status == OutputHandlerStatus.SKIPPED
        assert result.success is True  # Skipped is considered successful
        assert result.handler_name == "TestHandler"
        assert result.destination == "test-destination"
        assert result.execution_duration_ms == 5.0
        assert result.metadata["skip_reason"] == "No output required for this message type"
        assert result.metadata["message_type"] == "internal"


class TestOutputHandler:
    """Test the abstract OutputHandler base class."""
    
    @pytest.fixture
    def handler(self):
        """Create a concrete handler instance for testing."""
        return ConcreteOutputHandler(
            destination="test-destination",
            config={"key": "value", "timeout": 30}
        )
    
    @pytest.fixture
    def mock_message(self):
        """Create a mock message for testing."""
        message = MagicMock(spec=Message)
        message.message_id = "test-msg-123"
        message.correlation_id = "corr-456"
        message.payload = {"data": "test"}
        
        # Mock entity reference
        entity_ref = MagicMock()
        entity_ref.id = "entity-789"
        entity_ref.external_id = "ext-001"
        entity_ref.canonical_type = "test_type"
        message.entity_reference = entity_ref
        
        return message
    
    @pytest.fixture
    def mock_result(self):
        """Create a mock processing result for testing."""
        result = MagicMock(spec=ProcessingResult)
        result.success = True
        result.entities_created = ["entity-789"]
        result.processing_metadata = {"test": True}
        return result
    
    def test_handler_initialization(self, handler):
        """Test handler initialization with destination and config."""
        assert handler.destination == "test-destination"
        assert handler.config == {"key": "value", "timeout": 30}
        assert handler._handler_name == "ConcreteOutputHandler"
        assert handler.logger is not None
    
    def test_validate_configuration_default(self, handler):
        """Test default validate_configuration returns True."""
        assert handler.validate_configuration() is True
    
    def test_supports_retry_default(self, handler):
        """Test default supports_retry returns True."""
        assert handler.supports_retry() is True
    
    def test_get_handler_info(self, handler):
        """Test get_handler_info returns expected metadata."""
        info = handler.get_handler_info()
        
        assert info["handler_name"] == "ConcreteOutputHandler"
        assert info["destination"] == "test-destination"
        assert info["config_keys"] == ["key", "timeout"]
        assert info["supports_retry"] is True
    
    def test_execute_with_timing(self, handler):
        """Test _execute_with_timing helper method."""
        def test_operation():
            import time
            time.sleep(0.01)  # Sleep for 10ms
            return "result"
        
        result, duration_ms = handler._execute_with_timing(test_operation)
        
        assert result == "result"
        assert duration_ms >= 10.0  # Should be at least 10ms
        assert duration_ms < 100.0  # But not too long
    
    def test_execute_with_timing_exception(self, handler):
        """Test _execute_with_timing with exception."""
        def failing_operation():
            raise ValueError("Test error")
        
        with pytest.raises(ValueError, match="Test error"):
            handler._execute_with_timing(failing_operation)
    
    def test_create_success_result_helper(self, handler):
        """Test _create_success_result helper method."""
        result = handler._create_success_result(
            execution_duration_ms=15.5,
            metadata={"test": "data"}
        )
        
        assert result.status == OutputHandlerStatus.SUCCESS
        assert result.success is True
        assert result.handler_name == "ConcreteOutputHandler"
        assert result.destination == "test-destination"
        assert result.execution_duration_ms == 15.5
        assert result.metadata == {"test": "data"}
    
    def test_create_failure_result_helper(self, handler):
        """Test _create_failure_result helper method."""
        result = handler._create_failure_result(
            execution_duration_ms=20.0,
            error_message="Test failure",
            error_code="TEST_FAIL",
            can_retry=True,
            retry_after_seconds=30,
            error_details={"detail": "value"},
            metadata={"meta": "data"}
        )
        
        assert result.status == OutputHandlerStatus.RETRYABLE_ERROR
        assert result.success is False
        assert result.handler_name == "ConcreteOutputHandler"
        assert result.destination == "test-destination"
        assert result.execution_duration_ms == 20.0
        assert result.error_message == "Test failure"
        assert result.error_code == "TEST_FAIL"
        assert result.can_retry is True
        assert result.retry_after_seconds == 30
        assert result.error_details == {"detail": "value"}
        assert result.metadata == {"meta": "data"}
    
    def test_create_skipped_result_helper(self, handler):
        """Test _create_skipped_result helper method."""
        result = handler._create_skipped_result(
            execution_duration_ms=5.0,
            reason="Not applicable",
            metadata={"skip_type": "filter"}
        )
        
        assert result.status == OutputHandlerStatus.SKIPPED
        assert result.success is True
        assert result.handler_name == "ConcreteOutputHandler"
        assert result.destination == "test-destination"
        assert result.execution_duration_ms == 5.0
        assert result.metadata["skip_reason"] == "Not applicable"
        assert result.metadata["skip_type"] == "filter"
    
    def test_repr_method(self, handler):
        """Test __repr__ returns expected string."""
        assert repr(handler) == "ConcreteOutputHandler(destination='test-destination')"
    
    def test_str_method(self, handler):
        """Test __str__ returns human-readable string."""
        assert str(handler) == "ConcreteOutputHandler -> test-destination"
    
    def test_handle_success(self, handler, mock_message, mock_result):
        """Test successful handle execution."""
        result = handler.handle(mock_message, mock_result)
        
        assert handler.handle_called is True
        assert handler.last_message == mock_message
        assert handler.last_result == mock_result
        assert result.success is True
        assert result.status == OutputHandlerStatus.SUCCESS
    
    def test_handle_failure(self, handler, mock_message, mock_result):
        """Test handle execution with failure."""
        handler.should_fail = True
        handler.error_message = "Simulated failure"
        
        with pytest.raises(OutputHandlerError) as exc_info:
            handler.handle(mock_message, mock_result)
        
        error = exc_info.value
        assert error.message == "Simulated failure"
        assert error.error_code == "TEST_ERROR"
        assert error.can_retry is True
        assert error.retry_after_seconds == 60


class TestOutputHandlerIntegration:
    """Integration tests for output handler with real components."""
    
    def test_handler_with_empty_config(self):
        """Test handler initialization with no config."""
        handler = ConcreteOutputHandler("test-dest")
        
        assert handler.destination == "test-dest"
        assert handler.config == {}
        assert handler.validate_configuration() is True
    
    def test_handler_info_with_no_config(self):
        """Test get_handler_info with empty config."""
        handler = ConcreteOutputHandler("test-dest")
        info = handler.get_handler_info()
        
        assert info["handler_name"] == "ConcreteOutputHandler"
        assert info["destination"] == "test-dest"
        assert info["config_keys"] == []
        assert info["supports_retry"] is True