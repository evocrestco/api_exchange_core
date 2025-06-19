"""
Unit tests for NoOpOutputHandler.

Tests the no-operation output handler that's used when processors
don't need to route output to any downstream systems.
"""

from unittest.mock import MagicMock

import pytest

from api_exchange_core.processors.processing_result import ProcessingResult
from api_exchange_core.processors import Message
from api_exchange_core.processors import NoOpOutputHandler, OutputHandlerStatus


class TestNoOpOutputHandler:
    """Test the NoOpOutputHandler implementation."""
    
    @pytest.fixture
    def handler(self):
        """Create a NoOpOutputHandler instance for testing."""
        return NoOpOutputHandler(
            destination="terminal",
            config={
                "reason": "Final processor in pipeline",
                "metadata": {"processor_type": "terminal", "side_effects": ["notification_sent"]}
            }
        )
    
    @pytest.fixture
    def minimal_handler(self):
        """Create a minimal NoOpOutputHandler with defaults."""
        return NoOpOutputHandler()
    
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
        result.entities_created = ["entity-789", "entity-790"]
        result.entities_updated = ["entity-791"]
        result.processing_metadata = {"test": True}
        return result
    
    def test_handler_initialization(self, handler):
        """Test handler initialization with custom config."""
        assert handler.destination == "terminal"
        assert handler.reason == "Final processor in pipeline"
        assert handler.metadata == {"processor_type": "terminal", "side_effects": ["notification_sent"]}
        assert handler._handler_name == "NoOpOutputHandler"
    
    def test_minimal_handler_initialization(self, minimal_handler):
        """Test handler initialization with defaults."""
        assert minimal_handler.destination == "noop"
        assert minimal_handler.reason == "No output required"
        assert minimal_handler.metadata == {}
    
    def test_validate_configuration(self, handler):
        """Test validate_configuration always returns True."""
        assert handler.validate_configuration() is True
        
        # Even with empty handler
        empty_handler = NoOpOutputHandler("")
        assert empty_handler.validate_configuration() is True
    
    def test_supports_retry(self, handler):
        """Test supports_retry always returns False for no-op."""
        assert handler.supports_retry() is False
        
        # No-op handlers don't need retry since they always succeed
        minimal = NoOpOutputHandler()
        assert minimal.supports_retry() is False
    
    def test_get_handler_info(self, handler):
        """Test get_handler_info returns expected metadata."""
        info = handler.get_handler_info()
        
        assert info["handler_name"] == "NoOpOutputHandler"
        assert info["destination"] == "terminal"
        assert info["handler_type"] == "no_operation"
        assert info["reason"] == "Final processor in pipeline"
        assert info["produces_output"] is False
        assert info["side_effects"] is False
        assert info["always_succeeds"] is True
        assert info["metadata_keys"] == ["processor_type", "side_effects"]
        assert info["supports_retry"] is False
    
    def test_get_handler_info_minimal(self, minimal_handler):
        """Test get_handler_info with minimal configuration."""
        info = minimal_handler.get_handler_info()
        
        assert info["handler_name"] == "NoOpOutputHandler"
        assert info["destination"] == "noop"
        assert info["reason"] == "No output required"
        assert info["metadata_keys"] == []
    
    def test_handle_success(self, handler, mock_message, mock_result):
        """Test successful handle execution with logging."""
        # Execute handler
        result = handler.handle(mock_message, mock_result)
        
        # Verify result
        assert result.success is True
        assert result.status == OutputHandlerStatus.SUCCESS
        assert result.handler_name == "NoOpOutputHandler"
        assert result.destination == "terminal"
        assert result.execution_duration_ms >= 0
        
        # Check metadata
        metadata = result.metadata
        assert metadata["reason"] == "Final processor in pipeline"
        assert metadata["no_output_produced"] is True
        assert metadata["processing_completed"] is True
        assert metadata["message_id"] == "test-msg-123"
        assert metadata["correlation_id"] == "corr-456"
        assert metadata["entities_affected"] == 3  # 2 created + 1 updated
        assert metadata["processor_type"] == "terminal"
        assert metadata["side_effects"] == ["notification_sent"]
        
        # Note: We don't mock the logger since it's part of the real implementation
        # The test verifies the handler execution logic and metadata properly
    
    def test_handle_with_no_entities(self, minimal_handler, mock_message):
        """Test handle with no entities created or updated."""
        # Create result with no entities
        result = MagicMock(spec=ProcessingResult)
        result.success = True
        result.entities_created = []
        result.entities_updated = []
        result.processing_metadata = {}
        
        # Execute handler
        handler_result = minimal_handler.handle(mock_message, result)
        
        # Verify result
        assert handler_result.success is True
        assert handler_result.status == OutputHandlerStatus.SUCCESS
        assert handler_result.metadata["entities_affected"] == 0
        assert handler_result.metadata["reason"] == "No output required"
    
    def test_handle_always_succeeds(self, handler, mock_message, mock_result):
        """Test that handle never raises exceptions."""
        # Even with None values, it should handle gracefully
        mock_result.entities_created = None
        mock_result.entities_updated = None
        
        result = handler.handle(mock_message, mock_result)
        
        assert result.success is True
        assert result.status == OutputHandlerStatus.SUCCESS
        # Should handle None gracefully
        assert result.metadata["entities_affected"] == 0
    
    def test_handler_repr(self, handler):
        """Test string representation."""
        assert repr(handler) == "NoOpOutputHandler(destination='terminal')"
    
    def test_handler_str(self, handler):
        """Test human-readable string."""
        assert str(handler) == "NoOpOutputHandler -> terminal"
    
    def test_execution_timing(self, handler, mock_message, mock_result):
        """Test that execution time is measured."""
        result = handler.handle(mock_message, mock_result)
        
        # Should have some execution time
        assert result.execution_duration_ms > 0
        assert result.execution_duration_ms < 100  # Should be very fast
    
    def test_custom_destination(self, mock_message, mock_result):
        """Test handler with custom destination name."""
        handler = NoOpOutputHandler(
            destination="completed",
            config={"reason": "Processing complete"}
        )
        
        result = handler.handle(mock_message, mock_result)
        
        assert result.success is True
        assert result.destination == "completed"
        assert result.metadata["reason"] == "Processing complete"
    
    def test_empty_metadata_config(self, mock_message, mock_result):
        """Test handler with empty metadata in config."""
        handler = NoOpOutputHandler(
            destination="test",
            config={"metadata": {}}
        )
        
        result = handler.handle(mock_message, mock_result)
        
        assert result.success is True
        # Should still have the standard metadata fields
        assert "reason" in result.metadata
        assert "no_output_produced" in result.metadata
        assert "processing_completed" in result.metadata