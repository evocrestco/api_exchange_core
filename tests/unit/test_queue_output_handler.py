"""
Unit tests for QueueOutputHandler.

Tests the queue output handler for routing messages to Azure Storage Queues.
"""

import pytest
from unittest.mock import Mock, patch, call
from typing import Dict, Any

from api_exchange_core.processors.output_handlers.queue_output_handler import QueueOutputHandler
from api_exchange_core.processors.message import Message, MessageType
from api_exchange_core.processors.processing_result import ProcessingResult


class TestQueueOutputHandler:
    """Test QueueOutputHandler functionality."""

    def setup_method(self):
        """Set up test fixtures."""
        self.queue_mappings = {
            "success": "success-queue",
            "error": "error-queue",
            "audit": "audit-queue"
        }
        self.connection_string = "DefaultEndpointsProtocol=https;AccountName=test;AccountKey=key==;EndpointSuffix=core.windows.net"
        self.default_queue = "default-queue"
        
        self.handler = QueueOutputHandler(
            queue_mappings=self.queue_mappings,
            connection_string=self.connection_string,
            default_queue=self.default_queue
        )

    def test_init(self):
        """Test handler initialization."""
        assert self.handler.queue_mappings == self.queue_mappings
        assert self.handler.connection_string == self.connection_string
        assert self.handler.default_queue == self.default_queue
        assert self.handler.logger is not None

    def test_init_without_default_queue(self):
        """Test handler initialization without default queue."""
        handler = QueueOutputHandler(
            queue_mappings=self.queue_mappings,
            connection_string=self.connection_string
        )
        assert handler.default_queue is None

    def test_get_handler_name(self):
        """Test handler name."""
        assert self.handler.get_handler_name() == "QueueOutputHandler"

    def test_handle_output_no_messages(self):
        """Test handling result with no output messages."""
        # Create test data
        result = ProcessingResult.success_result()
        source_message = Message.create_simple_message(
            payload={"test": "data"},
            tenant_id="tenant-123",
            pipeline_id="pipeline-456"
        )
        context = {"test": "context"}

        # Should not raise any exceptions
        self.handler.handle_output(result, source_message, context)

    @patch('api_exchange_core.processors.output_handlers.queue_output_handler.send_message_to_queue_direct')
    def test_handle_output_single_message_with_mapping(self, mock_send):
        """Test handling single output message with queue mapping."""
        # Create output message with routing context
        output_message = Message.create_simple_message(
            payload={"result": "success"},
            tenant_id="tenant-123"
        )
        output_message.add_context(output_name="success")

        # Create result with output message
        result = ProcessingResult.success_result()
        result.add_output_message(output_message)

        # Create source message
        source_message = Message.create_simple_message(
            payload={"input": "data"},
            tenant_id="tenant-123",
            pipeline_id="pipeline-456"
        )

        context = {"processor": "test-processor"}

        # Handle output
        self.handler.handle_output(result, source_message, context)

        # Verify send_message_to_queue_direct was called correctly
        # Actual signature: send_message_to_queue_direct(connection_string, queue_name, message_data)
        mock_send.assert_called_once_with(
            connection_string=self.connection_string,
            queue_name="success-queue",
            message_data=output_message.model_dump()
        )

    @patch('api_exchange_core.processors.output_handlers.queue_output_handler.send_message_to_queue_direct')
    def test_handle_output_multiple_messages(self, mock_send):
        """Test handling multiple output messages."""
        # Create multiple output messages
        success_message = Message.create_simple_message(payload={"result": "success"})
        success_message.add_context(output_name="success")

        audit_message = Message.create_simple_message(payload={"audit": "log"})
        audit_message.add_context(output_name="audit")

        # Create result with output messages
        result = ProcessingResult.success_result()
        result.add_output_message(success_message)
        result.add_output_message(audit_message)

        # Create source message
        source_message = Message.create_simple_message(
            payload={"input": "data"},
            tenant_id="tenant-123"
        )

        context = {}

        # Handle output
        self.handler.handle_output(result, source_message, context)

        # Verify both messages were sent
        assert mock_send.call_count == 2
        
        # Check first call (success)
        first_call = mock_send.call_args_list[0]
        assert first_call[1]["connection_string"] == self.connection_string
        assert first_call[1]["queue_name"] == "success-queue"
        assert first_call[1]["message_data"] == success_message.model_dump()

        # Check second call (audit)
        second_call = mock_send.call_args_list[1]
        assert second_call[1]["connection_string"] == self.connection_string
        assert second_call[1]["queue_name"] == "audit-queue"
        assert second_call[1]["message_data"] == audit_message.model_dump()

    @patch('api_exchange_core.processors.output_handlers.queue_output_handler.send_message_to_queue_direct')
    def test_handle_output_context_routing(self, mock_send):
        """Test routing message based on context instead of message context."""
        # Create output message without routing context
        output_message = Message.create_simple_message(payload={"result": "data"})

        # Create result
        result = ProcessingResult.success_result()
        result.add_output_message(output_message)

        source_message = Message.create_simple_message(payload={"input": "data"})

        # Context contains routing info
        context = {"output_name": "error"}

        # Handle output
        self.handler.handle_output(result, source_message, context)

        # Verify message was routed to error queue
        mock_send.assert_called_once_with(
            connection_string=self.connection_string,
            queue_name="error-queue",
            message_data=output_message.model_dump()
        )

    @patch('api_exchange_core.processors.output_handlers.queue_output_handler.send_message_to_queue_direct')
    def test_handle_output_default_queue(self, mock_send):
        """Test routing to default queue when no mapping found."""
        # Create output message without routing context
        output_message = Message.create_simple_message(payload={"result": "data"})

        # Create result
        result = ProcessingResult.success_result()
        result.add_output_message(output_message)

        source_message = Message.create_simple_message(payload={"input": "data"})
        context = {}

        # Handle output
        self.handler.handle_output(result, source_message, context)

        # Verify message was routed to default queue
        mock_send.assert_called_once_with(
            connection_string=self.connection_string,
            queue_name="default-queue",
            message_data=output_message.model_dump()
        )

    def test_handle_output_no_mapping_no_default(self):
        """Test handling when no queue mapping and no default queue."""
        # Create handler without default queue
        handler = QueueOutputHandler(
            queue_mappings=self.queue_mappings,
            connection_string=self.connection_string
        )

        # Create output message without routing context
        output_message = Message.create_simple_message(payload={"result": "data"})

        # Create result
        result = ProcessingResult.success_result()
        result.add_output_message(output_message)

        source_message = Message.create_simple_message(payload={"input": "data"})
        context = {}

        # Should not raise exception (logs warning and continues)
        handler.handle_output(result, source_message, context)

    @patch('api_exchange_core.processors.output_handlers.queue_output_handler.send_message_to_queue_direct')
    def test_handle_output_send_failure_continues(self, mock_send):
        """Test that send failure doesn't stop processing other messages."""
        # Configure mock to fail on first call, succeed on second
        mock_send.side_effect = [Exception("Queue send failed"), None]

        # Create multiple output messages
        message1 = Message.create_simple_message(payload={"result": "1"})
        message1.add_context(output_name="success")

        message2 = Message.create_simple_message(payload={"result": "2"})
        message2.add_context(output_name="success")

        # Create result
        result = ProcessingResult.success_result()
        result.add_output_message(message1)
        result.add_output_message(message2)

        source_message = Message.create_simple_message(payload={"input": "data"})
        context = {}

        # Should not raise exception despite first failure
        self.handler.handle_output(result, source_message, context)

        # Verify both sends were attempted
        assert mock_send.call_count == 2

    @patch('api_exchange_core.processors.output_handlers.queue_output_handler.send_message_to_queue_direct')
    def test_send_message_to_queue_success(self, mock_send):
        """Test successful message sending."""
        message = Message.create_simple_message(payload={"test": "data"})
        message.add_context(output_name="success")
        context = {}

        # Call private method directly
        self.handler._send_message_to_queue(message, context)

        # Verify send was called correctly
        mock_send.assert_called_once_with(
            connection_string=self.connection_string,
            queue_name="success-queue",
            message_data=message.model_dump()
        )

    @patch('api_exchange_core.processors.output_handlers.queue_output_handler.send_message_to_queue_direct')
    def test_send_message_to_queue_failure_raises(self, mock_send):
        """Test that send failure raises exception."""
        mock_send.side_effect = Exception("Queue connection failed")

        message = Message.create_simple_message(payload={"test": "data"})
        message.add_context(output_name="success")
        context = {}

        # Should raise the exception
        with pytest.raises(Exception, match="Queue connection failed"):
            self.handler._send_message_to_queue(message, context)

    def test_get_queue_name_message_context(self):
        """Test queue name resolution from message context."""
        message = Message.create_simple_message(payload={"test": "data"})
        message.add_context(output_name="success")
        context = {}

        queue_name = self.handler._get_queue_name(message, context)
        assert queue_name == "success-queue"

    def test_get_queue_name_processing_context(self):
        """Test queue name resolution from processing context."""
        message = Message.create_simple_message(payload={"test": "data"})
        context = {"output_name": "error"}

        queue_name = self.handler._get_queue_name(message, context)
        assert queue_name == "error-queue"

    def test_get_queue_name_message_context_priority(self):
        """Test that message context takes priority over processing context."""
        message = Message.create_simple_message(payload={"test": "data"})
        message.add_context(output_name="success")
        context = {"output_name": "error"}  # Should be ignored

        queue_name = self.handler._get_queue_name(message, context)
        assert queue_name == "success-queue"

    def test_get_queue_name_default_queue(self):
        """Test falling back to default queue."""
        message = Message.create_simple_message(payload={"test": "data"})
        context = {}

        queue_name = self.handler._get_queue_name(message, context)
        assert queue_name == "default-queue"

    def test_get_queue_name_no_mapping_no_default(self):
        """Test when no mapping exists and no default queue."""
        handler = QueueOutputHandler(
            queue_mappings=self.queue_mappings,
            connection_string=self.connection_string
        )

        message = Message.create_simple_message(payload={"test": "data"})
        context = {}

        queue_name = handler._get_queue_name(message, context)
        assert queue_name is None

    def test_get_queue_name_unknown_output_name(self):
        """Test with output_name that has no mapping."""
        message = Message.create_simple_message(payload={"test": "data"})
        message.add_context(output_name="unknown")
        context = {}

        queue_name = self.handler._get_queue_name(message, context)
        assert queue_name == "default-queue"


class TestQueueOutputHandlerIntegration:
    """Integration tests for QueueOutputHandler."""

    @patch('api_exchange_core.processors.output_handlers.queue_output_handler.send_message_to_queue_direct')
    def test_full_workflow(self, mock_send):
        """Test complete workflow from processing result to queue routing."""
        # Setup handler
        queue_mappings = {
            "next": "processing-queue",
            "completed": "completed-queue"
        }
        handler = QueueOutputHandler(
            queue_mappings=queue_mappings,
            connection_string="test-connection",
            default_queue="fallback-queue"
        )

        # Create processing result with multiple outputs
        result = ProcessingResult.success_result()
        
        # Add next processing message
        next_message = Message.create_simple_message(
            payload={"data": "processed", "next_step": "validation"},
            tenant_id="tenant-123",
            pipeline_id="pipeline-456"
        )
        next_message.add_context(output_name="next")
        result.add_output_message(next_message)

        # Add completion message
        completion_message = Message.create_simple_message(
            payload={"status": "completed", "results": {"count": 100}},
            tenant_id="tenant-123",
            pipeline_id="pipeline-456"
        )
        completion_message.add_context(output_name="completed")
        result.add_output_message(completion_message)

        # Create source message
        source_message = Message.create_simple_message(
            payload={"original": "data"},
            tenant_id="tenant-123",
            pipeline_id="pipeline-456"
        )

        context = {"processor": "data-transformer"}

        # Process outputs
        handler.handle_output(result, source_message, context)

        # Verify both messages were sent to correct queues
        assert mock_send.call_count == 2

        # Check calls
        calls = mock_send.call_args_list
        
        # First call should be next_message to processing-queue
        first_call = calls[0]
        assert first_call[1]["queue_name"] == "processing-queue"
        assert first_call[1]["message_data"]["payload"]["next_step"] == "validation"

        # Second call should be completion_message to completed-queue
        second_call = calls[1]
        assert second_call[1]["queue_name"] == "completed-queue"
        assert second_call[1]["message_data"]["payload"]["status"] == "completed"