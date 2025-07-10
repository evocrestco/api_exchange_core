"""
Unit tests for queue utilities.

Tests the queue message sending functionality.
"""

import json
from unittest.mock import Mock, patch, MagicMock
import pytest

from api_exchange_core.utils.queue_utils import (
    send_message_to_queue_binding, 
    send_message_to_queue_direct,
    _send_to_binding_core
)


class TestSendMessageToQueueBinding:
    """Test send_message_to_queue_binding function."""
    
    def test_send_simple_message(self):
        """Test sending a simple message to queue."""
        # Create mock output binding
        mock_output = Mock()
        
        # Test message
        message = {"test": "data", "number": 123}
        
        # Send message
        send_message_to_queue_binding(mock_output, message, "test-queue")
        
        # Verify output was called with JSON
        mock_output.set.assert_called_once_with('{"test": "data", "number": 123}')
    
    def test_send_complex_message(self):
        """Test sending a message with nested data."""
        # Create mock output binding
        mock_output = Mock()
        
        # Complex message
        message = {
            "id": "test-123",
            "data": {
                "nested": {
                    "value": "deep"
                },
                "list": [1, 2, 3]
            },
            "metadata": None
        }
        
        # Send message
        send_message_to_queue_binding(mock_output, message, "complex-queue")
        
        # Verify output was called
        mock_output.set.assert_called_once()
        
        # Verify JSON structure
        sent_json = mock_output.set.call_args[0][0]
        parsed = json.loads(sent_json)
        assert parsed["id"] == "test-123"
        assert parsed["data"]["nested"]["value"] == "deep"
        assert parsed["data"]["list"] == [1, 2, 3]
        assert parsed["metadata"] is None
    
    @patch('api_exchange_core.utils.queue_utils.to_jsonable_python')
    def test_pydantic_serialization(self, mock_to_jsonable):
        """Test that pydantic_core serialization is used."""
        # Setup mock
        mock_to_jsonable.return_value = {"serialized": "data"}
        mock_output = Mock()
        
        # Test message
        message = {"original": "data"}
        
        # Send message
        send_message_to_queue_binding(mock_output, message, "pydantic-queue")
        
        # Verify pydantic serialization was called
        mock_to_jsonable.assert_called_once_with(message)
        
        # Verify output received serialized data
        mock_output.set.assert_called_once_with('{"serialized": "data"}')
    
    def test_empty_queue_name(self):
        """Test sending message without specifying queue name."""
        # Create mock output binding
        mock_output = Mock()
        
        # Test message
        message = {"test": "data"}
        
        # Send message without queue name
        send_message_to_queue_binding(mock_output, message)
        
        # Should still work
        mock_output.set.assert_called_once_with('{"test": "data"}')
    
    def test_send_queue_message_alias(self):
        """Test that send_message_to_queue_binding works."""
        # Create mock output binding
        mock_output = Mock()
        
        # Test message
        message = {"alias": "test"}
        
        # Use alias
        send_message_to_queue_binding(mock_output, message, "alias-queue")
        
        # Should work the same
        mock_output.set.assert_called_once_with('{"alias": "test"}')
    
    def test_special_json_values(self):
        """Test sending message with special JSON values."""
        # Create mock output binding
        mock_output = Mock()
        
        # Message with special values
        message = {
            "boolean_true": True,
            "boolean_false": False,
            "null_value": None,
            "float_value": 3.14,
            "unicode": "émoji 🚀"
        }
        
        # Send message
        send_message_to_queue_binding(mock_output, message, "special-queue")
        
        # Verify output was called
        mock_output.set.assert_called_once()
        
        # Verify JSON structure
        sent_json = mock_output.set.call_args[0][0]
        parsed = json.loads(sent_json)
        assert parsed["boolean_true"] is True
        assert parsed["boolean_false"] is False
        assert parsed["null_value"] is None
        assert parsed["float_value"] == 3.14
        assert parsed["unicode"] == "émoji 🚀"


class TestQueueUtilsIntegration:
    """Integration tests for queue utilities."""
    
    def test_message_round_trip(self):
        """Test that message can be serialized and deserialized."""
        # Create mock output binding
        mock_output = Mock()
        
        # Complex test message
        original_message = {
            "id": "integration-test",
            "timestamp": "2024-01-15T10:30:00Z",
            "data": {
                "items": [
                    {"name": "item1", "value": 100},
                    {"name": "item2", "value": 200}
                ],
                "settings": {
                    "enabled": True,
                    "threshold": 0.75
                }
            },
            "tags": ["test", "integration", "queue"]
        }
        
        # Send message
        send_message_to_queue_binding(mock_output, original_message, "integration-queue")
        
        # Get serialized message
        sent_json = mock_output.set.call_args[0][0]
        
        # Deserialize
        deserialized = json.loads(sent_json)
        
        # Verify round trip
        assert deserialized == original_message


class TestSendMessageToQueueBindingErrors:
    """Test error handling in send_message_to_queue_binding."""
    
    @patch('api_exchange_core.utils.queue_utils.to_jsonable_python')
    @patch('api_exchange_core.utils.queue_utils.get_logger')
    def test_json_serialization_error(self, mock_logger, mock_to_jsonable):
        """Test JSON serialization error handling."""
        # Setup mocks
        mock_to_jsonable.return_value = {"data": "test"}
        mock_output = Mock()
        logger_instance = Mock()
        mock_logger.return_value = logger_instance
        
        # Make json.dumps raise an exception
        with patch('api_exchange_core.utils.queue_utils.json.dumps') as mock_dumps:
            mock_dumps.side_effect = TypeError("Object not JSON serializable")
            
            # Test that exception is raised and logged
            with pytest.raises(TypeError, match="Object not JSON serializable"):
                send_message_to_queue_binding(mock_output, {"test": "data"}, "test-queue")
            
            # Verify error was logged
            logger_instance.error.assert_called_once()
            assert "Failed to serialize message for queue test-queue" in logger_instance.error.call_args[0][0]
            
            # Verify output binding was not called
            mock_output.set.assert_not_called()
    
    @patch('api_exchange_core.utils.queue_utils._send_to_binding_core')
    @patch('api_exchange_core.utils.queue_utils.get_logger')
    def test_binding_send_error_propagation(self, mock_logger, mock_send_core):
        """Test that binding send errors are propagated."""
        # Setup mocks
        logger_instance = Mock()
        mock_logger.return_value = logger_instance
        mock_send_core.side_effect = RuntimeError("Binding failed")
        
        mock_output = Mock()
        message = {"test": "data"}
        
        # Test that exception propagates
        with pytest.raises(RuntimeError, match="Binding failed"):
            send_message_to_queue_binding(mock_output, message, "test-queue")
        
        # Verify _send_to_binding_core was called
        mock_send_core.assert_called_once()


class TestSendToBindingCore:
    """Test _send_to_binding_core function."""
    
    @patch('api_exchange_core.utils.queue_utils.get_logger')
    def test_successful_send(self, mock_get_logger):
        """Test successful binding send."""
        # Setup mocks
        logger_instance = Mock()
        mock_get_logger.return_value = logger_instance
        mock_output = Mock()
        
        # Test successful send
        _send_to_binding_core(mock_output, "test data", "test-binding", logger_instance)
        
        # Verify output binding was called
        mock_output.set.assert_called_once_with("test data")
        
        # Verify debug logs
        assert logger_instance.debug.call_count == 2
        logger_instance.debug.assert_any_call("Sending data to binding: test-binding")
        logger_instance.debug.assert_any_call("Successfully sent to binding: test-binding")
    
    @patch('api_exchange_core.utils.queue_utils.get_logger')
    def test_binding_error_handling(self, mock_get_logger):
        """Test error handling in binding send."""
        # Setup mocks
        logger_instance = Mock()
        mock_get_logger.return_value = logger_instance
        mock_output = Mock()
        mock_output.set.side_effect = RuntimeError("Binding error")
        
        # Test error handling
        with pytest.raises(RuntimeError, match="Binding error"):
            _send_to_binding_core(mock_output, "test data", "test-binding", logger_instance)
        
        # Verify error was logged
        logger_instance.error.assert_called_once()
        assert "Failed to send to binding test-binding: Binding error" in logger_instance.error.call_args[0][0]
    
    @patch('api_exchange_core.utils.queue_utils.get_logger')
    def test_default_logger(self, mock_get_logger):
        """Test using default logger when none provided."""
        # Setup mocks
        logger_instance = Mock()
        mock_get_logger.return_value = logger_instance
        mock_output = Mock()
        
        # Test with no logger provided
        _send_to_binding_core(mock_output, "test data", "test-binding")
        
        # Verify get_logger was called
        mock_get_logger.assert_called_once()
        
        # Verify output binding was called
        mock_output.set.assert_called_once_with("test data")


class TestSendMessageToQueueDirect:
    """Test send_message_to_queue_direct function."""
    
    @patch('api_exchange_core.utils.queue_utils.QueueClient')
    @patch('api_exchange_core.utils.queue_utils.get_logger')
    def test_successful_send(self, mock_get_logger, mock_queue_client_class):
        """Test successful direct queue send."""
        # Setup mocks
        logger_instance = Mock()
        mock_get_logger.return_value = logger_instance
        
        mock_queue_client = Mock()
        mock_queue_client_class.from_connection_string.return_value = mock_queue_client
        
        # Test data
        connection_string = "DefaultEndpointsProtocol=https;AccountName=test;AccountKey=key=="
        queue_name = "test-queue"
        message_data = {"test": "data", "number": 123}
        
        # Send message
        send_message_to_queue_direct(connection_string, queue_name, message_data)
        
        # Verify QueueClient was created correctly
        mock_queue_client_class.from_connection_string.assert_called_once_with(
            conn_str=connection_string, 
            queue_name=queue_name
        )
        
        # Verify message was sent
        mock_queue_client.send_message.assert_called_once_with('{"test": "data", "number": 123}')
        
        # Verify debug logs
        assert logger_instance.debug.call_count == 2
        logger_instance.debug.assert_any_call(f"Sending message to queue: {queue_name}")
        logger_instance.debug.assert_any_call(f"Successfully sent message to queue: {queue_name}")
    
    @patch('api_exchange_core.utils.queue_utils.to_jsonable_python')
    @patch('api_exchange_core.utils.queue_utils.get_logger')
    def test_json_serialization_error(self, mock_get_logger, mock_to_jsonable):
        """Test JSON serialization error in direct send."""
        # Setup mocks
        logger_instance = Mock()
        mock_get_logger.return_value = logger_instance
        mock_to_jsonable.return_value = {"data": "test"}
        
        # Make json.dumps raise an exception
        with patch('api_exchange_core.utils.queue_utils.json.dumps') as mock_dumps:
            mock_dumps.side_effect = TypeError("Object not JSON serializable")
            
            # Test that exception is raised and logged
            with pytest.raises(TypeError, match="Object not JSON serializable"):
                send_message_to_queue_direct("conn_str", "test-queue", {"test": "data"})
            
            # Verify error was logged
            logger_instance.error.assert_called_once()
            assert "Failed to serialize message for queue test-queue" in logger_instance.error.call_args[0][0]
    
    @patch('api_exchange_core.utils.queue_utils.QueueClient')
    @patch('api_exchange_core.utils.queue_utils.get_logger')
    def test_queue_not_found_creates_queue(self, mock_get_logger, mock_queue_client_class):
        """Test queue creation when queue doesn't exist."""
        # Setup mocks
        logger_instance = Mock()
        mock_get_logger.return_value = logger_instance
        
        mock_queue_client = Mock()
        mock_queue_client_class.from_connection_string.return_value = mock_queue_client
        
        # First send_message call fails with QueueNotFound
        # Second send_message call (after create) succeeds
        mock_queue_client.send_message.side_effect = [
            Exception("QueueNotFound: The specified queue does not exist."),
            None  # Success on retry
        ]
        
        # Test data
        connection_string = "DefaultEndpointsProtocol=https;AccountName=test;AccountKey=key=="
        queue_name = "new-queue"
        message_data = {"test": "data"}
        
        # Send message
        send_message_to_queue_direct(connection_string, queue_name, message_data)
        
        # Verify queue creation was attempted
        mock_queue_client.create_queue.assert_called_once()
        
        # Verify send_message was called twice (initial fail + retry)
        assert mock_queue_client.send_message.call_count == 2
        
        # Verify debug logs
        logger_instance.debug.assert_any_call(f"Queue {queue_name} not found, creating it...")
        logger_instance.debug.assert_any_call(f"Message sent to queue after creation: {queue_name}")
    
    @patch('api_exchange_core.utils.queue_utils.QueueClient')
    @patch('api_exchange_core.utils.queue_utils.get_logger')
    def test_queue_creation_failure(self, mock_get_logger, mock_queue_client_class):
        """Test handling of queue creation failures."""
        # Setup mocks
        logger_instance = Mock()
        mock_get_logger.return_value = logger_instance
        
        mock_queue_client = Mock()
        mock_queue_client_class.from_connection_string.return_value = mock_queue_client
        
        # Send fails with QueueNotFound, create_queue also fails
        mock_queue_client.send_message.side_effect = Exception("QueueNotFound")
        mock_queue_client.create_queue.side_effect = Exception("Failed to create queue")
        
        # Test that exception is raised and logged
        with pytest.raises(Exception, match="Failed to create queue"):
            send_message_to_queue_direct("conn_str", "test-queue", {"test": "data"})
        
        # Verify error was logged
        logger_instance.error.assert_called_once()
        assert "Failed to create queue or send message to test-queue" in logger_instance.error.call_args[0][0]
    
    @patch('api_exchange_core.utils.queue_utils.QueueClient')
    @patch('api_exchange_core.utils.queue_utils.get_logger')
    def test_other_queue_errors(self, mock_get_logger, mock_queue_client_class):
        """Test handling of non-queue-not-found errors."""
        # Setup mocks
        logger_instance = Mock()
        mock_get_logger.return_value = logger_instance
        
        mock_queue_client = Mock()
        mock_queue_client_class.from_connection_string.return_value = mock_queue_client
        
        # Send fails with different error
        mock_queue_client.send_message.side_effect = Exception("Connection timeout")
        
        # Test that exception is raised and logged
        with pytest.raises(Exception, match="Connection timeout"):
            send_message_to_queue_direct("conn_str", "test-queue", {"test": "data"})
        
        # Verify create_queue was NOT called (not a queue not found error)
        mock_queue_client.create_queue.assert_not_called()
        
        # Verify error was logged
        logger_instance.error.assert_called_once()
        assert "Failed to send message to queue test-queue: Connection timeout" in logger_instance.error.call_args[0][0]
    
    @patch('api_exchange_core.utils.queue_utils.QueueClient')
    @patch('api_exchange_core.utils.queue_utils.get_logger')
    def test_queue_creation_retry_success(self, mock_get_logger, mock_queue_client_class):
        """Test successful retry after queue creation."""
        # Setup mocks
        logger_instance = Mock()
        mock_get_logger.return_value = logger_instance
        
        mock_queue_client = Mock()
        mock_queue_client_class.from_connection_string.return_value = mock_queue_client
        
        # First send fails, create succeeds, retry send succeeds
        call_count = 0
        def send_side_effect(data):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise Exception("does not exist")
            return None  # Success on retry
        
        mock_queue_client.send_message.side_effect = send_side_effect
        
        # Send message
        send_message_to_queue_direct("conn_str", "test-queue", {"test": "data"})
        
        # Verify queue creation and retry
        mock_queue_client.create_queue.assert_called_once()
        assert mock_queue_client.send_message.call_count == 2