"""
Unit tests for ServiceBusOutputHandler.

Tests the Azure Service Bus output handler with simulated Service Bus operations,
following the NO MOCKS policy where possible. These tests verify queue/topic messaging,
message preparation, error handling, and retry logic.

Note: Since Service Bus is an optional dependency and requires real Azure resources,
some tests use controlled simulation to avoid external dependencies.
"""

import json
import os
import uuid
from datetime import datetime, UTC
from typing import Dict, Any
from unittest.mock import MagicMock, patch, Mock

import pytest

from src.processors.v2.output_handlers.service_bus_output import (
    ServiceBusOutputHandler,
    SERVICEBUS_AVAILABLE,
)
from src.processors.v2.output_handlers.base import (
    OutputHandlerError,
    OutputHandlerResult,
    OutputHandlerStatus,
)
from src.processors.v2.message import Message, MessageType
from src.processors.processing_result import ProcessingResult, ProcessingStatus
from src.db.db_entity_models import Entity
from src.schemas.entity_schema import EntityReference
from src.utils.hash_utils import calculate_entity_hash


class TestServiceBusOutputHandler:
    """Test the ServiceBusOutputHandler implementation."""
    
    @pytest.fixture(autouse=True)
    def setup_environment(self):
        """Set up test environment variables."""
        # Set up Service Bus connection string for tests
        os.environ["AZURE_SERVICEBUS_CONNECTION_STRING"] = "Endpoint=sb://test.servicebus.windows.net/;SharedAccessKeyName=test;SharedAccessKey=testkey="
    
    @pytest.fixture
    def skip_if_no_servicebus(self):
        """Skip tests if Service Bus SDK is not available."""
        if not SERVICEBUS_AVAILABLE:
            pytest.skip("Azure Service Bus SDK not installed")
    
    @pytest.fixture
    def handler(self, skip_if_no_servicebus):
        """Create a ServiceBusOutputHandler instance for testing."""
        return ServiceBusOutputHandler(
            destination="test-processing-queue",
            config={
                "connection_string": "Endpoint=sb://test.servicebus.windows.net/;SharedAccessKeyName=test;SharedAccessKey=testkey=",
                "destination_type": "queue",
                "time_to_live_seconds": 3600,  # 1 hour
                "session_id": "test-session",
                "message_properties": {"priority": "high", "category": "processing"}
            }
        )
    
    @pytest.fixture
    def topic_handler(self, skip_if_no_servicebus):
        """Create a ServiceBusOutputHandler for topics."""
        return ServiceBusOutputHandler(
            destination="test-topic",
            config={
                "connection_string": "Endpoint=sb://test.servicebus.windows.net/;SharedAccessKeyName=test;SharedAccessKey=testkey=",
                "destination_type": "topic",
                "scheduled_enqueue_time": "2024-01-01T12:00:00Z"
            }
        )
    
    @pytest.fixture
    def minimal_handler(self, skip_if_no_servicebus):
        """Create a minimal ServiceBusOutputHandler with defaults."""
        return ServiceBusOutputHandler(
            destination="minimal-queue"
        )
    
    @pytest.fixture
    def mock_message(self):
        """Create a real message for testing."""
        entity = Entity.create(
            tenant_id="test_tenant",
            external_id=f"sb-test-{uuid.uuid4().hex[:8]}",
            canonical_type="test_type",
            source="test_source",
            content_hash=calculate_entity_hash({"test": "servicebus"})
        )
        
        return Message(
            message_id=f"sb-msg-{uuid.uuid4().hex[:8]}",
            correlation_id=f"sb-corr-{uuid.uuid4().hex[:8]}",
            created_at=datetime.now(UTC),
            message_type=MessageType.ENTITY_PROCESSING,
            entity=entity,  # Use entity directly in v2 format
            payload={"service": "bus", "value": 456},
            retry_count=0,
            max_retries=3
        )
    
    @pytest.fixture
    def mock_result(self):
        """Create a mock processing result for testing."""
        return ProcessingResult(
            status=ProcessingStatus.SUCCESS,
            success=True,
            entities_created=["sb-entity-123"],
            entities_updated=["sb-entity-456"],
            processing_metadata={"servicebus": True, "priority": "high"},
            processor_info={"name": "ServiceBusProcessor", "version": "2.0.0"},
            processing_duration_ms=75.3,
            completed_at=datetime.now(UTC)
        )
    
    def test_import_error_when_no_servicebus(self):
        """Test that handler raises ImportError when Service Bus SDK is not available."""
        with patch.dict('sys.modules', {'azure.servicebus': None}):
            with patch('src.processors.v2.output_handlers.service_bus_output.SERVICEBUS_AVAILABLE', False):
                with pytest.raises(ImportError) as exc_info:
                    ServiceBusOutputHandler("test-queue")
                
                error = exc_info.value
                assert "Azure Service Bus SDK is not installed" in str(error)
                assert "pip install azure-servicebus" in str(error)
    
    def test_handler_initialization(self, handler):
        """Test handler initialization with custom config."""
        assert handler.destination == "test-processing-queue"
        assert "test.servicebus.windows.net" in handler.connection_string
        assert handler.destination_type == "queue"
        assert handler.time_to_live_seconds == 3600
        assert handler.session_id == "test-session"
        assert handler.message_properties == {"priority": "high", "category": "processing"}
        assert handler.scheduled_enqueue_time is None
        assert handler._handler_name == "ServiceBusOutputHandler"
        assert handler._service_bus_client is None  # Lazy initialization
    
    def test_topic_handler_initialization(self, topic_handler):
        """Test handler initialization for topics."""
        assert topic_handler.destination == "test-topic"
        assert topic_handler.destination_type == "topic"
        assert topic_handler.scheduled_enqueue_time == "2024-01-01T12:00:00Z"
        assert topic_handler.session_id is None
        assert topic_handler.time_to_live_seconds is None
    
    def test_minimal_handler_initialization(self, minimal_handler):
        """Test handler initialization with defaults from environment."""
        assert minimal_handler.destination == "minimal-queue"
        assert minimal_handler.connection_string == os.getenv("AZURE_SERVICEBUS_CONNECTION_STRING")
        assert minimal_handler.destination_type == "queue"  # Default
        assert minimal_handler.session_id is None
        assert minimal_handler.time_to_live_seconds is None
        assert minimal_handler.message_properties == {}
    
    def test_validate_configuration_success(self, handler):
        """Test successful configuration validation."""
        assert handler.validate_configuration() is True
    
    def test_validate_configuration_no_connection(self, skip_if_no_servicebus):
        """Test validation fails without connection string."""
        # Clear environment variable
        original_env = os.environ.get("AZURE_SERVICEBUS_CONNECTION_STRING")
        if "AZURE_SERVICEBUS_CONNECTION_STRING" in os.environ:
            del os.environ["AZURE_SERVICEBUS_CONNECTION_STRING"]
        
        try:
            handler = ServiceBusOutputHandler(
                destination="test-queue",
                config={}  # No connection string
            )
            assert handler.validate_configuration() is False
        finally:
            # Restore environment
            if original_env:
                os.environ["AZURE_SERVICEBUS_CONNECTION_STRING"] = original_env
    
    def test_validate_configuration_no_destination(self, skip_if_no_servicebus):
        """Test validation fails without destination."""
        handler = ServiceBusOutputHandler(
            destination="",
            config={"connection_string": "Endpoint=sb://test.servicebus.windows.net/;SharedAccessKeyName=test;SharedAccessKey=testkey="}
        )
        assert handler.validate_configuration() is False
    
    def test_validate_configuration_invalid_destination_type(self, skip_if_no_servicebus):
        """Test validation fails with invalid destination type."""
        handler = ServiceBusOutputHandler(
            destination="test-dest",
            config={
                "connection_string": "Endpoint=sb://test.servicebus.windows.net/;SharedAccessKeyName=test;SharedAccessKey=testkey=",
                "destination_type": "invalid"
            }
        )
        assert handler.validate_configuration() is False
    
    def test_validate_configuration_invalid_entity_name(self, skip_if_no_servicebus):
        """Test validation fails with invalid Service Bus entity name."""
        invalid_names = [
            "",  # Empty
            "a" * 261,  # Too long (max 260)
            "/starts-with-slash",
            "ends-with-slash/",
            "double//slash",
            "invalid@characters!",
            "spaces not allowed"
        ]
        
        for invalid_name in invalid_names:
            handler = ServiceBusOutputHandler(
                destination=invalid_name,
                config={"connection_string": "Endpoint=sb://test.servicebus.windows.net/;SharedAccessKeyName=test;SharedAccessKey=testkey="}
            )
            assert handler.validate_configuration() is False
    
    def test_is_valid_entity_name(self, handler):
        """Test Service Bus entity name validation rules."""
        # Valid names
        assert handler._is_valid_entity_name("valid-queue-name") is True
        assert handler._is_valid_entity_name("ValidTopicName") is True
        assert handler._is_valid_entity_name("queue.with.dots") is True
        assert handler._is_valid_entity_name("queue_with_underscores") is True
        assert handler._is_valid_entity_name("path/to/entity") is True
        assert handler._is_valid_entity_name("a") is True
        assert handler._is_valid_entity_name("a" * 260) is True
        
        # Invalid names
        assert handler._is_valid_entity_name("") is False
        assert handler._is_valid_entity_name("a" * 261) is False  # Too long
        assert handler._is_valid_entity_name("/starts-slash") is False
        assert handler._is_valid_entity_name("ends-slash/") is False
        assert handler._is_valid_entity_name("double//slash") is False
        assert handler._is_valid_entity_name("invalid@char") is False
        assert handler._is_valid_entity_name("space char") is False
    
    @patch('src.processors.v2.output_handlers.service_bus_output.ServiceBusClient')
    def test_get_service_bus_client_success(self, mock_client_class, handler):
        """Test successful Service Bus client creation."""
        mock_client = Mock()
        mock_client_class.from_connection_string.return_value = mock_client
        
        # Get client
        client = handler._get_service_bus_client()
        
        assert client is mock_client
        mock_client_class.from_connection_string.assert_called_once_with(
            conn_str=handler.connection_string
        )
        
        # Second call should return cached client
        client2 = handler._get_service_bus_client()
        assert client2 is mock_client
        assert mock_client_class.from_connection_string.call_count == 1
    
    @patch('src.processors.v2.output_handlers.service_bus_output.ServiceBusClient')
    def test_get_service_bus_client_failure(self, mock_client_class, handler):
        """Test Service Bus client creation failure."""
        mock_client_class.from_connection_string.side_effect = Exception("Connection failed")
        
        with pytest.raises(OutputHandlerError) as exc_info:
            handler._get_service_bus_client()
        
        error = exc_info.value
        assert error.error_code == "SERVICE_BUS_CLIENT_CREATION_FAILED"
        assert error.can_retry is False
        assert error.error_details["connection_string_provided"] is True
    
    def test_prepare_service_bus_message_success(self, handler, mock_message, mock_result):
        """Test successful Service Bus message preparation."""
        with patch('src.processors.v2.output_handlers.service_bus_output.ServiceBusMessage') as mock_sb_message_class:
            mock_sb_message = Mock()
            mock_sb_message.application_properties = {}
            mock_sb_message_class.return_value = mock_sb_message
            
            # Prepare message
            result_message = handler._prepare_service_bus_message(mock_message, mock_result)
            
            assert result_message is mock_sb_message
            
            # Verify ServiceBusMessage was created with correct parameters
            call_args = mock_sb_message_class.call_args
            assert call_args[1]["content_type"] == "application/json"
            
            # Parse and verify message body
            message_body = json.loads(call_args[1]["body"])
            
            # Verify message metadata
            assert message_body["message_metadata"]["message_id"] == mock_message.message_id
            assert message_body["message_metadata"]["correlation_id"] == mock_message.correlation_id
            assert message_body["message_metadata"]["message_type"] == "entity_processing"
            
            # Verify entity reference
            assert message_body["entity_reference"]["external_id"] == mock_message.entity_reference.external_id
            assert message_body["entity_reference"]["canonical_type"] == "test_type"
            assert message_body["entity_reference"]["tenant_id"] == "test_tenant"
            
            # Verify payload
            assert message_body["payload"] == {"service": "bus", "value": 456}
            
            # Verify processing result
            assert message_body["processing_result"]["status"] == "success"
            assert message_body["processing_result"]["entities_created"] == ["sb-entity-123"]
            assert message_body["processing_result"]["processor_info"]["name"] == "ServiceBusProcessor"
            
            # Verify routing metadata
            assert message_body["routing_metadata"]["source_handler"] == "ServiceBusOutputHandler"
            assert message_body["routing_metadata"]["target_destination"] == "test-processing-queue"
            assert message_body["routing_metadata"]["destination_type"] == "queue"
            
            # Verify message properties were set
            assert mock_sb_message.message_id == mock_message.message_id
            assert mock_sb_message.correlation_id == mock_message.correlation_id
            assert mock_sb_message.session_id == "test-session"
            assert mock_sb_message.time_to_live == 3600
            
            # Verify application properties
            expected_props = {
                "priority": "high",
                "category": "processing",
                "source_processor": "ServiceBusProcessor",
                "entity_external_id": mock_message.entity_reference.external_id,
                "entity_canonical_type": "test_type",
                "processing_status": "success",
                "tenant_id": "test_tenant"
            }
            
            for key, value in expected_props.items():
                assert mock_sb_message.application_properties[key] == value
    
    def test_prepare_service_bus_message_with_scheduled_time(self, topic_handler, mock_message, mock_result):
        """Test message preparation with scheduled enqueue time."""
        with patch('src.processors.v2.output_handlers.service_bus_output.ServiceBusMessage') as mock_sb_message_class:
            mock_sb_message = Mock()
            mock_sb_message.application_properties = {}
            mock_sb_message_class.return_value = mock_sb_message
            
            # Prepare message
            topic_handler._prepare_service_bus_message(mock_message, mock_result)
            
            # Verify scheduled enqueue time was set
            from datetime import datetime
            expected_time = datetime.fromisoformat("2024-01-01T12:00:00Z")
            assert mock_sb_message.scheduled_enqueue_time == expected_time
    
    def test_prepare_service_bus_message_failure(self, handler, mock_message):
        """Test message preparation failure."""
        # Create invalid result that will cause JSON serialization to fail
        bad_result = Mock()
        bad_result.status.value = "success"
        bad_result.success = True
        bad_result.entities_created = [lambda x: x]  # Non-serializable function
        bad_result.entities_updated = []
        bad_result.processing_metadata = {}
        bad_result.processor_info = {}
        bad_result.processing_duration_ms = 75.3
        bad_result.completed_at = datetime.now(UTC)
        
        with pytest.raises(OutputHandlerError) as exc_info:
            handler._prepare_service_bus_message(mock_message, bad_result)
        
        error = exc_info.value
        assert error.error_code == "MESSAGE_PREPARATION_FAILED"
        assert error.can_retry is False
        assert error.error_details["message_id"] == mock_message.message_id
    
    @patch('src.processors.v2.output_handlers.service_bus_output.ServiceBusClient')
    @patch('src.processors.v2.output_handlers.service_bus_output.ServiceBusMessage')
    def test_handle_queue_success(self, mock_sb_message_class, mock_client_class, handler, mock_message, mock_result):
        """Test successful queue message handling."""
        # Setup mocks
        mock_client = Mock()
        mock_sender = Mock()
        mock_sb_message = Mock()
        
        # Create a context manager mock for the sender
        mock_sender_context = Mock()
        mock_sender_context.__enter__ = Mock(return_value=mock_sender)
        mock_sender_context.__exit__ = Mock(return_value=False)
        
        mock_client_class.from_connection_string.return_value = mock_client
        mock_client.get_queue_sender.return_value = mock_sender_context
        mock_sb_message_class.return_value = mock_sb_message
        
        # Set up all the properties that might be accessed
        mock_sb_message.application_properties = {}
        mock_sb_message.message_id = None
        mock_sb_message.correlation_id = None
        mock_sb_message.session_id = None
        mock_sb_message.time_to_live = None
        mock_sb_message.scheduled_enqueue_time = None
        mock_sb_message.body = '{"test": "data"}'
        
        # Execute handler
        result = handler.handle(mock_message, mock_result)
        
        # Verify result
        assert result.success is True
        assert result.status == OutputHandlerStatus.SUCCESS
        assert result.handler_name == "ServiceBusOutputHandler"
        assert result.destination == "test-processing-queue"
        assert result.execution_duration_ms > 0
        
        # Verify metadata
        assert result.metadata["service_bus_message_id"] == mock_sb_message.message_id
        assert result.metadata["destination"] == "test-processing-queue"
        assert result.metadata["destination_type"] == "queue"
        assert result.metadata["session_id"] == "test-session"
        
        # Verify Service Bus operations
        mock_client.get_queue_sender.assert_called_once_with(queue_name="test-processing-queue")
        mock_sender.send_messages.assert_called_once_with(mock_sb_message)
    
    @patch('src.processors.v2.output_handlers.service_bus_output.ServiceBusClient')
    @patch('src.processors.v2.output_handlers.service_bus_output.ServiceBusMessage')
    def test_handle_topic_success(self, mock_sb_message_class, mock_client_class, topic_handler, mock_message, mock_result):
        """Test successful topic message handling."""
        # Setup mocks
        mock_client = Mock()
        mock_sender = Mock()
        mock_sb_message = Mock()
        
        # Create a context manager mock for the sender
        mock_sender_context = Mock()
        mock_sender_context.__enter__ = Mock(return_value=mock_sender)
        mock_sender_context.__exit__ = Mock(return_value=False)
        
        mock_client_class.from_connection_string.return_value = mock_client
        mock_client.get_topic_sender.return_value = mock_sender_context
        mock_sb_message_class.return_value = mock_sb_message
        
        # Set up all the properties that might be accessed
        mock_sb_message.application_properties = {}
        mock_sb_message.message_id = None
        mock_sb_message.correlation_id = None
        mock_sb_message.session_id = None
        mock_sb_message.time_to_live = None
        mock_sb_message.scheduled_enqueue_time = None
        mock_sb_message.body = '{"test": "data"}'
        
        # Execute handler
        result = topic_handler.handle(mock_message, mock_result)
        
        # Verify result
        assert result.success is True
        assert result.metadata["destination_type"] == "topic"
        
        # Verify Service Bus operations
        mock_client.get_topic_sender.assert_called_once_with(topic_name="test-topic")
        mock_sender.send_messages.assert_called_once_with(mock_sb_message)
    
    @patch('src.processors.v2.output_handlers.service_bus_output.ServiceBusClient')
    def test_handle_service_request_error(self, mock_client_class, handler, mock_message, mock_result):
        """Test handling of Service Bus service errors."""
        from azure.core.exceptions import ServiceRequestError
        
        mock_client = Mock()
        mock_sender = Mock()
        
        # Create a context manager mock for the sender
        mock_sender_context = Mock()
        mock_sender_context.__enter__ = Mock(return_value=mock_sender)
        mock_sender_context.__exit__ = Mock(return_value=False)
        
        mock_client_class.from_connection_string.return_value = mock_client
        mock_client.get_queue_sender.return_value = mock_sender_context
        
        # Configure sender to raise ServiceRequestError
        mock_sender.send_messages.side_effect = ServiceRequestError("Service unavailable")
        
        with patch('src.processors.v2.output_handlers.service_bus_output.ServiceBusMessage') as mock_sb_message_class:
            mock_sb_message = Mock()
            mock_sb_message.application_properties = {}
            mock_sb_message.message_id = None
            mock_sb_message.correlation_id = None
            mock_sb_message.session_id = None
            mock_sb_message.time_to_live = None
            mock_sb_message.scheduled_enqueue_time = None
            mock_sb_message.body = '{"test": "data"}'
            mock_sb_message_class.return_value = mock_sb_message
            
            # Execute handler
            result = handler.handle(mock_message, mock_result)
            
            # Verify failure result
            assert result.success is False
            assert result.status == OutputHandlerStatus.RETRYABLE_ERROR
            assert result.error_code == "SERVICE_BUS_SERVICE_ERROR"
            assert result.can_retry is True
            # With exponential backoff and retry_count=0, base_delay=5: expect 5 seconds
            assert result.retry_after_seconds == 5
            assert "Service Bus service error" in result.error_message
            # Verify exponential backoff metadata is present
            assert "calculated_backoff_delay" in result.error_details
            assert "backoff_algorithm" in result.error_details
    
    def test_handle_invalid_configuration(self, skip_if_no_servicebus, mock_message, mock_result):
        """Test handling with invalid configuration."""
        handler = ServiceBusOutputHandler(
            destination="",  # Empty destination
            config={"connection_string": "Endpoint=sb://test.servicebus.windows.net/;SharedAccessKeyName=test;SharedAccessKey=testkey="}
        )
        
        # Execute handler - should fail validation
        result = handler.handle(mock_message, mock_result)
        
        # Verify failure
        assert result.success is False
        assert result.status == OutputHandlerStatus.FAILED
        assert result.error_code == "INVALID_CONFIGURATION"
        assert result.can_retry is False
    
    @patch('src.processors.v2.output_handlers.service_bus_output.ServiceBusClient')
    def test_handle_unexpected_error(self, mock_client_class, handler, mock_message, mock_result):
        """Test handling of unexpected errors."""
        mock_client = Mock()
        mock_sender = Mock()
        
        # Create a context manager mock for the sender
        mock_sender_context = Mock()
        mock_sender_context.__enter__ = Mock(return_value=mock_sender)
        mock_sender_context.__exit__ = Mock(return_value=False)
        
        mock_client_class.from_connection_string.return_value = mock_client
        mock_client.get_queue_sender.return_value = mock_sender_context
        
        # Configure sender to raise unexpected error
        mock_sender.send_messages.side_effect = RuntimeError("Unexpected error")
        
        with patch('src.processors.v2.output_handlers.service_bus_output.ServiceBusMessage') as mock_sb_message_class:
            mock_sb_message = Mock()
            mock_sb_message.application_properties = {}
            mock_sb_message.message_id = None
            mock_sb_message.correlation_id = None
            mock_sb_message.session_id = None
            mock_sb_message.time_to_live = None
            mock_sb_message.scheduled_enqueue_time = None
            mock_sb_message.body = '{"test": "data"}'
            mock_sb_message_class.return_value = mock_sb_message
            
            # Execute handler
            result = handler.handle(mock_message, mock_result)
            
            # Verify failure result
            assert result.success is False
            assert result.status == OutputHandlerStatus.RETRYABLE_ERROR
            assert result.error_code == "SERVICE_BUS_SEND_FAILED"
            assert result.can_retry is True
            # With exponential backoff and retry_count=0, base_delay=2: expect 2 seconds
            assert result.retry_after_seconds == 2
            # Verify exponential backoff metadata is present
            assert "calculated_backoff_delay" in result.error_details
            assert "backoff_algorithm" in result.error_details
    
    @patch('src.processors.v2.output_handlers.service_bus_output.ServiceBusClient')
    def test_handle_unsupported_destination_type(self, mock_client_class, skip_if_no_servicebus, mock_message, mock_result):
        """Test handling with unsupported destination type."""
        handler = ServiceBusOutputHandler(
            destination="test-dest",
            config={
                "connection_string": "Endpoint=sb://test.servicebus.windows.net/;SharedAccessKeyName=test;SharedAccessKey=testkey=",
                "destination_type": "queue"  # Start with valid type
            }
        )
        
        # Patch destination_type after creation to bypass validation
        handler.destination_type = "subscription"
        
        mock_client = Mock()
        mock_client_class.from_connection_string.return_value = mock_client
        
        with patch('src.processors.v2.output_handlers.service_bus_output.ServiceBusMessage') as mock_sb_message_class:
            mock_sb_message = Mock()
            mock_sb_message.application_properties = {}
            mock_sb_message.message_id = None
            mock_sb_message.correlation_id = None
            mock_sb_message.session_id = None
            mock_sb_message.time_to_live = None
            mock_sb_message.scheduled_enqueue_time = None
            mock_sb_message.body = '{"test": "data"}'
            mock_sb_message_class.return_value = mock_sb_message
            
            # Execute handler
            result = handler.handle(mock_message, mock_result)
            
            # Verify failure - validation catches invalid destination type first
            assert result.success is False
            assert result.status == OutputHandlerStatus.FAILED
            assert result.error_code == "INVALID_CONFIGURATION"
            assert result.can_retry is False
    
    def test_get_handler_info(self, handler):
        """Test get_handler_info returns expected metadata."""
        info = handler.get_handler_info()
        
        assert info["handler_name"] == "ServiceBusOutputHandler"
        assert info["destination"] == "test-processing-queue"
        assert info["destination_type"] == "queue"
        assert info["session_id"] == "test-session"
        assert info["time_to_live_seconds"] == 3600
        assert info["scheduled_enqueue_time"] is None
        assert info["message_properties_count"] == 2
        assert info["connection_configured"] is True
        assert info["supports_retry"] is True
    
    def test_supports_retry(self, handler):
        """Test supports_retry returns True."""
        assert handler.supports_retry() is True
    
    def test_handler_repr(self, handler):
        """Test string representation."""
        assert repr(handler) == "ServiceBusOutputHandler(destination='test-processing-queue')"
    
    def test_handler_str(self, handler):
        """Test human-readable string."""
        assert str(handler) == "ServiceBusOutputHandler -> test-processing-queue"
    
    def test_handler_cleanup(self, handler):
        """Test handler cleanup on destruction."""
        with patch('src.processors.v2.output_handlers.service_bus_output.ServiceBusClient') as mock_client_class:
            mock_client = Mock()
            mock_client_class.from_connection_string.return_value = mock_client
            
            # Initialize client
            client = handler._get_service_bus_client()
            assert client is mock_client
            
            # Manually call __del__ to test cleanup logic
            handler.__del__()
            
            # Verify client.close() was called
            mock_client.close.assert_called_once()
    
    def test_handler_cleanup_with_exception(self, handler):
        """Test handler cleanup handles exceptions gracefully."""
        with patch('src.processors.v2.output_handlers.service_bus_output.ServiceBusClient') as mock_client_class:
            mock_client = Mock()
            mock_client.close.side_effect = Exception("Cleanup error")
            mock_client_class.from_connection_string.return_value = mock_client
            
            # Initialize client
            handler._get_service_bus_client()
            
            # Delete handler - should not raise exception
            try:
                del handler
            except Exception:
                pytest.fail("Handler cleanup should not raise exceptions")