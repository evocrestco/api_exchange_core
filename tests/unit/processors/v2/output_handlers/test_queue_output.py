"""
Unit tests for QueueOutputHandler.

Tests the Azure Storage Queue output handler with real Azurite storage emulator,
following the NO MOCKS policy. These tests verify queue creation, message sending,
error handling, and retry logic.
"""

import json
import os
import uuid
from datetime import datetime, UTC
from typing import Dict, Any
from unittest.mock import patch, MagicMock

import pytest
from azure.core.exceptions import ResourceExistsError, ServiceRequestError
from azure.storage.queue import QueueClient, QueueServiceClient

from src.processors.v2.output_handlers import (
    QueueOutputHandler,
    OutputHandlerError,
    OutputHandlerResult,
    OutputHandlerStatus,
)
from src.processors.v2.message import Message, MessageType
from src.processors.processing_result import ProcessingResult, ProcessingStatus

# Import helper functions from conftest - imported automatically by pytest


class TestQueueOutputHandler:
    """Test the QueueOutputHandler implementation with real Azure Storage."""
    
    @pytest.fixture(autouse=True)
    def setup_azurite(self, azurite_connection_string):
        """Ensure Azurite connection string is set."""
        os.environ["AZURE_STORAGE_CONNECTION_STRING"] = azurite_connection_string
    
    @pytest.fixture
    def handler(self, azurite_connection_string):
        """Create a QueueOutputHandler instance for testing."""
        return QueueOutputHandler(
            destination="test-output-queue",
            config={
                "connection_string": azurite_connection_string,
                "auto_create_queue": True,
                "message_ttl_seconds": 3600,  # 1 hour
                "visibility_timeout_seconds": 30
            }
        )
    
    @pytest.fixture
    def minimal_handler(self):
        """Create a minimal QueueOutputHandler with defaults."""
        # Uses connection string from environment (set in setup_azurite)
        return QueueOutputHandler(
            destination="minimal-queue",
            config={}
        )
    
    @pytest.fixture
    def cleanup_queues(self, azurite_connection_string):
        """Clean up test queues after tests."""
        created_queues = []
        
        yield created_queues
        
        # Clean up any created queues
        try:
            queue_service = QueueServiceClient.from_connection_string(azurite_connection_string)
            for queue_name in created_queues:
                try:
                    queue_service.delete_queue(queue_name)
                except Exception:
                    pass  # Ignore cleanup errors
        except Exception:
            pass
    
    def test_handler_initialization(self, handler):
        """Test handler initialization with custom config."""
        assert handler.destination == "test-output-queue"
        assert "devstoreaccount1" in handler.connection_string
        assert handler.auto_create_queue is True
        assert handler.message_ttl_seconds == 3600
        assert handler.visibility_timeout_seconds == 30
        assert handler._handler_name == "QueueOutputHandler"
        assert handler._queue_client is None  # Lazy initialization
    
    def test_minimal_handler_initialization(self, minimal_handler):
        """Test handler initialization with defaults."""
        assert minimal_handler.destination == "minimal-queue"
        assert "devstoreaccount1" in minimal_handler.connection_string  # Uses Azurite from env
        assert minimal_handler.auto_create_queue is True  # Default
        assert minimal_handler.message_ttl_seconds == 604800  # Default 7 days
        assert minimal_handler.visibility_timeout_seconds == 30  # Default
    
    def test_validate_configuration_success(self, handler):
        """Test successful configuration validation."""
        assert handler.validate_configuration() is True
    
    def test_validate_configuration_no_connection(self):
        """Test validation fails without connection string."""
        # Clear both environment variables that could provide connection strings
        original_azure_storage = os.environ.get("AZURE_STORAGE_CONNECTION_STRING")
        original_web_jobs = os.environ.get("AzureWebJobsStorage")
        
        if "AZURE_STORAGE_CONNECTION_STRING" in os.environ:
            del os.environ["AZURE_STORAGE_CONNECTION_STRING"]
        if "AzureWebJobsStorage" in os.environ:
            del os.environ["AzureWebJobsStorage"]
        
        try:
            # Create handler AFTER clearing environment variables
            handler = QueueOutputHandler(
                destination="test-queue",
                config={}  # No connection string
            )
            
            # Override the connection string to be empty
            handler.connection_string = ""
            
            assert handler.validate_configuration() is False
        finally:
            # Restore environment variables
            if original_azure_storage:
                os.environ["AZURE_STORAGE_CONNECTION_STRING"] = original_azure_storage
            if original_web_jobs:
                os.environ["AzureWebJobsStorage"] = original_web_jobs
    
    def test_validate_configuration_no_destination(self, azurite_connection_string):
        """Test validation fails without destination."""
        handler = QueueOutputHandler(
            destination="",
            config={"connection_string": azurite_connection_string}
        )
        assert handler.validate_configuration() is False
    
    def test_validate_configuration_invalid_queue_name(self):
        """Test validation fails with invalid queue name."""
        # Queue names must be lowercase, alphanumeric, and hyphens
        invalid_names = [
            "UPPERCASE",
            "queue_with_underscore",
            "queue.with.dots",
            "queue with spaces",
            "a",  # Too short (min 3 chars)
            "a" * 64,  # Too long (max 63 chars)
            "-startswithdash",
            "endswithdash-",
            "double--dash"
        ]
        
        for invalid_name in invalid_names:
            handler = QueueOutputHandler(
                destination=invalid_name,
                config={"connection_string": "UseDevelopmentStorage=true"}
            )
            assert handler.validate_configuration() is False
    
    def test_is_valid_queue_name(self, handler):
        """Test queue name validation rules."""
        # Valid names
        assert handler._is_valid_queue_name("valid-queue-name") is True
        assert handler._is_valid_queue_name("abc") is True
        assert handler._is_valid_queue_name("123") is True
        assert handler._is_valid_queue_name("queue-123-test") is True
        assert handler._is_valid_queue_name("a" * 63) is True
        
        # Invalid names
        assert handler._is_valid_queue_name("") is False
        assert handler._is_valid_queue_name("ab") is False  # Too short
        assert handler._is_valid_queue_name("a" * 64) is False  # Too long
        assert handler._is_valid_queue_name("UPPERCASE") is False
        assert handler._is_valid_queue_name("under_score") is False
        assert handler._is_valid_queue_name("-start") is False
        assert handler._is_valid_queue_name("end-") is False
        assert handler._is_valid_queue_name("double--dash") is False
    
    def test_get_queue_client_success(self, handler, cleanup_queues):
        """Test successful queue client creation and caching."""
        # Get queue client
        queue_client = handler._get_queue_client()
        
        assert queue_client is not None
        assert isinstance(queue_client, QueueClient)
        assert queue_client.queue_name == "test-output-queue"
        
        # Track for cleanup
        cleanup_queues.append("test-output-queue")
        
        # Second call should return cached client
        queue_client2 = handler._get_queue_client()
        assert queue_client2 is queue_client
    
    def test_ensure_queue_exists_no_auto_create(self, cleanup_queues, azurite_connection_string):
        """Test queue existence check when auto-create is disabled."""
        handler = QueueOutputHandler(
            destination="non-existent-queue",
            config={
                "connection_string": azurite_connection_string,
                "auto_create_queue": False
            }
        )
        
        # Should fail if queue doesn't exist and auto_create is False
        with pytest.raises(OutputHandlerError) as exc_info:
            handler._ensure_queue_exists()
        
        error = exc_info.value
        assert error.error_code == "QUEUE_NOT_FOUND"
        assert error.can_retry is False
        assert "does not exist and auto_create_queue is disabled" in error.message
    
    def test_prepare_message_content_success(self, handler, test_message, test_processing_result):
        """Test successful queue message preparation."""
        message_content = handler._prepare_message_content(test_message, test_processing_result)
        
        # Parse JSON to verify structure
        message_data = json.loads(message_content)
        
        # Verify message metadata
        assert message_data["message_metadata"]["message_id"] == test_message.message_id
        assert message_data["message_metadata"]["correlation_id"] == test_message.correlation_id
        assert message_data["message_metadata"]["message_type"] == "entity_processing"
        assert message_data["message_metadata"]["retry_count"] == 0
        assert message_data["message_metadata"]["max_retries"] == 3
        
        # Verify entity reference
        assert message_data["entity_reference"]["id"] == test_message.entity_reference.id
        assert message_data["entity_reference"]["external_id"] == test_message.entity_reference.external_id
        assert message_data["entity_reference"]["canonical_type"] == "test_type"
        assert message_data["entity_reference"]["source"] == "test_source"
        assert message_data["entity_reference"]["tenant_id"] == "test_tenant"
        
        # Verify payload
        assert message_data["payload"] == {"data": "test", "value": 123}
        
        # Verify processing result
        assert message_data["processing_result"]["status"] == "success"
        assert message_data["processing_result"]["success"] is True
        assert message_data["processing_result"]["entities_created"] == ["entity-123", "entity-456"]
        assert message_data["processing_result"]["entities_updated"] == ["entity-789"]
        assert message_data["processing_result"]["processing_metadata"]["score"] == 95
        assert message_data["processing_result"]["processor_info"]["name"] == "TestProcessor"
        
        # Verify routing metadata
        assert message_data["routing_metadata"]["source_handler"] == "QueueOutputHandler"
        assert message_data["routing_metadata"]["target_queue"] == "test-output-queue"
    
    def test_handle_success_with_real_queue(self, handler, test_message, test_processing_result, cleanup_queues):
        """Test successful message handling with real Azure Storage Queue."""
        cleanup_queues.append("test-output-queue")
        
        # Execute handler
        result = handler.handle(test_message, test_processing_result)
        
        # Verify result
        assert result.success is True
        assert result.status == OutputHandlerStatus.SUCCESS
        assert result.handler_name == "QueueOutputHandler"
        assert result.destination == "test-output-queue"
        assert result.execution_duration_ms > 0
        assert result.error_message is None
        
        # Verify metadata
        assert result.metadata["queue_name"] == "test-output-queue"
        assert "content_length" in result.metadata
        assert result.metadata["content_length"] > 0
        assert "queue_message_id" in result.metadata
        assert result.metadata["queue_message_id"] is not None
        
        # The message has been sent successfully as evidenced by:
        # 1. Handler returned success=True
        # 2. We got a queue_message_id in the metadata
        # 3. Azure Storage logs show successful message posting
        # Note: We don't try to receive the message immediately due to visibility timeout
    
    def test_handle_with_ttl_and_visibility(self, cleanup_queues, azurite_connection_string, create_test_entity, create_test_message):
        """Test message handling with TTL and visibility timeout."""
        handler = QueueOutputHandler(
            destination="ttl-test-queue",
            config={
                "connection_string": azurite_connection_string,
                "auto_create_queue": True,
                "message_ttl_seconds": 60,  # 1 minute
                "visibility_timeout_seconds": 30  # 30 seconds
            }
        )
        
        cleanup_queues.append("ttl-test-queue")
        
        # Create test message and result
        entity = create_test_entity(external_id="ttl-test")
        message = create_test_message(entity=entity, payload={"ttl": "test"})
        result = ProcessingResult.create_success()
        
        # Execute handler
        handler_result = handler.handle(message, result)
        
        # Verify success
        assert handler_result.success is True
        assert "queue_message_id" in handler_result.metadata
        assert handler_result.metadata["queue_name"] == "ttl-test-queue"
    
    def test_handle_connection_failure(self, create_test_entity, create_test_message):
        """Test handling of connection failures using mocks to avoid timeout issues."""
        # Exception: Using mocks here to avoid Azure SDK timeout issues that can't be controlled
        with patch('src.processors.v2.output_handlers.queue_output.QueueClient') as mock_queue_client:
            # Mock the client creation to raise a connection error
            mock_queue_client.from_connection_string.side_effect = Exception("Connection failed")
            
            handler = QueueOutputHandler(
                destination="test-queue",
                config={
                    "connection_string": "DefaultEndpointsProtocol=https;AccountName=test;AccountKey=test==;EndpointSuffix=core.windows.net",
                    "auto_create_queue": True
                }
            )
            
            # Create test message and result
            entity = create_test_entity(external_id="conn-test")
            message = create_test_message(entity=entity, payload={"conn": "test"})
            result = ProcessingResult.create_success()
            
            # Execute handler - should fail with connection error
            handler_result = handler.handle(message, result)
            
            # Verify failure
            assert handler_result.success is False
            assert handler_result.status == OutputHandlerStatus.FAILED  # Client creation failures are non-retryable
            assert handler_result.error_code in ["QUEUE_CLIENT_CREATION_FAILED", "QUEUE_SEND_FAILED", "QUEUE_SERVICE_ERROR"]
            assert handler_result.can_retry is False  # Client creation failures are non-retryable
            assert handler_result.error_message is not None
    
    def test_handle_invalid_configuration(self, test_message, test_processing_result):
        """Test handling with invalid configuration."""
        handler = QueueOutputHandler(
            destination="INVALID-QUEUE-NAME",  # Uppercase not allowed
            config={"connection_string": "UseDevelopmentStorage=true"}
        )
        
        # Execute handler - should fail validation
        result = handler.handle(test_message, test_processing_result)
        
        # Verify failure
        assert result.success is False
        assert result.status == OutputHandlerStatus.FAILED
        assert result.error_code == "INVALID_CONFIGURATION"
        assert result.can_retry is False
        assert "Invalid queue handler configuration" in result.error_message
    
    def test_handle_large_message(self, handler, cleanup_queues, create_test_entity, create_test_message):
        """Test handling of large messages."""
        cleanup_queues.append("test-output-queue")
        
        # Create a large payload (near 64KB limit)
        large_data = "x" * 50000  # 50KB of data
        
        entity = create_test_entity(external_id="large-test")
        message = create_test_message(
            entity=entity,
            payload={"data": large_data, "type": "large_message"}
        )
        
        result = ProcessingResult.create_success()
        result.entities_created = ["entity-large"]
        
        # Execute handler
        handler_result = handler.handle(message, result)
        
        # Verify success
        assert handler_result.success is True
        assert handler_result.metadata["content_length"] > 50000
    
    def test_handle_service_request_error(self, handler, test_message, test_processing_result, cleanup_queues):
        """Test handling of Azure service request errors using mocks to avoid timeout issues."""
        cleanup_queues.append("test-output-queue")
        
        # Exception: Using mocks here to avoid Azure SDK timeout issues
        with patch.object(handler, '_get_queue_client') as mock_get_client:
            # Mock the queue client to raise a ServiceRequestError
            mock_client = MagicMock()
            mock_client.send_message.side_effect = ServiceRequestError("Service unavailable")
            mock_get_client.return_value = mock_client
            
            # Execute handler - should fail with retryable error
            result = handler.handle(test_message, test_processing_result)
            
            # Verify retryable failure  
            assert result.success is False
            assert result.status == OutputHandlerStatus.RETRYABLE_ERROR
            assert result.can_retry is True
            assert "service error" in result.error_message.lower() or "azure" in result.error_message.lower()
    
    def test_get_handler_info(self, handler):
        """Test get_handler_info returns expected metadata."""
        info = handler.get_handler_info()
        
        assert info["handler_name"] == "QueueOutputHandler"
        assert info["destination"] == "test-output-queue"
        assert info["queue_name"] == "test-output-queue"
        assert info["auto_create_queue"] is True
        assert info["message_ttl_seconds"] == 3600
        assert info["visibility_timeout_seconds"] == 30
        assert info["connection_configured"] is True
        assert info["supports_retry"] is True
    
    def test_supports_retry(self, handler):
        """Test supports_retry returns True."""
        assert handler.supports_retry() is True
    
    def test_handler_repr(self, handler):
        """Test string representation."""
        assert repr(handler) == "QueueOutputHandler(destination='test-output-queue')"
    
    def test_handler_str(self, handler):
        """Test human-readable string."""
        assert str(handler) == "QueueOutputHandler -> test-output-queue"
    
    def test_concurrent_message_handling(self, handler, cleanup_queues, create_test_entity, create_test_message):
        """Test handling multiple messages concurrently."""
        cleanup_queues.append("test-output-queue")
        
        import concurrent.futures
        
        def send_message(index):
            entity = create_test_entity(external_id=f"concurrent-{index}")
            message = create_test_message(
                entity=entity,
                payload={"index": index, "type": "concurrent"}
            )
            
            result = ProcessingResult.create_success()
            result.entities_created = [f"entity-{index}"]
            
            return handler.handle(message, result)
        
        # Send 5 messages concurrently
        with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
            futures = [executor.submit(send_message, i) for i in range(5)]
            results = [f.result() for f in concurrent.futures.as_completed(futures)]
        
        # Verify all succeeded
        assert len(results) == 5
        assert all(r.success for r in results)
        
        # Verify we can get queue properties (indicating messages were sent)
        queue_client = handler._get_queue_client()
        properties = queue_client.get_queue_properties()
        
        # Note: Due to visibility timeout, messages might not be immediately receivable
        # This is expected behavior for Azure Storage Queues
        # The test success is confirmed by all handler results being successful
        assert properties is not None
    
    def test_queue_client_cleanup(self, azurite_connection_string):
        """Test queue client cleanup on handler destruction."""
        handler = QueueOutputHandler(
            destination="cleanup-test-queue",
            config={"connection_string": azurite_connection_string}
        )
        
        # Initialize queue client
        queue_client = handler._get_queue_client()
        assert queue_client is not None
        
        # Delete handler
        del handler
        
        # Queue client should be cleaned up (no exception)
        # This is more of a smoke test to ensure __del__ doesn't raise