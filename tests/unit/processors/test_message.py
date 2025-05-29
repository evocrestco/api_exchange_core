"""
Tests for processor message classes.

Tests use real data and real code paths following the NO MOCKS philosophy.
Tests cover Message, EntityReference, and MessageType functionality.
"""

import pytest
from datetime import datetime
from uuid import UUID

from src.processors.message import EntityReference, Message, MessageType


class TestEntityReference:
    """Test EntityReference model and operations."""
    
    def test_create_entity_reference_minimal(self):
        """Test creating entity reference with minimal required fields."""
        entity_ref = EntityReference(
            external_id="ORDER-123",
            canonical_type="order",
            source="shopify",
            tenant_id="test-tenant"
        )
        
        assert entity_ref.external_id == "ORDER-123"
        assert entity_ref.canonical_type == "order"
        assert entity_ref.source == "shopify" 
        assert entity_ref.tenant_id == "test-tenant"
        assert entity_ref.entity_id is None
        assert entity_ref.version is None
    
    def test_create_entity_reference_complete(self):
        """Test creating entity reference with all fields."""
        entity_ref = EntityReference(
            entity_id="entity-456",
            external_id="ORDER-123",
            canonical_type="order",
            source="shopify",
            tenant_id="test-tenant",
            version=2
        )
        
        assert entity_ref.entity_id == "entity-456"
        assert entity_ref.external_id == "ORDER-123"
        assert entity_ref.canonical_type == "order"
        assert entity_ref.source == "shopify"
        assert entity_ref.tenant_id == "test-tenant"
        assert entity_ref.version == 2


class TestMessage:
    """Test Message model and operations."""
    
    @pytest.fixture
    def sample_entity_reference(self):
        """Create sample entity reference for testing."""
        return EntityReference(
            external_id="ORDER-123",
            canonical_type="order",
            source="shopify",
            tenant_id="test-tenant"
        )
    
    @pytest.fixture
    def sample_payload(self):
        """Create sample payload data for testing."""
        return {
            "order_id": "ORDER-123",
            "customer_name": "John Doe",
            "total_amount": 99.99,
            "items": [
                {"sku": "ITEM-1", "quantity": 2, "price": 49.99}
            ]
        }
    
    def test_create_message_minimal(self, sample_entity_reference, sample_payload):
        """Test creating message with minimal required fields."""
        message = Message(
            entity_reference=sample_entity_reference,
            payload=sample_payload
        )
        
        # Check required fields
        assert message.entity_reference == sample_entity_reference
        assert message.payload == sample_payload
        
        # Check defaults
        assert message.message_type == MessageType.ENTITY_PROCESSING
        assert message.metadata == {}
        assert message.routing_info == {}
        assert message.retry_count == 0
        assert message.max_retries == 3
        assert message.processed_at is None
        
        # Check generated fields
        assert isinstance(UUID(message.message_id), UUID)  # Valid UUID
        assert isinstance(UUID(message.correlation_id), UUID)  # Valid UUID
        assert isinstance(message.created_at, datetime)
    
    def test_create_message_complete(self, sample_entity_reference, sample_payload):
        """Test creating message with all fields specified."""
        correlation_id = "custom-correlation-123"
        metadata = {"processor": "test", "priority": "high"}
        routing_info = {"destination": "test-queue"}
        
        message = Message(
            message_id="custom-message-123",
            correlation_id=correlation_id,
            message_type=MessageType.CONTROL_MESSAGE,
            entity_reference=sample_entity_reference,
            payload=sample_payload,
            metadata=metadata,
            routing_info=routing_info,
            retry_count=1,
            max_retries=5
        )
        
        assert message.message_id == "custom-message-123"
        assert message.correlation_id == correlation_id
        assert message.message_type == MessageType.CONTROL_MESSAGE
        assert message.metadata == metadata
        assert message.routing_info == routing_info
        assert message.retry_count == 1
        assert message.max_retries == 5
    
    def test_create_entity_message_factory(self):
        """Test Message.create_entity_message factory method."""
        message = Message.create_entity_message(
            external_id="ORDER-123",
            canonical_type="order",
            source="shopify",
            tenant_id="test-tenant",
            payload={"test": "data"},
            metadata={"processor": "test"}
        )
        
        assert message.message_type == MessageType.ENTITY_PROCESSING
        assert message.entity_reference.external_id == "ORDER-123"
        assert message.entity_reference.canonical_type == "order"
        assert message.entity_reference.source == "shopify"
        assert message.entity_reference.tenant_id == "test-tenant"
        assert message.payload == {"test": "data"}
        assert message.metadata == {"processor": "test"}
    
    def test_create_entity_message_with_existing_entity(self):
        """Test creating message for existing entity with ID and version."""
        message = Message.create_entity_message(
            external_id="ORDER-123",
            canonical_type="order",
            source="shopify",
            tenant_id="test-tenant",
            payload={"test": "data"},
            entity_id="existing-entity-456",
            version=3,
            correlation_id="custom-correlation"
        )
        
        assert message.entity_reference.entity_id == "existing-entity-456"
        assert message.entity_reference.version == 3
        assert message.correlation_id == "custom-correlation"
    
    def test_create_control_message_factory(self):
        """Test Message.create_control_message factory method."""
        message = Message.create_control_message(
            command="pause",
            tenant_id="test-tenant",
            payload={"duration": 300},
            correlation_id="control-123"
        )
        
        assert message.message_type == MessageType.CONTROL_MESSAGE
        assert message.entity_reference.external_id == "control-pause"
        assert message.entity_reference.canonical_type == "control"
        assert message.entity_reference.source == "system"
        assert message.entity_reference.tenant_id == "test-tenant"
        assert message.payload == {"duration": 300, "command": "pause"}
        assert message.metadata == {"command": "pause"}
        assert message.correlation_id == "control-123"
    
    def test_mark_processed(self, sample_entity_reference, sample_payload):
        """Test marking message as processed."""
        message = Message(
            entity_reference=sample_entity_reference,
            payload=sample_payload
        )
        
        assert message.processed_at is None
        
        message.mark_processed()
        
        assert message.processed_at is not None
        assert isinstance(message.processed_at, datetime)
    
    def test_increment_retry(self, sample_entity_reference, sample_payload):
        """Test incrementing retry count."""
        message = Message(
            entity_reference=sample_entity_reference,
            payload=sample_payload
        )
        
        assert message.retry_count == 0
        
        message.increment_retry()
        assert message.retry_count == 1
        
        message.increment_retry()
        assert message.retry_count == 2
    
    def test_can_retry(self, sample_entity_reference, sample_payload):
        """Test retry capability checking."""
        message = Message(
            entity_reference=sample_entity_reference,
            payload=sample_payload,
            max_retries=2
        )
        
        # Should be able to retry initially (retry_count=0)
        assert message.can_retry() is True
        
        # Still can retry after first increment (retry_count=1)
        message.increment_retry()
        assert message.can_retry() is True
        
        # Cannot retry after second increment (retry_count=2, which equals max_retries)
        message.increment_retry()
        assert message.can_retry() is False
    
    def test_add_metadata(self, sample_entity_reference, sample_payload):
        """Test adding metadata to message."""
        message = Message(
            entity_reference=sample_entity_reference,
            payload=sample_payload
        )
        
        assert message.metadata == {}
        
        message.add_metadata("processor", "test-processor")
        assert message.metadata == {"processor": "test-processor"}
        
        message.add_metadata("priority", "high")
        assert message.metadata == {"processor": "test-processor", "priority": "high"}
    
    def test_add_routing_info(self, sample_entity_reference, sample_payload):
        """Test adding routing information to message."""
        message = Message(
            entity_reference=sample_entity_reference,
            payload=sample_payload
        )
        
        assert message.routing_info == {}
        
        message.add_routing_info("destination", "processing-queue")
        assert message.routing_info == {"destination": "processing-queue"}
        
        message.add_routing_info("priority", "high")
        assert message.routing_info == {"destination": "processing-queue", "priority": "high"}
    
    def test_get_processing_context(self, sample_entity_reference, sample_payload):
        """Test getting processing context from message."""
        message = Message(
            message_id="test-message-123",
            correlation_id="test-correlation-456",
            message_type=MessageType.ENTITY_PROCESSING,
            entity_reference=sample_entity_reference,
            payload=sample_payload
        )
        
        context = message.get_processing_context()
        
        expected_context = {
            "tenant_id": "test-tenant",
            "correlation_id": "test-correlation-456", 
            "message_id": "test-message-123",
            "entity_external_id": "ORDER-123",
            "entity_source": "shopify",
            "message_type": "entity_processing"
        }
        
        assert context == expected_context


class TestMessageType:
    """Test MessageType enum."""
    
    def test_message_type_values(self):
        """Test message type enum values."""
        assert MessageType.ENTITY_PROCESSING == "entity_processing"
        assert MessageType.CONTROL_MESSAGE == "control_message"
        assert MessageType.ERROR_MESSAGE == "error_message"
        assert MessageType.HEARTBEAT == "heartbeat"
        assert MessageType.METRICS == "metrics"
    
    def test_message_type_usage(self):
        """Test using message types in message creation."""
        entity_ref = EntityReference(
            external_id="TEST-123",
            canonical_type="test",
            source="test",
            tenant_id="test-tenant"
        )
        
        # Test each message type
        for msg_type in MessageType:
            message = Message(
                message_type=msg_type,
                entity_reference=entity_ref,
                payload={"test": "data"}
            )
            assert message.message_type == msg_type