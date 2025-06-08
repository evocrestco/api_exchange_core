"""
Unit tests for Message v2 class and its interactions.

Tests the Message class contract, entity integration, and lifecycle methods
using real Entity objects per the NO MOCKS policy.
"""

from datetime import datetime
from typing import Any, Dict

import pytest

from src.context.tenant_context import tenant_context as tenant_ctx
from src.processors.v2.message import Message, MessageType


class TestMessageCreation:
    """Test Message creation and initialization."""
    
    @pytest.mark.parametrize("message_type", [
        MessageType.ENTITY_PROCESSING,
        MessageType.CONTROL_MESSAGE,
        MessageType.ERROR_MESSAGE,
        MessageType.HEARTBEAT,
        MessageType.METRICS
    ])
    def test_create_entity_message_with_types(
        self, entity_service, tenant_context, message_type
    ):
        """Test Message creation with different message types."""
        with tenant_ctx(tenant_context["id"]):
            # Create real entity using entity service
            entity_id = entity_service.create_entity(
                external_id="test-message-001",
                canonical_type="test_type",
                source="test_source",
                attributes={"test": True}
            )
            entity = entity_service.get_entity(entity_id)
            
            # Create message with real entity
            payload = {"test_data": "message content", "value": 42}
            message = Message.create_entity_message(
                entity=entity,
                payload=payload
            )
            
            # Override message type for test
            message.message_type = message_type
            
            # Verify message structure
            assert message.entity == entity
            assert message.payload == payload
            assert message.message_type == message_type
            assert message.message_id is not None
            assert message.correlation_id is not None
            assert isinstance(message.created_at, datetime)
            assert message.processed_at is None
            assert message.retry_count == 0
            assert message.max_retries == 3
    
    def test_message_defaults(self, entity_service, tenant_context):
        """Test Message default values and auto-generation."""
        with tenant_ctx(tenant_context["id"]):
            # Create real entity
            entity_id = entity_service.create_entity(
                external_id="test-defaults-001",
                canonical_type="default_test",
                source="test_source",
                attributes={}
            )
            entity = entity_service.get_entity(entity_id)
            
            # Create message with minimal data
            message = Message.create_entity_message(
                entity=entity,
                payload={"minimal": "data"}
            )
            
            # Verify auto-generated values
            assert len(message.message_id) > 0
            assert len(message.correlation_id) > 0
            assert message.message_type == MessageType.ENTITY_PROCESSING
            assert message.metadata == {}
            assert message.routing_info == {}
            assert message.retry_count == 0
            assert message.max_retries == 3
            assert message.processed_at is None
            assert isinstance(message.created_at, datetime)
    
    def test_custom_correlation_id_and_metadata(self, entity_service, tenant_context):
        """Test Message creation with custom correlation_id and metadata."""
        with tenant_ctx(tenant_context["id"]):
            # Create real entity
            entity_id = entity_service.create_entity(
                external_id="test-custom-001",
                canonical_type="custom_test",
                source="test_source",
                attributes={"custom": True}
            )
            entity = entity_service.get_entity(entity_id)
            
            # Create message with custom values
            custom_correlation_id = "custom-correlation-123"
            custom_metadata = {"processor": "test", "stage": "validation"}
            
            message = Message.create_entity_message(
                entity=entity,
                payload={"test": "data"},
                correlation_id=custom_correlation_id,
                metadata=custom_metadata
            )
            
            # Verify custom values
            assert message.correlation_id == custom_correlation_id
            assert message.metadata == custom_metadata


class TestEntityReferenceIntegration:
    """Test Message integration with EntityReference and real entities."""
    
    def test_entity_reference_property(self, entity_service, tenant_context):
        """Test entity_reference property creates EntityReference from real entity."""
        with tenant_ctx(tenant_context["id"]):
            # Create real entity with specific attributes
            entity_id = entity_service.create_entity(
                external_id="test-ref-001",
                canonical_type="reference_test",
                source="ref_source",
                version=2,
                attributes={"reference": "test"}
            )
            entity = entity_service.get_entity(entity_id)
            
            # Create message
            message = Message.create_entity_message(
                entity=entity,
                payload={"reference_test": True}
            )
            
            # Verify entity_reference property
            entity_ref = message.entity_reference
            assert entity_ref.id == entity.id
            assert entity_ref.external_id == "test-ref-001"
            assert entity_ref.canonical_type == "reference_test"
            assert entity_ref.source == "ref_source"
            assert entity_ref.tenant_id == tenant_context["id"]
            assert entity_ref.version == 2
    
    def test_entity_reference_with_different_versions(
        self, entity_service, tenant_context
    ):
        """Test entity_reference with entities of different versions."""
        with tenant_ctx(tenant_context["id"]):
            # Create entity with version 1
            entity_id_v1 = entity_service.create_entity(
                external_id="test-version-001",
                canonical_type="version_test",
                source="version_source",
                version=1,
                attributes={"version": 1}
            )
            entity_v1 = entity_service.get_entity(entity_id_v1)
            
            # Create entity with version 3
            entity_id_v3 = entity_service.create_entity(
                external_id="test-version-002",
                canonical_type="version_test",
                source="version_source",
                version=3,
                attributes={"version": 3}
            )
            entity_v3 = entity_service.get_entity(entity_id_v3)
            
            # Create messages with different entity versions
            message_v1 = Message.create_entity_message(
                entity=entity_v1,
                payload={"version": 1}
            )
            message_v3 = Message.create_entity_message(
                entity=entity_v3,
                payload={"version": 3}
            )
            
            # Verify version differences
            assert message_v1.entity_reference.version == 1
            assert message_v3.entity_reference.version == 3
            assert message_v1.entity_reference.id != message_v3.entity_reference.id
    
    def test_tenant_isolation_in_entity_reference(
        self, entity_service, multi_tenant_context
    ):
        """Test entity_reference respects tenant isolation."""
        tenant1 = multi_tenant_context[0]
        tenant2 = multi_tenant_context[1]
        
        # Create entities in different tenants with same external_id
        external_id = "shared-external-id"
        
        with tenant_ctx(tenant1["id"]):
            entity_id_t1 = entity_service.create_entity(
                external_id=external_id,
                canonical_type="isolation_test",
                source="tenant_source",
                version=1,
                attributes={"tenant": "t1"}
            )
            entity_t1 = entity_service.get_entity(entity_id_t1)
        
        with tenant_ctx(tenant2["id"]):
            entity_id_t2 = entity_service.create_entity(
                external_id=external_id,
                canonical_type="isolation_test",
                source="tenant_source",
                version=1,
                attributes={"tenant": "t2"}
            )
            entity_t2 = entity_service.get_entity(entity_id_t2)
        
        # Create messages with entities from different tenants
        message_t1 = Message.create_entity_message(
            entity=entity_t1,
            payload={"tenant": "t1"}
        )
        message_t2 = Message.create_entity_message(
            entity=entity_t2,
            payload={"tenant": "t2"}
        )
        
        # Verify tenant isolation
        assert message_t1.entity_reference.tenant_id == tenant1["id"]
        assert message_t2.entity_reference.tenant_id == tenant2["id"]
        assert message_t1.entity_reference.id != message_t2.entity_reference.id
        assert message_t1.entity_reference.external_id == message_t2.entity_reference.external_id


class TestMessageLifecycle:
    """Test Message lifecycle methods and state management."""
    
    def test_mark_processed(self, entity_service, tenant_context):
        """Test mark_processed sets processed_at timestamp."""
        with tenant_ctx(tenant_context["id"]):
            # Create real entity
            entity_id = entity_service.create_entity(
                external_id="test-processed-001",
                canonical_type="lifecycle_test",
                source="test_source",
                attributes={}
            )
            entity = entity_service.get_entity(entity_id)
            
            # Create message
            message = Message.create_entity_message(
                entity=entity,
                payload={"lifecycle": "test"}
            )
            
            # Initially not processed
            assert message.processed_at is None
            
            # Mark as processed
            before_mark = datetime.utcnow()
            message.mark_processed()
            after_mark = datetime.utcnow()
            
            # Verify processed timestamp
            assert message.processed_at is not None
            assert before_mark <= message.processed_at <= after_mark
    
    def test_retry_management(self, entity_service, tenant_context):
        """Test retry count management and retry limits."""
        with tenant_ctx(tenant_context["id"]):
            # Create real entity
            entity_id = entity_service.create_entity(
                external_id="test-retry-001",
                canonical_type="retry_test",
                source="test_source",
                attributes={}
            )
            entity = entity_service.get_entity(entity_id)
            
            # Create message
            message = Message.create_entity_message(
                entity=entity,
                payload={"retry": "test"}
            )
            
            # Initial retry state
            assert message.retry_count == 0
            assert message.max_retries == 3
            assert message.can_retry() is True
            
            # Increment retries
            message.increment_retry()
            assert message.retry_count == 1
            assert message.can_retry() is True
            
            message.increment_retry()
            assert message.retry_count == 2
            assert message.can_retry() is True
            
            message.increment_retry()
            assert message.retry_count == 3
            assert message.can_retry() is False  # At max retries
            
            # Should still allow incrementing beyond max
            message.increment_retry()
            assert message.retry_count == 4
            assert message.can_retry() is False
    
    @pytest.mark.parametrize("max_retries,expected_can_retry", [
        (0, False),  # No retries allowed
        (1, True),   # One retry allowed
        (5, True),   # Multiple retries allowed
        (10, True),  # High retry limit
    ])
    def test_custom_max_retries(
        self, max_retries, expected_can_retry, entity_service, tenant_context
    ):
        """Test custom max_retries configuration."""
        with tenant_ctx(tenant_context["id"]):
            # Create real entity
            entity_id = entity_service.create_entity(
                external_id=f"test-max-retry-{max_retries}",
                canonical_type="max_retry_test",
                source="test_source",
                attributes={}
            )
            entity = entity_service.get_entity(entity_id)
            
            # Create message with custom max_retries
            message = Message.create_entity_message(
                entity=entity,
                payload={"max_retries": max_retries}
            )
            message.max_retries = max_retries
            
            # Verify custom max_retries
            assert message.max_retries == max_retries
            assert message.can_retry() == expected_can_retry


class TestMessageValidation:
    """Test Message validation and structure requirements."""
    
    def test_required_fields_validation(self, entity_service, tenant_context):
        """Test that Message requires entity and payload."""
        with tenant_ctx(tenant_context["id"]):
            # Create real entity
            entity_id = entity_service.create_entity(
                external_id="test-validation-001",
                canonical_type="validation_test",
                source="test_source",
                attributes={}
            )
            entity = entity_service.get_entity(entity_id)
            
            # Valid message creation should work
            message = Message.create_entity_message(
                entity=entity,
                payload={"valid": "data"}
            )
            assert message.entity == entity
            assert message.payload == {"valid": "data"}
    
    @pytest.mark.parametrize("payload_data", [
        {"simple": "string"},
        {"number": 42},
        {"boolean": True},
        {"null_value": None},
        {"nested": {"dict": {"deep": "value"}}},
        {"list": [1, 2, 3, "mixed", {"nested": True}]},
        {"complex": {
            "orders": [{"id": 1, "total": 99.99}],
            "metadata": {"processed": True, "timestamp": "2024-01-01T00:00:00Z"}
        }}
    ])
    def test_payload_structure_variations(
        self, payload_data, entity_service, tenant_context
    ):
        """Test Message handles various payload structures."""
        with tenant_ctx(tenant_context["id"]):
            # Create real entity
            entity_id = entity_service.create_entity(
                external_id=f"test-payload-{hash(str(payload_data)) % 10000}",
                canonical_type="payload_test",
                source="test_source",
                attributes={}
            )
            entity = entity_service.get_entity(entity_id)
            
            # Create message with varied payload
            message = Message.create_entity_message(
                entity=entity,
                payload=payload_data
            )
            
            # Verify payload preserved correctly
            assert message.payload == payload_data
    
    def test_metadata_and_routing_info_optional(self, entity_service, tenant_context):
        """Test metadata and routing_info are optional and default to empty dict."""
        with tenant_ctx(tenant_context["id"]):
            # Create real entity
            entity_id = entity_service.create_entity(
                external_id="test-optional-001",
                canonical_type="optional_test",
                source="test_source",
                attributes={}
            )
            entity = entity_service.get_entity(entity_id)
            
            # Create message without metadata/routing_info
            message = Message.create_entity_message(
                entity=entity,
                payload={"test": "data"}
            )
            
            # Verify defaults
            assert message.metadata == {}
            assert message.routing_info == {}
            
            # Create message with custom metadata
            custom_metadata = {"processor": "test", "version": "2.0"}
            message_with_metadata = Message.create_entity_message(
                entity=entity,
                payload={"test": "data"},
                metadata=custom_metadata
            )
            
            # Verify custom metadata preserved
            assert message_with_metadata.metadata == custom_metadata
            assert message_with_metadata.routing_info == {}  # Still defaults