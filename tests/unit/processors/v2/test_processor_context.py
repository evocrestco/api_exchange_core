"""
Tests for ProcessorContext - the v2 service access layer.

These tests use real implementations following the NO MOCKS philosophy.
Tests verify that ProcessorContext properly provides access to framework services.
"""

from datetime import datetime

import pytest

from src.context.tenant_context import tenant_context as tenant_ctx
from src.db.db_base import EntityStateEnum
from src.exceptions import ServiceError
from src.processing.duplicate_detection import DuplicateDetectionService
from src.processing.entity_attributes import EntityAttributeBuilder
from src.processing.processing_service import ProcessingService
from src.processing.processor_config import ProcessorConfig
from src.processors.v2.processor_interface import ProcessorContext
from src.repositories.entity_repository import EntityRepository
from src.repositories.processing_error_repository import ProcessingErrorRepository
from src.repositories.state_transition_repository import StateTransitionRepository
from src.schemas.entity_schema import EntityRead
from src.services.entity_service import EntityService
from src.services.processing_error_service import ProcessingErrorService
from src.services.state_tracking_service import StateTrackingService


class TestProcessorContext:
    """Test ProcessorContext service access functionality."""
    
    def test_persist_entity_creates_new_entity(self, processor_context, tenant_context):
        """Test that persist_entity creates a new entity correctly."""
        with tenant_ctx(tenant_context["id"]):
            # Persist entity through context
            entity_id = processor_context.persist_entity(
                external_id="ORDER-123",
                canonical_type="order",
                source="shopify",
                data={
                    "order_number": "123",
                    "customer": "John Doe",
                    "total": 99.99
                },
                metadata={"created_by": "test_processor"}
            )
            
            # Verify entity was created
            assert entity_id is not None
            assert isinstance(entity_id, str)
            
            # Retrieve and verify the entity
            entity = processor_context.get_entity(entity_id)
            assert entity is not None
            assert entity.external_id == "ORDER-123"
            assert entity.canonical_type == "order"
            assert entity.source == "shopify"
            
            # Framework only stores metadata, not content (security principle)
            assert "processing" in entity.attributes
            assert entity.attributes["processing"]["processor"] == "v2_processor"
            assert entity.attributes["processing"]["status"] == "processed"
            assert entity.attributes["processing"]["content_changed"] is True
            
            # Duplicate detection metadata is automatically added
            assert "duplicate_detection" in entity.attributes
            assert entity.attributes["duplicate_detection"]["is_duplicate"] is False
            
            # Content hash should be set for duplicate detection
            assert entity.content_hash is not None
            
            # Custom metadata should be preserved in source_metadata
            assert "source_metadata" in entity.attributes
            assert entity.attributes["source_metadata"]["created_by"] == "test_processor"
    
    def test_persist_entity_creates_new_version(self, processor_context, tenant_context):
        """Test that persist_entity ALWAYS creates new version (times seen pattern)."""
        with tenant_ctx(tenant_context["id"]):
            # Create initial entity sighting
            entity_id1 = processor_context.persist_entity(
                external_id="ORDER-123",
                canonical_type="order",
                source="shopify",
                data={"status": "pending"},
                metadata={"stage": "initial"}
            )
            
            # Second sighting of same entity (even with different content)
            entity_id2 = processor_context.persist_entity(
                external_id="ORDER-123",
                canonical_type="order",
                source="shopify",
                data={"status": "completed", "completed_at": "2024-01-01"},
                metadata={"stage": "final"}
            )
            
            # Should create NEW entity record (times seen pattern)
            assert entity_id2 != entity_id1
            
            # Both entities should exist
            entity1 = processor_context.get_entity(entity_id1)
            entity2 = processor_context.get_entity(entity_id2)
            
            # Both should have same external_id but different versions
            assert entity1.external_id == entity2.external_id == "ORDER-123"
            assert entity1.source == entity2.source == "shopify"
            assert entity2.version > entity1.version  # Second sighting has higher version
            
            # Second entity should have updated metadata
            assert entity2.attributes["source_metadata"]["stage"] == "final"
            
            # Duplicate detection should indicate content difference
            assert "duplicate_detection" in entity2.attributes
    
    def test_get_entity_by_external_id(self, processor_context, tenant_context):
        """Test retrieving entity by external ID."""
        with tenant_ctx(tenant_context["id"]):
            # Create entity
            processor_context.persist_entity(
                external_id="PRODUCT-456",
                canonical_type="product",
                source="woocommerce",
                data={"name": "Test Product", "sku": "SKU-456"},
                metadata={"product_type": "electronics"}
            )
            
            # Retrieve by external ID
            entity = processor_context.get_entity_by_external_id(
                external_id="PRODUCT-456",
                source="woocommerce"
            )
            
            assert entity is not None
            assert entity.external_id == "PRODUCT-456"
            assert entity.canonical_type == "product"
            assert entity.source == "woocommerce"
            # Verify metadata, not content
            assert "processing" in entity.attributes
            assert entity.attributes["processing"]["status"] == "processed"
            
            # Custom metadata should be in source_metadata
            assert "source_metadata" in entity.attributes
            assert entity.attributes["source_metadata"]["product_type"] == "electronics"
    
    def test_record_state_transition(self, processor_context, tenant_context):
        """Test recording state transitions through context."""
        with tenant_ctx(tenant_context["id"]):
            # Create entity first
            entity_id = processor_context.persist_entity(
                external_id="ORDER-789",
                canonical_type="order",
                source="shopify",
                data={"status": "new"}
            )
            
            # Record state transition
            processor_context.record_state_transition(
                entity_id=entity_id,
                from_state=EntityStateEnum.RECEIVED,
                to_state=EntityStateEnum.PROCESSING,
                processor_name="test_processor",
                metadata={"reason": "Starting processing"}
            )
            
            # Verify transition was recorded
            history = processor_context.get_entity_state_history(entity_id)
            assert history is not None
            assert len(history.transitions) > 0
            
            latest_transition = history.transitions[-1]
            assert latest_transition.from_state == EntityStateEnum.RECEIVED.value
            assert latest_transition.to_state == EntityStateEnum.PROCESSING.value
            assert latest_transition.processor_data["processor_name"] == "test_processor"
    
    def test_record_processing_error(self, processor_context, tenant_context):
        """Test recording processing errors through context."""
        with tenant_ctx(tenant_context["id"]):
            # Create entity
            entity_id = processor_context.persist_entity(
                external_id="ORDER-999",
                canonical_type="order",
                source="shopify",
                data={"status": "error"}
            )
            
            # Record processing error
            error_id = processor_context.record_processing_error(
                entity_id=entity_id,
                processor_name="test_processor",
                error_code="VALIDATION_ERROR",
                error_message="Invalid order data",
                error_details={"field": "customer_email", "reason": "missing"},
                is_retryable=True
            )
            
            assert error_id is not None
            
            # Verify error was recorded
            errors = processor_context.get_entity_errors(entity_id)
            assert len(errors) == 1
            assert errors[0].error_type_code == "VALIDATION_ERROR"
            assert errors[0].message == "Invalid order data"
            assert errors[0].processing_step == "test_processor"
    
    def test_context_without_optional_services(self, processing_service, tenant_context):
        """Test ProcessorContext works without state/error services."""
        # Create context with only required processing service
        context = ProcessorContext(
            processing_service=processing_service,
            state_tracking_service=None,
            error_service=None
        )
        
        with tenant_ctx(tenant_context["id"]):
            # Should still be able to persist entities
            entity_id = context.persist_entity(
                external_id="MIN-123",
                canonical_type="test",
                source="minimal",
                data={"test": True}
            )
            
            assert entity_id is not None
            
            # State and error operations should handle gracefully
            # (These methods should check if services are available)
            result = context.record_state_transition(
                entity_id=entity_id,
                from_state=EntityStateEnum.RECEIVED,
                to_state=EntityStateEnum.PROCESSING,
                processor_name="test"
            )
            # Should not crash but may return None or handle gracefully
    
    def test_persist_with_duplicate_detection(self, processor_context, tenant_context):
        """Test that persist_entity handles duplicate detection."""
        with tenant_ctx(tenant_context["id"]):
            # Create entity with specific content
            entity_id1 = processor_context.persist_entity(
                external_id="DUP-001",
                canonical_type="order",
                source="shopify",
                data={"order_items": ["item1", "item2"], "total": 100}
            )
            
            # Try to create duplicate with same content but different external_id
            entity_id2 = processor_context.persist_entity(
                external_id="DUP-002",
                canonical_type="order",
                source="shopify",
                data={"order_items": ["item1", "item2"], "total": 100}
            )
            
            # Should create new entity (framework handles duplicate detection internally)
            assert entity_id2 != entity_id1
            
            # Both entities should exist
            entity1 = processor_context.get_entity(entity_id1)
            entity2 = processor_context.get_entity(entity_id2)
            assert entity1 is not None
            assert entity2 is not None
    
    def test_batch_operations_support(self, processor_context, tenant_context):
        """Test that context supports batch operations efficiently."""
        with tenant_ctx(tenant_context["id"]):
            # Persist multiple entities
            entity_ids = []
            for i in range(5):
                entity_id = processor_context.persist_entity(
                    external_id=f"BATCH-{i}",
                    canonical_type="order",
                    source="batch_test",
                    data={"index": i, "batch": True},
                    metadata={"batch_index": i}
                )
                entity_ids.append(entity_id)
            
            # Verify all were created
            assert len(entity_ids) == 5
            assert len(set(entity_ids)) == 5  # All unique
            
            # Retrieve all entities and verify metadata
            for i, entity_id in enumerate(entity_ids):
                entity = processor_context.get_entity(entity_id)
                assert entity.external_id == f"BATCH-{i}"
                assert entity.canonical_type == "order"
                assert entity.source == "batch_test"
                # Verify metadata, not content
                assert "processing" in entity.attributes
                assert entity.attributes["processing"]["status"] == "processed"
                
                # Custom metadata should be in source_metadata
                assert "source_metadata" in entity.attributes
                assert entity.attributes["source_metadata"]["batch_index"] == i
                assert entity.content_hash is not None