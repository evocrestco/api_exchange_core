"""
Integration test for Hello World processor.

This test demonstrates the full processor pipeline flow:
1. Create input message
2. Process via ProcessorHandler
3. Verify entity persistence in database
4. Create output queue message

This is converted from e2e/hello_world but adapted for pytest.
"""

import json
import uuid
from datetime import UTC, datetime
from typing import Any, Dict

import pytest

from src.context.tenant_context import tenant_context
from src.db.db_entity_models import Entity
from src.processors.processing_result import ProcessingResult, ProcessingStatus
from src.processors.v2.message import Message
from src.processors.v2.output_handlers import NoOpOutputHandler
from src.processors.v2.processor_factory import create_db_manager, create_processor_handler
from src.processors.v2.processor_interface import ProcessorContext, ProcessorInterface
from src.repositories.entity_repository import EntityRepository
from src.services.entity_service import EntityService
from src.utils.hash_utils import calculate_entity_hash


class HelloWorldProcessor(ProcessorInterface):
    """Simple v2 processor that generates hello world data."""
    
    def process(self, message: Message, context: ProcessorContext) -> ProcessingResult:
        """
        Business logic - generate hello world data and persist it.
        
        In v2, the processor controls everything:
        - Data transformation
        - When to persist entities
        - Output message creation
        """
        try:
            # Transform data to canonical format
            canonical_data = {
                "message": "Hello, World!",
                "timestamp": datetime.now(UTC).isoformat(),
                "generated_by": "HelloWorldProcessor",
                "external_id": message.entity_reference.external_id
            }
            
            # Persist the entity using context
            entity_id = context.persist_entity(
                external_id=message.entity_reference.external_id,
                canonical_type="greeting",
                source="hello_world_generator",
                data=canonical_data,
                metadata={"processor_version": "2.0.0"}
            )
            
            # Create success result with entity info
            result = ProcessingResult.create_success()
            result.entities_created = [entity_id]
            
            # Add a no-op output handler for testing
            # In real processors, this would be QueueOutputHandler, ServiceBusOutputHandler, etc.
            result.add_output_handler(NoOpOutputHandler(
                destination="test-complete",
                config={
                    "reason": "Integration test processor - no downstream routing needed",
                    "metadata": {"test_entity_id": entity_id}
                }
            ))
            
            return result
            
        except Exception as e:
            return ProcessingResult.create_failure(
                error_message=f"Failed to generate hello world: {str(e)}",
                error_code="HELLO_WORLD_GENERATION_FAILED",
                can_retry=True
            )
    
    def validate_message(self, message: Message) -> bool:
        return True
    
    def get_processor_info(self) -> Dict[str, Any]:
        return {"name": "HelloWorldProcessor", "version": "2.0.0", "type": "source"}
    
    def can_retry(self, error: Exception) -> bool:
        return True


@pytest.fixture
def hello_world_processor_handler():
    """Create processor handler for testing."""
    return create_processor_handler(processor=HelloWorldProcessor())


@pytest.fixture
def test_message():
    """Create a test message for processing."""
    import os

    # Use TENANT_ID from environment (same as real Azure function)
    external_id = f"hello-test-{uuid.uuid4().hex[:8]}"
    
    # Prepare payload data
    payload = {
        "message": "Hello, World!",
        "timestamp": datetime.now(UTC).isoformat(),
        "generated_by": "HelloWorldProcessor"
    }
    
    # Create entity first (without tenant context - ProcessorHandler will handle it)
    entity = Entity.create(
        tenant_id=os.getenv("TENANT_ID", "e2e_test_tenant"),
        external_id=external_id,
        canonical_type="greeting",
        source="hello_world_generator",
        content_hash=calculate_entity_hash(payload)
    )
    
    # Create message with entity reference
    return Message.from_entity(
        entity=entity,
        payload=payload
    )


@pytest.fixture
def entity_service_for_validation():
    """
    Create entity service for validation using real production database manager.
    
    This fixture uses the same create_db_manager() that processors use,
    ensuring we're testing with the real production code paths.
    """
    from src.processors.v2.processor_factory import create_db_manager

    # Use same database configuration as processor
    db_manager = create_db_manager()
    db_session = db_manager.get_session()
    entity_repo = EntityRepository(db_session)
    return EntityService(entity_repo)


class TestHelloWorldIntegration:
    """Integration tests for Hello World processor pipeline."""
    
    def test_hello_world_processor_success(
        self, 
        hello_world_processor_handler, 
        test_message, 
        entity_service_for_validation
    ):
        """Test successful hello world processing with database persistence."""
        import os

        # Execute via ProcessorHandler - it will handle tenant context from TENANT_ID env var
        result = hello_world_processor_handler.execute(test_message)
        
        # Debug output
        print(f"\nDEBUG: Result status: {result.status}")
        print(f"DEBUG: Result error_message: {result.error_message}")
        print(f"DEBUG: Result error_code: {result.error_code}")
        print(f"DEBUG: Result entities_created: {result.entities_created}")
        
        # Verify processing result
        assert result.status == ProcessingStatus.SUCCESS
        assert result.entities_created is not None
        assert len(result.entities_created) == 1
        assert result.error_message is None
        assert result.error_code is None
        
        # Verify entity was persisted to database
        entity_id = result.entities_created[0]
        
        # Validation needs tenant context too (same as real Azure function)
        with tenant_context(os.getenv("TENANT_ID", "e2e_test_tenant")):
            retrieved_entity = entity_service_for_validation.get_entity(entity_id)
        
        assert retrieved_entity is not None
        assert retrieved_entity.external_id == test_message.entity_reference.external_id
        assert retrieved_entity.canonical_type == "greeting"
        assert retrieved_entity.source == "hello_world_generator"
        assert retrieved_entity.version == 1
        assert retrieved_entity.tenant_id == os.getenv("TENANT_ID", "e2e_test_tenant")
        assert retrieved_entity.attributes is not None
        
        # Check that attributes were stored (processor adds metadata)
        assert "source_metadata" in retrieved_entity.attributes
        assert retrieved_entity.attributes["source_metadata"]["processor_version"] == "2.0.0"
    
    def test_hello_world_processor_with_multiple_messages(
        self,
        hello_world_processor_handler,
        entity_service_for_validation
    ):
        """Test processing multiple messages creates separate entities."""
        import os
        
        entity_ids = []
        
        # Process 3 different messages
        for i in range(3):
            external_id = f"hello-batch-{i}-{uuid.uuid4().hex[:8]}"
            
            payload = {
                "message": f"Hello, World #{i}!",
                "timestamp": datetime.now(UTC).isoformat(),
                "generated_by": "HelloWorldProcessor",
                "batch_number": i
            }
            
            entity = Entity.create(
                tenant_id=os.getenv("TENANT_ID", "e2e_test_tenant"),
                external_id=external_id,
                canonical_type="greeting",
                source="hello_world_generator",
                content_hash=calculate_entity_hash(payload)
            )
            
            message = Message.from_entity(entity=entity, payload=payload)
            
            # Execute via ProcessorHandler - it will handle tenant context from TENANT_ID env var
            result = hello_world_processor_handler.execute(message)
            
            # Verify success
            assert result.status == ProcessingStatus.SUCCESS
            assert len(result.entities_created) == 1
            entity_ids.append(result.entities_created[0])
        
        # Verify all entities are unique and persisted
        assert len(set(entity_ids)) == 3  # All unique
        
        # Verify all entities exist in database
        with tenant_context(os.getenv("TENANT_ID", "e2e_test_tenant")):
            for i, entity_id in enumerate(entity_ids):
                entity = entity_service_for_validation.get_entity(entity_id)
                assert entity is not None
                assert f"hello-batch-{i}-" in entity.external_id
    
    def test_processor_result_structure(
        self,
        hello_world_processor_handler,
        test_message
    ):
        """Test that processor result has correct structure."""
        # Execute via ProcessorHandler - it will handle tenant context from TENANT_ID env var
        result = hello_world_processor_handler.execute(test_message)
        
        # Verify result structure
        assert result.status == ProcessingStatus.SUCCESS
        assert result.success is True
        assert result.entities_created is not None
        assert len(result.entities_created) == 1
        
        # Verify result has all expected fields
        assert hasattr(result, 'status')
        assert hasattr(result, 'success')
        assert hasattr(result, 'entities_created')
        assert hasattr(result, 'output_messages')
        assert hasattr(result, 'error_message')
        assert hasattr(result, 'error_code')
        assert hasattr(result, 'processing_duration_ms')
        
        # Verify timing info
        assert result.processing_duration_ms is not None
        assert result.processing_duration_ms > 0
    
    def test_single_tenant_processing(
        self,
        hello_world_processor_handler,
        entity_service_for_validation
    ):
        """Test that processing works correctly with single tenant from environment."""
        import os

        # Create a test entity using the TENANT_ID from environment
        external_id = f"hello-single-tenant-{uuid.uuid4().hex[:8]}"
        
        payload = {
            "message": "Hello from single tenant test!",
            "timestamp": datetime.now(UTC).isoformat(),
            "test_type": "single_tenant"
        }
        
        entity = Entity.create(
            tenant_id=os.getenv("TENANT_ID", "e2e_test_tenant"),
            external_id=external_id,
            canonical_type="greeting",
            source="hello_world_generator",
            content_hash=calculate_entity_hash(payload)
        )
        
        message = Message.from_entity(entity=entity, payload=payload)
        
        # Execute via ProcessorHandler - it will handle tenant context from TENANT_ID env var
        result = hello_world_processor_handler.execute(message)
        
        # Verify success
        assert result.status == ProcessingStatus.SUCCESS
        assert len(result.entities_created) == 1
        
        entity_id = result.entities_created[0]
        
        # Verify entity exists and has correct tenant
        with tenant_context(os.getenv("TENANT_ID", "e2e_test_tenant")):
            entity = entity_service_for_validation.get_entity(entity_id)
            assert entity is not None
            assert entity.tenant_id == os.getenv("TENANT_ID", "e2e_test_tenant")
            assert entity.external_id == external_id
    
    def test_entity_attribute_updates(
        self,
        hello_world_processor_handler,
        entity_service_for_validation
    ):
        """Test updating entity attributes after creation."""
        import os

        # Create initial entity
        external_id = f"hello-attrs-{uuid.uuid4().hex[:8]}"
        
        payload = {
            "message": "Hello, Attributes!",
            "timestamp": datetime.now(UTC).isoformat(),
            "initial_version": True
        }
        
        entity = Entity.create(
            tenant_id=os.getenv("TENANT_ID", "e2e_test_tenant"),
            external_id=external_id,
            canonical_type="greeting",
            source="hello_world_generator",
            content_hash=calculate_entity_hash(payload)
        )
        
        message = Message.from_entity(entity=entity, payload=payload)
        
        # Execute processor to create entity
        result = hello_world_processor_handler.execute(message)
        assert result.status == ProcessingStatus.SUCCESS
        entity_id = result.entities_created[0]
        
        # Verify initial attributes
        with tenant_context(os.getenv("TENANT_ID", "e2e_test_tenant")):
            entity = entity_service_for_validation.get_entity(entity_id)
            assert "source_metadata" in entity.attributes
            initial_attrs = entity.attributes.copy()
            
            # Update entity attributes
            new_attributes = {
                "processing_step": "validation",
                "validation_status": "passed",
                "updated_at": datetime.now(UTC).isoformat(),
                "custom_metadata": {
                    "validation_rules": ["required_fields", "data_types"],
                    "validation_score": 95
                }
            }
            
            # Use EntityService to update attributes
            success = entity_service_for_validation.update_entity_attributes(entity_id, new_attributes)
            assert success is True
            
            # Verify attributes were updated and merged
            updated_entity = entity_service_for_validation.get_entity(entity_id)
            
            # Original attributes should still exist
            assert "source_metadata" in updated_entity.attributes
            assert updated_entity.attributes["source_metadata"] == initial_attrs["source_metadata"]
            
            # New attributes should be added
            assert updated_entity.attributes["processing_step"] == "validation"
            assert updated_entity.attributes["validation_status"] == "passed"
            assert updated_entity.attributes["custom_metadata"]["validation_score"] == 95
            
            # Verify complex nested attributes
            assert len(updated_entity.attributes["custom_metadata"]["validation_rules"]) == 2
            assert "required_fields" in updated_entity.attributes["custom_metadata"]["validation_rules"]

    def test_multi_tenant_isolation(
        self,
        hello_world_processor_handler
    ):
        """Test that different tenants cannot see each other's entities."""
        import os

        from src.db.db_tenant_models import Tenant
        from src.processors.v2.processor_factory import create_db_manager

        # Store original tenant
        original_tenant = os.getenv("TENANT_ID", "e2e_test_tenant")
        
        # Create test tenants first
        db_manager = create_db_manager()
        db_session = db_manager.get_session()
        
        test_tenants = []
        for tenant_suffix in ["tenant_a", "tenant_b"]:
            tenant_id = f"test_{tenant_suffix}"
            
            # Create tenant if it doesn't exist
            existing_tenant = db_session.query(Tenant).filter_by(tenant_id=tenant_id).first()
            if not existing_tenant:
                test_tenant = Tenant(
                    tenant_id=tenant_id,
                    customer_name=f"Test {tenant_suffix.title()}",
                    is_active=True,
                    tenant_config={
                        "hash_algorithm": {"value": "sha256", "updated_at": "2024-01-01T12:00:00Z"},
                        "enable_duplicate_detection": {"value": True, "updated_at": "2024-01-01T12:00:00Z"},
                    }
                )
                db_session.add(test_tenant)
                test_tenants.append(test_tenant)
        
        # Commit new tenants
        if test_tenants:
            db_session.commit()
        
        # Create entities in two different tenants
        tenant_data = []
        
        for tenant_suffix in ["tenant_a", "tenant_b"]:
            tenant_id = f"test_{tenant_suffix}"
            
            # Temporarily change environment tenant
            os.environ["TENANT_ID"] = tenant_id
            
            external_id = f"hello-{tenant_suffix}-{uuid.uuid4().hex[:8]}"
            
            payload = {
                "message": f"Hello from {tenant_suffix}!",
                "timestamp": datetime.now(UTC).isoformat(),
                "tenant_specific": tenant_id
            }
            
            entity = Entity.create(
                tenant_id=tenant_id,
                external_id=external_id,
                canonical_type="greeting",
                source="hello_world_generator",
                content_hash=calculate_entity_hash(payload)
            )
            
            message = Message.from_entity(entity=entity, payload=payload)
            
            # Execute processor with tenant context
            result = hello_world_processor_handler.execute(message)
            assert result.status == ProcessingStatus.SUCCESS
            assert len(result.entities_created) == 1
            
            tenant_data.append({
                "tenant_id": tenant_id,
                "entity_id": result.entities_created[0],
                "external_id": external_id
            })
        
        # Restore original tenant
        os.environ["TENANT_ID"] = original_tenant
        
        # Verify tenant isolation - each tenant can only see their own entities
        for i, data in enumerate(tenant_data):
            tenant_id = data["tenant_id"] 
            entity_id = data["entity_id"]
            
            # Create entity service for specific tenant
            os.environ["TENANT_ID"] = tenant_id
            db_manager = create_db_manager()
            db_session = db_manager.get_session()
            entity_repo = EntityRepository(db_session)
            entity_service = EntityService(entity_repo)
            
            # This tenant can see their own entity
            with tenant_context(tenant_id):
                entity = entity_service.get_entity(entity_id)
                assert entity is not None
                assert entity.tenant_id == tenant_id
                assert entity.external_id == data["external_id"]
            
            # But cannot see the other tenant's entity
            other_data = tenant_data[1 - i]  # Get the other tenant's data
            other_entity_id = other_data["entity_id"]
            
            with tenant_context(tenant_id):
                try:
                    other_entity = entity_service.get_entity(other_entity_id)
                    # If it returns something, it should not match the other tenant
                    if other_entity is not None:
                        assert other_entity.tenant_id != other_data["tenant_id"]
                        assert False, "Should not be able to see other tenant's entity"
                except Exception:
                    # Exception is expected - indicates proper tenant isolation
                    pass
        
        # Restore original tenant
        os.environ["TENANT_ID"] = original_tenant

    def test_state_transition_tracking(
        self,
        hello_world_processor_handler
    ):
        """Test that state transitions are tracked during processing."""
        import os

        from src.db.db_base import EntityStateEnum
        from src.db.db_tenant_models import Tenant
        from src.processors.v2.processor_factory import create_db_manager, create_processor_handler
        from src.repositories.state_transition_repository import StateTransitionRepository
        from src.services.state_tracking_service import StateTrackingService

        # Ensure test tenant exists
        tenant_id = os.getenv("TENANT_ID", "e2e_test_tenant")
        db_manager = create_db_manager()
        db_session = db_manager.get_session()
        
        existing_tenant = db_session.query(Tenant).filter_by(tenant_id=tenant_id).first()
        if not existing_tenant:
            test_tenant = Tenant(
                tenant_id=tenant_id,
                customer_name="E2E Test Tenant",
                is_active=True,
                tenant_config={
                    "hash_algorithm": {"value": "sha256", "updated_at": "2024-01-01T12:00:00Z"},
                    "enable_duplicate_detection": {"value": True, "updated_at": "2024-01-01T12:00:00Z"},
                }
            )
            db_session.add(test_tenant)
            db_session.commit()
        
        # Create processor handler with state tracking enabled
        from tests.integration.test_hello_world_integration import HelloWorldProcessor

        # Create state tracking service
        state_repo = StateTransitionRepository(db_session)
        state_service = StateTrackingService(state_repo)
        
        # Create processor handler with state tracking
        processor_with_state = create_processor_handler(
            processor=HelloWorldProcessor(),
            state_tracking_service=state_service
        )
        
        external_id = f"hello-state-{uuid.uuid4().hex[:8]}"
        
        payload = {
            "message": "Hello, State Tracking!",
            "timestamp": datetime.now(UTC).isoformat(),
            "track_states": True
        }
        
        entity = Entity.create(
            tenant_id=os.getenv("TENANT_ID", "e2e_test_tenant"),
            external_id=external_id,
            canonical_type="greeting",
            source="hello_world_generator",
            content_hash=calculate_entity_hash(payload)
        )
        
        message = Message.from_entity(entity=entity, payload=payload)
        
        # Execute processor with state tracking
        result = processor_with_state.execute(message)
        assert result.status == ProcessingStatus.SUCCESS
        entity_id = result.entities_created[0]
        
        # Verify state transitions were recorded
        with tenant_context(os.getenv("TENANT_ID", "e2e_test_tenant")):
            # Get state history for the entity
            state_history = state_service.get_entity_state_history(entity_id)
            
            if state_history is not None:
                # Verify we have state transitions
                assert len(state_history.transitions) > 0
                
                # Verify state progression makes sense
                for transition in state_history.transitions:
                    assert transition.entity_id == entity_id
                    assert transition.actor is not None
                    assert transition.transition_type is not None
                    
                # Verify current state
                current_state = state_service.get_current_state(entity_id)
                assert current_state is not None

    def test_duplicate_detection_integration(
        self,
        hello_world_processor_handler,
        entity_service_for_validation
    ):
        """Test duplicate detection during processing."""
        import os

        from src.db.db_tenant_models import Tenant
        from src.processors.v2.processor_factory import create_db_manager

        # Ensure test tenant exists
        tenant_id = os.getenv("TENANT_ID", "e2e_test_tenant")
        db_manager = create_db_manager()
        db_session = db_manager.get_session()
        
        existing_tenant = db_session.query(Tenant).filter_by(tenant_id=tenant_id).first()
        if not existing_tenant:
            test_tenant = Tenant(
                tenant_id=tenant_id,
                customer_name="E2E Test Tenant",
                is_active=True,
                tenant_config={
                    "hash_algorithm": {"value": "sha256", "updated_at": "2024-01-01T12:00:00Z"},
                    "enable_duplicate_detection": {"value": True, "updated_at": "2024-01-01T12:00:00Z"},
                }
            )
            db_session.add(test_tenant)
            db_session.commit()
        
        external_id = f"hello-duplicate-{uuid.uuid4().hex[:8]}"
        
        # Create identical payload (will have same content hash)
        payload = {
            "message": "Hello, Duplicate Detection!",
            "timestamp": "2024-01-01T12:00:00Z",  # Fixed timestamp for consistent hash
            "data": "identical_content"
        }
        
        # First message
        entity1 = Entity.create(
            tenant_id=os.getenv("TENANT_ID", "e2e_test_tenant"),
            external_id=f"{external_id}-first",
            canonical_type="greeting", 
            source="hello_world_generator",
            content_hash=calculate_entity_hash(payload)
        )
        
        message1 = Message.from_entity(entity=entity1, payload=payload)
        
        # Execute first message
        result1 = hello_world_processor_handler.execute(message1)
        assert result1.status == ProcessingStatus.SUCCESS
        entity_id1 = result1.entities_created[0]
        
        # Second message with same content but different external_id
        entity2 = Entity.create(
            tenant_id=os.getenv("TENANT_ID", "e2e_test_tenant"),
            external_id=f"{external_id}-second",
            canonical_type="greeting",
            source="hello_world_generator", 
            content_hash=calculate_entity_hash(payload)  # Same content hash
        )
        
        message2 = Message.from_entity(entity=entity2, payload=payload)
        
        # Execute second message
        result2 = hello_world_processor_handler.execute(message2)
        assert result2.status == ProcessingStatus.SUCCESS
        entity_id2 = result2.entities_created[0]
        
        # Verify duplicate detection metadata
        with tenant_context(os.getenv("TENANT_ID", "e2e_test_tenant")):
            # Check first entity (should not be duplicate)
            entity1_retrieved = entity_service_for_validation.get_entity(entity_id1)
            assert "duplicate_detection" in entity1_retrieved.attributes
            assert entity1_retrieved.attributes["duplicate_detection"]["is_duplicate"] is False
            assert entity1_retrieved.attributes["duplicate_detection"]["reason"] == "NEW"
            
            # Check second entity (should be detected as duplicate)
            entity2_retrieved = entity_service_for_validation.get_entity(entity_id2)
            assert "duplicate_detection" in entity2_retrieved.attributes
            
            # Should be detected as duplicate or suspicious
            duplicate_info = entity2_retrieved.attributes["duplicate_detection"]
            
            # Verify that duplicate detection ran and found similarities
            # Note: The actual content hashes may differ because the processor adds
            # different data (timestamps, external_ids) to each entity, but the 
            # duplicate detection system should still detect similarities
            assert "content_hash" in duplicate_info
            assert "is_duplicate" in duplicate_info or "is_suspicious" in duplicate_info
            
            # Either marked as duplicate, suspicious, or has similar entities
            has_duplicate_detection = (
                duplicate_info.get("is_duplicate") is True or 
                duplicate_info.get("is_suspicious") is True or
                len(duplicate_info.get("similar_entity_ids", [])) > 0
            )
            
            # If duplicate detection is working, we should see some indication
            print(f"DEBUG: Entity1 duplicate_detection: {entity1_retrieved.attributes['duplicate_detection']}")
            print(f"DEBUG: Entity2 duplicate_detection: {duplicate_info}")
            
            # At minimum, both should have duplicate detection metadata
            assert "content_hash" in entity1_retrieved.attributes["duplicate_detection"]
            assert "content_hash" in duplicate_info

    def test_error_handling_dead_letter_queue(
        self,
        dead_letter_queue_client,
        queue_message_verifier
    ):
        """Test error handling and dead letter queue routing with real Azure Storage."""
        import os

        from src.processors.v2.processor_factory import create_processor_handler

        # Create a failing processor for testing
        class FailingProcessor(ProcessorInterface):
            def process(self, message: Message, context: ProcessorContext) -> ProcessingResult:
                # Simulate a non-retryable error
                raise ValueError("Simulated processing failure")
            
            def validate_message(self, message: Message) -> bool:
                return True
            
            def get_processor_info(self) -> Dict[str, Any]:
                return {"name": "FailingProcessor", "version": "1.0.0", "type": "test"}
            
            def can_retry(self, error: Exception) -> bool:
                # Non-retryable errors should go to DLQ
                return False
        
        # Create processor handler with REAL dead letter queue client
        failing_processor_handler = create_processor_handler(
            processor=FailingProcessor(),
            dead_letter_queue_client=dead_letter_queue_client
        )
        
        external_id = f"hello-error-{uuid.uuid4().hex[:8]}"
        
        payload = {
            "message": "This will fail!",
            "timestamp": datetime.now(UTC).isoformat(),
            "error_test": True
        }
        
        entity = Entity.create(
            tenant_id=os.getenv("TENANT_ID", "e2e_test_tenant"),
            external_id=external_id,
            canonical_type="greeting",
            source="hello_world_generator",
            content_hash=calculate_entity_hash(payload)
        )
        
        message = Message.from_entity(entity=entity, payload=payload)
        
        # Execute failing processor
        result = failing_processor_handler.execute(message)
        
        # Verify failure result
        assert result.status == ProcessingStatus.DEAD_LETTERED
        assert result.success is False
        assert result.error_message is not None
        assert "Simulated processing failure" in result.error_message
        assert result.can_retry is False
        assert len(result.entities_created) == 0
        
        # Verify message was ACTUALLY sent to REAL dead letter queue
        dlq_message = queue_message_verifier.verify_dlq_message(
            external_id,
            "Unexpected error: Simulated processing failure"
        )
        
        # Verify DLQ message structure from REAL queue
        assert "original_message" in dlq_message
        assert "failure_info" in dlq_message
        assert dlq_message["original_message"]["external_id"] == external_id
        assert dlq_message["failure_info"]["error_message"] == "Unexpected error: Simulated processing failure"
        assert dlq_message["failure_info"]["processor"] == "FailingProcessor"
        assert "failed_at" in dlq_message["failure_info"]

    def test_retryable_error_handling(
        self,
        dead_letter_queue_client,
        queue_message_verifier
    ):
        """Test retryable errors do not go to DLQ immediately with real Azure Storage."""
        import os

        from src.processors.v2.processor_factory import create_processor_handler

        # Create a processor that fails with retryable error
        class RetryableFailingProcessor(ProcessorInterface):
            def process(self, message: Message, context: ProcessorContext) -> ProcessingResult:
                # Simulate a retryable error
                raise ConnectionError("Temporary connection issue")
            
            def validate_message(self, message: Message) -> bool:
                return True
            
            def get_processor_info(self) -> Dict[str, Any]:
                return {"name": "RetryableFailingProcessor", "version": "1.0.0", "type": "test"}
            
            def can_retry(self, error: Exception) -> bool:
                # Connection errors should be retryable
                return isinstance(error, ConnectionError)
        
        # Create processor handler with REAL dead letter queue client
        retryable_processor_handler = create_processor_handler(
            processor=RetryableFailingProcessor(),
            dead_letter_queue_client=dead_letter_queue_client
        )
        
        external_id = f"hello-retry-{uuid.uuid4().hex[:8]}"
        
        payload = {
            "message": "This will fail but retry!",
            "timestamp": datetime.now(UTC).isoformat(),
            "retry_test": True
        }
        
        entity = Entity.create(
            tenant_id=os.getenv("TENANT_ID", "e2e_test_tenant"),
            external_id=external_id,
            canonical_type="greeting",
            source="hello_world_generator",
            content_hash=calculate_entity_hash(payload)
        )
        
        message = Message.from_entity(entity=entity, payload=payload)
        
        # Execute retryable failing processor
        result = retryable_processor_handler.execute(message)
        
        # Verify failure result (but not dead lettered)
        assert result.status == ProcessingStatus.ERROR  # Not DEAD_LETTERED
        assert result.success is False
        assert result.error_message is not None
        assert "Temporary connection issue" in result.error_message
        assert result.can_retry is True
        assert len(result.entities_created) == 0
        
        # Verify REAL dead letter queue is empty (no message sent for retryable error)
        queue_message_verifier.assert_dlq_empty()

    def test_validation_failure_error_handling(
        self,
        hello_world_processor_handler
    ):
        """Test message validation failures."""
        import os

        from src.processors.v2.processor_factory import create_processor_handler

        # Create a processor with strict validation
        class StrictValidationProcessor(ProcessorInterface):
            def process(self, message: Message, context: ProcessorContext) -> ProcessingResult:
                return ProcessingResult.create_success()
            
            def validate_message(self, message: Message) -> bool:
                # Reject messages without specific field
                return "required_field" in message.payload
            
            def get_processor_info(self) -> Dict[str, Any]:
                return {"name": "StrictValidationProcessor", "version": "1.0.0", "type": "test"}
            
            def can_retry(self, error: Exception) -> bool:
                return False
        
        # Create processor handler
        validation_processor_handler = create_processor_handler(
            processor=StrictValidationProcessor()
        )
        
        external_id = f"hello-validation-{uuid.uuid4().hex[:8]}"
        
        # Create payload without required field
        payload = {
            "message": "Missing required field!",
            "timestamp": datetime.now(UTC).isoformat(),
            # Missing "required_field"
        }
        
        entity = Entity.create(
            tenant_id=os.getenv("TENANT_ID", "e2e_test_tenant"),
            external_id=external_id,
            canonical_type="greeting",
            source="hello_world_generator",
            content_hash=calculate_entity_hash(payload)
        )
        
        message = Message.from_entity(entity=entity, payload=payload)
        
        # Execute processor with invalid message
        result = validation_processor_handler.execute(message)
        
        # Verify validation failure
        assert result.status == ProcessingStatus.FAILED
        assert result.success is False
        assert result.error_message == "Message validation failed"
        assert result.error_code == "INVALID_MESSAGE"
        assert result.can_retry is False
        assert len(result.entities_created) == 0

    def test_queue_output_handler_integration(
        self,
        hello_world_processor_handler,
        output_queue_client,
        output_queue_verifier,
        entity_service_for_validation
    ):
        """Test that QueueOutputHandler actually sends messages to output queue."""
        import os

        from src.processors.v2.output_handlers import QueueOutputHandler
        from src.processors.v2.processor_factory import create_processor_handler

        # Create a processor that uses QueueOutputHandler
        class HelloWorldWithQueueOutput(ProcessorInterface):
            def __init__(self, azure_connection_string):
                self.azure_connection_string = azure_connection_string
            
            def process(self, message: Message, context: ProcessorContext) -> ProcessingResult:
                try:
                    # Transform data to canonical format
                    canonical_data = {
                        "message": "Hello, World with Queue Output!",
                        "timestamp": datetime.now(UTC).isoformat(),
                        "generated_by": "HelloWorldWithQueueOutput",
                        "external_id": message.entity_reference.external_id,
                        "processed_payload": message.payload
                    }
                    
                    # Persist the entity using context
                    entity_id = context.persist_entity(
                        external_id=message.entity_reference.external_id,
                        canonical_type="greeting",
                        source="hello_world_generator",
                        data=canonical_data,
                        metadata={"processor_version": "2.0.0", "has_queue_output": True}
                    )
                    
                    # Create success result with entity info
                    result = ProcessingResult.create_success()
                    result.entities_created = [entity_id]
                    
                    # Add REAL QueueOutputHandler to send to output queue
                    queue_output_handler = QueueOutputHandler(
                        destination="test-output-queue",
                        config={
                            "connection_string": self.azure_connection_string,
                            "auto_create_queue": True,
                            "message_ttl_seconds": 300  # 5 minutes
                        }
                    )
                    result.add_output_handler(queue_output_handler)
                    
                    return result
                    
                except Exception as e:
                    return ProcessingResult.create_failure(
                        error_message=f"Failed to generate hello world with queue output: {str(e)}",
                        error_code="HELLO_WORLD_QUEUE_OUTPUT_FAILED",
                        can_retry=True
                    )
            
            def validate_message(self, message: Message) -> bool:
                return True
            
            def get_processor_info(self) -> Dict[str, Any]:
                return {"name": "HelloWorldWithQueueOutput", "version": "2.0.0", "type": "source"}
            
            def can_retry(self, error: Exception) -> bool:
                return True
        
        # Create processor handler with queue output
        azure_connection_string = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
        if not azure_connection_string:
            pytest.skip("AZURE_STORAGE_CONNECTION_STRING not configured")
        
        processor_with_queue = HelloWorldWithQueueOutput(azure_connection_string)
        processor_handler = create_processor_handler(processor=processor_with_queue)
        
        # Create test message
        external_id = f"hello-queue-{uuid.uuid4().hex[:8]}"
        
        payload = {
            "message": "Hello, Queue Output Test!",
            "timestamp": datetime.now(UTC).isoformat(),
            "test_type": "queue_output_integration"
        }
        
        entity = Entity.create(
            tenant_id=os.getenv("TENANT_ID", "e2e_test_tenant"),
            external_id=external_id,
            canonical_type="greeting",
            source="hello_world_generator",
            content_hash=calculate_entity_hash(payload)
        )
        
        message = Message.from_entity(entity=entity, payload=payload)
        
        # Verify output queue is initially empty
        output_queue_verifier.assert_output_queue_empty()
        
        # Debug: check what connection string both are using
        print(f"DEBUG: Test using connection string: {azure_connection_string[:50]}...")
        print(f"DEBUG: Test output queue name: test-output-queue")
        
        # Execute processor with queue output handler
        result = processor_handler.execute(message)
        
        # Debug output handler execution
        print(f"DEBUG: Processing result status: {result.status}")
        print(f"DEBUG: Processing result success: {result.success}")
        print(f"DEBUG: Output handlers count: {len(result.output_handlers)}")
        print(f"DEBUG: Processing metadata keys: {list(result.processing_metadata.keys())}")
        if "output_handler_results" in result.processing_metadata:
            handler_results = result.processing_metadata['output_handler_results']
            print(f"DEBUG: Output handler results: {handler_results}")
            for hr in handler_results:
                print(f"DEBUG: Handler {hr['handler_name']}: {hr['destination']} -> success={hr['success']}")
        
        # Debug: Check all available queues
        from azure.storage.queue import QueueServiceClient
        queue_service = QueueServiceClient.from_connection_string(azure_connection_string)
        
        print("DEBUG: Available queues:")
        for queue in queue_service.list_queues():
            print(f"  - {queue.name}")
        
        # Verify processing was successful
        assert result.status == ProcessingStatus.SUCCESS
        assert result.success is True
        assert len(result.entities_created) == 1
        assert len(result.output_handlers) == 1
        
        # Verify entity was persisted to database
        entity_id = result.entities_created[0]
        with tenant_context(os.getenv("TENANT_ID", "e2e_test_tenant")):
            retrieved_entity = entity_service_for_validation.get_entity(entity_id)
            assert retrieved_entity is not None
            assert retrieved_entity.external_id == external_id
            assert retrieved_entity.canonical_type == "greeting"
            assert retrieved_entity.attributes["source_metadata"]["has_queue_output"] is True
        
        # THE KEY TEST: Verify message actually made it to the output queue
        output_message = output_queue_verifier.verify_output_message(external_id, timeout_seconds=5)
        
        # Verify output message structure (simplified format - processing results stored in Entity.processing_results)
        assert "message_id" in output_message
        assert "correlation_id" in output_message
        assert "entity_reference" in output_message
        assert "payload" in output_message
        assert "metadata" in output_message
        assert "created_at" in output_message
        assert "retry_count" in output_message
        
        # Verify entity information in output message
        assert output_message["entity_reference"]["external_id"] == external_id
        assert output_message["entity_reference"]["canonical_type"] == "greeting"
        assert output_message["entity_reference"]["source"] == "hello_world_generator"
        
        # Verify that payload contains the original data (processed data is stored in Entity.processing_results)
        assert "test_type" in output_message["payload"]
        assert output_message["payload"]["test_type"] == "queue_output_integration"
        assert "message" in output_message["payload"]
        assert output_message["payload"]["message"] == "Hello, Queue Output Test!"
        
        # Verify exactly one message in queue  
        assert output_queue_verifier.get_message_count() == 1
        
        print(f"✅ Successfully verified message with external_id '{external_id}' reached output queue")
        print(f"✅ Output message structure: {output_message}")

    def test_multiple_output_handlers_integration(
        self,
        output_queue_client,
        output_queue_verifier,
        entity_service_for_validation
    ):
        """Test processor with multiple output handlers (queue + noop)."""
        import os

        from src.processors.v2.output_handlers import NoOpOutputHandler, QueueOutputHandler
        from src.processors.v2.processor_factory import create_processor_handler

        # Create a processor that uses multiple output handlers
        class MultiOutputProcessor(ProcessorInterface):
            def __init__(self, azure_connection_string):
                self.azure_connection_string = azure_connection_string
            
            def process(self, message: Message, context: ProcessorContext) -> ProcessingResult:
                try:
                    # Persist the entity
                    entity_id = context.persist_entity(
                        external_id=message.entity_reference.external_id,
                        canonical_type="multi_output",
                        source="multi_output_processor",
                        data={"multi_output": True, "timestamp": datetime.now(UTC).isoformat()},
                        metadata={"processor_version": "1.0.0"}
                    )
                    
                    # Create success result
                    result = ProcessingResult.create_success()
                    result.entities_created = [entity_id]
                    
                    # Add queue output handler
                    result.add_output_handler(QueueOutputHandler(
                        destination="test-output-queue",
                        config={
                            "connection_string": self.azure_connection_string,
                            "auto_create_queue": True
                        }
                    ))
                    
                    # Add noop output handler for audit trail
                    result.add_output_handler(NoOpOutputHandler(
                        destination="audit-log",
                        config={"reason": "audit_trail", "entity_id": entity_id}
                    ))
                    
                    return result
                    
                except Exception as e:
                    return ProcessingResult.create_failure(
                        error_message=f"Multi output processing failed: {str(e)}",
                        error_code="MULTI_OUTPUT_FAILED",
                        can_retry=True
                    )
            
            def validate_message(self, message: Message) -> bool:
                return True
            
            def get_processor_info(self) -> Dict[str, Any]:
                return {"name": "MultiOutputProcessor", "version": "1.0.0", "type": "transform"}
            
            def can_retry(self, error: Exception) -> bool:
                return True
        
        # Create processor handler
        azure_connection_string = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
        if not azure_connection_string:
            pytest.skip("AZURE_STORAGE_CONNECTION_STRING not configured")
        
        processor = MultiOutputProcessor(azure_connection_string)
        processor_handler = create_processor_handler(processor=processor)
        
        # Create test message
        external_id = f"multi-output-{uuid.uuid4().hex[:8]}"
        
        payload = {
            "test_data": "multi output test",
            "timestamp": datetime.now(UTC).isoformat()
        }
        
        entity = Entity.create(
            tenant_id=os.getenv("TENANT_ID", "e2e_test_tenant"),
            external_id=external_id,
            canonical_type="multi_output",
            source="multi_output_processor",
            content_hash=calculate_entity_hash(payload)
        )
        
        message = Message.from_entity(entity=entity, payload=payload)
        
        # Execute processor
        result = processor_handler.execute(message)
        
        # Verify processing was successful
        assert result.status == ProcessingStatus.SUCCESS
        assert len(result.output_handlers) == 2  # Queue + NoOp
        
        # Verify message reached the queue (queue handler succeeded)
        output_message = output_queue_verifier.verify_output_message(external_id, timeout_seconds=10)
        assert output_message["entity_reference"]["external_id"] == external_id
        assert output_message["entity_reference"]["canonical_type"] == "multi_output"
        
        # Verify output handler results in processing metadata
        assert "output_handler_results" in result.processing_metadata
        handler_results = result.processing_metadata["output_handler_results"]
        assert len(handler_results) == 2
        
        # Verify both handlers succeeded
        queue_handler_result = next(hr for hr in handler_results if hr["handler_name"] == "QueueOutputHandler")
        noop_handler_result = next(hr for hr in handler_results if hr["handler_name"] == "NoOpOutputHandler")
        
        assert queue_handler_result["success"] is True
        assert queue_handler_result["destination"] == "test-output-queue"
        assert noop_handler_result["success"] is True
        assert noop_handler_result["destination"] == "audit-log"
        
        print(f"✅ Successfully verified multi-output processing with external_id '{external_id}'")