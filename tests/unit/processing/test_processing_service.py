"""
Tests for ProcessingService covering error paths and edge cases.

This module tests the uncovered lines in processing_service.py,
focusing on error handling and processing flow.
"""

import pytest

from src.db.db_base import EntityStateEnum
from src.exceptions import ErrorCode, ServiceError, ValidationError
from src.processing.duplicate_detection import DuplicateDetectionResult, DuplicateDetectionService
from src.processing.entity_attributes import EntityAttributeBuilder
from src.processing.processing_service import ProcessingResult, ProcessingService
from src.processing.processor_config import ProcessorConfig
from src.repositories.entity_repository import EntityRepository
from src.repositories.processing_error_repository import ProcessingErrorRepository
from src.repositories.state_transition_repository import StateTransitionRepository
from src.services.entity_service import EntityService
from src.services.processing_error_service import ProcessingErrorService
from src.services.state_tracking_service import StateTrackingService
from src.utils.hash_config import HashConfig


class TestProcessingServiceErrorPaths:
    """Test error handling in ProcessingService."""

    @pytest.fixture
    def processing_service(self, db_session, tenant_context):
        """Create ProcessingService instance."""
        entity_repository = EntityRepository(db_session)
        entity_service = EntityService(entity_repository)
        duplicate_detection = DuplicateDetectionService(entity_repository)
        attribute_builder = EntityAttributeBuilder()
        
        return ProcessingService(
            entity_service=entity_service,
            duplicate_detection_service=duplicate_detection,
            attribute_builder=attribute_builder
        )

    def test_process_entity_with_invalid_external_id(self, processing_service):
        """Test process_entity with invalid external_id."""
        # Test with empty external_id - this should raise ServiceError with validation details
        with pytest.raises(ServiceError) as exc_info:
            processing_service.process_entity(
                external_id="",  # Invalid: empty external_id
                canonical_type="test_type",
                source="test_source",
                content={"data": "test"},
                config=ProcessorConfig(
                    processor_name="test_processor",
                    processor_version="1.0"
                )
            )
        
        assert "Entity processing failed" in str(exc_info.value)
        assert "String should have at least 1 character" in str(exc_info.value)

    def test_process_entity_with_non_source_processor_missing_entity(self, processing_service):
        """Test non-source processor raises error when entity doesn't exist."""
        config = ProcessorConfig(
            processor_name="test_processor",
            processor_version="1.0",
            is_source_processor=False  # Non-source processor
        )
        
        # Non-source processors should fail if entity doesn't exist
        with pytest.raises(ServiceError) as exc_info:
            processing_service.process_entity(
                external_id="non-existent-entity",
                canonical_type="test_type",
                source="test_source",
                content={"data": "test"},
                config=config
            )
        
        assert "Entity processing failed" in str(exc_info.value)
        assert "Entity not found for non-source processor" in str(exc_info.value)

    def test_determine_processing_action_with_duplicate(self, processing_service, tenant_context):
        """Test _determine_processing_action with duplicate detection."""
        # Create an entity
        config = ProcessorConfig(
            processor_name="test_processor",
            processor_version="1.0",
            enable_duplicate_detection=True,
            enable_entity_versioning=True
        )
        
        # Process entity first time
        result1 = processing_service.process_entity(
            external_id="dup-test-001",
            canonical_type="test_type",
            source="test_source",
            content={"data": "test"},
            config=config
        )
        
        # Process same content again - should be detected as duplicate
        result2 = processing_service.process_entity(
            external_id="dup-test-001",
            canonical_type="test_type",
            source="test_source",
            content={"data": "test"},  # Same content
            config=config
        )
        
        # Should create new version despite duplicate content
        assert result2.entity_id  # New version created
        assert not result2.is_new_entity  # Not a new entity, just new version
        assert result2.duplicate_detection_result.is_duplicate

    def test_process_entity_with_custom_attributes(self, processing_service):
        """Test process_entity with custom attributes."""
        config = ProcessorConfig(
            processor_name="test_processor",
            processor_version="1.0"
        )
        
        custom_attrs = {
            "processor_tag": "custom",
            "priority": "high"
        }
        
        result = processing_service.process_entity(
            external_id="custom-attr-001",
            canonical_type="test_type",
            source="test_source",
            content={"data": "test"},
            config=config,
            custom_attributes=custom_attrs
        )
        
        assert result.entity_id
        
        # Verify custom attributes were applied
        entity = processing_service.entity_service.get_entity(result.entity_id)
        assert entity.attributes["processor_tag"] == "custom"
        assert entity.attributes["priority"] == "high"

    def test_process_entity_with_source_metadata(self, processing_service):
        """Test process_entity with source metadata."""
        config = ProcessorConfig(
            processor_name="test_processor",
            processor_version="1.0"
        )
        
        source_meta = {
            "import_batch": "batch-123",
            "source_timestamp": "2023-12-25T10:00:00Z"
        }
        
        result = processing_service.process_entity(
            external_id="meta-test-001",
            canonical_type="test_type",
            source="test_source",
            content={"data": "test"},
            config=config,
            source_metadata=source_meta
        )
        
        assert result.entity_id
        
        # Verify metadata was stored
        entity = processing_service.entity_service.get_entity(result.entity_id)
        assert entity.attributes["source_metadata"]["import_batch"] == "batch-123"


class TestProcessingServiceWithStateAndErrorTracking:
    """Test ProcessingService with state tracking and error services."""

    @pytest.fixture
    def services(self, db_session, tenant_context):
        """Create all services needed for testing."""
        # Create repositories
        entity_repository = EntityRepository(db_session)
        state_repository = StateTransitionRepository(db_session)
        error_repository = ProcessingErrorRepository(db_session)
        
        # Create services
        entity_service = EntityService(entity_repository)
        state_service = StateTrackingService(state_repository)
        error_service = ProcessingErrorService(error_repository)
        duplicate_detection = DuplicateDetectionService(entity_repository)
        attribute_builder = EntityAttributeBuilder()
        
        # Create processing service
        processing_service = ProcessingService(
            entity_service=entity_service,
            duplicate_detection_service=duplicate_detection,
            attribute_builder=attribute_builder
        )
        
        # Inject state and error services
        processing_service.set_state_tracking_service(state_service)
        processing_service.set_processing_error_service(error_service)
        
        return {
            'processing': processing_service,
            'state': state_service,
            'error': error_service,
            'entity': entity_service
        }

    def test_state_tracking_for_new_entity(self, services):
        """Test that state transitions are recorded for new entities."""
        config = ProcessorConfig(
            processor_name="test_processor",
            processor_version="1.0",
            enable_state_tracking=True  # Enable state tracking
        )
        
        # Process new entity
        result = services['processing'].process_entity(
            external_id="state-test-001",
            canonical_type="test_type",
            source="test_source",
            content={"data": "test"},
            config=config
        )
        
        assert result.entity_id
        assert result.is_new_entity
        
        # Check state transitions were recorded
        history = services['state'].get_entity_state_history(result.entity_id)
        assert history is not None
        assert history.current_state == EntityStateEnum.PROCESSING.value
        assert len(history.transitions) >= 1
        
        # Verify transition details
        last_transition = history.transitions[0]
        assert last_transition.from_state == EntityStateEnum.RECEIVED.value
        assert last_transition.to_state == EntityStateEnum.PROCESSING.value
        assert last_transition.actor == "test_processor"
        assert "New entity created" in last_transition.notes

    def test_state_tracking_for_entity_version(self, services):
        """Test that state transitions are recorded for entity versions."""
        config = ProcessorConfig(
            processor_name="test_processor",
            processor_version="1.0",
            enable_state_tracking=True
        )
        
        # Create initial entity
        result1 = services['processing'].process_entity(
            external_id="version-test-001",
            canonical_type="test_type",
            source="test_source",
            content={"data": "v1"},
            config=config
        )
        
        # Create new version with different content
        result2 = services['processing'].process_entity(
            external_id="version-test-001",
            canonical_type="test_type",
            source="test_source",
            content={"data": "v2"},
            config=config
        )
        
        # Each version has its own entity_id (immutable records)
        assert result2.entity_id != result1.entity_id
        assert result2.external_id == result1.external_id
        assert result2.entity_version > result1.entity_version
        
        # Check state transitions for the second version
        # Since each version has its own entity_id, it will have its own state history
        history2 = services['state'].get_entity_state_history(result2.entity_id)
        assert history2 is not None
        assert len(history2.transitions) >= 1
        
        # Check the transition for version 2 creation
        version_transition = history2.transitions[0]
        assert "Entity version 2" in version_transition.notes
        assert version_transition.from_state == EntityStateEnum.PROCESSING.value
        assert version_transition.to_state == EntityStateEnum.PROCESSING.value

    def test_error_recording_on_processing_failure(self, services):
        """Test that pre-creation errors are logged but not recorded in database."""
        config = ProcessorConfig(
            processor_name="test_processor",
            processor_version="1.0"
        )
        
        # Force an error by using empty external_id (validation error)
        with pytest.raises(ServiceError) as exc_info:
            services['processing'].process_entity(
                external_id="",  # Empty external_id will cause validation error
                canonical_type="test_type",
                source="test_source",
                content={"data": "test"},
                config=config
            )
        
        # Verify the error was raised
        assert "Entity processing failed" in str(exc_info.value)
        assert "String should have at least 1 character" in str(exc_info.value)
        
        # Pre-creation errors are not recorded in the database (no entity_id available)
        # This is expected behavior - errors are logged instead

    def test_state_tracking_disabled(self, services):
        """Test that state tracking can be disabled."""
        config = ProcessorConfig(
            processor_name="test_processor",
            processor_version="1.0",
            enable_state_tracking=False  # Explicitly disable
        )
        
        # Process entity
        result = services['processing'].process_entity(
            external_id="no-state-test-001",
            canonical_type="test_type",
            source="test_source",
            content={"data": "test"},
            config=config
        )
        
        # State history should be empty
        history = services['state'].get_entity_state_history(result.entity_id)
        assert history is None or len(history.transitions) == 0

    def test_processing_without_injected_services(self, db_session, tenant_context):
        """Test that processing works without state/error services."""
        # Create processing service without injecting state/error services
        entity_repository = EntityRepository(db_session)
        entity_service = EntityService(entity_repository)
        duplicate_detection = DuplicateDetectionService(entity_repository)
        attribute_builder = EntityAttributeBuilder()
        
        processing_service = ProcessingService(
            entity_service=entity_service,
            duplicate_detection_service=duplicate_detection,
            attribute_builder=attribute_builder
        )
        # Note: NOT calling set_state_tracking_service or set_processing_error_service
        
        config = ProcessorConfig(
            processor_name="test_processor",
            processor_version="1.0",
            enable_state_tracking=True  # Even if enabled, should not fail
        )
        
        # Should process successfully without state/error tracking
        result = processing_service.process_entity(
            external_id="no-services-test-001",
            canonical_type="test_type",
            source="test_source",
            content={"data": "test"},
            config=config
        )
        
        assert result.entity_id
        assert result.is_new_entity


class TestProcessingServiceMessageIntegration:
    """Test ProcessingService.process_message method with output handler support."""

    @pytest.fixture
    def processing_service(self, db_session, tenant_context):
        """Create ProcessingService instance."""
        entity_repository = EntityRepository(db_session)
        entity_service = EntityService(entity_repository)
        duplicate_detection = DuplicateDetectionService(entity_repository)
        attribute_builder = EntityAttributeBuilder()
        
        return ProcessingService(
            entity_service=entity_service,
            duplicate_detection_service=duplicate_detection,
            attribute_builder=attribute_builder
        )

    @pytest.fixture
    def test_processor(self):
        """Create a real test processor for testing."""
        from datetime import UTC, datetime

        from src.processors.processing_result import ProcessingResult, ProcessingStatus
        
        class TestProcessor:
            """Real processor implementation for testing."""
            
            def __init__(self, should_succeed=True, processing_metadata=None):
                self.should_succeed = should_succeed
                self.processing_metadata = processing_metadata or {"processor_data": "test"}
                self.last_message = None
            
            def process(self, message):
                """Process the message and return a result."""
                self.last_message = message
                
                if self.should_succeed:
                    return ProcessingResult(
                        status=ProcessingStatus.SUCCESS,
                        success=True,
                        processing_metadata=self.processing_metadata,
                        processor_info={"name": "TestProcessor", "version": "1.0"},
                        processing_duration_ms=100.0,
                        completed_at=datetime.now(UTC)
                    )
                else:
                    return ProcessingResult(
                        status=ProcessingStatus.FAILED,
                        success=False,
                        error_message="Test processor failure",
                        error_code="TEST_ERROR",
                        processing_duration_ms=50.0,
                        completed_at=datetime.now(UTC)
                    )
        
        return TestProcessor()

    @pytest.fixture
    def test_message(self, create_test_entity):
        """Create a test message v2."""
        import uuid
        from datetime import UTC, datetime

        from src.processors.v2.message import Message, MessageType

        # Create entity with unique external_id to avoid conflicts
        entity = create_test_entity(
            external_id=f"msg-test-{uuid.uuid4().hex[:8]}",
            canonical_type="test_type"
        )
        
        return Message(
            message_id=f"test-msg-{uuid.uuid4().hex[:8]}",
            correlation_id=f"test-corr-{uuid.uuid4().hex[:8]}",
            created_at=datetime.now(UTC),
            message_type=MessageType.ENTITY_PROCESSING,
            entity=entity,
            payload={"data": "test_payload"},
            retry_count=0,
            max_retries=3
        )

    def test_process_message_success_new_entity(self, processing_service, test_processor, create_test_message):
        """Test successful message processing with new entity creation."""
        # Create a message that references a non-existent entity (for new entity creation)
        import uuid
        from datetime import UTC, datetime

        from src.processors.v2.message import Message, MessageType

        # Create a simple entity-like object that doesn't exist in database
        class MockEntity:
            def __init__(self):
                self.id = None  # No ID means it doesn't exist in DB yet
                self.tenant_id = "test_tenant"
                self.external_id = f"new-entity-{uuid.uuid4().hex[:8]}"
                self.canonical_type = "test_type"
                self.source = "test_source"
                self.version = 1
        
        mock_entity = MockEntity()
        message = Message(
            message_id=f"test-msg-{uuid.uuid4().hex[:8]}",
            correlation_id=f"test-corr-{uuid.uuid4().hex[:8]}",
            created_at=datetime.now(UTC),
            message_type=MessageType.ENTITY_PROCESSING,
            entity=mock_entity,
            payload={"data": "test_payload"},
            retry_count=0,
            max_retries=3
        )
        
        config = ProcessorConfig(
            processor_name="test_processor",
            processor_version="1.0",
            is_source_processor=True,
            enable_duplicate_detection=False
        )

        # Execute process_message
        result = processing_service.process_message(message, test_processor, config)

        # Verify processor was called with the message
        assert test_processor.last_message == message

        # Verify result structure
        assert result.success is True
        assert result.processing_duration_ms > 0
        assert result.processor_info["name"] == "test_processor"
        assert result.processor_info["version"] == "1.0"
        assert result.processor_info["is_source_processor"] is True
        assert result.completed_at is not None

        # Verify entity persistence metadata
        assert "entity_id" in result.processing_metadata
        assert "entity_version" in result.processing_metadata
        assert "is_new_entity" in result.processing_metadata
        assert result.processing_metadata["is_new_entity"] is True

        # Verify entity operations tracking
        assert len(result.entities_created) == 1
        assert len(result.entities_updated) == 0
        assert result.entities_created[0] == result.processing_metadata["entity_id"]

    def test_process_message_success_existing_entity(self, processing_service, test_processor, test_message):
        """Test successful message processing with existing entity."""
        config = ProcessorConfig(
            processor_name="test_processor",
            processor_version="1.0",
            is_source_processor=True
        )

        # First, create the entity
        processing_service.process_entity(
            external_id=test_message.entity_reference.external_id,
            canonical_type=test_message.entity_reference.canonical_type,
            source=test_message.entity_reference.source,
            content={"initial": "data"},
            config=config
        )

        # Now process message for existing entity
        result = processing_service.process_message(test_message, test_processor, config)

        # Verify result
        assert result.success is True
        assert result.processing_metadata["is_new_entity"] is False
        assert len(result.entities_created) == 0
        assert len(result.entities_updated) == 1

    def test_process_message_with_duplicate_detection(self, processing_service, test_processor, test_message):
        """Test message processing with duplicate detection enabled."""
        config = ProcessorConfig(
            processor_name="test_processor",
            processor_version="1.0",
            is_source_processor=True,
            enable_duplicate_detection=True
        )

        result = processing_service.process_message(test_message, test_processor, config)

        assert result.success is True
        # Should have duplicate detection metadata
        assert "duplicate_detection" in result.processing_metadata

    def test_process_message_processor_failure(self, processing_service, test_message):
        """Test message processing when processor fails."""
        from datetime import UTC, datetime

        from src.processors.processing_result import ProcessingResult, ProcessingStatus

        # Create processor that returns failure
        class FailingProcessor:
            def process(self, message):
                return ProcessingResult(
                    status=ProcessingStatus.FAILED,
                    success=False,
                    error_message="Processor failed",
                    error_code="PROCESSOR_ERROR",
                    processing_duration_ms=50.0,
                    completed_at=datetime.now(UTC)
                )
        
        failing_processor = FailingProcessor()

        config = ProcessorConfig(
            processor_name="failing_processor",
            processor_version="1.0",
            is_source_processor=True
        )

        result = processing_service.process_message(test_message, failing_processor, config)

        # Should return the processor's failure result with updated metadata
        assert result.success is False
        assert result.error_message == "Processor failed"
        assert result.error_code == "PROCESSOR_ERROR"
        assert result.processor_info["name"] == "failing_processor"
        
        # No entity persistence should occur
        assert len(result.entities_created) == 0
        assert len(result.entities_updated) == 0

    def test_process_message_invalid_processor_result(self, processing_service, test_message):
        """Test message processing with invalid processor result type."""
        
        # Create processor that returns invalid result
        class InvalidProcessor:
            def process(self, message):
                return "invalid_result"  # Wrong type - should return ProcessingResult
        
        invalid_processor = InvalidProcessor()

        config = ProcessorConfig(
            processor_name="invalid_processor",
            processor_version="1.0"
        )

        result = processing_service.process_message(test_message, invalid_processor, config)

        # Should return failure result
        assert result.success is False
        assert result.error_code == "PROCESSING_SERVICE_ERROR"
        assert "invalid result type" in result.error_message
        assert result.processor_info["name"] == "invalid_processor"

    def test_process_message_processor_exception(self, processing_service, test_message):
        """Test message processing when processor raises exception."""
        
        # Create processor that raises exception
        class ExceptionProcessor:
            def process(self, message):
                raise ValueError("Processor crashed")
        
        exception_processor = ExceptionProcessor()

        config = ProcessorConfig(
            processor_name="crash_processor",
            processor_version="1.0"
        )

        result = processing_service.process_message(test_message, exception_processor, config)

        # Should return failure result
        assert result.success is False
        assert result.error_code == "PROCESSING_SERVICE_ERROR"
        assert "Processor crashed" in result.error_message
        assert result.error_details["error_type"] == "ValueError"
        assert result.error_details["message_id"] == test_message.message_id

    def test_process_message_with_output_handlers(self, processing_service, test_message):
        """Test message processing preserves output handlers from processor result."""
        from datetime import UTC, datetime

        from src.processors.processing_result import ProcessingResult, ProcessingStatus
        from src.processors.v2.output_handlers.noop_output import NoOpOutputHandler

        # Create processor with output handlers
        class ProcessorWithHandlers:
            def process(self, message):
                result = ProcessingResult(
                    status=ProcessingStatus.SUCCESS,
                    success=True,
                    processing_metadata={"test": "data"},
                    processor_info={"name": "HandlerProcessor"},
                    processing_duration_ms=100.0,
                    completed_at=datetime.now(UTC)
                )
                # Add output handlers
                result.add_output_handler(NoOpOutputHandler("test-destination"))
                return result
        
        processor_with_handlers = ProcessorWithHandlers()

        config = ProcessorConfig(
            processor_name="handler_processor",
            processor_version="1.0",
            is_source_processor=True
        )

        result = processing_service.process_message(test_message, processor_with_handlers, config)

        # Verify output handlers are preserved
        assert result.success is True
        assert len(result.output_handlers) == 1
        assert isinstance(result.output_handlers[0], NoOpOutputHandler)

    def test_process_message_non_source_processor(self, processing_service, test_processor, test_message):
        """Test message processing with non-source processor (existing entity required)."""
        config = ProcessorConfig(
            processor_name="transform_processor",
            processor_version="1.0",
            is_source_processor=False  # Non-source processor
        )

        # First create entity as source processor
        source_config = ProcessorConfig(
            processor_name="source_processor",
            processor_version="1.0",
            is_source_processor=True
        )
        processing_service.process_entity(
            external_id=test_message.entity_reference.external_id,
            canonical_type=test_message.entity_reference.canonical_type,
            source=test_message.entity_reference.source,
            content={"initial": "data"},
            config=source_config
        )

        # Now process as non-source processor
        result = processing_service.process_message(test_message, test_processor, config)

        assert result.success is True
        assert result.processor_info["is_source_processor"] is False
        assert result.processing_metadata["is_new_entity"] is False
        assert len(result.entities_created) == 0
        assert len(result.entities_updated) == 1

