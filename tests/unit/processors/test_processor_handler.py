"""
Tests for the clean ProcessorHandler implementation.

This tests the new unified handler that combines execution, persistence,
and state tracking in one clean implementation.
"""
import pytest
from typing import Dict, Any
from unittest.mock import Mock, patch
import time

from src.processing.processing_service import ProcessingService, ProcessingResult as ServiceProcessingResult
from src.processing.duplicate_detection import DuplicateDetectionService
from src.processing.entity_attributes import EntityAttributeBuilder
from src.processing.processor_config import ProcessorConfig
from src.services.entity_service import EntityService
from src.services.processing_error_service import ProcessingErrorService
from src.services.state_tracking_service import StateTrackingService
from src.repositories.entity_repository import EntityRepository
from src.processors.message import Message, MessageType, EntityReference
from src.processors.processing_result import ProcessingResult, ProcessingStatus
from src.processors.processor_interface import ProcessorInterface
from src.processors.processor_handler import ProcessorHandler
from src.processors.mapper_interface import MapperInterface
from src.context.tenant_context import tenant_context
from src.db.db_config import DatabaseConfig, DatabaseManager
from src.db.db_base import EntityStateEnum
from src.exceptions import ValidationError, ServiceError


class TestProcessor(ProcessorInterface):
    """Test processor for unit tests."""
    
    def __init__(self, should_fail=False, error_type=None, can_retry_result=True, **kwargs):
        self.should_fail = should_fail
        self.error_type = error_type
        self.process_called = False
        self.validate_called = False
        self.can_retry_result = can_retry_result
    
    def process(self, message: Message) -> ProcessingResult:
        self.process_called = True
        
        if self.should_fail:
            if self.error_type == "validation":
                raise ValidationError("Test validation error")
            elif self.error_type == "service":
                raise ServiceError("Test service error")
            else:
                raise Exception("Test unexpected error")
        
        return ProcessingResult(
            status=ProcessingStatus.SUCCESS,
            success=True,
            processing_metadata={"test": "success"}
        )
    
    def validate_message(self, message: Message) -> bool:
        self.validate_called = True
        return not (self.should_fail and self.error_type == "invalid_message")
    
    def get_processor_info(self) -> dict:
        return {
            "name": "TestProcessor",
            "version": "1.0.0",
            "type": "intermediate"
        }
    
    def can_retry(self, error: Exception) -> bool:
        return self.can_retry_result


class TestMapperProcessor(TestProcessor, MapperInterface):
    """Test processor that implements mapper interface."""
    
    def to_canonical(self, external_data: Dict[str, Any], metadata: Dict[str, Any]) -> Dict[str, Any]:
        return {"canonical": external_data}
    
    def from_canonical(self, canonical_data: Dict[str, Any], metadata: Dict[str, Any]) -> Dict[str, Any]:
        return canonical_data
    
    def get_processor_info(self) -> dict:
        return {
            "name": "TestMapperProcessor",
            "version": "1.0.0",
            "type": "source"
        }


class TestProcessorHandler:
    """Test cases for the clean ProcessorHandler."""
    
    @pytest.fixture
    def setup_tenant(self, db_manager):
        """Ensure test tenant exists in the database."""
        from src.db.db_tenant_models import Tenant
        with db_manager.get_session() as session:
            # Check if tenant already exists
            existing = session.query(Tenant).filter_by(tenant_id="test_tenant").first()
            if not existing:
                tenant = Tenant(
                    tenant_id="test_tenant",
                    customer_name="Test Tenant",
                    tenant_config={},
                    is_active=True
                )
                session.add(tenant)
                session.commit()
    
    @pytest.fixture
    def processing_service(self, db_manager, setup_tenant):
        """Create ProcessingService with test database."""
        entity_repository = EntityRepository(db_manager=db_manager)
        entity_service = EntityService(entity_repository=entity_repository)
        duplicate_detection_service = DuplicateDetectionService(entity_repository=entity_repository)
        attribute_builder = EntityAttributeBuilder()
        
        return ProcessingService(
            entity_service=entity_service,
            entity_repository=entity_repository,
            duplicate_detection_service=duplicate_detection_service,
            attribute_builder=attribute_builder
        )
    
    @pytest.fixture
    def state_tracking_service(self):
        """Create mock state tracking service."""
        service = Mock(spec=StateTrackingService)
        service.record_transition = Mock()
        return service
    
    @pytest.fixture
    def error_service(self):
        """Create mock error service."""
        service = Mock(spec=ProcessingErrorService)
        service.record_error = Mock()
        return service
    
    @pytest.fixture
    def config(self):
        """Create test processor config."""
        return ProcessorConfig(
            processor_name="test_processor",
            processor_version="1.0.0",
            enable_state_tracking=True,
            is_source_processor=False
        )
    
    @pytest.fixture
    def source_config(self):
        """Create test processor config for source processors."""
        return ProcessorConfig(
            processor_name="test_processor",
            processor_version="1.0.0",
            enable_state_tracking=True,
            is_source_processor=True
        )
    
    @pytest.fixture
    def test_message(self):
        """Create test message."""
        return Message(
            message_id="test-123",
            message_type=MessageType.ENTITY_PROCESSING,
            entity_reference=EntityReference(
                external_id="test-entity",
                source="test_source",
                canonical_type="test_type",
                tenant_id="test_tenant"
            ),
            payload={"test": "data"}
        )
    
    def test_successful_execution(self, processing_service, state_tracking_service, config, test_message):
        """Test successful processor execution."""
        processor = TestProcessor()
        handler = ProcessorHandler(
            processor=processor,
            config=config,
            processing_service=processing_service,
            state_tracking_service=state_tracking_service
        )
        
        # Give the message an entity_id so state transitions can be recorded
        test_message.entity_reference.entity_id = "test-entity-123"
        
        with tenant_context("test_tenant"):
            result = handler.execute(test_message)
        
        assert result.success is True
        assert result.status == ProcessingStatus.SUCCESS
        assert processor.validate_called is True
        assert processor.process_called is True
        assert test_message.processed_at is not None
        assert result.processing_duration_ms > 0
        assert result.processor_info["name"] == "TestProcessor"
        
        # Verify state transitions
        assert state_tracking_service.record_transition.call_count == 2
        calls = state_tracking_service.record_transition.call_args_list
        # First call: received -> processing
        assert calls[0][1]["from_state"] == EntityStateEnum.RECEIVED
        assert calls[0][1]["to_state"] == EntityStateEnum.PROCESSING
        # Second call: processing -> completed
        assert calls[1][1]["from_state"] == EntityStateEnum.PROCESSING
        assert calls[1][1]["to_state"] == EntityStateEnum.COMPLETED
    
    def test_mapper_processor_persists_entity(self, processing_service, source_config, test_message):
        """Test that mapper processors persist entities."""
        processor = TestMapperProcessor()
        handler = ProcessorHandler(
            processor=processor,
            config=source_config,
            processing_service=processing_service
        )
        
        with tenant_context("test_tenant"):
            result = handler.execute(test_message)
        
        assert result.success is True
        assert len(result.entities_created) == 1
        assert "entity_id" in result.processing_metadata
        assert "entity_version" in result.processing_metadata
        assert result.processing_metadata["is_new_entity"] is True
    
    def test_handle_message_dict_compatibility(self, processing_service, config):
        """Test handle_message with dictionary input for compatibility."""
        processor = TestProcessor()
        handler = ProcessorHandler(
            processor=processor,
            config=config,
            processing_service=processing_service
        )
        
        message_dict = {
            "message_id": "test-123",
            "entity_reference": {
                "entity_id": "existing-entity-123",  # Add entity_id for non-source processor
                "external_id": "test-entity",
                "source": "test_source",
                "canonical_type": "test_type",
                "tenant_id": "test_tenant"
            },
            "payload": {"test": "data"}
        }
        
        with tenant_context("test_tenant"):
            result_dict = handler.handle_message(message_dict)
        
        assert result_dict["success"] is True
        assert result_dict["status"] == "success"
        # The original_message_id will be the generated one, not "test-123" from the dict
        assert result_dict["original_message_id"] is not None
        assert len(result_dict["original_message_id"]) > 0
        assert isinstance(result_dict["processing_duration_ms"], float)
    
    def test_validation_failure(self, processing_service, error_service, config, test_message):
        """Test handling of message validation failure."""
        processor = TestProcessor(should_fail=True, error_type="invalid_message")
        handler = ProcessorHandler(
            processor=processor,
            config=config,
            processing_service=processing_service,
            error_service=error_service
        )
        
        # Add entity_id so we get past the entity_id check
        test_message.entity_reference.entity_id = "test-entity-123"
        
        with tenant_context("test_tenant"):
            result = handler.execute(test_message)
        
        assert result.success is False
        assert result.error_message == "Message validation failed"
        assert result.error_code == "INVALID_MESSAGE"
        assert result.can_retry is False
        assert error_service.record_error.called is False  # No error recorded for validation
    
    def test_validation_error_handling(self, processing_service, error_service, state_tracking_service, config, test_message):
        """Test handling of validation errors during processing."""
        processor = TestProcessor(should_fail=True, error_type="validation")
        handler = ProcessorHandler(
            processor=processor,
            config=config,
            processing_service=processing_service,
            state_tracking_service=state_tracking_service,
            error_service=error_service
        )
        
        # Give the message an entity_id so state transitions can be recorded
        test_message.entity_reference.entity_id = "test-entity-123"
        
        with tenant_context("test_tenant"):
            result = handler.execute(test_message)
        
        assert result.success is False
        assert "Validation error" in result.error_message
        assert result.error_code == "VALIDATION_ERROR"
        assert result.can_retry is False
        
        # Verify error was recorded
        error_service.record_error.assert_called_once()
        error_call = error_service.record_error.call_args[1]
        assert error_call["error_type"] == "VALIDATION_ERROR"
        assert error_call["can_retry"] is False
        
        # Verify failed state transition
        calls = state_tracking_service.record_transition.call_args_list
        last_call = calls[-1][1]
        assert last_call["from_state"] == EntityStateEnum.PROCESSING
        assert last_call["to_state"] == EntityStateEnum.SYSTEM_ERROR
    
    def test_service_error_handling(self, processing_service, error_service, config, test_message):
        """Test handling of service errors with retry logic."""
        processor = TestProcessor(should_fail=True, error_type="service")
        handler = ProcessorHandler(
            processor=processor,
            config=config,
            processing_service=processing_service,
            error_service=error_service
        )
        
        # Add entity_id so we get past the entity_id check
        test_message.entity_reference.entity_id = "test-entity-123"
        
        with tenant_context("test_tenant"):
            result = handler.execute(test_message)
        
        assert result.success is False
        assert "Service error" in result.error_message
        assert result.error_code == "SERVICE_ERROR"
        assert result.can_retry is True
        assert result.retry_after_seconds == 1  # 2^0 = 1
        
        # Verify error was recorded
        error_service.record_error.assert_called_once()
    
    def test_retry_delay_calculation(self, processing_service, config, test_message):
        """Test exponential backoff retry delays."""
        processor = TestProcessor(should_fail=True, error_type="service")
        handler = ProcessorHandler(
            processor=processor,
            config=config,
            processing_service=processing_service
        )
        
        # Add entity_id so we get past the entity_id check
        test_message.entity_reference.entity_id = "test-entity-123"
        
        # Test different retry counts
        for retry_count, expected_delay in [(0, 1), (1, 2), (2, 4), (8, 256), (10, 300)]:
            test_message.retry_count = retry_count
            
            with tenant_context("test_tenant"):
                result = handler.execute(test_message)
            
            assert result.retry_after_seconds == expected_delay
    
    def test_entity_persistence_failure_doesnt_fail_processor(self, processing_service, config, test_message):
        """Test that entity persistence failure doesn't fail the processor."""
        processor = TestMapperProcessor()
        handler = ProcessorHandler(
            processor=processor,
            config=config,
            processing_service=processing_service
        )
        
        # Add entity_id so we get past the entity_id check
        test_message.entity_reference.entity_id = "test-entity-123"
        
        # Mock process_entity to raise an exception
        with patch.object(processing_service, 'process_entity', side_effect=Exception("DB error")):
            with tenant_context("test_tenant"):
                result = handler.execute(test_message)
        
        # Processor should still succeed
        assert result.success is True
        assert result.status == ProcessingStatus.SUCCESS
        assert len(result.entities_created) == 0  # No entities due to error
    
    def test_state_tracking_failure_doesnt_fail_processor(self, processing_service, state_tracking_service, config, test_message):
        """Test that state tracking failure doesn't fail the processor."""
        processor = TestProcessor()
        handler = ProcessorHandler(
            processor=processor,
            config=config,
            processing_service=processing_service,
            state_tracking_service=state_tracking_service
        )
        
        # Add entity_id so we get past the entity_id check
        test_message.entity_reference.entity_id = "test-entity-123"
        
        # Make state tracking fail
        state_tracking_service.record_transition.side_effect = Exception("State tracking error")
        
        with tenant_context("test_tenant"):
            result = handler.execute(test_message)
        
        # Processor should still succeed
        assert result.success is True
        assert result.status == ProcessingStatus.SUCCESS
    
    def test_processing_duration_measurement(self, processing_service, config, test_message):
        """Test accurate processing duration measurement."""
        processor = TestProcessor()
        handler = ProcessorHandler(
            processor=processor,
            config=config,
            processing_service=processing_service
        )
        
        # Add entity_id so we get past the entity_id check
        test_message.entity_reference.entity_id = "test-entity-123"
        
        with tenant_context("test_tenant"):
            start_time = time.time()
            result = handler.execute(test_message)
            end_time = time.time()
        
        # Duration should be positive and reasonable
        assert result.processing_duration_ms > 0
        assert result.processing_duration_ms <= (end_time - start_time) * 1000 + 10  # +10ms tolerance
    
    def test_non_source_processor_requires_entity_id(self, processing_service, config, test_message):
        """Test that non-source processors require entity_id in message."""
        processor = TestProcessor()
        handler = ProcessorHandler(
            processor=processor,
            config=config,  # Non-source processor
            processing_service=processing_service
        )
        
        # Remove entity_id from message
        test_message.entity_reference.entity_id = None
        
        with tenant_context("test_tenant"):
            result = handler.execute(test_message)
        
        assert result.success is False
        assert result.error_code == "MISSING_ENTITY_ID"
        assert result.error_message == "Missing entity_id for non-source processor"
        assert result.can_retry is False
        assert result.routing_info["dead_letter"] is True
        assert result.routing_info["reason"] == "missing_entity_id"
    
    def test_source_processor_doesnt_require_entity_id(self, processing_service, source_config, test_message):
        """Test that source processors don't require entity_id in message."""
        processor = TestMapperProcessor()
        handler = ProcessorHandler(
            processor=processor,
            config=source_config,  # Source processor
            processing_service=processing_service
        )
        
        # Remove entity_id from message
        test_message.entity_reference.entity_id = None
        
        with tenant_context("test_tenant"):
            result = handler.execute(test_message)
        
        # Should succeed even without entity_id
        assert result.success is True
        assert result.status == ProcessingStatus.SUCCESS
    
    def test_state_transitions_with_entity_id(self, processing_service, state_tracking_service, config, test_message):
        """Test state transitions are recorded when entity_id exists."""
        processor = TestProcessor()
        handler = ProcessorHandler(
            processor=processor,
            config=config,
            processing_service=processing_service,
            state_tracking_service=state_tracking_service
        )
        
        # Set entity_id in message
        test_message.entity_reference.entity_id = "existing-entity-123"
        
        with tenant_context("test_tenant"):
            result = handler.execute(test_message)
        
        assert result.success is True
        
        # Verify state transitions were recorded with correct entity_id
        assert state_tracking_service.record_transition.call_count == 2
        calls = state_tracking_service.record_transition.call_args_list
        
        # Both calls should use the entity_id from the message
        assert calls[0][1]["entity_id"] == "existing-entity-123"
        assert calls[1][1]["entity_id"] == "existing-entity-123"
    
    def test_no_state_transitions_without_entity_id(self, processing_service, state_tracking_service, source_config, test_message):
        """Test no state transitions are recorded for source processors without entity_id."""
        processor = TestProcessor()  # Not a mapper, so won't create entity
        handler = ProcessorHandler(
            processor=processor,
            config=source_config,
            processing_service=processing_service,
            state_tracking_service=state_tracking_service
        )
        
        # Remove entity_id from message
        test_message.entity_reference.entity_id = None
        
        with tenant_context("test_tenant"):
            result = handler.execute(test_message)
        
        assert result.success is True
        
        # No state transitions should be recorded without entity_id
        assert state_tracking_service.record_transition.call_count == 0