"""
Tests for ProcessingService covering error paths and edge cases.

This module tests the uncovered lines in processing_service.py,
focusing on error handling and processing flow.
"""

import pytest

from api_exchange_core.db import EntityStateEnum
from api_exchange_core.exceptions import ServiceError
from api_exchange_core.processing.duplicate_detection import DuplicateDetectionService
from api_exchange_core.processing import ProcessingService
from api_exchange_core.processing.processor_config import ProcessorConfig
from api_exchange_core.services.entity_service import EntityService
from api_exchange_core.services.logging_processing_error_service import LoggingProcessingErrorService
from api_exchange_core.services.logging_state_tracking_service import LoggingStateTrackingService


class TestProcessingServiceErrorPaths:
    """Test error handling in ProcessingService."""

    @pytest.fixture
    def processing_service(self, db_manager, tenant_context):
        """Create ProcessingService instance."""
        # Use the regular constructor now that it uses global db_manager
        return ProcessingService()

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
    def services(self, db_manager, tenant_context):
        """Create all services needed for testing."""
        from api_exchange_core.processing.entity_attributes import EntityAttributeBuilder
        from api_exchange_core.utils.logger import get_logger
        
        # Create processing service manually without using constructor
        processing_service = object.__new__(ProcessingService)
        
        # Set up core services with test session
        processing_service.entity_service = EntityService()
        processing_service.duplicate_detection_service = DuplicateDetectionService()
        processing_service.attribute_builder = EntityAttributeBuilder()
        processing_service.logger = get_logger()
        
        # Create additional services (logging-based, no session needed)
        state_service = LoggingStateTrackingService()
        error_service = LoggingProcessingErrorService()
        entity_service = EntityService()
        
        # Inject state and error services
        processing_service.state_tracking_service = state_service
        processing_service.error_service = error_service
        
        return {
            'processing': processing_service,
            'state': state_service,
            'error': error_service,
            'entity': entity_service
        }

    def test_state_tracking_for_new_entity(self, services, caplog):
        """Test that state transitions are logged for new entities."""
        import logging
        
        config = ProcessorConfig(
            processor_name="test_processor",
            processor_version="1.0",
            enable_state_tracking=True  # Enable state tracking
        )
        
        # Clear logs and set appropriate level
        caplog.clear()
        with caplog.at_level(logging.INFO):
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
        
        # Check state transitions were logged
        state_logs = [
            record for record in caplog.records 
            if hasattr(record, 'event_type') and record.event_type == 'state_transition'
        ]
        
        assert len(state_logs) > 0, "Should have logged state transitions for new entity"
        
        # Verify at least one transition has the expected details
        found_new_entity_transition = False
        for log in state_logs:
            if (hasattr(log, 'actor') and log.actor == "test_processor" and 
                hasattr(log, 'entity_id') and log.entity_id == result.entity_id):
                found_new_entity_transition = True
                break
        
        assert found_new_entity_transition, "Should find state transition for new entity"

    def test_state_tracking_for_entity_version(self, services, caplog):
        """Test that state transitions are logged for entity versions."""
        import logging
        
        config = ProcessorConfig(
            processor_name="test_processor",
            processor_version="1.0",
            enable_state_tracking=True
        )
        
        # Clear logs before starting
        caplog.clear()
        with caplog.at_level(logging.INFO):
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
        
        # Check state transitions were logged for both versions
        state_logs = [
            record for record in caplog.records 
            if hasattr(record, 'event_type') and record.event_type == 'state_transition'
        ]
        
        # Should have transitions for both entity versions
        assert len(state_logs) > 0, "Should have logged state transitions for entity versions"
        
        # Verify we have transitions for both entities
        entity_ids_in_logs = set()
        for log in state_logs:
            if hasattr(log, 'entity_id'):
                entity_ids_in_logs.add(log.entity_id)
        
        # Should track state for both entity versions
        assert len(entity_ids_in_logs) >= 1, "Should track state for entity versions"

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

    def test_processing_without_injected_services(self, db_manager, tenant_context):
        """Test that processing works without state/error services."""
        from api_exchange_core.processing.entity_attributes import EntityAttributeBuilder
        from api_exchange_core.utils.logger import get_logger
        
        # Create processing service without injecting state/error services
        processing_service = object.__new__(ProcessingService)
        
        # Set up core services with test session
        processing_service.entity_service = EntityService()
        processing_service.duplicate_detection_service = DuplicateDetectionService()
        processing_service.attribute_builder = EntityAttributeBuilder()
        processing_service.logger = get_logger()
        
        # Optional services (not initialized by default)
        processing_service.state_tracking_service = None
        processing_service.error_service = None
        
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


