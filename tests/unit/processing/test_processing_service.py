"""
Tests for ProcessingService covering error paths and edge cases.

This module tests the uncovered lines in processing_service.py,
focusing on error handling and processing flow.
"""

import pytest

from src.exceptions import ErrorCode, ServiceError, ValidationError
from src.processing.duplicate_detection import DuplicateDetectionResult, DuplicateDetectionService
from src.processing.entity_attributes import EntityAttributeBuilder
from src.processing.processing_service import ProcessingService, ProcessingResult
from src.processing.processor_config import ProcessorConfig
from src.repositories.entity_repository import EntityRepository
from src.services.entity_service import EntityService
from src.utils.hash_config import HashConfig


class TestProcessingServiceErrorPaths:
    """Test error handling in ProcessingService."""

    @pytest.fixture
    def processing_service(self, db_manager, tenant_context):
        """Create ProcessingService instance."""
        entity_repository = EntityRepository(db_manager)
        entity_service = EntityService(entity_repository)
        duplicate_detection = DuplicateDetectionService(entity_repository)
        attribute_builder = EntityAttributeBuilder()
        
        return ProcessingService(
            entity_service=entity_service,
            entity_repository=entity_repository,
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

