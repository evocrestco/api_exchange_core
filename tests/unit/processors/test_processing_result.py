"""
Tests for processor processing result classes.

Tests use real data and real code paths following the NO MOCKS philosophy.
Tests cover ProcessingResult, ProcessingStatus, and factory methods.
"""

import pytest
from datetime import datetime

from src.processors.message import Message, EntityReference
from src.processors.processing_result import ProcessingResult, ProcessingStatus


class TestProcessingStatus:
    """Test ProcessingStatus enum."""
    
    def test_processing_status_values(self):
        """Test processing status enum values."""
        assert ProcessingStatus.SUCCESS == "success"
        assert ProcessingStatus.FAILED == "failed"
        assert ProcessingStatus.ERROR == "error"
        assert ProcessingStatus.SKIPPED == "skipped"
        assert ProcessingStatus.PARTIAL == "partial"


class TestProcessingResult:
    """Test ProcessingResult model and operations."""
    
    @pytest.fixture
    def sample_output_message(self):
        """Create sample output message for testing."""
        return Message.create_entity_message(
            external_id="OUTPUT-123",
            canonical_type="order",
            source="processor",
            tenant_id="test-tenant",
            payload={"processed": True}
        )
    
    def test_create_processing_result_minimal(self):
        """Test creating processing result with minimal required fields."""
        result = ProcessingResult(
            status=ProcessingStatus.SUCCESS,
            success=True
        )
        
        assert result.status == ProcessingStatus.SUCCESS
        assert result.success is True
        assert result.output_messages == []
        assert result.routing_info == {}
        assert result.error_message is None
        assert result.error_code is None
        assert result.error_details == {}
        assert result.processing_metadata == {}
        assert result.processor_info == {}
        assert result.entities_created == []
        assert result.entities_updated == []
        assert result.processing_duration_ms is None
        assert isinstance(result.completed_at, datetime)
        assert result.retry_after_seconds is None
        assert result.can_retry is True
    
    def test_create_processing_result_complete(self, sample_output_message):
        """Test creating processing result with all fields."""
        routing_info = {"destination": "next-queue"}
        processing_metadata = {"processor": "test", "duration": 123}
        processor_info = {"name": "TestProcessor", "version": "1.0"}
        error_details = {"field": "data", "reason": "invalid"}
        
        result = ProcessingResult(
            status=ProcessingStatus.ERROR,
            success=False,
            output_messages=[sample_output_message],
            routing_info=routing_info,
            error_message="Processing failed",
            error_code="PROC_001",
            error_details=error_details,
            processing_metadata=processing_metadata,
            processor_info=processor_info,
            entities_created=["entity-1", "entity-2"],
            entities_updated=["entity-3"],
            processing_duration_ms=456.789,
            retry_after_seconds=300,
            can_retry=True
        )
        
        assert result.status == ProcessingStatus.ERROR
        assert result.success is False
        assert len(result.output_messages) == 1
        assert result.output_messages[0] == sample_output_message
        assert result.routing_info == routing_info
        assert result.error_message == "Processing failed"
        assert result.error_code == "PROC_001"
        assert result.error_details == error_details
        assert result.processing_metadata == processing_metadata
        assert result.processor_info == processor_info
        assert result.entities_created == ["entity-1", "entity-2"]
        assert result.entities_updated == ["entity-3"]
        assert result.processing_duration_ms == 456.789
        assert result.retry_after_seconds == 300
        assert result.can_retry is True
    
    def test_create_success_factory_minimal(self):
        """Test ProcessingResult.create_success factory method with minimal args."""
        result = ProcessingResult.create_success()
        
        assert result.status == ProcessingStatus.SUCCESS
        assert result.success is True
        assert result.output_messages == []
        assert result.routing_info == {}
        assert result.processing_metadata == {}
        assert result.entities_created == []
        assert result.entities_updated == []
        assert result.processing_duration_ms is None
        assert result.error_message is None
        assert result.can_retry is True
    
    def test_create_success_factory_complete(self, sample_output_message):
        """Test ProcessingResult.create_success factory method with all args."""
        routing_info = {"destination": "success-queue"}
        processing_metadata = {"execution_time": 100}
        entities_created = ["new-entity-1"]
        entities_updated = ["updated-entity-1"]
        
        result = ProcessingResult.create_success(
            output_messages=[sample_output_message],
            routing_info=routing_info,
            processing_metadata=processing_metadata,
            entities_created=entities_created,
            entities_updated=entities_updated,
            processing_duration_ms=123.45
        )
        
        assert result.status == ProcessingStatus.SUCCESS
        assert result.success is True
        assert len(result.output_messages) == 1
        assert result.output_messages[0] == sample_output_message
        assert result.routing_info == routing_info
        assert result.processing_metadata == processing_metadata
        assert result.entities_created == entities_created
        assert result.entities_updated == entities_updated
        assert result.processing_duration_ms == 123.45
    
    def test_create_failure_factory_minimal(self):
        """Test ProcessingResult.create_failure factory method with minimal args."""
        result = ProcessingResult.create_failure(
            error_message="Something went wrong"
        )
        
        assert result.status == ProcessingStatus.ERROR  # Default for retryable
        assert result.success is False
        assert result.error_message == "Something went wrong"
        assert result.error_code is None
        assert result.error_details == {}
        assert result.can_retry is True
        assert result.retry_after_seconds is None
        assert result.routing_info == {}
    
    def test_create_failure_factory_complete(self):
        """Test ProcessingResult.create_failure factory method with all args."""
        error_details = {"validation_field": "email", "error": "invalid format"}
        routing_info = {"destination": "error-queue"}
        
        result = ProcessingResult.create_failure(
            error_message="Validation failed",
            error_code="VAL_001",
            error_details=error_details,
            can_retry=False,
            retry_after_seconds=None,
            routing_info=routing_info
        )
        
        assert result.status == ProcessingStatus.FAILED  # Non-retryable
        assert result.success is False
        assert result.error_message == "Validation failed"
        assert result.error_code == "VAL_001"
        assert result.error_details == error_details
        assert result.can_retry is False
        assert result.retry_after_seconds is None
        assert result.routing_info == routing_info
    
    def test_create_failure_retryable_vs_non_retryable(self):
        """Test that create_failure sets correct status based on can_retry."""
        # Retryable error
        retryable = ProcessingResult.create_failure(
            error_message="Temporary error",
            can_retry=True
        )
        assert retryable.status == ProcessingStatus.ERROR
        
        # Non-retryable error
        non_retryable = ProcessingResult.create_failure(
            error_message="Permanent error",
            can_retry=False
        )
        assert non_retryable.status == ProcessingStatus.FAILED
    
    def test_create_skipped_factory_minimal(self):
        """Test ProcessingResult.create_skipped factory method with minimal args."""
        result = ProcessingResult.create_skipped(
            reason="Duplicate detected"
        )
        
        assert result.status == ProcessingStatus.SKIPPED
        assert result.success is True  # Skipping is considered successful
        assert result.processing_metadata["skip_reason"] == "Duplicate detected"
        assert result.routing_info == {}
    
    def test_create_skipped_factory_complete(self):
        """Test ProcessingResult.create_skipped factory method with all args."""
        processing_metadata = {"duplicate_id": "existing-123"}
        routing_info = {"destination": "skipped-queue"}
        
        result = ProcessingResult.create_skipped(
            reason="Already processed",
            processing_metadata=processing_metadata,
            routing_info=routing_info
        )
        
        assert result.status == ProcessingStatus.SKIPPED
        assert result.success is True
        assert result.processing_metadata["skip_reason"] == "Already processed"
        assert result.processing_metadata["duplicate_id"] == "existing-123"
        assert result.routing_info == routing_info
    
    def test_add_output_message(self, sample_output_message):
        """Test adding output messages to result."""
        result = ProcessingResult.create_success()
        
        assert len(result.output_messages) == 0
        
        result.add_output_message(sample_output_message)
        assert len(result.output_messages) == 1
        assert result.output_messages[0] == sample_output_message
        
        # Add another message
        message2 = Message.create_entity_message(
            external_id="OUTPUT-456",
            canonical_type="order",
            source="processor",
            tenant_id="test-tenant",
            payload={"processed": True}
        )
        result.add_output_message(message2)
        assert len(result.output_messages) == 2
    
    def test_add_routing_info(self):
        """Test adding routing information to result."""
        result = ProcessingResult.create_success()
        
        assert result.routing_info == {}
        
        result.add_routing_info("destination", "test-queue")
        assert result.routing_info == {"destination": "test-queue"}
        
        result.add_routing_info("priority", "high")
        assert result.routing_info == {"destination": "test-queue", "priority": "high"}
    
    def test_add_metadata(self):
        """Test adding processing metadata to result."""
        result = ProcessingResult.create_success()
        
        assert result.processing_metadata == {}
        
        result.add_metadata("processor_name", "TestProcessor")
        assert result.processing_metadata == {"processor_name": "TestProcessor"}
        
        result.add_metadata("execution_time", 100)
        assert result.processing_metadata == {"processor_name": "TestProcessor", "execution_time": 100}
    
    def test_add_entity_created(self):
        """Test recording created entities."""
        result = ProcessingResult.create_success()
        
        assert result.entities_created == []
        
        result.add_entity_created("entity-1")
        assert result.entities_created == ["entity-1"]
        
        result.add_entity_created("entity-2")
        assert result.entities_created == ["entity-1", "entity-2"]
    
    def test_add_entity_updated(self):
        """Test recording updated entities."""
        result = ProcessingResult.create_success()
        
        assert result.entities_updated == []
        
        result.add_entity_updated("entity-1")
        assert result.entities_updated == ["entity-1"]
        
        result.add_entity_updated("entity-2")
        assert result.entities_updated == ["entity-1", "entity-2"]
    
    def test_has_entities_changed(self):
        """Test checking if any entities were changed."""
        result = ProcessingResult.create_success()
        
        # No entities changed initially
        assert result.has_entities_changed() is False
        
        # Add created entity
        result.add_entity_created("entity-1")
        assert result.has_entities_changed() is True
        
        # Reset and test updated entity
        result = ProcessingResult.create_success()
        result.add_entity_updated("entity-2")
        assert result.has_entities_changed() is True
        
        # Test both
        result.add_entity_created("entity-3")
        assert result.has_entities_changed() is True
    
    def test_get_summary(self, sample_output_message):
        """Test getting result summary for logging/monitoring."""
        result = ProcessingResult.create_success(
            output_messages=[sample_output_message],
            entities_created=["entity-1"],
            entities_updated=["entity-2", "entity-3"],
            processing_duration_ms=123.45
        )
        
        summary = result.get_summary()
        
        expected_summary = {
            "status": "success",
            "success": True,
            "output_message_count": 1,
            "entities_created_count": 1,
            "entities_updated_count": 2,
            "has_error": False,
            "can_retry": True,
            "processing_duration_ms": 123.45
        }
        
        assert summary == expected_summary
    
    def test_get_summary_with_error(self):
        """Test getting summary for failed result."""
        result = ProcessingResult.create_failure(
            error_message="Processing failed",
            can_retry=False
        )
        
        summary = result.get_summary()
        
        assert summary["status"] == "failed"
        assert summary["success"] is False
        assert summary["has_error"] is True
        assert summary["can_retry"] is False
        assert summary["output_message_count"] == 0
        assert summary["entities_created_count"] == 0
        assert summary["entities_updated_count"] == 0