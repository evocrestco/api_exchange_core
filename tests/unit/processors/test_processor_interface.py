"""
Tests for ProcessorInterface covering default implementations.

This module tests the default implementations of methods in ProcessorInterface
to ensure complete code coverage.
"""

import pytest

from src.exceptions import ErrorCode, ValidationError
from src.processors.message import EntityReference, Message, MessageType
from src.processors.processing_result import ProcessingResult, ProcessingStatus
from src.processors.processor_interface import ProcessorInterface


class ConcreteProcessor(ProcessorInterface):
    """Concrete implementation for testing."""
    
    def process(self, message: Message) -> ProcessingResult:
        """Simple implementation that returns success."""
        return ProcessingResult(
            status=ProcessingStatus.SUCCESS,
            success=True,
            output_messages=[],
            processing_metadata={"processed": True}
        )


class ConcreteProcessorWithTransformations(ProcessorInterface):
    """Concrete processor that implements transformation methods."""
    
    def process(self, message: Message) -> ProcessingResult:
        """Simple implementation that returns success."""
        return ProcessingResult(
            status=ProcessingStatus.SUCCESS,
            success=True,
            output_messages=[],
            processing_metadata={"processed": True}
        )
    
    def to_canonical(self, external_data: dict, metadata: dict) -> dict:
        """Transform external data to canonical format."""
        return {
            "canonical_id": external_data.get("id"),
            "canonical_name": external_data.get("name"),
            "transformed": True
        }
    
    def from_canonical(self, canonical_data: dict, metadata: dict) -> dict:
        """Transform canonical data to external format."""
        return {
            "id": canonical_data.get("canonical_id"),
            "name": canonical_data.get("canonical_name"),
            "exported": True
        }


class TestProcessorInterface:
    """Test ProcessorInterface default implementations."""
    
    def test_get_processor_info_default(self):
        """Test default get_processor_info returns class information."""
        processor = ConcreteProcessor()
        info = processor.get_processor_info()
        
        assert info["processor_class"] == "ConcreteProcessor"
        assert info["processor_module"] == "test_processor_interface"
        assert len(info) == 2  # Only class and module by default
    
    def test_validate_message_default_accepts_all(self):
        """Test default validate_message accepts all messages."""
        processor = ConcreteProcessor()
        
        # Create entity reference for messages
        entity_ref = EntityReference(
            external_id="TEST-001",
            canonical_type="test_entity",
            source="test_source",
            tenant_id="test-tenant"
        )
        
        # Create various message types
        messages = [
            Message(
                message_id="test-1",
                message_type=MessageType.ENTITY_PROCESSING,
                entity_reference=entity_ref,
                payload={"data": "test"}
            ),
            Message(
                message_id="test-2",
                message_type=MessageType.CONTROL_MESSAGE,
                entity_reference=entity_ref,
                payload={"command": "stop"}
            ),
            Message(
                message_id="test-3",
                message_type=MessageType.ERROR_MESSAGE,
                entity_reference=entity_ref,
                payload={"error": "Something failed"}
            ),
        ]
        
        # All should be valid by default
        for message in messages:
            assert processor.validate_message(message) is True
    
    def test_can_retry_validation_error_returns_false(self):
        """Test can_retry returns False for ValidationError."""
        processor = ConcreteProcessor()
        
        validation_error = ValidationError(
            "Invalid data format",
            error_code=ErrorCode.VALIDATION_FAILED,
            field="email"
        )
        
        assert processor.can_retry(validation_error) is False
    
    def test_can_retry_other_errors_return_true(self):
        """Test can_retry returns True for non-validation errors."""
        processor = ConcreteProcessor()
        
        # Test various error types
        errors = [
            RuntimeError("Runtime error"),
            ValueError("Value error"),
            KeyError("key"),
            Exception("Generic error"),
        ]
        
        for error in errors:
            assert processor.can_retry(error) is True
    
    def test_processor_interface_is_abstract(self):
        """Test that ProcessorInterface cannot be instantiated directly."""
        with pytest.raises(TypeError, match="Can't instantiate abstract class"):
            ProcessorInterface()
    
    def test_process_method_is_abstract(self):
        """Test that process method must be implemented by subclasses."""
        # This is implicitly tested by the fact that we need ConcreteProcessor
        # to implement process() for it to be instantiable
        processor = ConcreteProcessor()
        
        entity_ref = EntityReference(
            external_id="TEST-001",
            canonical_type="test_entity",
            source="test_source",
            tenant_id="test-tenant"
        )
        
        message = Message(
            message_id="test",
            message_type=MessageType.ENTITY_PROCESSING,
            entity_reference=entity_ref,
            payload={"test": "data"}
        )
        
        result = processor.process(message)
        assert result.success is True
        assert result.processing_metadata["processed"] is True
    
    def test_to_canonical_raises_not_implemented_by_default(self):
        """Test that to_canonical raises NotImplementedError by default."""
        processor = ConcreteProcessor()
        
        external_data = {"id": "123", "name": "Test"}
        metadata = {"source": "test_system"}
        
        with pytest.raises(NotImplementedError) as exc_info:
            processor.to_canonical(external_data, metadata)
        
        expected_message = "ConcreteProcessor does not implement data import"
        assert expected_message in str(exc_info.value)
        assert "Override to_canonical()" in str(exc_info.value)
    
    def test_from_canonical_raises_not_implemented_by_default(self):
        """Test that from_canonical raises NotImplementedError by default."""
        processor = ConcreteProcessor()
        
        canonical_data = {"canonical_id": "123", "canonical_name": "Test"}
        metadata = {"target": "test_system"}
        
        with pytest.raises(NotImplementedError) as exc_info:
            processor.from_canonical(canonical_data, metadata)
        
        expected_message = "ConcreteProcessor does not implement data export"
        assert expected_message in str(exc_info.value)
        assert "Override from_canonical()" in str(exc_info.value)
    
    def test_to_canonical_can_be_overridden(self):
        """Test that to_canonical can be successfully overridden."""
        processor = ConcreteProcessorWithTransformations()
        
        external_data = {"id": "ext-123", "name": "External Name"}
        metadata = {"source": "external_system"}
        
        result = processor.to_canonical(external_data, metadata)
        
        assert result["canonical_id"] == "ext-123"
        assert result["canonical_name"] == "External Name"
        assert result["transformed"] is True
    
    def test_from_canonical_can_be_overridden(self):
        """Test that from_canonical can be successfully overridden."""
        processor = ConcreteProcessorWithTransformations()
        
        canonical_data = {"canonical_id": "can-123", "canonical_name": "Canonical Name"}
        metadata = {"target": "external_system"}
        
        result = processor.from_canonical(canonical_data, metadata)
        
        assert result["id"] == "can-123"
        assert result["name"] == "Canonical Name"
        assert result["exported"] is True
    
    def test_transformation_methods_work_with_empty_data(self):
        """Test that transformation methods handle empty data gracefully."""
        processor = ConcreteProcessorWithTransformations()
        
        # Test with empty dictionaries
        empty_external = {}
        empty_canonical = {}
        empty_metadata = {}
        
        to_result = processor.to_canonical(empty_external, empty_metadata)
        assert to_result["canonical_id"] is None
        assert to_result["canonical_name"] is None
        assert to_result["transformed"] is True
        
        from_result = processor.from_canonical(empty_canonical, empty_metadata)
        assert from_result["id"] is None
        assert from_result["name"] is None
        assert from_result["exported"] is True