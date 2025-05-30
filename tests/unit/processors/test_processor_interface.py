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