"""
Tests for ProcessorInterface v2 abstract contract and ProcessorContext.

Uses parameterization and good test practices to keep tests concise but comprehensive.
Follows NO MOCKS philosophy - uses real implementations and services.
"""

from abc import ABC
from typing import Any, Dict, Optional

import pytest

from src.context.tenant_context import tenant_context as tenant_ctx
from src.exceptions import ValidationError
from src.processors.processing_result import ProcessingResult, ProcessingStatus
from src.processors.v2.message import Message, MessageType
from src.processors.v2.processor_interface import ProcessorContext, ProcessorInterface

# ==================== TEST PROCESSOR IMPLEMENTATIONS ====================

class ConcreteTestProcessor(ProcessorInterface):
    """Simple concrete processor for testing."""
    
    def __init__(self, name: str = "TestProcessor"):
        self.name = name
        self.call_count = 0
        
    def process(self, message: Message, context: ProcessorContext) -> ProcessingResult:
        self.call_count += 1
        return ProcessingResult.create_success(
            processing_metadata={"processor": self.name, "calls": self.call_count}
        )
    
    def get_processor_info(self) -> Dict[str, Any]:
        return {"name": self.name, "version": "1.0.0"}


class ValidatingProcessor(ProcessorInterface):
    """Processor with custom validation logic."""
    
    def process(self, message: Message, context: ProcessorContext) -> ProcessingResult:
        return ProcessingResult.create_success()
        
    def validate_message(self, message: Message) -> bool:
        return message.payload.get("valid", False)


class RetryableProcessor(ProcessorInterface):
    """Processor with custom retry logic."""
    
    def process(self, message: Message, context: ProcessorContext) -> ProcessingResult:
        return ProcessingResult.create_success()
        
    def can_retry(self, error: Exception) -> bool:
        return isinstance(error, (ConnectionError, TimeoutError))


class IncompleteProcessor(ProcessorInterface):
    """Processor missing abstract method implementation."""
    pass


# ==================== TEST CLASSES ====================

class TestProcessorInterfaceContract:
    """Test ProcessorInterface abstract contract enforcement."""
    
    def test_cannot_instantiate_abstract_interface(self):
        """ProcessorInterface cannot be instantiated directly."""
        with pytest.raises(TypeError, match="Can't instantiate abstract class"):
            ProcessorInterface()
    
    def test_abstract_method_enforcement(self):
        """Concrete classes must implement abstract process method."""
        with pytest.raises(TypeError, match="Can't instantiate abstract class"):
            IncompleteProcessor()
    
    @pytest.mark.parametrize("processor_class", [
        ConcreteTestProcessor,
        ValidatingProcessor, 
        RetryableProcessor
    ])
    def test_concrete_implementations_valid(self, processor_class):
        """Concrete implementations can be instantiated and called."""
        processor = processor_class()
        assert isinstance(processor, ProcessorInterface)
        assert hasattr(processor, 'process')
        assert callable(processor.process)


class TestDefaultMethodImplementations:
    """Test default method implementations in ProcessorInterface."""
    
    @pytest.fixture
    def processor(self):
        return ConcreteTestProcessor()
    
    @pytest.fixture
    def sample_message(self, processor_context, tenant_context):
        with tenant_ctx(tenant_context["id"]):
            # Create a real entity first
            entity_id = processor_context.persist_entity(
                external_id="test-message-entity",
                canonical_type="test",
                source="test_system",
                data={"test": "data"}
            )
            
            # Get the real entity
            entity = processor_context.get_entity(entity_id)
            
            # Create message with real entity
            return Message.create_entity_message(
                entity=entity,
                payload={"test": "data"}
            )
    
    def test_validate_message_default_accepts_all(self, processor, sample_message):
        """Default validate_message accepts all messages."""
        assert processor.validate_message(sample_message) is True
        
        # Test with various message types using the same entity but different payloads
        entity = sample_message.entity
        
        empty_message = Message.create_entity_message(entity=entity, payload={})
        assert processor.validate_message(empty_message) is True
        
        complex_message = Message.create_entity_message(
            entity=entity,
            payload={"nested": {"data": [1, 2, 3]}, "status": "complex"}
        )
        assert processor.validate_message(complex_message) is True
    
    def test_validate_message_custom_implementation(self, sample_message):
        """Custom validate_message can reject messages."""
        processor = ValidatingProcessor()
        entity = sample_message.entity
        
        valid_message = Message.create_entity_message(entity=entity, payload={"valid": True})
        invalid_message = Message.create_entity_message(entity=entity, payload={"valid": False})
        
        assert processor.validate_message(valid_message) is True
        assert processor.validate_message(invalid_message) is False
    
    def test_get_processor_info_default(self):
        """Default get_processor_info returns class name and version."""
        # Create processor without custom get_processor_info implementation
        class DefaultInfoProcessor(ProcessorInterface):
            def process(self, message, context):
                return ProcessingResult.create_success()
        
        processor = DefaultInfoProcessor()
        info = processor.get_processor_info()
        
        assert isinstance(info, dict)
        assert info["name"] == "DefaultInfoProcessor"
        assert "version" in info
    
    @pytest.mark.parametrize("error_type,expected_retry", [
        (ConnectionError("Network issue"), False),
        (TimeoutError("Request timeout"), False), 
        (ValueError("Invalid data"), False),
        (Exception("Generic error"), False),
    ])
    def test_can_retry_default_no_retry(self, processor, error_type, expected_retry):
        """Default can_retry never allows retries."""
        assert processor.can_retry(error_type) is expected_retry
    
    def test_can_retry_custom_implementation(self):
        """Custom can_retry can allow specific error types."""
        processor = RetryableProcessor()
        
        # Should retry these errors
        assert processor.can_retry(ConnectionError()) is True
        assert processor.can_retry(TimeoutError()) is True
        
        # Should not retry these
        assert processor.can_retry(ValueError()) is False
        assert processor.can_retry(Exception()) is False


class TestProcessorContextIntegration:
    """Test ProcessorContext integration with processors."""
    
    @pytest.fixture
    def processor(self):
        return ConcreteTestProcessor("IntegrationProcessor")
    
    @pytest.fixture 
    def sample_message(self, processor_context, tenant_context):
        with tenant_ctx(tenant_context["id"]):
            # Create a real entity first
            entity_id = processor_context.persist_entity(
                external_id="integration-test-entity",
                canonical_type="order",
                source="test_system",
                data={"order_id": "test-order-123"}
            )
            
            # Get the real entity
            entity = processor_context.get_entity(entity_id)
            
            # Create message with real entity
            return Message.create_entity_message(
                entity=entity,
                payload={
                    "order_id": "test-order-123",
                    "customer": "Test Customer",
                    "items": [{"sku": "ITEM-001", "qty": 2}]
                }
            )
    
    def test_processor_context_services_available(self, processor_context):
        """ProcessorContext provides access to all framework services."""
        assert processor_context.processing_service is not None
        assert processor_context.state_tracking_service is not None  
        assert processor_context.error_service is not None
    
    def test_minimal_processor_context(self, processing_service):
        """ProcessorContext works with minimal service configuration."""
        context = ProcessorContext(
            processing_service=processing_service,
            state_tracking_service=None,
            error_service=None
        )
        
        assert context.processing_service is processing_service
        assert context.state_tracking_service is None
        assert context.error_service is None
    
    def test_entity_persistence_workflow(self, processor_context, tenant_context):
        """Test entity persistence through ProcessorContext."""
        with tenant_ctx(tenant_context["id"]):
            # Persist entity
            entity_id = processor_context.persist_entity(
                external_id="workflow-test-001",
                canonical_type="order", 
                source="test_system",
                data={"status": "new", "total": 100.00},
                metadata={"workflow": "test"}
            )
            
            assert entity_id is not None
            
            # Retrieve entity
            entity = processor_context.get_entity(entity_id)
            assert entity is not None
            assert entity.external_id == "workflow-test-001"
            assert entity.canonical_type == "order"
            assert entity.source == "test_system"
    
    def test_processor_execution_with_context(self, processor, processor_context, sample_message):
        """Test processor execution with full context."""
        # Execute processor
        result = processor.process(sample_message, processor_context)
        
        # Verify result
        assert isinstance(result, ProcessingResult)
        assert result.status == ProcessingStatus.SUCCESS
        assert processor.call_count == 1
        
        # Verify processor info
        info = processor.get_processor_info()
        assert info["name"] == "IntegrationProcessor"


class TestMessageProcessingWorkflow:
    """Test complete message processing workflows."""
    
    @pytest.fixture
    def processor(self):
        return ConcreteTestProcessor("WorkflowProcessor")
    
    @pytest.mark.parametrize("message_data,expected_valid", [
        ({"valid": True, "data": "test"}, True),
        ({"valid": False, "data": "test"}, False),
        ({}, False),  # Missing 'valid' key defaults to False
    ])
    def test_message_validation_workflow(self, message_data, expected_valid, processor_context, tenant_context):
        """Test message validation with different data."""
        processor = ValidatingProcessor()
        
        with tenant_ctx(tenant_context["id"]):
            # Create a real entity for the test
            entity_id = processor_context.persist_entity(
                external_id="validation-test-entity",
                canonical_type="test",
                source="test_system",
                data={"test": "data"}
            )
            entity = processor_context.get_entity(entity_id)
            
            # Create message with test payload
            message = Message.create_entity_message(entity=entity, payload=message_data)
            
            is_valid = processor.validate_message(message)
            assert is_valid is expected_valid
    
    def test_entity_state_tracking_workflow(self, processor_context, tenant_context):
        """Test entity creation and state tracking."""
        from src.db.db_base import EntityStateEnum
        
        with tenant_ctx(tenant_context["id"]):
            # Create entity
            entity_id = processor_context.persist_entity(
                external_id="state-test-001",
                canonical_type="order",
                source="test_system", 
                data={"status": "received"}
            )
            
            # Record state transition
            processor_context.record_state_transition(
                entity_id=entity_id,
                from_state=EntityStateEnum.RECEIVED,
                to_state=EntityStateEnum.PROCESSING,
                processor_name="WorkflowProcessor",
                metadata={"step": "validation"}
            )
            
            # Verify transition was recorded
            history = processor_context.get_entity_state_history(entity_id)
            assert history is not None
            assert len(history.transitions) > 0
    
    def test_error_handling_workflow(self, processor_context, tenant_context):
        """Test error recording and retrieval."""
        with tenant_ctx(tenant_context["id"]):
            # Create entity
            entity_id = processor_context.persist_entity(
                external_id="error-test-001", 
                canonical_type="order",
                source="test_system",
                data={"status": "error"}
            )
            
            # Record error
            error_id = processor_context.record_processing_error(
                entity_id=entity_id,
                processor_name="WorkflowProcessor",
                error_code="VALIDATION_ERROR",
                error_message="Test validation failed",
                error_details={"field": "customer_id", "issue": "missing"},
                is_retryable=True
            )
            
            assert error_id is not None
            
            # Verify error was recorded
            errors = processor_context.get_entity_errors(entity_id)
            assert len(errors) == 1
            assert errors[0].error_type_code == "VALIDATION_ERROR"
            assert errors[0].message == "Test validation failed"


class TestEdgeCasesAndErrorConditions:
    """Test edge cases and error conditions."""
    
    def test_processor_with_invalid_message(self):
        """Test processor behavior with None or invalid messages."""
        processor = ConcreteTestProcessor()
        
        # These should not crash (defensive programming)
        assert processor.validate_message(None) is True  # Default accepts all
        assert isinstance(processor.get_processor_info(), dict)
        assert processor.can_retry(Exception()) is False
    
    def test_context_graceful_degradation(self, processing_service):
        """Test context gracefully handles missing optional services."""
        context = ProcessorContext(
            processing_service=processing_service,
            state_tracking_service=None,
            error_service=None
        )
        
        # These should not crash when services are missing
        result = context.record_state_transition(
            entity_id="test-id",
            from_state="RECEIVED", 
            to_state="PROCESSING",
            processor_name="TestProcessor"
        )
        # Should handle gracefully (returns None or similar)
        
        errors = context.get_entity_errors("test-id")
        assert isinstance(errors, list)  # Should return empty list
        assert len(errors) == 0
    
    @pytest.mark.parametrize("processor_name", [
        "SimpleProcessor",
        "Complex-Processor_v2", 
        "Processor123",
        "П", # Unicode
    ])
    def test_processor_info_with_various_names(self, processor_name):
        """Test processor info with various naming patterns."""
        
        class NamedProcessor(ProcessorInterface):
            def __init__(self, name):
                self.name = name
                
            def process(self, message, context):
                return ProcessingResult.create_success()
                
            def get_processor_info(self):
                return {"name": self.name, "version": "1.0.0"}
        
        processor = NamedProcessor(processor_name)
        info = processor.get_processor_info()
        
        assert info["name"] == processor_name
        assert "version" in info