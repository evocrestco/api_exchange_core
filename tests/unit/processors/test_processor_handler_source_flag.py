"""
Test ProcessorHandler respects is_source_processor flag for entity persistence.

This test ensures that only source processors attempt to persist entities,
while intermediate and terminal processors do not.
"""

import pytest
from unittest.mock import Mock, MagicMock, patch
from uuid import uuid4

from src.processors.processor_handler import ProcessorHandler
from src.processors.processor_interface import ProcessorInterface
from src.processors.message import Message, EntityReference
from src.processors.processing_result import ProcessingResult
from src.processing.processor_config import ProcessorConfig
from src.processing.processing_service import ProcessingService
from src.context.tenant_context import tenant_context


class MockSourceProcessor(ProcessorInterface):
    """Mock source processor with to_canonical method."""
    
    def __init__(self):
        self.to_canonical = Mock(return_value={"id": "test", "data": "canonical"})
    
    def process(self, message: Message) -> ProcessingResult:
        return ProcessingResult.create_success(
            output_messages=[],
            processing_metadata={"test": "source"}
        )
    
    def validate_message(self, message: Message) -> bool:
        return True
    
    def get_processor_info(self):
        return {"name": "MockSourceProcessor"}


class MockIntermediateProcessor(ProcessorInterface):
    """Mock intermediate processor without to_canonical method."""
    
    def process(self, message: Message) -> ProcessingResult:
        return ProcessingResult.create_success(
            output_messages=[],
            processing_metadata={"test": "intermediate"}
        )
    
    def validate_message(self, message: Message) -> bool:
        return True
    
    def get_processor_info(self):
        return {"name": "MockIntermediateProcessor"}


class MockTerminalProcessor(ProcessorInterface):
    """Mock terminal processor without to_canonical method."""
    
    def process(self, message: Message) -> ProcessingResult:
        return ProcessingResult.create_success(
            output_messages=[],
            processing_metadata={"test": "terminal"}
        )
    
    def validate_message(self, message: Message) -> bool:
        return True
    
    def get_processor_info(self):
        return {"name": "MockTerminalProcessor"}


@pytest.fixture
def mock_processing_service():
    """Create mock processing service."""
    service = Mock(spec=ProcessingService)
    service.process_entity = Mock(return_value=str(uuid4()))
    return service


@pytest.fixture
def test_message():
    """Create test message."""
    return Message(
        entity_reference=EntityReference(
            external_id="test-123",
            canonical_type="test_entity",
            source="test_source",
            tenant_id="test_tenant"
        ),
        payload={"data": "test"},
        metadata={}
    )


class TestProcessorHandlerSourceFlag:
    """Test ProcessorHandler respects is_source_processor flag."""
    
    def test_source_processor_persists_entity(self, mock_processing_service, test_message):
        """Test that source processors persist entities."""
        # Create source processor
        processor = MockSourceProcessor()
        
        # Create config with is_source_processor=True
        config = ProcessorConfig(
            processor_name="test_source",
            processor_version="1.0.0",
            is_source_processor=True,
            enable_state_tracking=False
        )
        
        # Create handler
        handler = ProcessorHandler(
            processor=processor,
            config=config,
            processing_service=mock_processing_service
        )
        
        # Execute with tenant context
        with tenant_context("test_tenant"):
            result = handler.execute(test_message)
        
        # Verify entity was persisted
        assert result.success
        mock_processing_service.process_entity.assert_called_once()
        
        # Verify to_canonical was called
        processor.to_canonical.assert_called_once()
    
    def test_intermediate_processor_no_persist(self, mock_processing_service, test_message):
        """Test that intermediate processors don't persist entities."""
        # Create intermediate processor (no to_canonical method)
        processor = MockIntermediateProcessor()
        
        # Create config with is_source_processor=False
        config = ProcessorConfig(
            processor_name="test_intermediate",
            processor_version="1.0.0",
            is_source_processor=False,
            is_terminal_processor=False,
            enable_state_tracking=False
        )
        
        # Add entity_id to message (required for non-source processors)
        test_message.entity_reference.entity_id = str(uuid4())
        
        # Create handler
        handler = ProcessorHandler(
            processor=processor,
            config=config,
            processing_service=mock_processing_service
        )
        
        # Execute with tenant context
        with tenant_context("test_tenant"):
            result = handler.execute(test_message)
        
        # Verify entity was NOT persisted
        assert result.success
        mock_processing_service.process_entity.assert_not_called()
    
    def test_terminal_processor_no_persist(self, mock_processing_service, test_message):
        """Test that terminal processors don't persist entities."""
        # Create terminal processor
        processor = MockTerminalProcessor()
        
        # Create config with is_terminal_processor=True
        config = ProcessorConfig(
            processor_name="test_terminal",
            processor_version="1.0.0",
            is_source_processor=False,
            is_terminal_processor=True,
            enable_state_tracking=False
        )
        
        # Add entity_id to message (required for non-source processors)
        test_message.entity_reference.entity_id = str(uuid4())
        
        # Create handler
        handler = ProcessorHandler(
            processor=processor,
            config=config,
            processing_service=mock_processing_service
        )
        
        # Execute with tenant context
        with tenant_context("test_tenant"):
            result = handler.execute(test_message)
        
        # Verify entity was NOT persisted
        assert result.success
        mock_processing_service.process_entity.assert_not_called()
    
    def test_source_processor_without_to_canonical_no_persist(self, mock_processing_service, test_message):
        """Test that source processors without to_canonical don't persist."""
        # Create processor without to_canonical
        processor = MockIntermediateProcessor()  # Reuse this as it has no to_canonical
        
        # Create config with is_source_processor=True
        config = ProcessorConfig(
            processor_name="test_source_no_canonical",
            processor_version="1.0.0",
            is_source_processor=True,
            enable_state_tracking=False
        )
        
        # Create handler
        handler = ProcessorHandler(
            processor=processor,
            config=config,
            processing_service=mock_processing_service
        )
        
        # Execute with tenant context
        with tenant_context("test_tenant"):
            result = handler.execute(test_message)
        
        # Verify entity was NOT persisted (no to_canonical method)
        assert result.success
        mock_processing_service.process_entity.assert_not_called()
    
    def test_non_source_processor_with_to_canonical_no_persist(self, mock_processing_service, test_message):
        """Test that non-source processors with to_canonical don't persist."""
        # Create processor with to_canonical but not a source
        processor = MockSourceProcessor()  # Has to_canonical
        
        # Create config with is_source_processor=False
        config = ProcessorConfig(
            processor_name="test_not_source",
            processor_version="1.0.0",
            is_source_processor=False,
            enable_state_tracking=False
        )
        
        # Add entity_id to message (required for non-source processors)
        test_message.entity_reference.entity_id = str(uuid4())
        
        # Create handler
        handler = ProcessorHandler(
            processor=processor,
            config=config,
            processing_service=mock_processing_service
        )
        
        # Execute with tenant context
        with tenant_context("test_tenant"):
            result = handler.execute(test_message)
        
        # Verify entity was NOT persisted despite having to_canonical
        assert result.success
        mock_processing_service.process_entity.assert_not_called()
        # to_canonical should not even be called
        processor.to_canonical.assert_not_called()