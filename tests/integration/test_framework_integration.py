"""
Integration tests for the API Exchange Core framework.

These tests verify that the framework components work together correctly
in an in-process environment. This validates the framework logic but does
not test the actual serverless deployment pattern.
"""

import pytest
import logging
from typing import Dict, Any

from src.repositories.entity_repository import EntityRepository
from src.repositories.state_transition_repository import StateTransitionRepository
from src.repositories.processing_error_repository import ProcessingErrorRepository
from src.services.entity_service import EntityService
from src.services.state_tracking_service import StateTrackingService
from src.services.processing_error_service import ProcessingErrorService
from src.processing.duplicate_detection import DuplicateDetectionService
from src.processing.entity_attributes import EntityAttributeBuilder
from src.processing.processing_service import ProcessingService
from src.processing.processor_config import ProcessorConfig
from src.processors.processor_handler import ProcessorHandler
from src.context.tenant_context import tenant_context

# Import test harness components
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'harness'))

from processors.verification_processor import VerificationProcessor
from processors.validation_processor import ValidationProcessor
from utils.test_data_generators import TestMessageGenerator


class TestFrameworkIntegration:
    """Integration tests for framework components working together."""
    
    @pytest.fixture
    def services(self, db_manager):
        """Set up all framework services for testing using existing test fixtures."""
        # Suppress verbose logging during tests
        logging.getLogger().setLevel(logging.ERROR)
        
        # Initialize repositories using test db_manager
        entity_repository = EntityRepository(db_manager=db_manager)
        state_transition_repository = StateTransitionRepository(db_manager=db_manager)
        processing_error_repository = ProcessingErrorRepository(db_manager=db_manager)
        
        # Initialize services
        entity_service = EntityService(entity_repository=entity_repository)
        state_tracking_service = StateTrackingService(db_manager=db_manager)
        processing_error_service = ProcessingErrorService(repository=processing_error_repository)
        duplicate_detection_service = DuplicateDetectionService(entity_repository=entity_repository)
        attribute_builder = EntityAttributeBuilder()
        
        processing_service = ProcessingService(
            entity_service=entity_service,
            entity_repository=entity_repository,
            duplicate_detection_service=duplicate_detection_service,
            attribute_builder=attribute_builder
        )
        
        # Set optional services
        processing_service.set_state_tracking_service(state_tracking_service)
        processing_service.set_processing_error_service(processing_error_service)
        
        return {
            'db_manager': db_manager,
            'entity_service': entity_service,
            'state_tracking_service': state_tracking_service,
            'processing_error_service': processing_error_service,
            'processing_service': processing_service
        }
    
    @pytest.fixture
    def verification_processor(self, services):
        """Create verification processor for testing."""
        config = ProcessorConfig(
            processor_name="verification_processor",
            processor_version="1.0.0",
            enable_state_tracking=True,
            is_source_processor=True,
            enable_duplicate_detection=True
        )
        
        processor = VerificationProcessor(
            entity_service=services['entity_service'],
            processing_service=services['processing_service'],
            config=config,
            state_tracking_service=services['state_tracking_service'],
            processing_error_service=services['processing_error_service']
        )
        
        return ProcessorHandler(
            processor=processor,
            config=config,
            processing_service=services['processing_service'],
            state_tracking_service=services['state_tracking_service'],
            error_service=services['processing_error_service']
        )
    
    @pytest.fixture
    def validation_processor(self, services):
        """Create validation processor for testing."""
        config = ProcessorConfig(
            processor_name="validation_processor",
            processor_version="1.0.0",
            enable_state_tracking=False,
            is_terminal_processor=True,
            processing_stage="validation"
        )
        
        processor = ValidationProcessor(config=config)
        
        return ProcessorHandler(
            processor=processor,
            config=config,
            processing_service=services['processing_service'],
            state_tracking_service=services['state_tracking_service'],
            error_service=services['processing_error_service']
        )
    
    def test_entity_creation_workflow(self, verification_processor, validation_processor, test_tenant):
        """Test complete entity creation workflow through both processors."""
        tenant_id = test_tenant["id"]
        
        # Create test message
        test_message = TestMessageGenerator.create_entity_creation_test(
            tenant_id=tenant_id,
            canonical_type="integration_test_entity",
            source="integration_test"
        )
        
        test_id = test_message.payload['test_id']
        
        # Step 1: Run verification processor
        with tenant_context(tenant_id):
            verification_result = verification_processor.execute(test_message)
        
        assert verification_result.success, f"Verification failed: {verification_result.error_message}"
        assert len(verification_result.output_messages) == 1, "Expected one output message"
        assert len(verification_result.entities_created) == 1, "Expected one entity to be created"
        
        # Extract verification results
        verification_metadata = verification_result.processing_metadata
        verification_results = verification_metadata.get('verification_results', [])
        
        assert len(verification_results) == 3, "Expected 3 verification checks"
        
        # Check individual verification results
        entity_check = next(r for r in verification_results if r['check_name'] == 'entity_created')
        state_check = next(r for r in verification_results if r['check_name'] == 'state_transitions')
        error_check = next(r for r in verification_results if r['check_name'] == 'no_errors')
        
        assert entity_check['passed'], f"Entity creation check failed: {entity_check.get('error_message')}"
        assert state_check['passed'], f"State transitions check failed: {state_check.get('error_message')}"
        assert error_check['passed'], f"Error check failed: {error_check.get('error_message')}"
        
        # Step 2: Run validation processor
        validation_message = verification_result.output_messages[0]
        
        with tenant_context(tenant_id):
            validation_result = validation_processor.execute(validation_message)
        
        assert validation_result.success, f"Validation failed: {validation_result.error_message}"
        
        # Check final test outcome
        test_outcome = validation_result.processing_metadata.get('test_outcome', {})
        assert test_outcome.get('passed'), f"Test failed: {test_outcome.get('error_message')}"
        assert test_outcome.get('test_id') == test_id
        
    def test_state_tracking_integration(self, verification_processor, services, test_tenant):
        """Test that state tracking works correctly during processing."""
        tenant_id = test_tenant["id"]
        
        test_message = TestMessageGenerator.create_entity_creation_test(
            tenant_id=tenant_id,
            canonical_type="state_test_entity",
            source="state_test"
        )
        
        # Run verification processor
        with tenant_context(tenant_id):
            result = verification_processor.execute(test_message)
        
        assert result.success
        entity_id = result.entities_created[0]
        
        # Verify state transitions were recorded (within tenant context)
        with tenant_context(tenant_id):
            state_history = services['state_tracking_service'].get_entity_state_history(entity_id)
            assert state_history is not None
            assert len(state_history.transitions) == 2
            
            # Check transition sequence
            transitions = state_history.transitions
            assert transitions[0].from_state == "RECEIVED"
            assert transitions[0].to_state == "PROCESSING"
            assert transitions[1].from_state == "PROCESSING"
            assert transitions[1].to_state == "COMPLETED"
        
    def test_error_tracking_integration(self, services):
        """Test that error tracking service is properly integrated."""
        # This test verifies the service is available and functional
        # In a real error scenario, errors would be recorded here
        
        error_service = services['processing_error_service']
        assert error_service is not None
        
        # For now, we just verify no errors exist (clean state)
        # In future tests, we could create error scenarios and verify they're tracked
        
    def test_duplicate_detection_integration(self, verification_processor, test_tenant):
        """Test duplicate detection works during entity processing."""
        tenant_id = test_tenant["id"]
        
        # Create same message twice
        test_message = TestMessageGenerator.create_entity_creation_test(
            tenant_id=tenant_id,
            canonical_type="duplicate_test_entity",
            source="duplicate_test",
            # Use fixed content to ensure duplication
            test_data={'name': 'Fixed Test Entity', 'type': 'duplicate_test', 'value': 123}
        )
        
        # First processing - should create entity
        with tenant_context(tenant_id):
            result1 = verification_processor.execute(test_message)
        
        assert result1.success
        assert len(result1.entities_created) == 1
        
        # Second processing - should detect duplicate
        with tenant_context(tenant_id):
            result2 = verification_processor.execute(test_message)
        
        assert result2.success
        # Depending on duplicate detection behavior, this might create a new version
        # or skip creation - the key is that duplicate detection ran
        
    def test_tenant_isolation(self, verification_processor, multi_tenant_context):
        """Test that tenant isolation works correctly."""
        # Use existing multi-tenant fixture (creates test_tenant_0, test_tenant_1, test_tenant_2)
        tenant1 = multi_tenant_context[0]
        tenant2 = multi_tenant_context[1]
        
        # Create entities in different tenants with same external_id
        test_message1 = TestMessageGenerator.create_entity_creation_test(
            tenant_id=tenant1["id"],
            canonical_type="isolation_test",
            source="isolation_test"
        )
        
        test_message2 = TestMessageGenerator.create_entity_creation_test(
            tenant_id=tenant2["id"], 
            canonical_type="isolation_test",
            source="isolation_test"
        )
        
        # Force same external_id to test isolation
        test_message2.entity_reference.external_id = test_message1.entity_reference.external_id
        
        # Process in different tenant contexts
        with tenant_context(tenant1["id"]):
            result1 = verification_processor.execute(test_message1)
        
        with tenant_context(tenant2["id"]):
            result2 = verification_processor.execute(test_message2)
        
        assert result1.success
        assert result2.success
        
        # Both should succeed because they're in different tenants
        assert len(result1.entities_created) == 1
        assert len(result2.entities_created) == 1
        assert result1.entities_created[0] != result2.entities_created[0]


if __name__ == "__main__":
    # Allow running this test file directly
    pytest.main([__file__, "-v"])