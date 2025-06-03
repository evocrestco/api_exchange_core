"""
Simple test to verify the harness works.
"""

import sys
import os

# Add parent directories to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

from src.context.tenant_context import tenant_context
from src.db.db_config import DatabaseConfig, DatabaseManager
from src.processing.processing_service import ProcessingService
from src.processing.duplicate_detection import DuplicateDetectionService
from src.processing.entity_attributes import EntityAttributeBuilder
from src.processing.processor_config import ProcessorConfig
from src.services.entity_service import EntityService
from src.repositories.entity_repository import EntityRepository
from src.processors.processor_handler import ProcessorHandler
from src.services.state_tracking_service import StateTrackingService
from src.repositories.state_transition_repository import StateTransitionRepository

from processors.verification_processor import VerificationProcessor
from processors.validation_processor import ValidationProcessor
from utils.test_data_generators import TestMessageGenerator

def run_simple_test():
    import logging
    # Set up logging to see all errors
    logging.basicConfig(level=logging.ERROR, format='%(levelname)s: %(message)s')
    """Run a simple entity creation test."""
    print("Setting up test database connection...")
    
    # Use test harness database
    db_config = DatabaseConfig(
        db_type="postgres",
        host="localhost",
        port="5433",  # Test harness postgres on different port
        database="test_harness",
        username="harness_user",
        password="harness_password"
    )
    
    # Initialize services
    db_manager = DatabaseManager(db_config)
    entity_repository = EntityRepository(db_manager=db_manager)
    entity_service = EntityService(entity_repository=entity_repository)
    duplicate_detection_service = DuplicateDetectionService(entity_repository=entity_repository)
    attribute_builder = EntityAttributeBuilder()
    
    # Initialize state tracking service
    state_transition_repository = StateTransitionRepository(db_manager=db_manager)
    state_tracking_service = StateTrackingService(db_manager=db_manager)
    
    # Initialize processing error service
    from src.repositories.processing_error_repository import ProcessingErrorRepository
    from src.services.processing_error_service import ProcessingErrorService
    processing_error_repository = ProcessingErrorRepository(db_manager=db_manager)
    processing_error_service = ProcessingErrorService(repository=processing_error_repository)
    
    processing_service = ProcessingService(
        entity_service=entity_service,
        entity_repository=entity_repository,
        duplicate_detection_service=duplicate_detection_service,
        attribute_builder=attribute_builder
    )
    
    # Set state tracking service on processing service
    processing_service.set_state_tracking_service(state_tracking_service)
    
    print("Creating test message...")
    
    # Create test message
    test_message = TestMessageGenerator.create_entity_creation_test(
        tenant_id="simple_test_tenant",
        canonical_type="simple_entity",
        source="simple_test"
    )
    
    print(f"Test ID: {test_message.payload['test_id']}")
    
    # Create processor config
    verification_config = ProcessorConfig(
        processor_name="verification_processor",
        processor_version="1.0.0",
        enable_state_tracking=True,
        is_source_processor=True,
        enable_duplicate_detection=True
    )
    
    # Create verification processor
    verification_processor = VerificationProcessor(
        entity_service=entity_service,
        processing_service=processing_service,
        config=verification_config,
        state_tracking_service=state_tracking_service,
        processing_error_service=processing_error_service
    )
    
    # Create handler
    verification_handler = ProcessorHandler(
        processor=verification_processor,
        config=verification_config,
        processing_service=processing_service,
        state_tracking_service=state_tracking_service,
        error_service=processing_error_service
    )
    
    print("Running verification processor...")
    
    # Run with tenant context
    with tenant_context("simple_test_tenant"):
        result = verification_handler.execute(test_message)
    
    if result.success:
        print("✅ Verification processor succeeded!")
        print(f"Entities created: {len(result.entities_created)}")
        print(f"Processing metadata: {result.processing_metadata}")
        
        # Check if we have output for validation
        if result.output_messages:
            print("\nRunning validation processor...")
            
            validation_config = ProcessorConfig(
                processor_name="validation_processor",
                processor_version="1.0.0",
                is_terminal_processor=True
            )
            
            validation_processor = ValidationProcessor(config=validation_config)
            validation_handler = ProcessorHandler(
                processor=validation_processor,
                config=validation_config,
                processing_service=processing_service
            )
            
            validation_message = result.output_messages[0]
            
            with tenant_context("simple_test_tenant"):
                val_result = validation_handler.execute(validation_message)
            
            if val_result.success:
                print("✅ Validation processor succeeded!")
                outcome = val_result.processing_metadata.get("test_outcome", {})
                print(f"\nFinal Test Result: {'✅ PASSED' if outcome.get('passed') else '❌ FAILED'}")
                print(f"Test Summary: {val_result.processing_metadata.get('test_summary', 'N/A')}")
            else:
                print(f"❌ Validation failed: {val_result.error_message}")
        else:
            print("❌ No output message from verification processor")
    else:
        print(f"❌ Verification failed: {result.error_message}")
        if result.error_details:
            print(f"   Error details: {result.error_details}")
        print(f"   Error code: {result.error_code}")
    
    # Cleanup
    db_manager.close()
    print("\nTest complete!")

if __name__ == "__main__":
    run_simple_test()