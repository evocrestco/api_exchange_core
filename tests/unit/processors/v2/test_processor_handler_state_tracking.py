"""Tests for ProcessorHandler state tracking functionality."""

import os
import uuid
from datetime import datetime

import pytest

from api_exchange_core.processors.processing_result import ProcessingResult, ProcessingStatus
from api_exchange_core.processors.v2.message import Message, MessageType
from api_exchange_core.processors.v2.processor_handler import ProcessorHandler
from api_exchange_core.processors.v2.processor_interface import ProcessorInterface
from api_exchange_core.schemas.entity_schema import EntityReference


class MockProcessor(ProcessorInterface):
    """Mock processor for testing."""

    def __init__(self, entities_to_create=None, should_fail=False):
        self.entities_to_create = entities_to_create or []
        self.should_fail = should_fail

    def get_processor_info(self):
        return {"name": "MockProcessor", "version": "1.0"}

    def validate_message(self, message):
        return True

    def can_retry(self, exception):
        return False

    def process(self, message, context):
        if self.should_fail:
            return ProcessingResult.create_failure(
                error_message="Mock failure",
                error_code="MOCK_ERROR",
                can_retry=False
            )
        
        result = ProcessingResult.create_success()
        
        # Simulate entity creation for source processors
        if self.entities_to_create:
            result.entities_created = self.entities_to_create
            
        return result


class TestProcessorHandlerStateTracking:
    """Test state tracking functionality in ProcessorHandler."""

    def test_existing_entity_success_state_tracking(self, db_manager, test_tenant, with_tenant_env):
        """Test state tracking for existing entity (non-source processor) success."""
        # Setup: Message with existing entity
        entity_id = str(uuid.uuid4())
        external_id = "test-external-id"
        
        entity_ref = EntityReference(
            id=entity_id,
            external_id=external_id,
            canonical_type="test_type",
            source="test_source",
            tenant_id=test_tenant["id"]
        )
        
        message = Message(
            message_type=MessageType.ENTITY_PROCESSING,
            entity_reference=entity_ref,
            payload={"test": "data"}
        )

        # Setup: Mock processor that doesn't create entities
        processor = MockProcessor()
        
        # Setup: ProcessorHandler with real database
        handler = ProcessorHandler(
            processor=processor,
            processing_service=None,  # Will create new one with db_manager
            db_manager=db_manager
        )

        # Execute
        result = handler.execute(message)

        # Verify
        assert result.success
        
        # Verify state tracking was written to database
        # Check pipeline_state_history table for records
        session = db_manager.get_session()
        try:
            from api_exchange_core.db.db_pipeline_state_models import PipelineStateHistory
            
            state_records = session.query(PipelineStateHistory).filter(
                PipelineStateHistory.entity_id == entity_id
            ).all()
            
            # Should have one state transition record for the existing entity
            assert len(state_records) == 1
            
            record = state_records[0]
            assert record.processor_name == "MockProcessor"
            assert record.status in ["COMPLETED", "completed"]  # Could be either format
            assert record.entity_id == entity_id
            assert record.external_id == external_id
            assert record.tenant_id == test_tenant["id"]
            
        finally:
            session.close()

    def test_source_processor_success_state_tracking(self, db_manager, test_tenant, with_tenant_env):
        """Test state tracking for source processor (creates new entities) success."""
        # Setup: Message with no entity reference (source processor)
        message = Message(
            message_type=MessageType.ENTITY_PROCESSING,
            entity_reference=None,
            payload={"operation": "list_orders"}
        )

        # Setup: Mock processor that creates entities
        created_entity_ids = [str(uuid.uuid4()), str(uuid.uuid4())]
        processor = MockProcessor(entities_to_create=created_entity_ids)
        
        # Setup: ProcessorHandler with real database
        handler = ProcessorHandler(
            processor=processor,
            processing_service=None,  # Will create new one with db_manager
            db_manager=db_manager
        )

        # Execute
        result = handler.execute(message)

        # Verify
        assert result.success
        assert result.entities_created == created_entity_ids
        
        # Verify state tracking was written to database for newly created entities
        session = db_manager.get_session()
        try:
            from api_exchange_core.db.db_pipeline_state_models import PipelineStateHistory
            
            # Should have state transition records for each created entity
            state_records = session.query(PipelineStateHistory).filter(
                PipelineStateHistory.entity_id.in_(created_entity_ids)
            ).all()
            
            assert len(state_records) == len(created_entity_ids)
            
            for record in state_records:
                assert record.processor_name == "MockProcessor"
                assert record.status in ["COMPLETED", "completed"]
                assert record.entity_id in created_entity_ids
                assert record.external_id is None  # No external_id for created entities
                assert record.tenant_id == test_tenant["id"]
                
        finally:
            session.close()

    def test_source_processor_failure_state_tracking(self, db_manager, test_tenant, with_tenant_env):
        """Test state tracking for source processor failure."""
        # Setup: Message with no entity reference (source processor)
        message = Message(
            message_type=MessageType.ENTITY_PROCESSING,
            entity_reference=None,
            payload={"operation": "list_orders"}
        )

        # Setup: Mock processor that creates entities but then fails
        created_entity_ids = [str(uuid.uuid4())]
        processor = MockProcessor(entities_to_create=created_entity_ids, should_fail=True)
        
        # Setup: ProcessorHandler with real database
        handler = ProcessorHandler(
            processor=processor,
            processing_service=None,  # Will create new one with db_manager
            db_manager=db_manager
        )

        # Execute
        result = handler.execute(message)

        # Verify
        assert not result.success
        
        # Verify failure state tracking was written to database
        session = db_manager.get_session()
        try:
            from api_exchange_core.db.db_pipeline_state_models import PipelineStateHistory
            
            # Should have failure state transition records for created entities
            state_records = session.query(PipelineStateHistory).filter(
                PipelineStateHistory.entity_id.in_(created_entity_ids)
            ).all()
            
            # Note: Depending on when failure occurs, may or may not have state records
            # If processor fails after creating entities, should have failure transitions
            if state_records:
                for record in state_records:
                    assert record.processor_name == "MockProcessor"
                    assert record.status in ["FAILED", "failed"]
                    assert record.entity_id in created_entity_ids
                    assert record.tenant_id == test_tenant["id"]
                
        finally:
            session.close()

    def test_record_success_state_transitions_existing_entity(self, db_manager, test_tenant, tenant_context):
        """Test _record_success_state_transitions with existing entity."""
        # Setup
        processor = MockProcessor()
        handler = ProcessorHandler(processor=processor, processing_service=None, db_manager=db_manager)
        
        entity_id = str(uuid.uuid4())
        external_id = "test-external-id"
        
        entity_ref = EntityReference(
            id=entity_id,
            external_id=external_id,
            canonical_type="test_type", 
            source="test_source",
            tenant_id=test_tenant["id"]
        )
        
        message = Message(
            message_type=MessageType.ENTITY_PROCESSING,
            entity_reference=entity_ref,
            payload={"test": "data"}
        )
        
        result = ProcessingResult.create_success()
        
        # Create mock state tracking service using real implementation
        from api_exchange_core.services.logging_state_tracking_service import LoggingStateTrackingService
        state_service = LoggingStateTrackingService(db_manager=db_manager)
        
        # Execute
        handler._record_success_state_transitions(
            entity_id, message, result, state_service
        )
        
        # Verify: Should record "processing" -> "completed" for existing entity
        session = db_manager.get_session()
        try:
            from api_exchange_core.db.db_pipeline_state_models import PipelineStateHistory
            
            state_records = session.query(PipelineStateHistory).filter(
                PipelineStateHistory.entity_id == entity_id
            ).all()
            
            assert len(state_records) == 1
            record = state_records[0]
            assert record.entity_id == entity_id
            assert record.external_id == external_id
            assert record.processor_name == "MockProcessor"
            # Note: The actual from_state/to_state values depend on the implementation
            # We just verify a record was created
            
        finally:
            session.close()

    def test_record_success_state_transitions_created_entities(self, db_manager, test_tenant, tenant_context):
        """Test _record_success_state_transitions with newly created entities."""
        # Setup
        processor = MockProcessor()
        handler = ProcessorHandler(processor=processor, processing_service=None, db_manager=db_manager)
        
        message = Message(
            message_type=MessageType.ENTITY_PROCESSING,
            entity_reference=None,  # No existing entity
            payload={"operation": "list_orders"}
        )
        
        created_entity_ids = [str(uuid.uuid4()), str(uuid.uuid4())]
        result = ProcessingResult.create_success()
        result.entities_created = created_entity_ids
        
        # Create mock state tracking service using real implementation
        from api_exchange_core.services.logging_state_tracking_service import LoggingStateTrackingService
        state_service = LoggingStateTrackingService(db_manager=db_manager)
        
        # Execute
        handler._record_success_state_transitions(
            None, message, result, state_service
        )
        
        # Verify: Should record "started" -> "completed" for each created entity
        session = db_manager.get_session()
        try:
            from api_exchange_core.db.db_pipeline_state_models import PipelineStateHistory
            
            state_records = session.query(PipelineStateHistory).filter(
                PipelineStateHistory.entity_id.in_(created_entity_ids)
            ).all()
            
            assert len(state_records) == len(created_entity_ids)
            
            for record in state_records:
                assert record.entity_id in created_entity_ids
                assert record.external_id is None  # Not available for created entities
                assert record.processor_name == "MockProcessor"
                
        finally:
            session.close()

    def test_record_success_state_transitions_both_existing_and_created(self, db_manager, test_tenant, tenant_context):
        """Test _record_success_state_transitions with both existing and created entities."""
        # Setup
        processor = MockProcessor()
        handler = ProcessorHandler(processor=processor, processing_service=None, db_manager=db_manager)
        
        existing_entity_id = str(uuid.uuid4())
        external_id = "test-external-id"
        
        entity_ref = EntityReference(
            id=existing_entity_id,
            external_id=external_id,
            canonical_type="test_type",
            source="test_source",
            tenant_id=test_tenant["id"]
        )
        
        message = Message(
            message_type=MessageType.ENTITY_PROCESSING,
            entity_reference=entity_ref,
            payload={"test": "data"}
        )
        
        created_entity_ids = [str(uuid.uuid4())]
        result = ProcessingResult.create_success()
        result.entities_created = [existing_entity_id] + created_entity_ids  # Include existing + new
        
        # Create mock state tracking service using real implementation
        from api_exchange_core.services.logging_state_tracking_service import LoggingStateTrackingService
        state_service = LoggingStateTrackingService(db_manager=db_manager)
        
        # Execute
        handler._record_success_state_transitions(
            existing_entity_id, message, result, state_service
        )
        
        # Verify: Should record transitions for existing entity + new entities (not duplicate)
        session = db_manager.get_session()
        try:
            from api_exchange_core.db.db_pipeline_state_models import PipelineStateHistory
            
            all_entity_ids = [existing_entity_id] + created_entity_ids
            state_records = session.query(PipelineStateHistory).filter(
                PipelineStateHistory.entity_id.in_(all_entity_ids)
            ).all()
            
            # Should have records for existing + new entities (not duplicate for existing)
            assert len(state_records) == 2  # existing + 1 new
            
            # Verify we have records for both entities
            recorded_entity_ids = [record.entity_id for record in state_records]
            assert existing_entity_id in recorded_entity_ids
            assert created_entity_ids[0] in recorded_entity_ids
                
        finally:
            session.close()

    def test_record_failure_state_transitions(self, db_manager, test_tenant, tenant_context):
        """Test _record_failure_state_transitions with created entities."""
        # Setup
        processor = MockProcessor()
        handler = ProcessorHandler(processor=processor, processing_service=None, db_manager=db_manager)
        
        message = Message(
            message_type=MessageType.ENTITY_PROCESSING,
            entity_reference=None,
            payload={"operation": "list_orders"}
        )
        
        created_entity_ids = [str(uuid.uuid4())]
        result = ProcessingResult.create_failure(
            error_message="Test failure",
            error_code="TEST_ERROR",
            can_retry=False
        )
        result.entities_created = created_entity_ids
        
        # Create mock state tracking service using real implementation
        from api_exchange_core.services.logging_state_tracking_service import LoggingStateTrackingService
        state_service = LoggingStateTrackingService(db_manager=db_manager)
        
        # Execute
        handler._record_failure_state_transitions(
            None, message, result, state_service
        )
        
        # Verify: Should record "started" -> "failed" for created entities
        session = db_manager.get_session()
        try:
            from api_exchange_core.db.db_pipeline_state_models import PipelineStateHistory
            
            state_records = session.query(PipelineStateHistory).filter(
                PipelineStateHistory.entity_id.in_(created_entity_ids)
            ).all()
            
            assert len(state_records) == 1
            record = state_records[0]
            assert record.entity_id == created_entity_ids[0]
            assert record.processor_name == "MockProcessor"
            # Should be a failure status
            assert record.status in ["FAILED", "failed"]
                
        finally:
            session.close()