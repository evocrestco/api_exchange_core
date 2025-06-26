"""Tests for LoggingStateTrackingService with database integration."""

import uuid
from datetime import datetime, timezone
from unittest.mock import patch
from contextlib import contextmanager

import pytest

from api_exchange_core.context.tenant_context import tenant_context
from api_exchange_core.db import DatabaseManager, EntityStateEnum, PipelineStateHistory
from api_exchange_core.enums import TransitionTypeEnum
from api_exchange_core.exceptions import set_correlation_id, clear_correlation_id
from api_exchange_core.schemas import PipelineStateTransitionCreate
from api_exchange_core.services.logging_state_tracking_service import LoggingStateTrackingService


@contextmanager
def correlation_context(correlation_id: str):
    """Context manager for setting correlation ID in tests."""
    set_correlation_id(correlation_id)
    try:
        yield
    finally:
        clear_correlation_id()


class TestLoggingStateTrackingService:
    """Test cases for LoggingStateTrackingService with database integration."""

    def test_service_initialization_without_db_manager(self):
        """Test service can be initialized without database manager (backward compatibility)."""
        service = LoggingStateTrackingService()
        
        assert service.logger is not None
        assert service.db_manager is None

    def test_service_initialization_with_db_manager(self, db_manager):
        """Test service initialization with database manager."""
        service = LoggingStateTrackingService(db_manager=db_manager)
        
        assert service.logger is not None
        assert service.db_manager is db_manager

    def test_record_transition_logs_only_without_db_manager(self, caplog, tenant_context_fixture):
        """Test that without db_manager, only logging occurs (backward compatibility)."""
        service = LoggingStateTrackingService()
        correlation_id = str(uuid.uuid4())
        
        transition_data = PipelineStateTransitionCreate(
            entity_id="entity-123",
            from_state="RECEIVED",
            to_state="PROCESSING",
            actor="TestProcessor",
            transition_type="NORMAL",
            external_id="external-456",
            queue_source="input-queue",
            queue_destination="output-queue",
            transition_duration=150,
            notes="Test transition",
        )
        
        with tenant_context("test-tenant"):
            with correlation_context(correlation_id):
                result = service.record_transition(transition_data)
        
        # Verify result is properly typed
        assert result is not None
        assert hasattr(result, 'transition_id')
        assert len(result.transition_id) == 36  # UUID format
        assert result.entity_id == "entity-123"
        assert result.from_state == "RECEIVED"
        assert result.to_state == "PROCESSING"
        assert result.actor == "TestProcessor"
        
        # Verify log message was created
        assert "State transition: RECEIVED → PROCESSING" in caplog.text
        
        # Verify structured logging data
        log_record = None
        for record in caplog.records:
            if "State transition" in record.message:
                log_record = record
                break
        
        assert log_record is not None
        assert hasattr(log_record, 'transition_id')
        assert log_record.transition_id == result.transition_id
        assert log_record.entity_id == "entity-123"
        assert log_record.external_id == "external-456"
        assert log_record.tenant_id == "test-tenant"
        # correlation_id is None and gets filtered out of log data, so no attribute exists
        assert not hasattr(log_record, 'correlation_id')
        assert log_record.from_state == "RECEIVED"
        assert log_record.to_state == "PROCESSING"
        assert log_record.actor == "TestProcessor"
        assert log_record.transition_type == "NORMAL"

    def test_record_transition_with_db_manager_writes_to_database(self, db_manager, db_session, caplog, tenant_context_fixture):
        """Test that with db_manager, both logging and database write occur."""
        service = LoggingStateTrackingService(db_manager=db_manager)
        correlation_id = str(uuid.uuid4())
        
        transition_data = PipelineStateTransitionCreate(
            entity_id="entity-123",
            from_state="RECEIVED",
            to_state="PROCESSING",
            actor="TestProcessor",
            transition_type="NORMAL",
            external_id="external-456",
            queue_source="input-queue",
            queue_destination="output-queue",
            transition_duration=150,
            notes="Test transition",
        )
        
        with tenant_context("test-tenant"):
            with correlation_context(correlation_id):
                result = service.record_transition(transition_data)
        
        # Verify result is properly typed
        assert result is not None
        assert result.transition_id is not None
        
        # Verify log message was created
        assert "State transition: RECEIVED → PROCESSING" in caplog.text
        
        # Verify database record was created
        db_record = (
            db_session.query(PipelineStateHistory)
            .filter(PipelineStateHistory.id == result.transition_id)
            .first()
        )
        
        assert db_record is not None
        assert db_record.tenant_id == "test-tenant"
        # correlation_id column removed - no longer checking
        assert db_record.entity_id == "entity-123"
        assert db_record.external_id == "external-456"
        assert db_record.processor_name == "TestProcessor"
        assert db_record.status == "PROCESSING"  # Maps to to_state
        assert db_record.source_queue == "input-queue"
        assert db_record.destination_queue == "output-queue"
        assert db_record.processing_duration_ms == 150
        assert db_record.error_message is None  # Not a failed transition

    def test_record_transition_error_state_mapping(self, db_manager, db_session, tenant_context_fixture):
        """Test that ERROR transition types map to FAILED status."""
        service = LoggingStateTrackingService(db_manager=db_manager)
        correlation_id = str(uuid.uuid4())
        
        transition_data = PipelineStateTransitionCreate(
            entity_id="entity-123",
            from_state=EntityStateEnum.PROCESSING.value,
            to_state=EntityStateEnum.VALIDATION_ERROR.value,
            actor="ValidationProcessor",
            transition_type=TransitionTypeEnum.ERROR.value,
            notes="Validation failed: missing required field",
        )
        
        with tenant_context("test-tenant"):
            with correlation_context(correlation_id):
                result = service.record_transition(transition_data)
                transition_id = result.transition_id
        
        # Verify database record has correct status mapping
        db_record = (
            db_session.query(PipelineStateHistory)
            .filter(PipelineStateHistory.id == transition_id)
            .first()
        )
        
        assert db_record is not None
        assert db_record.status == "FAILED"  # ERROR transition_type maps to FAILED
        assert db_record.error_message == "Validation failed: missing required field"

    def test_record_transition_retry_state_mapping(self, db_manager, db_session, tenant_context_fixture):
        """Test that RETRY transition types map to RETRYING status."""
        service = LoggingStateTrackingService(db_manager=db_manager)
        correlation_id = str(uuid.uuid4())
        
        transition_data = PipelineStateTransitionCreate(
            entity_id="entity-123",
            from_state=EntityStateEnum.DELIVERY_ERROR.value,
            to_state=EntityStateEnum.PROCESSING.value,
            actor="RetryProcessor",
            transition_type=TransitionTypeEnum.RETRY.value,
            notes="Retrying after temporary failure",
        )
        
        with tenant_context("test-tenant"):
            with correlation_context(correlation_id):
                result = service.record_transition(transition_data)
                transition_id = result.transition_id
        
        # Verify database record has correct status mapping
        db_record = (
            db_session.query(PipelineStateHistory)
            .filter(PipelineStateHistory.id == transition_id)
            .first()
        )
        
        assert db_record is not None
        assert db_record.status == "RETRYING"  # RETRY transition_type maps to RETRYING
        assert db_record.error_message is None  # Not an error, just a retry

    def test_record_transition_with_string_states(self, db_manager, db_session, tenant_context_fixture):
        """Test recording transition with string states instead of enums."""
        service = LoggingStateTrackingService(db_manager=db_manager)
        correlation_id = str(uuid.uuid4())
        
        transition_data = PipelineStateTransitionCreate(
            entity_id="entity-123",
            from_state="STARTED",
            to_state="COMPLETED",
            actor="CustomProcessor",
            transition_type="NORMAL",
        )
        
        with tenant_context("test-tenant"):
            with correlation_context(correlation_id):
                result = service.record_transition(transition_data)
                transition_id = result.transition_id
        
        # Verify database record handles string states
        db_record = (
            db_session.query(PipelineStateHistory)
            .filter(PipelineStateHistory.id == transition_id)
            .first()
        )
        
        assert db_record is not None
        assert db_record.status == "COMPLETED"
        assert db_record.processor_name == "CustomProcessor"

    def test_record_transition_minimal_parameters(self, db_manager, db_session, tenant_context_fixture):
        """Test recording transition with only required parameters."""
        service = LoggingStateTrackingService(db_manager=db_manager)
        correlation_id = str(uuid.uuid4())
        
        transition_data = PipelineStateTransitionCreate(
            entity_id="entity-minimal",
            from_state="INIT",
            to_state="STARTED",
            actor="MinimalProcessor",
        )
        
        with tenant_context("test-tenant"):
            with correlation_context(correlation_id):
                result = service.record_transition(transition_data)
                transition_id = result.transition_id
        
        # Verify database record with minimal data
        db_record = (
            db_session.query(PipelineStateHistory)
            .filter(PipelineStateHistory.id == transition_id)
            .first()
        )
        
        assert db_record is not None
        assert db_record.entity_id == "entity-minimal"
        assert db_record.processor_name == "MinimalProcessor"
        assert db_record.status == "STARTED"
        assert db_record.external_id is None
        assert db_record.source_queue is None
        assert db_record.destination_queue is None
        assert db_record.processing_duration_ms is None
        assert db_record.error_message is None

    def test_record_transition_database_failure_continues_logging(self, db_manager, caplog, tenant_context_fixture):
        """Test that database failures don't interrupt logging (reliability)."""
        service = LoggingStateTrackingService(db_manager=db_manager)
        correlation_id = str(uuid.uuid4())
        
        transition_data = PipelineStateTransitionCreate(
            entity_id="entity-db-fail",
            from_state="STARTED",
            to_state="PROCESSING",
            actor="FailureTestProcessor",
        )
        
        # Mock database failure by making db_manager.get_session() raise an exception
        with patch.object(db_manager, 'get_session', side_effect=Exception("Database connection failed")):
            with tenant_context("test-tenant"):
                with correlation_context(correlation_id):
                    result = service.record_transition(transition_data)
                    transition_id = result.transition_id
        
        # Verify transition ID is still returned
        assert transition_id is not None
        
        # Verify log message was still created
        assert "State transition: STARTED → PROCESSING" in caplog.text
        
        # Verify warning about database failure was logged
        assert "Failed to write state to database" in caplog.text


    def test_record_transition_removes_none_values_from_log_data(self, caplog, tenant_context_fixture):
        """Test that None values are removed from log data to keep logs clean."""
        service = LoggingStateTrackingService()
        correlation_id = str(uuid.uuid4())
        
        transition_data = PipelineStateTransitionCreate(
            entity_id="entity-clean-logs",
            from_state="STARTED",
            to_state="PROCESSING",
            actor="CleanLogProcessor",
            # Explicitly passing None values
            external_id=None,
            queue_source=None,
            queue_destination=None,
            notes=None,
            transition_duration=None,
            processor_data=None,
        )
        
        with tenant_context("test-tenant"):
            with correlation_context(correlation_id):
                service.record_transition(transition_data)
        
        # Verify log record doesn't contain None values
        log_record = None
        for record in caplog.records:
            if "State transition" in record.message:
                log_record = record
                break
        
        assert log_record is not None
        
        # Verify None fields are not present in log data
        log_dict = log_record.__dict__
        assert 'external_id' not in log_dict
        assert 'queue_source' not in log_dict
        assert 'queue_destination' not in log_dict
        assert 'notes' not in log_dict
        assert 'transition_duration_ms' not in log_dict
        assert 'processor_data' not in log_dict
        
        # Verify required fields are present
        assert log_dict['entity_id'] == "entity-clean-logs"
        assert log_dict['actor'] == "CleanLogProcessor"

    def test_backward_compatibility_methods_return_expected_values(self):
        """Test that backward compatibility methods work as expected."""
        service = LoggingStateTrackingService()
        
        # Test get_entity_state_history
        result = service.get_entity_state_history("entity-123")
        assert result is None
        
        # Test get_current_state
        result = service.get_current_state("entity-123")
        assert result is None
        
        # Test get_entities_in_state
        result = service.get_entities_in_state("PROCESSING")
        assert result == []
        
        # Test get_stuck_entities
        result = service.get_stuck_entities("PROCESSING", 60)
        assert result == []
        
        # Test get_state_statistics
        result = service.get_state_statistics()
        assert result is None
        
        # Test calculate_avg_processing_time
        result = service.calculate_avg_processing_time("STARTED", "COMPLETED")
        assert result is None

    def test_update_message_with_state_preserves_data(self):
        """Test that update_message_with_state works correctly."""
        service = LoggingStateTrackingService()
        
        original_message = {
            "entity_id": "entity-123",
            "payload": {"data": "test"},
            "metadata": {"version": "1.0"}
        }
        
        updated_message = service.update_message_with_state(original_message, "PROCESSING")
        
        # Verify original message is not modified
        assert "state" not in original_message
        assert "current_state" not in original_message.get("metadata", {})
        
        # Verify updated message has state information
        assert updated_message["state"] == "PROCESSING"
        assert updated_message["metadata"]["current_state"] == "PROCESSING"
        assert "state_updated_at" in updated_message["metadata"]
        assert "state_changed_at" in updated_message["metadata"]
        
        # Verify original data is preserved
        assert updated_message["entity_id"] == "entity-123"
        assert updated_message["payload"] == {"data": "test"}
        assert updated_message["metadata"]["version"] == "1.0"