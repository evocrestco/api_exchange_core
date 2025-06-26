"""Tests for pipeline state history models."""

import uuid
from datetime import datetime, timezone

import pytest

from api_exchange_core.db.db_pipeline_state_models import PipelineStateHistory


class TestPipelineStateHistory:
    """Test cases for PipelineStateHistory model."""

    def test_create_pipeline_state_history(self, db_session):
        """Test creating a pipeline state history record."""
        log_timestamp = datetime.now(timezone.utc)
        
        record = PipelineStateHistory.create(
            tenant_id="test-tenant",
            processor_name="TestProcessor",
            status="STARTED",
            log_timestamp=log_timestamp,
            entity_id="entity-123",
            external_id="external-456",
            result_code="INIT_SUCCESS",
            source_queue="input-queue",
            destination_queue="output-queue",
            processing_duration_ms=150,
            message_payload_hash="abc123hash",
        )
        
        # Verify all fields are set correctly
        assert record.tenant_id == "test-tenant"
        assert record.processor_name == "TestProcessor"
        assert record.status == "STARTED"
        assert record.log_timestamp == log_timestamp
        assert record.entity_id == "entity-123"
        assert record.external_id == "external-456"
        assert record.result_code == "INIT_SUCCESS"
        assert record.source_queue == "input-queue"
        assert record.destination_queue == "output-queue"
        assert record.processing_duration_ms == 150
        assert record.message_payload_hash == "abc123hash"
        
        # Verify auto-generated fields
        assert record.id is not None
        assert len(record.id) == 36  # UUID format
        assert record.created_at is not None
        assert record.updated_at is not None

    def test_create_minimal_pipeline_state_history(self, db_session):
        """Test creating pipeline state history with minimal required fields."""
        log_timestamp = datetime.now(timezone.utc)
        
        record = PipelineStateHistory.create(
            tenant_id="test-tenant",
            processor_name="MinimalProcessor",
            status="COMPLETED",
            log_timestamp=log_timestamp,
        )
        
        # Verify required fields
        assert record.tenant_id == "test-tenant"
        assert record.processor_name == "MinimalProcessor"
        assert record.status == "COMPLETED"
        assert record.log_timestamp == log_timestamp
        
        # Verify optional fields are None
        assert record.entity_id is None
        assert record.external_id is None
        assert record.result_code is None
        assert record.error_message is None
        assert record.source_queue is None
        assert record.destination_queue is None
        assert record.processing_duration_ms is None
        assert record.message_payload_hash is None

    def test_pipeline_state_history_persistence(self, db_session):
        """Test saving and retrieving pipeline state history."""
        log_timestamp = datetime.now(timezone.utc)
        
        record = PipelineStateHistory.create(
            tenant_id="test-tenant",
            processor_name="PersistenceProcessor",
            status="FAILED",
            log_timestamp=log_timestamp,
            error_message="Test error message",
        )
        
        # Save to database
        db_session.add(record)
        db_session.commit()
        
        # Retrieve from database
        retrieved = (
            db_session.query(PipelineStateHistory)
            .filter(PipelineStateHistory.processor_name == "PersistenceProcessor")
            .first()
        )
        
        assert retrieved is not None
        assert retrieved.id == record.id
        assert retrieved.tenant_id == "test-tenant"
        assert retrieved.processor_name == "PersistenceProcessor"
        assert retrieved.status == "FAILED"
        assert retrieved.error_message == "Test error message"

    def test_pipeline_state_history_indexes(self, db_session):
        """Test that indexes work correctly for common queries."""
        log_timestamp = datetime.now(timezone.utc)
        
        # Create multiple records for testing
        records = []
        for i in range(5):
            record = PipelineStateHistory.create(
                tenant_id="test-tenant",
                processor_name=f"Processor{i}",
                status="COMPLETED" if i % 2 == 0 else "FAILED",
                log_timestamp=log_timestamp,
                entity_id=f"entity-{i}",
            )
            records.append(record)
            db_session.add(record)
        
        db_session.commit()
        
        # Test tenant_id index
        tenant_records = (
            db_session.query(PipelineStateHistory)
            .filter(PipelineStateHistory.tenant_id == "test-tenant")
            .all()
        )
        assert len(tenant_records) == 5
        
        # Test processor_name index
        processor_record = (
            db_session.query(PipelineStateHistory)
            .filter(PipelineStateHistory.processor_name == "Processor2")
            .first()
        )
        assert processor_record is not None
        assert processor_record.processor_name == "Processor2"
        
        # Test status index
        failed_records = (
            db_session.query(PipelineStateHistory)
            .filter(PipelineStateHistory.status == "FAILED")
            .all()
        )
        assert len(failed_records) == 2  # Records 1 and 3
        
        # Test composite index (tenant_id, processor_name)
        composite_record = (
            db_session.query(PipelineStateHistory)
            .filter(
                PipelineStateHistory.tenant_id == "test-tenant",
                PipelineStateHistory.processor_name == "Processor4"
            )
            .first()
        )
        assert composite_record is not None
        assert composite_record.processor_name == "Processor4"

    def test_pipeline_state_history_repr(self, db_session):
        """Test string representation of PipelineStateHistory."""
        record = PipelineStateHistory.create(
            tenant_id="test-tenant",
            processor_name="ReprProcessor",
            status="STARTED",
            log_timestamp=datetime.now(timezone.utc),
            entity_id="entity-repr",
        )
        
        repr_str = repr(record)
        assert "PipelineStateHistory" in repr_str
        assert "ReprProcessor" in repr_str
        assert "STARTED" in repr_str
        assert "entity-repr" in repr_str

    def test_pipeline_state_history_long_fields(self, db_session):
        """Test pipeline state history with maximum length fields."""
        long_error_message = "A" * 1000  # Long error message
        
        record = PipelineStateHistory.create(
            tenant_id="test-tenant",
            processor_name="LongFieldProcessor",
            status="FAILED",
            log_timestamp=datetime.now(timezone.utc),
            error_message=long_error_message,
        )
        
        db_session.add(record)
        db_session.commit()
        
        # Verify long text field is stored correctly
        retrieved = (
            db_session.query(PipelineStateHistory)
            .filter(PipelineStateHistory.processor_name == "LongFieldProcessor")
            .first()
        )
        
        assert retrieved.error_message == long_error_message

    def test_pipeline_state_history_unique_id(self, db_session):
        """Test that each record gets a unique ID."""
        log_timestamp = datetime.now(timezone.utc)
        
        record1 = PipelineStateHistory.create(
            tenant_id="test-tenant",
            processor_name="UniqueProcessor1",
            status="STARTED",
            log_timestamp=log_timestamp,
        )
        
        record2 = PipelineStateHistory.create(
            tenant_id="test-tenant",
            processor_name="UniqueProcessor2",
            status="STARTED",
            log_timestamp=log_timestamp,
        )
        
        assert record1.id != record2.id
        assert len(record1.id) == 36  # UUID format
        assert len(record2.id) == 36  # UUID format