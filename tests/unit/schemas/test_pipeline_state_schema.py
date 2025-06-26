"""Tests for pipeline state Pydantic schemas and validators."""

from datetime import datetime

import pytest

from api_exchange_core.exceptions import ValidationError
from api_exchange_core.schemas.pipeline_state_schema import (
    PipelineStateHistoryCreate,
    StateTransitionCreate,
    StatusSummaryRead,
)


class TestPipelineStateHistoryCreate:
    """Test cases for PipelineStateHistoryCreate schema validation."""

    def test_valid_schema_creation(self):
        """Test creating schema with valid data."""
        data = PipelineStateHistoryCreate(
            tenant_id="test-tenant",
            correlation_id="test-correlation-id",
            processor_name="TestProcessor",
            status="STARTED",
            log_timestamp=datetime.now(),
            entity_id="entity-123",
            processing_duration_ms=100,
        )
        
        assert data.tenant_id == "test-tenant"
        assert data.status == "STARTED"
        assert data.processing_duration_ms == 100

    def test_status_validation_valid_values(self):
        """Test that valid status values are accepted."""
        valid_statuses = ["STARTED", "COMPLETED", "FAILED", "RETRYING", "PROCESSING"]
        
        for status in valid_statuses:
            data = PipelineStateHistoryCreate(
                tenant_id="test-tenant",
                correlation_id="test-correlation-id",
                processor_name="TestProcessor",
                status=status,
                log_timestamp=datetime.now(),
            )
            assert data.status == status

    def test_status_validation_invalid_value(self):
        """Test that invalid status values raise ValidationError."""
        with pytest.raises(ValidationError) as exc_info:
            PipelineStateHistoryCreate(
                tenant_id="test-tenant",
                correlation_id="test-correlation-id",
                processor_name="TestProcessor",
                status="INVALID_STATUS",
                log_timestamp=datetime.now(),
            )
        
        error = exc_info.value
        assert "Status must be one of:" in error.message
        assert error.context["field"] == "status"
        assert error.context["value"] == "INVALID_STATUS"

    def test_processing_duration_validation_positive_value(self):
        """Test that positive processing duration is accepted."""
        data = PipelineStateHistoryCreate(
            tenant_id="test-tenant",
            correlation_id="test-correlation-id",
            processor_name="TestProcessor",
            status="STARTED",
            log_timestamp=datetime.now(),
            processing_duration_ms=500,
        )
        assert data.processing_duration_ms == 500

    def test_processing_duration_validation_zero_value(self):
        """Test that zero processing duration is accepted."""
        data = PipelineStateHistoryCreate(
            tenant_id="test-tenant",
            correlation_id="test-correlation-id",
            processor_name="TestProcessor",
            status="STARTED",
            log_timestamp=datetime.now(),
            processing_duration_ms=0,
        )
        assert data.processing_duration_ms == 0

    def test_processing_duration_validation_none_value(self):
        """Test that None processing duration is accepted."""
        data = PipelineStateHistoryCreate(
            tenant_id="test-tenant",
            correlation_id="test-correlation-id",
            processor_name="TestProcessor",
            status="STARTED",
            log_timestamp=datetime.now(),
            processing_duration_ms=None,
        )
        assert data.processing_duration_ms is None

    def test_processing_duration_validation_negative_value(self):
        """Test that negative processing duration raises ValidationError."""
        with pytest.raises(ValidationError) as exc_info:
            PipelineStateHistoryCreate(
                tenant_id="test-tenant",
                correlation_id="test-correlation-id",
                processor_name="TestProcessor",
                status="STARTED",
                log_timestamp=datetime.now(),
                processing_duration_ms=-100,
            )
        
        error = exc_info.value
        assert "Processing duration must be non-negative" in error.message
        assert error.context["field"] == "processing_duration_ms"
        assert error.context["value"] == -100


class TestStateTransitionCreate:
    """Test cases for StateTransitionCreate schema validation."""

    def test_valid_schema_creation(self):
        """Test creating schema with valid data."""
        data = StateTransitionCreate(
            entity_id="entity-123",
            from_state="RECEIVED",
            to_state="PROCESSING",
            actor="TestProcessor",
            transition_type="NORMAL",
            transition_duration=150,
        )
        
        assert data.entity_id == "entity-123"
        assert data.transition_type == "NORMAL"
        assert data.transition_duration == 150

    def test_transition_type_validation_valid_values(self):
        """Test that valid transition type values are accepted."""
        valid_types = ["NORMAL", "ERROR", "RECOVERY", "MANUAL", "TIMEOUT", "RETRY"]
        
        for transition_type in valid_types:
            data = StateTransitionCreate(
                entity_id="entity-123",
                from_state="RECEIVED",
                to_state="PROCESSING",
                actor="TestProcessor",
                transition_type=transition_type,
            )
            assert data.transition_type == transition_type

    def test_transition_type_validation_invalid_value(self):
        """Test that invalid transition type values raise ValidationError."""
        with pytest.raises(ValidationError) as exc_info:
            StateTransitionCreate(
                entity_id="entity-123",
                from_state="RECEIVED",
                to_state="PROCESSING",
                actor="TestProcessor",
                transition_type="INVALID_TYPE",
            )
        
        error = exc_info.value
        assert "Transition type must be one of:" in error.message
        assert error.context["field"] == "transition_type"
        assert error.context["value"] == "INVALID_TYPE"

    def test_transition_duration_validation_positive_value(self):
        """Test that positive transition duration is accepted."""
        data = StateTransitionCreate(
            entity_id="entity-123",
            from_state="RECEIVED",
            to_state="PROCESSING",
            actor="TestProcessor",
            transition_duration=250,
        )
        assert data.transition_duration == 250

    def test_transition_duration_validation_zero_value(self):
        """Test that zero transition duration is accepted."""
        data = StateTransitionCreate(
            entity_id="entity-123",
            from_state="RECEIVED",
            to_state="PROCESSING",
            actor="TestProcessor",
            transition_duration=0,
        )
        assert data.transition_duration == 0

    def test_transition_duration_validation_none_value(self):
        """Test that None transition duration is accepted."""
        data = StateTransitionCreate(
            entity_id="entity-123",
            from_state="RECEIVED",
            to_state="PROCESSING",
            actor="TestProcessor",
            transition_duration=None,
        )
        assert data.transition_duration is None

    def test_transition_duration_validation_negative_value(self):
        """Test that negative transition duration raises ValidationError."""
        with pytest.raises(ValidationError) as exc_info:
            StateTransitionCreate(
                entity_id="entity-123",
                from_state="RECEIVED",
                to_state="PROCESSING",
                actor="TestProcessor",
                transition_duration=-50,
            )
        
        error = exc_info.value
        assert "Transition duration must be non-negative" in error.message
        assert error.context["field"] == "transition_duration"
        assert error.context["value"] == -50

    def test_default_transition_type(self):
        """Test that default transition type is NORMAL."""
        data = StateTransitionCreate(
            entity_id="entity-123",
            from_state="RECEIVED",
            to_state="PROCESSING",
            actor="TestProcessor",
        )
        assert data.transition_type == "NORMAL"


class TestStatusSummaryRead:
    """Test cases for StatusSummaryRead schema validation."""

    def test_valid_schema_creation(self):
        """Test creating schema with valid data."""
        data = StatusSummaryRead(
            period_hours=24,
            total_processing_events=1000,
            status_breakdown={"COMPLETED": 950, "FAILED": 50},
            success_rate_percentage=95.0,
            health_status="healthy",
            generated_at="2024-01-15T10:30:00Z",
        )
        
        assert data.period_hours == 24
        assert data.success_rate_percentage == 95.0
        assert data.health_status == "healthy"

    def test_health_status_validation_valid_values(self):
        """Test that valid health status values are accepted."""
        valid_statuses = ["healthy", "degraded", "unhealthy"]
        
        for health_status in valid_statuses:
            data = StatusSummaryRead(
                period_hours=24,
                total_processing_events=1000,
                status_breakdown={"COMPLETED": 1000},
                success_rate_percentage=100.0,
                health_status=health_status,
                generated_at="2024-01-15T10:30:00Z",
            )
            assert data.health_status == health_status

    def test_health_status_validation_invalid_value(self):
        """Test that invalid health status values raise ValidationError."""
        with pytest.raises(ValidationError) as exc_info:
            StatusSummaryRead(
                period_hours=24,
                total_processing_events=1000,
                status_breakdown={"COMPLETED": 1000},
                success_rate_percentage=100.0,
                health_status="invalid_health",
                generated_at="2024-01-15T10:30:00Z",
            )
        
        error = exc_info.value
        assert "Health status must be one of:" in error.message
        assert error.context["field"] == "health_status"
        assert error.context["value"] == "invalid_health"

    def test_success_rate_validation_valid_values(self):
        """Test that valid success rate values are accepted."""
        valid_rates = [0.0, 50.0, 99.99, 100.0]
        
        for rate in valid_rates:
            data = StatusSummaryRead(
                period_hours=24,
                total_processing_events=1000,
                status_breakdown={"COMPLETED": 1000},
                success_rate_percentage=rate,
                health_status="healthy",
                generated_at="2024-01-15T10:30:00Z",
            )
            assert data.success_rate_percentage == rate

    def test_success_rate_validation_negative_value(self):
        """Test that negative success rate raises ValidationError."""
        with pytest.raises(ValidationError) as exc_info:
            StatusSummaryRead(
                period_hours=24,
                total_processing_events=1000,
                status_breakdown={"FAILED": 1000},
                success_rate_percentage=-5.0,
                health_status="unhealthy",
                generated_at="2024-01-15T10:30:00Z",
            )
        
        error = exc_info.value
        assert "Success rate must be between 0 and 100" in error.message
        assert error.context["field"] == "success_rate_percentage"
        assert error.context["value"] == -5.0

    def test_success_rate_validation_over_100_value(self):
        """Test that success rate over 100 raises ValidationError."""
        with pytest.raises(ValidationError) as exc_info:
            StatusSummaryRead(
                period_hours=24,
                total_processing_events=1000,
                status_breakdown={"COMPLETED": 1000},
                success_rate_percentage=105.0,
                health_status="healthy",
                generated_at="2024-01-15T10:30:00Z",
            )
        
        error = exc_info.value
        assert "Success rate must be between 0 and 100" in error.message
        assert error.context["field"] == "success_rate_percentage"
        assert error.context["value"] == 105.0