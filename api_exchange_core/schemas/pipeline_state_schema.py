"""
Pydantic schemas for pipeline state tracking and monitoring.

These schemas provide input validation and output serialization for
pipeline state history and monitoring functionality.
"""

from datetime import datetime
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, ConfigDict, Field, field_validator

from ..exceptions import ValidationError


class PipelineStateHistoryCreate(BaseModel):
    """Schema for creating pipeline state history records."""

    tenant_id: str = Field(..., description="Tenant identifier")
    processor_name: str = Field(..., description="Name of the processor")
    status: str = Field(..., description="Processing status (STARTED, COMPLETED, FAILED, etc.)")
    log_timestamp: datetime = Field(..., description="Timestamp when the state was recorded")
    entity_id: Optional[str] = Field(None, description="Entity identifier if applicable")
    external_id: Optional[str] = Field(None, description="External identifier")
    result_code: Optional[str] = Field(None, description="Result code from processing")
    error_message: Optional[str] = Field(None, description="Error message if failed")
    source_queue: Optional[str] = Field(None, description="Source queue name")
    destination_queue: Optional[str] = Field(None, description="Destination queue name")
    processing_duration_ms: Optional[int] = Field(
        None, description="Processing duration in milliseconds"
    )
    message_payload_hash: Optional[str] = Field(None, description="Hash of the message payload")

    @field_validator("status")
    @classmethod
    def validate_status(cls, v):
        """Validate status values."""
        allowed_statuses = {"STARTED", "COMPLETED", "FAILED", "RETRYING", "PROCESSING"}
        if v not in allowed_statuses:
            raise ValidationError(f"Status must be one of: {allowed_statuses}", field="status", value=v)
        return v

    @field_validator("processing_duration_ms")
    @classmethod
    def validate_duration(cls, v):
        """Validate processing duration is non-negative."""
        if v is not None and v < 0:
            raise ValidationError("Processing duration must be non-negative", field="processing_duration_ms", value=v)
        return v


class PipelineStateHistoryRead(BaseModel):
    """Schema for reading pipeline state history records."""

    id: str = Field(..., description="Unique identifier for the state record")
    created_at: datetime = Field(..., description="Record creation timestamp")
    updated_at: datetime = Field(..., description="Record last update timestamp")
    tenant_id: str = Field(..., description="Tenant identifier")
    processor_name: str = Field(..., description="Name of the processor")
    status: str = Field(..., description="Processing status")
    log_timestamp: datetime = Field(..., description="Timestamp when the state was recorded")
    entity_id: Optional[str] = Field(None, description="Entity identifier if applicable")
    external_id: Optional[str] = Field(None, description="External identifier")
    result_code: Optional[str] = Field(None, description="Result code from processing")
    error_message: Optional[str] = Field(None, description="Error message if failed")
    source_queue: Optional[str] = Field(None, description="Source queue name")
    destination_queue: Optional[str] = Field(None, description="Destination queue name")
    processing_duration_ms: Optional[int] = Field(
        None, description="Processing duration in milliseconds"
    )
    message_payload_hash: Optional[str] = Field(None, description="Hash of the message payload")

    model_config = ConfigDict(from_attributes=True)


class PipelineStateQuery(BaseModel):
    """Schema for pipeline state query parameters."""

    hours: int = Field(24, ge=1, le=168, description="Number of hours to look back (1-168)")
    limit: int = Field(
        100, ge=1, le=1000, description="Maximum number of records to return (1-1000)"
    )


class EntityQuery(BaseModel):
    """Schema for entity-based queries."""

    entity_id: str = Field(..., description="Entity ID to track")


class ProcessorMetricsRead(BaseModel):
    """Schema for processor performance metrics."""

    processor_name: str = Field(..., description="Name of the processor")
    status: str = Field(..., description="Processing status")
    count: int = Field(..., description="Number of processing events")
    avg_duration_ms: Optional[float] = Field(
        None, description="Average processing duration in milliseconds"
    )
    max_duration_ms: Optional[int] = Field(
        None, description="Maximum processing duration in milliseconds"
    )
    min_duration_ms: Optional[int] = Field(
        None, description="Minimum processing duration in milliseconds"
    )


class StatusSummaryRead(BaseModel):
    """Schema for overall pipeline status summary."""

    period_hours: int = Field(..., description="Time period analyzed in hours")
    total_processing_events: int = Field(..., description="Total number of processing events")
    status_breakdown: Dict[str, int] = Field(..., description="Count of events by status")
    success_rate_percentage: float = Field(..., description="Success rate as percentage")
    health_status: str = Field(
        ..., description="Overall health status (healthy/degraded/unhealthy)"
    )
    generated_at: str = Field(..., description="Timestamp when summary was generated")

    @field_validator("health_status")
    @classmethod
    def validate_health_status(cls, v):
        """Validate health status values."""
        allowed_statuses = {"healthy", "degraded", "unhealthy"}
        if v not in allowed_statuses:
            raise ValidationError(f"Health status must be one of: {allowed_statuses}", field="health_status", value=v)
        return v

    @field_validator("success_rate_percentage")
    @classmethod
    def validate_success_rate(cls, v):
        """Validate success rate is between 0 and 100."""
        if not 0 <= v <= 100:
            raise ValidationError("Success rate must be between 0 and 100", field="success_rate_percentage", value=v)
        return v


class StateTransitionCreate(BaseModel):
    """Schema for creating state transitions (for LoggingStateTrackingService)."""

    entity_id: str = Field(..., description="ID of the entity")
    from_state: str = Field(..., description="Previous state")
    to_state: str = Field(..., description="New state")
    actor: str = Field(..., description="Actor (processor or user) making the transition")
    transition_type: str = Field(
        "NORMAL", description="Type of transition (NORMAL, ERROR, RETRY, etc.)"
    )
    external_id: Optional[str] = Field(None, description="External identifier")
    queue_source: Optional[str] = Field(None, description="Queue from which message was received")
    queue_destination: Optional[str] = Field(None, description="Queue to which message was sent")
    notes: Optional[str] = Field(None, description="Additional notes about the transition")
    transition_duration: Optional[int] = Field(
        None, description="Duration in ms of the previous state"
    )
    processor_data: Optional[Dict[str, Any]] = Field(None, description="Additional processor data")

    @field_validator("transition_type")
    @classmethod
    def validate_transition_type(cls, v):
        """Validate transition type values."""
        allowed_types = {"NORMAL", "ERROR", "RECOVERY", "MANUAL", "TIMEOUT", "RETRY"}
        if v not in allowed_types:
            raise ValidationError(f"Transition type must be one of: {allowed_types}", field="transition_type", value=v)
        return v

    @field_validator("transition_duration")
    @classmethod
    def validate_transition_duration(cls, v):
        """Validate transition duration is non-negative."""
        if v is not None and v < 0:
            raise ValidationError("Transition duration must be non-negative", field="transition_duration", value=v)
        return v


class StateTransitionRead(BaseModel):
    """Schema for reading state transition records."""

    transition_id: str = Field(..., description="Unique identifier for the transition")
    entity_id: str = Field(..., description="ID of the entity")
    tenant_id: str = Field(..., description="Tenant identifier")
    correlation_id: Optional[str] = Field(None, description="Correlation ID")
    from_state: str = Field(..., description="Previous state")
    to_state: str = Field(..., description="New state")
    actor: str = Field(..., description="Actor making the transition")
    transition_type: str = Field(..., description="Type of transition")
    timestamp: datetime = Field(..., description="When the transition occurred")
    external_id: Optional[str] = Field(None, description="External identifier")
    queue_source: Optional[str] = Field(None, description="Source queue")
    queue_destination: Optional[str] = Field(None, description="Destination queue")
    notes: Optional[str] = Field(None, description="Additional notes")
    transition_duration: Optional[int] = Field(None, description="Duration in milliseconds")
    processor_data: Optional[Dict[str, Any]] = Field(None, description="Additional processor data")


# Response containers for multiple records
class PipelineHistoryResponse(BaseModel):
    """Response container for pipeline history queries."""

    records: List[PipelineStateHistoryRead] = Field(
        ..., description="List of pipeline state records"
    )
    total_count: int = Field(..., description="Total number of records found")
    query_parameters: PipelineStateQuery = Field(..., description="Query parameters used")


class ProcessorMetricsResponse(BaseModel):
    """Response container for processor metrics."""

    metrics: List[ProcessorMetricsRead] = Field(..., description="List of processor metrics")
    period_hours: int = Field(..., description="Time period analyzed")
    generated_at: str = Field(..., description="When metrics were generated")
