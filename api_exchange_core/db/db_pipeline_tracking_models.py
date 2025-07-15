"""
Simple pipeline tracking models for V2 framework.

Tracks pipeline execution for observability - what happened at each processor,
timing, inputs/outputs, errors, etc. Just data models, no business logic.
"""

from sqlalchemy import Boolean, Column, DateTime, Index, Integer, String

from .db_base import JSON, TimestampMixin, UUIDMixin
from .db_config import Base


class PipelineExecution(Base, UUIDMixin, TimestampMixin):
    """Track overall pipeline execution from trigger to completion."""

    __tablename__ = "pipeline_execution"

    # Core tracking
    pipeline_id = Column(String(36), nullable=False, index=True)  # From Message.pipeline_id
    tenant_id = Column(String(100), nullable=False, index=True)
    correlation_id = Column(String(36), nullable=True, index=True)  # For tracing

    # Execution state
    status = Column(String(20), nullable=False, default="started")  # started, completed, failed
    started_at = Column(DateTime(timezone=True), nullable=False)
    completed_at = Column(DateTime(timezone=True), nullable=True)
    duration_ms = Column(Integer, nullable=True)  # Total execution time

    # Trigger information
    trigger_type = Column(String(50), nullable=False)  # queue, timer, webhook, http
    trigger_source = Column(String(200), nullable=True)  # queue name, endpoint, etc

    # Execution metrics
    step_count = Column(Integer, nullable=False, default=0)  # Number of processors executed
    message_count = Column(Integer, nullable=False, default=1)  # Messages processed
    error_count = Column(Integer, nullable=False, default=0)  # Errors encountered

    # Error information (if failed)
    error_message = Column(String(500), nullable=True)
    error_step = Column(String(100), nullable=True)  # Which step failed

    # Additional context
    context = Column(JSON, nullable=True)  # Any additional metadata

    # Indexes for common queries
    __table_args__ = (
        Index("ix_pipeline_exec_lookup", "tenant_id", "pipeline_id", "status"),
        Index("ix_pipeline_exec_time", "started_at", "completed_at"),
        Index("ix_pipeline_exec_correlation", "correlation_id"),
    )


class PipelineStep(Base, UUIDMixin, TimestampMixin):
    """Track individual processor execution within a pipeline."""

    __tablename__ = "pipeline_step"

    # Links to pipeline execution
    execution_id = Column(String(36), nullable=False, index=True)  # FK to PipelineExecution
    pipeline_id = Column(String(36), nullable=False)  # Same as parent execution
    tenant_id = Column(String(100), nullable=False)

    # Step identification
    step_name = Column(String(100), nullable=False)  # e.g., "process_orders"
    processor_name = Column(String(100), nullable=False)  # e.g., "OrderProcessor"
    function_name = Column(String(100), nullable=True)  # Azure Function name

    # Message being processed
    message_id = Column(String(36), nullable=False)  # Message.message_id
    correlation_id = Column(String(36), nullable=True)  # Message.correlation_id

    # Execution timing
    started_at = Column(DateTime, nullable=False)
    completed_at = Column(DateTime, nullable=True)
    duration_ms = Column(Integer, nullable=True)

    # Execution status
    status = Column(String(20), nullable=False, default="processing")  # processing, completed, failed

    # Results
    output_count = Column(Integer, nullable=False, default=0)  # Number of output messages
    output_queues = Column(JSON, nullable=True)  # List of queues messages were sent to

    # Error information (if failed)
    error_message = Column(String(500), nullable=True)
    error_type = Column(String(50), nullable=True)  # validation, connection, etc

    # Additional context
    context = Column(JSON, nullable=True)  # Step-specific metadata

    # Indexes for queries
    __table_args__ = (
        Index("ix_pipeline_step_execution", "execution_id", "started_at"),
        Index("ix_pipeline_step_lookup", "tenant_id", "pipeline_id", "step_name"),
        Index("ix_pipeline_step_message", "message_id"),
    )


class PipelineMessage(Base, UUIDMixin, TimestampMixin):
    """Optionally capture input/output messages for debugging (security sensitive)."""

    __tablename__ = "pipeline_message"

    # Links to step
    step_id = Column(String(36), nullable=False, index=True)  # FK to PipelineStep
    execution_id = Column(String(36), nullable=False)  # FK to PipelineExecution
    tenant_id = Column(String(100), nullable=False)

    # Message info
    message_id = Column(String(36), nullable=False)
    message_type = Column(String(10), nullable=False)  # input, output

    # Message content (optional - may be disabled for security)
    message_payload = Column(JSON, nullable=True)  # Actual message data
    message_size_bytes = Column(Integer, nullable=True)  # Size tracking

    # Message routing
    source_queue = Column(String(100), nullable=True)  # Where it came from
    target_queue = Column(String(100), nullable=True)  # Where it's going

    # Sanitization info (if message was sanitized for storage)
    is_sanitized = Column(Boolean, nullable=False, default=False)
    sanitization_rules = Column(JSON, nullable=True)  # What was removed/masked

    # Additional context
    context = Column(JSON, nullable=True)

    # Indexes
    __table_args__ = (
        Index("ix_pipeline_msg_step", "step_id", "message_type"),
        Index("ix_pipeline_msg_lookup", "tenant_id", "execution_id"),
    )
