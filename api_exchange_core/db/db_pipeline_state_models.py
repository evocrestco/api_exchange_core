"""
Pipeline state tracking models for monitoring and debugging pipeline flows.

This module provides models for tracking pipeline state transitions
projected from log entries, enabling GUI monitoring without circular dependencies.
"""

import uuid
from typing import Optional

from sqlalchemy import Column, DateTime, Index, Integer, String, Text

from .db_base import Base, utc_now


class PipelineStateHistory(Base):
    """
    Pipeline state history tracking for monitoring and debugging.

    This table is populated by projecting log entries from the logs-queue,
    avoiding circular dependencies while enabling queryable state tracking.
    """

    __tablename__ = "pipeline_state_history"

    # Primary key and timestamps
    id = Column(String(36), primary_key=True)
    created_at = Column(DateTime, nullable=False, default=utc_now)
    updated_at = Column(DateTime, nullable=False, default=utc_now, onupdate=utc_now)

    # Core tracking fields
    tenant_id = Column(String(100), nullable=False, index=True)
    entity_id = Column(String(36), nullable=True, index=True)
    external_id = Column(String(100), nullable=True)

    # Processing context
    processor_name = Column(String(100), nullable=False, index=True)
    status = Column(String(20), nullable=False, index=True)  # STARTED, COMPLETED, FAILED, RETRYING
    result_code = Column(String(50), nullable=True)  # Success codes or error codes
    error_message = Column(Text, nullable=True)

    # Queue routing
    source_queue = Column(String(100), nullable=True)
    destination_queue = Column(String(100), nullable=True)

    # Performance tracking
    processing_duration_ms = Column(Integer, nullable=True)
    message_payload_hash = Column(String(64), nullable=True)  # For duplicate detection

    # Timestamps
    log_timestamp = Column(DateTime, nullable=False, index=True)

    # Composite indexes for common query patterns
    __table_args__ = (
        Index("ix_pipeline_state_entity_timeline", "entity_id", "log_timestamp"),
        Index("ix_pipeline_state_processor_status", "processor_name", "status"),
        Index("ix_pipeline_state_timeline", "log_timestamp"),
        Index("ix_pipeline_state_tenant_timeline", "tenant_id", "log_timestamp"),
    )

    @classmethod
    def create(
        cls,
        tenant_id: str,
        processor_name: str,
        status: str,
        log_timestamp,
        entity_id: Optional[str] = None,
        external_id: Optional[str] = None,
        result_code: Optional[str] = None,
        error_message: Optional[str] = None,
        source_queue: Optional[str] = None,
        destination_queue: Optional[str] = None,
        processing_duration_ms: Optional[int] = None,
        message_payload_hash: Optional[str] = None,
    ) -> "PipelineStateHistory":
        """Factory method to create a new PipelineStateHistory record."""
        return cls(
            id=str(uuid.uuid4()),
            tenant_id=tenant_id,
            entity_id=entity_id,
            external_id=external_id,
            processor_name=processor_name,
            status=status,
            result_code=result_code,
            error_message=error_message,
            source_queue=source_queue,
            destination_queue=destination_queue,
            processing_duration_ms=processing_duration_ms,
            message_payload_hash=message_payload_hash,
            log_timestamp=log_timestamp,
            created_at=utc_now(),
            updated_at=utc_now(),
        )

    def __repr__(self) -> str:
        return (
            f"<PipelineStateHistory("
            f"entity_id={self.entity_id}, "
            f"processor={self.processor_name}, "
            f"status={self.status}, "
            f"external_id={self.external_id})>"
        )
