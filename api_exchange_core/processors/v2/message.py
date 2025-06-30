"""
Message classes v2 for processor pipeline communication.

Simplified message structure for queue transport:
- Uses EntityReference instead of full Entity for lightweight transport
- Removes processing-specific fields (moved to Entity.processing_results)
- Simplified structure works better with Azure Functions queue triggers
"""

from datetime import datetime
from enum import Enum
from typing import Any, Dict, Optional, Union
from uuid import uuid4

from pydantic import BaseModel, Field, field_validator

from ...schemas.entity_schema import EntityReference


class MessageType(str, Enum):
    """Types of messages that can flow through the pipeline."""

    ENTITY_PROCESSING = "entity_processing"
    CONTROL_MESSAGE = "control_message"
    ERROR_MESSAGE = "error_message"
    HEARTBEAT = "heartbeat"
    METRICS = "metrics"


class Message(BaseModel):
    """Lightweight message for queue transport between processors."""

    # Message identification
    message_id: str = Field(default_factory=lambda: str(uuid4()))
    correlation_id: str = Field(default_factory=lambda: str(uuid4()))
    pipeline_id: str = Field(default_factory=lambda: str(uuid4()))
    message_type: MessageType = Field(default=MessageType.ENTITY_PROCESSING)

    # Entity reference (not full entity) - for lightweight transport
    entity_reference: Optional[EntityReference] = Field(default=None)

    # The actual data being processed - accepts dicts or Pydantic models
    payload: Union[Dict[str, Any], BaseModel] = Field()

    @field_validator("payload", mode="before")
    @classmethod
    def convert_payload(cls, v):
        """Convert Pydantic models to dicts for JSON serialization."""
        if isinstance(v, BaseModel):
            return v.model_dump()
        return v

    @field_validator("pipeline_id", mode="before")
    @classmethod
    def validate_pipeline_id(cls, v):
        """Generate UUID for pipeline_id if None is provided."""
        if v is None:
            return str(uuid4())
        return v

    # Optional metadata for routing/processing hints
    metadata: Dict[str, Any] = Field(default_factory=dict)

    # Timestamps
    created_at: datetime = Field(default_factory=datetime.utcnow)

    # Simple retry tracking
    retry_count: int = Field(default=0)

    def increment_retry(self) -> None:
        """Increment retry count."""
        self.retry_count += 1

    @classmethod
    def create_entity_message(
        cls,
        entity_reference: EntityReference,
        payload: Dict[str, Any],
        correlation_id: Optional[str] = None,
        pipeline_id: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> "Message":
        """Create message for entity processing."""
        return cls(
            correlation_id=correlation_id or str(uuid4()),
            pipeline_id=pipeline_id or str(uuid4()),
            entity_reference=entity_reference,
            payload=payload,
            metadata=metadata or {},
        )

    @classmethod
    def from_entity(
        cls,
        entity: Any,
        payload: Dict[str, Any],
        correlation_id: Optional[str] = None,
        pipeline_id: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> "Message":
        """Create message from an entity object (backward compatibility)."""
        entity_ref = EntityReference.from_entity(entity)
        return cls.create_entity_message(
            entity_reference=entity_ref,
            payload=payload,
            correlation_id=correlation_id,
            pipeline_id=pipeline_id,
            metadata=metadata,
        )
