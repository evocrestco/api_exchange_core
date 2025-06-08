"""
Message classes v2 for processor pipeline communication.

Key improvements from v1:
- Takes Entity directly instead of EntityReference
- Eliminates circular dependencies 
- EntityReference created on-demand from Entity
- Cleaner entity creation sequence
"""

from datetime import datetime
from enum import Enum
from typing import Any, Dict, Optional
from uuid import uuid4

from pydantic import BaseModel, Field, ConfigDict

from src.schemas.entity_schema import EntityReference


class MessageType(str, Enum):
    """Types of messages that can flow through the pipeline."""
    ENTITY_PROCESSING = "entity_processing"
    CONTROL_MESSAGE = "control_message"
    ERROR_MESSAGE = "error_message"
    HEARTBEAT = "heartbeat"
    METRICS = "metrics"


class Message(BaseModel):
    """Message format v2 - takes Entity directly."""

    message_id: str = Field(default_factory=lambda: str(uuid4()))
    correlation_id: str = Field(default_factory=lambda: str(uuid4()))
    message_type: MessageType = Field(default=MessageType.ENTITY_PROCESSING)

    # Entity - direct reference to the actual entity
    entity: Any = Field()

    payload: Dict[str, Any] = Field()
    metadata: Dict[str, Any] = Field(default_factory=dict)
    routing_info: Dict[str, Any] = Field(default_factory=dict)

    created_at: datetime = Field(default_factory=datetime.utcnow)
    processed_at: Optional[datetime] = Field(default=None)

    retry_count: int = Field(default=0)
    max_retries: int = Field(default=3)

    @property
    def entity_reference(self) -> EntityReference:
        """Get EntityReference from the entity on-demand."""
        return EntityReference.from_entity(self.entity)

    def mark_processed(self) -> None:
        """Mark the message as processed."""
        self.processed_at = datetime.utcnow()

    def increment_retry(self) -> None:
        """Increment retry count."""
        self.retry_count += 1

    def can_retry(self) -> bool:
        """Check if message can be retried."""
        return self.retry_count < self.max_retries

    @classmethod
    def create_entity_message(
            cls,
            entity,
            payload: Dict[str, Any],
            correlation_id: Optional[str] = None,
            metadata: Optional[Dict[str, Any]] = None,
    ) -> "Message":
        """Create message for entity processing."""
        return cls(
            correlation_id=correlation_id or str(uuid4()),
            entity=entity,
            payload=payload,
            metadata=metadata or {},
        )

    model_config = ConfigDict(arbitrary_types_allowed=True)
