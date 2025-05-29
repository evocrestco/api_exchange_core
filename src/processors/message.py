"""
Message classes for processor pipeline communication.

This module defines the standardized message format used for communication
between processors in data integration pipelines.
"""

from datetime import datetime
from enum import Enum
from typing import Any, Dict, Optional
from uuid import uuid4

from pydantic import BaseModel, Field


class MessageType(str, Enum):
    """
    Types of messages that can flow through the pipeline.

    Provides semantic meaning for pipeline monitoring and routing decisions.
    """

    ENTITY_PROCESSING = "entity_processing"  # Processing entity data
    CONTROL_MESSAGE = "control_message"  # Pipeline control signals
    ERROR_MESSAGE = "error_message"  # Error notifications
    HEARTBEAT = "heartbeat"  # Health check signals
    METRICS = "metrics"  # Performance metrics


class EntityReference(BaseModel):
    """
    Reference to an entity in the system.

    Provides the link between pipeline messages and stored entities
    without requiring the full entity data in every message.
    """

    entity_id: Optional[str] = Field(
        default=None, description="Internal entity ID (if entity exists)"
    )

    external_id: str = Field(description="External identifier from source system")

    canonical_type: str = Field(description="Type of canonical data (e.g., 'order', 'customer')")

    source: str = Field(description="Source system identifier")

    tenant_id: str = Field(description="Tenant identifier for multi-tenant isolation")

    version: Optional[int] = Field(
        default=None, description="Entity version number (if entity exists)"
    )


class Message(BaseModel):
    """
    Standardized message format for processor pipeline communication.

    Messages flow between processors carrying entity references, payload data,
    and metadata necessary for processing and routing decisions.
    """

    # Message identification
    message_id: str = Field(
        default_factory=lambda: str(uuid4()), description="Unique identifier for this message"
    )

    correlation_id: str = Field(
        default_factory=lambda: str(uuid4()),
        description="Correlation ID for tracking across pipeline stages",
    )

    message_type: MessageType = Field(
        default=MessageType.ENTITY_PROCESSING,
        description="Type of message for routing and processing",
    )

    # Entity reference
    entity_reference: EntityReference = Field(description="Reference to the entity being processed")

    # Payload and metadata
    payload: Dict[str, Any] = Field(description="The actual data being processed")

    metadata: Dict[str, Any] = Field(
        default_factory=dict, description="Processing metadata and context information"
    )

    # Routing information
    routing_info: Dict[str, Any] = Field(
        default_factory=dict, description="Routing instructions for pipeline stages"
    )

    # Timestamps
    created_at: datetime = Field(
        default_factory=datetime.utcnow, description="When the message was created"
    )

    processed_at: Optional[datetime] = Field(
        default=None, description="When the message was last processed"
    )

    # Processing context
    retry_count: int = Field(default=0, description="Number of processing retry attempts")

    max_retries: int = Field(default=3, description="Maximum number of retry attempts allowed")

    def mark_processed(self) -> None:
        """Mark the message as processed with current timestamp."""
        self.processed_at = datetime.utcnow()

    def increment_retry(self) -> None:
        """Increment the retry count for failed processing."""
        self.retry_count += 1

    def can_retry(self) -> bool:
        """Check if the message can be retried."""
        return self.retry_count < self.max_retries

    def add_metadata(self, key: str, value: Any) -> None:
        """Add metadata to the message."""
        self.metadata[key] = value

    def add_routing_info(self, key: str, value: Any) -> None:
        """Add routing information to the message."""
        self.routing_info[key] = value

    def get_processing_context(self) -> Dict[str, Any]:
        """
        Get context information for processing operations.

        Returns:
            Dictionary with tenant_id, correlation_id, and other context
            needed for processing operations
        """
        return {
            "tenant_id": self.entity_reference.tenant_id,
            "correlation_id": self.correlation_id,
            "message_id": self.message_id,
            "entity_external_id": self.entity_reference.external_id,
            "entity_source": self.entity_reference.source,
            "message_type": self.message_type.value,
        }

    @classmethod
    def create_entity_message(
        cls,
        external_id: str,
        canonical_type: str,
        source: str,
        tenant_id: str,
        payload: Dict[str, Any],
        entity_id: Optional[str] = None,
        version: Optional[int] = None,
        correlation_id: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> "Message":
        """
        Create a message for entity processing.

        Convenience method for creating messages with entity references.

        Args:
            external_id: External identifier from source system
            canonical_type: Type of canonical data
            source: Source system identifier
            tenant_id: Tenant identifier
            payload: The data to process
            entity_id: Internal entity ID (if entity exists)
            version: Entity version (if entity exists)
            correlation_id: Correlation ID for tracking
            metadata: Additional metadata

        Returns:
            Message configured for entity processing
        """
        entity_ref = EntityReference(
            entity_id=entity_id,
            external_id=external_id,
            canonical_type=canonical_type,
            source=source,
            tenant_id=tenant_id,
            version=version,
        )

        return cls(
            correlation_id=correlation_id or str(uuid4()),
            message_type=MessageType.ENTITY_PROCESSING,
            entity_reference=entity_ref,
            payload=payload,
            metadata=metadata or {},
        )

    @classmethod
    def create_control_message(
        cls,
        command: str,
        tenant_id: str,
        payload: Optional[Dict[str, Any]] = None,
        correlation_id: Optional[str] = None,
    ) -> "Message":
        """
        Create a control message for pipeline management.

        Args:
            command: Control command (e.g., 'pause', 'resume', 'shutdown')
            tenant_id: Tenant identifier
            payload: Command parameters
            correlation_id: Correlation ID for tracking

        Returns:
            Message configured for control operations
        """
        # Create minimal entity reference for control messages
        entity_ref = EntityReference(
            external_id=f"control-{command}",
            canonical_type="control",
            source="system",
            tenant_id=tenant_id,
        )

        # Merge command into payload
        control_payload = payload.copy() if payload else {}
        control_payload["command"] = command

        return cls(
            correlation_id=correlation_id or str(uuid4()),
            message_type=MessageType.CONTROL_MESSAGE,
            entity_reference=entity_ref,
            payload=control_payload,
            metadata={"command": command},
        )
