"""
Lightweight message structure for pipeline communication.

This module provides a simple Message class for passing data between processors
in the pipeline, with automatic pipeline_id tracking for execution flow.
"""

from datetime import datetime, timezone
from enum import Enum
from typing import Any, Dict, Optional
from uuid import uuid4

from pydantic import BaseModel, Field


class MessageType(str, Enum):
    """Types of messages in the pipeline."""

    DATA_PROCESSING = "data_processing"
    PIPELINE_TRIGGER = "pipeline_trigger"
    ERROR_NOTIFICATION = "error_notification"


class Message(BaseModel):
    """
    Lightweight message for queue transport between processors.

    Contains everything needed for pipeline execution tracking and
    data routing between processors.
    """

    message_id: str = Field(default_factory=lambda: str(uuid4()))
    correlation_id: str = Field(default_factory=lambda: str(uuid4()))
    pipeline_id: str = Field(default_factory=lambda: str(uuid4()))
    message_type: MessageType = Field(default=MessageType.DATA_PROCESSING)

    # Core message data
    payload: Dict[str, Any] = Field(default_factory=dict)

    # Optional metadata
    tenant_id: Optional[str] = Field(default=None)
    processor_name: Optional[str] = Field(default=None)
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))

    # Processing context
    context: Dict[str, Any] = Field(default_factory=dict)

    @classmethod
    def create_simple_message(
        cls,
        payload: Dict[str, Any],
        pipeline_id: Optional[str] = None,
        tenant_id: Optional[str] = None,
        processor_name: Optional[str] = None,
        **kwargs,
    ) -> "Message":
        """
        Create a simple message with payload.

        Args:
            payload: The message data
            pipeline_id: Pipeline ID to track execution (auto-generated if not provided)
            tenant_id: Tenant context
            processor_name: Name of the processor creating this message
            **kwargs: Additional message fields

        Returns:
            New Message instance
        """
        return cls(
            payload=payload,
            pipeline_id=pipeline_id or str(uuid4()),
            tenant_id=tenant_id,
            processor_name=processor_name,
            **kwargs,
        )

    @classmethod
    def create_trigger_message(
        cls,
        payload: Dict[str, Any],
        tenant_id: Optional[str] = None,
        processor_name: Optional[str] = None,
        **kwargs,
    ) -> "Message":
        """
        Create a pipeline trigger message.

        This is typically used to start a new pipeline execution.

        Args:
            payload: The trigger data
            tenant_id: Tenant context
            processor_name: Name of the processor creating this message
            **kwargs: Additional message fields

        Returns:
            New Message instance configured as pipeline trigger
        """
        return cls(
            payload=payload,
            message_type=MessageType.PIPELINE_TRIGGER,
            tenant_id=tenant_id,
            processor_name=processor_name,
            **kwargs,
        )

    def create_child_message(
        self, payload: Dict[str, Any], processor_name: Optional[str] = None, **kwargs
    ) -> "Message":
        """
        Create a child message that inherits context from this message.

        This preserves pipeline_id, tenant_id, and correlation_id to maintain
        traceability through the pipeline.

        Args:
            payload: The new message data
            processor_name: Name of the processor creating this message
            **kwargs: Additional message fields

        Returns:
            New Message instance with inherited context
        """
        return self.__class__(
            payload=payload,
            pipeline_id=self.pipeline_id,
            correlation_id=self.correlation_id,
            tenant_id=self.tenant_id,
            processor_name=processor_name,
            **kwargs,
        )

    def add_context(self, **context_data: Any) -> None:
        """
        Add context data to the message.

        Args:
            **context_data: Context key-value pairs to add
        """
        self.context.update(context_data)

    def get_context(self, key: str, default: Any = None) -> Any:
        """
        Get context value.

        Args:
            key: Context key
            default: Default value if key not found

        Returns:
            Context value or default
        """
        return self.context.get(key, default)
