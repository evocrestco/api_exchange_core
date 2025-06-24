"""
Processing result classes for processor pipeline communication.

This module defines the standardized result format returned by processors
to indicate processing outcomes and routing decisions.
"""

from datetime import datetime
from enum import Enum
from typing import TYPE_CHECKING, Any, Dict, List, Optional

from pydantic import BaseModel, Field, field_serializer

if TYPE_CHECKING:
    from . import Message
    from .v2.output_handlers.base import OutputHandler


class ProcessingStatus(str, Enum):
    """
    Status values for processing results.

    Indicates the outcome of processor execution for routing and error handling.
    """

    SUCCESS = "success"  # Processing completed successfully
    FAILED = "failed"  # Processing failed and should not retry
    ERROR = "error"  # Processing error that may be retryable
    SKIPPED = "skipped"  # Processing was skipped (e.g., duplicate)
    PARTIAL = "partial"  # Some parts succeeded, some failed
    DEAD_LETTERED = "dead_lettered"  # Processing failed and was routed to dead letter queue


class ProcessingResult(BaseModel):
    """
    Result returned by processors indicating processing outcomes and routing.

    Contains information about processing success/failure, any output messages
    to route to subsequent pipeline stages, and metadata about the operation.
    """

    # Processing outcome
    status: ProcessingStatus = Field(description="Overall status of the processing operation")

    success: bool = Field(description="Whether processing completed successfully")

    # Output and routing
    output_messages: List["Message"] = Field(
        default_factory=list, description="Messages to route to subsequent pipeline stages"
    )

    output_handlers: List[Any] = Field(
        default_factory=list, description="Type-safe output handlers for structured routing"
    )

    # Error information
    error_message: Optional[str] = Field(
        default=None, description="Human-readable error message if processing failed"
    )

    error_code: Optional[str] = Field(
        default=None, description="Machine-readable error code for categorization"
    )

    error_details: Dict[str, Any] = Field(
        default_factory=dict, description="Additional error context and debugging information"
    )

    # Processing metadata
    processing_metadata: Dict[str, Any] = Field(
        default_factory=dict, description="Metadata about the processing operation"
    )

    processor_info: Dict[str, Any] = Field(
        default_factory=dict, description="Information about the processor that created this result"
    )

    # Entity operations
    entities_created: List[str] = Field(
        default_factory=list, description="IDs of entities created during processing"
    )

    entities_updated: List[str] = Field(
        default_factory=list, description="IDs of entities updated during processing"
    )

    # Entity data for source processors
    entity_data: Optional[Dict[str, Any]] = Field(
        default=None, description="Entity data to be persisted (for source processors)"
    )

    entity_metadata: Optional[Dict[str, Any]] = Field(
        default=None, description="Entity metadata for persistence (for source processors)"
    )

    # Timing information
    processing_duration_ms: Optional[float] = Field(
        default=None, description="Processing duration in milliseconds"
    )

    completed_at: datetime = Field(
        default_factory=datetime.utcnow, description="When processing was completed"
    )

    # Retry information
    retry_after_seconds: Optional[int] = Field(
        default=None, description="Suggested delay before retry (for retryable errors)"
    )

    can_retry: bool = Field(default=True, description="Whether this processing can be retried")

    @field_serializer("output_handlers")
    def serialize_output_handlers(self, output_handlers: List[Any], _info) -> List[Dict[str, Any]]:
        """Serialize output handlers to their JSON-safe metadata representation."""
        serialized = []
        for handler in output_handlers:
            if hasattr(handler, "get_handler_info"):
                # Use the handler's own serialization method
                serialized.append(handler.get_handler_info())
            else:
                # Fallback for handlers without the method
                serialized.append(
                    {
                        "handler_type": handler.__class__.__name__,
                        "destination": getattr(handler, "destination", "unknown"),
                    }
                )
        return serialized

    @classmethod
    def create_success(
        cls,
        output_messages: Optional[List["Message"]] = None,
        output_handlers: Optional[List[Any]] = None,
        processing_metadata: Optional[Dict[str, Any]] = None,
        entities_created: Optional[List[str]] = None,
        entities_updated: Optional[List[str]] = None,
        processing_duration_ms: Optional[float] = None,
    ) -> "ProcessingResult":
        """
        Create a successful processing result.

        Convenience method for creating success results with common parameters.

        Args:
            output_messages: Messages to route to next stages
            output_handlers: Type-safe output handlers for routing
            processing_metadata: Additional processing metadata
            entities_created: IDs of created entities
            entities_updated: IDs of updated entities
            processing_duration_ms: Processing duration

        Returns:
            ProcessingResult indicating successful processing
        """
        return cls(
            status=ProcessingStatus.SUCCESS,
            success=True,
            output_messages=output_messages or [],
            output_handlers=output_handlers or [],
            processing_metadata=processing_metadata or {},
            entities_created=entities_created or [],
            entities_updated=entities_updated or [],
            processing_duration_ms=processing_duration_ms,
        )

    @classmethod
    def create_failure(
        cls,
        error_message: str,
        error_code: Optional[str] = None,
        error_details: Optional[Dict[str, Any]] = None,
        can_retry: bool = True,
        retry_after_seconds: Optional[int] = None,
        output_handlers: Optional[List["OutputHandler"]] = None,
        processing_duration_ms: float = 0.0,
    ) -> "ProcessingResult":
        """
        Create a failed processing result.

        Convenience method for creating failure results with error information.

        Args:
            error_message: Human-readable error description
            error_code: Machine-readable error code
            error_details: Additional error context
            can_retry: Whether processing can be retried
            retry_after_seconds: Suggested retry delay
            output_handlers: Output handlers for error routing (e.g., to error queue)
            processing_duration_ms: Processing duration in milliseconds

        Returns:
            ProcessingResult indicating failed processing
        """
        status = ProcessingStatus.ERROR if can_retry else ProcessingStatus.FAILED

        return cls(
            status=status,
            success=False,
            error_message=error_message,
            error_code=error_code,
            error_details=error_details or {},
            can_retry=can_retry,
            retry_after_seconds=retry_after_seconds,
            output_handlers=output_handlers or [],
            processing_duration_ms=processing_duration_ms,
        )

    @classmethod
    def create_skipped(
        cls,
        reason: str,
        processing_metadata: Optional[Dict[str, Any]] = None,
        output_handlers: Optional[List["OutputHandler"]] = None,
    ) -> "ProcessingResult":
        """
        Create a skipped processing result.

        Used when processing is intentionally skipped (e.g., duplicates, filters).

        Args:
            reason: Why processing was skipped
            processing_metadata: Additional metadata
            output_handlers: Output handlers for routing skipped results

        Returns:
            ProcessingResult indicating skipped processing
        """
        return cls(
            status=ProcessingStatus.SKIPPED,
            success=True,  # Skipping is considered "successful"
            processing_metadata={
                **(processing_metadata or {}),
                "skip_reason": reason,
            },
            output_handlers=output_handlers or [],
        )

    def add_output_message(self, message: "Message") -> None:
        """Add an output message for routing."""
        self.output_messages.append(message)

    def add_output_handler(self, handler: Any) -> None:
        """Add an output handler for structured routing."""
        self.output_handlers.append(handler)

    def has_output_handlers(self) -> bool:
        """Check if any output handlers are configured."""
        return bool(self.output_handlers)

    def add_metadata(self, key: str, value: Any) -> None:
        """Add processing metadata."""
        self.processing_metadata[key] = value

    def add_entity_created(self, entity_id: str) -> None:
        """Record that an entity was created."""
        self.entities_created.append(entity_id)

    def add_entity_updated(self, entity_id: str) -> None:
        """Record that an entity was updated."""
        self.entities_updated.append(entity_id)

    def has_entities_changed(self) -> bool:
        """Check if any entities were created or updated."""
        return bool(self.entities_created or self.entities_updated)

    def set_entity_data(
        self, data: Dict[str, Any], metadata: Optional[Dict[str, Any]] = None
    ) -> None:
        """Set entity data for source processors."""
        self.entity_data = data
        self.entity_metadata = metadata or {}

    def has_entity_data(self) -> bool:
        """Check if entity data is available for persistence."""
        return self.entity_data is not None

    def get_summary(self) -> Dict[str, Any]:
        """
        Get a summary of the processing result for logging/monitoring.

        Returns:
            Dictionary with key result information
        """
        return {
            "status": self.status.value,
            "success": self.success,
            "output_message_count": len(self.output_messages),
            "output_handler_count": len(self.output_handlers),
            "entities_created_count": len(self.entities_created),
            "entities_updated_count": len(self.entities_updated),
            "has_error": bool(self.error_message),
            "can_retry": self.can_retry,
            "processing_duration_ms": self.processing_duration_ms,
        }


# Forward reference resolution handled by TYPE_CHECKING in imports
