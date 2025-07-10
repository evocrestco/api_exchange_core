"""
Processing result structure for pipeline operations.

This module provides a clean result object for processor operations,
including success/failure status and output routing instructions.
"""

from datetime import datetime, timezone
from enum import Enum
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field

from .message import Message


class ProcessingStatus(str, Enum):
    """Status of processing operation."""

    SUCCESS = "success"
    FAILURE = "failure"
    PARTIAL_SUCCESS = "partial_success"


class ProcessingResult(BaseModel):
    """
    Result of a processor operation.

    Contains status, output messages, and optional metadata about the processing.
    """

    status: ProcessingStatus = Field(description="Processing status")
    success: bool = Field(description="Whether processing succeeded")

    # Output routing
    output_messages: List[Message] = Field(
        default_factory=list, description="Messages to route to output queues"
    )

    # Optional metadata
    processing_duration_ms: Optional[int] = Field(
        default=None, description="Processing duration in milliseconds"
    )

    records_processed: int = Field(default=0, description="Number of records processed")

    # Error information
    error_message: Optional[str] = Field(
        default=None, description="Error message if processing failed"
    )

    error_code: Optional[str] = Field(default=None, description="Error code if processing failed")

    # Additional context
    context: Dict[str, Any] = Field(
        default_factory=dict, description="Additional processing context"
    )

    created_at: datetime = Field(
        default_factory=lambda: datetime.now(timezone.utc), description="Result creation timestamp"
    )

    @classmethod
    def success_result(
        cls,
        output_messages: Optional[List[Message]] = None,
        records_processed: int = 0,
        processing_duration_ms: Optional[int] = None,
        **kwargs,
    ) -> "ProcessingResult":
        """
        Create a successful processing result.

        Args:
            output_messages: Messages to route to output queues
            records_processed: Number of records processed
            processing_duration_ms: Processing duration in milliseconds
            **kwargs: Additional context

        Returns:
            ProcessingResult configured for success
        """
        return cls(
            status=ProcessingStatus.SUCCESS,
            success=True,
            output_messages=output_messages or [],
            records_processed=records_processed,
            processing_duration_ms=processing_duration_ms,
            **kwargs,
        )

    @classmethod
    def failure_result(
        cls,
        error_message: str,
        error_code: Optional[str] = None,
        processing_duration_ms: Optional[int] = None,
        **kwargs,
    ) -> "ProcessingResult":
        """
        Create a failed processing result.

        Args:
            error_message: Error description
            error_code: Error code
            processing_duration_ms: Processing duration in milliseconds
            **kwargs: Additional context

        Returns:
            ProcessingResult configured for failure
        """
        return cls(
            status=ProcessingStatus.FAILURE,
            success=False,
            error_message=error_message,
            error_code=error_code,
            processing_duration_ms=processing_duration_ms,
            **kwargs,
        )

    @classmethod
    def partial_success_result(
        cls,
        output_messages: Optional[List[Message]] = None,
        records_processed: int = 0,
        error_message: Optional[str] = None,
        processing_duration_ms: Optional[int] = None,
        **kwargs,
    ) -> "ProcessingResult":
        """
        Create a partial success processing result.

        Args:
            output_messages: Messages to route to output queues
            records_processed: Number of records processed
            error_message: Error description for failed portion
            processing_duration_ms: Processing duration in milliseconds
            **kwargs: Additional context

        Returns:
            ProcessingResult configured for partial success
        """
        return cls(
            status=ProcessingStatus.PARTIAL_SUCCESS,
            success=True,  # Partial success still counts as success for routing
            output_messages=output_messages or [],
            records_processed=records_processed,
            error_message=error_message,
            processing_duration_ms=processing_duration_ms,
            **kwargs,
        )

    def add_output_message(self, message: Message) -> None:
        """
        Add an output message to the result.

        Args:
            message: Message to add to output
        """
        self.output_messages.append(message)

    def add_context(self, **context_data: Any) -> None:
        """
        Add context data to the result.

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
