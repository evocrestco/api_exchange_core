"""
Simple processor interface for the V2 framework.

This module provides a clean, minimal interface for processors that
focuses purely on message transformation without external dependencies.
"""

from abc import ABC, abstractmethod
from typing import Any, Dict

from .message import Message
from .processing_result import ProcessingResult


class SimpleProcessorInterface(ABC):
    """
    Minimal processor interface for pipeline operations.

    This interface focuses purely on message transformation:
    - Receive a Message
    - Apply business logic
    - Return a ProcessingResult

    No dependencies on persistence, external services, or complex state management.
    """

    @abstractmethod
    def process(self, message: Message, context: Dict[str, Any]) -> ProcessingResult:
        """
        Process a message and return the result.

        This is the core method that all processors must implement.
        It should contain only the business logic for transforming
        the input message into output messages.

        Args:
            message: Input message to process
            context: Processing context (tenant_id, request_id, etc.)

        Returns:
            ProcessingResult with success/failure status and output messages

        Raises:
            Should not raise exceptions - return failure result instead
        """
        pass

    def get_processor_name(self) -> str:
        """
        Get the name of this processor.

        Default implementation returns the class name.
        Override this method to provide a custom name.

        Returns:
            Processor name for logging and tracking
        """
        return self.__class__.__name__

    def validate_message(self, message: Message) -> bool:
        """
        Validate that the message is suitable for processing.

        Default implementation performs basic validation.
        Override this method to add custom validation logic.

        Args:
            message: Message to validate

        Returns:
            True if message is valid, False otherwise
        """
        # Basic validation
        if not message.payload:
            return False

        if not message.pipeline_id:
            return False

        return True

    def create_output_message(self, payload: Dict[str, Any], source_message: Message) -> Message:
        """
        Create an output message that inherits context from the source message.

        Helper method for creating output messages that maintain pipeline tracking.

        Args:
            payload: Output message data
            source_message: Source message to inherit context from

        Returns:
            New Message with inherited context
        """
        return source_message.create_child_message(
            payload=payload, processor_name=self.get_processor_name()
        )
