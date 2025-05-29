"""
Unified processor interface for data integration pipelines.

This module defines the single ProcessorInterface that all processors implement,
eliminating the need for separate Source/Intermediate/Terminal processor types.
"""

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from src.processors.message import Message
    from src.processors.processing_result import ProcessingResult


class ProcessorInterface(ABC):
    """
    Unified interface for all processors in data integration pipelines.

    This single interface replaces the old Source/Intermediate/Terminal pattern
    with a flexible approach that allows processors to:
    - Create entities from external data
    - Update existing entities
    - Transform data
    - Make routing decisions
    - Terminate processing chains

    The specific behavior is determined by the processor implementation and
    configuration, not by artificial type constraints.
    """

    @abstractmethod
    def process(self, message: "Message") -> "ProcessingResult":
        """
        Process a message and return the result with routing information.

        This is the single method all processors must implement. Processors
        can perform any combination of operations:

        - **Entity Operations**: Create new entities or update existing ones
        - **Data Transformation**: Modify, validate, or enrich the message payload
        - **Routing Decisions**: Determine where to send the processed message
        - **Business Logic**: Apply domain-specific rules and workflows
        - **External Integration**: Communicate with APIs, databases, or files

        Args:
            message: The message to process, containing entity reference,
                    payload data, and processing metadata

        Returns:
            ProcessingResult indicating success/failure, any output messages
            for routing, and metadata about the processing operation

        Raises:
            ProcessingError: If processing fails and cannot be retried
            ValidationError: If message data is invalid
            ServiceError: If external dependencies fail
        """
        pass

    def get_processor_info(self) -> dict:
        """
        Get information about this processor for monitoring and debugging.

        Returns:
            Dictionary with processor metadata including name, version,
            and configuration details. Default implementation returns
            basic class information.
        """
        return {
            "processor_class": self.__class__.__name__,
            "processor_module": self.__class__.__module__,
        }

    def validate_message(self, message: "Message") -> bool:
        """
        Validate that a message can be processed by this processor.

        Default implementation accepts all messages. Processors can override
        this to implement specific validation rules.

        Args:
            message: Message to validate

        Returns:
            True if message is valid for processing, False otherwise
        """
        return True

    def can_retry(self, error: Exception) -> bool:
        """
        Determine if processing can be retried after an error.

        Default implementation allows retry for most errors except
        validation errors. Processors can override for custom retry logic.

        Args:
            error: Exception that occurred during processing

        Returns:
            True if processing can be retried, False otherwise
        """
        from src.exceptions import ValidationError

        # Don't retry validation errors - they won't succeed on retry
        if isinstance(error, ValidationError):
            return False

        # Retry other errors by default
        return True
