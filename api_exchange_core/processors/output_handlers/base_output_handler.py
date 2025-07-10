"""
Base output handler providing common interface for all output handlers.

This abstract base class defines the standard interface that all
output handlers must implement.
"""

from abc import ABC, abstractmethod
from typing import Any, Dict

from ..message import Message
from ..processing_result import ProcessingResult


class BaseOutputHandler(ABC):
    """
    Abstract base class for all output handlers.

    Defines the standard interface that all output handlers must implement.
    """

    @abstractmethod
    def handle_output(self, result: ProcessingResult, source_message: Message, context: Dict[str, Any]) -> None:
        """
        Handle output messages from processing results.

        Args:
            result: Processing result with output messages
            source_message: Original message that was processed
            context: Processing context
        """
        pass

    @abstractmethod
    def get_handler_name(self) -> str:
        """
        Get the name of this output handler.

        Returns:
            Handler name for logging and identification
        """
        pass
