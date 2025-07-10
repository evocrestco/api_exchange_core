"""
No-op output handler for terminal processors.

This handler does nothing with the output messages - it's used
for processors that are terminal points in the pipeline.
"""

from typing import Any, Dict

from ..message import Message
from ..processing_result import ProcessingResult
from .base_output_handler import BaseOutputHandler


class NoOpOutputHandler(BaseOutputHandler):
    """
    Output handler that does nothing with output messages.

    This is useful for terminal processors that don't need to
    route their output anywhere.
    """

    def __init__(self):
        """Initialize the no-op output handler."""
        pass

    def handle_output(self, result: ProcessingResult, source_message: Message, context: Dict[str, Any]) -> None:
        """
        Handle output messages by doing nothing.

        Args:
            result: Processing result with output messages
            source_message: Original message that was processed
            context: Processing context
        """
        # Do nothing - this is a terminal processor
        pass

    def get_handler_name(self) -> str:
        """
        Get the name of this output handler.

        Returns:
            Handler name for logging
        """
        return "NoOpOutputHandler"
