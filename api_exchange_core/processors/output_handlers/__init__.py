"""Output handlers for routing processor results."""

from .no_op_output_handler import NoOpOutputHandler
from .queue_output_handler import QueueOutputHandler

__all__ = [
    "NoOpOutputHandler",
    "QueueOutputHandler",
]
