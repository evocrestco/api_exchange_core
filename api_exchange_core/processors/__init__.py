"""Core processor framework for API Exchange V2."""

from .message import Message, MessageType
from .output_handlers import NoOpOutputHandler, QueueOutputHandler
from .processing_result import ProcessingResult, ProcessingStatus
from .simple_processor_handler import SimpleProcessorHandler
from .simple_processor_interface import SimpleProcessorInterface

__all__ = [
    "Message",
    "MessageType",
    "ProcessingResult",
    "ProcessingStatus",
    "SimpleProcessorInterface",
    "SimpleProcessorHandler",
    "NoOpOutputHandler",
    "QueueOutputHandler",
]
