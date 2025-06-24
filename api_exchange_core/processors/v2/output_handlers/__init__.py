"""
Output handler infrastructure for processors v2.

This package provides type-safe output handling for processors, replacing
the magic string routing_info dictionaries with structured handler classes.

Available output handlers:
- OutputHandler: Abstract base class for all handlers
- QueueOutputHandler: Azure Storage Queue output
- ServiceBusOutputHandler: Azure Service Bus output
- FileOutputHandler: Local file system output
- NoOpOutputHandler: No-operation handler for processors with no output

Usage:
    from src.processors.v2.output_handlers import QueueOutputHandler

    # In processor.process() method:
    result = ProcessingResult.create_success()
    result.add_output_handler(
        QueueOutputHandler(
            queue_name="next-step-queue",
            connection_string="UseDevelopmentStorage=true"
        )
    )
    return result
"""

from .base import OutputHandler, OutputHandlerError, OutputHandlerResult, OutputHandlerStatus
from .file_output import FileOutputHandler
from .noop_output import NoOpOutputHandler
from .queue_output import QueueOutputHandler

# Import ServiceBusOutputHandler only if azure-servicebus is available
try:
    from .service_bus_output import ServiceBusOutputHandler  # noqa: F401

    __all__ = [
        "OutputHandler",
        "OutputHandlerError",
        "OutputHandlerResult",
        "OutputHandlerStatus",
        "QueueOutputHandler",
        "ServiceBusOutputHandler",
        "FileOutputHandler",
        "NoOpOutputHandler",
    ]
except ImportError:
    # ServiceBus SDK not available, exclude from exports
    __all__ = [
        "OutputHandler",
        "OutputHandlerError",
        "OutputHandlerResult",
        "OutputHandlerStatus",
        "QueueOutputHandler",
        "FileOutputHandler",
        "NoOpOutputHandler",
    ]
