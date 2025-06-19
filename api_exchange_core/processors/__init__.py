"""
Unified processor framework for data integration pipelines.

This module provides the core interfaces and classes for building flexible
data processing pipelines.
"""

# Import shared components
from .mapper_interface import CompositeMapper, MapperInterface
from .processing_result import ProcessingResult, ProcessingStatus

# Import core processor components
from .v2.message import Message, MessageType
from .v2.processor_factory import (
    create_db_manager,
    create_processor_handler,
)
from .v2.processor_handler import ProcessorHandler
from .v2.processor_interface import ProcessorContext, ProcessorInterface

# Import output handlers
from .v2.output_handlers import (
    OutputHandler,
    OutputHandlerError,
    OutputHandlerResult,
    OutputHandlerStatus,
    QueueOutputHandler,
    FileOutputHandler,
    NoOpOutputHandler,
)

# Import infrastructure processors
from . import infrastructure

# Import entity reference for compatibility
from ..schemas.entity_schema import EntityReference

# Rebuild models to resolve forward references
ProcessingResult.model_rebuild()

__all__ = [
    # Core interfaces and classes
    "ProcessorInterface",
    "ProcessorContext",
    "Message",
    "MessageType",
    "EntityReference",
    # Shared components
    "MapperInterface",
    "CompositeMapper",
    "ProcessingResult",
    "ProcessingStatus",
    # Factory and handler patterns
    "ProcessorHandler",
    "create_processor_handler",
    "create_db_manager",
    # Output handlers
    "OutputHandler",
    "OutputHandlerError",
    "OutputHandlerResult",
    "OutputHandlerStatus",
    "QueueOutputHandler",
    "FileOutputHandler",
    "NoOpOutputHandler",
    # Infrastructure
    "infrastructure",
]
