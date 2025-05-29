"""
Unified processor framework for data integration pipelines.

This module provides the core interfaces and classes for building flexible
data processing pipelines without artificial processor type distinctions.
"""

from src.processors.message import EntityReference, Message, MessageType
from src.processors.processing_result import ProcessingResult, ProcessingStatus
from src.processors.processor_executor import ProcessorExecutor
from src.processors.processor_factory import (
    ProcessorFactory,
    ProcessorHandler,
    create_processor_handler,
)
from src.processors.processor_interface import ProcessorInterface

# Rebuild models to resolve forward references
ProcessingResult.model_rebuild()

__all__ = [
    # Core interfaces and classes
    "ProcessorInterface",
    "Message",
    "MessageType",
    "EntityReference",
    "ProcessingResult",
    "ProcessingStatus",
    # Execution framework
    "ProcessorExecutor",
    # Factory and handler patterns
    "ProcessorFactory",
    "ProcessorHandler",
    "create_processor_handler",
]
