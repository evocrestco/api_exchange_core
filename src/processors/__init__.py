"""
Unified processor framework for data integration pipelines.

This module provides the core interfaces and classes for building flexible
data processing pipelines using the v2 architecture.
"""

# Import v2 components
from src.processors.v2.message import Message, MessageType
from src.processors.v2.processor_interface import ProcessorInterface, ProcessorContext
from src.processors.v2.processor_handler import ProcessorHandler
from src.processors.v2.processor_factory import (
    create_processor_handler,
    create_db_manager,
)

# Import shared components
from src.processors.mapper_interface import CompositeMapper, MapperInterface
from src.processors.processing_result import ProcessingResult, ProcessingStatus

# Import v2 message components for backwards compatibility
from src.schemas.entity_schema import EntityReference

# Rebuild models to resolve forward references
ProcessingResult.model_rebuild()

__all__ = [
    # Core interfaces and classes (v2)
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
    # Factory and handler patterns (v2)
    "ProcessorHandler",
    "create_processor_handler",
    "create_db_manager",
]