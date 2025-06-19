"""
Processing module for entity processing services.

This module provides services for entity processing workflows including
duplicate detection, attribute building, and processing orchestration.
"""

from .duplicate_detection import DuplicateDetectionResult, DuplicateDetectionService
from .entity_attributes import EntityAttributeBuilder
from .processing_service import ProcessingResult, ProcessingService
from .processor_config import ProcessorConfig

__all__ = [
    "DuplicateDetectionResult",
    "DuplicateDetectionService",
    "EntityAttributeBuilder",
    "ProcessingResult",
    "ProcessingService",
    "ProcessorConfig",
]
