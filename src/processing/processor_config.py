"""
Processor configuration models for controlling processing behavior.

This module provides configuration classes for customizing processor behavior
including duplicate detection, versioning, and processing options.
"""

from typing import Any, Dict, Optional, List

from pydantic import BaseModel, ConfigDict, Field

from src.utils.hash_config import HashConfig


class ProcessorConfig(BaseModel):
    """
    Configuration for processor behavior.

    Controls various aspects of entity processing including duplicate detection,
    versioning strategy, and custom processing options.
    """

    # Duplicate detection configuration
    enable_duplicate_detection: bool = Field(
        default=True, description="Whether to perform duplicate detection"
    )

    duplicate_detection_strategy: str = Field(
        default="content_hash",
        description="Strategy for duplicate detection (content_hash, custom)",
    )

    hash_config: Optional[HashConfig] = Field(
        default=None, description="Configuration for content hash calculation"
    )

    # Versioning configuration
    force_new_version: bool = Field(
        default=False, description="Always create new version regardless of content changes"
    )

    is_source_processor: bool = Field(
        default=True,
        description="Whether this is a source processor (creates entities) or processing processor",
    )

    # Processing options
    update_attributes_on_duplicate: bool = Field(
        default=True, description="Whether to update entity attributes when duplicates are found"
    )

    preserve_attribute_keys: list = Field(
        default_factory=list, description="Attribute keys to preserve when merging attributes"
    )

    # Metadata and identification
    processor_name: str = Field(description="Name of the processor for tracking and logging")

    processor_version: str = Field(default="1.0.0", description="Version of the processor")

    # Custom configuration
    custom_config: Dict[str, Any] = Field(
        default_factory=dict, description="Custom configuration specific to the processor"
    )

    # Error handling
    fail_on_duplicate_detection_error: bool = Field(
        default=False, description="Whether to fail processing if duplicate detection fails"
    )

    max_similar_entities: int = Field(
        default=10, description="Maximum number of similar entities to track in duplicate detection"
    )

    # State tracking configuration
    enable_state_tracking: bool = Field(
        default=False, description="Whether to record state transitions for entity processing"
    )

    # Output handler configuration
    output_handlers_config: Optional[Dict[str, Dict[str, Any]]] = Field(
        default=None,
        description="Configuration for output handlers (e.g., queues, files, service bus)"
    )
    
    default_output_handlers: Optional[List[str]] = Field(
        default=None,
        description="List of output handler names to use by default"
    )
    
    output_handler_env_prefix: Optional[str] = Field(
        default=None,
        description="Environment variable prefix for loading output handler configs"
    )

    model_config = ConfigDict(extra="allow")  # Allow additional configuration fields
