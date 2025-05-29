"""
Type definitions for the API Exchange Core framework.

This module provides TypedDict and other type definitions for better
type safety and IDE support throughout the codebase.
"""

from typing import Any, Dict, Literal

try:
    from typing import NotRequired  # Python 3.11+

    from typing_extensions import (
        TypedDict,  # Use typing_extensions for better Pydantic compatibility
    )
except ImportError:
    from typing_extensions import NotRequired, TypedDict  # Python 3.8-3.10


# State literals for type safety
EntityStateLiteral = Literal[
    "RECEIVED",
    "VALIDATED",
    "TRANSFORMED",
    "ENRICHED",
    "READY_TO_DELIVER",
    "DELIVERED",
    "COMPLETED",
    "FAILED",
    "VALIDATION_ERROR",
    "TRANSFORMATION_ERROR",
    "DELIVERY_ERROR",
    "SYSTEM_ERROR",
    "UPDATE_RECEIVED",
    "UPDATE_PROCESSING",
    "UPDATE_VALIDATED",
    "UPDATE_DELIVERED",
    "UPDATE_COMPLETED",
    "UPDATE_ERROR",
    "DUPLICATE_DETECTED",
    "MANUALLY_RESOLVED",
    "ON_HOLD",
    "PENDING_REVIEW",
]

TransitionTypeLiteral = Literal["NORMAL", "ERROR", "RETRY", "MANUAL", "SYSTEM"]


class ProcessorData(TypedDict):
    """Type definition for processor data in state transitions."""

    processor_name: str
    processor_version: NotRequired[str]
    error_detail: NotRequired[str]
    processing_time_ms: NotRequired[int]
    retry_count: NotRequired[int]
    custom_data: NotRequired[Dict[str, Any]]


class MessageMetadata(TypedDict):
    """Type definition for message metadata."""

    previous_state: NotRequired[str]
    state_changed_at: NotRequired[str]
    correlation_id: NotRequired[str]
    source_system: NotRequired[str]
    processing_flags: NotRequired[Dict[str, bool]]


class MessageDict(TypedDict):
    """Type definition for messages passed between processors."""

    entity_id: str
    state: str
    tenant_id: str
    metadata: NotRequired[MessageMetadata]
    payload: NotRequired[Dict[str, Any]]


class EntityAttributes(TypedDict, total=False):
    """Type definition for entity attributes.

    Using total=False to make all fields optional since attributes
    are flexible and domain-specific.
    """

    # Common attributes that might be used
    name: str
    description: str
    status: str
    category: str
    tags: list[str]
    # Allow any additional attributes
    # Note: TypedDict doesn't support arbitrary keys directly,
    # so in practice this will still be Dict[str, Any] in many places


class TenantConfig(TypedDict):
    """Type definition for tenant configuration."""

    feature_flags: NotRequired[Dict[str, bool]]
    api_limits: NotRequired[Dict[str, int]]
    custom_settings: NotRequired[Dict[str, Any]]
