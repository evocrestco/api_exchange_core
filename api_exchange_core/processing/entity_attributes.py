"""
Entity attribute builder service for standardized attribute management.

This module provides services for building and managing standardized entity
attributes including duplicate detection data, processing metadata, and custom attributes.
"""

from datetime import datetime
from typing import Any, Dict, Optional

from ..context.operation_context import operation
from ..utils.logger import get_logger
from .duplicate_detection import DuplicateDetectionResult


class EntityAttributeBuilder:
    """
    Service for building standardized entity attributes.

    Provides a builder pattern for constructing entity attributes with
    consistent structure and validation across the processing framework.
    """

    def __init__(self):
        """Initialize the attribute builder."""
        self.logger = get_logger()
        self._attributes: Dict[str, Any] = {}

    @operation(name="entity_attributes_build")
    def build(
        self,
        duplicate_detection_result: Optional[DuplicateDetectionResult] = None,
        custom_attributes: Optional[Dict[str, Any]] = None,
        processing_status: str = "processed",
        content_changed: bool = True,
        processor_name: Optional[str] = None,
        source_metadata: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """
        Build standardized entity attributes.

        Args:
            duplicate_detection_result: Duplicate detection analysis result
            custom_attributes: Custom attributes to include
            processing_status: Processing status (e.g., "processed", "failed")
            content_changed: Whether content changed from previous version
            processor_name: Name of processor that created/updated the entity
            source_metadata: Metadata from the source system

        Returns:
            Standardized attributes dictionary
        """
        attributes = {}

        # Add processing metadata
        processing_metadata = {
            "status": processing_status,
            "processed_at": datetime.utcnow().isoformat(),
            "content_changed": content_changed,
        }

        if processor_name:
            processing_metadata["processor"] = processor_name

        attributes["processing"] = processing_metadata

        # Add duplicate detection result if provided
        if duplicate_detection_result:
            attributes["duplicate_detection"] = duplicate_detection_result.to_dict()

        # Add source metadata if provided
        if source_metadata:
            attributes["source_metadata"] = source_metadata

        # Add custom attributes (merged last to allow overrides)
        if custom_attributes:
            attributes.update(custom_attributes)

        self.logger.debug(
            "Built entity attributes",
            extra={
                "attribute_keys": list(attributes.keys()),
                "has_duplicate_detection": duplicate_detection_result is not None,
                "processing_status": processing_status,
                "content_changed": content_changed,
            },
        )

        return attributes

    @operation(name="entity_attributes_merge")
    def merge_attributes(
        self,
        existing_attributes: Optional[Dict[str, Any]],
        new_attributes: Dict[str, Any],
        preserve_keys: Optional[list] = None,
    ) -> Dict[str, Any]:
        """
        Merge new attributes with existing attributes.

        Args:
            existing_attributes: Current entity attributes
            new_attributes: New attributes to merge
            preserve_keys: List of keys to preserve from existing attributes

        Returns:
            Merged attributes dictionary
        """
        if not existing_attributes:
            return new_attributes.copy()

        merged = existing_attributes.copy()

        # Preserve specified keys from existing attributes
        preserved = {}
        if preserve_keys:
            for key in preserve_keys:
                if key in merged:
                    preserved[key] = merged[key]

        # Update with new attributes
        merged.update(new_attributes)

        # Restore preserved keys
        merged.update(preserved)

        self.logger.debug(
            "Merged entity attributes",
            extra={
                "existing_keys": list(existing_attributes.keys()) if existing_attributes else [],
                "new_keys": list(new_attributes.keys()),
                "merged_keys": list(merged.keys()),
                "preserved_keys": preserve_keys or [],
            },
        )

        return merged

    @operation(name="entity_attributes_update_duplicate_detection")
    def update_duplicate_detection(
        self,
        existing_attributes: Optional[Dict[str, Any]],
        new_detection_result: DuplicateDetectionResult,
        merge_results: bool = True,
    ) -> Dict[str, Any]:
        """
        Update duplicate detection information in entity attributes.

        Args:
            existing_attributes: Current entity attributes
            new_detection_result: New duplicate detection result
            merge_results: Whether to merge with existing detection result

        Returns:
            Updated attributes with new duplicate detection data
        """
        attributes = existing_attributes.copy() if existing_attributes else {}

        if merge_results and "duplicate_detection" in attributes:
            try:
                # Try to merge with existing detection result
                existing_result = DuplicateDetectionResult.from_dict(
                    attributes["duplicate_detection"]
                )
                merged_result = existing_result.merge_with(new_detection_result)
                attributes["duplicate_detection"] = merged_result.to_dict()
            except Exception as e:
                self.logger.warning(
                    f"Failed to merge duplicate detection results: {e}",
                    extra={"error": str(e)},
                )
                # Fall back to replacing with new result
                attributes["duplicate_detection"] = new_detection_result.to_dict()
        else:
            # Replace existing result
            attributes["duplicate_detection"] = new_detection_result.to_dict()

        self.logger.debug(
            "Updated duplicate detection in attributes",
            extra={
                "confidence": new_detection_result.confidence,
                "is_duplicate": new_detection_result.is_duplicate,
                "reason": new_detection_result.reason,
                "merged": merge_results and "duplicate_detection" in (existing_attributes or {}),
            },
        )

        return attributes

    @operation(name="entity_attributes_get_processing_metadata")
    def get_processing_metadata(self, attributes: Optional[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Extract processing metadata from entity attributes.

        Args:
            attributes: Entity attributes

        Returns:
            Processing metadata dictionary
        """
        if not attributes:
            return {}

        processing_metadata = attributes.get("processing", {})

        self.logger.debug(
            "Extracted processing metadata",
            extra={
                "has_metadata": bool(processing_metadata),
                "metadata_keys": list(processing_metadata.keys()) if processing_metadata else [],
            },
        )

        return processing_metadata

    @operation(name="entity_attributes_is_suspicious")
    def is_suspicious_entity(self, attributes: Optional[Dict[str, Any]]) -> bool:
        """
        Check if entity is flagged as suspicious based on attributes.

        Args:
            attributes: Entity attributes

        Returns:
            True if entity should be flagged for review
        """
        if not attributes:
            return False

        # Check duplicate detection for suspicious flag
        duplicate_data = attributes.get("duplicate_detection", {})
        if duplicate_data.get("is_suspicious", False):
            return True

        # Check processing metadata for suspicious indicators
        processing_data = attributes.get("processing", {})
        if processing_data.get("requires_review", False):
            return True

        return False
