"""
Duplicate detection service for entity processing.

This module provides comprehensive duplicate detection capabilities including
content hash matching, confidence scoring, and detection result management.
"""

from datetime import datetime
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field

from ..context.operation_context import operation
from ..context.tenant_context import tenant_aware
from ..exceptions import ErrorCode, ServiceError
from ..schemas.entity_schema import EntityRead
from ..utils.hash_config import HashConfig
from ..utils.hash_utils import calculate_entity_hash
from ..utils.logger import get_logger


class DuplicateDetectionResult(BaseModel):
    """
    Result of duplicate detection analysis.

    Encapsulates the outcome of duplicate detection including confidence levels,
    similar entity information, and detection reasoning.
    """

    is_duplicate: bool = Field(description="Whether duplicates were found")
    confidence: int = Field(ge=0, le=100, description="Confidence percentage (0-100)")
    reason: str = Field(description="Reason for the detection result")
    similar_entity_ids: List[str] = Field(
        default_factory=list, description="IDs of similar entities"
    )
    similar_entity_external_ids: List[str] = Field(
        default_factory=list, description="External IDs of similar entities"
    )
    content_hash: Optional[str] = Field(default=None, description="Content hash used for detection")
    detection_timestamp: datetime = Field(default_factory=datetime.utcnow)
    is_suspicious: bool = Field(
        default=False, description="Whether result should be flagged for review"
    )
    metadata: Dict[str, Any] = Field(
        default_factory=dict, description="Additional detection metadata"
    )

    def merge_with(self, other: "DuplicateDetectionResult") -> "DuplicateDetectionResult":
        """
        Merge this result with another detection result.

        Uses highest confidence and combines entity lists.

        Args:
            other: Another detection result to merge with

        Returns:
            New merged detection result
        """
        # Use result with higher confidence as base
        if other.confidence > self.confidence:
            base_result = other.model_copy()
            secondary_result = self
        else:
            base_result = self.model_copy()
            secondary_result = other

        # Combine entity lists
        all_entity_ids = list(
            set(base_result.similar_entity_ids + secondary_result.similar_entity_ids)
        )
        all_external_ids = list(
            set(
                base_result.similar_entity_external_ids
                + secondary_result.similar_entity_external_ids
            )
        )

        # Merge metadata
        merged_metadata = {**secondary_result.metadata, **base_result.metadata}

        # Update base result
        base_result.similar_entity_ids = all_entity_ids
        base_result.similar_entity_external_ids = all_external_ids
        base_result.metadata = merged_metadata
        base_result.is_suspicious = base_result.is_suspicious or secondary_result.is_suspicious

        return base_result

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for storage in entity attributes."""
        data = self.model_dump()
        # Convert datetime to ISO string for JSON serialization
        if "detection_timestamp" in data and data["detection_timestamp"]:
            data["detection_timestamp"] = data["detection_timestamp"].isoformat()
        return data

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "DuplicateDetectionResult":
        """Create from dictionary stored in entity attributes."""
        # Convert ISO string back to datetime if needed
        data_copy = data.copy()
        if "detection_timestamp" in data_copy and isinstance(data_copy["detection_timestamp"], str):
            data_copy["detection_timestamp"] = datetime.fromisoformat(
                data_copy["detection_timestamp"]
            )
        return cls.model_validate(data_copy)


class DuplicateDetectionService:
    """
    Service for detecting duplicate entities based on content hash matching.

    Provides configurable duplicate detection with confidence scoring and
    sophisticated matching logic for different scenarios.

    Uses session-per-service pattern - creates its own EntityService.
    """

    def __init__(self):
        """Initialize the duplicate detection service with global database manager."""
        # Import here to avoid circular dependencies
        from ..services.entity_service import EntityService

        self.entity_service = EntityService()
        self.logger = get_logger()

    def close(self):
        """Close the entity service session."""
        if hasattr(self.entity_service, "close"):
            self.entity_service.close()

    @tenant_aware
    @operation(name="duplicate_detection_detect")
    def detect_duplicates(
        self,
        content: Any,
        entity_type: str,
        source: str,
        external_id: Optional[str] = None,
        hash_config: Optional[HashConfig] = None,
        exclude_entity_id: Optional[str] = None,
    ) -> DuplicateDetectionResult:
        """
        Detect duplicate entities based on content hash.

        Args:
            content: Content to calculate hash from
            entity_type: Type of entity being checked
            source: Source system identifier
            external_id: External ID to exclude from same-source matches
            hash_config: Optional hash configuration
            exclude_entity_id: Entity ID to exclude from results

        Returns:
            DuplicateDetectionResult with detection analysis

        Raises:
            ServiceError: If duplicate detection fails
        """
        try:
            # Calculate content hash
            content_hash = calculate_entity_hash(data=content, config=hash_config)

            # Find entities with matching content hash
            matching_entities = self._find_matching_entities(
                content_hash=content_hash,
                source=source,
                exclude_entity_id=exclude_entity_id,
                exclude_external_id=external_id,
            )

            # Analyze results and determine confidence
            return self._analyze_matches(
                matching_entities=matching_entities,
                content_hash=content_hash,
                source=source,
                external_id=external_id,
            )

        except Exception as e:
            self.logger.error(
                f"Duplicate detection failed for {entity_type}",
                extra={
                    "entity_type": entity_type,
                    "source": source,
                    "external_id": external_id,
                    "error": str(e),
                },
                exc_info=True,
            )
            raise ServiceError(
                f"Duplicate detection failed: {str(e)}",
                error_code=ErrorCode.INTERNAL_ERROR,
                operation="detect_duplicates",
                cause=e,
            )

    def _find_matching_entities(
        self,
        content_hash: str,
        source: str,
        exclude_entity_id: Optional[str] = None,
        exclude_external_id: Optional[str] = None,
    ) -> List[EntityRead]:
        """
        Find entities with matching content hash.

        Args:
            content_hash: Content hash to search for
            source: Source system identifier
            exclude_entity_id: Entity ID to exclude from results
            exclude_external_id: External ID to exclude from same-source matches

        Returns:
            List of matching entities
        """
        # Find entities with matching content hash
        matching_entities = []

        # Get entities with same content hash from same source
        same_source_entity = self.entity_service.get_entity_by_content_hash(content_hash, source)
        if same_source_entity:
            # Only exclude by entity ID, not by external ID (we want to detect new versions)
            if exclude_entity_id and same_source_entity.id == exclude_entity_id:
                pass  # Skip this specific entity
            else:
                matching_entities.append(same_source_entity)

        # TODO: Add cross-source matching logic if needed
        # This would involve more sophisticated matching across different sources

        return matching_entities

    def _analyze_matches(
        self,
        matching_entities: List[EntityRead],
        content_hash: str,
        source: str,
        external_id: Optional[str] = None,
    ) -> DuplicateDetectionResult:
        """
        Analyze matching entities and determine confidence levels.

        Args:
            matching_entities: List of entities with matching content hash
            content_hash: Content hash that was matched
            source: Source system identifier
            external_id: External ID being processed

        Returns:
            DuplicateDetectionResult with analysis
        """
        if not matching_entities:
            # No duplicates found
            return DuplicateDetectionResult(
                is_duplicate=False,
                confidence=100,
                reason="NEW",
                content_hash=content_hash,
            )

        # Extract entity information
        entity_ids = [entity.id for entity in matching_entities]
        external_ids = [entity.external_id for entity in matching_entities]

        # Determine confidence based on match type
        same_source_matches = [e for e in matching_entities if e.source == source]

        if same_source_matches:
            # Same source, same content - high confidence duplicate or new version
            if external_id and any(e.external_id == external_id for e in same_source_matches):
                # Same external ID - this is a new version of existing entity
                confidence = 90
                reason = "NEW_VERSION"
                is_duplicate = True
            else:
                # Different external ID but same content - likely duplicate
                confidence = 90
                reason = "SAME_SOURCE_CONTENT_MATCH"
                is_duplicate = True
                is_suspicious = True  # Flag for review since external IDs differ
        else:
            # Cross-source content match - medium confidence
            confidence = 50
            reason = "CROSS_SOURCE_CONTENT_MATCH"
            is_duplicate = True
            is_suspicious = True  # Flag for review

        return DuplicateDetectionResult(
            is_duplicate=is_duplicate,
            confidence=confidence,
            reason=reason,
            similar_entity_ids=entity_ids,
            similar_entity_external_ids=external_ids,
            content_hash=content_hash,
            is_suspicious=locals().get("is_suspicious", False),
            metadata={
                "same_source_matches": len(same_source_matches),
                "total_matches": len(matching_entities),
                "source": source,
            },
        )

    @tenant_aware
    @operation(name="duplicate_detection_get_previous_result")
    def get_previous_detection_result(self, entity_id: str) -> Optional[DuplicateDetectionResult]:
        """
        Get previous duplicate detection result for an entity.

        Args:
            entity_id: Entity ID to get result for

        Returns:
            Previous detection result if available

        Raises:
            ServiceError: If retrieval fails
        """
        try:
            entity = self.entity_service.get_entity(entity_id)
            if not entity or not entity.attributes:
                return None

            duplicate_data = entity.attributes.get("duplicate_detection")
            if not duplicate_data:
                return None

            return DuplicateDetectionResult.from_dict(duplicate_data)

        except ServiceError as e:
            # If entity is not found, return None
            if "not found" in str(e).lower():
                return None
            raise
        except Exception as e:
            self.logger.error(
                f"Failed to get previous detection result for entity {entity_id}",
                extra={"entity_id": entity_id, "error": str(e)},
                exc_info=True,
            )
            raise ServiceError(
                f"Failed to get previous detection result: {str(e)}",
                error_code=ErrorCode.INTERNAL_ERROR,
                operation="get_previous_detection_result",
                entity_id=entity_id,
                cause=e,
            )
