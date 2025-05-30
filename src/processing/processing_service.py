"""
Processing service for entity processing workflow orchestration.

This module provides the main service for orchestrating entity processing workflows
including entity creation, versioning, duplicate detection, and attribute management.
"""

from typing import Any, Dict, Optional

from pydantic import BaseModel, Field

from src.context.operation_context import operation
from src.context.tenant_context import tenant_aware
from src.db.db_base import EntityStateEnum
from src.db.db_state_transition_models import TransitionTypeEnum
from src.exceptions import ErrorCode, ServiceError, ValidationError
from src.processing.duplicate_detection import DuplicateDetectionResult, DuplicateDetectionService
from src.processing.entity_attributes import EntityAttributeBuilder
from src.processing.processor_config import ProcessorConfig
from src.repositories.entity_repository import EntityRepository
from src.services.entity_service import EntityService
from src.utils.logger import get_logger


class ProcessingResult(BaseModel):
    """
    Result of entity processing operation.

    Contains information about the processed entity including IDs, version,
    and processing metadata.
    """

    entity_id: str = Field(description="ID of the processed entity")
    entity_version: int = Field(description="Version number of the processed entity")
    external_id: str = Field(description="External ID of the entity")
    content_changed: bool = Field(description="Whether content changed from previous version")
    is_new_entity: bool = Field(description="Whether this is a completely new entity")
    duplicate_detection_result: Optional[DuplicateDetectionResult] = Field(
        default=None, description="Result of duplicate detection analysis"
    )
    processing_metadata: Dict[str, Any] = Field(
        default_factory=dict, description="Additional processing metadata"
    )


class ProcessingService:
    """
    Service for orchestrating entity processing workflows.

    Coordinates entity creation, versioning, duplicate detection, and attribute
    management according to processor configuration.
    """

    def __init__(
        self,
        entity_service: EntityService,
        entity_repository: EntityRepository,
        duplicate_detection_service: DuplicateDetectionService,
        attribute_builder: EntityAttributeBuilder,
    ):
        """
        Initialize the processing service.

        Args:
            entity_service: Service for entity operations
            entity_repository: Repository for direct entity access
            duplicate_detection_service: Service for duplicate detection
            attribute_builder: Service for building entity attributes
        """
        self.entity_service = entity_service
        self.entity_repository = entity_repository
        self.duplicate_detection_service = duplicate_detection_service
        self.attribute_builder = attribute_builder
        self.logger = get_logger()
        
        # Initialize state tracking and error services (will be injected)
        self.state_tracking_service = None
        self.processing_error_service = None

    def set_state_tracking_service(self, service):
        """Set the state tracking service for this processing service."""
        self.state_tracking_service = service

    def set_processing_error_service(self, service):
        """Set the processing error service for this processing service."""
        self.processing_error_service = service

    def _get_current_tenant_id(self) -> Optional[str]:
        """Get current tenant ID from context."""
        from src.context.tenant_context import TenantContext
        return TenantContext.get_current_tenant_id()

    @tenant_aware
    @operation(name="processing_service_process_entity")
    def process_entity(
        self,
        external_id: str,
        canonical_type: str,
        source: str,
        content: Any,
        config: ProcessorConfig,
        custom_attributes: Optional[Dict[str, Any]] = None,
        source_metadata: Optional[Dict[str, Any]] = None,
    ) -> ProcessingResult:
        """
        Process an entity through the complete workflow.

        Handles entity creation/versioning, duplicate detection, and attribute management
        according to the provided processor configuration.

        Args:
            external_id: External identifier from source system
            canonical_type: Type of canonical data
            source: Source system identifier
            content: Entity content for processing
            config: Processor configuration
            custom_attributes: Custom attributes to include
            source_metadata: Metadata from source system

        Returns:
            ProcessingResult with processing outcome

        Raises:
            ValidationError: If validation fails
            ServiceError: If processing fails
        """
        try:
            self.logger.info(
                f"Processing entity: {external_id}",
                extra={
                    "external_id": external_id,
                    "canonical_type": canonical_type,
                    "source": source,
                    "processor": config.processor_name,
                    "is_source": config.is_source_processor,
                },
            )

            # Perform duplicate detection if enabled
            duplicate_result = None
            if config.enable_duplicate_detection:
                duplicate_result = self._perform_duplicate_detection(
                    content=content,
                    canonical_type=canonical_type,
                    source=source,
                    external_id=external_id,
                    config=config,
                )

            # Determine processing strategy based on processor type and duplicate detection
            if config.is_source_processor:
                return self._process_source_entity(
                    external_id=external_id,
                    canonical_type=canonical_type,
                    source=source,
                    content=content,
                    config=config,
                    duplicate_result=duplicate_result,
                    custom_attributes=custom_attributes,
                    source_metadata=source_metadata,
                )
            else:
                return self._process_existing_entity(
                    external_id=external_id,
                    source=source,
                    content=content,
                    config=config,
                    duplicate_result=duplicate_result,
                    custom_attributes=custom_attributes,
                    source_metadata=source_metadata,
                )

        except Exception as e:
            self.logger.error(
                f"Entity processing failed: {external_id}",
                extra={
                    "external_id": external_id,
                    "source": source,
                    "processor": config.processor_name,
                    "error": str(e),
                },
                exc_info=True,
            )
            
            # Note: Pre-creation errors are not recorded in the database since entity_id is required
            # These errors are logged for debugging purposes
            
            raise ServiceError(
                f"Entity processing failed: {str(e)}",
                error_code=ErrorCode.INTERNAL_ERROR,
                operation="process_entity",
                cause=e,
            )

    def _perform_duplicate_detection(
        self,
        content: Any,
        canonical_type: str,
        source: str,
        external_id: str,
        config: ProcessorConfig,
    ) -> Optional[DuplicateDetectionResult]:
        """
        Perform duplicate detection analysis.

        Args:
            content: Entity content
            canonical_type: Entity type
            source: Source system
            external_id: External ID
            config: Processor configuration

        Returns:
            Duplicate detection result or None if detection fails
        """
        try:
            return self.duplicate_detection_service.detect_duplicates(
                content=content,
                entity_type=canonical_type,
                source=source,
                external_id=external_id,
                hash_config=config.hash_config,
            )
        except Exception as e:
            self.logger.warning(
                f"Duplicate detection failed for {external_id}: {e}",
                extra={
                    "external_id": external_id,
                    "source": source,
                    "error": str(e),
                },
            )

            if config.fail_on_duplicate_detection_error:
                raise

            # Return default result if detection fails and we're not failing on errors
            return DuplicateDetectionResult(
                is_duplicate=False,
                confidence=0,
                reason="DETECTION_FAILED",
                metadata={"error": str(e)},
            )

    def _process_source_entity(
        self,
        external_id: str,
        canonical_type: str,
        source: str,
        content: Any,
        config: ProcessorConfig,
        duplicate_result: Optional[DuplicateDetectionResult],
        custom_attributes: Optional[Dict[str, Any]],
        source_metadata: Optional[Dict[str, Any]],
    ) -> ProcessingResult:
        """
        Process entity as a source processor (can create new entities).

        Args:
            external_id: External ID
            canonical_type: Entity type
            source: Source system
            content: Entity content
            config: Processor configuration
            duplicate_result: Duplicate detection result
            custom_attributes: Custom attributes
            source_metadata: Source metadata

        Returns:
            ProcessingResult with outcome
        """
        # Check if entity already exists
        existing_entity = self.entity_service.get_entity_by_external_id(
            external_id=external_id,
            source=source,
        )

        # Build attributes
        attributes = self.attribute_builder.build(
            duplicate_detection_result=duplicate_result,
            custom_attributes=custom_attributes,
            processor_name=config.processor_name,
            source_metadata=source_metadata,
            content_changed=True,  # Assume content changed for source processors
        )

        if existing_entity is None:
            # Create new entity
            entity_id = self.entity_service.create_entity(
                external_id=external_id,
                canonical_type=canonical_type,
                source=source,
                content=content,
                attributes=attributes,
                hash_config=config.hash_config,
            )
            
            # Record state transition for new entity
            if self.state_tracking_service and config.enable_state_tracking:
                try:
                    self.state_tracking_service.record_transition(
                        entity_id=entity_id,
                        from_state=EntityStateEnum.RECEIVED,
                        to_state=EntityStateEnum.PROCESSING,
                        actor=config.processor_name,
                        transition_type=TransitionTypeEnum.NORMAL,
                        processor_data={
                            "processor_name": config.processor_name,
                            "processor_version": config.processor_version,
                            "custom_data": {
                                "is_new_entity": True,
                                "version": 1,
                                "duplicate_detection": duplicate_result.model_dump() if duplicate_result else None
                            }
                        },
                        notes=f"New entity created by {config.processor_name}"
                    )
                except Exception as e:
                    self.logger.warning(f"Failed to record state transition for new entity: {e}")

            return ProcessingResult(
                entity_id=entity_id,
                entity_version=1,
                external_id=external_id,
                content_changed=True,
                is_new_entity=True,
                duplicate_detection_result=duplicate_result,
                processing_metadata={"processor": config.processor_name},
            )
        else:
            # Create new version of existing entity
            entity_id, version = self.entity_service.create_new_version(
                external_id=external_id,
                source=source,
                content=content,
                attributes=attributes,
                hash_config=config.hash_config,
            )
            
            # Record state transition for version update
            if self.state_tracking_service and config.enable_state_tracking:
                try:
                    self.state_tracking_service.record_transition(
                        entity_id=entity_id,
                        from_state=EntityStateEnum.PROCESSING,
                        to_state=EntityStateEnum.PROCESSING,
                        actor=config.processor_name,
                        transition_type=TransitionTypeEnum.NORMAL,
                        processor_data={
                            "processor_name": config.processor_name,
                            "processor_version": config.processor_version,
                            "custom_data": {
                                "is_new_entity": False,
                                "version": version,
                                "duplicate_detection": duplicate_result.model_dump() if duplicate_result else None
                            }
                        },
                        notes=f"Entity version {version} created by {config.processor_name}"
                    )
                except Exception as e:
                    self.logger.warning(f"Failed to record state transition for version update: {e}")

            return ProcessingResult(
                entity_id=entity_id,
                entity_version=version,
                external_id=external_id,
                content_changed=True,  # New version implies content change
                is_new_entity=False,
                duplicate_detection_result=duplicate_result,
                processing_metadata={"processor": config.processor_name},
            )

    def _process_existing_entity(
        self,
        external_id: str,
        source: str,
        content: Any,
        config: ProcessorConfig,
        duplicate_result: Optional[DuplicateDetectionResult],
        custom_attributes: Optional[Dict[str, Any]],
        source_metadata: Optional[Dict[str, Any]],
    ) -> ProcessingResult:
        """
        Process existing entity (non-source processor).

        Args:
            external_id: External ID
            source: Source system
            content: Entity content
            config: Processor configuration
            duplicate_result: Duplicate detection result
            custom_attributes: Custom attributes
            source_metadata: Source metadata

        Returns:
            ProcessingResult with outcome

        Raises:
            ValidationError: If entity doesn't exist
        """
        # Get existing entity
        existing_entity = self.entity_service.get_entity_by_external_id(
            external_id=external_id,
            source=source,
        )

        if existing_entity is None:
            raise ValidationError(
                f"Entity not found for non-source processor: {external_id}",
                error_code=ErrorCode.NOT_FOUND,
                field="external_id",
                value=external_id,
            )

        # Update entity attributes if configured
        if config.update_attributes_on_duplicate and custom_attributes:
            # Merge with existing attributes
            updated_attributes = self.attribute_builder.merge_attributes(
                existing_attributes=existing_entity.attributes,
                new_attributes=custom_attributes,
                preserve_keys=config.preserve_attribute_keys,
            )

            # Update duplicate detection if available
            if duplicate_result:
                updated_attributes = self.attribute_builder.update_duplicate_detection(
                    existing_attributes=updated_attributes,
                    new_detection_result=duplicate_result,
                    merge_results=True,
                )

            # Update entity attributes
            self.entity_service.update_entity_attributes(
                entity_id=existing_entity.id,
                attributes=updated_attributes,
            )

        return ProcessingResult(
            entity_id=existing_entity.id,
            entity_version=existing_entity.version,
            external_id=external_id,
            content_changed=False,  # Non-source processors don't change content
            is_new_entity=False,
            duplicate_detection_result=duplicate_result,
            processing_metadata={"processor": config.processor_name},
        )
