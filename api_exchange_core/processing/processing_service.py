"""
Processing service for entity processing workflow orchestration.

This module provides the main service for orchestrating entity processing workflows
including entity creation, versioning, duplicate detection, and attribute management.
"""

from typing import TYPE_CHECKING, Any, Dict, Optional

from pydantic import BaseModel, Field

from ..context.operation_context import operation
from ..context.tenant_context import TenantContext, tenant_aware
from ..db import EntityStateEnum
from ..db import TransitionTypeEnum
from ..exceptions import ErrorCode, ServiceError, ValidationError
from .duplicate_detection import DuplicateDetectionResult, DuplicateDetectionService
from .entity_attributes import EntityAttributeBuilder
from .processor_config import ProcessorConfig
from ..utils.logger import get_logger

if TYPE_CHECKING:
    from ..services.logging_processing_error_service import LoggingProcessingErrorService
    from ..services.logging_state_tracking_service import LoggingStateTrackingService


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

    def __init__(self, db_manager=None):
        """
        Initialize the processing service with shared session for all services.
        
        All services share the same session to ensure transaction coordination.
        
        Args:
            db_manager: Optional database manager. If provided, uses it to create session.
                       If None, creates from environment variables (for backward compatibility).
        """
        # Import here to avoid circular dependencies
        from ..services.entity_service import EntityService
        from ..services.logging_state_tracking_service import LoggingStateTrackingService
        from ..services.logging_processing_error_service import LoggingProcessingErrorService
        
        # Create shared session for all services
        if db_manager is not None:
            # Use provided database manager
            self.session = db_manager.get_session()
        else:
            # Backward compatibility: create from environment
            self.session = self._create_session()
        
        # Create services - EntityService with session, logging services without
        self.entity_service = EntityService(session=self.session)
        self.state_tracking_service = LoggingStateTrackingService()
        self.error_service = LoggingProcessingErrorService()
        
        # Keep existing services that don't need sessions
        self.duplicate_detection_service = DuplicateDetectionService()
        self.attribute_builder = EntityAttributeBuilder()
        self.logger = get_logger()

    def _create_session(self):
        """Create a new database session using same pattern as SessionManagedService."""
        from sqlalchemy import create_engine
        from sqlalchemy.orm import sessionmaker
        from ..db.db_config import get_production_config
        
        db_config = get_production_config()
        engine = create_engine(db_config.get_connection_string())
        Session = sessionmaker(bind=engine)
        return Session()

    def close_services(self):
        """Close the shared session and clean up resources."""
        try:
            if hasattr(self, 'session') and self.session:
                self.session.close()
        except Exception as e:
            self.logger.warning(f"Error closing session: {e}")
        
        # Close individual services if they have cleanup methods
        if hasattr(self.entity_service, 'close'):
            self.entity_service.close()
        if hasattr(self.state_tracking_service, 'close'):
            self.state_tracking_service.close()
        if hasattr(self.error_service, 'close'):
            self.error_service.close()
        if hasattr(self.duplicate_detection_service, 'close'):
            self.duplicate_detection_service.close()

    def __enter__(self):
        """Support for 'with' statement."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Auto-close services on exit."""
        self.close_services()


    def _get_current_tenant_id(self) -> Optional[str]:
        """Get current tenant ID from context."""
        return TenantContext.get_current_tenant_id()

    def _record_state_transition(
        self,
        entity_id: str,
        config: ProcessorConfig,
        is_new_entity: bool,
        version: int,
        duplicate_result: Optional[DuplicateDetectionResult],
    ) -> None:
        """
        Record state transition for entity processing.

        Args:
            entity_id: ID of the entity
            config: Processor configuration
            is_new_entity: Whether this is a new entity or version update
            version: Entity version number
            duplicate_result: Duplicate detection result, if any
        """
        if not config.enable_state_tracking:
            return

        try:
            # Determine transition states based on entity type
            from_state = EntityStateEnum.RECEIVED if is_new_entity else EntityStateEnum.PROCESSING
            to_state = EntityStateEnum.PROCESSING

            # Create processor data with common fields
            processor_data = {
                "processor_name": config.processor_name,
                "processor_version": config.processor_version,
                "custom_data": {
                    "is_new_entity": is_new_entity,
                    "version": version,
                    "duplicate_detection": (
                        duplicate_result.model_dump(mode="json") if duplicate_result else None
                    ),
                },
            }

            # Generate appropriate notes
            action = "created" if is_new_entity else f"version {version} created"
            notes = (
                f"{'New entity' if is_new_entity else 'Entity'} {action} by {config.processor_name}"
            )

            self.state_tracking_service.record_transition(
                entity_id=entity_id,
                from_state=from_state,
                to_state=to_state,
                actor=config.processor_name,
                transition_type=TransitionTypeEnum.NORMAL,
                processor_data=processor_data,
                notes=notes,
            )
            # Note: No commit here - ProcessorHandler manages the shared session transaction
        except Exception as e:
            entity_type = "new entity" if is_new_entity else "version update"
            self.logger.warning(f"Failed to record state transition for {entity_type}: {e}")

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
    ) -> DuplicateDetectionResult:
        """
        Perform duplicate detection analysis.

        Args:
            content: Entity content
            canonical_type: Entity type
            source: Source system
            external_id: External ID
            config: Processor configuration

        Returns:
            Duplicate detection result (never None - returns default on failure)
        """
        try:
            result: DuplicateDetectionResult = self.duplicate_detection_service.detect_duplicates(
                content=content,
                entity_type=canonical_type,
                source=source,
                external_id=external_id,
                hash_config=config.hash_config,
            )
            return result
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
            self._record_state_transition(
                entity_id=entity_id,
                config=config,
                is_new_entity=True,
                version=1,
                duplicate_result=duplicate_result,
            )

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
            self._record_state_transition(
                entity_id=entity_id,
                config=config,
                is_new_entity=False,
                version=version,
                duplicate_result=duplicate_result,
            )

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

