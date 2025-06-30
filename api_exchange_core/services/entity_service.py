"""
Entity service for working with the simplified Entity model.

This module provides service methods for creating, reading, and
deleting entities with a focus on versioning and immutability.
It follows the architecture where entities are immutable and changes
are represented by creating new versions rather than updating existing records.
"""

import uuid
from datetime import datetime
from typing import Any, Dict, Generator, List, Optional, Tuple, Union

from pydantic import ValidationError as PydanticValidationError
from sqlalchemy import and_, exists
from sqlalchemy.exc import IntegrityError

from ..context.operation_context import OperationHandler, operation
from ..context.tenant_context import tenant_aware
from ..db.db_entity_models import Entity
from ..db.db_tenant_models import Tenant
from ..exceptions import ErrorCode, ServiceError, ValidationError
from ..schemas import EntityCreate, EntityFilter, EntityRead
from ..utils.hash_config import HashConfig
from ..utils.hash_utils import calculate_entity_hash
from .base_service import SessionManagedService


class EntityService(SessionManagedService):
    """
    Pythonic service for entity management with direct SQLAlchemy access.

    This service handles entity creation, retrieval, and versioning operations
    following the principle that entities are immutable and changes are
    represented by creating new versions rather than updating existing records.

    Uses SQLAlchemy directly - simple, explicit, and efficient.
    """

    def __init__(self, logger=None):
        """
        Initialize the service with global database manager.

        Args:
            logger: Optional logger instance
        """
        super().__init__(logger=logger)
        self.handler = OperationHandler(logger=self.logger)

    def _calculate_content_hash(
        self, content: Optional[Any], hash_config: Optional[HashConfig] = None
    ) -> Optional[str]:
        """Calculate content hash if content is provided."""
        if content is None:
            return None
        return calculate_entity_hash(data=content, config=hash_config)

    @tenant_aware
    @operation(name="entity_service_create")
    def create_entity(
        self,
        external_id: str,
        canonical_type: str,
        source: str,
        content: Optional[Any] = None,
        attributes: Optional[Dict[str, Any]] = None,
        version: int = 1,
        hash_config: Optional[HashConfig] = None,
    ) -> str:
        """
        Create a new entity.

        Args:
            external_id: External identifier from the source system
            canonical_type: Type of canonical data (e.g., 'entity_type_a', 'entity_type_b')
            source: Source system identifier
            content: Optional content to calculate hash from
            attributes: Optional attributes
            version: Version number (defaults to 1)
            hash_config: Optional configuration for hash calculation

        Returns:
            The ID of the created entity

        Raises:
            ValidationError: If validation fails
            ServiceError: If entity already exists or another error occurs
        """
        tenant_id = self._get_current_tenant_id()
        if not tenant_id:
            raise ValidationError("No tenant context set")

        try:
            # Calculate content hash
            content_hash = self._calculate_content_hash(content, hash_config)

            # Use Pydantic schema for validation
            entity_data = EntityCreate(
                tenant_id=tenant_id,
                external_id=external_id,
                canonical_type=canonical_type,
                source=source,
                content_hash=content_hash,
                attributes=attributes or {},
                version=version,
            )

            # Explicit tenant validation - Pythonic approach
            if not self.session.query(exists().where(Tenant.tenant_id == tenant_id)).scalar():
                raise ServiceError(
                    f"Invalid tenant: {tenant_id}",
                    error_code=ErrorCode.CONSTRAINT_VIOLATION,
                    operation="create_entity",
                    tenant_id=tenant_id,
                )

            # Create entity using validated data
            entity = Entity(
                id=str(uuid.uuid4()),
                tenant_id=entity_data.tenant_id,
                external_id=entity_data.external_id,
                canonical_type=entity_data.canonical_type,
                source=entity_data.source,
                content_hash=entity_data.content_hash,
                attributes=entity_data.attributes or {},
                processing_results=[],
                version=entity_data.version,
                created_at=datetime.utcnow(),
                updated_at=datetime.utcnow(),
            )

            self.session.add(entity)
            # Transaction managed by caller

            self.logger.info(
                f"Created entity: id={entity.id}, tenant={tenant_id}, "
                f"external_id={external_id}, source={source}, version={version}"
            )

            return entity.id

        except PydanticValidationError as e:
            # Handle Pydantic validation errors
            raise ValidationError(
                f"Invalid entity data: {str(e)}",
                details={"validation_errors": e.errors()},
            ) from e
        except IntegrityError as e:
            # Transaction managed by caller
            # Check if it's a foreign key constraint (tenant validation)
            if "foreign key constraint" in str(e).lower():
                raise ServiceError(
                    f"Invalid tenant: {tenant_id}",
                    error_code=ErrorCode.CONSTRAINT_VIOLATION,
                    operation="create_entity",
                    tenant_id=tenant_id,
                    cause=e,
                ) from e
            # Check if it's a unique constraint (duplicate)
            elif "unique constraint" in str(e).lower():
                raise ServiceError(
                    f"Entity already exists: external_id={external_id}, source={source}",
                    error_code=ErrorCode.DUPLICATE,
                    operation="create_entity",
                    entity_external_id=external_id,
                    tenant_id=tenant_id,
                    cause=e,
                ) from e
            else:
                raise ServiceError(
                    "Entity creation failed due to data integrity constraints",
                    error_code=ErrorCode.INVALID_DATA,
                    operation="create_entity",
                    entity_external_id=external_id,
                    tenant_id=tenant_id,
                    cause=e,
                ) from e
        except Exception as e:
            # Transaction managed by caller
            self._handle_service_exception("create_entity", e)

    @tenant_aware
    @operation(name="entity_service_create_new_version")
    def create_new_version(
        self,
        external_id: str,
        source: str,
        content: Optional[Any] = None,
        attributes: Optional[Dict[str, Any]] = None,
        hash_config: Optional[HashConfig] = None,
    ) -> Tuple[str, int]:
        """
        Create a new version of an existing entity.

        This follows the entity versioning architecture where entities are immutable
        and changes are represented as new versions rather than updates to existing
        records.

        Args:
            external_id: External ID of the entity
            source: Source system identifier
            content: Optional content to calculate hash from
            attributes: Optional attributes for the new version
            hash_config: Optional configuration for hash calculation

        Returns:
            Tuple of (entity_id, version_number) for the new entity version

        Raises:
            ValidationError: If validation fails
            ServiceError: If trying to version a non-existent entity or another error occurs
        """
        try:
            tenant_id = self._get_current_tenant_id()

            # Get the current max version
            current_max_version = self.get_max_version(external_id, source)
            new_version = current_max_version + 1

            # If this is the first version (max_version was 0), we need an existing entity
            if current_max_version == 0:
                raise ServiceError(
                    f"Cannot create new version for non-existent entity: external_id={external_id}, source={source}",
                    error_code=ErrorCode.NOT_FOUND,
                    operation="create_new_version",
                    tenant_id=tenant_id,
                )

            # Get the canonical_type from the latest entity
            latest_entity_read = self.get_entity_by_external_id(external_id, source)
            if not latest_entity_read:
                raise ServiceError(
                    f"Cannot find latest entity: external_id={external_id}, source={source}",
                    error_code=ErrorCode.NOT_FOUND,
                    operation="create_new_version",
                    tenant_id=tenant_id,
                )

            canonical_type = latest_entity_read.canonical_type

            # Calculate content hash
            content_hash = self._calculate_content_hash(content, hash_config)

            # Create the new entity version directly
            entity = Entity(
                id=str(uuid.uuid4()),
                tenant_id=tenant_id,
                external_id=external_id,
                canonical_type=canonical_type,
                source=source,
                content_hash=content_hash,
                attributes=attributes or {},
                processing_results=[],
                version=new_version,
                created_at=datetime.utcnow(),
                updated_at=datetime.utcnow(),
            )

            self.session.add(entity)
            # Transaction managed by caller

            self.logger.info(
                f"Created new version: id={entity.id}, tenant={tenant_id}, "
                f"external_id={external_id}, source={source}, version={new_version}"
            )

            return entity.id, new_version

        except Exception as e:
            # Transaction managed by caller
            if isinstance(e, ServiceError):
                # Re-wrap ServiceError with create_new_version operation context
                raise ServiceError(
                    str(e),
                    error_code=e.error_code,
                    operation="create_new_version",
                    tenant_id=self._get_current_tenant_id(),
                    cause=e,
                ) from e
            self._handle_service_exception("create_new_version", e)

    @tenant_aware
    @operation(name="entity_service_get_max_version")
    def get_max_version(self, external_id: str, source: str) -> int:
        """
        Get the maximum version number for an entity.

        Args:
            external_id: External identifier from the source system
            source: Source system identifier

        Returns:
            The maximum version number for the entity or 0 if no entity exists

        Raises:
            ServiceError: If an error occurs during repository access
        """
        try:
            tenant_id = self._get_current_tenant_id()

            # Query for the maximum version directly using SQL's MAX function
            from sqlalchemy import func

            max_version = (
                self.session.query(func.max(Entity.version))
                .filter(
                    Entity.external_id == external_id,
                    Entity.source == source,
                    Entity.tenant_id == tenant_id,
                )
                .scalar()
            )

            # If no entities found, return 0 (versions start at 1)
            return max_version or 0

        except Exception as e:
            self._handle_service_exception("get_max_version", e)

    @tenant_aware
    @operation(name="entity_service_get")
    def get_entity(self, entity_id: str) -> EntityRead:
        """
        Get an entity by ID.

        Args:
            entity_id: Entity identifier

        Returns:
            Entity data

        Raises:
            ServiceError: If entity is not found or another error occurs
        """
        try:
            tenant_id = self._get_current_tenant_id()

            # Query entity directly with SQLAlchemy
            entity = (
                self.session.query(Entity)
                .filter(
                    Entity.id == entity_id,
                    Entity.tenant_id == tenant_id,
                )
                .first()
            )

            if entity is None:
                raise ServiceError(
                    f"Entity not found: entity_id={entity_id}",
                    error_code=ErrorCode.NOT_FOUND,
                    entity_id=entity_id,
                    tenant_id=tenant_id,
                )

            # Convert to EntityRead
            return EntityRead.model_validate(entity)

        except Exception as e:
            if isinstance(e, ServiceError):
                raise
            self._handle_service_exception("get_entity", e, entity_id)

    @tenant_aware
    @operation(name="entity_service_get_by_external_id")
    def get_entity_by_external_id(
        self,
        external_id: str,
        source: str,
        version: Optional[int] = None,
        all_versions: bool = False,
    ) -> Union[Optional[EntityRead], List[EntityRead]]:
        """
        Get an entity by external ID and source.

        Args:
            external_id: External identifier
            source: Source system
            version: Optional specific version to retrieve
            all_versions: If True, returns all versions ordered by version number

        Returns:
            If all_versions=True: List of entities
            If version is specified: Specific version or None
            Otherwise: Latest version or None

        Raises:
            ServiceError: If an error occurs
        """
        try:
            tenant_id = self._get_current_tenant_id()

            # Base query with SQLAlchemy
            base_query = self.session.query(Entity).filter(
                Entity.external_id == external_id,
                Entity.source == source,
                Entity.tenant_id == tenant_id,
            )

            # Handle the different retrieval cases
            if all_versions:
                entities = base_query.order_by(Entity.version).all()
                return [EntityRead.model_validate(entity) for entity in entities]
            elif version is not None:
                entity = base_query.filter(Entity.version == version).first()
                return EntityRead.model_validate(entity) if entity else None
            else:
                entity = base_query.order_by(Entity.version.desc()).first()
                return EntityRead.model_validate(entity) if entity else None

        except Exception as e:
            self._handle_service_exception("get_entity_by_external_id", e)

    @tenant_aware
    @operation(name="entity_service_get_by_content_hash")
    def get_entity_by_content_hash(self, content_hash: str, source: str) -> Optional[EntityRead]:
        """
        Get an entity by its content hash and source.

        Args:
            content_hash: Content hash
            source: Source system

        Returns:
            Entity if found, otherwise None

        Raises:
            ServiceError: If an error occurs
        """
        try:
            tenant_id = self._get_current_tenant_id()

            entity = (
                self.session.query(Entity)
                .filter(
                    Entity.content_hash == content_hash,
                    Entity.source == source,
                    Entity.tenant_id == tenant_id,
                )
                .first()
            )

            return EntityRead.model_validate(entity) if entity else None

        except Exception as e:
            self._handle_service_exception("get_entity_by_content_hash", e)

    @tenant_aware
    @operation(name="entity_service_delete")
    def delete_entity(self, entity_id: str) -> bool:
        """
        Delete an entity.

        Args:
            entity_id: Entity identifier

        Returns:
            True if deletion was successful, otherwise False

        Raises:
            ServiceError: If an error occurs
        """
        try:
            tenant_id = self._get_current_tenant_id()

            # Get entity for deletion
            entity = (
                self.session.query(Entity)
                .filter(
                    Entity.id == entity_id,
                    Entity.tenant_id == tenant_id,
                )
                .first()
            )

            if not entity:
                return False

            self.session.delete(entity)
            # Transaction managed by caller

            self.logger.info(f"Deleted entity: id={entity_id}, tenant={tenant_id}")
            return True

        except Exception as e:
            # Transaction managed by caller
            self._handle_service_exception("delete_entity", e, entity_id)

    @tenant_aware
    @operation(name="entity_service_list")
    def list_entities(
        self, filter_data: EntityFilter, limit: int = 100, offset: int = 0
    ) -> Tuple[List[EntityRead], int]:
        """
        List entities with filtering.

        Args:
            filter_data: Filter criteria
            limit: Maximum number of entities to return
            offset: Offset for pagination

        Returns:
            Tuple of (entities list, total count)

        Raises:
            ServiceError: If an error occurs
        """
        try:
            tenant_id = self._get_current_tenant_id()
            query = self.session.query(Entity)

            # Always filter by tenant_id for tenant isolation
            query = query.filter(Entity.tenant_id == tenant_id)

            # Define filter mappings - exact match filters
            exact_filters = {
                "external_id": Entity.external_id,
                "canonical_type": Entity.canonical_type,
                "source": Entity.source,
                "content_hash": Entity.content_hash,
            }

            # Apply exact match filters
            for field_name, entity_field in exact_filters.items():
                value = getattr(filter_data, field_name)
                if value is not None:
                    query = query.filter(entity_field == value)

            # Define range filters
            range_filters = [
                ("created_after", Entity.created_at, lambda f, v: f >= v),
                ("created_before", Entity.created_at, lambda f, v: f <= v),
                ("updated_after", Entity.updated_at, lambda f, v: f >= v),
                ("updated_before", Entity.updated_at, lambda f, v: f <= v),
            ]

            # Apply range filters
            for field_name, entity_field, compare_op in range_filters:
                value = getattr(filter_data, field_name)
                if value is not None:
                    query = query.filter(compare_op(entity_field, value))

            # Get total count before pagination
            total_count = query.count()

            # Apply ordering and pagination
            query = query.order_by(Entity.updated_at.desc())
            query = query.offset(offset).limit(limit)

            # Execute query
            entities = query.all()

            entity_reads = [EntityRead.model_validate(entity) for entity in entities]

            return entity_reads, total_count

        except Exception as e:
            self._handle_service_exception("list_entities", e)

    @tenant_aware
    @operation(name="entity_service_check_existence")
    def check_entity_existence(self, external_id: str, source: str) -> bool:
        """
        Check if an entity exists by external ID and source.

        Args:
            external_id: External identifier
            source: Source system

        Returns:
            True if entity exists, otherwise False

        Raises:
            ServiceError: If an error occurs
        """
        try:
            tenant_id = self._get_current_tenant_id()

            # Check if entity exists using exists() for efficiency
            exists_query = self.session.query(
                exists().where(
                    and_(
                        Entity.external_id == external_id,
                        Entity.source == source,
                        Entity.tenant_id == tenant_id,
                    )
                )
            ).scalar()

            return exists_query

        except Exception as e:
            self._handle_service_exception("check_entity_existence", e)

    @tenant_aware
    @operation(name="entity_service_update_attributes")
    def update_entity_attributes(self, entity_id: str, attributes: Dict[str, Any]) -> bool:
        """
        Update the attributes of an entity.

        This method updates the existing attributes of an entity. It merges the
        provided attributes with the existing ones, rather than replacing them completely.

        Args:
            entity_id: ID of the entity to update
            attributes: New attributes to add or update

        Returns:
            True if update was successful, otherwise False

        Raises:
            ServiceError: If entity is not found or another error occurs
        """
        try:
            tenant_id = self._get_current_tenant_id()

            # Get entity for update
            entity = (
                self.session.query(Entity)
                .filter(
                    Entity.id == entity_id,
                    Entity.tenant_id == tenant_id,
                )
                .first()
            )

            if not entity:
                raise ServiceError(
                    f"Entity not found: {entity_id}",
                    error_code=ErrorCode.NOT_FOUND,
                    operation="update_entity_attributes",
                    entity_id=entity_id,
                    tenant_id=tenant_id,
                )

            # Merge the new attributes with existing ones
            current_attributes = entity.attributes or {}
            updated_attributes = {**current_attributes, **attributes}

            # Update attributes and mark as modified
            entity.attributes = updated_attributes
            entity.updated_at = datetime.utcnow()

            # Flag the JSONB column as modified for SQLAlchemy
            from sqlalchemy.orm.attributes import flag_modified

            flag_modified(entity, "attributes")
            # Transaction managed by caller

            self.logger.info(f"Updated attributes for entity {entity_id}")
            return True

        except Exception as e:
            # Transaction managed by caller
            if isinstance(e, ServiceError):
                raise
            self._handle_service_exception("update_entity_attributes", e, entity_id)

    @tenant_aware
    @operation(name="entity_service_iter")
    def iter_entities(
        self, filter_data: EntityFilter, batch_size: int = 100
    ) -> Generator[EntityRead, None, None]:
        """
        Iterate over entities for memory-efficient processing of large datasets.

        This method is ideal for processing large numbers of entities without
        loading them all into memory at once. It fetches entities in batches
        and yields them one at a time.

        Args:
            filter_data: Filter criteria
            batch_size: Number of entities to fetch per batch (default: 100)

        Yields:
            EntityRead objects one at a time

        Raises:
            ServiceError: If an error occurs during iteration
        """
        offset = 0

        while True:
            # Fetch a batch
            entities, total_count = self.list_entities(filter_data, limit=batch_size, offset=offset)

            if not entities:
                # No more entities
                break

            # Yield entities one at a time
            for entity in entities:
                yield entity

            # Move to next batch
            offset += batch_size

            # Stop if we've processed all entities
            if offset >= total_count:
                break

    @tenant_aware
    @operation(name="entity_service_add_processing_result")
    def add_processing_result(self, entity_id: str, processing_result) -> bool:
        """
        Add a processing result to an entity's processing history.

        Args:
            entity_id: Entity ID
            processing_result: ProcessingResult instance to add to history

        Returns:
            True if successful

        Raises:
            ServiceError: If entity not found or other service error occurs
        """
        try:
            tenant_id = self._get_current_tenant_id()

            # Get entity for update
            entity = (
                self.session.query(Entity)
                .filter(
                    Entity.id == entity_id,
                    Entity.tenant_id == tenant_id,
                )
                .first()
            )

            if not entity:
                raise ServiceError(
                    f"Entity not found: {entity_id}",
                    error_code=ErrorCode.NOT_FOUND,
                    operation="add_processing_result",
                    entity_id=entity_id,
                    tenant_id=tenant_id,
                )

            # Initialize processing_results if None
            if entity.processing_results is None:
                entity.processing_results = []

            # Convert ProcessingResult to dict for JSON storage
            result_dict = processing_result.model_dump(mode="json")

            # Append to processing results array
            entity.processing_results.append(result_dict)

            # Flag the JSONB column as modified for SQLAlchemy
            from sqlalchemy.orm.attributes import flag_modified

            flag_modified(entity, "processing_results")

            # Update the entity's updated_at timestamp
            entity.updated_at = datetime.utcnow()
            # Transaction managed by caller

            return True

        except Exception as e:
            # Transaction managed by caller
            if isinstance(e, ServiceError):
                raise
            self._handle_service_exception("add_processing_result", e, entity_id=entity_id)

    @tenant_aware
    @operation(name="entity_service_get_processing_summary")
    def get_processing_summary(self, entity_id: str) -> Dict[str, Any]:
        """
        Get a summary of an entity's processing history.

        Args:
            entity_id: Entity ID

        Returns:
            Dictionary with processing history summary

        Raises:
            ServiceError: If entity not found or other service error occurs
        """
        try:
            tenant_id = self._get_current_tenant_id()

            # Get entity
            entity = (
                self.session.query(Entity)
                .filter(
                    Entity.id == entity_id,
                    Entity.tenant_id == tenant_id,
                )
                .first()
            )

            if not entity:
                raise ServiceError(
                    f"Entity not found: {entity_id}",
                    error_code=ErrorCode.NOT_FOUND,
                    operation="get_processing_summary",
                    entity_id=entity_id,
                    tenant_id=tenant_id,
                )

            # Initialize summary
            summary = {
                "total_processing_attempts": 0,
                "successful_attempts": 0,
                "failed_attempts": 0,
                "processors_involved": [],
                "has_unrecoverable_failures": False,
                "last_processed_at": None,
            }

            # Process results if they exist
            if entity.processing_results:
                summary["total_processing_attempts"] = len(entity.processing_results)

                processor_names = set()

                for result in entity.processing_results:
                    # Count successes and failures
                    if result.get("success", False):
                        summary["successful_attempts"] += 1
                    else:
                        summary["failed_attempts"] += 1
                        # Check if it's unrecoverable
                        if not result.get("can_retry", True):
                            summary["has_unrecoverable_failures"] = True

                    # Track processor names
                    processor_name = result.get("processing_metadata", {}).get("processor_name")
                    if processor_name:
                        processor_names.add(processor_name)

                    # Track last processed time
                    completed_at = result.get("completed_at")
                    if completed_at:
                        if (
                            not summary["last_processed_at"]
                            or completed_at > summary["last_processed_at"]
                        ):
                            summary["last_processed_at"] = completed_at

                summary["processors_involved"] = sorted(list(processor_names))

            return summary

        except Exception as e:
            if isinstance(e, ServiceError):
                raise
            self._handle_service_exception("get_processing_summary", e, entity_id=entity_id)
