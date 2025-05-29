"""
Entity service for working with the simplified Entity model.

This module provides service methods for creating, reading, and
deleting entities with a focus on versioning and immutability.
It follows the architecture where entities are immutable and changes
are represented by creating new versions rather than updating existing records.
"""

from typing import Any, Dict, Generator, List, NoReturn, Optional, Tuple, Union

from src.context.operation_context import OperationHandler, operation
from src.context.service_decorators import handle_repository_errors
from src.context.tenant_context import TenantContext, tenant_aware
from src.exceptions import ErrorCode, RepositoryError, ServiceError, ValidationError
from src.repositories.entity_repository import EntityRepository
from src.schemas.entity_schema import EntityCreate, EntityFilter, EntityRead, EntityUpdate
from src.services.base_service import BaseService
from src.utils.hash_config import HashConfig
from src.utils.hash_utils import calculate_entity_hash


class EntityService(BaseService[EntityCreate, EntityRead, EntityUpdate, EntityFilter]):
    """
    Service for working with the simplified Entity model.

    This service provides data access operations for entities, following
    the architectural principle that entities are immutable and changes
    are represented as new versions rather than updates to existing records.
    """

    repository: EntityRepository  # Type annotation for mypy

    def __init__(self, entity_repository: EntityRepository, logger=None):
        """
        Initialize the service with a repository.

        Args:
            entity_repository: Entity repository for database operations
            logger: Optional logger instance
        """
        super().__init__(
            repository=entity_repository,
            read_schema_class=EntityRead,
            logger=logger,
        )
        self.handler = OperationHandler(logger=self.logger)

    def _calculate_content_hash(
        self, content: Optional[Any], hash_config: Optional[HashConfig] = None
    ) -> Optional[str]:
        """Calculate content hash if content is provided."""
        if content is None:
            return None
        return calculate_entity_hash(data=content, config=hash_config)

    def _prepare_entity_data(
        self,
        external_id: str,
        canonical_type: str,
        source: str,
        content_hash: Optional[str],
        attributes: Optional[Dict[str, Any]],
        version: int,
        tenant_id: str,
    ) -> EntityCreate:
        """Prepare entity data for creation."""
        return EntityCreate(
            tenant_id=tenant_id,
            external_id=external_id,
            canonical_type=canonical_type,
            source=source,
            content_hash=content_hash,
            attributes=attributes or {},
            version=version,
        )

    @staticmethod
    def _handle_repo_error(
        e: RepositoryError, operation_name: str, entity_id: Optional[str] = None
    ) -> NoReturn:
        """
        Convert repository errors to service errors with appropriate context.

        Args:
            e: The original repository exception
            operation_name: Name of the operation that failed
            entity_id: Optional entity ID involved in the operation

        Raises:
            ServiceError: With appropriate error code based on the original error
        """
        tenant_id = TenantContext.get_current_tenant_id()

        # Check if it's a not found error by checking the error code
        if hasattr(e, "error_code") and e.error_code == ErrorCode.NOT_FOUND:
            raise ServiceError(
                f"Entity not found: entity_id={entity_id}",
                error_code=ErrorCode.NOT_FOUND,
                operation=operation_name,
                entity_id=entity_id,
                tenant_id=tenant_id,
                cause=e,
            )

        # Check if it's a duplicate error by checking the error code
        if hasattr(e, "error_code") and e.error_code == ErrorCode.DUPLICATE:
            raise ServiceError(
                f"Duplicate entity: entity_id={entity_id}",
                error_code=ErrorCode.DUPLICATE,
                operation=operation_name,
                entity_id=entity_id,
                tenant_id=tenant_id,
                cause=e,
            )

        # General repository error
        raise ServiceError(
            f"Error in {operation_name}: {str(e)}",
            operation=operation_name,
            entity_id=entity_id,
            tenant_id=tenant_id,
        ) from e

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
        tenant_id = TenantContext.get_current_tenant_id()
        if not tenant_id:
            raise ValidationError("No tenant context set")

        try:
            # Calculate content hash
            content_hash = self._calculate_content_hash(content, hash_config)

            # Prepare entity data
            entity_data = self._prepare_entity_data(
                external_id=external_id,
                canonical_type=canonical_type,
                source=source,
                content_hash=content_hash,
                attributes=attributes,
                version=version,
                tenant_id=tenant_id,
            )

            # Create the entity
            entity_id = self.repository.create(entity_data)

            self.logger.info(
                f"Created entity: id={entity_id}, tenant={tenant_id}, "
                f"external_id={external_id}, source={source}, version={version}"
            )

            return entity_id

        except RepositoryError as e:
            self._handle_repo_error(e, "create_entity")
        except Exception as e:
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
        tenant_id = TenantContext.get_current_tenant_id()

        try:
            # Calculate content hash
            content_hash = self._calculate_content_hash(content, hash_config)

            # Create the new version in the repository
            entity_id, version = self.repository.create_new_version(
                external_id=external_id,
                source=source,
                content_hash=content_hash,
                attributes=attributes,
            )

            self.logger.info(
                f"Created new version: id={entity_id}, tenant={tenant_id}, "
                f"external_id={external_id}, source={source}, version={version}"
            )

            return entity_id, version

        except RepositoryError as e:
            self._handle_repo_error(e, "create_new_version")
        except Exception as e:
            self._handle_service_exception("create_new_version", e)

    @tenant_aware
    @operation(name="entity_service_get_max_version")
    @handle_repository_errors("get_max_version")
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
        # Get max version from repository
        return self.repository.get_max_version(external_id, source)

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
            # Get the entity
            entity = self.repository.get_by_id(entity_id)

            # Check if entity exists
            if entity is None:
                tenant_id = TenantContext.get_current_tenant_id()
                raise ServiceError(
                    f"Entity not found: entity_id={entity_id}",
                    error_code=ErrorCode.NOT_FOUND,
                    entity_id=entity_id,
                    tenant_id=tenant_id or "unknown",
                )

            # Return the EntityRead object directly from repository
            return entity

        except RepositoryError as e:
            self._handle_repo_error(e, "get_entity", entity_id)

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
            # Get the entity/entities
            result = self.repository.get_by_external_id(
                external_id=external_id, source=source, version=version, all_versions=all_versions
            )

            if result is None:
                return None

            # Process based on what kind of result we got
            if all_versions:
                # List of EntityRead objects - return directly
                return result
            else:
                # Single EntityRead object - return directly
                return result

        except RepositoryError as e:
            self._handle_repo_error(e, "get_entity_by_external_id")
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
            # Get the entity
            entity = self.repository.get_by_content_hash(content_hash, source)

            if not entity:
                return None

            # Return EntityRead object directly from repository
            return entity

        except RepositoryError as e:
            self._handle_repo_error(e, "get_entity_by_content_hash")
        except Exception as e:
            self._handle_service_exception("get_entity_by_content_hash", e)

    @tenant_aware
    @operation(name="entity_service_delete")
    @handle_repository_errors("delete_entity")
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
        # Delete the entity
        return self.repository.delete(entity_id)

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
            # List entities
            entities, total_count = self.repository.list(filter_data, limit, offset)

            # Return EntityRead objects directly from repository
            return entities, total_count

        except RepositoryError as e:
            self._handle_repo_error(e, "list_entities")

    @tenant_aware
    @operation(name="entity_service_check_existence")
    @handle_repository_errors("check_entity_existence")
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
        # Check if entity exists
        entity = self.repository.get_by_external_id(external_id, source)
        return entity is not None

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
        TenantContext.get_current_tenant_id()  # Validate tenant context

        try:
            # Use repository to update attributes
            success = self.repository.update_attributes(entity_id, attributes)

            self.logger.info(f"Updated attributes for entity {entity_id}")
            return success

        except RepositoryError as e:
            self._handle_repo_error(e, "update_entity_attributes", entity_id)
        except Exception as e:
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
        try:
            # Delegate to repository's iterator
            yield from self.repository.iter_entities(filter_data, batch_size)
        except RepositoryError as e:
            self._handle_repo_error(e, "iter_entities")
        except Exception as e:
            self._handle_service_exception("iter_entities", e)
