"""
Entity repository for working with the simplified Entity model.

This module provides repository methods for creating, reading, updating,
and deleting entity records.
"""

from typing import Any, Dict, Generator, List, Optional, Tuple, Union

from sqlalchemy import func

from src.context.operation_context import OperationHandler, operation
from src.context.tenant_context import TenantContext, tenant_aware
from src.db.db_entity_models import Entity
from src.exceptions import ErrorCode, RepositoryError, not_found
from src.repositories.base_repository import BaseRepository
from src.schemas.entity_schema import EntityCreate, EntityFilter, EntityRead


class EntityRepository(BaseRepository[Entity]):
    """
    Repository for working with the Entity model.

    This class provides data access methods for the Entity model.
    """

    def __init__(self, session, logger=None):
        """
        Initialize the repository with a database session.

        Args:
            session: SQLAlchemy session for database operations
            logger: Optional logger instance
        """
        super().__init__(session, Entity, logger)
        self.handler = OperationHandler(logger=self.logger)

    @tenant_aware
    @operation(name="entity_get_max_version")
    def get_max_version(self, external_id: str, source: str) -> int:
        """
        Get the maximum version number for an entity.

        Args:
            external_id: External ID of the entity
            source: Source system of the entity

        Returns:
            The maximum version number for the entity or 0 if the entity doesn't exist

        Raises:
            RepositoryError: If a database error occurs
        """
        with self._session_operation("get_max_version", external_id) as session:
            tenant_id = self._get_current_tenant_id()

            # Query for the maximum version directly using SQL's MAX function
            max_version = (
                session.query(func.max(Entity.version))
                .filter(
                    Entity.external_id == external_id,
                    Entity.source == source,
                    Entity.tenant_id == tenant_id,
                )
                .scalar()
            )

            # If no entities found, return 0 (versions start at 1)
            return max_version or 0

    @tenant_aware
    @operation(name="entity_create")
    def create(self, entity_data: EntityCreate) -> str:
        """
        Create a new entity in the database.

        Args:
            entity_data: Entity data to create

        Returns:
            The ID of the created entity

        Raises:
            RepositoryError: If database integrity constraints are violated or another
                database error occurs
        """
        with self._session_operation("create") as session:
            tenant_id = self._get_current_tenant_id()

            # Prepare entity data using Entity's custom create method
            # Build dict with required parameters first
            entity_dict = {
                "tenant_id": tenant_id,
                "external_id": entity_data.external_id,
                "canonical_type": entity_data.canonical_type,
                "source": entity_data.source,
                "version": getattr(entity_data, "version", 1),
            }

            # Add optional parameters only if they're not None
            if entity_data.content_hash is not None:
                entity_dict["content_hash"] = entity_data.content_hash
            if entity_data.attributes is not None:
                entity_dict["attributes"] = entity_data.attributes

            # Use Entity's create method to generate id and timestamps
            temp_entity = Entity.create(**entity_dict)

            # Create the actual entity in the database
            entity = Entity(
                id=temp_entity.id,
                tenant_id=temp_entity.tenant_id,
                external_id=temp_entity.external_id,
                canonical_type=temp_entity.canonical_type,
                source=temp_entity.source,
                content_hash=temp_entity.content_hash,
                attributes=temp_entity.attributes,
                version=temp_entity.version,
                created_at=temp_entity.created_at,
                updated_at=temp_entity.updated_at,
            )

            session.add(entity)
            session.flush()

            # Log while entity is still in session
            self.logger.info(
                f"Created entity: id={entity.id}, tenant={entity.tenant_id}, "
                f"type={entity.canonical_type}, source={entity.source}, "
                f"version={entity.version}"
            )

            return entity.id  # type: ignore[return-value]  # SQLAlchemy Column access

    @tenant_aware
    @operation(name="entity_get_by_id")
    def get_by_id(self, entity_id: str) -> Optional[EntityRead]:
        """
        Get an entity by its ID.

        Args:
            entity_id: Entity ID

        Returns:
            EntityRead if found, None if not found

        Raises:
            RepositoryError: If a database error occurs
        """
        entity = self._get_by_id(entity_id)
        return EntityRead.model_validate(entity) if entity else None

    @tenant_aware
    @operation(name="entity_require_by_id")
    def require_by_id(self, entity_id: str) -> EntityRead:
        """
        Get an entity by ID, raising an exception if not found.

        Args:
            entity_id: Entity ID

        Returns:
            EntityRead (guaranteed to exist)

        Raises:
            RepositoryError: If a database error occurs or the entity is not found
        """
        entity = self.get_by_id(entity_id)

        if not entity:
            tenant_id = self._get_current_tenant_id()
            raise not_found(
                "Entity",
                entity_id=entity_id,
                tenant_id=tenant_id,
            )

        return entity

    @tenant_aware
    @operation(name="entity_get_by_external_id")
    def get_by_external_id(
        self,
        external_id: str,
        source: str,
        version: Optional[int] = None,
        all_versions: bool = False,
    ) -> Union[Optional[EntityRead], List[EntityRead]]:
        """
        Get an entity by its external ID and source.

        Args:
            external_id: External ID
            source: Source system
            version: Optional specific version to retrieve
            all_versions: If True, returns all versions ordered by version number

        Returns:
            If all_versions=True: List of EntityRead
            If version is specified: Specific EntityRead or None
            Otherwise: Latest EntityRead or None

        Raises:
            RepositoryError: If a database error occurs
        """
        with self._session_operation("get_by_external_id", external_id) as session:
            tenant_id = self._get_current_tenant_id()

            # Base query that applies to all cases
            base_query = session.query(Entity).filter(
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

    @tenant_aware
    @operation(name="entity_require_by_external_id")
    def require_by_external_id(
        self,
        external_id: str,
        source: str,
        version: Optional[int] = None,
    ) -> EntityRead:
        """
        Get an entity by external ID and source, raising an exception if not found.

        Args:
            external_id: External ID of the entity
            source: Source system identifier
            version: Specific version to retrieve (defaults to latest)

        Returns:
            EntityRead (guaranteed to exist)

        Raises:
            RepositoryError: If a database error occurs or the entity is not found
        """
        entity = self.get_by_external_id(external_id, source, version)

        if not entity:
            tenant_id = self._get_current_tenant_id()
            raise not_found(
                "Entity",
                tenant_id=tenant_id,
                external_id=external_id,
                source=source,
                version=version,
            )

        return entity

    @tenant_aware
    @operation(name="entity_get_by_content_hash")
    def get_by_content_hash(self, content_hash: str, source: str) -> Optional[EntityRead]:
        """
        Get an entity by its content hash and source.

        Args:
            content_hash: Content hash
            source: Source system

        Returns:
            EntityRead if found, otherwise None

        Raises:
            RepositoryError: If a database error occurs
        """
        with self._session_operation("get_by_content_hash") as session:
            tenant_id = self._get_current_tenant_id()

            entity = (
                session.query(Entity)
                .filter(
                    Entity.content_hash == content_hash,
                    Entity.source == source,
                    Entity.tenant_id == tenant_id,
                )
                .first()
            )

            return EntityRead.model_validate(entity) if entity else None

    @tenant_aware
    @operation(name="entity_create_new_version")
    def create_new_version(
        self,
        external_id: str,
        source: str,
        content_hash: str,
        attributes: Optional[Dict[str, Any]] = None,
        canonical_type: Optional[str] = None,
    ) -> Tuple[str, int]:
        """
        Create a new version of an existing entity.

        This follows the entity versioning architecture where entities are immutable
        and changes are represented as new versions rather than updates to existing
        records.

        Args:
            external_id: External ID of the entity
            source: Source system identifier
            content_hash: Hash of the new content
            attributes: Optional attributes for the new version
            canonical_type: Optional canonical type (if not provided, uses the type from
                latest version)

        Returns:
            Tuple of (entity_id, version_number) for the new entity version

        Raises:
            RepositoryError: If a database error occurs or trying to version an entity
                that doesn't exist
        """
        tenant_id = self._get_current_tenant_id()

        # Get the current max version
        current_max_version = self.get_max_version(external_id, source)
        new_version = current_max_version + 1

        # If this is the first version (max_version was 0), we need canonical_type
        if current_max_version == 0 and not canonical_type:
            raise ValueError(
                "canonical_type is required when creating the first version of an entity"
            )

        # If this is a new version of an existing entity, get the canonical_type if not provided
        if current_max_version > 0 and not canonical_type:
            latest_entity = self.get_by_external_id(external_id, source)
            if not latest_entity:
                raise not_found(
                    "Entity",
                    external_id=external_id,
                    source=source,
                    tenant_id=tenant_id,
                )
            canonical_type = latest_entity.canonical_type

        # Create the new entity version
        entity_data = EntityCreate(
            tenant_id=tenant_id,
            external_id=external_id,
            canonical_type=canonical_type,
            source=source,
            content_hash=content_hash,
            attributes=attributes or {},
            version=new_version,
        )

        # Create the new entity version
        entity_id = self.create(entity_data)

        self.logger.info(
            f"Created new version: id={entity_id}, tenant={tenant_id}, "
            f"external_id={external_id}, source={source}, "
            f"version={new_version} (previous max: {current_max_version})"
        )

        return entity_id, new_version

    @tenant_aware
    @operation(name="entity_delete")
    def delete(self, entity_id: str) -> bool:
        """
        Delete an entity.

        Args:
            entity_id: Entity ID

        Returns:
            True if deletion was successful, otherwise False

        Raises:
            RepositoryError: If a database error occurs
        """
        tenant_id = self._get_current_tenant_id()

        result = self._delete(entity_id)

        if result:
            self.logger.info(f"Deleted entity: id={entity_id}, tenant={tenant_id}")

        return result

    @tenant_aware
    @operation(name="entity_list")
    def list(
        self, filter_data: EntityFilter, limit: int = 100, offset: int = 0
    ) -> Tuple[List[EntityRead], int]:
        """
        List entities with filtering.

        Args:
            filter_data: Filter criteria
            limit: Maximum number of entities to return
            offset: Offset for pagination

        Returns:
            Tuple of (EntityRead list, total count)

        Raises:
            RepositoryError: If a database error occurs
        """
        with self._session_operation("list_entities") as session:
            tenant_id = self._get_current_tenant_id()
            query = session.query(Entity)

            # Always filter by tenant_id for tenant isolation
            query = self._apply_tenant_filter(query, tenant_id)

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

            # Use BaseRepository's pagination helper
            entities, total_count = self._list_with_pagination(
                query, limit, offset, order_by="updated_at", order_dir="desc"
            )

            entity_reads = [EntityRead.model_validate(entity) for entity in entities]

            return entity_reads, total_count

    @tenant_aware
    @operation(name="entity_iter")
    def iter_entities(
        self, filter_data: EntityFilter, batch_size: int = 100
    ) -> Generator[EntityRead, None, None]:
        """
        Iterate over entities in batches for memory-efficient processing.

        This method yields entities one at a time while fetching them in batches,
        making it suitable for processing large datasets without loading everything
        into memory at once.

        Args:
            filter_data: Filter criteria
            batch_size: Number of entities to fetch per batch

        Yields:
            EntityRead objects one at a time

        Raises:
            RepositoryError: If a database error occurs
        """
        offset = 0

        while True:
            # Fetch a batch
            entities, total_count = self.list(filter_data, limit=batch_size, offset=offset)

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
    @operation(name="entity_update_attributes")
    def update_attributes(self, entity_id: str, attributes: Dict[str, Any]) -> bool:
        """
        Update entity attributes.

        Args:
            entity_id: Entity ID
            attributes: New attributes to merge with existing ones

        Returns:
            True if successful

        Raises:
            RepositoryError: If entity not found or database error occurs
        """
        with self._session_operation("update_attributes", entity_id):
            # Get entity for update
            entity = self._get_by_id(entity_id, for_update=True)
            if not entity:
                raise RepositoryError(
                    message=f"Entity not found: {entity_id}",
                    error_code=ErrorCode.NOT_FOUND,
                    context={"entity_id": entity_id},
                )

            # Merge the new attributes with existing ones
            current_attributes = entity.attributes or {}
            updated_attributes = {**current_attributes, **attributes}

            # Use the entity's update_attributes method to update and flag as modified
            entity.update_attributes(updated_attributes)

            # Commit handled by context manager
            return True
