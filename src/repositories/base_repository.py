"""
Base repository implementation with common functionality for all repositories.

This module provides a base class with shared methods and patterns to
reduce duplication across repository implementations.
"""

import logging
from contextlib import contextmanager
from typing import Any, Dict, Generic, List, NoReturn, Optional, Tuple, Type, TypeVar

from sqlalchemy import asc, desc, select
from sqlalchemy.exc import IntegrityError, SQLAlchemyError
from sqlalchemy.orm import Session

from src.context.tenant_context import TenantContext
from src.db.db_base import BaseModel
from src.exceptions import ErrorCode, RepositoryError, duplicate, not_found

# Type variable for entity models
T = TypeVar("T", bound=BaseModel)


class BaseRepository(Generic[T]):
    """Base repository with common functionality for all repositories."""

    def __init__(
        self,
        session: Session,
        entity_class: Type[T],
        logger: Optional[logging.Logger] = None,
    ):
        """
        Initialize the base repository.

        Args:
            session: SQLAlchemy session for database operations
            entity_class: SQLAlchemy model class this repository handles
            logger: Optional logger instance
        """
        self.session = session
        self.entity_class = entity_class
        self.logger = logger or logging.getLogger(f"{__name__}.{self.__class__.__name__}")
        self.entity_name = entity_class.__name__

    def _get_current_tenant_id(self) -> str:
        """Get current tenant ID from context."""
        tenant_id = TenantContext.get_current_tenant_id()
        if not tenant_id:
            raise ValueError("No tenant context set - ensure tenant_context is active")
        return tenant_id

    def _handle_db_error(
        self,
        e: Exception,
        operation_name: str,
        entity_id: Optional[str] = None,
        **context: Any,
    ) -> NoReturn:
        """
        Handle database errors with sophisticated error mapping.

        Args:
            e: The original exception
            operation_name: Name of the operation that failed
            entity_id: Optional entity ID involved in the operation
            **context: Additional context for the error

        Raises:
            RepositoryError: With appropriate error code and context
        """
        # If it's already a RepositoryError, just re-raise it to preserve the error code
        if isinstance(e, RepositoryError):
            raise e

        # Get current tenant if available
        tenant_id = context.get("tenant_id") or TenantContext.get_current_tenant_id()

        # Add standard context
        error_context = {
            "operation_name": operation_name,
            "entity_type": self.entity_name,
            "tenant_id": tenant_id,
            **context,
        }
        if entity_id:
            error_context["entity_id"] = entity_id

        # Handle IntegrityError specifically
        if isinstance(e, IntegrityError):
            error_message = str(e.orig).lower() if hasattr(e, "orig") else str(e).lower()

            # Foreign key constraint violation
            if "foreign key constraint" in error_message:
                self.logger.warning(
                    f"Foreign key constraint violation in {operation_name}: {str(e)}",
                    extra=error_context,
                )
                raise RepositoryError(
                    f"Invalid tenant or reference in {self.entity_name}: {str(e)}",
                    error_code=ErrorCode.CONSTRAINT_VIOLATION,
                    cause=e,
                    **error_context,
                )

            # Unique constraint violation (duplicate)
            elif "unique constraint" in error_message or "duplicate" in error_message:
                self.logger.warning(
                    f"Duplicate {self.entity_name} in {operation_name}: {str(e)}",
                    extra=error_context,
                )
                # Use the duplicate factory function
                raise duplicate(
                    resource_type=self.entity_name,
                    cause=e,
                    **error_context,
                )

            # Other integrity constraints
            else:
                self.logger.error(
                    f"Integrity constraint violation in {operation_name}: {str(e)}",
                    extra=error_context,
                )
                raise RepositoryError(
                    f"Database constraint violation for {self.entity_name}: {str(e)}",
                    error_code=ErrorCode.CONSTRAINT_VIOLATION,
                    cause=e,
                    **error_context,
                )

        # Handle other SQLAlchemy errors
        elif isinstance(e, SQLAlchemyError):
            self.logger.error(
                f"Database error in {operation_name}: {str(e)}",
                extra=error_context,
            )
            raise RepositoryError(
                f"Database error for {self.entity_name}: {str(e)}",
                error_code=ErrorCode.DATABASE_ERROR,
                cause=e,
                **error_context,
            )

        # Handle unexpected errors
        else:
            self.logger.error(
                f"Unexpected error in {operation_name}: {str(e)}",
                extra=error_context,
            )
            raise RepositoryError(
                f"Unexpected error for {self.entity_name}: {str(e)}",
                error_code=ErrorCode.INTERNAL_ERROR,
                cause=e,
                **error_context,
            )

    @contextmanager
    def _session_operation(
        self, operation_name: str, entity_id: Optional[str] = None, is_read_only: bool = False
    ):
        """
        Context manager for operations on existing session with error handling.

        This is for the new session-based repository pattern where the repository
        receives a session and doesn't manage session lifecycle.

        Args:
            operation_name: Name of the operation for error reporting
            entity_id: Optional ID of the entity being operated on
            is_read_only: If True, skip flush to avoid transaction conflicts on read operations

        Yields:
            The existing session

        Raises:
            RepositoryError: If there's a database error
        """
        try:
            yield self.session
            # NOTE: We don't commit here - that's handled by the service layer
            # Only flush for write operations to catch constraint violations early
            # Read operations don't need flush and it can cause transaction conflicts
            if not is_read_only:
                self.session.flush()
        except Exception as e:
            # NOTE: We don't rollback here - that's handled by the service layer
            self._handle_db_error(e, operation_name, entity_id)

    def _entity_to_dict(self, entity: T) -> Dict[str, Any]:
        """
        Convert an entity to a dictionary.

        This method should be overridden by subclasses to provide
        entity-specific conversion logic.

        Args:
            entity: Entity to convert

        Returns:
            Dictionary representation of the entity
        """
        result = {
            "id": entity.id,
            "created_at": entity.created_at,
            "updated_at": entity.updated_at,
        }

        # Add tenant_id if the entity has it
        if hasattr(entity, "tenant_id"):
            result["tenant_id"] = entity.tenant_id

        return result

    # ==================== BASE CRUD METHODS ====================

    def _get_by_id(self, entity_id: str, for_update: bool = False) -> Optional[T]:
        """
        Get an entity by its ID using existing session.

        Args:
            entity_id: The entity's ID
            for_update: Whether to lock the row for update

        Returns:
            The entity or None if not found
        """
        query = select(self.entity_class).where(self.entity_class.id == entity_id)

        # Apply tenant filter if entity has tenant_id
        if hasattr(self.entity_class, "tenant_id"):
            tenant_id = self._get_current_tenant_id()
            if tenant_id:
                query = query.where(
                    self.entity_class.tenant_id == tenant_id  # type: ignore[attr-defined]
                )

        if for_update:
            query = query.with_for_update()

        result = self.session.execute(query)
        entity = result.scalar_one_or_none()

        if entity:
            self.session.refresh(entity)

        return entity  # type: ignore[no-any-return]

    def _delete(self, entity_id: str, soft_delete: bool = True) -> bool:
        """
        Delete an entity by its ID using existing session.

        Args:
            entity_id: The entity's ID
            soft_delete: Whether to soft delete (if supported) or hard delete

        Returns:
            True if deleted, False if not found

        Raises:
            RepositoryError: If deletion fails
        """
        entity = self._get_by_id(entity_id, for_update=True)

        if not entity:
            return False

        if soft_delete and hasattr(entity, "deleted_at"):
            # Soft delete - just mark as deleted
            from datetime import datetime, timezone

            entity.deleted_at = datetime.now(timezone.utc)
        else:
            # Hard delete
            self.session.delete(entity)

        self.session.flush()

        self.logger.info(
            f"{'Soft' if soft_delete else 'Hard'} deleted {self.entity_name} "
            f"with ID: {entity_id}",
            extra={"entity_id": entity_id, "entity_type": self.entity_name},
        )

        return True

    def _delete_batch(self, entity_ids: List[str], soft_delete: bool = True) -> int:
        """
        Delete multiple entities in a single transaction using existing session.

        Args:
            entity_ids: List of entity IDs to delete
            soft_delete: Whether to soft delete (if supported) or hard delete

        Returns:
            Number of entities deleted

        Raises:
            RepositoryError: If deletion fails
        """
        if not entity_ids:
            return 0

        # Build query for all entities
        query = select(self.entity_class).where(self.entity_class.id.in_(entity_ids))

        # Apply tenant filter if needed
        if hasattr(self.entity_class, "tenant_id"):
            tenant_id = self._get_current_tenant_id()
            if tenant_id:
                query = query.where(
                    self.entity_class.tenant_id == tenant_id  # type: ignore[attr-defined]
                )

        result = self.session.execute(query)
        entities = result.scalars().all()

        deleted_count = len(entities)

        if soft_delete and hasattr(self.entity_class, "deleted_at"):
            # Soft delete all
            from datetime import datetime, timezone

            now = datetime.now(timezone.utc)
            for entity in entities:
                entity.deleted_at = now
        else:
            # Hard delete all
            for entity in entities:
                self.session.delete(entity)

        self.session.flush()

        self.logger.info(
            f"{'Soft' if soft_delete else 'Hard'} deleted {deleted_count} "
            f"{self.entity_name} entities in batch",
            extra={
                "entity_type": self.entity_name,
                "requested_count": len(entity_ids),
                "deleted_count": deleted_count,
                "entity_ids": [e.id for e in entities],
            },
        )

        return deleted_count

    # ==================== UTILITY METHODS ====================

    def _apply_tenant_filter(self, query, tenant_id: str):
        """
        Apply tenant filter to a query.

        Args:
            query: SQLAlchemy query object
            tenant_id: Tenant ID to filter by

        Returns:
            Query with tenant filter applied
        """
        return query.where(self.entity_class.tenant_id == tenant_id)  # type: ignore[attr-defined]

    @staticmethod
    def _apply_pagination(query, limit: int = 100, offset: int = 0):
        """
        Apply pagination to a query.

        Args:
            query: SQLAlchemy query object
            limit: Maximum number of results to return
            offset: Number of results to skip

        Returns:
            Query with pagination applied
        """
        return query.offset(offset).limit(limit)

    def _apply_ordering(self, query, sort_by: Optional[str] = None, sort_direction: str = "desc"):
        """
        Apply ordering to a query.

        Args:
            query: SQLAlchemy query object
            sort_by: Field to sort by
            sort_direction: Sort direction ('asc' or 'desc')

        Returns:
            Query with ordering applied
        """
        # Default sort is by created_at
        sort_field = getattr(self.entity_class, sort_by or "created_at")

        if sort_direction.lower() == "asc":
            return query.order_by(asc(sort_field))
        return query.order_by(desc(sort_field))

    def _prepare_create_data(
        self, create_schema, additional_fields: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        Prepare data for creation with tenant validation.

        Args:
            create_schema: Pydantic schema with create data
            additional_fields: Optional additional fields to add/override

        Returns:
            Dictionary ready for database creation

        Raises:
            RepositoryError: If tenant_id is not available
        """
        # Get tenant_id from context
        tenant_id = self._get_current_tenant_id()

        if not tenant_id:
            raise RepositoryError(
                "Tenant ID must be provided in context",
                error_code=ErrorCode.VALIDATION_FAILED,
            )

        # Convert schema to dict
        data_dict = create_schema.model_dump()

        # Ensure tenant_id is set
        data_dict["tenant_id"] = tenant_id

        # Apply any additional fields
        if additional_fields:
            data_dict.update(additional_fields)

        return data_dict  # type: ignore[no-any-return]

    def _build_filter_map(
        self, filter_params, field_mapping: Dict[str, str]
    ) -> Tuple[Dict[str, Any], Dict[str, str]]:
        """
        Build filters and filter_map from filter parameters.

        Args:
            filter_params: Pydantic model or object with filter attributes
            field_mapping: Dictionary mapping param names to database field names

        Returns:
            Tuple of (filters dict, filter_map dict)
        """
        filters = {}
        filter_map = {}

        # Process field mappings
        for param_name, db_field in field_mapping.items():
            value = getattr(filter_params, param_name, None)
            if value is not None:
                filters[param_name] = value
                filter_map[param_name] = db_field

        # Handle common date filters if they exist
        date_fields = ["created_after", "created_before", "updated_after", "updated_before"]
        for date_field in date_fields:
            if hasattr(filter_params, date_field):
                value = getattr(filter_params, date_field)
                if value is not None:
                    filters[date_field] = value

        return filters, filter_map

    def _apply_filters(self, query, filters: Dict[str, Any], filter_map: Dict[str, str]):
        """
        Apply filters to a query based on a field mapping.

        Args:
            query: SQLAlchemy query object
            filters: Dictionary of filter criteria
            filter_map: Mapping of filter keys to entity fields

        Returns:
            Query with filters applied
        """
        for filter_key, field_name in filter_map.items():
            if filter_key in filters and filters[filter_key] is not None:
                field = getattr(self.entity_class, field_name)
                query = query.where(field == filters[filter_key])

        # Handle date range filters if present
        if "created_after" in filters and filters["created_after"]:
            query = query.where(self.entity_class.created_at >= filters["created_after"])

        if "created_before" in filters and filters["created_before"]:
            query = query.where(self.entity_class.created_at <= filters["created_before"])

        if "updated_after" in filters and filters["updated_after"]:
            if hasattr(self.entity_class, "updated_at"):
                query = query.where(self.entity_class.updated_at >= filters["updated_after"])

        if "updated_before" in filters and filters["updated_before"]:
            if hasattr(self.entity_class, "updated_at"):
                query = query.where(self.entity_class.updated_at <= filters["updated_before"])

        return query

    def _list_with_pagination(
        self,
        base_query,
        limit: int = 100,
        offset: int = 0,
        order_by: Optional[str] = None,
        order_dir: str = "desc",
    ) -> Tuple[List[T], int]:
        """
        Execute a query with pagination and return results with total count.

        Args:
            base_query: SQLAlchemy query object with filters already applied
            limit: Maximum number of results to return
            offset: Number of results to skip
            order_by: Field to order by (defaults to created_at)
            order_dir: Order direction ('asc' or 'desc')

        Returns:
            Tuple of (list of entities, total count)
        """
        # Get total count before pagination
        total_count = base_query.count()

        # Apply ordering
        query = self._apply_ordering(base_query, order_by, order_dir)

        # Apply pagination
        query = self._apply_pagination(query, limit, offset)

        # Execute query
        results = query.all()

        return results, total_count
