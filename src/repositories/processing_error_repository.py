"""
Repository for ProcessingError entity operations.

This module provides data access methods for the ProcessingError entity,
following the repository pattern to encapsulate data access logic.
"""

import logging
from typing import List, Optional

from sqlalchemy import and_, select

from src.context.tenant_context import TenantContext
from src.db.db_error_models import ProcessingError
from src.exceptions import not_found
from src.repositories.base_repository import BaseRepository
from src.schemas.processing_error_schema import (
    ProcessingErrorCreate,
    ProcessingErrorFilter,
    ProcessingErrorRead,
)


class ProcessingErrorRepository(BaseRepository[ProcessingError]):
    """Repository for ProcessingError entity data access operations."""

    def __init__(self, session, logger: Optional[logging.Logger] = None):
        """
        Initialize the ProcessingError repository.

        Args:
            session: SQLAlchemy session for database operations
            logger: Optional logger instance
        """
        super().__init__(session, ProcessingError, logger)

    def create(self, error_data: ProcessingErrorCreate) -> str:
        """
        Create a new processing error record.

        Args:
            error_data: Validated processing error data

        Returns:
            The ID of the created error record

        Raises:
            RepositoryError: If there's a database error during creation
        """
        tenant_id = self._get_current_tenant_id()
            
        with self._session_operation("create") as session:
            # Create ProcessingError instance with tenant context
            error = ProcessingError(
                tenant_id=tenant_id,
                entity_id=error_data.entity_id,
                error_type_code=error_data.error_type_code,
                message=error_data.message,
                processing_step=error_data.processing_step,
                stack_trace=error_data.stack_trace,
            )
            
            session.add(error)
            session.flush()

            self.logger.info(f"Created processing error with ID: {error.id}")
            return error.id  # type: ignore[return-value]

    def get_by_id(self, error_id: str) -> Optional[ProcessingErrorRead]:
        """
        Get a processing error by its ID.

        Args:
            error_id: The ID of the error to retrieve

        Returns:
            ProcessingErrorRead schema object or None if not found

        Raises:
            EntityNotFoundError: If the error doesn't exist
            RepositoryError: If there's a database error
        """
        with self._session_operation("get_by_id", error_id) as session:
            tenant_id = self._get_current_tenant_id()
            
            error = session.query(ProcessingError).filter(
                ProcessingError.id == error_id,
                ProcessingError.tenant_id == tenant_id
            ).first()

            if not error:
                return None

            return ProcessingErrorRead.model_validate(error)
    
    def require_by_id(self, error_id: str) -> ProcessingErrorRead:
        """
        Get a processing error by its ID, raising an exception if not found.

        Args:
            error_id: The ID of the error to retrieve

        Returns:
            ProcessingErrorRead schema object

        Raises:
            RepositoryError: If the error doesn't exist or there's a database error
        """
        error = self.get_by_id(error_id)
        if not error:
            raise not_found("ProcessingError", error_id=error_id)
        return error

    def find_by_entity_id(self, entity_id: str) -> List[ProcessingErrorRead]:
        """
        Get all processing errors for a specific entity.

        Args:
            entity_id: The ID of the entity

        Returns:
            List of ProcessingErrorRead schema objects

        Raises:
            RepositoryError: If there's a database error
        """
        with self._session_operation("find_by_entity_id") as session:
            tenant_id = self._get_current_tenant_id()

            query = select(ProcessingError).where(
                and_(
                    ProcessingError.entity_id == entity_id,
                    ProcessingError.tenant_id == tenant_id,
                )
            )

            # Apply ordering by created_at descending (most recent first)
            query = self._apply_ordering(query, "created_at", "desc")

            errors = session.execute(query).scalars().all()
            return [ProcessingErrorRead.model_validate(error) for error in errors]

    def get_by_filter(
        self, filter_params: ProcessingErrorFilter, limit: int = 100, offset: int = 0
    ) -> List[ProcessingErrorRead]:
        """
        Find processing errors based on filter criteria.

        Args:
            filter_params: ProcessingErrorFilter schema with filter criteria
            limit: Maximum number of records to return (default: 100)
            offset: Number of records to skip (default: 0)

        Returns:
            List of ProcessingErrorRead schema objects

        Raises:
            RepositoryError: If there's a database error
        """
        with self._session_operation("get_by_filter") as session:
            tenant_id = self._get_current_tenant_id()

            query = select(ProcessingError)

            # Apply tenant filter
            if tenant_id:
                query = self._apply_tenant_filter(query, tenant_id)

            # Build filter map using BaseRepository helper
            field_mapping = {
                "entity_id": "entity_id",
                "error_type_code": "error_type_code",
                "processing_step": "processing_step",
            }
            filters, filter_map = self._build_filter_map(filter_params, field_mapping)

            # Apply filters using BaseRepository helper
            query = self._apply_filters(query, filters, filter_map)

            # Apply ordering
            query = self._apply_ordering(query, "created_at", "desc")

            # Apply pagination
            query = self._apply_pagination(query, limit, offset)

            errors = session.execute(query).scalars().all()
            return [ProcessingErrorRead.model_validate(error) for error in errors]

    def delete(self, error_id: str) -> bool:
        """
        Delete a processing error by ID.

        Args:
            error_id: The ID of the error to delete

        Returns:
            True if deletion was successful, False if the error wasn't found

        Raises:
            RepositoryError: If there's a database error
        """
        with self._session_operation("delete", error_id) as session:
            tenant_id = self._get_current_tenant_id()
            
            error = session.query(ProcessingError).filter(
                ProcessingError.id == error_id,
                ProcessingError.tenant_id == tenant_id
            ).first()
            
            if not error:
                return False
                
            session.delete(error)
            session.flush()
            
            self.logger.info(f"Deleted processing error with ID: {error_id}")
            return True

    def delete_by_entity_id(self, entity_id: str) -> int:
        """
        Delete all processing errors for a specific entity.

        Args:
            entity_id: The ID of the entity

        Returns:
            Number of deleted records

        Raises:
            RepositoryError: If there's a database error
        """
        with self._session_operation("delete_by_entity_id") as session:
            tenant_id = self._get_current_tenant_id()

            # First, get all error IDs for this entity
            query = select(ProcessingError.id).where(
                and_(
                    ProcessingError.entity_id == entity_id,
                    ProcessingError.tenant_id == tenant_id,
                )
            )
            error_ids = [row[0] for row in session.execute(query).all()]

            if not error_ids:
                return 0

            # Use batch delete
            return self._delete_batch(error_ids, soft_delete=False)
