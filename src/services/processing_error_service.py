"""
Service layer for processing error operations.

This module provides business logic for managing processing errors,
abstracting the repository operations and adding business rules.
"""

import logging
from typing import List, Optional

from src.context.operation_context import operation
from src.context.service_decorators import handle_repository_errors
from src.repositories.processing_error_repository import ProcessingErrorRepository
from src.schemas.processing_error_schema import (
    ProcessingErrorCreate,
    ProcessingErrorFilter,
    ProcessingErrorRead,
)
from src.services.base_service import BaseService


class ProcessingErrorService(
    BaseService[ProcessingErrorCreate, ProcessingErrorRead, None, ProcessingErrorFilter]
):
    """Service for processing error business operations."""

    def __init__(
        self, repository: ProcessingErrorRepository, logger: Optional[logging.Logger] = None
    ):
        """
        Initialize the processing error service.

        Args:
            repository: Repository for processing error data access
            logger: Optional logger instance
        """
        super().__init__(repository, ProcessingErrorRead, logger)

    @operation()
    @handle_repository_errors("create_error")
    def create_error(self, error_data: ProcessingErrorCreate) -> str:
        """
        Create a new processing error.

        Args:
            error_data: Validated error data

        Returns:
            ID of the created error

        Raises:
            ServiceError: If there's an error during creation
        """
        error_id = self.repository.create(error_data)
        self.logger.info(
            f"Created processing error for entity {error_data.entity_id}: "
            f"{error_data.error_type_code}",
            extra={
                "entity_id": error_data.entity_id,
                "error_type": error_data.error_type_code,
                "processing_step": error_data.processing_step,
            },
        )
        return error_id

    @operation()
    @handle_repository_errors("get_error")
    def get_error(self, error_id: str) -> ProcessingErrorRead:
        """
        Get a processing error by ID.

        Args:
            error_id: ID of the error to retrieve

        Returns:
            ProcessingError data

        Raises:
            EntityNotFoundError: If the error doesn't exist
            ServiceError: If there's an error during retrieval
        """
        error_data = self.repository.get_by_id(error_id)
        return error_data

    @operation()
    @handle_repository_errors("get_entity_errors")
    def get_entity_errors(self, entity_id: str) -> List[ProcessingErrorRead]:
        """
        Get all errors for a specific entity.

        Args:
            entity_id: ID of the entity

        Returns:
            List of processing errors

        Raises:
            ServiceError: If there's an error during retrieval
        """
        errors = self.repository.find_by_entity_id(entity_id)
        return errors

    @operation()
    @handle_repository_errors("find_errors")
    def find_errors(
        self, filters: ProcessingErrorFilter, limit: int = 100, offset: int = 0
    ) -> List[ProcessingErrorRead]:
        """
        Find errors based on filter criteria.

        Args:
            filters: Filter criteria
            limit: Maximum number of records to return
            offset: Number of records to skip

        Returns:
            List of processing errors matching criteria

        Raises:
            ServiceError: If there's an error during retrieval
        """
        errors = self.repository.get_by_filter(filters, limit=limit, offset=offset)
        return errors

    @operation()
    @handle_repository_errors("delete_error")
    def delete_error(self, error_id: str) -> bool:
        """
        Delete a processing error.

        Args:
            error_id: ID of the error to delete

        Returns:
            True if deletion was successful, False if error wasn't found

        Raises:
            ServiceError: If there's an error during deletion
        """
        result = self.repository.delete(error_id)
        if result:
            self.logger.info(f"Deleted processing error: {error_id}")
        else:
            self.logger.warning(f"Processing error not found for deletion: {error_id}")
        return result

    @operation()
    @handle_repository_errors("delete_entity_errors")
    def delete_entity_errors(self, entity_id: str) -> int:
        """
        Delete all errors for a specific entity.

        Args:
            entity_id: ID of the entity

        Returns:
            Number of deleted records

        Raises:
            ServiceError: If there's an error during deletion
        """
        count = self.repository.delete_by_entity_id(entity_id)
        self.logger.info(f"Deleted {count} processing errors for entity: {entity_id}")
        return count

    @operation()
    def record_error(
        self,
        entity_id: str,
        error_type: str,
        message: str,
        processing_step: str,
        stack_trace: Optional[str] = None,
    ) -> str:
        """
        Convenience method to record an error in one step.

        Args:
            entity_id: ID of the entity
            error_type: Type of error
            message: Error message
            processing_step: Processing step where error occurred
            stack_trace: Optional stack trace

        Returns:
            ID of the created error

        Raises:
            ServiceError: If there's an error during creation
        """
        error_data = ProcessingErrorCreate(
            entity_id=entity_id,
            error_type_code=error_type,
            message=message,
            processing_step=processing_step,
            stack_trace=stack_trace,
        )

        return self.create_error(error_data)
