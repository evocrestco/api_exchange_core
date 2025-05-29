"""
Base service implementation with common functionality for all services.

This module provides a base class with shared methods and patterns to
reduce duplication across service implementations.
"""

import logging
from typing import Any, Dict, Generic, List, NoReturn, Optional, Type, TypeVar

from pydantic import BaseModel

from src.context.operation_context import operation
from src.exceptions import ErrorCode, RepositoryError, ServiceError
from src.utils.logger import get_logger

# Type variables for schema models
TCreate = TypeVar("TCreate", bound=BaseModel)
TRead = TypeVar("TRead", bound=BaseModel)
TUpdate = TypeVar("TUpdate", bound=BaseModel)
TFilter = TypeVar("TFilter", bound=BaseModel)


class BaseService(Generic[TCreate, TRead, TUpdate, TFilter]):
    """Base service with common functionality for all services."""

    def __init__(
        self,
        repository: Any,
        read_schema_class: Type[TRead],
        logger: Optional[logging.Logger] = None,
    ):
        """
        Initialize the base service.

        Args:
            repository: Repository for data access
            read_schema_class: Pydantic schema class for read operations
            logger: Optional logger instance
        """
        self.repository = repository
        self.read_schema_class = read_schema_class
        self.logger = get_logger()

    def _handle_service_exception(
        self, operation: str, exception: Exception, entity_id: Optional[str] = None  # noqa
    ) -> NoReturn:
        """
        Handle and log service exceptions consistently.

        Args:
            operation: Operation being performed
            exception: Exception that occurred
            entity_id: Optional ID of the entity involved

        Raises:
            The original exception after logging
        """
        if isinstance(exception, RepositoryError) and exception.error_code == ErrorCode.NOT_FOUND:
            # Convert NOT_FOUND RepositoryError to ServiceError
            self.logger.warning(
                f"Entity not found in {operation}",
                extra={
                    "operation": operation,
                    "entity_id": entity_id,
                    "error_type": type(exception).__name__,
                    "error_details": str(exception),
                },
            )
            raise ServiceError(
                str(exception),
                error_code=ErrorCode.NOT_FOUND,
                operation=operation,
                entity_id=entity_id,
                cause=exception,
            )
        else:
            # Wrap other exceptions in ServiceError
            error_msg = f"Error in {operation}: {str(exception)}"
            self.logger.error(
                error_msg,
                extra={
                    "operation": operation,
                    "entity_id": entity_id,
                    "error_type": type(exception).__name__,
                    "error_details": str(exception),
                },
                exc_info=True,
            )
            raise ServiceError(
                error_msg,
                error_code=ErrorCode.INTERNAL_ERROR,
                operation=operation,
                entity_id=entity_id,
                cause=exception,
            )

    def _entity_to_schema(self, entity_dict: Dict[str, Any]) -> TRead:
        """
        Convert an entity dictionary to a schema model.

        Args:
            entity_dict: Dictionary representation of an entity

        Returns:
            Schema model instance
        """
        return self.read_schema_class(**entity_dict)

    def _entities_to_schemas(self, entity_dicts: List[Dict[str, Any]]) -> List[TRead]:
        """
        Convert a list of entity dictionaries to schema models.

        Args:
            entity_dicts: List of entity dictionaries

        Returns:
            List of schema model instances
        """
        return [self._entity_to_schema(entity_dict) for entity_dict in entity_dicts]

    @operation()
    def paginate_results(
        self, results: List[TRead], total_count: int, page: int, page_size: int
    ) -> Dict[str, Any]:
        """
        Create a standardized pagination response.

        Args:
            results: Results for current page
            total_count: Total number of records
            page: Current page number
            page_size: Size of each page

        Returns:
            Paginated response with metadata
        """
        total_pages = (total_count + page_size - 1) // page_size if page_size > 0 else 0

        return {
            "data": results,
            "pagination": {
                "page": page,
                "page_size": page_size,
                "total_count": total_count,
                "total_pages": total_pages,
                "has_previous": page > 1,
                "has_next": page < total_pages,
            },
        }
