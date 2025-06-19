"""
Base service implementation with common functionality for all services.

This module provides a base class with shared methods and patterns to
reduce duplication across service implementations.
"""

import logging
from typing import Any, Dict, Generic, List, NoReturn, Optional, Type, TypeVar
from contextlib import contextmanager

from pydantic import BaseModel
from sqlalchemy.orm import Session, sessionmaker

from ..context.operation_context import operation
from ..exceptions import ErrorCode, RepositoryError, ServiceError
from ..utils.logger import get_logger
from ..db.db_config import get_production_config

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

    def _get_current_tenant_id(self) -> str:
        """Get current tenant ID from context with validation."""
        from ..context.tenant_context import TenantContext

        tenant_id = TenantContext.get_current_tenant_id()
        if not tenant_id:
            raise ValueError("No tenant context set - ensure tenant_context is active")
        return tenant_id

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


class SessionManagedService(BaseService[TCreate, TRead, TUpdate, TFilter]):
    """
    Service that owns and manages its own database session.
    
    This is the new pattern where each service has its own session,
    eliminating the session conflicts we had with shared sessions.
    """
    
    def __init__(
        self,
        repository_class: Optional[Type] = None,
        read_schema_class: Optional[Type[TRead]] = None,
        logger: Optional[logging.Logger] = None,
        session: Optional[Session] = None,
    ):
        """
        Initialize service with its own session.
        
        Args:
            repository_class: DEPRECATED - Repository class (for backward compatibility)
            read_schema_class: DEPRECATED - Pydantic schema class (for backward compatibility)
            logger: Optional logger instance
            session: Optional existing session (for testing or coordination)
        """
        # Create or use provided session
        if session:
            self.session = session
            self._owns_session = False
        else:
            self.session = self._create_session()
            self._owns_session = True
            
        # Legacy repository support (DEPRECATED - use SQLAlchemy directly)
        if repository_class:
            repository = repository_class(self.session)
        else:
            repository = None
        
        # Initialize base service if repository exists (for backward compatibility)
        if repository:
            super().__init__(repository, read_schema_class, logger)
        else:
            # For Pythonic services, initialize manually
            self.read_schema_class = read_schema_class
            self.logger = logger or get_logger()
        
    def _create_session(self) -> Session:
        """Create a new database session."""
        from sqlalchemy import create_engine
        
        db_config = get_production_config()
        engine = create_engine(db_config.get_connection_string())
        Session = sessionmaker(bind=engine)
        return Session()
    
    @contextmanager
    def transaction(self):
        """
        Context manager for transactional operations.
        
        Usage:
            with service.transaction():
                service.create_something()
                service.update_something()
                # Auto-commits on success, rollback on exception
        """
        try:
            yield self.session
            if self._owns_session:
                self.session.commit()
        except Exception:
            if self._owns_session:
                self.session.rollback()
            raise
    
    def commit(self):
        """Manually commit the current transaction."""
        if self._owns_session:
            self.session.commit()
    
    def rollback(self):
        """Manually rollback the current transaction.""" 
        if self._owns_session:
            self.session.rollback()
    
    def close(self):
        """Close the session if we own it."""
        if self._owns_session and self.session:
            self.session.close()
    
    def __enter__(self):
        """Support for 'with' statement."""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Auto-close session on exit."""
        if exc_type:
            self.rollback()
        else:
            self.commit()
        self.close()
