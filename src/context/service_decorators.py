"""
Service layer decorators for reducing code duplication.

This module provides reusable decorators for common service patterns,
particularly error handling when calling repository methods.
"""

from functools import wraps
from typing import Any, Callable, Optional, TypeVar, cast

from src.exceptions import RepositoryError

F = TypeVar("F", bound=Callable[..., Any])


def handle_repository_errors(operation_name: Optional[str] = None):
    """
    Decorator to automatically handle repository errors in service methods.

    This decorator wraps service methods that call repository operations and
    automatically converts RepositoryError exceptions to ServiceError exceptions
    using the service's error handling methods.

    Args:
        operation_name: Optional name for the operation. If not provided,
                       uses the function name.

    Usage:
        @handle_repository_errors("create_entity")
        def create_entity(self, ...):
            return self.repository.create(...)

        # Or with automatic operation name
        @handle_repository_errors()
        def create_entity(self, ...):
            return self.repository.create(...)
    """

    def decorator(func: F) -> F:
        @wraps(func)
        def wrapper(self, *args, **kwargs):
            op_name = operation_name or func.__name__
            try:
                return func(self, *args, **kwargs)
            except RepositoryError as e:
                # Check if service has _handle_repo_error method (legacy EntityService)
                if hasattr(self, "_handle_repo_error"):
                    # Extract entity_id if it's in args for better context
                    entity_id = None
                    if args and isinstance(args[0], str):
                        entity_id = args[0]
                    self._handle_repo_error(e, op_name, entity_id)
                # Check if service has custom exception handler (like TenantService)
                elif hasattr(self, "_handle_tenant_service_exception"):
                    # Extract entity_id if it's in args for better context
                    entity_id = None
                    if args and isinstance(args[0], str):
                        entity_id = args[0]
                    elif "tenant_id" in kwargs:
                        entity_id = kwargs["tenant_id"]
                    self._handle_tenant_service_exception(op_name, e, entity_id)
                # Otherwise use the base service exception handler
                elif hasattr(self, "_handle_service_exception"):
                    # Extract entity_id if it's in args/kwargs for better context
                    entity_id = None
                    if args and isinstance(args[0], str):
                        entity_id = args[0]
                    elif "entity_id" in kwargs:
                        entity_id = kwargs["entity_id"]
                    elif "tenant_id" in kwargs:
                        entity_id = kwargs["tenant_id"]
                    self._handle_service_exception(op_name, e, entity_id)
                else:
                    # Fallback - just re-raise
                    raise
            except Exception as e:
                # Handle unexpected errors - use the same priority for custom handlers
                if hasattr(self, "_handle_tenant_service_exception"):
                    entity_id = None
                    if args and isinstance(args[0], str):
                        entity_id = args[0]
                    elif "tenant_id" in kwargs:
                        entity_id = kwargs["tenant_id"]
                    self._handle_tenant_service_exception(op_name, e, entity_id)
                elif hasattr(self, "_handle_service_exception"):
                    entity_id = None
                    if args and isinstance(args[0], str):
                        entity_id = args[0]
                    elif "entity_id" in kwargs:
                        entity_id = kwargs["entity_id"]
                    elif "tenant_id" in kwargs:
                        entity_id = kwargs["tenant_id"]
                    self._handle_service_exception(op_name, e, entity_id)
                else:
                    # Fallback - just re-raise
                    raise

        return cast(F, wrapper)

    return decorator


def transactional():
    """
    Decorator to automatically handle database transactions in service methods.

    This decorator wraps service methods that perform database operations and
    automatically commits transactions on success or rolls back on errors.
    It works by accessing the service's repository session to manage the transaction.

    Usage:
        @transactional()
        @handle_repository_errors("create_entity")
        def create_entity(self, ...):
            return self.repository.create(...)

    Note: This decorator should be applied BEFORE @handle_repository_errors()
    to ensure transactions are properly managed before error handling.
    """

    def decorator(func: F) -> F:
        @wraps(func)
        def wrapper(self, *args, **kwargs):
            # Get the session from the service's repository
            if not hasattr(self, "repository") or not hasattr(self.repository, "session"):
                # If no repository or session, just call the function normally
                return func(self, *args, **kwargs)

            session = self.repository.session

            try:
                # Execute the function
                result = func(self, *args, **kwargs)
                
                # Commit the transaction on success
                session.commit()
                
                return result
                
            except Exception:
                # Rollback on any exception
                session.rollback()
                raise

        return cast(F, wrapper)

    return decorator
