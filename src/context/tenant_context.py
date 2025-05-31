"""
Tenant context management for the Entity Integration System.

This module provides utilities for managing tenant context throughout the application
to ensure proper tenant isolation in the multi-tenant architecture.

IMPORTANT: Always import this module as 'src.context.tenant_context' to avoid
multiple module instances which would break tenant isolation.
"""

import logging
import threading
from contextlib import contextmanager
from functools import wraps
from typing import TYPE_CHECKING, Any, Callable, Generator, Optional, Union

from sqlalchemy.orm import Session

from src.db.db_config import DatabaseManager

if TYPE_CHECKING:
    from src.schemas.tenant_schema import TenantRead

# Validate import path
if __name__ not in ["src.context.tenant_context"]:
    logging.warning(
        f"Invalid import path for tenant_context: {__name__}. "
        f"This may cause tenant isolation issues. "
        f"Please use 'from src.context.tenant_context import ...'"
    )


class TenantContext:
    """
    Manages tenant context throughout the application using thread-local storage.

    This class provides utilities for getting and setting the current tenant
    context to ensure proper tenant isolation.
    """

    # Use thread-local storage for tenant context
    _thread_local = threading.local()
    _logger = logging.getLogger(__name__)

    @classmethod
    def set_current_tenant(cls, tenant_id: str) -> None:
        """
        Set the current tenant ID for the execution context.

        Args:
            tenant_id: ID of the tenant

        Raises:
            ValueError: If tenant_id is empty or invalid
        """
        if not tenant_id or not isinstance(tenant_id, str) or not tenant_id.strip():
            raise ValueError("tenant_id must be a non-empty string")

        cls._thread_local.tenant_id = tenant_id.strip()
        cls._logger.debug(f"Current tenant set to: {tenant_id}")

    @classmethod
    def get_current_tenant_id(cls) -> Optional[str]:
        """
        Get the current tenant ID from the execution context.

        Returns:
            Current tenant ID or None if not set
        """
        return getattr(cls._thread_local, "tenant_id", None)

    @classmethod
    def clear_current_tenant(cls) -> None:
        """
        Clear the current tenant ID from the execution context.
        """
        if hasattr(cls._thread_local, "tenant_id"):
            delattr(cls._thread_local, "tenant_id")
        cls._logger.debug("Current tenant cleared")

    @classmethod
    def clear_cache(cls, tenant_id: Optional[str] = None) -> None:
        """
        Clear the tenant cache.

        Args:
            tenant_id: If provided, only clear this specific tenant from cache.
                      If None, clear entire cache.
        """
        if not hasattr(cls._thread_local, "tenant_cache"):
            return

        if tenant_id:
            # Clear specific tenant from cache
            cls._thread_local.tenant_cache.pop(tenant_id, None)
            cls._logger.debug(f"Tenant cache cleared for: {tenant_id}")
        else:
            # Clear entire cache
            delattr(cls._thread_local, "tenant_cache")
            cls._logger.debug("Tenant cache cleared")

    @classmethod
    def get_tenant(
        cls, db_manager_or_session, tenant_id: Optional[str] = None
    ) -> Optional["TenantRead"]:
        """
        Get a tenant object by ID or from current context using repository pattern.

        Args:
            db_manager_or_session: DatabaseManager or Session (for backward compatibility)
            tenant_id: Optional explicit tenant ID (prioritized if provided)

        Returns:
            TenantRead schema object or None if not found
        """
        from src.db.db_tenant_models import TenantNotFoundError
        from src.repositories.tenant_repository import TenantRepository

        if not TYPE_CHECKING:
            from src.schemas.tenant_schema import TenantRead  # noqa: F401

        # If explicit tenant_id is provided, use it directly
        # Otherwise fall back to current context
        effective_tenant_id = tenant_id or cls.get_current_tenant_id()

        if not effective_tenant_id:
            cls._logger.warning("No tenant ID provided or set in context")
            return None

        # Initialize tenant cache if needed
        if not hasattr(cls._thread_local, "tenant_cache"):
            cls._thread_local.tenant_cache = {}

        # Check cache first
        if effective_tenant_id in cls._thread_local.tenant_cache:
            return cls._thread_local.tenant_cache[effective_tenant_id]

        # Get tenant via repository
        try:
            # Handle both DatabaseManager and Session for backward compatibility
            if isinstance(db_manager_or_session, DatabaseManager):
                repository = TenantRepository(db_manager_or_session)
            else:
                # Legacy: create a temporary DatabaseManager wrapper
                db_manager = type(
                    "TempDBManager",
                    (),
                    {
                        "get_session": lambda self: db_manager_or_session,
                        "close_session": lambda self, s: None,
                    },
                )()
                repository = TenantRepository(db_manager)
            tenant = repository.get_by_id(effective_tenant_id)

            # Cache for future use (limit cache size to prevent memory issues)
            if len(cls._thread_local.tenant_cache) >= 100:  # Max 100 cached tenants
                # Remove oldest entry (simple FIFO)
                oldest_key = next(iter(cls._thread_local.tenant_cache))
                del cls._thread_local.tenant_cache[oldest_key]

            cls._thread_local.tenant_cache[effective_tenant_id] = tenant
            return tenant
        except TenantNotFoundError:
            cls._logger.warning(f"Tenant not found: {effective_tenant_id}")
            return None
        except Exception as e:
            cls._logger.warning(f"Error retrieving tenant: {effective_tenant_id}, error: {str(e)}")
            return None


@contextmanager
def tenant_context(tenant_id: str) -> Generator[None, None, None]:
    """
    Context manager for tenant operations.

    Sets the current tenant for the duration of the context and clears it afterward.

    Args:
        tenant_id: ID of the tenant

    Yields:
        None
    """
    previous_tenant = TenantContext.get_current_tenant_id()
    TenantContext.set_current_tenant(tenant_id)
    try:
        yield
    finally:
        if previous_tenant:
            TenantContext.set_current_tenant(previous_tenant)
        else:
            TenantContext.clear_current_tenant()


def tenant_aware(tenant_id: Union[Optional[str], Callable] = None):
    """
    Parameterized decorator to make a function tenant-aware.

    Requires an explicit tenant_id parameter or retrieves it from context.

    Args:
        tenant_id: Optional tenant ID to use

    Returns:
        Decorator function
    """

    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            # Use the tenant_id passed to the decorator
            effective_tenant_id = tenant_id

            # If no tenant_id was passed to the decorator, get from context
            if not effective_tenant_id:
                effective_tenant_id = TenantContext.get_current_tenant_id()

            # Remove any tenant_id from kwargs to avoid parameter conflicts
            if "tenant_id" in kwargs:
                kwargs = {k: v for k, v in kwargs.items() if k != "tenant_id"}

            if effective_tenant_id and isinstance(effective_tenant_id, str):
                with tenant_context(effective_tenant_id):
                    return func(*args, **kwargs)
            else:
                TenantContext._logger.warning("No tenant ID provided for tenant-aware function")
                raise ValueError("No tenant ID provided for tenant-aware function")

        return wrapper

    # Handle usage as @tenant_aware (without args)
    if callable(tenant_id):
        func = tenant_id
        tenant_id = None
        return decorator(func)

    # Handle usage as @tenant_aware() or @tenant_aware(tenant_id="...")
    return decorator


def get_tenant_config(session: Session, key: str, default: Any = None) -> Any:
    """
    Get a configuration value for the current tenant.

    Args:
        session: Database session
        key: Configuration key
        default: Default value if key not found

    Returns:
        Configuration value or default
    """
    tenant = TenantContext.get_tenant(session)
    if not tenant:
        return default

    return tenant.get_config_value(key, default)
