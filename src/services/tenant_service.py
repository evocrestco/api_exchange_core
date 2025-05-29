"""
Tenant service for the Entity Integration System.
"""

import logging
from typing import Any, List, Optional

from src.context.operation_context import operation
from src.context.service_decorators import handle_repository_errors
from src.context.tenant_context import TenantContext, tenant_aware
from src.exceptions import ErrorCode, RepositoryError, ValidationError
from src.repositories.tenant_repository import TenantRepository
from src.schemas.tenant_schema import (
    TenantConfigUpdate,
    TenantCreate,
    TenantFilter,
    TenantRead,
    TenantUpdate,
)
from src.services.base_service import BaseService


class TenantService(BaseService[TenantCreate, TenantRead, TenantUpdate, TenantFilter]):
    """
    Service for managing tenants in the multi-tenant system.
    """

    def __init__(
        self, tenant_repository: TenantRepository, logger: Optional[logging.Logger] = None
    ):
        """
        Initialize the tenant service.

        Args:
            tenant_repository: Tenant repository for database operations
            logger: Optional logger instance
        """
        super().__init__(
            repository=tenant_repository,
            read_schema_class=TenantRead,
            logger=logger,
        )

    def _handle_tenant_service_exception(
        self, operation: str, exception: Exception, tenant_id: Optional[str] = None
    ) -> None:
        """
        Handle tenant-specific service exceptions.

        Args:
            operation: Operation being performed
            exception: Exception that occurred
            tenant_id: Optional ID of the tenant involved

        Raises:
            The original exception after logging
        """
        if isinstance(exception, RepositoryError) and exception.error_code == ErrorCode.NOT_FOUND:
            # Re-raise tenant not found errors as-is
            raise
        # Delegate to base class for standard handling
        super()._handle_service_exception(operation, exception, tenant_id)

    @operation()
    def create_tenant(self, tenant_data: TenantCreate) -> TenantCreate:
        """
        Create a new tenant with comprehensive validation and logging.

        Args:
            tenant_data: Validated tenant data

        Returns:
            Created tenant as Pydantic model

        Raises:
            ValueError: If tenant already exists
            ServiceError: If there's an error during creation
        """
        try:
            # Check if tenant already exists
            try:
                self.repository.get_by_id(tenant_data.tenant_id)
                self.logger.warning(
                    f"Tenant already exists with ID: {tenant_data.tenant_id}",
                    extra={"tenant_id": tenant_data.tenant_id},
                )
                raise ValidationError(
                    f"Tenant with ID {tenant_data.tenant_id} already exists",
                    error_code=ErrorCode.VALIDATION_FAILED,
                    field="tenant_id",
                    value=tenant_data.tenant_id,
                )
            except RepositoryError as e:
                if e.error_code != ErrorCode.NOT_FOUND:
                    raise
                # Expected when tenant doesn't exist
                pass

            # Save to database via repository (repository expects TenantCreate schema)
            created_tenant = self.repository.create(tenant_data)

            # Clear context cache
            TenantContext.clear_cache()

            self.logger.info(
                f"Created new tenant: {tenant_data.tenant_id}",
                extra={"tenant_id": tenant_data.tenant_id},
            )

            # Convert to Pydantic model
            return TenantCreate.model_validate(created_tenant)
        except ValidationError:
            # Re-raise validation errors
            raise
        except Exception as e:
            self._handle_tenant_service_exception("create_tenant", e, tenant_data.tenant_id)

    @operation()
    def create_tenant_from_dict(self, tenant_id: str, customer_name: str, **kwargs) -> TenantCreate:
        """
        Create a new tenant from dictionary data.

        Args:
            tenant_id: Unique tenant identifier
            customer_name: Name of the customer
            **kwargs: Additional tenant details

        Returns:
            Created tenant as Pydantic model

        Raises:
            ValueError: If tenant already exists or validation fails
            ServiceError: If there's an error during creation
        """
        try:
            # Create and validate with Pydantic first
            tenant_data = TenantCreate(tenant_id=tenant_id, customer_name=customer_name, **kwargs)

            return self.create_tenant(tenant_data)
        except Exception as e:
            self._handle_tenant_service_exception("create_tenant_from_dict", e, tenant_id)

    @tenant_aware
    @operation()
    def update_tenant(self, update_data: TenantUpdate) -> TenantRead:
        """
        Update an existing tenant.

        Args:
            update_data: Validated update data

        Returns:
            Updated tenant as Pydantic model

        Raises:
            ServiceError: If tenant not found or there's an error during update
        """
        tenant_id = TenantContext.get_current_tenant_id()
        try:
            # Update tenant via repository (repository handles the update logic)
            updated_tenant = self.repository.update(tenant_id, update_data)

            # Clear cache
            TenantContext.clear_cache()

            self.logger.info(
                f"Updated tenant: {tenant_id}",
                extra={
                    "tenant_id": tenant_id,
                    "updated_fields": list(update_data.model_dump(exclude_unset=True).keys()),
                },
            )
            return updated_tenant
        except Exception as e:
            self._handle_tenant_service_exception("update_tenant", e, tenant_id)

    @tenant_aware
    @operation()
    def update_tenant_from_dict(self, **kwargs) -> TenantCreate:
        """
        Update an existing tenant from dictionary data.

        Args:
            **kwargs: Updated field values including tenant_id (injected by tenant_aware)

        Returns:
            Updated tenant as Pydantic model

        Raises:
            ServiceError: If tenant not found or there's an error during update
        """
        tenant_id = TenantContext.get_current_tenant_id()
        try:
            # Create and validate with Pydantic first
            update_data = TenantUpdate(**kwargs)
            return self.update_tenant(update_data)
        except Exception as e:
            self._handle_tenant_service_exception("update_tenant_from_dict", e, tenant_id)

    @operation()
    def update_tenant_config(self, tenant_id: str, key: str, value: Any) -> bool:
        """
        Update a tenant's configuration.

        Args:
            tenant_id: Tenant ID
            key: Configuration key
            value: Configuration value

        Returns:
            True if successful, False if tenant not found

        Raises:
            ServiceError: If there's an error during update
        """
        try:
            # Validate with Pydantic
            config_update = TenantConfigUpdate(key=key, value=value)

            # Update via repository
            self.repository.update_config(tenant_id, config_update)

            # Clear cache
            TenantContext.clear_cache()

            self.logger.info(
                f"Updated config for tenant {tenant_id}: {key}",
                extra={"tenant_id": tenant_id, "config_key": key},
            )
            return True
        except RepositoryError as e:
            if e.error_code == ErrorCode.NOT_FOUND:
                self.logger.warning(
                    f"Cannot update config - tenant not found: {tenant_id}",
                    extra={"tenant_id": tenant_id, "config_key": key},
                )
                return False
            raise
        except Exception as e:
            self._handle_tenant_service_exception("update_tenant_config", e, tenant_id)

    @operation()
    @handle_repository_errors("get_tenants")
    def get_tenants(self, include_inactive: bool = False) -> List[TenantCreate]:
        """
        Get all tenants.

        Args:
            include_inactive: Whether to include inactive tenants

        Returns:
            List of tenants as Pydantic models

        Raises:
            ServiceError: If there's an error during retrieval
        """
        tenants = self.repository.get_all_tenants(include_inactive)
        return [TenantCreate.model_validate(tenant) for tenant in tenants]

    @operation()
    @handle_repository_errors("get_tenant")
    def get_tenant(self, tenant_id: str) -> TenantCreate:
        """
        Get a tenant by ID.

        Args:
            tenant_id: Tenant ID

        Returns:
            Tenant as Pydantic model

        Raises:
            ServiceError: If tenant not found or there's an error during retrieval
        """
        tenant = self.repository.get_by_id(tenant_id)
        return TenantCreate.model_validate(tenant)

    @operation()
    def activate_tenant(self, tenant_id: str) -> bool:
        """
        Activate a tenant.

        Args:
            tenant_id: Tenant ID

        Returns:
            True if successful, False if tenant not found

        Raises:
            ServiceError: If there's an error during activation
        """
        try:
            # Use repository update with proper schema
            update_data = TenantUpdate(is_active=True)
            self.repository.update(tenant_id, update_data)

            # Clear cache
            TenantContext.clear_cache()

            self.logger.info(
                f"Activated tenant: {tenant_id}",
                extra={"tenant_id": tenant_id, "action": "activate"},
            )
            return True
        except RepositoryError as e:
            if e.error_code == ErrorCode.NOT_FOUND:
                self.logger.warning(
                    f"Cannot activate tenant - not found: {tenant_id}",
                    extra={"tenant_id": tenant_id, "action": "activate"},
                )
                return False
            raise
        except Exception as e:
            self._handle_tenant_service_exception("activate_tenant", e, tenant_id)

    @operation()
    def deactivate_tenant(self, tenant_id: str) -> bool:
        """
        Deactivate a tenant.

        Args:
            tenant_id: Tenant ID

        Returns:
            True if successful, False if tenant not found

        Raises:
            ServiceError: If there's an error during deactivation
        """
        try:
            # Use repository update with proper schema
            update_data = TenantUpdate(is_active=False)
            self.repository.update(tenant_id, update_data)

            # Clear cache
            TenantContext.clear_cache()

            self.logger.info(
                f"Deactivated tenant: {tenant_id}",
                extra={"tenant_id": tenant_id, "action": "deactivate"},
            )
            return True
        except RepositoryError as e:
            if e.error_code == ErrorCode.NOT_FOUND:
                self.logger.warning(
                    f"Cannot deactivate tenant - not found: {tenant_id}",
                    extra={"tenant_id": tenant_id, "action": "deactivate"},
                )
                return False
            raise
        except Exception as e:
            self._handle_tenant_service_exception("deactivate_tenant", e, tenant_id)
