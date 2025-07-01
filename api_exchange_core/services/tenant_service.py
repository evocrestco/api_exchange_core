"""
Pythonic tenant service with direct SQLAlchemy access.

This module provides business logic for managing tenants
using SQLAlchemy directly - simple, explicit, and efficient.
"""

import uuid
from datetime import datetime
from typing import Any, List

from pydantic import ValidationError as PydanticValidationError
from sqlalchemy import exists
from sqlalchemy.exc import IntegrityError

from .base_service import SessionManagedService
from ..context.operation_context import operation
from ..db.db_tenant_models import Tenant
from ..exceptions import ErrorCode, ServiceError, ValidationError
from ..schemas.tenant_schema import (
    TenantConfigValue,
    TenantCreate,
    TenantRead,
    TenantUpdate,
)
from ..utils import get_logger


class TenantService(SessionManagedService):
    """
    Pythonic service for managing tenants with direct SQLAlchemy access.

    Uses SQLAlchemy directly - simple, explicit, and efficient.
    """

    def __init__(self):
        """
        Initialize the tenant service with global database manager.

        Args:
            logger: Optional logger instance
        """
        super().__init__(logger=get_logger())

    @operation()
    def create_tenant(self, tenant_data: TenantCreate) -> TenantRead:
        """
        Create a new tenant.

        Args:
            tenant_data: Validated tenant data

        Returns:
            Created tenant data

        Raises:
            ServiceError: If there's an error during creation
        """
        try:
            # Check if tenant_id already exists
            if self.session.query(
                exists().where(Tenant.tenant_id == tenant_data.tenant_id)
            ).scalar():
                raise ServiceError(
                    f"Tenant already exists: {tenant_data.tenant_id}",
                    error_code=ErrorCode.DUPLICATE,
                    operation="create_tenant",
                    tenant_id=tenant_data.tenant_id,
                )

            # Create tenant using validated schema data
            tenant = Tenant(
                id=str(uuid.uuid4()),
                tenant_id=tenant_data.tenant_id,
                customer_name=tenant_data.customer_name,
                primary_contact_name=tenant_data.primary_contact_name,
                primary_contact_email=tenant_data.primary_contact_email,
                primary_contact_phone=tenant_data.primary_contact_phone,
                address_line1=tenant_data.address_line1,
                address_line2=tenant_data.address_line2,
                city=tenant_data.city,
                state=tenant_data.state,
                postal_code=tenant_data.postal_code,
                country=tenant_data.country,
                tenant_config=tenant_data.tenant_config or {},
                notes=tenant_data.notes,
                is_active=tenant_data.is_active,
                created_at=datetime.utcnow(),
                updated_at=datetime.utcnow(),
            )

            self.session.add(tenant)
            # Transaction managed by caller

            self.logger.info(f"Created tenant: id={tenant.id}, tenant_id={tenant_data.tenant_id}")

            # Return the created tenant as TenantRead
            return TenantRead.model_validate(tenant)

        except PydanticValidationError as e:
            # Handle Pydantic validation errors
            raise ValidationError(
                f"Invalid tenant data: {str(e)}",
                details={"validation_errors": e.errors()},
            ) from e
        except IntegrityError as e:
            # Transaction managed by caller
            # Check if it's a unique constraint (duplicate tenant_id)
            if "unique constraint" in str(e).lower():
                raise ServiceError(
                    f"Tenant already exists: {tenant_data.tenant_id}",
                    error_code=ErrorCode.DUPLICATE,
                    operation="create_tenant",
                    tenant_id=tenant_data.tenant_id,
                    cause=e,
                ) from e
            else:
                raise ServiceError(
                    "Tenant creation failed due to data integrity constraints",
                    error_code=ErrorCode.INVALID_DATA,
                    operation="create_tenant",
                    tenant_id=tenant_data.tenant_id,
                    cause=e,
                ) from e
        except Exception as e:
            # Transaction managed by caller
            if isinstance(e, ServiceError):
                raise
            self._handle_service_exception("create_tenant", e)

    @operation()
    def get_current_tenant(self) -> TenantRead:
        """
        Get the current tenant from context.

        Returns:
            Current tenant data

        Raises:
            ServiceError: If the tenant doesn't exist or there's an error during retrieval
        """
        tenant_id = self._get_current_tenant_id()
        return self.get_tenant(tenant_id)

    @operation()
    def get_tenant(self, tenant_id: str) -> TenantRead:
        """
        Get a tenant by tenant_id.

        Args:
            tenant_id: Tenant identifier

        Returns:
            Tenant data

        Raises:
            ServiceError: If the tenant doesn't exist or there's an error during retrieval
        """
        try:
            # Query tenant directly with SQLAlchemy
            tenant = self.session.query(Tenant).filter(Tenant.tenant_id == tenant_id).first()

            if tenant is None:
                raise ServiceError(
                    f"Tenant not found: tenant_id={tenant_id}",
                    error_code=ErrorCode.NOT_FOUND,
                    tenant_id=tenant_id,
                )

            # Convert to TenantRead
            return TenantRead.model_validate(tenant)

        except Exception as e:
            if isinstance(e, ServiceError):
                raise
            self._handle_service_exception("get_tenant", e)

    @operation()
    def list_tenants(self, limit: int = 100, offset: int = 0) -> List[TenantRead]:
        """
        List all tenants.

        Args:
            limit: Maximum number of tenants to return
            offset: Number of tenants to skip

        Returns:
            List of tenants

        Raises:
            ServiceError: If there's an error during retrieval
        """
        try:
            # Query tenants directly with SQLAlchemy
            tenants = (
                self.session.query(Tenant)
                .order_by(Tenant.created_at.desc())
                .offset(offset)
                .limit(limit)
                .all()
            )

            # Convert to TenantRead objects
            return [TenantRead.model_validate(tenant) for tenant in tenants]

        except Exception as e:
            self._handle_service_exception("list_tenants", e)

    @operation()
    def update_tenant(self, update_data: TenantUpdate) -> TenantRead:
        """
        Update the current tenant from context.

        Args:
            update_data: Updated tenant data

        Returns:
            Updated tenant data

        Raises:
            ServiceError: If the tenant doesn't exist or there's an error during update
        """
        try:
            # Get current tenant ID from context
            tenant_id = self._get_current_tenant_id()

            # Get tenant for update
            tenant = self.session.query(Tenant).filter(Tenant.tenant_id == tenant_id).first()

            if not tenant:
                raise ServiceError(
                    f"Tenant not found: {tenant_id}",
                    error_code=ErrorCode.NOT_FOUND,
                    tenant_id=tenant_id,
                )

            # Update tenant fields using TenantUpdate schema
            if update_data.customer_name is not None:
                tenant.customer_name = update_data.customer_name
            if update_data.primary_contact_name is not None:
                tenant.primary_contact_name = update_data.primary_contact_name
            if update_data.primary_contact_email is not None:
                tenant.primary_contact_email = update_data.primary_contact_email
            if update_data.primary_contact_phone is not None:
                tenant.primary_contact_phone = update_data.primary_contact_phone
            if update_data.address_line1 is not None:
                tenant.address_line1 = update_data.address_line1
            if update_data.address_line2 is not None:
                tenant.address_line2 = update_data.address_line2
            if update_data.city is not None:
                tenant.city = update_data.city
            if update_data.state is not None:
                tenant.state = update_data.state
            if update_data.postal_code is not None:
                tenant.postal_code = update_data.postal_code
            if update_data.country is not None:
                tenant.country = update_data.country
            if update_data.is_active is not None:
                tenant.is_active = update_data.is_active

            tenant.updated_at = datetime.utcnow()
            # Transaction managed by caller

            self.logger.info(f"Updated tenant: tenant_id={tenant_id}")

            # Return updated tenant
            return TenantRead.model_validate(tenant)

        except Exception as e:
            # Transaction managed by caller
            if isinstance(e, ServiceError):
                raise
            self._handle_service_exception("update_tenant", e)

    @operation()
    def update_tenant_config(self, key: str, value: Any) -> bool:
        """
        Update a tenant configuration value.

        Args:
            key: Configuration key
            value: Configuration value

        Returns:
            True if successful, False if tenant not found

        Raises:
            ServiceError: If there's an error during update
        """
        try:
            tenant_id = self._get_current_tenant_id()

            # Get current tenant
            tenant = self.session.query(Tenant).filter(Tenant.tenant_id == tenant_id).first()

            if not tenant:
                return False

            # Update tenant config
            if not tenant.tenant_config:
                tenant.tenant_config = {}

            tenant.tenant_config[key] = TenantConfigValue(value=value)
            tenant.updated_at = datetime.utcnow()

            # Transaction managed by caller

            self.logger.info(f"Updated tenant config: tenant_id={tenant_id}, key={key}")

            return True

        except Exception as e:
            # Transaction managed by caller
            if isinstance(e, ServiceError):
                raise
            self._handle_service_exception("update_tenant_config", e)

    @operation()
    def activate_tenant(self) -> TenantRead:
        """
        Activate the current tenant.

        Returns:
            Updated tenant data

        Raises:
            ServiceError: If the tenant doesn't exist or there's an error during update
        """
        try:
            tenant_id = self._get_current_tenant_id()

            tenant = self.session.query(Tenant).filter(Tenant.tenant_id == tenant_id).first()

            if not tenant:
                raise ServiceError(
                    f"Tenant not found: {tenant_id}",
                    error_code=ErrorCode.NOT_FOUND,
                    tenant_id=tenant_id,
                )

            tenant.is_active = True
            tenant.updated_at = datetime.utcnow()

            # Transaction managed by caller

            self.logger.info(f"Activated tenant: tenant_id={tenant_id}")

            return TenantRead.model_validate(tenant)

        except Exception as e:
            # Transaction managed by caller
            if isinstance(e, ServiceError):
                raise
            self._handle_service_exception("activate_tenant", e)

    @operation()
    def activate_current_tenant(self) -> bool:
        """
        Activate the current tenant from context.

        Returns:
            True if successful, False if tenant not found
        """
        try:
            tenant_id = self._get_current_tenant_id()

            tenant = self.session.query(Tenant).filter(Tenant.tenant_id == tenant_id).first()

            if not tenant:
                return False

            tenant.is_active = True
            tenant.updated_at = datetime.utcnow()

            # Transaction managed by caller

            self.logger.info(f"Activated tenant: tenant_id={tenant_id}")

            return True

        except Exception as e:
            # Transaction managed by caller
            if isinstance(e, ServiceError):
                raise
            self._handle_service_exception("activate_current_tenant", e)

    @operation()
    def deactivate_tenant(self) -> TenantRead:
        """
        Deactivate the current tenant.

        Returns:
            Updated tenant data

        Raises:
            ServiceError: If the tenant doesn't exist or there's an error during update
        """
        try:
            tenant_id = self._get_current_tenant_id()

            tenant = self.session.query(Tenant).filter(Tenant.tenant_id == tenant_id).first()

            if not tenant:
                raise ServiceError(
                    f"Tenant not found: {tenant_id}",
                    error_code=ErrorCode.NOT_FOUND,
                    tenant_id=tenant_id,
                )

            tenant.is_active = False
            tenant.updated_at = datetime.utcnow()

            # Transaction managed by caller

            self.logger.info(f"Deactivated tenant: tenant_id={tenant_id}")

            return TenantRead.model_validate(tenant)

        except Exception as e:
            # Transaction managed by caller
            if isinstance(e, ServiceError):
                raise
            self._handle_service_exception("deactivate_tenant", e)

    @operation()
    def deactivate_current_tenant(self) -> bool:
        """
        Deactivate the current tenant from context.

        Returns:
            True if successful, False if tenant not found
        """
        try:
            tenant_id = self._get_current_tenant_id()

            tenant = self.session.query(Tenant).filter(Tenant.tenant_id == tenant_id).first()

            if not tenant:
                return False

            tenant.is_active = False
            tenant.updated_at = datetime.utcnow()

            # Transaction managed by caller

            self.logger.info(f"Deactivated tenant: tenant_id={tenant_id}")

            return True

        except Exception as e:
            # Transaction managed by caller
            if isinstance(e, ServiceError):
                raise
            self._handle_service_exception("deactivate_current_tenant", e)

    @operation()
    def update_tenant_from_dict(self, **kwargs) -> TenantRead:
        """
        Update the current tenant from dictionary data.

        Args:
            **kwargs: Fields to update as keyword arguments

        Returns:
            Updated tenant data

        Raises:
            ServiceError: If the tenant doesn't exist or there's an error during update
        """
        try:
            # Convert kwargs to TenantUpdate schema
            update_data = TenantUpdate(**kwargs)
            return self.update_tenant(update_data)

        except Exception as e:
            if isinstance(e, ServiceError):
                raise
            self._handle_service_exception("update_tenant_from_dict", e)
