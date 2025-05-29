"""
Repository for Tenant data access operations.

This module provides data access methods for the Tenant entity,
following the repository pattern to encapsulate data access logic.
"""

import logging
from typing import Any, Dict, List, Optional

from src.db.db_tenant_models import Tenant
from src.exceptions import not_found
from src.repositories.base_repository import BaseRepository
from src.schemas.tenant_schema import TenantConfigUpdate, TenantCreate, TenantRead, TenantUpdate


class TenantRepository(BaseRepository[Tenant]):
    """Repository for tenant data access operations."""

    def __init__(self, db_manager, logger: Optional[logging.Logger] = None):
        """
        Initialize the tenant repository.

        Args:
            db_manager: Database manager for database operations
            logger: Optional logger instance
        """
        super().__init__(db_manager, Tenant, logger)

    def _entity_to_dict(self, tenant: Tenant) -> Dict[str, Any]:
        """
        Convert a Tenant entity to a dictionary.

        Args:
            tenant: Tenant entity

        Returns:
            Dictionary representation of the Tenant
        """
        return {
            "tenant_id": tenant.tenant_id,
            "tenant_name": tenant.tenant_name,
            "is_active": tenant.is_active,
            "tenant_config": tenant.tenant_config,
            "created_at": tenant.created_at,
            "updated_at": tenant.updated_at,
        }

    def get_by_id(self, tenant_id: str) -> TenantRead:
        """
        Get a tenant by ID.

        Args:
            tenant_id: Unique tenant identifier

        Returns:
            TenantRead schema instance

        Raises:
            RepositoryError: If no tenant exists with the given ID or there's a database error
        """
        # Note: Tenant uses tenant_id as primary key, not id
        # So we need custom logic here
        with self._db_operation("get_by_id", tenant_id) as session:
            tenant = session.query(Tenant).filter(Tenant.tenant_id == tenant_id).first()
            if not tenant:
                raise not_found(
                    "Tenant",
                    tenant_id=tenant_id,
                )
            return TenantRead.model_validate(tenant)

    def get_active_tenants(self) -> List[TenantRead]:
        """
        Get all active tenants.

        Returns:
            List of active TenantRead schema instances

        Raises:
            RepositoryError: If there's a database error
        """
        with self._db_operation("get_active_tenants") as session:
            tenants = session.query(Tenant).filter(Tenant.is_active.is_(True)).all()
            return [TenantRead.model_validate(tenant) for tenant in tenants]

    def get_all_tenants(self, include_inactive: bool = False) -> List[TenantRead]:
        """
        Get all tenants.

        Args:
            include_inactive: Whether to include inactive tenants

        Returns:
            List of TenantRead schema instances

        Raises:
            RepositoryError: If there's a database error
        """
        with self._db_operation("get_all_tenants") as session:
            if include_inactive:
                tenants = session.query(Tenant).all()
                return [TenantRead.model_validate(tenant) for tenant in tenants]
            return self.get_active_tenants()

    def create(self, tenant_data: TenantCreate) -> TenantRead:
        """
        Create a new tenant.

        Args:
            tenant_data: Validated tenant creation data

        Returns:
            Created TenantRead schema instance

        Raises:
            RepositoryError: If there's a database error or validation fails
        """
        # Convert schema to database model
        tenant = Tenant(
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
        )

        with self._db_operation("create", tenant_data.tenant_id) as session:
            session.add(tenant)
            session.flush()  # Flush to get the generated ID if any
            self.logger.info(f"Created tenant with ID: {tenant.tenant_id}")
            return TenantRead.model_validate(tenant)

    def update(self, tenant_id: str, update_data: TenantUpdate) -> TenantRead:
        """
        Update an existing tenant.

        Args:
            tenant_id: ID of tenant to update
            update_data: Validated tenant update data

        Returns:
            Updated TenantRead schema instance

        Raises:
            RepositoryError: If tenant doesn't exist, there's a database error or validation fails
        """
        with self._db_operation("update", tenant_id) as session:
            # Validate tenant exists in database
            existing_tenant = session.query(Tenant).filter_by(tenant_id=tenant_id).first()
            if not existing_tenant:
                raise not_found(
                    "Tenant",
                    tenant_id=tenant_id,
                )

            # Update only provided fields
            update_dict = update_data.model_dump(exclude_unset=True)
            for field, value in update_dict.items():
                if hasattr(existing_tenant, field):
                    setattr(existing_tenant, field, value)

            session.flush()  # Ensure changes are applied
            self.logger.info(f"Updated tenant with ID: {tenant_id}")
            return TenantRead.model_validate(existing_tenant)

    def update_config(self, tenant_id: str, config_data: TenantConfigUpdate) -> TenantRead:
        """
        Update a tenant's configuration.

        Args:
            tenant_id: ID of tenant to update
            config_data: Validated configuration update data

        Returns:
            Updated TenantRead schema instance

        Raises:
            RepositoryError: If tenant doesn't exist, there's a database error or validation fails
        """
        with self._db_operation("update_config", tenant_id) as session:
            # Get tenant from database
            tenant = session.query(Tenant).filter_by(tenant_id=tenant_id).first()
            if not tenant:
                raise not_found(
                    "Tenant",
                    tenant_id=tenant_id,
                )

            config = dict(tenant.tenant_config or {})
            config[config_data.key] = {"value": config_data.value, "updated_at": None}

            # Assign the whole config dictionary
            tenant.tenant_config = config
            session.flush()  # Ensure changes are applied
            self.logger.info(
                f"Updated configuration key '{config_data.key}' for tenant: {tenant_id}"
            )
            return TenantRead.model_validate(tenant)
