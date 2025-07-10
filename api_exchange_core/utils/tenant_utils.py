"""
Simple tenant utilities using generic CRUD helpers and Pydantic schemas.

This module provides tenant operations using the generic CRUD system
and Pydantic schemas for type safety and validation.
"""

from typing import Any, Dict, List, Optional

from sqlalchemy.orm import Session

from ..db.db_tenant_models import Tenant
from ..exceptions import duplicate
from ..schemas.tenant_schemas import (
    TenantCreate,
)
from ..utils.crud_helpers import (
    create_record,
    delete_record,
    get_record,
    list_records,
    update_record,
)


def get_tenant_config(session: Session, tenant_id: str, key: Optional[str] = None) -> Any:
    """
    Get tenant configuration using generic CRUD.

    Args:
        session: Database session
        tenant_id: Tenant identifier
        key: Optional specific config key

    Returns:
        Configuration value or full config dict

    Raises:
        RepositoryError: If tenant not found
    """
    # Use generic CRUD to get tenant by tenant_id
    tenant = get_record(session, Tenant, {"tenant_id": tenant_id})

    if not tenant:
        from ..exceptions import not_found

        raise not_found("Tenant", tenant_id=tenant_id)

    # Config is already deserialized by SQLAlchemy's JSON type
    # No need to deserialize - it's already a dict/list/primitive
    config = tenant.config if tenant.config is not None else {}

    if key:
        # For key access, return the raw attribute if config is a Pydantic model
        if hasattr(config, key):
            return getattr(config, key)
        elif isinstance(config, dict):  # type: ignore[unreachable]
            return config.get(key)
        else:
            return None

    return config


def create_tenant(session: Session, tenant_create: TenantCreate) -> str:
    """
    Create a new tenant using generic CRUD and Pydantic validation.

    Args:
        session: Database session
        tenant_create: TenantCreate schema with validated data

    Returns:
        Tenant ID

    Raises:
        DuplicateError: If tenant already exists
    """
    # Check if tenant already exists using generic CRUD
    existing = get_record(session, Tenant, {"tenant_id": tenant_create.tenant_id})
    if existing:
        raise duplicate("Tenant", tenant_id=tenant_create.tenant_id)

    # Convert config to dict if it's a Pydantic model
    config_data = None
    if tenant_create.config:
        # If it's a Pydantic model, convert to dict
        if hasattr(tenant_create.config, "model_dump"):
            config_data = tenant_create.config.model_dump()
        else:
            config_data = tenant_create.config

    # Create tenant data
    tenant_data = {
        "tenant_id": tenant_create.tenant_id,
        "name": tenant_create.name,
        "description": tenant_create.description,
        "is_active": tenant_create.is_active,
        "config": config_data,
    }

    # Create using generic CRUD
    new_tenant = create_record(session, Tenant, tenant_data)
    return str(new_tenant.tenant_id)


def update_tenant(
    session: Session,
    tenant_id: str,
    tenant_name: Optional[str] = None,
    config: Optional[Dict[str, Any]] = None,
) -> bool:
    """
    Update an existing tenant using generic CRUD.

    Args:
        session: Database session
        tenant_id: Tenant identifier
        tenant_name: Optional new tenant name
        config: Optional new tenant configuration

    Returns:
        True if updated, False if not found
    """
    # Prepare update data
    update_data: Dict[str, Any] = {}
    if tenant_name:
        update_data["name"] = tenant_name
    if config:
        update_data["config"] = config

    if not update_data:
        return False

    # Find existing tenant using generic CRUD
    existing = get_record(session, Tenant, {"tenant_id": tenant_id})
    if not existing:
        return False

    # Update using generic CRUD with the actual UUID id
    updated = update_record(session, Tenant, str(existing.id), update_data)
    return updated is not None


def update_tenant_config(session: Session, tenant_id: str, key: str, value: Any) -> bool:
    """
    Update a specific tenant configuration key using generic CRUD.

    Args:
        session: Database session
        tenant_id: Tenant identifier
        key: Configuration key
        value: Configuration value

    Returns:
        True if updated, False if tenant not found
    """
    # Get current tenant using generic CRUD
    tenant = get_record(session, Tenant, {"tenant_id": tenant_id})

    if not tenant:
        return False

    # Config is already deserialized by SQLAlchemy's JSON type
    config = tenant.config if tenant.config is not None else {}

    # Create a new dict to ensure SQLAlchemy detects the change
    new_config = dict(config)
    new_config[key] = value

    # Update using generic CRUD with the actual UUID id
    # SQLAlchemy's JSON type will handle serialization automatically
    updated = update_record(session, Tenant, str(tenant.id), {"config": new_config})
    return updated is not None


def list_tenants(session: Session, limit: Optional[int] = None, offset: Optional[int] = None) -> List[Dict[str, Any]]:
    """
    List all tenants using generic CRUD.

    Args:
        session: Database session
        limit: Optional limit
        offset: Optional offset

    Returns:
        List of tenant dictionaries
    """
    # Use generic CRUD to list tenants
    tenants = list_records(session, Tenant, limit=limit, offset=offset, order_by="created_at")

    return [
        {
            "id": tenant.id,
            "tenant_id": tenant.tenant_id,
            "name": tenant.name,
            "config": tenant.config or {},
            "created_at": tenant.created_at.isoformat(),
            "updated_at": tenant.updated_at.isoformat(),
        }
        for tenant in tenants
    ]


def delete_tenant(session: Session, tenant_id: str) -> bool:
    """
    Delete a tenant using generic CRUD.

    Args:
        session: Database session
        tenant_id: Tenant identifier

    Returns:
        True if deleted, False if not found
    """
    # Find existing tenant using generic CRUD
    existing = get_record(session, Tenant, {"tenant_id": tenant_id})
    if not existing:
        return False

    # Use generic CRUD to delete tenant with the actual UUID id
    return delete_record(session, Tenant, str(existing.id))
