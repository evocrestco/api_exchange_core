"""
Schema factory to eliminate CRUD schema duplication.

This module provides factory functions that generate Create/Read/Update/Filter
schemas dynamically, eliminating the need for 20+ repetitive schema classes.
"""

from datetime import datetime
from typing import Any, Dict, List, Optional, Type

from pydantic import BaseModel, Field, create_model


def create_crud_schemas(
    base_name: str,
    base_fields: Dict[str, Any],
    include_tenant: bool = True,
    include_timestamps: bool = True,
    filterable_fields: Optional[List[str]] = None,
) -> tuple:
    """
    Generate Create/Read/Update/Filter schemas for any domain object.

    Args:
        base_name: Base name for schemas (e.g., "Tenant", "Credential")
        base_fields: Dictionary of field definitions
        include_tenant: Whether to include tenant_id field
        include_timestamps: Whether to include timestamp fields
        filterable_fields: List of fields that can be filtered on

    Returns:
        Tuple of (CreateSchema, ReadSchema, UpdateSchema, FilterSchema)
    """
    # Create schema - just the base fields
    create_fields = base_fields.copy()
    if include_tenant:
        create_fields["tenant_id"] = (str, Field(description="Tenant identifier"))

    CreateSchema = create_model(f"{base_name}Create", **create_fields)

    # Read schema - base fields + id + timestamps
    read_fields = base_fields.copy()
    read_fields["id"] = (str, Field(description="Unique identifier"))

    if include_tenant:
        read_fields["tenant_id"] = (str, Field(description="Tenant identifier"))

    if include_timestamps:
        read_fields["created_at"] = (datetime, Field(description="Creation timestamp"))
        read_fields["updated_at"] = (datetime, Field(description="Last update timestamp"))

    ReadSchema = create_model(f"{base_name}Read", **read_fields)

    # Update schema - optional versions of base fields
    update_fields = {}
    for field_name, field_def in base_fields.items():
        if isinstance(field_def, tuple):
            field_type, field_info = field_def
            update_fields[field_name] = (
                Optional[field_type],
                Field(default=None, description=field_info.description),
            )
        else:
            update_fields[field_name] = (Optional[field_def], Field(default=None))

    UpdateSchema = create_model(f"{base_name}Update", **update_fields)

    # Filter schema - filterable fields + date range
    filter_fields = {}

    if filterable_fields:
        for field_name in filterable_fields:
            if field_name in base_fields:
                field_def = base_fields[field_name]
                if isinstance(field_def, tuple):
                    field_type, _ = field_def
                    filter_fields[field_name] = (Optional[field_type], Field(default=None))
                else:
                    filter_fields[field_name] = (Optional[field_def], Field(default=None))

    if include_tenant:
        filter_fields["tenant_id"] = (
            Optional[str],
            Field(default=None, description="Filter by tenant"),
        )

    # Add date range filtering
    filter_fields["created_after"] = (
        Optional[datetime],
        Field(default=None, description="Filter by creation date"),
    )
    filter_fields["created_before"] = (
        Optional[datetime],
        Field(default=None, description="Filter by creation date"),
    )
    filter_fields["updated_after"] = (
        Optional[datetime],
        Field(default=None, description="Filter by update date"),
    )
    filter_fields["updated_before"] = (
        Optional[datetime],
        Field(default=None, description="Filter by update date"),
    )

    FilterSchema = create_model(f"{base_name}Filter", **filter_fields)

    return CreateSchema, ReadSchema, UpdateSchema, FilterSchema


def create_simple_schema(name: str, fields: Dict[str, Any]) -> Type[BaseModel]:
    """
    Create a simple schema with the given fields.

    Args:
        name: Schema name
        fields: Dictionary of field definitions

    Returns:
        Pydantic model class
    """
    return create_model(name, **fields)


def create_enum_schema(name: str, values: List[str]) -> Type[BaseModel]:
    """
    Create a simple enum schema.

    Args:
        name: Schema name
        values: List of enum values

    Returns:
        Pydantic model class with enum field
    """
    from enum import Enum

    enum_class = Enum(name, {val.upper(): val for val in values})
    return create_model(name, value=(enum_class, Field(description=f"{name} value")))


# Pre-built schema sets for common domain objects
def create_tenant_schemas():
    """Create tenant CRUD schemas."""
    fields = {
        "name": (str, Field(description="Tenant name")),
        "config": (
            Optional[Dict[str, Any]],
            Field(default_factory=dict, description="Tenant configuration"),
        ),
    }
    return create_crud_schemas("Tenant", fields, include_tenant=False, filterable_fields=["name"])


def create_credential_schemas():
    """Create credential CRUD schemas."""
    fields = {
        "system_name": (str, Field(description="External system name")),
        "credential_data": (Dict[str, Any], Field(description="Encrypted credential data")),
        "expires_at": (
            Optional[datetime],
            Field(default=None, description="Credential expiration"),
        ),
    }
    return create_crud_schemas("Credential", fields, filterable_fields=["system_name"])


def create_api_token_schemas():
    """Create API token CRUD schemas."""
    fields = {
        "api_provider": (str, Field(description="API provider name")),
        "token_value": (str, Field(description="Token value")),
        "expires_at": (datetime, Field(description="Token expiration")),
        "is_active": (bool, Field(default=True, description="Token active status")),
        "metadata": (
            Optional[Dict[str, Any]],
            Field(default_factory=dict, description="Token metadata"),
        ),
    }
    return create_crud_schemas("APIToken", fields, filterable_fields=["api_provider", "is_active"])


# Export commonly used schemas
TenantCreate, TenantRead, TenantUpdate, TenantFilter = create_tenant_schemas()
CredentialCreate, CredentialRead, CredentialUpdate, CredentialFilter = create_credential_schemas()
APITokenCreate, APITokenRead, APITokenUpdate, APITokenFilter = create_api_token_schemas()
