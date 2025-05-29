"""
Pydantic schemas for Tenant models in the Entity Integration System.

This module defines validation schemas for tenant-related data transfer.
"""

from datetime import datetime
from typing import Any, Dict, Optional, Union

from pydantic import BaseModel, ConfigDict, Field, field_validator

from src.schemas.mixins import DateRangeFilterMixin, IdMixin, TimestampMixin


class TenantConfigValue(BaseModel):
    """
    Schema for a tenant configuration value.
    """

    value: Any
    updated_at: Optional[str] = Field(default_factory=lambda: datetime.now().isoformat())

    model_config = ConfigDict(
        from_attributes=True,  # Enables ORM mode
        arbitrary_types_allowed=True,  # Allows any type for configuration values
    )


class TenantCreate(BaseModel):
    """
    Schema for creating a new tenant.
    """

    tenant_id: str = Field(min_length=1, max_length=100)
    customer_name: str = Field(min_length=1, max_length=200)

    # Contact information
    primary_contact_name: Optional[str] = Field(default=None, max_length=200)
    primary_contact_email: Optional[str] = Field(default=None, max_length=200)
    primary_contact_phone: Optional[str] = Field(default=None, max_length=50)

    # Address information
    address_line1: Optional[str] = Field(default=None, max_length=200)
    address_line2: Optional[str] = Field(default=None, max_length=200)
    city: Optional[str] = Field(default=None, max_length=100)
    state: Optional[str] = Field(default=None, max_length=100)
    postal_code: Optional[str] = Field(default=None, max_length=20)
    country: Optional[str] = Field(default=None, max_length=100)

    # Configuration
    tenant_config: Optional[Dict[str, Union[TenantConfigValue, Any]]] = Field(
        default_factory=lambda: {}
    )
    notes: Optional[str] = Field(default=None)

    # Status
    is_active: bool = Field(default=True)

    model_config = ConfigDict(
        from_attributes=True, extra="ignore"  # Enables ORM mode  # Ignore extra fields
    )

    @field_validator("primary_contact_email")
    def validate_email(cls, v: Optional[str]) -> Optional[str]:
        """
        Basic email validation.
        """
        if v and "@" not in v:
            raise ValueError("Invalid email address")
        return v


class TenantUpdate(BaseModel):
    """
    Schema for updating an existing tenant.
    """

    customer_name: Optional[str] = Field(default=None, max_length=200)

    # Contact information
    primary_contact_name: Optional[str] = Field(default=None, max_length=200)
    primary_contact_email: Optional[str] = Field(default=None, max_length=200)
    primary_contact_phone: Optional[str] = Field(default=None, max_length=50)

    # Address information
    address_line1: Optional[str] = Field(default=None, max_length=200)
    address_line2: Optional[str] = Field(default=None, max_length=200)
    city: Optional[str] = Field(default=None, max_length=100)
    state: Optional[str] = Field(default=None, max_length=100)
    postal_code: Optional[str] = Field(default=None, max_length=20)
    country: Optional[str] = Field(default=None, max_length=100)

    # Status
    is_active: Optional[bool] = None

    model_config = ConfigDict(
        from_attributes=True, extra="ignore"  # Enables ORM mode  # Ignore extra fields
    )

    @field_validator("primary_contact_email")
    def validate_email(cls, v: Optional[str]) -> Optional[str]:
        """
        Basic email validation.
        """
        if v and "@" not in v:
            raise ValueError("Invalid email address")
        return v


class TenantRead(IdMixin, TimestampMixin):
    """
    Schema for reading tenant data.
    """

    tenant_id: str
    customer_name: str

    # Contact information
    primary_contact_name: Optional[str] = None
    primary_contact_email: Optional[str] = None
    primary_contact_phone: Optional[str] = None

    # Address information
    address_line1: Optional[str] = None
    address_line2: Optional[str] = None
    city: Optional[str] = None
    state: Optional[str] = None
    postal_code: Optional[str] = None
    country: Optional[str] = None

    # Configuration
    tenant_config: Optional[Dict[str, Union[TenantConfigValue, Any]]] = None
    notes: Optional[str] = None

    # Status
    is_active: bool

    model_config = ConfigDict(
        from_attributes=True,  # Enables ORM mode
        extra="ignore",  # Ignore extra fields
        arbitrary_types_allowed=True,  # Allows any type for configuration values
    )

    def get_config_value(self, key: str, default: Any = None) -> Any:
        """
        Get a configuration value for this tenant.

        Args:
            key: Configuration key
            default: Default value if key not found

        Returns:
            Configuration value or default
        """
        if not self.tenant_config or key not in self.tenant_config:
            return default

        config_item = self.tenant_config[key]
        if isinstance(config_item, TenantConfigValue):
            return config_item.value
        else:
            # Legacy support: config_item might be a raw value
            return config_item


class TenantConfigUpdate(BaseModel):
    """
    Schema for updating tenant configuration.
    """

    key: str
    value: Any

    model_config = ConfigDict(
        from_attributes=True,  # Enables ORM mode
        extra="ignore",  # Ignore extra fields
        arbitrary_types_allowed=True,  # Allows any type for configuration values
    )


class TenantFilter(DateRangeFilterMixin):
    """
    Schema for filtering tenants.
    """

    tenant_id: Optional[str] = Field(default=None)
    customer_name: Optional[str] = Field(default=None)
    is_active: Optional[bool] = Field(default=None)

    model_config = ConfigDict(
        from_attributes=True,  # Enables ORM mode
        extra="ignore",  # Ignore extra fields
    )
