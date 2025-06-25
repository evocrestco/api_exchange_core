"""
Pydantic schemas for Tenant models in the Entity Integration System.

This module defines validation schemas for tenant-related data transfer.
"""

from datetime import datetime
from typing import Any, Dict, Optional, Union

from pydantic import BaseModel, ConfigDict, Field, field_validator

from ..exceptions import ErrorCode, ValidationError
from .mixins import DateRangeFilterMixin, IdMixin, TimestampMixin


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
            raise ValidationError(
                "Invalid email address",
                error_code=ErrorCode.INVALID_FORMAT,
                field="primary_contact_email",
                value=v
            )
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
            raise ValidationError(
                "Invalid email address",
                error_code=ErrorCode.INVALID_FORMAT,
                field="primary_contact_email",
                value=v
            )
        return v


class TokenManagementConfig(BaseModel):
    """
    Schema for token management configuration.
    """

    refresh_buffer_minutes: int = Field(
        default=20,
        ge=5,
        le=60,
        description="How early to refresh tokens before expiry (5-60 minutes)",
    )
    cleanup_frequency_minutes: int = Field(
        default=20, ge=5, le=120, description="How often to run token cleanup (5-120 minutes)"
    )
    cleanup_age_minutes: int = Field(
        default=40,
        ge=30,
        le=180,
        description="How old tokens must be before deletion (30-180 minutes)",
    )

    model_config = ConfigDict(from_attributes=True, extra="forbid")  # Strict validation

    @field_validator("cleanup_age_minutes")
    def validate_cleanup_age(cls, v: int, info) -> int:
        """Ensure cleanup age is greater than refresh buffer."""
        refresh_buffer = info.data.get("refresh_buffer_minutes", 20)
        if v <= refresh_buffer:
            raise ValidationError(
                "cleanup_age_minutes must be greater than refresh_buffer_minutes",
                error_code=ErrorCode.CONSTRAINT_VIOLATION,
                field="cleanup_age_minutes",
                value=v
            )
        return v


def get_token_management_config(tenant_config: Dict[str, Any]) -> TokenManagementConfig:
    """
    Extract token management configuration from tenant config.

    Args:
        tenant_config: Tenant configuration dictionary

    Returns:
        TokenManagementConfig with defaults for missing values
    """
    token_config_data = tenant_config.get("token_management", {})

    # Handle TenantConfigValue wrapper if present
    if isinstance(token_config_data, dict) and "value" in token_config_data:
        token_config_data = token_config_data["value"]

    return TokenManagementConfig(**token_config_data)


def set_token_management_config(
    tenant_config: Dict[str, Any], token_config: TokenManagementConfig
) -> Dict[str, Any]:
    """
    Set token management configuration in tenant config.

    Args:
        tenant_config: Existing tenant configuration dictionary
        token_config: New token management configuration

    Returns:
        Updated tenant configuration dictionary
    """
    updated_config = tenant_config.copy()
    updated_config["token_management"] = TenantConfigValue(
        value=token_config.model_dump(), updated_at=datetime.now().isoformat()
    )
    return updated_config


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
