"""
Pydantic schemas for tenant configuration.

Defines the structure and validation for tenant configuration data.
"""

from typing import Any, Dict, List, Optional, Union

from pydantic import BaseModel, ConfigDict, Field, field_validator

from ..exceptions import ValidationError


class BaseTenantConfigSchema(BaseModel):
    """Base schema for all tenant configuration types."""

    model_config = ConfigDict(
        str_strip_whitespace=True,
        validate_assignment=True,
        extra="forbid",  # Don't allow extra fields
    )


class DatabaseConfigSchema(BaseTenantConfigSchema):
    """Database configuration for tenant."""

    connection_pool_size: Optional[int] = Field(None, gt=0, le=100, description="Connection pool size")
    query_timeout: Optional[int] = Field(None, gt=0, le=300, description="Query timeout in seconds")
    retry_attempts: Optional[int] = Field(None, ge=0, le=10, description="Number of retry attempts")
    isolation_level: Optional[str] = Field(None, description="Database isolation level")

    @field_validator("isolation_level")
    @classmethod
    def validate_isolation_level(cls, v):
        """Validate isolation level values."""
        if v is not None:
            allowed_levels = {
                "READ_UNCOMMITTED",
                "READ_COMMITTED",
                "REPEATABLE_READ",
                "SERIALIZABLE",
            }
            if v.upper() not in allowed_levels:
                raise ValueError(f"Isolation level must be one of: {allowed_levels}")
        return v.upper() if v else v


class ProcessingConfigSchema(BaseTenantConfigSchema):
    """Processing configuration for tenant."""

    max_concurrent_processes: Optional[int] = Field(None, gt=0, le=100, description="Max concurrent processes")
    batch_size: Optional[int] = Field(None, gt=0, le=10000, description="Processing batch size")
    timeout: Optional[int] = Field(None, gt=0, le=3600, description="Processing timeout in seconds")
    retry_delay: Optional[int] = Field(None, ge=0, le=300, description="Retry delay in seconds")
    enable_dead_letter: Optional[bool] = Field(None, description="Enable dead letter queue")
    max_retries: Optional[int] = Field(None, ge=0, le=10, description="Maximum retry attempts")


class ApiConfigSchema(BaseTenantConfigSchema):
    """API configuration for tenant."""

    rate_limit_requests: Optional[int] = Field(None, gt=0, le=10000, description="Rate limit requests per minute")
    rate_limit_window: Optional[int] = Field(None, gt=0, le=3600, description="Rate limit window in seconds")
    allowed_origins: Optional[List[str]] = Field(None, description="CORS allowed origins")
    api_version: Optional[str] = Field(None, description="API version")
    enable_swagger: Optional[bool] = Field(None, description="Enable Swagger documentation")

    @field_validator("allowed_origins")
    @classmethod
    def validate_origins(cls, v):
        """Validate origin URLs."""
        if v is not None:
            for origin in v:
                if not origin.startswith(("http://", "https://", "*")):
                    raise ValueError(f"Invalid origin format: {origin}")
        return v


class SecurityConfigSchema(BaseTenantConfigSchema):
    """Security configuration for tenant."""

    encryption_enabled: Optional[bool] = Field(None, description="Enable encryption")
    token_expiry: Optional[int] = Field(None, gt=0, le=86400, description="Token expiry in seconds")
    require_https: Optional[bool] = Field(None, description="Require HTTPS")
    allowed_ips: Optional[List[str]] = Field(None, description="Allowed IP addresses")
    audit_enabled: Optional[bool] = Field(None, description="Enable audit logging")

    @field_validator("allowed_ips")
    @classmethod
    def validate_ips(cls, v):
        """Validate IP addresses."""
        if v is not None:
            import ipaddress

            for ip in v:
                try:
                    ipaddress.ip_network(ip, strict=False)
                except ValueError:
                    raise ValueError(f"Invalid IP address or CIDR: {ip}")
        return v


class EnvironmentConfigSchema(BaseTenantConfigSchema):
    """Environment configuration for tenant."""

    environment: Optional[str] = Field(None, description="Environment name")
    region: Optional[str] = Field(None, description="Deployment region")
    features: Optional[List[str]] = Field(None, description="Enabled features")
    debug_mode: Optional[bool] = Field(None, description="Enable debug mode")
    log_level: Optional[str] = Field(None, description="Logging level")

    @field_validator("environment")
    @classmethod
    def validate_environment(cls, v):
        """Validate environment values."""
        if v is not None:
            allowed_envs = {"dev", "test", "staging", "prod", "production"}
            if v.lower() not in allowed_envs:
                raise ValueError(f"Environment must be one of: {allowed_envs}")
        return v.lower() if v else v

    @field_validator("log_level")
    @classmethod
    def validate_log_level(cls, v):
        """Validate log level values."""
        if v is not None:
            allowed_levels = {"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"}
            if v.upper() not in allowed_levels:
                raise ValueError(f"Log level must be one of: {allowed_levels}")
        return v.upper() if v else v


class CustomTenantConfigSchema(BaseTenantConfigSchema):
    """Custom tenant configuration for unique requirements."""

    config_data: Dict[str, Any] = Field(..., description="Custom configuration data")

    @field_validator("config_data")
    @classmethod
    def validate_not_empty(cls, v):
        """Ensure config data is not empty."""
        if not v:
            raise ValueError("Custom config data cannot be empty")
        return v


# Union type for all supported tenant configuration types
TenantConfigData = Union[
    DatabaseConfigSchema,
    ProcessingConfigSchema,
    ApiConfigSchema,
    SecurityConfigSchema,
    EnvironmentConfigSchema,
    CustomTenantConfigSchema,
]


class TenantCreate(BaseModel):
    """Schema for creating tenants."""

    tenant_id: str = Field(..., min_length=1, max_length=100, description="Tenant identifier")
    name: str = Field(..., min_length=1, max_length=200, description="Tenant name")
    description: Optional[str] = Field(None, max_length=500, description="Tenant description")
    is_active: bool = Field(default=True, description="Tenant active status")
    config: Optional[TenantConfigData] = Field(None, description="Tenant configuration")

    @field_validator("tenant_id")
    @classmethod
    def validate_tenant_id(cls, v):
        """Validate tenant ID format."""
        import re

        # Allow alphanumeric, underscore, hyphen
        if not re.match(r"^[a-zA-Z0-9_-]+$", v):
            raise ValueError("Tenant ID can only contain letters, numbers, underscore, and hyphen")
        return v.lower()  # Store as lowercase for consistency


class TenantRead(BaseModel):
    """Schema for reading tenants."""

    id: str = Field(..., description="Internal tenant UUID")
    tenant_id: str = Field(..., description="Tenant identifier")
    name: str = Field(..., description="Tenant name")
    description: Optional[str] = Field(None, description="Tenant description")
    is_active: bool = Field(..., description="Tenant active status")
    config: Optional[TenantConfigData] = Field(None, description="Tenant configuration")
    created_at: str = Field(..., description="Creation timestamp")
    updated_at: str = Field(..., description="Last update timestamp")

    model_config = ConfigDict(from_attributes=True)  # Allow creation from SQLAlchemy models


class TenantUpdate(BaseModel):
    """Schema for updating tenants."""

    name: Optional[str] = Field(None, min_length=1, max_length=200, description="Updated tenant name")
    description: Optional[str] = Field(None, max_length=500, description="Updated tenant description")
    is_active: Optional[bool] = Field(None, description="Updated active status")
    config: Optional[TenantConfigData] = Field(None, description="Updated tenant configuration")


def serialize_tenant_config(config: TenantConfigData) -> str:
    """
    Serialize tenant configuration to JSON string for storage.

    Args:
        config: Pydantic tenant configuration model

    Returns:
        JSON string representation
    """
    return config.model_dump_json()


def deserialize_tenant_config(data: str) -> TenantConfigData:
    """
    Deserialize tenant configuration from JSON string.

    Args:
        data: JSON string from database

    Returns:
        Pydantic tenant configuration model

    Raises:
        ValidationError: If data cannot be deserialized to any known configuration type
    """
    import json

    try:
        raw_data = json.loads(data)
    except json.JSONDecodeError as e:
        raise ValidationError(f"Invalid JSON data: {e}")

    # Try to determine configuration type from data structure
    config_types = [
        DatabaseConfigSchema,
        ProcessingConfigSchema,
        ApiConfigSchema,
        SecurityConfigSchema,
        EnvironmentConfigSchema,
        CustomTenantConfigSchema,  # Try this last as it's most permissive
    ]

    for config_class in config_types:
        try:
            return config_class.model_validate(raw_data)
        except Exception:
            continue

    # If no specific type matches, fall back to CustomTenantConfigSchema
    return CustomTenantConfigSchema(config_data=raw_data)
