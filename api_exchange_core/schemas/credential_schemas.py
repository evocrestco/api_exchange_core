"""
Pydantic schemas for external credentials.

Defines the structure and validation for different types of credentials
used to authenticate with external systems.
"""

from datetime import datetime
from typing import Any, Dict, List, Optional, Type, Union

from pydantic import BaseModel, ConfigDict, Field, field_validator

from ..exceptions import ValidationError


class BaseCredentialSchema(BaseModel):
    """Base schema for all credential types."""

    model_config = ConfigDict(
        str_strip_whitespace=True,
        validate_assignment=True,
        extra="forbid",  # Don't allow extra fields
    )


class OAuthCredentials(BaseCredentialSchema):
    """OAuth 2.0 credentials schema."""

    access_token: str = Field(..., min_length=1, description="OAuth access token")
    refresh_token: Optional[str] = Field(None, description="OAuth refresh token")
    token_type: str = Field(default="Bearer", description="Token type")
    expires_in: Optional[int] = Field(None, gt=0, description="Token expiry in seconds")
    scope: Optional[str] = Field(None, description="Token scope")

    @field_validator("token_type")
    @classmethod
    def validate_token_type(cls, v):
        """Validate token type is a known value."""
        allowed_types = {"Bearer", "bearer", "Basic", "basic"}
        if v not in allowed_types:
            raise ValueError(f"Token type must be one of: {allowed_types}")
        return v


class APIKeyCredentials(BaseCredentialSchema):
    """API key credentials schema."""

    api_key: str = Field(..., min_length=1, description="API key")
    secret_key: Optional[str] = Field(None, description="Secret key or API secret")
    key_id: Optional[str] = Field(None, description="Key identifier")

    @field_validator("api_key", "secret_key")
    @classmethod
    def validate_no_whitespace(cls, v):
        """Ensure keys don't have leading/trailing whitespace."""
        if v and v != v.strip():
            raise ValueError("API keys cannot have leading or trailing whitespace")
        return v


class BasicAuthCredentials(BaseCredentialSchema):
    """Basic authentication credentials schema."""

    username: str = Field(..., min_length=1, description="Username")
    password: str = Field(..., min_length=1, description="Password")

    @field_validator("username")
    @classmethod
    def validate_username(cls, v):
        """Validate username format."""
        if not v or v.isspace():
            raise ValueError("Username cannot be empty or whitespace")
        return v


class AzureServicePrincipalCredentials(BaseCredentialSchema):
    """Azure Service Principal credentials schema."""

    tenant_id: str = Field(..., min_length=1, description="Azure tenant ID")
    client_id: str = Field(..., min_length=1, description="Application/client ID")
    client_secret: str = Field(..., min_length=1, description="Client secret")
    resource: Optional[str] = Field(None, description="Resource URI")

    @field_validator("tenant_id", "client_id")
    @classmethod
    def validate_guid_format(cls, v):
        """Validate GUID format for Azure IDs."""
        import re

        guid_pattern = r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$"
        if not re.match(guid_pattern, v):
            raise ValueError("Must be a valid GUID format")
        return v


class DatabaseCredentials(BaseCredentialSchema):
    """Database connection credentials schema."""

    host: str = Field(..., min_length=1, description="Database host")
    port: int = Field(..., gt=0, le=65535, description="Database port")
    database: str = Field(..., min_length=1, description="Database name")
    username: str = Field(..., min_length=1, description="Database username")
    password: str = Field(..., min_length=1, description="Database password")
    ssl_mode: Optional[str] = Field(None, description="SSL mode")

    @field_validator("ssl_mode")
    @classmethod
    def validate_ssl_mode(cls, v):
        """Validate SSL mode values."""
        if v is not None:
            allowed_modes = {"disable", "allow", "prefer", "require", "verify-ca", "verify-full"}
            if v.lower() not in allowed_modes:
                raise ValueError(f"SSL mode must be one of: {allowed_modes}")
        return v


class CustomCredentials(BaseCredentialSchema):
    """Custom credentials for systems with unique requirements."""

    credentials: Dict[str, Any] = Field(..., description="Custom credential data")

    @field_validator("credentials")
    @classmethod
    def validate_not_empty(cls, v):
        """Ensure credentials dict is not empty."""
        if not v:
            raise ValueError("Custom credentials cannot be empty")
        return v


# Union type for all supported credential types
CredentialData = Union[
    OAuthCredentials,
    APIKeyCredentials,
    BasicAuthCredentials,
    AzureServicePrincipalCredentials,
    DatabaseCredentials,
    CustomCredentials,
]


class ExternalCredentialCreate(BaseModel):
    """Schema for creating external credentials."""

    system_name: str = Field(..., min_length=1, max_length=100, description="External system name")
    credential_data: CredentialData = Field(..., description="Credential data")
    expires_at: Optional[datetime] = Field(None, description="Credential expiry date")
    context: Optional[Dict[str, Any]] = Field(None, description="Additional context")

    @field_validator("system_name")
    @classmethod
    def validate_system_name(cls, v):
        """Validate system name format."""
        import re

        # Allow alphanumeric, underscore, hyphen, dot
        if not re.match(r"^[a-zA-Z0-9_.-]+$", v):
            raise ValueError("System name can only contain letters, numbers, underscore, hyphen, and dot")
        return v.lower()  # Store as lowercase for consistency


class ExternalCredentialRead(BaseModel):
    """Schema for reading external credentials."""

    id: str = Field(..., description="Credential ID")
    system_name: str = Field(..., description="External system name")
    credential_data: CredentialData = Field(..., description="Credential data")
    expires_at: Optional[datetime] = Field(None, description="Credential expiry date")
    context: Optional[Dict[str, Any]] = Field(None, description="Additional context")
    created_at: datetime = Field(..., description="Creation timestamp")
    updated_at: datetime = Field(..., description="Last update timestamp")

    model_config = ConfigDict(from_attributes=True)  # Allow creation from SQLAlchemy models


class ExternalCredentialUpdate(BaseModel):
    """Schema for updating external credentials."""

    credential_data: Optional[CredentialData] = Field(None, description="Updated credential data")
    expires_at: Optional[datetime] = Field(None, description="Updated expiry date")
    context: Optional[Dict[str, Any]] = Field(None, description="Updated context")


def serialize_credentials(credentials: CredentialData) -> str:
    """
    Serialize credential data to JSON string for storage.

    Args:
        credentials: Pydantic credential model

    Returns:
        JSON string representation
    """
    return credentials.model_dump_json()


def deserialize_credentials(data: str, credential_type: Optional[str] = None) -> CredentialData:
    """
    Deserialize credential data from JSON string.

    Args:
        data: JSON string from database
        credential_type: Optional hint about credential type

    Returns:
        Pydantic credential model

    Raises:
        ValidationError: If data cannot be deserialized to any known credential type
    """
    import json

    try:
        raw_data = json.loads(data)
    except json.JSONDecodeError as e:
        raise ValidationError(f"Invalid JSON data: {e}")

    # Try to determine credential type from data structure
    credential_types: List[Type[BaseModel]] = [
        OAuthCredentials,
        APIKeyCredentials,
        BasicAuthCredentials,
        AzureServicePrincipalCredentials,
        DatabaseCredentials,
        CustomCredentials,  # Try this last as it's most permissive
    ]

    for cred_class in credential_types:
        try:
            return cred_class.model_validate(raw_data)  # type: ignore[return-value]
        except Exception:
            continue

    # If no specific type matches, fall back to CustomCredentials
    return CustomCredentials(credentials=raw_data)
