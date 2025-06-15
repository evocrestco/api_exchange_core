"""
Pydantic schemas for credential management operations.

This module provides schemas for validating and serializing credential data
while maintaining security by never exposing sensitive credential content.
"""

from datetime import datetime
from typing import Any, Dict, Optional

from pydantic import BaseModel, ConfigDict, Field

from src.schemas.mixins import TimestampMixin


class CredentialRead(TimestampMixin):
    """
    Schema for reading credential metadata (excludes sensitive data).

    This schema provides credential metadata without exposing the actual
    encrypted credential content for security purposes.
    """

    model_config = ConfigDict(from_attributes=True)

    id: str = Field(..., description="Unique credential identifier")
    tenant_id: str = Field(..., description="Tenant identifier")
    system_name: str = Field(..., description="External system name")
    auth_type: str = Field(..., description="Authentication type")
    is_active: str = Field(..., description="Credential status")
    expires_at: Optional[datetime] = Field(None, description="Expiration timestamp")

    # Note: encrypted_credentials field is intentionally excluded for security


class CredentialCreate(BaseModel):
    """
    Schema for creating new credentials.
    """

    model_config = ConfigDict(extra="forbid")

    system_name: str = Field(..., min_length=1, max_length=100, description="External system name")
    auth_type: str = Field(..., min_length=1, max_length=50, description="Authentication type")
    credentials: Dict[str, Any] = Field(..., description="Credential data to encrypt")
    expires_at: Optional[datetime] = Field(None, description="Optional expiration timestamp")


class CredentialUpdate(BaseModel):
    """
    Schema for updating existing credentials.
    """

    model_config = ConfigDict(extra="forbid")

    credentials: Dict[str, Any] = Field(..., description="New credential data to encrypt")
    expires_at: Optional[datetime] = Field(None, description="Optional new expiration timestamp")


class CredentialFilter(BaseModel):
    """
    Schema for filtering credentials in list operations.
    """

    model_config = ConfigDict(extra="forbid")

    system_name: Optional[str] = Field(None, description="Filter by system name")
    auth_type: Optional[str] = Field(None, description="Filter by authentication type")
    is_active: Optional[str] = Field(None, description="Filter by status")
    include_expired: bool = Field(False, description="Include expired credentials")
