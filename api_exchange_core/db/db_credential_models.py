"""
Simplified credential models for V2 framework.

Just the data structure - no business logic or class methods.
"""

from sqlalchemy import Column, DateTime, Index, String

from .db_base import JSON, EncryptedBinary, TimestampMixin, UUIDMixin
from .db_config import Base


class ExternalCredential(Base, UUIDMixin, TimestampMixin):
    """Simple credential model - just data, no logic."""

    __tablename__ = "external_credentials"

    # Core fields
    tenant_id = Column(String(100), nullable=False, index=True)
    system_name = Column(String(100), nullable=False)
    credential_data = Column(EncryptedBinary, nullable=False)  # Encrypted storage

    # Optional expiration
    expires_at = Column(DateTime, nullable=True)

    # Context data
    context = Column(JSON, nullable=True)

    # Unique constraint for tenant + system
    __table_args__ = (Index("ix_credential_lookup", "tenant_id", "system_name", unique=True),)
