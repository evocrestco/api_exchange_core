"""
Simplified API token models for V2 framework.

Just the data structure - no business logic or class methods.
All operations handled by utility functions.
"""

from sqlalchemy import Boolean, Column, DateTime, Index, Integer, String

from .db_base import JSON, EncryptedBinary, TimestampMixin, UUIDMixin
from .db_config import Base


class APIToken(Base, UUIDMixin, TimestampMixin):
    """Simple API token model - just data, no logic."""

    __tablename__ = "api_tokens"

    # Core fields
    tenant_id = Column(String(100), nullable=False, index=True)
    api_provider = Column(String(50), nullable=False)
    token_value = Column(EncryptedBinary, nullable=False)  # Encrypted storage

    # Lifecycle
    expires_at = Column(DateTime, nullable=False, index=True)
    last_used_at = Column(DateTime, nullable=True)
    usage_count = Column(Integer, nullable=False, default=0)
    is_active = Column(Boolean, nullable=False, default=True)

    # Context data
    context = Column(JSON, nullable=True)

    # Simple composite index for common queries
    __table_args__ = (Index("ix_api_token_lookup", "tenant_id", "api_provider", "is_active"),)
