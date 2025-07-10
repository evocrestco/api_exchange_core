"""
Simplified tenant models for V2 framework.

Just the data structure - no business logic or class methods.
"""

from sqlalchemy import Boolean, Column, Index, String

from .db_base import JSON, TimestampMixin, UUIDMixin
from .db_config import Base


class Tenant(Base, UUIDMixin, TimestampMixin):
    """Simple tenant model - just data, no logic."""

    __tablename__ = "tenant"

    # Use standard UUID primary key + separate tenant_id field
    tenant_id = Column(String(100), nullable=False, unique=True, index=True)
    name = Column(String(200), nullable=False)
    description = Column(String(500), nullable=True)
    is_active = Column(Boolean, nullable=False, default=True)

    # Configuration stored as JSON
    config = Column(JSON, nullable=True)

    # Index for tenant_id lookups
    __table_args__ = (Index("ix_tenant_lookup", "tenant_id"),)
