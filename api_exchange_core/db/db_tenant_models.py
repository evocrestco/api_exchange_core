"""
Tenant models for the Entity Integration System.
"""

from typing import Any

from sqlalchemy import Boolean, Column, Index, String, Text
from sqlalchemy.orm import relationship

from .db_base import JSON, BaseModel
from .db_config import Base


class TenantNotFoundError(ValueError):
    """Raised when a tenant is not found."""

    pass


class Tenant(Base, BaseModel):
    """
    Tenant model for managing customer information.
    """

    # Core tenant identifiers
    tenant_id = Column(String(100), nullable=False, unique=True)
    customer_name = Column(String(200), nullable=False)

    # Status and visibility
    is_active = Column(Boolean, default=True, nullable=False)

    # Contact information
    primary_contact_name = Column(String(200), nullable=True)
    primary_contact_email = Column(String(200), nullable=True)
    primary_contact_phone = Column(String(50), nullable=True)

    # Address information
    address_line1 = Column(String(200), nullable=True)
    address_line2 = Column(String(200), nullable=True)
    city = Column(String(100), nullable=True)
    state = Column(String(100), nullable=True)
    postal_code = Column(String(20), nullable=True)
    country = Column(String(100), nullable=True)

    # Additional tenant configuration
    tenant_config = Column(JSON, nullable=False, default=dict)
    notes = Column(Text, nullable=True)

    entities = relationship("Entity", backref="tenant", lazy="dynamic")

    # Indexes for efficient queries
    __table_args__ = (
        Index("ix_tenant_tenant_id", "tenant_id"),
        Index("ix_tenant_customer_name", "customer_name"),
        Index("ix_tenant_is_active", "is_active"),
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

        config_value = self.tenant_config[key]
        # Handle nested dict config (e.g., {"value": actual_value})
        if hasattr(config_value, "get") and "value" in config_value:
            return config_value["value"]
        return config_value
