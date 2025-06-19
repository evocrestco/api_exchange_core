"""
Common Pydantic schema mixins for infrastructure-level patterns.

This module provides reusable mixins for common technical patterns across schemas,
ensuring consistency and reducing code duplication for framework-level concerns.
"""

from datetime import datetime
from typing import Any, Dict

from pydantic import BaseModel, Field


class IdMixin(BaseModel):
    """Mixin for schemas that include a unique identifier."""

    id: str = Field(..., description="Unique identifier for the record")


class TenantMixin(BaseModel):
    """Mixin for schemas that include tenant isolation (multi-tenant architecture)."""

    tenant_id: str = Field(
        ..., min_length=1, max_length=100, description="Unique identifier for the tenant"
    )


class TimestampMixin(BaseModel):
    """Mixin for schemas that include creation and update timestamps."""

    created_at: datetime = Field(..., description="Timestamp when the record was created")
    updated_at: datetime = Field(..., description="Timestamp when the record was last updated")


class AttributesMixin(BaseModel):
    """Mixin for schemas that include flexible metadata/attributes."""

    attributes: Dict[str, Any] = Field(
        default_factory=dict, description="Additional attributes or metadata"
    )


# Common combinations for convenience
class CoreEntityMixin(IdMixin, TenantMixin, TimestampMixin):
    """Common mixin for core entity schemas with ID, tenant isolation, and timestamps."""

    pass


class DateRangeFilterMixin(BaseModel):
    """Mixin for filter schemas that support date range queries."""

    created_after: datetime | None = Field(
        None, description="Filter for records created after this timestamp"
    )
    created_before: datetime | None = Field(
        None, description="Filter for records created before this timestamp"
    )
    updated_after: datetime | None = Field(
        None, description="Filter for records updated after this timestamp"
    )
    updated_before: datetime | None = Field(
        None, description="Filter for records updated before this timestamp"
    )
