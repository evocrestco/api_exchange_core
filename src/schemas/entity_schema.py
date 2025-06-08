from datetime import datetime
from typing import Any, Dict, Optional

from pydantic import BaseModel, Field

from src.schemas.mixins import AttributesMixin, IdMixin, TenantMixin, TimestampMixin


class EntityBase(TenantMixin, AttributesMixin):
    """Base model for entity schemas with common fields."""

    external_id: str = Field(
        ...,
        min_length=1,
        max_length=100,
        description="Identifier set by external system for the entity",
    )
    canonical_type: str = Field(
        ..., min_length=1, max_length=50, description="Canonical type of the entity"
    )
    source: str = Field(..., min_length=1, max_length=50, description="Source of the entity")
    version: int = Field(default=1, ge=1, description="Version number of the entity")
    content_hash: Optional[str] = None


class EntityCreate(EntityBase):
    """Schema for creating a new entity."""

    pass


class EntityUpdate(BaseModel):
    """Schema for updating an existing entity."""

    content_hash: Optional[str] = None
    attributes: Optional[Dict[str, Any]] = None

    model_config = {"validate_assignment": True}


class EntityRead(EntityBase, IdMixin, TimestampMixin):
    """Schema for reading entity data."""

    model_config = {"from_attributes": True}


class EntityFilter(BaseModel):
    """Schema for filtering entities in queries."""

    tenant_id: Optional[str] = None
    external_id: Optional[str] = None
    canonical_type: Optional[str] = None
    source: Optional[str] = None
    content_hash: Optional[str] = None
    created_after: Optional[datetime] = None
    created_before: Optional[datetime] = None
    updated_after: Optional[datetime] = None
    updated_before: Optional[datetime] = None


class EntityReference(BaseModel):
    """Lightweight reference to an entity."""

    id: Optional[str] = None
    tenant_id: str
    external_id: str
    canonical_type: str
    source: str
    version: int = Field(default=1, description="Entity version number")

    @classmethod
    def from_entity(cls, entity) -> "EntityReference":
        """Create EntityReference from Entity model."""
        return cls(
            id=entity.id,
            tenant_id=entity.tenant_id,
            external_id=entity.external_id,
            canonical_type=entity.canonical_type,
            source=entity.source,
            version=entity.version,
        )
