import uuid

# datetime imports removed - using db_base.utc_now() instead
from typing import Any, Dict, Optional

from sqlalchemy import Column, DateTime, ForeignKey, Index, Integer, String, UniqueConstraint
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm.attributes import flag_modified

from ..exceptions import ErrorCode, ValidationError
from .db_base import Base, utc_now


class Entity(Base):
    """Base class for all entities."""

    __tablename__ = "entity"

    id = Column(String(36), primary_key=True)
    tenant_id = Column(
        String(100), ForeignKey("tenant.tenant_id", ondelete="CASCADE"), nullable=False
    )
    external_id = Column(String(100), nullable=False)
    canonical_type = Column(String(50), nullable=False)
    source = Column(String(50), nullable=False)
    version = Column(Integer, nullable=False, default=1)
    content_hash = Column(String(64), nullable=True)
    attributes = Column(JSONB, nullable=True)
    processing_results = Column(JSONB, nullable=True, default=list)
    created_at = Column(DateTime(timezone=True), nullable=False, default=utc_now)
    updated_at = Column(DateTime(timezone=True), nullable=False, default=utc_now, onupdate=utc_now)

    # Relationships - state transitions and processing errors now handled via logging

    # Indexes for efficient querying and constraints for data integrity
    __table_args__ = (
        Index("ix_entity_tenant", "tenant_id"),
        Index("ix_entity_external_id", "external_id"),
        Index("ix_entity_tenant_source", "tenant_id", "source"),
        Index("ix_entity_tenant_type", "tenant_id", "canonical_type"),
        Index("ix_entity_content_hash", "content_hash"),
        Index("ix_entity_tenant_source_hash", "tenant_id", "source", "content_hash"),
        Index("ix_entity_tenant_external_id", "tenant_id", "external_id"),
        UniqueConstraint(
            "tenant_id",
            "source",
            "external_id",
            "version",
            name="uq_entity_tenant_source_external_id_version",
        ),
    )

    @classmethod
    def create(
        cls,
        tenant_id: str,
        external_id: str,
        canonical_type: str,
        source: str,
        content_hash: Optional[str] = None,
        attributes: Optional[Dict[str, Any]] = None,
        version: int = 1,
    ) -> "Entity":
        """
        Factory method to create a new Entity with validated parameters.
        """
        if not tenant_id:
            raise ValidationError(
                "tenant_id is required",
                error_code=ErrorCode.MISSING_REQUIRED,
                field="tenant_id",
                value=tenant_id
            )
        if not external_id:
            raise ValidationError(
                "external_id is required",
                error_code=ErrorCode.MISSING_REQUIRED,
                field="external_id",
                value=external_id
            )
        if not canonical_type:
            raise ValidationError(
                "canonical_type is required",
                error_code=ErrorCode.MISSING_REQUIRED,
                field="canonical_type",
                value=canonical_type
            )
        if not source:
            raise ValidationError(
                "source is required",
                error_code=ErrorCode.MISSING_REQUIRED,
                field="source",
                value=source
            )

        return cls(
            id=str(uuid.uuid4()),
            tenant_id=tenant_id,
            external_id=external_id,
            canonical_type=canonical_type,
            source=source,
            content_hash=content_hash,
            attributes=attributes or {},
            processing_results=[],
            version=version,
            created_at=utc_now(),
            updated_at=utc_now(),
        )

    def update_content_hash(self, content_hash: str) -> None:
        """Update the content hash when entity data changes."""
        self.content_hash = content_hash  # type: ignore[assignment]
        self.updated_at = utc_now()

    def update_attributes(self, attributes: Dict[str, Any]) -> None:
        """Update entity attributes."""
        self.attributes = attributes  # type: ignore[assignment]
        self.updated_at = utc_now()

        flag_modified(self, "attributes")

    def __repr__(self) -> str:
        """String representation of the Entity."""
        return (
            f"<Entity(id='{self.id}', tenant_id='{self.tenant_id}', "
            f"external_id='{self.external_id}', canonical_type='{self.canonical_type}')>"
        )
