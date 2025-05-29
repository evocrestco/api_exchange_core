"""
Database models for error tracking.

This module defines SQLAlchemy models for processing errors in the Entity Integration System.
"""

from sqlalchemy import Column, ForeignKey, Index, String, Text
from sqlalchemy.orm import relationship

from src.db.db_base import BaseModel
from src.db.db_config import Base


class ProcessingError(Base, BaseModel):
    """
    Database model for tracking processing errors.

    This model stores error information for entity processing operations,
    including error type, message, and processing context.
    """

    # Foreign keys
    entity_id = Column(String(36), ForeignKey("entity.id"), nullable=False)
    tenant_id = Column(
        String(100), ForeignKey("tenant.tenant_id", ondelete="CASCADE"), nullable=False
    )

    # Error details
    error_type_code = Column(String(50), nullable=False)
    message = Column(Text, nullable=False)
    processing_step = Column(String(100), nullable=False)
    stack_trace = Column(Text, nullable=True)

    # Relationships
    # Change relationship declarations to use strings for the target class names
    entity = relationship("Entity", back_populates="processing_errors")
    tenant = relationship("Tenant", backref="processing_errors")

    # Indexes for performance
    __table_args__ = (
        Index("ix_processing_error_entity_id", "entity_id"),
        Index("ix_processing_error_type", "error_type_code"),
        Index("ix_processing_error_tenant", "tenant_id"),
    )

    def __repr__(self) -> str:
        """String representation of the ProcessingError."""
        return (
            f"<ProcessingError(id='{self.id}', entity_id='{self.entity_id}', "
            f"error_type='{self.error_type_code}')>"
        )
