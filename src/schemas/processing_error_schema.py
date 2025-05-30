"""
Pydantic schemas for ProcessingError models in the Entity Integration System.

This module defines validation schemas for error-related data transfer.
"""

from typing import Optional

from pydantic import BaseModel, ConfigDict, Field

from src.schemas.mixins import CoreEntityMixin, DateRangeFilterMixin


class ProcessingErrorBase(BaseModel):
    """Base schema with common ProcessingError attributes."""

    error_type_code: str = Field(..., description="The type of error that occurred")
    message: str = Field(..., description="Error message details")
    processing_step: str = Field(..., description="Step in the process where the error occurred")
    stack_trace: Optional[str] = Field(None, description="Stack trace if available")

    model_config = ConfigDict(
        from_attributes=True, extra="ignore"  # Enables ORM mode  # Ignore extra fields
    )


class ProcessingErrorCreate(ProcessingErrorBase):
    """Schema for creating a new processing error."""

    entity_id: str = Field(
        ..., min_length=1, description="ID of the entity associated with this error"
    )
    tenant_id: Optional[str] = Field(
        None,
        description="ID of the tenant associated with this error "
        "(can be automatically set by tenant_aware decorator)",
    )


class ProcessingErrorRead(ProcessingErrorBase, CoreEntityMixin):
    """Schema for reading a processing error."""

    entity_id: str = Field(
        ..., min_length=1, description="ID of the entity associated with this error"
    )


class ProcessingErrorFilter(DateRangeFilterMixin):
    """Schema for filtering processing errors."""

    entity_id: Optional[str] = Field(None, description="Filter by entity ID")
    error_type_code: Optional[str] = Field(None, description="Filter by error type")
    processing_step: Optional[str] = Field(None, description="Filter by processing step")
    tenant_id: Optional[str] = Field(None, description="Filter by tenant ID")

    model_config = ConfigDict(extra="ignore")  # Ignore extra fields