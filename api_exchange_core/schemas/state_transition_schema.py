"""
Pydantic schemas for StateTransition models in the Entity Integration System.

This module defines validation schemas for state transition data transfer.
"""

from datetime import datetime
from typing import Dict, List, Optional

from pydantic import BaseModel, ConfigDict, Field

from ..enums import TransitionTypeEnum
from ..type_definitions import ProcessorData
from .mixins import CoreEntityMixin, DateRangeFilterMixin


class StateTransitionBase(BaseModel):
    """Base schema with common StateTransition attributes."""

    from_state: str = Field(..., description="Previous state of the entity")
    to_state: str = Field(..., description="New state of the entity")
    actor: str = Field(..., description="Processor or user that triggered the transition")
    notes: Optional[str] = Field(None, description="Optional notes about the transition")

    # New fields for enhanced state tracking
    transition_type: TransitionTypeEnum = Field(
        default=TransitionTypeEnum.NORMAL, description="Categorization of the transition"
    )
    processor_data: Optional[ProcessorData] = Field(
        default=None, description="Processor-specific data about the transition"
    )
    queue_source: Optional[str] = Field(
        None, description="Queue from which the message was received"
    )
    queue_destination: Optional[str] = Field(
        None, description="Queue to which the message was sent"
    )
    transition_duration: Optional[int] = Field(
        None, description="Time (in ms) spent in the previous state"
    )
    sequence_number: Optional[int] = Field(
        None, description="Sequence number for ordering transitions"
    )

    model_config = ConfigDict(
        from_attributes=True, extra="ignore"  # Enables ORM mode  # Ignore extra fields
    )


class StateTransitionCreate(StateTransitionBase):
    """Schema for creating a new state transition."""

    entity_id: str = Field(..., description="ID of the entity associated with this transition")
    tenant_id: Optional[str] = Field(
        None, description="ID of the tenant (can be automatically set by tenant_aware decorator)"
    )


class StateTransitionRead(StateTransitionBase, CoreEntityMixin):
    """Schema for reading a state transition."""

    entity_id: str = Field(..., description="ID of the entity associated with this transition")


class StateTransitionUpdate(BaseModel):
    """Schema for updating a state transition (currently not used as transitions are immutable)."""

    notes: Optional[str] = Field(None, description="Optional notes about the transition")


class StateTransitionFilter(DateRangeFilterMixin):
    """Schema for filtering state transitions."""

    entity_id: Optional[str] = Field(None, description="Filter by entity ID")
    tenant_id: Optional[str] = Field(None, description="Filter by tenant ID")
    from_state: Optional[str] = Field(None, description="Filter by previous state")
    to_state: Optional[str] = Field(None, description="Filter by new state")
    actor: Optional[str] = Field(None, description="Filter by actor")
    transition_type: Optional[TransitionTypeEnum] = Field(
        None, description="Filter by transition type"
    )

    model_config = ConfigDict(extra="ignore")  # Ignore extra fields


class StateTransitionStats(BaseModel):
    """Schema for state transition statistics."""

    total_transitions: int = Field(..., description="Total number of transitions")
    transitions_by_state: Dict[str, int] = Field(..., description="Count of transitions by state")
    avg_duration_by_state: Dict[str, float] = Field(
        ..., description="Average duration in each state (ms)"
    )
    error_rate: float = Field(..., description="Rate of error transitions")
    most_common_error_states: List[str] = Field(..., description="Most common error states")

    model_config = ConfigDict(extra="ignore")  # Ignore extra fields


class EntityStateHistory(BaseModel):
    """Schema for entity state history."""

    entity_id: str = Field(..., description="ID of the entity")
    current_state: str = Field(..., description="Current state of the entity")
    transitions: List[StateTransitionRead] = Field(..., description="List of state transitions")
    total_transitions: int = Field(..., description="Total number of transitions")
    first_seen: datetime = Field(..., description="Timestamp of first transition")
    last_updated: datetime = Field(..., description="Timestamp of last transition")
    total_processing_time: int = Field(..., description="Total time in processing (ms)")

    model_config = ConfigDict(extra="ignore")  # Ignore extra fields
