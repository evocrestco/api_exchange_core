"""
State Transition models for the Entity Integration System.

This module defines the SQLAlchemy ORM models for tracking entity state changes
with a focus on tenant isolation and clean architecture.
"""

import enum

from sqlalchemy import Column, ForeignKey, Index, Integer, String, Text
from sqlalchemy.orm import relationship
from sqlalchemy.types import Enum

from src.db.db_base import JSON, BaseModel
from src.db.db_config import Base


class TransitionTypeEnum(enum.Enum):
    """Enum for categorizing different types of state transitions."""

    NORMAL = "NORMAL"
    ERROR = "ERROR"
    RECOVERY = "RECOVERY"
    MANUAL = "MANUAL"
    TIMEOUT = "TIMEOUT"
    RETRY = "RETRY"


class StateTransition(Base, BaseModel):
    """
    Record of a state change for an entity with enhanced tenant isolation.

    This model tracks state transitions with explicit tenant association,
    ensuring complete isolation and preventing cross-tenant data access.
    """

    # Core state transition fields
    entity_id = Column(String(36), ForeignKey("entity.id"), nullable=False)
    from_state = Column(String(50), nullable=False)
    to_state = Column(String(50), nullable=False)
    actor = Column(String(100), nullable=False)
    notes = Column(Text, nullable=True)

    # New fields for enhanced state tracking
    transition_type: Column[TransitionTypeEnum] = Column(
        Enum(
            TransitionTypeEnum, native_enum=False
        ),  # Use string-based enum for SQLite compatibility
        default=TransitionTypeEnum.NORMAL,
        nullable=False,
    )
    processor_data = Column(
        JSON, nullable=True, comment="Processor-specific data about the transition"
    )
    queue_source = Column(
        String(100), nullable=True, comment="Queue from which the message was received"
    )
    queue_destination = Column(
        String(100), nullable=True, comment="Queue to which the message was sent"
    )
    transition_duration = Column(
        Integer, nullable=True, comment="Time (in ms) spent in the previous state"
    )
    sequence_number = Column(
        Integer, nullable=True, comment="Sequence number for ordering transitions"
    )

    # Explicit tenant association with cascading delete
    tenant_id = Column(
        String(100), ForeignKey("tenant.tenant_id", ondelete="CASCADE"), nullable=False
    )

    # Relationships with explicit back_populates
    entity = relationship("Entity", back_populates="state_transitions", lazy="select")
    tenant = relationship("Tenant", backref="state_transitions", lazy="select")

    # Optimized indexes for performance and isolation
    __table_args__ = (
        Index("ix_state_transition_entity_id", "entity_id"),
        Index("ix_state_transition_to_state", "to_state"),
        Index("ix_state_transition_tenant", "tenant_id"),
        Index("ix_state_transition_entity_tenant", "entity_id", "tenant_id"),
        Index("ix_state_transition_sequence", "entity_id", "sequence_number"),
    )

    def __repr__(self) -> str:
        """
        String representation of the StateTransition.

        Provides a clear, informative representation for debugging and logging.
        """
        return (
            f"<StateTransition(id='{self.id}', "
            f"entity_id='{self.entity_id}', "
            f"from_state='{self.from_state}', "
            f"to_state='{self.to_state}', "
            f"transition_type='{self.transition_type.value if self.transition_type else None}', "
            f"tenant_id='{self.tenant_id}')>"
        )

    @classmethod
    def get_migrated_fields(cls):
        """
        Provides a mapping of old to new field names for migration support.

        Returns:
            Dict mapping legacy field names to current field names.
        """
        return {
            "entityId": "entity_id",
            "fromState": "from_state",
            "toState": "to_state",
            "transitionType": "transition_type",
        }
