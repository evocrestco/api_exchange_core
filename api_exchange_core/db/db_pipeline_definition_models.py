"""
Simple pipeline definition models for V2 framework.

Stores pipeline structure for GUI visualization and discovery.
Just data models, no business logic.
"""

from sqlalchemy import Boolean, Column, Index, String

from .db_base import JSON, TimestampMixin, UUIDMixin
from .db_config import Base


class PipelineDefinition(Base, UUIDMixin, TimestampMixin):
    """Store pipeline structure for visualization and discovery."""

    __tablename__ = "pipeline_definition"

    # Core identification
    pipeline_name = Column(String(100), nullable=False, index=True)
    version = Column(String(20), nullable=False, default="1.0.0")
    description = Column(String(500), nullable=True)
    tenant_id = Column(String(100), nullable=True, index=True)  # Multi-tenant support

    # Pipeline structure (DAG)
    pipeline_structure = Column(JSON, nullable=False)  # Complete step definitions and connections

    # Settings
    capture_messages = Column(Boolean, nullable=False, default=True)  # Whether to store input/output messages
    is_active = Column(Boolean, nullable=False, default=True)

    # Context
    context = Column(JSON, nullable=True)  # Additional metadata

    # Indexes
    __table_args__ = (
        Index("ix_pipeline_def_lookup", "pipeline_name", "is_active"),
        Index("ix_pipeline_def_tenant", "tenant_id", "pipeline_name"),
    )


class PipelineStepDefinition(Base, UUIDMixin, TimestampMixin):
    """Store individual step definitions within a pipeline."""

    __tablename__ = "pipeline_step_definition"

    # Links to pipeline
    pipeline_definition_id = Column(String(36), nullable=False, index=True)
    pipeline_name = Column(String(100), nullable=False)  # Denormalized for easy queries
    tenant_id = Column(String(100), nullable=True, index=True)  # Multi-tenant support

    # Step identification
    step_name = Column(String(100), nullable=False)
    processor_name = Column(String(100), nullable=False)
    function_name = Column(String(100), nullable=True)  # Azure Function name

    # Queue configuration
    input_trigger = Column(String(100), nullable=True)  # queue name, "timer", "http", etc
    output_queues = Column(JSON, nullable=True)  # List of output queue names

    # Step metadata
    step_order = Column(String(10), nullable=True)  # For display ordering
    is_root = Column(Boolean, nullable=False, default=False)  # Entry point step

    # Context
    context = Column(JSON, nullable=True)

    # Indexes
    __table_args__ = (
        Index("ix_pipeline_step_def_lookup", "pipeline_name", "step_name"),
        Index("ix_pipeline_step_function", "function_name"),
        Index("ix_pipeline_step_tenant", "tenant_id", "pipeline_name"),
    )
