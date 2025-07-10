"""
Simple pipeline CRUD utilities for V2 framework.

Provides basic CRUD operations for pipeline definitions and execution tracking.
Uses generic CRUD helpers for consistency.
"""

from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from sqlalchemy.orm import Session

from ..db.db_pipeline_definition_models import PipelineDefinition, PipelineStepDefinition
from ..db.db_pipeline_tracking_models import PipelineExecution, PipelineStep
from ..utils.crud_helpers import (
    create_record,
    get_record,
    get_record_by_id,
    list_records,
    update_record,
)


# Pipeline Definition CRUD
def create_pipeline_definition(
    session: Session,
    pipeline_name: str,
    pipeline_structure: Dict[str, Any],
    version: str = "1.0.0",
    description: Optional[str] = None,
    capture_messages: bool = True,
    tenant_id: Optional[str] = None,
) -> str:
    """Create a new pipeline definition."""
    data = {
        "pipeline_name": pipeline_name,
        "version": version,
        "description": description,
        "pipeline_structure": pipeline_structure,
        "capture_messages": capture_messages,
        "is_active": True,
        "tenant_id": tenant_id,
    }

    pipeline = create_record(session, PipelineDefinition, data)
    return pipeline.id  # type: ignore[return-value]


def get_pipeline_definition(
    session: Session, pipeline_name: str, tenant_id: Optional[str] = None
) -> Optional[Dict[str, Any]]:
    """Get a pipeline definition by name."""
    filters = {"pipeline_name": pipeline_name, "is_active": True}
    if tenant_id:
        filters["tenant_id"] = tenant_id

    pipeline = get_record(session, PipelineDefinition, filters)

    if not pipeline:
        return None

    return {
        "id": pipeline.id,
        "pipeline_name": pipeline.pipeline_name,
        "version": pipeline.version,
        "description": pipeline.description,
        "pipeline_structure": pipeline.pipeline_structure,
        "capture_messages": pipeline.capture_messages,
        "is_active": pipeline.is_active,
        "tenant_id": pipeline.tenant_id,
        "created_at": pipeline.created_at.isoformat(),
        "updated_at": pipeline.updated_at.isoformat(),
    }


def update_pipeline_definition(
    session: Session, pipeline_id: str, updates: Dict[str, Any], tenant_id: Optional[str] = None
) -> bool:
    """Update a pipeline definition."""
    try:
        update_record(session, PipelineDefinition, pipeline_id, updates, tenant_id)
        return True
    except Exception:
        return False


def delete_pipeline_definition(
    session: Session, pipeline_id: str, tenant_id: Optional[str] = None
) -> bool:
    """Delete a pipeline definition (soft delete by setting is_active=False)."""
    try:
        update_record(session, PipelineDefinition, pipeline_id, {"is_active": False}, tenant_id)
        return True
    except Exception:
        return False


def list_pipeline_definitions(
    session: Session, tenant_id: Optional[str] = None, active_only: bool = True
) -> List[Dict[str, Any]]:
    """List all pipeline definitions."""
    filters: Dict[str, Any] = {}
    if active_only:
        filters["is_active"] = True
    if tenant_id:
        filters["tenant_id"] = tenant_id

    pipelines = list_records(session, PipelineDefinition, filters, order_by="pipeline_name")

    return [
        {
            "id": p.id,
            "pipeline_name": p.pipeline_name,
            "version": p.version,
            "description": p.description,
            "capture_messages": p.capture_messages,
            "is_active": p.is_active,
            "tenant_id": p.tenant_id,
            "created_at": p.created_at.isoformat(),
            "updated_at": p.updated_at.isoformat(),
        }
        for p in pipelines
    ]


# Pipeline Step Definition CRUD
def create_pipeline_step_definition(
    session: Session,
    pipeline_definition_id: str,
    pipeline_name: str,
    step_name: str,
    processor_name: str,
    function_name: Optional[str] = None,
    input_trigger: Optional[str] = None,
    output_queues: Optional[List[str]] = None,
    step_order: Optional[str] = None,
    is_root: bool = False,
    tenant_id: Optional[str] = None,
) -> str:
    """Create a new pipeline step definition."""
    data = {
        "pipeline_definition_id": pipeline_definition_id,
        "pipeline_name": pipeline_name,
        "step_name": step_name,
        "processor_name": processor_name,
        "function_name": function_name,
        "input_trigger": input_trigger,
        "output_queues": output_queues,
        "step_order": step_order,
        "is_root": is_root,
        "tenant_id": tenant_id,
    }

    step = create_record(session, PipelineStepDefinition, data)
    return step.id  # type: ignore[return-value]


def get_pipeline_steps(
    session: Session, pipeline_name: str, tenant_id: Optional[str] = None
) -> List[Dict[str, Any]]:
    """Get all steps for a pipeline."""
    filters = {"pipeline_name": pipeline_name}
    if tenant_id:
        filters["tenant_id"] = tenant_id

    steps = list_records(
        session,
        PipelineStepDefinition,
        filters,
        order_by="step_order",
    )

    return [
        {
            "id": s.id,
            "pipeline_definition_id": s.pipeline_definition_id,
            "pipeline_name": s.pipeline_name,
            "step_name": s.step_name,
            "processor_name": s.processor_name,
            "function_name": s.function_name,
            "input_trigger": s.input_trigger,
            "output_queues": s.output_queues,
            "step_order": s.step_order,
            "is_root": s.is_root,
            "tenant_id": s.tenant_id,
            "created_at": s.created_at.isoformat(),
            "updated_at": s.updated_at.isoformat(),
        }
        for s in steps
    ]


# Pipeline Execution Tracking CRUD
def create_pipeline_execution(
    session: Session,
    pipeline_id: str,
    tenant_id: str,
    trigger_type: str,
    trigger_source: Optional[str] = None,
    correlation_id: Optional[str] = None,
) -> str:
    """Create a new pipeline execution record."""
    data = {
        "pipeline_id": pipeline_id,
        "tenant_id": tenant_id,
        "correlation_id": correlation_id,
        "status": "started",
        "started_at": datetime.now(timezone.utc),
        "trigger_type": trigger_type,
        "trigger_source": trigger_source,
        "step_count": 0,
        "message_count": 1,
        "error_count": 0,
    }

    execution = create_record(session, PipelineExecution, data)
    return execution.id  # type: ignore[return-value]


def update_pipeline_execution(
    session: Session, execution_id: str, updates: Dict[str, Any], tenant_id: Optional[str] = None
) -> bool:
    """Update a pipeline execution."""
    try:
        update_record(session, PipelineExecution, execution_id, updates, tenant_id)
        return True
    except Exception:
        return False


def complete_pipeline_execution(
    session: Session,
    execution_id: str,
    status: str = "completed",
    error_message: Optional[str] = None,
    tenant_id: Optional[str] = None,
) -> bool:
    """Complete a pipeline execution."""
    now = datetime.now(timezone.utc)
    updates = {"status": status, "completed_at": now, "error_message": error_message}

    # Calculate duration if we have started_at
    execution = get_record_by_id(session, PipelineExecution, execution_id)
    if execution and execution.started_at:
        # Handle timezone-aware vs timezone-naive datetime comparison
        started_at = execution.started_at
        if started_at.tzinfo is None:
            # Convert timezone-aware to naive for comparison
            now = now.replace(tzinfo=None)
        duration = (now - started_at).total_seconds() * 1000  # Convert to milliseconds
        updates["duration_ms"] = int(duration)

    return update_pipeline_execution(session, execution_id, updates, tenant_id)


def get_pipeline_execution(
    session: Session, execution_id: str, tenant_id: Optional[str] = None
) -> Optional[Dict[str, Any]]:
    """Get a pipeline execution by ID."""
    execution = get_record_by_id(session, PipelineExecution, execution_id)

    if not execution or (tenant_id and execution.tenant_id != tenant_id):
        return None

    return {
        "id": execution.id,
        "pipeline_id": execution.pipeline_id,
        "tenant_id": execution.tenant_id,
        "correlation_id": execution.correlation_id,
        "status": execution.status,
        "started_at": execution.started_at.isoformat(),
        "completed_at": execution.completed_at.isoformat() if execution.completed_at else None,
        "duration_ms": execution.duration_ms,
        "trigger_type": execution.trigger_type,
        "trigger_source": execution.trigger_source,
        "step_count": execution.step_count,
        "message_count": execution.message_count,
        "error_count": execution.error_count,
        "error_message": execution.error_message,
        "error_step": execution.error_step,
        "created_at": execution.created_at.isoformat(),
        "updated_at": execution.updated_at.isoformat(),
    }


def list_pipeline_executions(
    session: Session,
    tenant_id: str,
    pipeline_id: Optional[str] = None,
    status: Optional[str] = None,
    limit: Optional[int] = None,
) -> List[Dict[str, Any]]:
    """List pipeline executions with optional filters."""
    filters = {"tenant_id": tenant_id}
    if pipeline_id:
        filters["pipeline_id"] = pipeline_id
    if status:
        filters["status"] = status

    executions = list_records(
        session, PipelineExecution, filters, tenant_id, limit=limit, order_by="started_at"
    )

    return [
        {
            "id": e.id,
            "pipeline_id": e.pipeline_id,
            "correlation_id": e.correlation_id,
            "status": e.status,
            "started_at": e.started_at.isoformat(),
            "completed_at": e.completed_at.isoformat() if e.completed_at else None,
            "duration_ms": e.duration_ms,
            "trigger_type": e.trigger_type,
            "step_count": e.step_count,
            "message_count": e.message_count,
            "error_count": e.error_count,
        }
        for e in executions
    ]


# Pipeline Step Tracking CRUD
def create_pipeline_step(
    session: Session,
    execution_id: str,
    pipeline_id: str,
    tenant_id: str,
    step_name: str,
    processor_name: str,
    message_id: str,
    function_name: Optional[str] = None,
    correlation_id: Optional[str] = None,
) -> str:
    """Create a new pipeline step record."""
    data = {
        "execution_id": execution_id,
        "pipeline_id": pipeline_id,
        "tenant_id": tenant_id,
        "step_name": step_name,
        "processor_name": processor_name,
        "message_id": message_id,
        "function_name": function_name,
        "correlation_id": correlation_id,
        "status": "processing",
        "started_at": datetime.now(timezone.utc),
        "output_count": 0,
    }

    step = create_record(session, PipelineStep, data)
    return step.id  # type: ignore[return-value]


def complete_pipeline_step(
    session: Session,
    step_id: str,
    status: str = "completed",
    output_count: int = 0,
    output_queues: Optional[List[str]] = None,
    error_message: Optional[str] = None,
    error_type: Optional[str] = None,
) -> bool:
    """Complete a pipeline step."""
    now = datetime.now(timezone.utc)
    updates = {
        "status": status,
        "completed_at": now,
        "output_count": output_count,
        "output_queues": output_queues or [],
        "error_message": error_message,
        "error_type": error_type,
    }

    # Calculate duration if we have started_at
    try:
        step = get_record_by_id(session, PipelineStep, step_id)
        if step and step.started_at:
            # Handle timezone-aware vs timezone-naive datetime comparison
            started_at = step.started_at
            if started_at.tzinfo is None:
                # Convert timezone-aware to naive for comparison
                now = now.replace(tzinfo=None)
            duration = (now - started_at).total_seconds() * 1000  # Convert to milliseconds
            updates["duration_ms"] = int(duration)

        update_record(session, PipelineStep, step_id, updates)
        return True
    except Exception:
        return False


def get_pipeline_steps_for_execution(
    session: Session, execution_id: str, tenant_id: Optional[str] = None
) -> List[Dict[str, Any]]:
    """Get all steps for a pipeline execution."""
    steps = list_records(
        session, PipelineStep, {"execution_id": execution_id}, tenant_id, order_by="started_at"
    )

    return [
        {
            "id": s.id,
            "execution_id": s.execution_id,
            "pipeline_id": s.pipeline_id,
            "step_name": s.step_name,
            "processor_name": s.processor_name,
            "function_name": s.function_name,
            "message_id": s.message_id,
            "correlation_id": s.correlation_id,
            "status": s.status,
            "started_at": s.started_at.isoformat(),
            "completed_at": s.completed_at.isoformat() if s.completed_at else None,
            "duration_ms": s.duration_ms,
            "output_count": s.output_count,
            "output_queues": s.output_queues,
            "error_message": s.error_message,
            "error_type": s.error_type,
        }
        for s in steps
    ]
