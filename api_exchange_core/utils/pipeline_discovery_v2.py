"""
Simple pipeline discovery utilities for V2 framework.

Provides simple functions for Azure Functions to register their pipeline definitions.
Uses CRUD helpers instead of services/repositories.
"""

from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from sqlalchemy.orm import Session

from ..db.db_pipeline_definition_models import PipelineDefinition, PipelineStepDefinition
from ..utils.crud_helpers import create_record, get_record, list_records, update_record
from ..utils.logger import get_logger


def register_pipeline_definition(
    session: Session,
    pipeline_name: str,
    pipeline_structure: Dict[str, Any],
    version: str = "1.0.0",
    description: Optional[str] = None,
    capture_messages: bool = True,
) -> str:
    """
    Register or update a pipeline definition.

    Args:
        session: Database session
        pipeline_name: Unique pipeline name
        pipeline_structure: Complete pipeline structure (steps, connections, etc)
        version: Pipeline version
        description: Optional description
        capture_messages: Whether to capture input/output messages

    Returns:
        Pipeline definition ID
    """
    logger = get_logger()

    # Check if pipeline already exists
    existing = get_record(session, PipelineDefinition, {"pipeline_name": pipeline_name})

    pipeline_data = {
        "pipeline_name": pipeline_name,
        "version": version,
        "description": description,
        "pipeline_structure": pipeline_structure,
        "capture_messages": capture_messages,
        "is_active": True,
    }

    if existing:
        # Update existing
        updated = update_record(session, PipelineDefinition, existing.id, pipeline_data)
        logger.info(
            f"Updated pipeline definition: {pipeline_name}",
            extra={"pipeline_name": pipeline_name, "version": version, "pipeline_id": updated.id},
        )
        return updated.id
    else:
        # Create new
        new_pipeline = create_record(session, PipelineDefinition, pipeline_data)
        logger.info(
            f"Created pipeline definition: {pipeline_name}",
            extra={
                "pipeline_name": pipeline_name,
                "version": version,
                "pipeline_id": new_pipeline.id,
            },
        )
        return new_pipeline.id


def register_function_step(
    session: Session,
    pipeline_name: str,
    step_name: str,
    processor_name: str,
    function_name: Optional[str] = None,
    input_trigger: Optional[str] = None,
    output_queues: Optional[List[str]] = None,
    step_order: Optional[str] = None,
    is_root: bool = False,
) -> str:
    """
    Register a single function step in a pipeline.

    Args:
        session: Database session
        pipeline_name: Pipeline this step belongs to
        step_name: Unique step name
        processor_name: Processor class name
        function_name: Azure Function name
        input_trigger: Input trigger (queue name, "timer", "http", etc)
        output_queues: List of output queue names
        step_order: Display order
        is_root: Whether this is an entry point step

    Returns:
        Step definition ID
    """
    logger = get_logger()

    # Get pipeline definition (must exist)
    pipeline_def = get_record(session, PipelineDefinition, {"pipeline_name": pipeline_name})

    if not pipeline_def:
        # Auto-create basic pipeline definition
        pipeline_structure = {
            "steps": [step_name],
            "auto_discovered": True,
            "discovery_time": datetime.now(timezone.utc).isoformat(),
        }
        pipeline_def_id = register_pipeline_definition(
            session,
            pipeline_name,
            pipeline_structure,
            description=f"Auto-discovered pipeline for {pipeline_name}",
        )
    else:
        pipeline_def_id = pipeline_def.id

    # Check if step already exists
    existing_step = get_record(session, PipelineStepDefinition, {"pipeline_name": pipeline_name, "step_name": step_name})

    step_data = {
        "pipeline_definition_id": pipeline_def_id,
        "pipeline_name": pipeline_name,
        "step_name": step_name,
        "processor_name": processor_name,
        "function_name": function_name,
        "input_trigger": input_trigger,
        "output_queues": output_queues or [],
        "step_order": step_order,
        "is_root": is_root,
    }

    if existing_step:
        # Update existing
        updated = update_record(session, PipelineStepDefinition, existing_step.id, step_data)
        logger.info(
            f"Updated pipeline step: {pipeline_name}.{step_name}",
            extra={
                "pipeline_name": pipeline_name,
                "step_name": step_name,
                "processor_name": processor_name,
                "function_name": function_name,
            },
        )
        return updated.id
    else:
        # Create new
        new_step = create_record(session, PipelineStepDefinition, step_data)
        logger.info(
            f"Registered pipeline step: {pipeline_name}.{step_name}",
            extra={
                "pipeline_name": pipeline_name,
                "step_name": step_name,
                "processor_name": processor_name,
                "function_name": function_name,
            },
        )
        return new_step.id


def get_pipeline_structure(session: Session, pipeline_name: str) -> Optional[Dict[str, Any]]:
    """
    Get the complete pipeline structure for visualization.

    Args:
        session: Database session
        pipeline_name: Pipeline name

    Returns:
        Pipeline structure dict or None if not found
    """
    # Get pipeline definition
    pipeline_def = get_record(session, PipelineDefinition, {"pipeline_name": pipeline_name, "is_active": True})

    if not pipeline_def:
        return None

    # Get all steps
    steps = list_records(session, PipelineStepDefinition, {"pipeline_name": pipeline_name})

    return {
        "pipeline_id": pipeline_def.id,
        "pipeline_name": pipeline_def.pipeline_name,
        "version": pipeline_def.version,
        "description": pipeline_def.description,
        "capture_messages": pipeline_def.capture_messages,
        "pipeline_structure": pipeline_def.pipeline_structure,
        "steps": [
            {
                "step_id": step.id,
                "step_name": step.step_name,
                "processor_name": step.processor_name,
                "function_name": step.function_name,
                "input_trigger": step.input_trigger,
                "output_queues": step.output_queues,
                "step_order": step.step_order,
                "is_root": step.is_root,
                "context": step.context,
            }
            for step in steps
        ],
        "created_at": pipeline_def.created_at.isoformat(),
        "updated_at": pipeline_def.updated_at.isoformat(),
    }


def list_pipeline_definitions(session: Session, active_only: bool = True) -> List[Dict[str, Any]]:
    """
    List all pipeline definitions.

    Args:
        session: Database session
        active_only: Only return active pipelines

    Returns:
        List of pipeline definition summaries
    """
    filters = {"is_active": True} if active_only else {}

    pipelines = list_records(session, PipelineDefinition, filters, order_by="pipeline_name")

    return [
        {
            "pipeline_id": pipeline.id,
            "pipeline_name": pipeline.pipeline_name,
            "version": pipeline.version,
            "description": pipeline.description,
            "capture_messages": pipeline.capture_messages,
            "is_active": pipeline.is_active,
            "created_at": pipeline.created_at.isoformat(),
            "updated_at": pipeline.updated_at.isoformat(),
        }
        for pipeline in pipelines
    ]


def auto_register_function_step(
    session: Session,
    function_name: str,
    processor_name: str,
    step_name: Optional[str] = None,
    pipeline_name: Optional[str] = None,
    input_trigger: Optional[str] = None,
    output_queues: Optional[List[str]] = None,
    is_root: bool = False,
) -> str:
    """
    Auto-register a function step with sensible defaults.

    Convenience function for Azure Functions to self-register on startup.

    Args:
        session: Database session
        function_name: Azure Function name
        processor_name: Processor class name
        step_name: Step name (defaults to function_name)
        pipeline_name: Pipeline name (defaults to function_name prefix)
        input_trigger: Input trigger (auto-detected if possible)
        output_queues: Output queues (empty list if not specified)
        is_root: Whether this is a root step

    Returns:
        Step definition ID
    """
    # Use sensible defaults
    step_name = step_name or function_name
    pipeline_name = pipeline_name or function_name.split("_")[0] if "_" in function_name else function_name

    return register_function_step(
        session=session,
        pipeline_name=pipeline_name,
        step_name=step_name,
        processor_name=processor_name,
        function_name=function_name,
        input_trigger=input_trigger,
        output_queues=output_queues or [],
        is_root=is_root,
    )
