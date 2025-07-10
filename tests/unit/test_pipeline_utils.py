"""
Unit tests for pipeline utilities using generic CRUD helpers.

Tests the pipeline utility functions that provide business logic
for pipeline definitions and execution tracking.
"""

import pytest
from datetime import datetime, timezone, timedelta
from sqlalchemy.orm import Session

from api_exchange_core.db.db_pipeline_definition_models import PipelineDefinition, PipelineStepDefinition
from api_exchange_core.db.db_pipeline_tracking_models import PipelineExecution, PipelineStep
from api_exchange_core.utils.pipeline_utils import (
    # Pipeline Definition CRUD
    create_pipeline_definition,
    get_pipeline_definition,
    update_pipeline_definition,
    delete_pipeline_definition,
    list_pipeline_definitions,
    # Pipeline Step Definition CRUD
    create_pipeline_step_definition,
    get_pipeline_steps,
    # Pipeline Execution Tracking CRUD
    create_pipeline_execution,
    update_pipeline_execution,
    complete_pipeline_execution,
    get_pipeline_execution,
    list_pipeline_executions,
    # Pipeline Step Tracking CRUD
    create_pipeline_step,
    complete_pipeline_step,
    get_pipeline_steps_for_execution,
)


class TestPipelineDefinitionCRUD:
    """Test pipeline definition CRUD operations."""
    
    def test_create_pipeline_definition_success(self, db_session: Session):
        """Test creating a new pipeline definition."""
        pipeline_structure = {
            "steps": [
                {"name": "step1", "processor": "processor1"},
                {"name": "step2", "processor": "processor2"}
            ]
        }
        
        pipeline_id = create_pipeline_definition(
            db_session,
            pipeline_name="test_pipeline",
            pipeline_structure=pipeline_structure,
            version="1.0.0",
            description="Test pipeline",
            capture_messages=True,
            tenant_id="test-tenant"
        )
        
        assert pipeline_id is not None
        
        # Verify pipeline was created
        pipeline = db_session.query(PipelineDefinition).filter_by(id=pipeline_id).first()
        assert pipeline is not None
        assert pipeline.pipeline_name == "test_pipeline"
        assert pipeline.version == "1.0.0"
        assert pipeline.description == "Test pipeline"
        assert pipeline.pipeline_structure == pipeline_structure
        assert pipeline.capture_messages is True
        assert pipeline.is_active is True
        assert pipeline.tenant_id == "test-tenant"
    
    def test_create_pipeline_definition_minimal(self, db_session: Session):
        """Test creating pipeline definition with minimal parameters."""
        pipeline_structure = {"steps": []}
        
        pipeline_id = create_pipeline_definition(
            db_session,
            pipeline_name="minimal_pipeline",
            pipeline_structure=pipeline_structure
        )
        
        assert pipeline_id is not None
        
        # Verify defaults
        pipeline = db_session.query(PipelineDefinition).filter_by(id=pipeline_id).first()
        assert pipeline.version == "1.0.0"
        assert pipeline.description is None
        assert pipeline.capture_messages is True
        assert pipeline.is_active is True
        assert pipeline.tenant_id is None
    
    def test_get_pipeline_definition_success(self, db_session: Session):
        """Test getting a pipeline definition by name."""
        # Create pipeline
        pipeline_structure = {"steps": [{"name": "step1"}]}
        pipeline_id = create_pipeline_definition(
            db_session,
            pipeline_name="get_test",
            pipeline_structure=pipeline_structure,
            description="Get test pipeline",
            tenant_id="test-tenant"
        )
        
        # Get pipeline
        result = get_pipeline_definition(db_session, "get_test", "test-tenant")
        
        assert result is not None
        assert result["id"] == pipeline_id
        assert result["pipeline_name"] == "get_test"
        assert result["version"] == "1.0.0"
        assert result["description"] == "Get test pipeline"
        assert result["pipeline_structure"] == pipeline_structure
        assert result["capture_messages"] is True
        assert result["is_active"] is True
        assert "created_at" in result
        assert "updated_at" in result
    
    def test_get_pipeline_definition_not_found(self, db_session: Session):
        """Test getting non-existent pipeline definition."""
        result = get_pipeline_definition(db_session, "nonexistent", "test-tenant")
        assert result is None
    
    def test_get_pipeline_definition_inactive(self, db_session: Session):
        """Test getting inactive pipeline definition."""
        # Create and then deactivate pipeline
        pipeline_id = create_pipeline_definition(
            db_session,
            pipeline_name="inactive_test",
            pipeline_structure={"steps": []},
            tenant_id="test-tenant"
        )
        
        # Deactivate
        update_pipeline_definition(db_session, pipeline_id, {"is_active": False}, "test-tenant")
        
        # Should not find inactive pipeline
        result = get_pipeline_definition(db_session, "inactive_test", "test-tenant")
        assert result is None
    
    def test_update_pipeline_definition_success(self, db_session: Session):
        """Test updating a pipeline definition."""
        # Create pipeline
        pipeline_id = create_pipeline_definition(
            db_session,
            pipeline_name="update_test",
            pipeline_structure={"steps": []},
            description="Original description",
            tenant_id="test-tenant"
        )
        
        # Update pipeline
        new_structure = {"steps": [{"name": "updated_step"}]}
        updates = {
            "description": "Updated description",
            "pipeline_structure": new_structure,
            "version": "2.0.0"
        }
        
        result = update_pipeline_definition(db_session, pipeline_id, updates, "test-tenant")
        assert result is True
        
        # Verify update
        pipeline = db_session.query(PipelineDefinition).filter_by(id=pipeline_id).first()
        assert pipeline.description == "Updated description"
        assert pipeline.pipeline_structure == new_structure
        assert pipeline.version == "2.0.0"
    
    def test_update_pipeline_definition_not_found(self, db_session: Session):
        """Test updating non-existent pipeline definition."""
        result = update_pipeline_definition(db_session, "nonexistent-id", {"description": "test"}, "test-tenant")
        assert result is False
    
    def test_delete_pipeline_definition_success(self, db_session: Session):
        """Test deleting a pipeline definition (soft delete)."""
        # Create pipeline
        pipeline_id = create_pipeline_definition(
            db_session,
            pipeline_name="delete_test",
            pipeline_structure={"steps": []},
            tenant_id="test-tenant"
        )
        
        # Delete pipeline
        result = delete_pipeline_definition(db_session, pipeline_id, "test-tenant")
        assert result is True
        
        # Verify soft delete
        pipeline = db_session.query(PipelineDefinition).filter_by(id=pipeline_id).first()
        assert pipeline is not None
        assert pipeline.is_active is False
    
    def test_delete_pipeline_definition_not_found(self, db_session: Session):
        """Test deleting non-existent pipeline definition."""
        result = delete_pipeline_definition(db_session, "nonexistent-id", "test-tenant")
        assert result is False
    
    def test_list_pipeline_definitions_success(self, db_session: Session):
        """Test listing pipeline definitions."""
        # Create multiple pipelines
        pipeline1_id = create_pipeline_definition(
            db_session,
            pipeline_name="pipeline_a",
            pipeline_structure={"steps": []},
            description="Pipeline A",
            tenant_id="test-tenant"
        )
        
        pipeline2_id = create_pipeline_definition(
            db_session,
            pipeline_name="pipeline_b",
            pipeline_structure={"steps": []},
            description="Pipeline B",
            tenant_id="test-tenant"
        )
        
        # List pipelines
        result = list_pipeline_definitions(db_session, "test-tenant")
        
        assert len(result) == 2
        
        # Verify order (should be by pipeline_name)
        assert result[0]["pipeline_name"] == "pipeline_a"
        assert result[1]["pipeline_name"] == "pipeline_b"
        
        # Verify structure
        pipeline_a = result[0]
        assert pipeline_a["id"] == pipeline1_id
        assert pipeline_a["description"] == "Pipeline A"
        assert pipeline_a["is_active"] is True
        assert "created_at" in pipeline_a
        assert "updated_at" in pipeline_a
    
    def test_list_pipeline_definitions_active_only(self, db_session: Session):
        """Test listing only active pipeline definitions."""
        # Create active pipeline
        active_id = create_pipeline_definition(
            db_session,
            pipeline_name="active_pipeline",
            pipeline_structure={"steps": []},
            tenant_id="test-tenant"
        )
        
        # Create inactive pipeline
        inactive_id = create_pipeline_definition(
            db_session,
            pipeline_name="inactive_pipeline",
            pipeline_structure={"steps": []},
            tenant_id="test-tenant"
        )
        
        # Deactivate one pipeline
        update_pipeline_definition(db_session, inactive_id, {"is_active": False}, "test-tenant")
        
        # List active only
        result = list_pipeline_definitions(db_session, "test-tenant", active_only=True)
        
        assert len(result) == 1
        assert result[0]["id"] == active_id
        assert result[0]["pipeline_name"] == "active_pipeline"
    
    def test_list_pipeline_definitions_include_inactive(self, db_session: Session):
        """Test listing all pipeline definitions including inactive."""
        # Create active pipeline
        create_pipeline_definition(
            db_session,
            pipeline_name="active_pipeline",
            pipeline_structure={"steps": []},
            tenant_id="test-tenant"
        )
        
        # Create inactive pipeline
        inactive_id = create_pipeline_definition(
            db_session,
            pipeline_name="inactive_pipeline",
            pipeline_structure={"steps": []},
            tenant_id="test-tenant"
        )
        
        # Deactivate one pipeline
        update_pipeline_definition(db_session, inactive_id, {"is_active": False}, "test-tenant")
        
        # List all
        result = list_pipeline_definitions(db_session, "test-tenant", active_only=False)
        
        assert len(result) == 2
        
        # Find inactive pipeline
        inactive_pipeline = next(p for p in result if p["pipeline_name"] == "inactive_pipeline")
        assert inactive_pipeline["is_active"] is False


class TestPipelineStepDefinitionCRUD:
    """Test pipeline step definition CRUD operations."""
    
    def test_create_pipeline_step_definition_success(self, db_session: Session):
        """Test creating a new pipeline step definition."""
        # Create parent pipeline first
        pipeline_id = create_pipeline_definition(
            db_session,
            pipeline_name="step_test_pipeline",
            pipeline_structure={"steps": []},
            tenant_id="test-tenant"
        )
        
        # Create step definition
        step_id = create_pipeline_step_definition(
            db_session,
            pipeline_definition_id=pipeline_id,
            pipeline_name="step_test_pipeline",
            step_name="test_step",
            processor_name="test_processor",
            function_name="test_function",
            input_trigger="test_queue",
            output_queues=["output1", "output2"],
            step_order="1",
            is_root=True,
            tenant_id="test-tenant"
        )
        
        assert step_id is not None
        
        # Verify step was created
        step = db_session.query(PipelineStepDefinition).filter_by(id=step_id).first()
        assert step is not None
        assert step.pipeline_definition_id == pipeline_id
        assert step.pipeline_name == "step_test_pipeline"
        assert step.step_name == "test_step"
        assert step.processor_name == "test_processor"
        assert step.function_name == "test_function"
        assert step.input_trigger == "test_queue"
        assert step.output_queues == ["output1", "output2"]
        assert step.step_order == "1"
        assert step.is_root is True
        assert step.tenant_id == "test-tenant"
    
    def test_create_pipeline_step_definition_minimal(self, db_session: Session):
        """Test creating step definition with minimal parameters."""
        # Create parent pipeline first
        pipeline_id = create_pipeline_definition(
            db_session,
            pipeline_name="minimal_step_pipeline",
            pipeline_structure={"steps": []},
            tenant_id="test-tenant"
        )
        
        # Create minimal step definition
        step_id = create_pipeline_step_definition(
            db_session,
            pipeline_definition_id=pipeline_id,
            pipeline_name="minimal_step_pipeline",
            step_name="minimal_step",
            processor_name="minimal_processor",
            tenant_id="test-tenant"
        )
        
        assert step_id is not None
        
        # Verify defaults
        step = db_session.query(PipelineStepDefinition).filter_by(id=step_id).first()
        assert step.function_name is None
        assert step.input_trigger is None
        assert step.output_queues is None  # Defaults to None, not empty list
        assert step.step_order is None
        assert step.is_root is False
    
    def test_get_pipeline_steps_success(self, db_session: Session):
        """Test getting pipeline steps."""
        # Create parent pipeline
        pipeline_id = create_pipeline_definition(
            db_session,
            pipeline_name="steps_test_pipeline",
            pipeline_structure={"steps": []},
            tenant_id="test-tenant"
        )
        
        # Create multiple steps
        step1_id = create_pipeline_step_definition(
            db_session,
            pipeline_definition_id=pipeline_id,
            pipeline_name="steps_test_pipeline",
            step_name="step_1",
            processor_name="processor_1",
            step_order="1",
            tenant_id="test-tenant"
        )
        
        step2_id = create_pipeline_step_definition(
            db_session,
            pipeline_definition_id=pipeline_id,
            pipeline_name="steps_test_pipeline",
            step_name="step_2",
            processor_name="processor_2",
            step_order="2",
            tenant_id="test-tenant"
        )
        
        # Get steps
        result = get_pipeline_steps(db_session, "steps_test_pipeline", "test-tenant")
        
        assert len(result) == 2
        
        # Verify order (should be by step_order)
        assert result[0]["step_name"] == "step_1"
        assert result[1]["step_name"] == "step_2"
        
        # Verify structure
        step1 = result[0]
        assert step1["id"] == step1_id
        assert step1["pipeline_definition_id"] == pipeline_id
        assert step1["pipeline_name"] == "steps_test_pipeline"
        assert step1["processor_name"] == "processor_1"
        assert step1["step_order"] == "1"
        assert "created_at" in step1
        assert "updated_at" in step1
    
    def test_get_pipeline_steps_empty(self, db_session: Session):
        """Test getting steps for pipeline with no steps."""
        result = get_pipeline_steps(db_session, "nonexistent_pipeline", "test-tenant")
        assert result == []


class TestPipelineExecutionCRUD:
    """Test pipeline execution CRUD operations."""
    
    def test_create_pipeline_execution_success(self, db_session: Session):
        """Test creating a new pipeline execution."""
        execution_id = create_pipeline_execution(
            db_session,
            pipeline_id="test-pipeline-id",
            tenant_id="test-tenant",
            trigger_type="manual",
            trigger_source="test_source",
            correlation_id="test-correlation-123"
        )
        
        assert execution_id is not None
        
        # Verify execution was created
        execution = db_session.query(PipelineExecution).filter_by(id=execution_id).first()
        assert execution is not None
        assert execution.pipeline_id == "test-pipeline-id"
        assert execution.tenant_id == "test-tenant"
        assert execution.correlation_id == "test-correlation-123"
        assert execution.status == "started"
        assert execution.trigger_type == "manual"
        assert execution.trigger_source == "test_source"
        assert execution.step_count == 0
        assert execution.message_count == 1
        assert execution.error_count == 0
        assert execution.started_at is not None
        assert execution.completed_at is None
    
    def test_create_pipeline_execution_minimal(self, db_session: Session):
        """Test creating execution with minimal parameters."""
        execution_id = create_pipeline_execution(
            db_session,
            pipeline_id="minimal-pipeline-id",
            tenant_id="test-tenant",
            trigger_type="scheduled"
        )
        
        assert execution_id is not None
        
        # Verify defaults
        execution = db_session.query(PipelineExecution).filter_by(id=execution_id).first()
        assert execution.trigger_source is None
        assert execution.correlation_id is None
        assert execution.status == "started"
    
    def test_update_pipeline_execution_success(self, db_session: Session):
        """Test updating a pipeline execution."""
        # Create execution
        execution_id = create_pipeline_execution(
            db_session,
            pipeline_id="update-test-pipeline",
            tenant_id="test-tenant",
            trigger_type="manual"
        )
        
        # Update execution
        updates = {
            "step_count": 5,
            "message_count": 10,
            "error_count": 1
        }
        
        result = update_pipeline_execution(db_session, execution_id, updates, "test-tenant")
        assert result is True
        
        # Verify update
        execution = db_session.query(PipelineExecution).filter_by(id=execution_id).first()
        assert execution.step_count == 5
        assert execution.message_count == 10
        assert execution.error_count == 1
    
    def test_update_pipeline_execution_not_found(self, db_session: Session):
        """Test updating non-existent execution."""
        result = update_pipeline_execution(db_session, "nonexistent-id", {"status": "completed"}, "test-tenant")
        assert result is False
    
    def test_complete_pipeline_execution_success(self, db_session: Session):
        """Test completing a pipeline execution."""
        # Create execution
        execution_id = create_pipeline_execution(
            db_session,
            pipeline_id="complete-test-pipeline",
            tenant_id="test-tenant",
            trigger_type="manual"
        )
        
        # Complete execution
        result = complete_pipeline_execution(
            db_session,
            execution_id,
            status="completed",
            error_message=None,
            tenant_id="test-tenant"
        )
        
        assert result is True
        
        # Verify completion
        execution = db_session.query(PipelineExecution).filter_by(id=execution_id).first()
        assert execution.status == "completed"
        assert execution.completed_at is not None
        assert execution.duration_ms is not None
        assert execution.duration_ms >= 0
        assert execution.error_message is None
    
    def test_complete_pipeline_execution_with_error(self, db_session: Session):
        """Test completing a pipeline execution with error."""
        # Create execution
        execution_id = create_pipeline_execution(
            db_session,
            pipeline_id="error-test-pipeline",
            tenant_id="test-tenant",
            trigger_type="manual"
        )
        
        # Complete with error
        result = complete_pipeline_execution(
            db_session,
            execution_id,
            status="failed",
            error_message="Test error occurred",
            tenant_id="test-tenant"
        )
        
        assert result is True
        
        # Verify completion
        execution = db_session.query(PipelineExecution).filter_by(id=execution_id).first()
        assert execution.status == "failed"
        assert execution.error_message == "Test error occurred"
        assert execution.completed_at is not None
        assert execution.duration_ms is not None
    
    def test_get_pipeline_execution_success(self, db_session: Session):
        """Test getting a pipeline execution."""
        # Create execution
        execution_id = create_pipeline_execution(
            db_session,
            pipeline_id="get-test-pipeline",
            tenant_id="test-tenant",
            trigger_type="manual",
            trigger_source="test_source",
            correlation_id="test-correlation"
        )
        
        # Get execution
        result = get_pipeline_execution(db_session, execution_id, "test-tenant")
        
        assert result is not None
        assert result["id"] == execution_id
        assert result["pipeline_id"] == "get-test-pipeline"
        assert result["tenant_id"] == "test-tenant"
        assert result["correlation_id"] == "test-correlation"
        assert result["status"] == "started"
        assert result["trigger_type"] == "manual"
        assert result["trigger_source"] == "test_source"
        assert result["step_count"] == 0
        assert result["message_count"] == 1
        assert result["error_count"] == 0
        assert result["started_at"] is not None
        assert result["completed_at"] is None
        assert result["duration_ms"] is None
        assert result["error_message"] is None
        assert result["error_step"] is None
        assert "created_at" in result
        assert "updated_at" in result
    
    def test_get_pipeline_execution_not_found(self, db_session: Session):
        """Test getting non-existent execution."""
        result = get_pipeline_execution(db_session, "nonexistent-id", "test-tenant")
        assert result is None
    
    def test_get_pipeline_execution_wrong_tenant(self, db_session: Session):
        """Test getting execution with wrong tenant."""
        # Create execution for one tenant
        execution_id = create_pipeline_execution(
            db_session,
            pipeline_id="tenant-test-pipeline",
            tenant_id="tenant-1",
            trigger_type="manual"
        )
        
        # Try to get with different tenant
        result = get_pipeline_execution(db_session, execution_id, "tenant-2")
        assert result is None
    
    def test_list_pipeline_executions_success(self, db_session: Session):
        """Test listing pipeline executions."""
        # Create multiple executions
        execution1_id = create_pipeline_execution(
            db_session,
            pipeline_id="list-test-pipeline-1",
            tenant_id="test-tenant",
            trigger_type="manual"
        )
        
        execution2_id = create_pipeline_execution(
            db_session,
            pipeline_id="list-test-pipeline-2",
            tenant_id="test-tenant",
            trigger_type="scheduled"
        )
        
        # List executions
        result = list_pipeline_executions(db_session, "test-tenant")
        
        assert len(result) == 2
        
        # Verify structure
        execution_ids = {e["id"] for e in result}
        assert execution1_id in execution_ids
        assert execution2_id in execution_ids
        
        # Verify each execution has required fields
        for execution in result:
            assert "id" in execution
            assert "pipeline_id" in execution
            assert "correlation_id" in execution
            assert "status" in execution
            assert "started_at" in execution
            assert "trigger_type" in execution
            assert "step_count" in execution
            assert "message_count" in execution
            assert "error_count" in execution
    
    def test_list_pipeline_executions_with_filters(self, db_session: Session):
        """Test listing executions with filters."""
        # Create executions for different pipelines and statuses
        execution1_id = create_pipeline_execution(
            db_session,
            pipeline_id="filter-pipeline-1",
            tenant_id="test-tenant",
            trigger_type="manual"
        )
        
        execution2_id = create_pipeline_execution(
            db_session,
            pipeline_id="filter-pipeline-2",
            tenant_id="test-tenant",
            trigger_type="manual"
        )
        
        # Complete one execution
        complete_pipeline_execution(db_session, execution1_id, "completed", tenant_id="test-tenant")
        
        # Filter by pipeline_id
        result = list_pipeline_executions(db_session, "test-tenant", pipeline_id="filter-pipeline-1")
        assert len(result) == 1
        assert result[0]["id"] == execution1_id
        
        # Filter by status
        result = list_pipeline_executions(db_session, "test-tenant", status="completed")
        assert len(result) == 1
        assert result[0]["id"] == execution1_id
        
        # Filter by status (no matches)
        result = list_pipeline_executions(db_session, "test-tenant", status="failed")
        assert len(result) == 0
    
    def test_list_pipeline_executions_with_limit(self, db_session: Session):
        """Test listing executions with limit."""
        # Create multiple executions
        for i in range(5):
            create_pipeline_execution(
                db_session,
                pipeline_id=f"limit-test-pipeline-{i}",
                tenant_id="test-tenant",
                trigger_type="manual"
            )
        
        # List with limit
        result = list_pipeline_executions(db_session, "test-tenant", limit=3)
        assert len(result) == 3


class TestPipelineStepCRUD:
    """Test pipeline step CRUD operations."""
    
    def test_create_pipeline_step_success(self, db_session: Session):
        """Test creating a new pipeline step."""
        step_id = create_pipeline_step(
            db_session,
            execution_id="test-execution-id",
            pipeline_id="test-pipeline-id",
            tenant_id="test-tenant",
            step_name="test_step",
            processor_name="test_processor",
            message_id="test-message-123",
            function_name="test_function",
            correlation_id="test-correlation"
        )
        
        assert step_id is not None
        
        # Verify step was created
        step = db_session.query(PipelineStep).filter_by(id=step_id).first()
        assert step is not None
        assert step.execution_id == "test-execution-id"
        assert step.pipeline_id == "test-pipeline-id"
        assert step.tenant_id == "test-tenant"
        assert step.step_name == "test_step"
        assert step.processor_name == "test_processor"
        assert step.message_id == "test-message-123"
        assert step.function_name == "test_function"
        assert step.correlation_id == "test-correlation"
        assert step.status == "processing"
        assert step.started_at is not None
        assert step.output_count == 0
        assert step.completed_at is None
    
    def test_create_pipeline_step_minimal(self, db_session: Session):
        """Test creating step with minimal parameters."""
        step_id = create_pipeline_step(
            db_session,
            execution_id="minimal-execution-id",
            pipeline_id="minimal-pipeline-id",
            tenant_id="test-tenant",
            step_name="minimal_step",
            processor_name="minimal_processor",
            message_id="minimal-message-123"
        )
        
        assert step_id is not None
        
        # Verify defaults
        step = db_session.query(PipelineStep).filter_by(id=step_id).first()
        assert step.function_name is None
        assert step.correlation_id is None
        assert step.status == "processing"
        assert step.output_count == 0
    
    def test_complete_pipeline_step_success(self, db_session: Session):
        """Test completing a pipeline step."""
        # Create step
        step_id = create_pipeline_step(
            db_session,
            execution_id="complete-execution-id",
            pipeline_id="complete-pipeline-id",
            tenant_id="test-tenant",
            step_name="complete_step",
            processor_name="complete_processor",
            message_id="complete-message-123"
        )
        
        # Complete step
        result = complete_pipeline_step(
            db_session,
            step_id,
            status="completed",
            output_count=3,
            output_queues=["queue1", "queue2"],
            error_message=None,
            error_type=None
        )
        
        assert result is True
        
        # Verify completion
        step = db_session.query(PipelineStep).filter_by(id=step_id).first()
        assert step.status == "completed"
        assert step.output_count == 3
        assert step.output_queues == ["queue1", "queue2"]
        assert step.completed_at is not None
        assert step.duration_ms is not None
        assert step.duration_ms >= 0
        assert step.error_message is None
        assert step.error_type is None
    
    def test_complete_pipeline_step_with_error(self, db_session: Session):
        """Test completing a pipeline step with error."""
        # Create step
        step_id = create_pipeline_step(
            db_session,
            execution_id="error-execution-id",
            pipeline_id="error-pipeline-id",
            tenant_id="test-tenant",
            step_name="error_step",
            processor_name="error_processor",
            message_id="error-message-123"
        )
        
        # Complete with error
        result = complete_pipeline_step(
            db_session,
            step_id,
            status="failed",
            output_count=0,
            output_queues=[],
            error_message="Test error occurred",
            error_type="ProcessingError"
        )
        
        assert result is True
        
        # Verify completion
        step = db_session.query(PipelineStep).filter_by(id=step_id).first()
        assert step.status == "failed"
        assert step.output_count == 0
        assert step.output_queues == []
        assert step.error_message == "Test error occurred"
        assert step.error_type == "ProcessingError"
        assert step.completed_at is not None
        assert step.duration_ms is not None
    
    def test_get_pipeline_steps_for_execution_success(self, db_session: Session):
        """Test getting steps for a pipeline execution."""
        execution_id = "steps-execution-id"
        
        # Create multiple steps
        step1_id = create_pipeline_step(
            db_session,
            execution_id=execution_id,
            pipeline_id="steps-pipeline-id",
            tenant_id="test-tenant",
            step_name="step_1",
            processor_name="processor_1",
            message_id="message-1"
        )
        
        step2_id = create_pipeline_step(
            db_session,
            execution_id=execution_id,
            pipeline_id="steps-pipeline-id",
            tenant_id="test-tenant",
            step_name="step_2",
            processor_name="processor_2",
            message_id="message-2"
        )
        
        # Get steps
        result = get_pipeline_steps_for_execution(db_session, execution_id, "test-tenant")
        
        assert len(result) == 2
        
        # Verify structure
        step_ids = {s["id"] for s in result}
        assert step1_id in step_ids
        assert step2_id in step_ids
        
        # Verify each step has required fields
        for step in result:
            assert "id" in step
            assert "execution_id" in step
            assert "pipeline_id" in step
            assert "step_name" in step
            assert "processor_name" in step
            assert "message_id" in step
            assert "correlation_id" in step
            assert "status" in step
            assert "started_at" in step
            assert "completed_at" in step
            assert "duration_ms" in step
            assert "output_count" in step
            assert "output_queues" in step
            assert "error_message" in step
            assert "error_type" in step
    
    def test_get_pipeline_steps_for_execution_empty(self, db_session: Session):
        """Test getting steps for execution with no steps."""
        result = get_pipeline_steps_for_execution(db_session, "nonexistent-execution", "test-tenant")
        assert result == []


class TestPipelineUtilsIntegration:
    """Integration tests for pipeline utilities."""
    
    def test_complete_pipeline_workflow(self, db_session: Session):
        """Test complete pipeline workflow from definition to execution."""
        # 1. Create pipeline definition
        pipeline_structure = {
            "steps": [
                {"name": "ingest", "processor": "data_ingester"},
                {"name": "transform", "processor": "data_transformer"},
                {"name": "export", "processor": "data_exporter"}
            ]
        }
        
        pipeline_id = create_pipeline_definition(
            db_session,
            pipeline_name="integration_pipeline",
            pipeline_structure=pipeline_structure,
            description="Integration test pipeline",
            tenant_id="test-tenant"
        )
        
        # 2. Create step definitions
        step1_id = create_pipeline_step_definition(
            db_session,
            pipeline_definition_id=pipeline_id,
            pipeline_name="integration_pipeline",
            step_name="ingest",
            processor_name="data_ingester",
            step_order="1",
            is_root=True,
            tenant_id="test-tenant"
        )
        
        step2_id = create_pipeline_step_definition(
            db_session,
            pipeline_definition_id=pipeline_id,
            pipeline_name="integration_pipeline",
            step_name="transform",
            processor_name="data_transformer",
            step_order="2",
            tenant_id="test-tenant"
        )
        
        # 3. Create pipeline execution
        execution_id = create_pipeline_execution(
            db_session,
            pipeline_id=pipeline_id,
            tenant_id="test-tenant",
            trigger_type="manual",
            correlation_id="integration-test-123"
        )
        
        # 4. Create and complete pipeline steps
        exec_step1_id = create_pipeline_step(
            db_session,
            execution_id=execution_id,
            pipeline_id=pipeline_id,
            tenant_id="test-tenant",
            step_name="ingest",
            processor_name="data_ingester",
            message_id="msg-1",
            correlation_id="integration-test-123"
        )
        
        # Complete first step
        complete_pipeline_step(
            db_session,
            exec_step1_id,
            status="completed",
            output_count=1,
            output_queues=["transform_queue"]
        )
        
        exec_step2_id = create_pipeline_step(
            db_session,
            execution_id=execution_id,
            pipeline_id=pipeline_id,
            tenant_id="test-tenant",
            step_name="transform",
            processor_name="data_transformer",
            message_id="msg-2",
            correlation_id="integration-test-123"
        )
        
        # Complete second step
        complete_pipeline_step(
            db_session,
            exec_step2_id,
            status="completed",
            output_count=1,
            output_queues=["export_queue"]
        )
        
        # 5. Complete pipeline execution
        complete_pipeline_execution(
            db_session,
            execution_id,
            status="completed",
            tenant_id="test-tenant"
        )
        
        # 6. Verify the complete workflow
        
        # Verify pipeline definition
        pipeline_def = get_pipeline_definition(db_session, "integration_pipeline", "test-tenant")
        assert pipeline_def is not None
        assert pipeline_def["pipeline_name"] == "integration_pipeline"
        
        # Verify step definitions
        step_defs = get_pipeline_steps(db_session, "integration_pipeline", "test-tenant")
        assert len(step_defs) == 2
        assert step_defs[0]["step_name"] == "ingest"
        assert step_defs[1]["step_name"] == "transform"
        
        # Verify execution
        execution = get_pipeline_execution(db_session, execution_id, "test-tenant")
        assert execution is not None
        assert execution["status"] == "completed"
        assert execution["correlation_id"] == "integration-test-123"
        assert execution["completed_at"] is not None
        assert execution["duration_ms"] is not None
        
        # Verify execution steps
        exec_steps = get_pipeline_steps_for_execution(db_session, execution_id, "test-tenant")
        assert len(exec_steps) == 2
        
        # Verify all steps completed
        for step in exec_steps:
            assert step["status"] == "completed"
            assert step["completed_at"] is not None
            assert step["duration_ms"] is not None
        
        # Verify pipeline listing
        pipelines = list_pipeline_definitions(db_session, "test-tenant")
        assert len(pipelines) == 1
        assert pipelines[0]["pipeline_name"] == "integration_pipeline"
        
        # Verify execution listing
        executions = list_pipeline_executions(db_session, "test-tenant")
        assert len(executions) == 1
        assert executions[0]["id"] == execution_id
        assert executions[0]["status"] == "completed"