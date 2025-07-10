"""
Unit tests for pipeline discovery utilities.

Tests the pipeline discovery and registration functions for Azure Functions.
"""

import pytest
from datetime import datetime, timezone
from unittest.mock import Mock, patch
from sqlalchemy.orm import Session

from api_exchange_core.db.db_pipeline_definition_models import PipelineDefinition, PipelineStepDefinition
from api_exchange_core.utils.pipeline_discovery_v2 import (
    register_pipeline_definition,
    register_function_step,
    get_pipeline_structure,
    list_pipeline_definitions,
    auto_register_function_step
)


class TestRegisterPipelineDefinition:
    """Test register_pipeline_definition function."""
    
    def test_register_new_pipeline(self, db_session: Session):
        """Test registering a new pipeline definition."""
        # Test data
        pipeline_name = "test_pipeline"
        pipeline_structure = {
            "steps": ["step1", "step2", "step3"],
            "connections": [
                {"from": "step1", "to": "step2"},
                {"from": "step2", "to": "step3"}
            ]
        }
        
        # Register pipeline
        pipeline_id = register_pipeline_definition(
            db_session,
            pipeline_name=pipeline_name,
            pipeline_structure=pipeline_structure,
            version="1.0.0",
            description="Test pipeline for unit tests",
            capture_messages=True
        )
        
        # Verify pipeline was created
        assert pipeline_id is not None
        
        pipeline = db_session.query(PipelineDefinition).filter_by(id=pipeline_id).first()
        assert pipeline is not None
        assert pipeline.pipeline_name == pipeline_name
        assert pipeline.version == "1.0.0"
        assert pipeline.description == "Test pipeline for unit tests"
        assert pipeline.pipeline_structure == pipeline_structure
        assert pipeline.capture_messages is True
        assert pipeline.is_active is True
    
    def test_register_minimal_pipeline(self, db_session: Session):
        """Test registering pipeline with minimal parameters."""
        pipeline_name = "minimal_pipeline"
        pipeline_structure = {"steps": []}
        
        pipeline_id = register_pipeline_definition(
            db_session,
            pipeline_name=pipeline_name,
            pipeline_structure=pipeline_structure
        )
        
        pipeline = db_session.query(PipelineDefinition).filter_by(id=pipeline_id).first()
        assert pipeline.version == "1.0.0"  # Default
        assert pipeline.description is None
        assert pipeline.capture_messages is True  # Default
    
    def test_update_existing_pipeline(self, db_session: Session):
        """Test updating an existing pipeline definition."""
        pipeline_name = "update_pipeline"
        
        # First registration
        original_structure = {"steps": ["old_step"]}
        pipeline_id_1 = register_pipeline_definition(
            db_session,
            pipeline_name=pipeline_name,
            pipeline_structure=original_structure,
            version="1.0.0",
            description="Original description"
        )
        
        # Update registration
        new_structure = {"steps": ["new_step1", "new_step2"]}
        pipeline_id_2 = register_pipeline_definition(
            db_session,
            pipeline_name=pipeline_name,
            pipeline_structure=new_structure,
            version="2.0.0",
            description="Updated description"
        )
        
        # Should return same ID
        assert pipeline_id_1 == pipeline_id_2
        
        # Verify update
        pipeline = db_session.query(PipelineDefinition).filter_by(id=pipeline_id_1).first()
        assert pipeline.version == "2.0.0"
        assert pipeline.description == "Updated description"
        assert pipeline.pipeline_structure == new_structure
    
    @patch('api_exchange_core.utils.pipeline_discovery_v2.get_logger')
    def test_logging_for_new_pipeline(self, mock_get_logger, db_session: Session):
        """Test that creation is logged properly."""
        mock_logger = Mock()
        mock_get_logger.return_value = mock_logger
        
        pipeline_name = "logged_pipeline"
        pipeline_id = register_pipeline_definition(
            db_session,
            pipeline_name=pipeline_name,
            pipeline_structure={"steps": []},
            version="1.0.0"
        )
        
        # Verify logging
        mock_logger.info.assert_called_once()
        call_args = mock_logger.info.call_args
        assert f"Created pipeline definition: {pipeline_name}" in call_args[0][0]
        assert call_args[1]["extra"]["pipeline_name"] == pipeline_name
        assert call_args[1]["extra"]["version"] == "1.0.0"
        assert call_args[1]["extra"]["pipeline_id"] == pipeline_id
    
    @patch('api_exchange_core.utils.pipeline_discovery_v2.get_logger')
    def test_logging_for_updated_pipeline(self, mock_get_logger, db_session: Session):
        """Test that updates are logged properly."""
        mock_logger = Mock()
        mock_get_logger.return_value = mock_logger
        
        pipeline_name = "update_logged_pipeline"
        
        # Create first
        register_pipeline_definition(
            db_session,
            pipeline_name=pipeline_name,
            pipeline_structure={"steps": []}
        )
        
        # Reset mock to check only update call
        mock_logger.reset_mock()
        
        # Update
        pipeline_id = register_pipeline_definition(
            db_session,
            pipeline_name=pipeline_name,
            pipeline_structure={"steps": ["updated"]},
            version="2.0.0"
        )
        
        # Verify update logging
        mock_logger.info.assert_called_once()
        call_args = mock_logger.info.call_args
        assert f"Updated pipeline definition: {pipeline_name}" in call_args[0][0]
        assert call_args[1]["extra"]["pipeline_id"] == pipeline_id


class TestRegisterFunctionStep:
    """Test register_function_step function."""
    
    def test_register_step_with_existing_pipeline(self, db_session: Session):
        """Test registering a step for an existing pipeline."""
        # Create pipeline first
        pipeline_name = "existing_pipeline"
        pipeline_id = register_pipeline_definition(
            db_session,
            pipeline_name=pipeline_name,
            pipeline_structure={"steps": ["step1"]}
        )
        
        # Register step
        step_id = register_function_step(
            db_session,
            pipeline_name=pipeline_name,
            step_name="step1",
            processor_name="TestProcessor",
            function_name="test_function",
            input_trigger="test_queue",
            output_queues=["output1", "output2"],
            step_order="1",
            is_root=True
        )
        
        # Verify step was created
        assert step_id is not None
        
        step = db_session.query(PipelineStepDefinition).filter_by(id=step_id).first()
        assert step is not None
        assert step.pipeline_definition_id == pipeline_id
        assert step.pipeline_name == pipeline_name
        assert step.step_name == "step1"
        assert step.processor_name == "TestProcessor"
        assert step.function_name == "test_function"
        assert step.input_trigger == "test_queue"
        assert step.output_queues == ["output1", "output2"]
        assert step.step_order == "1"
        assert step.is_root is True
    
    def test_register_step_auto_creates_pipeline(self, db_session: Session):
        """Test that registering a step auto-creates pipeline if missing."""
        pipeline_name = "auto_created_pipeline"
        
        # Register step without creating pipeline first
        step_id = register_function_step(
            db_session,
            pipeline_name=pipeline_name,
            step_name="auto_step",
            processor_name="AutoProcessor"
        )
        
        # Verify step was created
        step = db_session.query(PipelineStepDefinition).filter_by(id=step_id).first()
        assert step is not None
        
        # Verify pipeline was auto-created
        pipeline = db_session.query(PipelineDefinition).filter_by(
            pipeline_name=pipeline_name
        ).first()
        assert pipeline is not None
        assert pipeline.description == f"Auto-discovered pipeline for {pipeline_name}"
        assert pipeline.pipeline_structure["auto_discovered"] is True
        assert "discovery_time" in pipeline.pipeline_structure
        assert pipeline.pipeline_structure["steps"] == ["auto_step"]
    
    def test_register_minimal_step(self, db_session: Session):
        """Test registering step with minimal parameters."""
        pipeline_name = "minimal_step_pipeline"
        
        step_id = register_function_step(
            db_session,
            pipeline_name=pipeline_name,
            step_name="minimal_step",
            processor_name="MinimalProcessor"
        )
        
        step = db_session.query(PipelineStepDefinition).filter_by(id=step_id).first()
        assert step.function_name is None
        assert step.input_trigger is None
        assert step.output_queues == []
        assert step.step_order is None
        assert step.is_root is False
    
    def test_update_existing_step(self, db_session: Session):
        """Test updating an existing step."""
        pipeline_name = "update_step_pipeline"
        step_name = "update_step"
        
        # First registration
        step_id_1 = register_function_step(
            db_session,
            pipeline_name=pipeline_name,
            step_name=step_name,
            processor_name="OldProcessor",
            function_name="old_function"
        )
        
        # Update registration
        step_id_2 = register_function_step(
            db_session,
            pipeline_name=pipeline_name,
            step_name=step_name,
            processor_name="NewProcessor",
            function_name="new_function",
            output_queues=["new_output"]
        )
        
        # Should return same ID
        assert step_id_1 == step_id_2
        
        # Verify update
        step = db_session.query(PipelineStepDefinition).filter_by(id=step_id_1).first()
        assert step.processor_name == "NewProcessor"
        assert step.function_name == "new_function"
        assert step.output_queues == ["new_output"]
    
    @patch('api_exchange_core.utils.pipeline_discovery_v2.get_logger')
    def test_step_logging(self, mock_get_logger, db_session: Session):
        """Test that step registration is logged properly."""
        mock_logger = Mock()
        mock_get_logger.return_value = mock_logger
        
        pipeline_name = "logged_step_pipeline"
        step_name = "logged_step"
        
        step_id = register_function_step(
            db_session,
            pipeline_name=pipeline_name,
            step_name=step_name,
            processor_name="LoggedProcessor",
            function_name="logged_function"
        )
        
        # Find the registration log (not the auto-create pipeline log)
        registration_calls = [
            call for call in mock_logger.info.call_args_list
            if "Registered pipeline step" in call[0][0]
        ]
        
        assert len(registration_calls) == 1
        call_args = registration_calls[0]
        assert f"{pipeline_name}.{step_name}" in call_args[0][0]
        assert call_args[1]["extra"]["processor_name"] == "LoggedProcessor"
        assert call_args[1]["extra"]["function_name"] == "logged_function"


class TestGetPipelineStructure:
    """Test get_pipeline_structure function."""
    
    def test_get_existing_pipeline_structure(self, db_session: Session):
        """Test getting structure for existing pipeline with steps."""
        # Setup pipeline with steps
        pipeline_name = "structure_pipeline"
        pipeline_structure = {
            "steps": ["step1", "step2"],
            "connections": [{"from": "step1", "to": "step2"}]
        }
        
        pipeline_id = register_pipeline_definition(
            db_session,
            pipeline_name=pipeline_name,
            pipeline_structure=pipeline_structure,
            version="1.0.0",
            description="Test structure"
        )
        
        # Register steps
        step1_id = register_function_step(
            db_session,
            pipeline_name=pipeline_name,
            step_name="step1",
            processor_name="Processor1",
            function_name="function1",
            input_trigger="queue1",
            output_queues=["queue2"],
            step_order="1",
            is_root=True
        )
        
        step2_id = register_function_step(
            db_session,
            pipeline_name=pipeline_name,
            step_name="step2",
            processor_name="Processor2",
            function_name="function2",
            input_trigger="queue2",
            output_queues=["queue3"],
            step_order="2"
        )
        
        # Get structure
        structure = get_pipeline_structure(db_session, pipeline_name)
        
        assert structure is not None
        assert structure["pipeline_id"] == pipeline_id
        assert structure["pipeline_name"] == pipeline_name
        assert structure["version"] == "1.0.0"
        assert structure["description"] == "Test structure"
        assert structure["capture_messages"] is True
        assert structure["pipeline_structure"] == pipeline_structure
        
        # Check steps
        assert len(structure["steps"]) == 2
        
        step1_data = next(s for s in structure["steps"] if s["step_name"] == "step1")
        assert step1_data["step_id"] == step1_id
        assert step1_data["processor_name"] == "Processor1"
        assert step1_data["function_name"] == "function1"
        assert step1_data["input_trigger"] == "queue1"
        assert step1_data["output_queues"] == ["queue2"]
        assert step1_data["step_order"] == "1"
        assert step1_data["is_root"] is True
        
        step2_data = next(s for s in structure["steps"] if s["step_name"] == "step2")
        assert step2_data["step_id"] == step2_id
        assert step2_data["processor_name"] == "Processor2"
        assert step2_data["function_name"] == "function2"
        assert step2_data["input_trigger"] == "queue2"
        assert step2_data["output_queues"] == ["queue3"]
        assert step2_data["step_order"] == "2"
        assert step2_data["is_root"] is False
        
        # Check timestamps
        assert "created_at" in structure
        assert "updated_at" in structure
    
    def test_get_nonexistent_pipeline(self, db_session: Session):
        """Test getting structure for non-existent pipeline."""
        structure = get_pipeline_structure(db_session, "nonexistent_pipeline")
        assert structure is None
    
    def test_get_inactive_pipeline(self, db_session: Session):
        """Test that inactive pipelines are not returned."""
        # Create pipeline
        pipeline_name = "inactive_pipeline"
        pipeline_id = register_pipeline_definition(
            db_session,
            pipeline_name=pipeline_name,
            pipeline_structure={"steps": []}
        )
        
        # Make it inactive
        pipeline = db_session.query(PipelineDefinition).filter_by(id=pipeline_id).first()
        pipeline.is_active = False
        db_session.commit()
        
        # Should not be found
        structure = get_pipeline_structure(db_session, pipeline_name)
        assert structure is None
    
    def test_get_pipeline_with_no_steps(self, db_session: Session):
        """Test getting structure for pipeline with no steps."""
        pipeline_name = "empty_pipeline"
        register_pipeline_definition(
            db_session,
            pipeline_name=pipeline_name,
            pipeline_structure={"steps": []}
        )
        
        structure = get_pipeline_structure(db_session, pipeline_name)
        
        assert structure is not None
        assert structure["steps"] == []


class TestListPipelineDefinitions:
    """Test list_pipeline_definitions function."""
    
    def test_list_all_active_pipelines(self, db_session: Session):
        """Test listing all active pipelines."""
        # Create multiple pipelines
        pipelines_data = [
            ("pipeline_a", "1.0.0", "Pipeline A"),
            ("pipeline_b", "2.0.0", "Pipeline B"),
            ("pipeline_c", "1.5.0", "Pipeline C")
        ]
        
        pipeline_ids = []
        for name, version, desc in pipelines_data:
            pid = register_pipeline_definition(
                db_session,
                pipeline_name=name,
                pipeline_structure={"steps": []},
                version=version,
                description=desc
            )
            pipeline_ids.append(pid)
        
        # List pipelines
        pipelines = list_pipeline_definitions(db_session)
        
        assert len(pipelines) == 3
        
        # Should be ordered by name
        assert pipelines[0]["pipeline_name"] == "pipeline_a"
        assert pipelines[1]["pipeline_name"] == "pipeline_b"
        assert pipelines[2]["pipeline_name"] == "pipeline_c"
        
        # Check structure
        for pipeline in pipelines:
            assert "pipeline_id" in pipeline
            assert "pipeline_name" in pipeline
            assert "version" in pipeline
            assert "description" in pipeline
            assert "capture_messages" in pipeline
            assert "is_active" in pipeline
            assert "created_at" in pipeline
            assert "updated_at" in pipeline
        
        # Verify specific data
        pipeline_a = pipelines[0]
        assert pipeline_a["pipeline_id"] == pipeline_ids[0]
        assert pipeline_a["version"] == "1.0.0"
        assert pipeline_a["description"] == "Pipeline A"
        assert pipeline_a["is_active"] is True
    
    def test_list_includes_inactive_when_requested(self, db_session: Session):
        """Test listing includes inactive pipelines when requested."""
        # Create active pipeline
        register_pipeline_definition(
            db_session,
            pipeline_name="active_pipeline",
            pipeline_structure={"steps": []}
        )
        
        # Create inactive pipeline
        pipeline_id = register_pipeline_definition(
            db_session,
            pipeline_name="inactive_pipeline",
            pipeline_structure={"steps": []}
        )
        
        # Make it inactive
        pipeline = db_session.query(PipelineDefinition).filter_by(id=pipeline_id).first()
        pipeline.is_active = False
        db_session.commit()
        
        # List active only (default)
        active_pipelines = list_pipeline_definitions(db_session, active_only=True)
        assert len(active_pipelines) == 1
        assert active_pipelines[0]["pipeline_name"] == "active_pipeline"
        
        # List all
        all_pipelines = list_pipeline_definitions(db_session, active_only=False)
        assert len(all_pipelines) == 2
        
        # Find inactive
        inactive = next(p for p in all_pipelines if p["pipeline_name"] == "inactive_pipeline")
        assert inactive["is_active"] is False
    
    def test_list_empty_pipelines(self, db_session: Session):
        """Test listing when no pipelines exist."""
        pipelines = list_pipeline_definitions(db_session)
        assert pipelines == []


class TestAutoRegisterFunctionStep:
    """Test auto_register_function_step function."""
    
    def test_auto_register_with_defaults(self, db_session: Session):
        """Test auto-registration with default values."""
        function_name = "process_orders_function"
        processor_name = "OrderProcessor"
        
        step_id = auto_register_function_step(
            db_session,
            function_name=function_name,
            processor_name=processor_name
        )
        
        # Verify step was created
        step = db_session.query(PipelineStepDefinition).filter_by(id=step_id).first()
        assert step is not None
        
        # Check defaults
        assert step.step_name == function_name  # Defaults to function name
        assert step.pipeline_name == "process"  # Extracted from function name prefix
        assert step.function_name == function_name
        assert step.processor_name == processor_name
        assert step.output_queues == []
        assert step.is_root is False
    
    def test_auto_register_with_explicit_values(self, db_session: Session):
        """Test auto-registration with explicit values."""
        step_id = auto_register_function_step(
            db_session,
            function_name="my_function",
            processor_name="MyProcessor",
            step_name="custom_step",
            pipeline_name="custom_pipeline",
            input_trigger="custom_queue",
            output_queues=["output1", "output2"],
            is_root=True
        )
        
        # Verify step
        step = db_session.query(PipelineStepDefinition).filter_by(id=step_id).first()
        assert step.step_name == "custom_step"
        assert step.pipeline_name == "custom_pipeline"
        assert step.input_trigger == "custom_queue"
        assert step.output_queues == ["output1", "output2"]
        assert step.is_root is True
    
    def test_auto_register_function_without_underscore(self, db_session: Session):
        """Test auto-registration when function name has no underscore."""
        function_name = "simplefunction"
        
        step_id = auto_register_function_step(
            db_session,
            function_name=function_name,
            processor_name="SimpleProcessor"
        )
        
        step = db_session.query(PipelineStepDefinition).filter_by(id=step_id).first()
        assert step.pipeline_name == function_name  # Uses full name when no underscore
    
    def test_auto_register_calls_register_function_step(self, db_session: Session):
        """Test that auto_register calls register_function_step properly."""
        # This is really testing the integration
        function_name = "test_auto_function"
        processor_name = "TestAutoProcessor"
        
        step_id = auto_register_function_step(
            db_session,
            function_name=function_name,
            processor_name=processor_name,
            output_queues=["test_output"]
        )
        
        # Verify the step exists with expected values
        step = db_session.query(PipelineStepDefinition).filter_by(id=step_id).first()
        assert step is not None
        assert step.step_name == function_name
        assert step.pipeline_name == "test"  # Prefix before underscore
        assert step.output_queues == ["test_output"]
        
        # Verify pipeline was auto-created
        pipeline = db_session.query(PipelineDefinition).filter_by(
            pipeline_name="test"
        ).first()
        assert pipeline is not None


class TestPipelineDiscoveryIntegration:
    """Integration tests for pipeline discovery."""
    
    def test_complete_pipeline_registration_workflow(self, db_session: Session):
        """Test complete workflow of registering pipeline and steps."""
        # 1. Register pipeline definition
        pipeline_name = "order_processing"
        pipeline_structure = {
            "steps": ["validate_order", "process_payment", "fulfill_order"],
            "connections": [
                {"from": "validate_order", "to": "process_payment"},
                {"from": "process_payment", "to": "fulfill_order"}
            ],
            "metadata": {
                "author": "test",
                "created": datetime.now(timezone.utc).isoformat()
            }
        }
        
        pipeline_id = register_pipeline_definition(
            db_session,
            pipeline_name=pipeline_name,
            pipeline_structure=pipeline_structure,
            version="1.0.0",
            description="Order processing pipeline"
        )
        
        # 2. Register function steps
        step_ids = []
        
        # Validation step
        step_ids.append(register_function_step(
            db_session,
            pipeline_name=pipeline_name,
            step_name="validate_order",
            processor_name="OrderValidator",
            function_name="validate_order_func",
            input_trigger="order_queue",
            output_queues=["payment_queue"],
            step_order="1",
            is_root=True
        ))
        
        # Payment step
        step_ids.append(register_function_step(
            db_session,
            pipeline_name=pipeline_name,
            step_name="process_payment",
            processor_name="PaymentProcessor",
            function_name="process_payment_func",
            input_trigger="payment_queue",
            output_queues=["fulfillment_queue"],
            step_order="2"
        ))
        
        # Fulfillment step
        step_ids.append(register_function_step(
            db_session,
            pipeline_name=pipeline_name,
            step_name="fulfill_order",
            processor_name="FulfillmentProcessor",
            function_name="fulfill_order_func",
            input_trigger="fulfillment_queue",
            output_queues=["notification_queue"],
            step_order="3"
        ))
        
        # 3. Get pipeline structure
        structure = get_pipeline_structure(db_session, pipeline_name)
        
        assert structure is not None
        assert structure["pipeline_id"] == pipeline_id
        assert len(structure["steps"]) == 3
        
        # Verify step order
        steps_by_order = sorted(structure["steps"], key=lambda s: s["step_order"])
        assert steps_by_order[0]["step_name"] == "validate_order"
        assert steps_by_order[1]["step_name"] == "process_payment"
        assert steps_by_order[2]["step_name"] == "fulfill_order"
        
        # Verify connections via queues
        assert steps_by_order[0]["output_queues"] == ["payment_queue"]
        assert steps_by_order[1]["input_trigger"] == "payment_queue"
        assert steps_by_order[1]["output_queues"] == ["fulfillment_queue"]
        assert steps_by_order[2]["input_trigger"] == "fulfillment_queue"
        
        # 4. List pipelines
        pipelines = list_pipeline_definitions(db_session)
        assert len(pipelines) == 1
        assert pipelines[0]["pipeline_name"] == pipeline_name
    
    def test_auto_discovery_workflow(self, db_session: Session):
        """Test auto-discovery workflow where functions self-register."""
        # Simulate multiple Azure Functions self-registering on startup
        functions = [
            ("data_ingestion_func", "DataIngester", True),
            ("data_transform_func", "DataTransformer", False),
            ("data_export_func", "DataExporter", False)
        ]
        
        for func_name, processor, is_root in functions:
            auto_register_function_step(
                db_session,
                function_name=func_name,
                processor_name=processor,
                is_root=is_root,
                input_trigger=f"{func_name}_queue" if not is_root else "source_queue",
                output_queues=[f"{func_name}_output"] if func_name != "data_export_func" else []
            )
        
        # Check that pipeline was auto-created
        pipelines = list_pipeline_definitions(db_session)
        assert len(pipelines) == 1
        assert pipelines[0]["pipeline_name"] == "data"
        
        # Get full structure
        structure = get_pipeline_structure(db_session, "data")
        assert len(structure["steps"]) == 3
        
        # Verify all functions were registered
        step_names = {step["step_name"] for step in structure["steps"]}
        assert step_names == {"data_ingestion_func", "data_transform_func", "data_export_func"}
        
        # Verify root step
        root_steps = [s for s in structure["steps"] if s["is_root"]]
        assert len(root_steps) == 1
        assert root_steps[0]["step_name"] == "data_ingestion_func"