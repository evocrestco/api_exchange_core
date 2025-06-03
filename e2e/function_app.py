"""
E2E Testing Azure Functions App

This app demonstrates comprehensive testing of the API Exchange Core framework using
Azure Functions. It validates entity processing, state tracking, error handling, and
queue-based processor chaining through various test scenarios.

Pipeline Flow:
HTTP POST → [scenario-routing queue] → [validation queue] → [results queue]

Test Scenarios:
- Good Path: Entity creates successfully with proper state transitions
- Bad Path: Validation errors handled gracefully with error states
- Ugly Path: Chaos testing with random failures and retry logic

Run with: func start (after starting Docker Compose)
Test with: POST to http://localhost:7071/api/test-scenario
"""

import json
import os
import sys
import azure.functions as func
from typing import Dict, Any

# Add the project root to Python path for imports
project_root = os.path.join(os.path.dirname(__file__), '..')
sys.path.insert(0, project_root)

# Ensure Azure Functions settings are available as environment variables
# This is needed for the framework to access AzureWebJobsStorage
if "AzureWebJobsStorage" not in os.environ:
    # Try to get from local.settings.json during local development
    try:
        import json
        settings_path = os.path.join(os.path.dirname(__file__), 'local.settings.json')
        with open(settings_path, 'r') as f:
            settings = json.load(f)
            for key, value in settings.get('Values', {}).items():
                if key not in os.environ:
                    os.environ[key] = value
    except:
        pass

# Import framework components
from src.processors.processor_factory import create_processor_handler
from src.processors.processor_handler import ProcessorHandler
from src.processing.processor_config import ProcessorConfig
from src.processing.processing_service import ProcessingService
from src.processing.duplicate_detection import DuplicateDetectionService
from src.processing.entity_attributes import EntityAttributeBuilder
from src.services.entity_service import EntityService
from src.services.state_tracking_service import StateTrackingService
from src.services.processing_error_service import ProcessingErrorService
from src.repositories.entity_repository import EntityRepository
from src.repositories.state_transition_repository import StateTransitionRepository
from src.repositories.processing_error_repository import ProcessingErrorRepository
from src.db.db_config import DatabaseConfig, DatabaseManager, import_all_models
from src.processors.message import Message
from src.processors.processing_result import ProcessingStatus
from src.context.tenant_context import tenant_context
from src.utils.logger import get_logger

# Import test processors
from processors.verification_processor import VerificationProcessor
from processors.validation_processor import ValidationProcessor

# Create the Azure Functions app
app = func.FunctionApp()

# Initialize framework services (once per app)
logger = get_logger()

# Import all models to ensure proper initialization
import_all_models()

# Database configuration from environment
db_config = DatabaseConfig(
    db_type="postgres",
    host=os.getenv("DB_HOST", "localhost"),
    port=os.getenv("DB_PORT", "5432"),
    database=os.getenv("DB_NAME", "e2e_test"),
    username=os.getenv("DB_USER", "test_user"),
    password=os.getenv("DB_PASSWORD", "test_password")
)

db_manager = DatabaseManager(db_config)

# Initialize repositories
entity_repository = EntityRepository(db_manager=db_manager)
state_transition_repository = StateTransitionRepository(db_manager=db_manager)
processing_error_repository = ProcessingErrorRepository(db_manager=db_manager)

# Initialize services
entity_service = EntityService(entity_repository=entity_repository)
state_tracking_service = StateTrackingService(db_manager=db_manager)
processing_error_service = ProcessingErrorService(repository=processing_error_repository)
duplicate_detection_service = DuplicateDetectionService(entity_repository=entity_repository)
attribute_builder = EntityAttributeBuilder()

# Initialize processing service with all dependencies
processing_service = ProcessingService(
    entity_service=entity_service,
    entity_repository=entity_repository,
    duplicate_detection_service=duplicate_detection_service,
    attribute_builder=attribute_builder
)

# Set optional services
processing_service.set_state_tracking_service(state_tracking_service)
processing_service.set_processing_error_service(processing_error_service)

# Create processors and wrap with ProcessorHandler for proper infrastructure support
verification_config = ProcessorConfig(
    processor_name="scenario_router",
    processor_version="1.0.0",
    enable_state_tracking=True,
    is_source_processor=True,
    enable_duplicate_detection=True,
    processing_stage="routing"
)

verification_processor_impl = VerificationProcessor(
    entity_service=entity_service,
    processing_service=processing_service,
    config=verification_config,
    state_tracking_service=state_tracking_service,
    processing_error_service=processing_error_service
)

verification_processor = ProcessorHandler(
    processor=verification_processor_impl,
    config=verification_config,
    processing_service=processing_service,
    state_tracking_service=state_tracking_service,
    error_service=processing_error_service
)

validation_config = ProcessorConfig(
    processor_name="scenario_validator",
    processor_version="1.0.0",
    enable_state_tracking=True,
    is_terminal_processor=True,
    processing_stage="validation"
)

validation_processor_impl = ValidationProcessor(
    config=validation_config,
    processing_error_service=processing_error_service
)

validation_processor = ProcessorHandler(
    processor=validation_processor_impl,
    config=validation_config,
    processing_service=processing_service,
    state_tracking_service=state_tracking_service,
    error_service=processing_error_service
)


@app.function_name(name="ScenarioRouter")
@app.route(route="test-scenario", methods=["POST"], auth_level=func.AuthLevel.ANONYMOUS)
@app.queue_output(arg_name="routingqueue", queue_name="%SCENARIO_ROUTING_QUEUE%", connection="AzureWebJobsStorage")
def scenario_router(req: func.HttpRequest, routingqueue: func.Out[str]) -> func.HttpResponse:
    """
    HTTP triggered function for test scenario initiation.
    
    Receives test scenarios via POST and routes them through the testing pipeline.
    Supports good/bad/ugly test scenarios with embedded configuration.
    
    Example request:
    POST /api/test-scenario
    {
        "scenario": "good|bad|ugly",
        "test_id": "unique-test-id",
        "test_data": {...},
        "error_injection": {...}  # Optional for bad/ugly scenarios
    }
    """
    try:
        # Parse request body
        request_data = req.get_json()
        if not request_data:
            return func.HttpResponse(
                json.dumps({
                    "status": "error",
                    "message": "Invalid request: JSON body required"
                }),
                status_code=400,
                mimetype="application/json"
            )
        
        # Validate scenario type
        scenario_type = request_data.get("scenario", "good")
        if scenario_type not in ["good", "bad", "ugly"]:
            return func.HttpResponse(
                json.dumps({
                    "status": "error",
                    "message": f"Invalid scenario type: {scenario_type}. Must be 'good', 'bad', or 'ugly'"
                }),
                status_code=400,
                mimetype="application/json"
            )
        
        # Create message for processing
        test_id = request_data.get("test_id", f"test-{scenario_type}-{req.headers.get('x-request-id', 'unknown')}")
        tenant_id = os.getenv("TENANT_ID", "e2e_test_tenant")
        
        message = Message.create_entity_message(
            external_id=test_id,
            canonical_type="test_entity",
            source="e2e_test",
            tenant_id=tenant_id,
            payload=request_data
        )
        
        # Process using verification processor with tenant context via ProcessorHandler
        with tenant_context(tenant_id):
            result = verification_processor.execute(message)
        
        if result.status == ProcessingStatus.SUCCESS:
            # Route output messages to queue
            for output_msg in result.output_messages:
                # Convert Message object to dict manually
                message_dict = {
                    "entity_reference": {
                        "external_id": output_msg.entity_reference.external_id,
                        "canonical_type": output_msg.entity_reference.canonical_type,
                        "source": output_msg.entity_reference.source,
                        "tenant_id": output_msg.entity_reference.tenant_id
                    },
                    "payload": output_msg.payload,
                    "metadata": output_msg.metadata,
                    "correlation_id": output_msg.correlation_id
                }
                routingqueue.set(json.dumps(message_dict))
            
            return func.HttpResponse(
                json.dumps({
                    "status": "success",
                    "message": f"{scenario_type.capitalize()} scenario initiated",
                    "test_id": test_id,
                    "processing_metadata": result.processing_metadata
                }),
                mimetype="application/json"
            )
        else:
            return func.HttpResponse(
                json.dumps({
                    "status": "error",
                    "message": result.error_message
                }),
                status_code=500,
                mimetype="application/json"
            )
            
    except Exception as e:
        logger.error(f"Scenario router error: {str(e)}", exc_info=True)
        return func.HttpResponse(
            json.dumps({
                "status": "error",
                "message": f"Processing failed: {str(e)}"
            }),
            status_code=500,
            mimetype="application/json"
        )


@app.function_name(name="ScenarioValidation")
@app.queue_trigger(arg_name="msg", queue_name="%SCENARIO_ROUTING_QUEUE%", connection="AzureWebJobsStorage")
@app.queue_output(arg_name="resultsqueue", queue_name="%TEST_RESULTS_QUEUE%", connection="AzureWebJobsStorage")
def scenario_validation(msg: func.QueueMessage, resultsqueue: func.Out[str]) -> None:
    """
    Queue triggered function for scenario validation.
    
    Validates test scenarios and records results for analysis.
    Handles error injection for bad/ugly scenarios.
    """
    try:
        # Parse queue message
        message_data = json.loads(msg.get_body().decode('utf-8'))
        
        # Extract tenant_id from message
        tenant_id = message_data.get("entity_reference", {}).get("tenant_id", os.getenv("TENANT_ID", "e2e_test_tenant"))
        
        # Process using validation processor with tenant context
        with tenant_context(tenant_id):
            # Convert message_data back to Message object
            from src.processors.message import EntityReference
            entity_ref = EntityReference(
                external_id=message_data.get("entity_reference", {}).get("external_id"),
                canonical_type=message_data.get("entity_reference", {}).get("canonical_type", "test_entity"),
                source=message_data.get("entity_reference", {}).get("source", "e2e_test"),
                tenant_id=tenant_id
            )
            message = Message(
                entity_reference=entity_ref,
                payload=message_data.get("payload", {}),
                metadata=message_data.get("metadata", {}),
                correlation_id=message_data.get("correlation_id")
            )
            result = validation_processor.execute(message)
        
        # Always output results for test analysis
        test_result = {
            "test_id": message_data.get("entity_reference", {}).get("external_id"),
            "scenario": message_data.get("payload", {}).get("scenario", "unknown"),
            "success": result.status == ProcessingStatus.SUCCESS,
            "metadata": result.processing_metadata,
            "error": result.error_message if result.status != ProcessingStatus.SUCCESS else None
        }
        
        resultsqueue.set(json.dumps(test_result))
        
    except Exception as e:
        # Log error but also record it as a test result
        logger.error(f"Scenario validation error: {str(e)}", exc_info=True)
        
        error_result = {
            "test_id": "unknown",
            "scenario": "error",
            "success": False,
            "error": str(e)
        }
        resultsqueue.set(json.dumps(error_result))


@app.function_name(name="TestResultCollector")
@app.queue_trigger(arg_name="msg", queue_name="%TEST_RESULTS_QUEUE%", connection="AzureWebJobsStorage")
def test_result_collector(msg: func.QueueMessage) -> None:
    """
    Terminal queue triggered function for test result collection.
    
    Collects and logs test results for analysis.
    This is a terminal processor - results are logged for external analysis.
    """
    try:
        # Parse test result
        result_data = json.loads(msg.get_body().decode('utf-8'))
        
        # Log test result with structured format
        logger.info(
            "TEST_RESULT",
            extra={
                "test_id": result_data.get("test_id"),
                "scenario": result_data.get("scenario"),
                "success": result_data.get("success"),
                "metadata": result_data.get("metadata"),
                "error": result_data.get("error")
            }
        )
        
        # For debugging, also print to console
        if result_data.get("success"):
            print(f"✅ Test {result_data.get('test_id')} ({result_data.get('scenario')}) passed")
        else:
            print(f"❌ Test {result_data.get('test_id')} ({result_data.get('scenario')}) failed: {result_data.get('error')}")
        
    except Exception as e:
        logger.error(f"Test result collector error: {str(e)}", exc_info=True)