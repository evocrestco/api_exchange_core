"""
Coffee Pipeline Azure Functions App

This app demonstrates a complete data integration pipeline using the API Exchange Core framework.
It processes pretentious coffee orders through three stages:

1. Order Ingestion (HTTP) → transforms pretentious orders to canonical format
2. Complexity Analysis (Queue) → analyzes and enhances canonical data  
3. Human Translation (Queue) → converts to human-readable output

Pipeline Flow:
HTTP POST → [complexity-analysis queue] → [human-translation queue] → Logs

Features:
- Full framework integration with automatic entity persistence
- State tracking for complete audit trails
- Error recording and handling
- Database operations with tenant context
- Comprehensive logging and metrics

Run with: func start (after starting Azurite)
Test with: POST to http://localhost:7071/api/order
"""

import json
import os
import sys
import azure.functions as func

# Add the project root to Python path for imports
project_root = os.path.join(os.path.dirname(__file__), '..', '..')
sys.path.insert(0, project_root)

# Import framework components
from src.processors.processor_factory import create_processor_handler
from src.processing.processor_config import ProcessorConfig
from src.processing.processing_service import ProcessingService
from src.processing.duplicate_detection import DuplicateDetectionService
from src.processing.entity_attributes import EntityAttributeBuilder
from src.services.entity_service import EntityService
from src.repositories.entity_repository import EntityRepository
from src.db.db_config import DatabaseConfig, DatabaseManager
from src.processors.message import Message
from src.context.tenant_context import tenant_context

# Import our processors
from processors.order_ingestion_processor import OrderIngestionProcessor
from processors.complexity_analysis_processor import ComplexityAnalysisProcessor
from processors.human_translation_processor import HumanTranslationProcessor

# Create the Azure Functions app
app = func.FunctionApp()

# Initialize framework services (once per app)
db_config = DatabaseConfig(
    db_type="postgres",
    host="localhost",
    port="5432",
    database="coffee_pipeline",
    username="coffee_admin",
    password="pretentious_password_123"
)
db_manager = DatabaseManager(db_config)
entity_repository = EntityRepository(db_manager=db_manager)
entity_service = EntityService(entity_repository=entity_repository)
duplicate_detection_service = DuplicateDetectionService(entity_repository=entity_repository)
attribute_builder = EntityAttributeBuilder()
processing_service = ProcessingService(
    entity_service=entity_service,
    entity_repository=entity_repository,
    duplicate_detection_service=duplicate_detection_service,
    attribute_builder=attribute_builder
)

# Create processor handlers with framework integration
order_ingestion_handler = create_processor_handler(
    processor_class=OrderIngestionProcessor,
    config=ProcessorConfig(
        processor_name="order_ingestion",
        processor_version="1.0.0",
        enable_state_tracking=True,
        is_source_processor=True,
        enable_duplicate_detection=True,
        processing_stage="ingestion"
    ),
    entity_service=entity_service,
    entity_repository=entity_repository,
    processing_service=processing_service
)

complexity_analysis_handler = create_processor_handler(
    processor_class=ComplexityAnalysisProcessor,
    config=ProcessorConfig(
        processor_name="complexity_analysis",
        processor_version="1.0.0",
        enable_state_tracking=True,
        is_source_processor=False,
        update_entity_attributes=True,
        processing_stage="analysis"
    ),
    entity_service=entity_service,
    entity_repository=entity_repository,
    processing_service=processing_service
)

human_translation_handler = create_processor_handler(
    processor_class=HumanTranslationProcessor,
    config=ProcessorConfig(
        processor_name="human_translation",
        processor_version="1.0.0",
        enable_state_tracking=True,
        is_terminal_processor=True,
        processing_stage="final"
    ),
    entity_service=entity_service,
    entity_repository=entity_repository,
    processing_service=processing_service
)


@app.function_name(name="OrderIngestion")
@app.route(route="order", methods=["POST"], auth_level=func.AuthLevel.ANONYMOUS)
@app.queue_output(arg_name="complexityqueue", queue_name="complexity-analysis", connection="AzureWebJobsStorage")
def order_ingestion(req: func.HttpRequest, complexityqueue: func.Out[str]) -> func.HttpResponse:
    """
    HTTP triggered function for coffee order ingestion.
    
    Receives pretentious coffee orders via POST and routes them to complexity analysis.
    Uses the unified processor framework for automatic entity persistence, state tracking, and error recording.
    
    Example request:
    POST /api/order
    {
        "order": "Triple-shot, oat milk, half-caf, organic, fair-trade, single-origin Ethiopian Yirgacheffe with a hint of Madagascar vanilla, served at exactly 140°F in a hand-thrown ceramic cup"
    }
    
    Framework features:
    - Automatic entity creation and versioning
    - State tracking for audit trails
    - Error recording in database
    - Tenant context management
    """
    try:
        # Parse request body
        request_data = req.get_json()
        if not request_data or not request_data.get("order"):
            return func.HttpResponse(
                "Invalid request: 'order' field required",
                status_code=400
            )
        
        # Create message for processing
        message = Message.create_entity_message(
            external_id=f"order-{req.headers.get('x-request-id', 'unknown')}",
            canonical_type="coffee_order",
            source="artisanal_api",
            tenant_id="coffee_shop",
            payload=request_data
        )
        
        # Process using framework handler with tenant context
        with tenant_context("coffee_shop"):
            result = order_ingestion_handler.handle_message(message)
        
        if result["success"]:
            # Route output messages to queue
            for output_msg in result["output_messages"]:
                complexityqueue.set(json.dumps(output_msg))
            
            return func.HttpResponse(
                json.dumps({
                    "status": "success",
                    "message": "Order processed successfully",
                    "order_id": message.entity_reference.external_id,
                    "pretentiousness_score": result["processing_metadata"].get("pretentiousness_score", 0)
                }),
                mimetype="application/json"
            )
        else:
            return func.HttpResponse(
                json.dumps({
                    "status": "error",
                    "message": result["error_message"]
                }),
                status_code=500,
                mimetype="application/json"
            )
            
    except Exception as e:
        return func.HttpResponse(
            json.dumps({
                "status": "error",
                "message": f"Processing failed: {str(e)}"
            }),
            status_code=500,
            mimetype="application/json"
        )


@app.function_name(name="ComplexityAnalysis")
@app.queue_trigger(arg_name="msg", queue_name="complexity-analysis", connection="AzureWebJobsStorage")
@app.queue_output(arg_name="translationqueue", queue_name="human-translation", connection="AzureWebJobsStorage")
def complexity_analysis(msg: func.QueueMessage, translationqueue: func.Out[str]) -> None:
    """
    Queue triggered function for complexity analysis.
    
    Analyzes canonical coffee orders for complexity, preparation time, and barista stress factors.
    Routes enhanced orders to human translation.
    Uses the unified processor framework for automatic state tracking and error recording.
    
    Framework features:
    - Automatic state transitions recording
    - Error handling with database persistence
    - Tenant context preservation
    - Comprehensive logging and metrics
    """
    try:
        # Parse queue message
        message_data = json.loads(msg.get_body().decode('utf-8'))
        
        # Extract tenant_id from message
        tenant_id = message_data.get("entity_reference", {}).get("tenant_id", "coffee_shop")
        
        # Process using framework handler with tenant context
        with tenant_context(tenant_id):
            result = complexity_analysis_handler.handle_message(message_data)
        
        if result["success"]:
            # Route output messages to next queue
            for output_msg in result["output_messages"]:
                translationqueue.set(json.dumps(output_msg))
        # Error handling is automatic in the framework
        
    except Exception as e:
        # Framework handles errors, but log for debugging
        print(f"Error in complexity analysis function: {str(e)}")


@app.function_name(name="HumanTranslation") 
@app.queue_trigger(arg_name="msg", queue_name="human-translation", connection="AzureWebJobsStorage")
def human_translation(msg: func.QueueMessage) -> None:
    """
    Terminal queue triggered function for human translation.
    
    Converts complexity-analyzed canonical orders to human-readable language.
    Outputs final results to logs (terminal processor).
    Uses the unified processor framework for complete pipeline tracking.
    
    Framework features:
    - Final state transitions to completed status
    - Complete audit trail from HTTP request to final output
    - Error handling with database persistence
    - Comprehensive pipeline metrics and logging
    """
    try:
        # Parse queue message
        message_data = json.loads(msg.get_body().decode('utf-8'))
        
        # Extract tenant_id from message
        tenant_id = message_data.get("entity_reference", {}).get("tenant_id", "coffee_shop")
        
        # Process using framework handler with tenant context (terminal - no output queue)
        with tenant_context(tenant_id):
            result = human_translation_handler.handle_message(message_data)
        
        # Terminal processor outputs to logs only
        # Framework handles final state transitions automatically
        
    except Exception as e:
        # Framework handles errors, but log for debugging
        print(f"Error in human translation function: {str(e)}")