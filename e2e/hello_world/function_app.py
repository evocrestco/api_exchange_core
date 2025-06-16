"""
Hello World Azure Functions App - Simple timer-triggered source processor
"""

import os
import sys
import uuid
from datetime import datetime, UTC
from typing import Dict, Any

import azure.functions as func

# Add the project root to Python path for imports
project_root = os.path.join(os.path.dirname(__file__), '../..')
sys.path.insert(0, project_root)

# Disable verbose Azure SDK logging
import logging
logging.getLogger('azure').setLevel(logging.WARNING)

# Import framework components
from src.processors.v2.processor_interface import ProcessorInterface, ProcessorContext
from src.processors.v2.processor_factory import create_processor_handler
from src.processors.v2.message import Message
from src.processors.processing_result import ProcessingResult, ProcessingStatus
from src.processors.v2.output_handlers import QueueOutputHandler
from src.db.db_config import import_all_models
from src.db.db_entity_models import Entity
from src.context.tenant_context import tenant_context
from src.utils.logger import get_logger
from src.utils.hash_utils import calculate_entity_hash

# Create the Azure Functions app
app = func.FunctionApp()

# Initialize logging and models
logger = get_logger()
import_all_models()


class HelloWorldProcessor(ProcessorInterface):
    """Simple v2 processor that generates hello world data."""
    
    def process(self, message: Message, context: ProcessorContext) -> ProcessingResult:
        """
        Business logic - generate hello world data and persist it.
        
        In v2, the processor controls everything:
        - Data transformation
        - When to persist entities
        - Output message creation
        """
        try:
            # Transform data to canonical format
            canonical_data = {
                "message": "Hello, World!",
                "timestamp": datetime.now(UTC).isoformat(),
                "generated_by": "HelloWorldProcessor",
                "external_id": message.entity_reference.external_id
            }
            
            # Persist the entity using context
            entity_id = context.create_entity(
                external_id=message.entity_reference.external_id,
                canonical_type="greeting",
                source="hello_world_generator",
                data=canonical_data,
                metadata={"processor_version": "2.0.0"}
            )
            
            # Create success result with entity info
            result = ProcessingResult.create_success()
            result.entities_created = [entity_id]
            
            # Add output handler to send to next queue in pipeline
            # In a real scenario, this could route to different queues based on business logic
            output_queue_handler = QueueOutputHandler(
                destination="greetings-output",
                config={
                    "connection_string": os.getenv("AzureWebJobsStorage", "UseDevelopmentStorage=true"),
                    "auto_create_queue": True,
                    "message_ttl_seconds": 86400  # 1 day
                }
            )
            result.add_output_handler(output_queue_handler)
            
            # Optionally add more handlers (e.g., notifications, file output for audit)
            # result.add_output_handler(FileOutputHandler(...))
            # result.add_output_handler(WebhookOutputHandler(...))
            
            return result
            
        except Exception as e:
            return ProcessingResult.create_failure(
                error_message=f"Failed to generate hello world: {str(e)}",
                error_code="HELLO_WORLD_GENERATION_FAILED",
                can_retry=True
            )
    
    def validate_message(self, message: Message) -> bool:
        return True
    
    def get_processor_info(self) -> Dict[str, Any]:
        return {"name": "HelloWorldProcessor", "version": "2.0.0", "type": "source"}
    
    def can_retry(self, error: Exception) -> bool:
        return True


# Use v2 factory - no db_manager needed, will create from environment!
hello_world_processor = create_processor_handler(
    processor=HelloWorldProcessor()
)


@app.function_name(name="HelloWorldGenerator")
@app.timer_trigger(schedule="0 */1 * * * *", arg_name="timer")  # Every minute
def hello_world_generator(timer: func.TimerRequest) -> None:
    """Timer-triggered function that generates hello world data every minute."""
    try:
        # Create entity first, then message with entity - v2 approach
        # ProcessorHandler will handle tenant context from TENANT_ID env var
        external_id = f"hello-{uuid.uuid4().hex[:8]}"
        
        # Prepare payload data
        payload = {
            "message": "Hello, World!",
            "timestamp": datetime.now(UTC).isoformat(),
            "generated_by": "HelloWorldProcessor"
        }
        
        # Create entity first (without tenant context - ProcessorHandler will handle it)
        entity = Entity.create(
            tenant_id=os.getenv("TENANT_ID", "e2e_test_tenant"),
            external_id=external_id,
            canonical_type="greeting",
            source="hello_world_generator",
            content_hash=calculate_entity_hash(payload)
        )
        
        # Create message with entity
        message = Message.create_entity_message(
            entity=entity,
            payload=payload
        )
        
        # Execute via ProcessorHandler - it will handle tenant context
        result = hello_world_processor.execute(message)
        
        # OPTIONAL: Validate entity was persisted correctly
        # NOTE: In production, you would typically remove this validation code.
        # ProcessorContext.create_entity() is trusted to work correctly.
        # This validation is only here to demonstrate/verify the framework works.
        if result.status == ProcessingStatus.SUCCESS and result.entities_created:
            try:
                from src.repositories.entity_repository import EntityRepository
                from src.services.entity_service import EntityService
                from src.processors.v2.processor_factory import create_db_manager

                # Validation needs tenant context too
                with tenant_context(os.getenv("TENANT_ID", "e2e_test_tenant")):
                    # Create EntityService to validate persistence (same config as processor)
                    db_manager = create_db_manager()
                    db_session = db_manager.get_session()
                    entity_repo = EntityRepository(db_session)
                    entity_service = EntityService(entity_repo)

                    # Retrieve the created entity
                    entity_id = result.entities_created[0]
                    retrieved_entity = entity_service.get_entity(entity_id)

                    if retrieved_entity:
                        logger.info(
                            "✅ Entity validation successful - found in database",
                            extra={
                                "entity_id": entity_id,
                                "external_id": retrieved_entity.external_id,
                                "canonical_type": retrieved_entity.canonical_type,
                                "version": retrieved_entity.version,
                                "tenant_id": retrieved_entity.tenant_id
                            }
                        )
                    else:
                        logger.error(
                            "❌ Entity validation failed - not found in database",
                            extra={"entity_id": entity_id}
                        )
            except Exception as e:
                logger.error(
                    "💥 Entity validation exception",
                    extra={
                        "entity_id": result.entities_created[0] if result.entities_created else "unknown",
                        "error": str(e),
                        "error_type": type(e).__name__
                    },
                    exc_info=True
                )
        
        if result.status == ProcessingStatus.SUCCESS:
            logger.info(
                "✅ Hello world generated successfully",
                extra={
                    "external_id": external_id,
                    "entities_created": result.entities_created,
                    "entity_count": len(result.entities_created),
                    "duration_ms": result.processing_duration_ms,
                    "processor": "HelloWorldProcessor"
                }
            )
        else:
            logger.error(
                "❌ Hello world generation failed",
                extra={
                    "external_id": external_id,
                    "status": result.status.value,
                    "error_code": result.error_code,
                    "error_message": result.error_message,
                    "can_retry": result.can_retry,
                    "processor": "HelloWorldProcessor"
                }
            )
            
    except Exception as e:
        logger.error(
            "💥 Hello world generator exception",
            extra={
                "error": str(e),
                "error_type": type(e).__name__,
                "processor": "HelloWorldProcessor"
            },
            exc_info=True
        )