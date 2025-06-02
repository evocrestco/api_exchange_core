# Unified Processor Framework

The API Exchange Core provides a unified processor framework for building data integration pipelines. This framework eliminates the artificial distinctions between "source", "intermediate", and "terminal" processors in favor of a flexible, unified approach.

## Architecture Philosophy

### Unified Processor Interface

Instead of forcing processors into rigid categories, we provide a single `ProcessorInterface` that can handle any processing logic:

```python
class ProcessorInterface:
    def process(self, message: Message) -> ProcessingResult:
        """
        Process a message and return result with routing information.
        
        Can perform any combination of:
        - Create entities from external data
        - Update existing entities
        - Transform data
        - Make routing decisions
        - Terminate processing chains
        """
        pass
```

### Key Benefits

1. **Flexibility**: Processors can create entities, update them, or just transform data as needed
2. **Simplicity**: No artificial constraints or complex inheritance hierarchies
3. **Composability**: Easy to chain processors in any configuration
4. **Testability**: Simple interface makes testing straightforward

## Core Components

### 1. ProcessorInterface

The single interface all processors implement. Processors receive a `Message` and return a `ProcessingResult`.

### 2. Message

Standardized message format for pipeline communication:
- **Entity Reference**: Links to entities in the system
- **Payload**: The actual data being processed
- **Metadata**: Processing context and routing information
- **Correlation ID**: For tracking across pipeline stages

### 3. ProcessingResult

Standardized result format indicating:
- **Success/Failure**: Processing outcome
- **Output Messages**: Messages to route to next stages
- **Routing Information**: Queue destinations and conditions
- **Entity Operations**: Whether entities were created/updated

### 4. ProcessorHandler

The unified handler that bridges Azure Functions (or other triggers) and the processor framework:
- **Message Conversion**: Converts between dict and Message formats
- **Processor Execution**: Executes processors with comprehensive error handling and retry logic
- **Error Handling**: Catches and classifies errors (validation, service, unexpected)
- **Retry Logic**: Exponential backoff for recoverable errors
- **Performance Monitoring**: Tracks execution duration and metrics
- **Tenant Context Management**: Ensures proper multi-tenant isolation
- **Message Validation**: Validates messages before processing
- **Entity Persistence**: Automatically persists entities for processors with mapper interface
- **State Tracking**: Records state transitions for audit trails
- **Result Transformation**: Converts ProcessingResult to framework-compatible format
- **Dependency Management**: Manages processor lifecycle and dependencies

### 5. ProcessorFactory

Creates processors with proper dependency injection:
- **Automatic Dependency Injection**: Injects EntityService, ProcessingService, etc.
- **Configuration Management**: Applies ProcessorConfig to processors
- **Logging Setup**: Ensures proper logging context

### 7. Processing Services Integration

Processors can use Core services for entity operations:
- **ProcessingService**: Entity creation, versioning, duplicate detection
- **EntityService**: Direct entity CRUD operations
- **StateTrackingService**: State transitions and monitoring

## How to Build and Use a Processor

### Step 1: Implement ProcessorInterface

Create your processor by implementing the `ProcessorInterface`:

```python
from src.processors.processor_interface import ProcessorInterface
from src.processors.processing_result import ProcessingResult, ProcessingStatus
from src.processors.message import Message

class MyBusinessProcessor(ProcessorInterface):
    def process(self, message: Message) -> ProcessingResult:
        # Your business logic here
        transformed_data = self.transform(message.payload)
        
        # Return result with routing
        return ProcessingResult(
            status=ProcessingStatus.SUCCESS,
            success=True,
            output_messages=[
                Message(
                    payload=transformed_data,
                    entity_reference=message.entity_reference,
                    metadata={"processed_by": "my_processor"}
                )
            ],
            routing_info={"destination": "next-queue"}
        )
    
    def validate_message(self, message: Message) -> bool:
        # Optional: Validate message before processing
        return message.entity_reference is not None
    
    def get_processor_info(self) -> dict:
        # Optional: Return processor metadata
        return {
            "name": "MyBusinessProcessor",
            "version": "1.0.0",
            "capabilities": ["transform", "validate"]
        }
```

### Step 2: Create Azure Function with ProcessorHandler

Use the framework's `ProcessorHandler` to integrate with Azure Functions:

```python
import azure.functions as func
from src.processors.processor_factory import create_processor_handler
from src.processing.processor_config import ProcessorConfig
from src.repositories.entity_repository import EntityRepository
from src.services.entity_service import EntityService
from src.processing.processing_service import ProcessingService
from src.db.db_config import get_db_manager
from processors.my_business_processor import MyBusinessProcessor

# Initialize dependencies (typically done once per function app)
db_manager = get_db_manager()
entity_repository = EntityRepository(db_manager)
entity_service = EntityService(entity_repository)
# ... initialize other services

processing_service = ProcessingService(
    entity_service=entity_service,
    entity_repository=entity_repository,
    # ... other dependencies
)

# Create processor configuration
config = ProcessorConfig(
    processor_name="my_business_processor",
    processor_version="1.0.0",
    enable_state_tracking=True,
    is_source_processor=False
)

# Create the handler
handler = create_processor_handler(
    processor_class=MyBusinessProcessor,
    config=config,
    entity_service=entity_service,
    entity_repository=entity_repository,
    processing_service=processing_service
)

# Azure Function entry point
@app.queue_trigger(arg_name="msg", queue_name="input-queue")
@app.queue_output(arg_name="output", queue_name="output-queue")
def process_message(msg: func.QueueMessage, output: func.Out[str]) -> None:
    # Parse queue message
    import json
    message_data = json.loads(msg.get_body().decode('utf-8'))
    
    # Process with handler
    result = handler.handle_message(message_data)
    
    # Route output messages
    if result["success"] and result["output_messages"]:
        for msg in result["output_messages"]:
            output.set(json.dumps(msg))
```

### Step 3: What the Framework Provides Automatically

When you use `ProcessorHandler`, you get:

1. **Error Handling** - ProcessorHandler catches all errors and classifies them
2. **Retry Logic** - Automatic exponential backoff for recoverable errors
3. **Performance Tracking** - Execution duration and metrics
4. **Tenant Context** - Multi-tenant isolation handled automatically
5. **Message Validation** - Pre-processing validation
6. **Logging** - Structured logging with correlation IDs

### Step 4: Source Processors (Creating Entities)

For processors that create entities, there are two approaches:

#### Approach 1: Direct ProcessingService Usage (Recommended for batch operations)

```python
class DataIngestionProcessor(ProcessorInterface):
    def __init__(self, processing_service: ProcessingService, **kwargs):
        self.processing_service = processing_service
        
    def process(self, message: Message) -> ProcessingResult:
        # Fetch external data
        external_data = self.fetch_from_api()
        
        # Create entities using ProcessingService
        entities_created = []
        for item in external_data:
            result = self.processing_service.process_entity(
                external_id=item["id"],
                canonical_type="order",
                source="external_api",
                content=item,
                config=ProcessorConfig(
                    enable_duplicate_detection=True,
                    enable_state_tracking=True
                )
            )
            entities_created.append(result.entity_id)
        
        return ProcessingResult(
            success=True,
            entities_created=entities_created,
            output_messages=[...],
            processing_metadata={"items_processed": len(external_data)}
        )
```

#### Approach 2: Simple Processor with Framework Handler

For simple cases, just return the data and let the framework handle entity creation:

```python
class SimpleIngestionProcessor(ProcessorInterface):
    def process(self, message: Message) -> ProcessingResult:
        # Just transform the data
        external_data = self.fetch_from_api()
        
        # Return messages for each item
        output_messages = []
        for item in external_data:
            output_messages.append(
                Message.create_entity_message(
                    external_id=item["id"],
                    canonical_type="order",
                    source="external_api",
                    tenant_id="default",
                    payload=item
                )
            )
        
        return ProcessingResult(
            success=True,
            output_messages=output_messages,
            processing_metadata={"items_fetched": len(external_data)}
        )

# In Azure Function setup:
config = ProcessorConfig(
    processor_name="simple_ingestion",
    is_source_processor=True,  # This tells handler to create entities!
    enable_state_tracking=True,
    enable_duplicate_detection=True
)
```

## Common Processor Patterns

### Business Logic Processor

Applies business rules and transformations:

```python
class ValidationProcessor(ProcessorInterface):
    def process(self, message: Message) -> ProcessingResult:
        # Apply business validation
        is_valid = self.validate_business_rules(message.payload)
        
        if is_valid:
            # Route to success queue
            return ProcessingResult(
                success=True,
                output_messages=[message],
                routing_info={"destination": "validated_queue"}
            )
        else:
            # Route to error handling
            return ProcessingResult(
                success=False,
                error_message="Validation failed",
                routing_info={"destination": "error_queue"}
            )
```

### Output Processor

Sends data to final destinations:

```python
class FileOutputProcessor(ProcessorInterface):
    def process(self, message: Message) -> ProcessingResult:
        # Write to file system
        self.write_to_file(message.payload)
        
        # Mark as complete (no further routing)
        return ProcessingResult(
            success=True,
            output_messages=[],  # Terminal - no further routing
            metadata={"file_written": True}
        )
```

## Entity Operations in Processors

### When to Create Entities

Processors should create entities when:
- Ingesting data from external sources
- Processing raw data that needs to enter the system
- Creating derived entities from existing data

### Using ProcessingService

The `ProcessingService` handles entity operations with:
- **Duplicate detection** based on content hashing
- **Automatic versioning** for processing attempts
- **Attribute standardization** for consistent metadata
- **Configuration-driven behavior** per processor type

```python
# In processor
result = self.processing_service.process_entity(
    external_id="ORDER-123",
    canonical_type="order", 
    source="shopify",
    content=order_data,
    config=self.processor_config,
    custom_attributes={"priority": "high"}
)
```

### Entity Updates vs New Versions

- **New Versions**: Created for each processing attempt (tracks processing history)
- **Attribute Updates**: For metadata changes without content changes
- **Content Changes**: Automatically detected via content hashing

## Pipeline Configuration

### Message Routing

Processors indicate routing through `ProcessingResult`:

```python
# Route to multiple destinations
return ProcessingResult(
    success=True,
    output_messages=[
        Message(payload=transformed_data, routing={"queue": "high_priority"}),
        Message(payload=audit_data, routing={"queue": "audit_log"})
    ]
)

# Conditional routing
destination = "approved_queue" if is_approved else "review_queue"
return ProcessingResult(
    success=True,
    output_messages=[message],
    routing_info={"destination": destination}
)
```

### Error Handling

Processors can handle errors gracefully:

```python
try:
    result = self.process_data(message.payload)
    return ProcessingResult(success=True, output_messages=[result])
except ProcessingError as e:
    return ProcessingResult(
        success=False,
        error_message=str(e),
        routing_info={"destination": "error_queue"},
        retry_after_seconds=300
    )
```

## Framework vs Business Logic

### What's in the Framework

- `ProcessorInterface` definition
- `Message` and `ProcessingResult` classes
- `ProcessorHandler` for Azure Function integration with built-in error handling and retry logic
- `ProcessorFactory` for dependency injection
- `ProcessingService` for entity operations
- State tracking and error recording (automatic)
- Configuration management

### What's in Your Implementation

- Specific business logic (implement `process()` method)
- External system integrations (APIs, databases)
- Data transformation rules
- Routing decisions
- Custom validation logic

### Complete Example: Coffee Order Processor

Here's a complete example showing all the pieces together:

```python
# processors/coffee_processor.py
from src.processors.processor_interface import ProcessorInterface
from src.processors.processing_result import ProcessingResult, ProcessingStatus
from src.processors.message import Message

class CoffeeOrderProcessor(ProcessorInterface):
    """Simple processor - just implement business logic!"""
    
    def process(self, message: Message) -> ProcessingResult:
        # Extract coffee order
        order = message.payload
        
        # Business logic: validate coffee order
        if not order.get("drink_type"):
            return ProcessingResult(
                status=ProcessingStatus.FAILED,
                success=False,
                error_message="Missing drink type"
            )
        
        # Transform: add processing info
        processed_order = {
            **order,
            "processed_at": datetime.now().isoformat(),
            "processor": "coffee_order_processor"
        }
        
        # Return success with routing
        return ProcessingResult(
            status=ProcessingStatus.SUCCESS,
            success=True,
            output_messages=[
                Message(
                    payload=processed_order,
                    entity_reference=message.entity_reference,
                    metadata={"stage": "processed"}
                )
            ],
            routing_info={"destination": "barista-queue"}
        )

# function_app.py - Azure Function setup
import azure.functions as func
from src.processors.processor_factory import create_processor_handler
from src.processing.processor_config import ProcessorConfig
# ... other imports

# Initialize framework (once per app)
db_manager = get_db_manager()
entity_repository = EntityRepository(db_manager)
entity_service = EntityService(entity_repository)
processing_service = ProcessingService(...)

# Configure processor
config = ProcessorConfig(
    processor_name="coffee_order_processor",
    enable_state_tracking=True,  # Automatic state tracking!
    is_source_processor=False
)

# Create handler - this connects everything!
handler = create_processor_handler(
    processor_class=CoffeeOrderProcessor,
    config=config,
    entity_service=entity_service,
    entity_repository=entity_repository,
    processing_service=processing_service
)

# Azure Function - just pass to handler!
@app.queue_trigger(arg_name="msg", queue_name="coffee-orders")
@app.queue_output(arg_name="output", queue_name="barista-queue")
def process_coffee_order(msg: func.QueueMessage, output: func.Out[str]):
    message_data = json.loads(msg.get_body().decode('utf-8'))
    result = handler.handle_message(message_data)
    
    if result["success"]:
        for msg in result["output_messages"]:
            output.set(json.dumps(msg))
```

**That's it!** The framework handles:
- ✅ Error catching and classification
- ✅ Retry logic with exponential backoff
- ✅ State tracking (automatic with `enable_state_tracking=True`)
- ✅ Performance monitoring
- ✅ Tenant context
- ✅ Message validation
- ✅ Structured logging

## Migration from Old Framework

The new unified approach replaces the old Source/Intermediate/Terminal pattern:

| Old Pattern | New Pattern |
|-------------|-------------|
| `SourceProcessor` | Any processor that creates entities |
| `IntermediateProcessor` | Any processor that transforms/routes |
| `TerminalProcessor` | Any processor that doesn't route further |

### Benefits of Migration

1. **Reduced Complexity**: No more artificial processor categories
2. **Increased Flexibility**: Processors can do whatever they need
3. **Better Testability**: Simpler interfaces and dependency injection
4. **Core Integration**: Built-in entity management and duplicate detection
5. **Configuration-Driven**: Behavior controlled by `ProcessorConfig`

## Next Steps

1. Implement specific processors using `ProcessorInterface`
2. Configure pipeline routing between processors
3. Set up trigger mechanisms (timers, queues, webhooks)
4. Deploy with appropriate error handling and monitoring

The unified processor framework provides the foundation for building flexible, maintainable data integration pipelines while leveraging the full power of the API Exchange Core services.