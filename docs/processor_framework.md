# Unified Processor Framework v2

The API Exchange Core provides a unified processor framework for building data integration pipelines. Version 2 introduces enhanced operation decorators, context-based processing, and improved output handlers for robust production deployments.

## Architecture Philosophy

### Unified Processor Interface v2

Instead of forcing processors into rigid categories, we provide a single `ProcessorInterface` that can handle any processing logic with enhanced context and operation tracking:

```python
class ProcessorInterface:
    def process(self, message: Message, context: ProcessorContext) -> ProcessingResult:
        """
        Process a message using framework context and return result with routing information.
        
        Can perform any combination of:
        - Create entities from external data (using context.persist_entity)
        - Update existing entities (using context.get_entity)
        - Transform data with operation tracking
        - Make routing decisions via output handlers
        - Terminate processing chains
        """
        pass
        
    def validate_message(self, message: Message) -> bool:
        """Validate incoming message before processing."""
        return True
        
    def get_processor_info(self) -> Dict[str, Any]:
        """Return processor metadata for monitoring and debugging."""
        return {"name": self.__class__.__name__, "version": "1.0.0"}
        
    def can_retry(self, error: Exception) -> bool:
        """Determine if an error is retryable."""
        return True
```

### Key Benefits v2

1. **Enhanced Context**: `ProcessorContext` provides direct access to framework services
2. **Operation Tracking**: `@operation` decorators provide automatic logging and metrics
3. **Output Handlers**: Type-safe, configurable routing to queues, files, APIs
4. **Error Classification**: Built-in retry logic based on error types
5. **Multi-tenant Ready**: Automatic tenant context management
6. **Testability**: NO MOCKS policy - use real implementations in tests

## Core Components v2

### 1. ProcessorInterface v2

The single interface all processors implement. Processors receive a `Message`, `ProcessorContext` and return a `ProcessingResult` with enhanced routing capabilities.

### 2. ProcessorContext

New in v2: Provides direct access to framework services without complex dependency injection:

```python
class ProcessorContext:
    processing_service: ProcessingService
    state_tracking_service: Optional[StateTrackingService]
    error_service: Optional[ProcessingErrorService]
    
    def persist_entity(self, external_id: str, canonical_type: str, 
                      source: str, data: Dict[str, Any], **kwargs) -> str:
        """Create entity directly using framework services."""
        
    def get_entity(self, entity_id: str) -> Entity:
        """Retrieve entity by ID."""
```

### 3. @operation Decorator

New in v2: Automatic operation tracking, logging, and metrics:

```python
from src.context.operation_context import operation

class MyProcessor(ProcessorInterface):
    @operation("my_processor.process")
    def process(self, message: Message, context: ProcessorContext) -> ProcessingResult:
        # Business logic here - logging/metrics handled automatically
        pass
    
    @operation("my_processor.validate")  
    def validate_message(self, message: Message) -> bool:
        # Validation logic with automatic tracking
        pass
```

Benefits:
- **Automatic Logging**: Operation start/end with duration
- **Metrics Collection**: Performance and success/failure rates
- **Error Tracking**: Automatic error categorization and reporting
- **Correlation IDs**: Request tracing across operations

### 4. Message v2

Enhanced message format for pipeline communication:
- **Entity Reference**: Links to entities in the system (can be None for new entities)
- **Payload**: The actual data being processed 
- **Metadata**: Processing context and operation routing information
- **Message ID**: Unique identifier for tracking
- **Message Type**: ENTITY, TIMER, QUEUE, HTTP for different triggers

### 5. ProcessingResult v2

Enhanced result format with output handlers:
- **Success/Failure**: Processing outcome with detailed error information
- **Output Handlers**: Type-safe routing to queues, files, APIs (replaces simple routing)
- **Entity Operations**: IDs of entities created/updated during processing
- **Processing Metadata**: Custom metrics and debugging information
- **Retry Logic**: Configurable retry behavior based on error types

```python
# v2 ProcessingResult with Output Handlers
result = ProcessingResult.create_success(
    entities_created=["entity-123"],
    processing_metadata={"orders_processed": 5}
)

# Add type-safe output handler
queue_handler = QueueOutputHandler(
    destination="next-queue",
    config={"retry_count": 3, "timeout_seconds": 30}
)
result.add_output_handler(queue_handler)
```

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

## How to Build and Use a Processor v2

### Step 1: Implement ProcessorInterface v2

Create your processor using the enhanced v2 interface with `@operation` decorators and `ProcessorContext`:

```python
from datetime import datetime, UTC
from typing import Dict, Any
from src.context.operation_context import operation
from src.processors.v2.processor_interface import ProcessorInterface, ProcessorContext
from src.processors.processing_result import ProcessingResult, ProcessingStatus
from src.processors.v2.message import Message
from src.processors.v2.output_handlers import QueueOutputHandler

class MyBusinessProcessor(ProcessorInterface):
    
    def __init__(self, config: MyProcessorConfig):
        """Initialize processor with customer-specific configuration."""
        self.config = config
        
    @operation("my_processor.process")
    def process(self, message: Message, context: ProcessorContext) -> ProcessingResult:
        """Process message with automatic operation tracking."""
        # Extract operation type from message metadata
        operation_type = message.metadata.get("operation", "default")
        
        if operation_type == "create_entity":
            return self._handle_entity_creation(message, context)
        elif operation_type == "transform_data":
            return self._handle_data_transformation(message, context)
        else:
            return ProcessingResult.create_failure(
                error_message=f"Unknown operation: {operation_type}",
                error_code="INVALID_OPERATION",
                can_retry=False
            )
    
    @operation("my_processor.create_entity")
    def _handle_entity_creation(self, message: Message, context: ProcessorContext) -> ProcessingResult:
        """Create new entity from external data."""
        try:
            # Use context to persist entity directly
            entity_id = context.persist_entity(
                external_id=message.payload["id"],
                canonical_type="my_business_object",
                source="external_api",
                data=message.payload,
                metadata={
                    "processed_at": datetime.now(UTC).isoformat(),
                    "processor_version": "2.0.0"
                }
            )
            
            # Create success result with output handler
            result = ProcessingResult.create_success(
                entities_created=[entity_id],
                processing_metadata={
                    "operation": "create_entity",
                    "external_id": message.payload["id"]
                }
            )
            
            # Add type-safe output handler for next stage
            queue_handler = QueueOutputHandler(
                destination=self.config.next_stage_queue,
                config={"retry_count": 3}
            )
            result.add_output_handler(queue_handler)
            
            return result
            
        except Exception as e:
            return ProcessingResult.create_failure(
                error_message=f"Entity creation failed: {str(e)}",
                error_code="ENTITY_CREATION_ERROR", 
                can_retry=self.can_retry(e)
            )
    
    @operation("my_processor.validate")
    def validate_message(self, message: Message) -> bool:
        """Validate message with automatic tracking."""
        if not message or not message.payload:
            return False
        
        operation = message.metadata.get("operation")
        if operation == "create_entity":
            return "id" in message.payload
        elif operation == "transform_data":
            return "data" in message.payload
        
        return True
    
    def can_retry(self, error: Exception) -> bool:
        """Determine retry behavior based on error type."""
        # Network errors are retryable
        if isinstance(error, (ConnectionError, TimeoutError)):
            return True
        # Validation errors are not retryable  
        if isinstance(error, ValueError):
            return False
        # Default to retryable
        return True
        
    def get_processor_info(self) -> Dict[str, Any]:
        """Return processor metadata for monitoring."""
        return {
            "name": "MyBusinessProcessor",
            "version": "2.0.0",
            "operations": ["create_entity", "transform_data"],
            "config": self.config.to_dict()
        }
```

### Step 2: Create Azure Function with ProcessorHandler v2

Use the framework's v2 `ProcessorHandler` with simplified setup:

```python
import os
import azure.functions as func
from src.processors.v2.processor_factory import create_processor_handler
from src.context.tenant_context import tenant_context
from src.db.db_config import import_all_models
from processors.my_business_processor import MyBusinessProcessor
from processors.my_processor_config import MyProcessorConfig

# Create the Azure Functions app
app = func.FunctionApp()

# Initialize models
import_all_models()

# Create processor configuration from environment or hardcoded values
processor_config = MyProcessorConfig(
    tenant_id=os.getenv("TENANT_ID", "default-tenant"),
    next_stage_queue=os.getenv("NEXT_STAGE_QUEUE", "next-queue"),
    enable_logging=True
)

# Create processor with configuration
my_processor = MyBusinessProcessor(config=processor_config)

# Create processor handler using factory - handles ALL infrastructure concerns
my_processor_handler = create_processor_handler(processor=my_processor)

# Azure Function entry point - ultra-thin wrapper
@app.function_name(name="ProcessMyBusinessData")
@app.queue_trigger(arg_name="msg", queue_name="input-queue")
def process_business_data(msg: func.QueueMessage) -> None:
    """
    Ultra-thin Azure Function wrapper.
    
    All error handling, retry logic, logging, metrics, and output routing
    is handled automatically by the processor framework.
    """
    # Convert Azure Functions message to framework Message
    message_data = msg.get_json()
    
    # Create Message object (framework v2 pattern)
    from src.processors.v2.message import Message
    message = Message.create_from_queue_data(message_data)
    
    # Execute with framework - handles everything automatically
    with tenant_context(processor_config.tenant_id):
        my_processor_handler.execute(message)

# Timer-triggered function example
@app.function_name(name="ProcessTimerTrigger")
@app.timer_trigger(schedule="0 */5 * * * *", arg_name="timer")
def process_timer_trigger(timer: func.TimerRequest) -> None:
    """Timer trigger example with v2 framework."""
    # Create timer message using processor helper methods
    message = my_processor.create_timer_message(timer)
    
    with tenant_context(processor_config.tenant_id):
        my_processor_handler.execute(message)

# HTTP-triggered function example
@app.function_name(name="ProcessHttpRequest")
@app.route(route="process", methods=["POST"])
def process_http_request(req: func.HttpRequest) -> func.HttpResponse:
    """HTTP trigger example with v2 framework."""
    try:
        # Create message from HTTP request
        message = my_processor.create_http_message(req)
        
        with tenant_context(processor_config.tenant_id):
            result = my_processor_handler.execute(message)
            
        # Return HTTP response based on processing result
        if result.success:
            return func.HttpResponse(
                body='{"status": "success"}',
                status_code=200,
                headers={"Content-Type": "application/json"}
            )
        else:
            return func.HttpResponse(
                body=f'{{"status": "error", "message": "{result.error_message}"}}',
                status_code=400 if result.can_retry else 500,
                headers={"Content-Type": "application/json"}
            )
            
    except Exception as e:
        return func.HttpResponse(
            body=f'{{"status": "error", "message": "Unexpected error: {str(e)}"}}',
            status_code=500,
            headers={"Content-Type": "application/json"}
        )
```

### Step 3: What the Framework v2 Provides Automatically

When you use `ProcessorHandler` v2, you get:

1. **Error Handling** - Automatic error classification and routing to dead letter queues
2. **Retry Logic** - Exponential backoff with configurable retry behavior
3. **Operation Tracking** - Automatic logging and metrics via `@operation` decorators
4. **Tenant Context** - Multi-tenant isolation with automatic context management
5. **Message Validation** - Pre-processing validation via `validate_message()`
6. **Output Routing** - Type-safe output handlers for queues, files, APIs
7. **Entity Management** - Direct entity operations via `ProcessorContext`
8. **State Tracking** - Automatic state transitions and audit trails

### Step 4: Output Handlers v2

The v2 framework introduces type-safe output handlers for robust routing:

```python
from src.processors.v2.output_handlers import (
    QueueOutputHandler, 
    FileOutputHandler,
    ServiceBusOutputHandler,
    NoOpOutputHandler
)

# Queue output with retry configuration
queue_handler = QueueOutputHandler(
    destination="next-stage-queue",
    config={
        "retry_count": 3,
        "timeout_seconds": 30,
        "exponential_backoff": True
    }
)

# File output for archival
file_handler = FileOutputHandler(
    destination="/data/processed",
    config={
        "file_pattern": "order_{timestamp}.json",
        "compress": True
    }
)

# Service Bus for critical messages
service_bus_handler = ServiceBusOutputHandler(
    destination="critical-notifications",
    config={
        "session_id": "order_processing",
        "time_to_live": 3600
    }
)

# Add handlers to result
result.add_output_handler(queue_handler)
result.add_output_handler(file_handler)
```

### Step 5: Testing with NO MOCKS Policy

The v2 framework follows a strict NO MOCKS policy for better test reliability:

```python
import pytest
from src.context.tenant_context import tenant_context
from src.processors.v2.message import Message
from processors.my_business_processor import MyBusinessProcessor

class TestMyBusinessProcessor:
    """Test processor using real implementations."""
    
    def test_entity_creation_success(self, processor, processor_context, test_tenant):
        """Test entity creation with real framework services."""
        # Use real tenant context
        with tenant_context(test_tenant["id"]):
            # Create real message
            entity_id = processor_context.persist_entity(
                external_id="test_entity",
                canonical_type="test_type",
                source="test",
                data={"test": "data"}
            )
            entity = processor_context.get_entity(entity_id)
            
            message = Message.create_entity_message(
                entity=entity,
                payload={"id": "test-123", "data": "test_data"}
            )
            message.metadata["operation"] = "create_entity"
            
            # Execute with real processor context
            result = processor.process(message, processor_context)
            
            # Verify with real results
            assert result.success is True
            assert len(result.entities_created) == 1
            assert len(result.output_handlers) == 1
    
    def test_validation_logic(self, processor):
        """Test validation using real message objects."""
        # Real message with valid data
        valid_message = Message.create_entity_message(
            entity=test_entity,
            payload={"id": "test-123"}
        )
        valid_message.metadata["operation"] = "create_entity"
        
        # Real message with invalid data
        invalid_message = Message.create_entity_message(
            entity=test_entity,
            payload={}  # Missing required 'id'
        )
        invalid_message.metadata["operation"] = "create_entity"
        
        # Test with real validation logic
        assert processor.validate_message(valid_message) is True
        assert processor.validate_message(invalid_message) is False

# Fixtures use real framework services
@pytest.fixture
def processor(test_config):
    """Create real processor instance."""
    return MyBusinessProcessor(config=test_config)

@pytest.fixture  
def processor_context(processing_service):
    """Create real processor context."""
    from src.processors.v2.processor_interface import ProcessorContext
    return ProcessorContext(processing_service=processing_service)
```

**Key Testing Principles:**
- ✅ **NO MOCKS** - Use real framework services and database
- ✅ **Real Messages** - Create actual `Message` objects  
- ✅ **Real Context** - Use `ProcessorContext` with real services
- ✅ **Real Validation** - Test actual processor logic
- ❌ **Exception**: Only mock Azure SDK timeout/connection scenarios

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

## Migration from v1 to v2 Framework

The v2 framework significantly enhances the original unified approach:

| v1 Pattern | v2 Pattern | Benefits |
|------------|------------|-----------|
| `process(message)` | `process(message, context)` | Direct access to framework services |
| Manual dependency injection | `ProcessorContext` | Simplified service access |
| Simple error handling | `@operation` decorators | Automatic logging and metrics |
| Basic routing | Output handlers | Type-safe, configurable routing |
| Manual entity operations | `context.persist_entity()` | Framework-managed entity lifecycle |
| Mock-heavy testing | NO MOCKS policy | More reliable, realistic tests |

### Benefits of v2 Migration

1. **Enhanced Context**: Direct access to framework services via `ProcessorContext`
2. **Operation Tracking**: Automatic logging, metrics, and performance monitoring
3. **Type-Safe Routing**: Output handlers replace basic routing dictionaries
4. **Multi-tenant Ready**: Built-in tenant context management
5. **Reliable Testing**: NO MOCKS policy ensures tests match production behavior
6. **Simplified Deployment**: Ultra-thin Azure Functions with framework handling infrastructure

### Real-World Example: Temple Webster Integration

The Temple Webster processor demonstrates v2 best practices:

```python
class TempleWebsterProcessor(ProcessorInterface):
    def __init__(self, config: TWProcessorConfig):
        self.config = config  # Customer-specific configuration
        
    @operation("temple_webster.process")
    def process(self, message: Message, context: ProcessorContext) -> ProcessingResult:
        operation_type = message.metadata.get("operation", "get_order_details")
        
        if operation_type == "list_orders":
            return self._handle_list_orders(message, context)
        elif operation_type == "get_order_details":
            return self._handle_get_order_details(message, context)
            
    @operation("temple_webster.list_orders")
    def _handle_list_orders(self, message: Message, context: ProcessorContext) -> ProcessingResult:
        # Get TW client using framework context
        tw_client = self._get_tw_client(context)
        
        # Fetch orders from Temple Webster API
        response = tw_client.list_purchase_orders(...)
        
        # Create entities for each order discovered
        result = ProcessingResult.create_success()
        for purchase_order in response.data.purchase_order:
            entity_id = context.persist_entity(
                external_id=f"tw_order_discovery_{purchase_order}",
                canonical_type="temple_webster_order_discovery",
                source="temple_webster_list_orders",
                data={"purchase_order": purchase_order}
            )
            result.add_entity_created(entity_id)
        
        # Add output handler for fan-out processing
        queue_handler = QueueOutputHandler(
            destination=self.config.order_details_queue,
            config=self.config.get_queue_config()
        )
        result.add_output_handler(queue_handler)
        
        return result
```

### Multi-Customer Deployment Pattern

v2 enables complete customer isolation:

```
deployments/
├── customer-a/     # Production customer
│   └── azure/
│       ├── function_app.py    # Customer A config
│       └── host.json          # High-volume settings
└── customer-b/     # Testing customer  
    └── azure/
        ├── function_app.py    # Customer B config
        └── host.json          # Conservative settings
```

Each customer gets:
- ✅ **Isolated Azure Functions app**
- ✅ **Customer-specific configuration**
- ✅ **Independent scaling and monitoring**
- ✅ **Separate databases and queues**
- ✅ **Shared processor framework code**

## Next Steps

1. Implement specific processors using `ProcessorInterface`
2. Configure pipeline routing between processors
3. Set up trigger mechanisms (timers, queues, webhooks)
4. Deploy with appropriate error handling and monitoring

The unified processor framework provides the foundation for building flexible, maintainable data integration pipelines while leveraging the full power of the API Exchange Core services.