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

### 4. Processing Services Integration

Processors can use Core services for entity operations:
- **ProcessingService**: Entity creation, versioning, duplicate detection
- **EntityService**: Direct entity CRUD operations
- **StateTrackingService**: State transitions and monitoring

## Common Processor Patterns

### Data Ingestion Processor

Brings external data into the system:

```python
class DataIngestionProcessor(ProcessorInterface):
    def __init__(self, processing_service: ProcessingService, config: ProcessorConfig):
        self.processing_service = processing_service
        self.config = config
    
    def process(self, message: Message) -> ProcessingResult:
        # Fetch data from external source
        external_data = self.fetch_from_source()
        
        # Create/version entities using ProcessingService
        for item in external_data:
            result = self.processing_service.process_entity(
                external_id=item.id,
                canonical_type="order",
                source="shopify",
                content=item.data,
                config=self.config
            )
            
        # Route to next stage
        return ProcessingResult(
            success=True,
            output_messages=[create_message(result)],
            routing_info={"destination": "processing_queue"}
        )
```

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
- Integration with Core services
- Handler and execution patterns
- Configuration management

### What's in Your Implementation

- Specific business logic
- External system integrations (APIs, databases)
- Data transformation rules
- Routing decisions
- Custom validation logic

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