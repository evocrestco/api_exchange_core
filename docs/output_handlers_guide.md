# Output Handlers Usage Guide

## Overview

Output handlers are a core component of the API Exchange Core framework that enable flexible message routing and processing result delivery. They provide a pluggable architecture for sending processed data to various destinations including message queues, file systems, and enterprise messaging systems.

## Architecture and Design Principles

### Key Concepts

1. **Separation of Concerns**: Processing logic is separated from output routing
2. **Pluggable Architecture**: New output handlers can be added without modifying core processor code
3. **Configuration-Driven**: Output destinations and behavior can be configured externally
4. **Error Resilience**: Built-in error handling and retry capabilities
5. **Performance Tracking**: Execution timing and success metrics

### Output Handler Lifecycle

```
ProcessingResult → OutputHandler.handle() → Destination
                         ↓
                   Validation
                         ↓
                   Execution
                         ↓
                OutputHandlerResult
```

## Available Output Handlers

### QueueOutputHandler

Sends messages to Azure Storage Queues for reliable, scalable message delivery.

**Use Cases:**
- Decoupled microservice communication
- Work queue patterns
- Fan-out processing scenarios

**Configuration:**
```python
from src.processors.v2.output_handlers import QueueOutputHandler

handler = QueueOutputHandler(
    destination="processing-queue",
    config={
        "connection_string": "UseDevelopmentStorage=true",  # or Azure connection string
        "auto_create_queue": True,
        "message_ttl_seconds": 604800,  # 7 days
        "visibility_timeout_seconds": 30
    }
)
```

**Environment Configuration:**
```bash
# Using environment variables
export AZURE_STORAGE_CONNECTION_STRING="UseDevelopmentStorage=true"

# Handler will auto-detect connection string
handler = QueueOutputHandler(destination="my-queue")
```

### ServiceBusOutputHandler

Integrates with Azure Service Bus for enterprise messaging scenarios.

**Use Cases:**
- Pub/sub messaging patterns
- Session-based message processing
- Dead letter queue management
- Message scheduling

**Configuration:**
```python
from src.processors.v2.output_handlers import ServiceBusOutputHandler

handler = ServiceBusOutputHandler(
    destination="orders-topic",
    config={
        "connection_string": "Endpoint=sb://...",
        "destination_type": "topic",  # or "queue"
        "session_id": "user-123",
        "time_to_live_seconds": 3600,
        "scheduled_enqueue_time": "2024-01-01T12:00:00Z",
        "message_properties": {
            "priority": "high",
            "region": "us-west"
        }
    }
)
```

**Advanced Features:**
```python
# Session-aware messaging
handler = ServiceBusOutputHandler(
    destination="user-sessions",
    config={
        "session_id": message.payload.get("user_id"),
        "message_properties": {
            "session_sequence": 1
        }
    }
)

# Scheduled delivery
from datetime import datetime, timedelta
scheduled_time = (datetime.utcnow() + timedelta(hours=1)).isoformat() + "Z"
handler = ServiceBusOutputHandler(
    destination="scheduled-tasks",
    config={
        "scheduled_enqueue_time": scheduled_time
    }
)
```

### FileOutputHandler

Writes processing results to the local file system for archival, debugging, or integration with file-based systems.

**Use Cases:**
- Data archival
- Audit logging
- Debug output
- File-based integrations

**Configuration:**
```python
from src.processors.v2.output_handlers import FileOutputHandler

handler = FileOutputHandler(
    destination="/data/processed",
    config={
        "output_format": "json",  # json, jsonl, text
        "file_pattern": "{date}/{canonical_type}/{external_id}.json",
        "append_mode": False,
        "create_directories": True,
        "pretty_print": True,
        "encoding": "utf-8"
    }
)
```

**File Pattern Variables:**
- `{message_id}`: Original message ID
- `{correlation_id}`: Message correlation ID
- `{timestamp}`: Current timestamp (ISO format)
- `{date}`: Current date (YYYY-MM-DD)
- `{time}`: Current time (HH-MM-SS)
- `{external_id}`: Entity external ID
- `{canonical_type}`: Entity type
- `{tenant_id}`: Tenant identifier

**Output Formats:**

1. **JSON Format** (single file per message):
```json
{
  "message_id": "msg-123",
  "entity": {
    "external_id": "order-456",
    "canonical_type": "order"
  },
  "processing_result": {
    "status": "success",
    "timestamp": "2024-01-01T10:00:00Z"
  }
}
```

2. **JSONL Format** (append mode):
```
{"message_id":"msg-123","entity":{...},"timestamp":"2024-01-01T10:00:00Z"}
{"message_id":"msg-124","entity":{...},"timestamp":"2024-01-01T10:01:00Z"}
```

3. **Text Format** (human-readable):
```
Message ID: msg-123
Correlation ID: corr-456
Entity: order-789 (order)
Processor: OrderProcessor
Status: success
Timestamp: 2024-01-01T10:00:00Z

Payload:
{
  "order_id": "order-789",
  "amount": 99.99
}
```

### NoOpOutputHandler

A no-operation handler for testing, conditional routing, or when no output is needed.

**Use Cases:**
- Unit testing
- Conditional output suppression
- Pipeline termination
- Performance benchmarking

**Configuration:**
```python
from src.processors.v2.output_handlers import NoOpOutputHandler

handler = NoOpOutputHandler(
    destination="test-complete",
    config={
        "reason": "End of processing pipeline",
        "log_output": True,
        "metadata": {
            "test_id": "test-123",
            "completed": True
        }
    }
)
```

## Configuration Management

### Using OutputHandlerConfig

The framework provides a centralized configuration system for managing output handlers:

```python
from src.processors.v2.output_handlers.config import (
    QueueOutputHandlerConfig,
    OutputHandlerConfigFactory,
    OutputHandlerConfigManager
)

# Create configuration object
config = QueueOutputHandlerConfig(
    destination="my-queue",
    connection_string="UseDevelopmentStorage=true",
    auto_create_queue=True,
    message_ttl_seconds=3600
)

# Create handler from config
handler = QueueOutputHandler("my-queue", config)
```

### Environment Variable Configuration

Load configurations from environment variables with prefixes:

```python
# Set environment variables
export MYAPP_QUEUE_CONNECTION_STRING="UseDevelopmentStorage=true"
export MYAPP_QUEUE_AUTO_CREATE_QUEUE="true"
export MYAPP_QUEUE_MESSAGE_TTL_SECONDS="3600"

# Load configuration
config = OutputHandlerConfigFactory.from_env_prefix(
    "queue",
    "output-queue", 
    "MYAPP_QUEUE_"
)
```

### Configuration Manager

Manage multiple output handler configurations:

```python
manager = OutputHandlerConfigManager()

# Load from dictionary
configs = {
    "primary_queue": {
        "handler_type": "queue",
        "destination": "main-queue",
        "connection_string": "..."
    },
    "backup_file": {
        "handler_type": "file",
        "destination": "/backup",
        "output_format": "jsonl"
    }
}
manager.load_from_dict(configs)

# Or load from JSON file
manager.load_from_json_file("output_handlers.json")

# Use in processor
def process(self, message, context):
    result = ProcessingResult.create_success()
    
    # Add configured handlers
    for name in ["primary_queue", "backup_file"]:
        config = manager.get_config(name)
        if config:
            handler = create_handler_from_config(config)
            result.add_output_handler(handler)
    
    return result
```

## Best Practices and Patterns

### 1. Conditional Routing

Route messages based on content or processing results:

```python
def process(self, message: Message, context: ProcessorContext) -> ProcessingResult:
    order = message.payload
    result = ProcessingResult.create_success()
    
    # Route based on order value
    if order["total"] > 1000:
        result.add_output_handler(QueueOutputHandler(
            destination="high-value-orders",
            config={"priority": "high"}
        ))
    else:
        result.add_output_handler(QueueOutputHandler(
            destination="standard-orders"
        ))
    
    # Always archive
    result.add_output_handler(FileOutputHandler(
        destination="/archive/orders",
        config={"output_format": "jsonl", "append_mode": True}
    ))
    
    return result
```

### 2. Fan-Out Pattern

Send to multiple destinations for parallel processing:

```python
def process(self, message: Message, context: ProcessorContext) -> ProcessingResult:
    result = ProcessingResult.create_success()
    
    # Fan out to multiple queues
    for region in ["us-west", "us-east", "eu-west"]:
        result.add_output_handler(QueueOutputHandler(
            destination=f"orders-{region}",
            config={"metadata": {"region": region}}
        ))
    
    return result
```

### 3. Error Handling Pattern

Handle output failures gracefully:

```python
def process(self, message: Message, context: ProcessorContext) -> ProcessingResult:
    result = ProcessingResult.create_success()
    
    # Primary output
    result.add_output_handler(ServiceBusOutputHandler(
        destination="primary-topic"
    ))
    
    # Fallback output for resilience
    result.add_output_handler(QueueOutputHandler(
        destination="fallback-queue",
        config={"metadata": {"fallback": True}}
    ))
    
    # Check results after processing
    if result.processing_metadata.get("output_handler_results"):
        failures = [r for r in result.processing_metadata["output_handler_results"] 
                   if not r["success"]]
        if failures:
            # Handle failures appropriately
            context.log_error(f"Output failures: {failures}", "OUTPUT_FAILURE")
    
    return result
```

### 4. Dynamic Configuration

Load output configurations dynamically:

```python
class DynamicProcessor(ProcessorInterface):
    def __init__(self):
        self.config_manager = OutputHandlerConfigManager()
        self.reload_config()
    
    def reload_config(self):
        # Load from external source (file, database, API)
        self.config_manager.load_from_json_file("current_routes.json")
    
    def process(self, message: Message, context: ProcessorContext) -> ProcessingResult:
        result = ProcessingResult.create_success()
        
        # Get routing rules based on message type
        message_type = message.payload.get("type", "default")
        route_configs = self.get_routes_for_type(message_type)
        
        for config_name in route_configs:
            config = self.config_manager.get_config(config_name)
            if config:
                handler = self.create_handler_from_config(config)
                result.add_output_handler(handler)
        
        return result
```

## Error Handling and Retry Strategies

### Built-in Retry Logic

Output handlers include retry capabilities:

```python
# Configure retry behavior
handler = QueueOutputHandler(
    destination="critical-queue",
    config={
        "max_retries": 3,
        "retry_backoff_seconds": 2,
        "timeout_seconds": 30
    }
)
```

### Custom Error Handling

Implement custom error handling in processors:

```python
def process(self, message: Message, context: ProcessorContext) -> ProcessingResult:
    try:
        # Processing logic
        result = ProcessingResult.create_success()
        
        # Add output with error handling
        handler = QueueOutputHandler(destination="output-queue")
        result.add_output_handler(handler)
        
        return result
        
    except Exception as e:
        # Create failure result
        result = ProcessingResult.create_failure(
            error_message=str(e),
            error_code="PROCESSING_FAILED",
            can_retry=True
        )
        
        # Still try to send to error queue
        result.add_output_handler(QueueOutputHandler(
            destination="error-queue",
            config={"metadata": {"error": str(e)}}
        ))
        
        return result
```

## Performance Considerations

### 1. Connection Pooling

Output handlers reuse connections where possible:

```python
class BatchProcessor(ProcessorInterface):
    def __init__(self):
        # Create handler once, reuse for all messages
        self.queue_handler = QueueOutputHandler(
            destination="batch-output",
            config={"connection_string": "..."}
        )
    
    def process(self, message: Message, context: ProcessorContext) -> ProcessingResult:
        result = ProcessingResult.create_success()
        # Reuse existing handler (connection pooled)
        result.add_output_handler(self.queue_handler)
        return result
```

### 2. Async Patterns

For high-throughput scenarios, consider async patterns:

```python
# Output handlers execute synchronously by default
# For async patterns, use queue-based decoupling:

def process(self, message: Message, context: ProcessorContext) -> ProcessingResult:
    # Quick processing
    result = ProcessingResult.create_success()
    
    # Queue for async processing
    result.add_output_handler(QueueOutputHandler(
        destination="async-processing-queue"
    ))
    
    # Return quickly
    return result
```

### 3. Batching Considerations

For file outputs, consider batching strategies:

```python
# JSONL format with append mode for efficient batching
handler = FileOutputHandler(
    destination="/data/events",
    config={
        "output_format": "jsonl",
        "append_mode": True,
        "file_pattern": "{date}/events.jsonl"
    }
)
```

## Migration from routing_info

If migrating from the older `routing_info` pattern:

**Old Pattern:**
```python
result = ProcessingResult.create_success()
result.routing_info = {
    "next_queue": "processing-queue",
    "priority": "high"
}
```

**New Pattern:**
```python
result = ProcessingResult.create_success()
result.add_output_handler(QueueOutputHandler(
    destination="processing-queue",
    config={"metadata": {"priority": "high"}}
))
```

**Benefits of Migration:**
1. Type-safe handler configuration
2. Built-in validation and error handling
3. Consistent execution and result tracking
4. Support for multiple outputs
5. Pluggable architecture for new handler types

## Testing Strategies

### Unit Testing

Test processors with output handlers:

```python
def test_processor_output_routing():
    # Create processor
    processor = MyProcessor()
    
    # Create test message
    message = Message.create_entity_message(
        entity=test_entity,
        payload={"test": "data"}
    )
    
    # Process
    result = processor.process(message, mock_context)
    
    # Verify output handlers
    assert result.has_output_handlers()
    assert len(result.output_handlers) == 2
    
    # Verify handler configuration
    queue_handler = result.output_handlers[0]
    assert isinstance(queue_handler, QueueOutputHandler)
    assert queue_handler.destination == "expected-queue"
```

### Integration Testing

Test with real infrastructure:

```python
def test_queue_output_integration():
    # Use real Azure Storage
    handler = QueueOutputHandler(
        destination="test-queue",
        config={"connection_string": "UseDevelopmentStorage=true"}
    )
    
    # Execute handler
    result = handler.handle(test_message, test_result)
    
    # Verify message in queue
    queue_client = QueueClient.from_connection_string(
        "UseDevelopmentStorage=true",
        "test-queue"
    )
    messages = queue_client.peek_messages()
    assert len(messages) > 0
```

## Troubleshooting

### Common Issues and Solutions

1. **Connection String Not Found**
   - Check environment variables: `AZURE_STORAGE_CONNECTION_STRING`, `AzureWebJobsStorage`
   - Verify configuration is passed correctly
   - Use explicit connection strings for debugging

2. **Queue/Topic Does Not Exist**
   - Enable `auto_create_queue` or `create_queue_if_not_exists`
   - Verify queue/topic name follows Azure naming rules
   - Check permissions for queue creation

3. **Message Size Limits**
   - Azure Storage Queues: 64KB limit
   - Service Bus: 256KB (standard) or 1MB (premium)
   - Consider storing large payloads in blob storage with reference in message

4. **File Permission Errors**
   - Verify write permissions for output directory
   - Enable `create_directories` for automatic directory creation
   - Check file system quotas and available space

### Debug Logging

Enable detailed logging for troubleshooting:

```python
import logging

# Enable debug logging for output handlers
logging.getLogger("src.processors.v2.output_handlers").setLevel(logging.DEBUG)

# Handler will log detailed execution information
handler = QueueOutputHandler(
    destination="debug-queue",
    config={"connection_string": "..."}
)
```

### Performance Monitoring

Monitor output handler performance:

```python
def process(self, message: Message, context: ProcessorContext) -> ProcessingResult:
    result = ProcessingResult.create_success()
    
    # Add handlers
    result.add_output_handler(queue_handler)
    result.add_output_handler(file_handler)
    
    # After processing, check performance
    if result.processing_metadata.get("output_handler_results"):
        for handler_result in result.processing_metadata["output_handler_results"]:
            print(f"Handler: {handler_result['handler_name']}")
            print(f"Duration: {handler_result['duration_ms']}ms")
            print(f"Success: {handler_result['success']}")
```

## Advanced Topics

### Custom Output Handlers

Create custom output handlers for specific needs:

```python
from src.processors.v2.output_handlers.base import OutputHandler, OutputHandlerResult

class WebhookOutputHandler(OutputHandler):
    """Send results to HTTP webhooks."""
    
    def handle(self, message: Message, result: ProcessingResult) -> OutputHandlerResult:
        import requests
        
        webhook_url = self.destination
        payload = self._prepare_payload(message, result)
        
        try:
            response = requests.post(
                webhook_url,
                json=payload,
                timeout=self.config.get("timeout", 30)
            )
            
            if response.status_code < 300:
                return self._create_success_result(
                    metadata={"status_code": response.status_code}
                )
            else:
                return self._create_failure_result(
                    error_message=f"HTTP {response.status_code}",
                    error_code="WEBHOOK_ERROR",
                    can_retry=response.status_code >= 500
                )
                
        except Exception as e:
            return self._create_failure_result(
                error_message=str(e),
                error_code="WEBHOOK_FAILED",
                can_retry=True
            )
```

### Handler Chaining

Chain handlers for complex workflows:

```python
class ChainedOutputHandler(OutputHandler):
    """Execute multiple handlers in sequence."""
    
    def __init__(self, destination: str, handlers: List[OutputHandler]):
        super().__init__(destination)
        self.handlers = handlers
    
    def handle(self, message: Message, result: ProcessingResult) -> OutputHandlerResult:
        for handler in self.handlers:
            handler_result = handler.handle(message, result)
            if not handler_result.success:
                # Stop chain on failure
                return handler_result
        
        return self._create_success_result()
```

### Metrics and Monitoring

Integrate with monitoring systems:

```python
class MetricsOutputHandler(OutputHandler):
    """Send metrics to monitoring system."""
    
    def handle(self, message: Message, result: ProcessingResult) -> OutputHandlerResult:
        # Send metrics
        self.send_metric("messages_processed", 1, {
            "processor": result.processor_info.get("name"),
            "status": result.status.value,
            "destination": self.destination
        })
        
        # Always succeed (metrics are best-effort)
        return self._create_success_result()
```

## Conclusion

Output handlers provide a powerful, flexible way to route processing results in the API Exchange Core framework. By following the patterns and practices in this guide, you can build robust, scalable data processing pipelines that adapt to your specific needs.

Key takeaways:
- Use output handlers to decouple processing logic from routing decisions
- Configure handlers through code, configuration files, or environment variables
- Implement error handling and retry strategies for resilience
- Monitor handler performance and success rates
- Create custom handlers for specialized requirements

For more information, see the API documentation and example implementations in the codebase.