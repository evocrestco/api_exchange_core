# API Exchange Core V2

**A clean, minimal data integration framework focused on pipeline-based message processing.**

## What This Is

API Exchange Core V2 is a complete rewrite of the data integration framework, removing all legacy concepts and focusing purely on:

- **Pipeline-based processing** using `pipeline_id` for tracking
- **Message-driven architecture** with simple Message objects  
- **Serverless-first design** for Azure Functions / AWS Lambda
- **No external dependencies** - processors just transform and route messages
- **Clean separation of concerns** - processors do business logic, handlers do routing

## What Was Removed (And Why)

❌ **All "entity" concepts** - The old system was deeply coupled to entity tracking, versioning, and persistence. This created massive complexity and made processors hard to test and understand.

❌ **Complex state tracking** - The state management system was tightly coupled to entities and created more problems than it solved.

❌ **Heavy database dependencies** - Most processors don't need complex database operations. Keep it simple.

❌ **Duplicate detection** - This was entity-specific and added complexity. Handle at message level if needed.

❌ **Complex error tracking** - Basic error handling is sufficient. Don't over-engineer.

## Core Architecture

### Message Flow
```
Queue A → [Function: SimpleProcessor] → Queue B → [Function: SimpleProcessor] → Queue C
```

Each processor:
1. Receives a `Message` from input queue
2. Processes the message data (business logic)
3. Returns a `ProcessingResult`
4. Framework routes result to output queues

### Key Components (To Be Built)

#### 1. **Message**
- Lightweight message structure for queue transport
- Contains `pipeline_id` for tracking execution across steps
- Supports any payload structure (dict or Pydantic models)
- Auto-generates UUIDs for correlation

#### 2. **SimpleProcessorInterface**
- Clean interface: `process(message, context) -> ProcessingResult`
- No dependencies on persistence or external systems
- Easy to test and implement

#### 3. **ProcessingResult**
- Clean result object with success/failure status
- Contains routing instructions (which queues to send to)
- No external dependencies

#### 4. **Output Handlers** (Optional)
- **QueueOutputHandler** - Route messages to Azure Storage Queues
- **NoOpOutputHandler** - Terminal processor (no routing)

## Design Principles

1. **Message-Centric** - Everything flows through Message objects
2. **Stateless Processors** - No persistence logic in business code
3. **Serverless Native** - Designed for function-based execution  
4. **Minimal Dependencies** - Keep it simple
5. **Easy Testing** - Simple interfaces, no mocking required
6. **Pipeline Observability** - Track execution flow via `pipeline_id`

## Example (Future Implementation)

```python
import azure.functions as func
from api_exchange_core.processors.v2 import (
    SimpleProcessorInterface,
    Message,
    ProcessingResult
)

class OrderProcessor(SimpleProcessorInterface):
    def process(self, message: Message, context: dict) -> ProcessingResult:
        order_data = message.payload
        processed_order = self.transform_order(order_data)
        
        return ProcessingResult.success_result(
            output_messages=[
                Message.create_simple_message(
                    payload=processed_order,
                    pipeline_id=message.pipeline_id
                )
            ]
        )

# Azure Function
@app.queue_trigger(arg_name="msg", queue_name="orders")
@app.queue_output(arg_name="output", queue_name="processed-orders")  
def process_order(msg: func.QueueMessage, output: func.Out[str]) -> None:
    processor = OrderProcessor()
    message = Message.model_validate_json(msg.get_body())
    result = processor.process(message, {"tenant_id": "test"})
    
    if result.success and result.output_messages:
        output.set(result.output_messages[0].model_dump_json())
```

## Migration Strategy

1. **Start fresh** - Don't try to migrate the old codebase
2. **Build minimal components** - Only what's actually needed  
3. **Test each component** - Ensure everything works before adding complexity
4. **Add features incrementally** - Based on real needs, not theoretical requirements

## Current Status

This is a clean slate. We will build only the minimal components needed for:
- Message processing
- Pipeline tracking via `pipeline_id`
- Simple routing between processors
- Basic error handling

Everything else can be added later if actually needed.