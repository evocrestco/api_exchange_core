# 🚀 Coffee Pipeline - Framework Integration Guide

The coffee pipeline demonstrates how to properly use the API Exchange Core framework with ProcessorInterface and ProcessorHandler for automatic enterprise features.

## 📚 Architecture Overview

The framework provides automatic infrastructure services while processors focus on business logic:

```
Azure Function → ProcessorHandler → ProcessorInterface → Business Logic
                       ↓
           (Automatic Services)
           • Entity Persistence
           • State Tracking
           • Error Recording
           • Retry Logic
           • Performance Monitoring
```

## 🔧 Implementation Pattern

### 1. Processor Implementation (Business Logic Only)

```python
# processors/order_ingestion_processor.py
class OrderIngestionProcessor(ProcessorInterface):
    def __init__(self, **kwargs):
        # Framework injects dependencies via kwargs
        self.logger = kwargs.get("logger", logging.getLogger())
    
    def process(self, message: Message) -> ProcessingResult:
        # ONLY business logic - transform coffee order
        canonical_data = self.mapper.to_canonical(message.payload)
        
        # Return routing decision
        return ProcessingResult(
            success=True,
            output_messages=[Message(payload=canonical_data)],
            processing_metadata={"next": "complexity_analysis"}
        )
```

### 2. Azure Function Setup (Framework Integration)

```python
# function_app.py
from src.processors.processor_factory import create_processor_handler

# Initialize framework services (once per app)
processing_service = ProcessingService(...)
entity_service = EntityService(...)

# Configure processor
config = ProcessorConfig(
    processor_name="order_ingestion",
    enable_state_tracking=True,      # Automatic!
    is_source_processor=True,        # Creates entities!
    enable_duplicate_detection=True  # Automatic!
)

# Create handler - this is the key!
handler = create_processor_handler(
    processor_class=OrderIngestionProcessor,
    config=config,
    processing_service=processing_service,
    entity_service=entity_service
)

# Azure Function just delegates to handler
@app.queue_trigger(...)
def my_function(msg):
    result = handler.handle_message(msg)
```

## ✅ What the Framework Provides Automatically

### Entity Management (for source processors)
- Creates entities with unique IDs
- Tracks entity versions
- Detects duplicates based on content hash
- Stores entity attributes and metadata

### State Tracking (when enabled)
- Records all state transitions
- Tracks: NONE → RECEIVED → PROCESSING → COMPLETED
- Captures actor, timestamp, and metadata
- Provides complete audit trail

### Error Handling
- Catches and classifies all errors
- Determines recoverability
- Records errors in database
- Implements retry logic with exponential backoff

### Performance Monitoring
- Tracks execution duration
- Records processing metadata
- Logs with correlation IDs
- Maintains tenant context

## 📝 Processor Types

### Source Processor (Creates Entities)
```python
config = ProcessorConfig(
    is_source_processor=True,  # Key setting!
    enable_duplicate_detection=True
)
```

### Intermediate Processor (Transforms/Routes)
```python
config = ProcessorConfig(
    is_source_processor=False,
    update_entity_attributes=True
)
```

### Terminal Processor (No Routing)
```python
config = ProcessorConfig(
    is_terminal_processor=True,
    processing_stage="final"
)
```

## 🎯 Benefits

1. **Clean Separation** - Business logic separate from infrastructure
2. **Automatic Features** - No manual state tracking or error handling
3. **Enterprise Ready** - Production-grade error handling and monitoring
4. **Testable** - Simple interfaces, dependency injection
5. **Maintainable** - Clear patterns, consistent structure

## 🚀 Running the Example

1. Start dependencies:
   ```bash
   docker-compose up -d  # PostgreSQL and Azurite
   ```

2. Run the pipeline:
   ```bash
   cd examples/coffee_pipeline
   func start
   ```

3. Send a pretentious order:
   ```bash
   curl -X POST http://localhost:7071/api/order \
     -H "Content-Type: application/json" \
     -d '{"order": "Venti half-caf, triple-shot, sugar-free vanilla, soy latte at 140°F"}'
   ```

4. Check the database for:
   - Entity created with the order
   - State transitions recorded
   - Complete audit trail

## 📊 Database Records Created

### Entities Table
- New entity with canonical coffee order
- Unique entity_id and external_id
- Content hash for duplicate detection

### State Transitions Table
- NONE → RECEIVED (order_ingestion)
- RECEIVED → PROCESSING (complexity_analysis)  
- PROCESSING → COMPLETED (human_translation)

### Processing Errors Table (if errors occur)
- Error classification and details
- Recovery strategy recommendations
- Full stack traces for debugging

The coffee pipeline shows that processors can be simple business logic components while the framework handles all the enterprise concerns automatically! ☕✨