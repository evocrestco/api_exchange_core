# API Exchange Core Examples

This directory contains examples demonstrating how to use the API Exchange Core framework. 
These samples show proper implementation patterns for adapters, processors, and canonical models.

## Examples Overview

### Basic Integration

- **SimpleAdapter**: A minimal adapter implementation showing required methods
- **SimpleProcessor**: Basic processor implementation with core functionality
- **SimpleCanonical**: Example canonical model implementation

### Advanced Patterns

- **CompleteAdapter**: Full-featured adapter with error handling and retry logic
- **RoutingProcessor**: Processor with complex routing logic based on content
- **ExtendedCanonicalModel**: Demonstrates extending the canonical model system

## Getting Started

To run the examples:

1. Install the API Exchange Core package
2. Set up required environment variables
3. Run the example files directly or integrate into your own projects

## Creating Your Own Integration

### Adapters

To create a custom adapter:

1. Implement the `AbstractAdapter` interface
2. Create a configuration class extending `AdapterConfig`
3. Implement required methods (`fetch`, `send`, `update`, etc.)
4. Register your adapter with the system

Example:

```python
from api_exchange_core.core.interfaces.adapter import AbstractAdapter, AdapterConfig

class MyAdapterConfig(AdapterConfig):
    api_key: str
    base_url: str
    timeout: int = 30

class MyAdapter(AbstractAdapter[MyCanonicalModel]):
    def __init__(self, config: MyAdapterConfig):
        super().__init__(config)
        self.client = ExternalSystemClient(
            api_key=config.api_key,
            base_url=config.base_url,
            timeout=config.timeout,
        )
    
    def fetch(self, params):
        # Implementation...
        
    def send(self, model):
        # Implementation...
        
    def update(self, external_id, model):
        # Implementation...
        
    def to_canonical(self, external_data):
        # Implementation...
        
    def from_canonical(self, model):
        # Implementation...
```

### Processors

To create a custom processor:

1. Implement the `ProcessorInterface` 
2. Handle source data or intermediate processing
3. Return appropriate results

Example:

```python
from api_exchange_core.core.interfaces.processor import ProcessorInterface, ProcessingResult

class MyProcessor(ProcessorInterface[MyEntity]):
    def __init__(self, config):
        self.config = config
        
    def __call__(self, data):
        # Process data
        result = self._process_data(data)
        
        # Return appropriate result
        return ProcessingResult(
            entity_id=result.entity_id,
            output_name="approved",
            attributes_update={"status": "approved"}
        )
```

### Canonical Models

To create a custom canonical model:

1. Implement the `AbstractCanonicalEntity` interface
2. Define your entity structure
3. Implement required methods

Example:

```python
from pydantic import BaseModel, Field
from api_exchange_core.core.interfaces.canonical import AbstractCanonicalEntity

class MyCanonicalModel(BaseModel, AbstractCanonicalEntity):
    id: Optional[str] = None
    external_id: str
    name: str
    attributes: Dict[str, Any] = Field(default_factory=dict)
    
    @property
    def entity_type(self) -> str:
        return "my_entity"
        
    def model_dump(self) -> Dict[str, Any]:
        return {
            "id": self.id,
            "external_id": self.external_id,
            "name": self.name,
            "attributes": self.attributes,
        }
```

## Best Practices

- Keep adapters focused on data transformation and external communication
- Use contextual decorators (`@operation`, `@tenant_aware`) on adapter methods
- Design processors to be stateless and focused on a single task
- Follow consistent error handling patterns
- Use message-based communication between components