# How to Test Framework Extensions

This guide shows framework users how to test their custom implementations using the API Exchange Core testing infrastructure.

## Using Framework Test Examples

When building your own processors, adapters, or canonical models, reuse the framework's example models and testing patterns:

```python
# In your custom tests
from api_exchange_core.tests.fixtures.example_models import ExampleOrder
from api_exchange_core.tests.fixtures.factories import ExampleOrderFactory

def test_my_custom_processor(example_order_data):
    # Use framework examples in your tests
    processor = MyCustomOrderProcessor()
    result = processor.process(example_order_data)
    
    # Verify framework contracts
    assert isinstance(result, ProcessorResult)
    assert result.success is True
```

## Testing Custom Canonical Models

```python
from api_exchange_core.tests.fixtures.example_models import BaseExampleModel

class MyBusinessEntity(BaseExampleModel):
    # Your custom fields
    business_specific_field: str
    
def test_my_business_entity():
    # Follow framework patterns
    entity = MyBusinessEntity(
        business_specific_field="test_value",
        **example_base_data
    )
    assert entity.validate()
```

## Testing Custom Processors

```python
def test_custom_processor_integration(
    example_order_data, 
    processor_factory, 
    tenant_context
):
    # Test your processor with framework infrastructure
    processor = MyCustomProcessor()
    
    # Use real tenant context
    with tenant_context("test_tenant"):
        result = processor.process(example_order_data)
        
    # Verify framework contracts
    assert result.tenant_id == "test_tenant"
    assert result.entity_id is not None
```

This ensures your extensions work correctly with the framework and follow established patterns.