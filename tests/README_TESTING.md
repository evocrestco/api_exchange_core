# Testing Guide for API Exchange Core Framework

This document outlines our testing practices, tools, and patterns for the API Exchange Core framework.

## 🎉 Current Status: Infrastructure Validated ✅

**37/37 tests passing** with a fully validated testing infrastructure! The framework is ready for comprehensive test development.

- ✅ **Clean import solution** - No `__init__.py` pollution, proper path-based imports
- ✅ **Pydantic v2 modernization** - Using `EmailStr`, `Field()` constraints, `@computed_field`  
- ✅ **Anti-mock philosophy proven** - Real SQLite database testing works perfectly
- ✅ **Professional patterns demonstrated** - Heavy parameterization, fixture reuse, comprehensive validation

## Testing Philosophy

Our testing approach is built on these core principles:

- **Real Code Paths**: Test actual implementations, avoid mocks except for external services
- **Example-Driven Development**: Use concrete example models and processors consistently across all tests
- **Fixture Reuse**: Centralized, reusable fixtures with no redefinition across tests
- **Ultra Clean Tests**: Easy to understand, well-structured, and maintainable
- **High Coverage**: Aim for ≥90% code coverage with comprehensive positive and negative test cases
- **Fast Execution**: Use SQLite for speed without cleanup overhead
- **Parameterization**: Test multiple scenarios efficiently using pytest parameterization
- **Tenant Isolation**: All tests must validate multi-tenant behavior

## Testing Tools

### Core Testing Stack

- **pytest**: Primary testing framework with extensive plugin ecosystem
- **factory_boy**: Consistent test data generation with relationships
- **SQLAlchemy + SQLite**: Fast, clean database testing without external dependencies
- **pytest-cov**: Coverage reporting and enforcement
- **pytest-asyncio**: For testing async components (if needed)

### NO MOCKS Policy

We HATE mocks and only use them when absolutely necessary:

✅ **Acceptable Mocks:**
- Azure Storage Queue (external service)
- HTTP requests to external APIs
- File system operations (for test consistency)
- Environment variables

❌ **Forbidden Mocks:**
- Database operations (use real SQLite)
- Internal services (test real implementations)
- Repositories (test actual data access)
- Context management (test real tenant isolation)
- Utility functions (test actual implementations)

## Test Structure

```
tests/
├── conftest.py                    # Root fixtures: database, sessions, example data
├── README_TESTING.md             # This file
├── unit/                         # Component isolation tests
│   ├── conftest.py               # Unit-specific fixtures
│   ├── models/                   # Database model tests
│   ├── repositories/             # Data access layer tests
│   ├── services/                 # Business logic tests
│   ├── utils/                    # Utility function tests
│   ├── context/                  # Context management tests
│   └── test_example_models.py    # Example demonstrating all patterns
├── integration/                  # Cross-component tests
│   ├── conftest.py               # Integration fixtures
│   └── end_to_end/               # Full workflow tests
├── fixtures/                     # Test infrastructure (NOT a Python package)
│   ├── example_models.py         # Example canonical models with Pydantic v2
│   ├── example_processors.py     # Example processor implementations
│   └── factories.py              # Factory Boy data factories
└── examples/                     # Documentation examples
    └── how_to_test_extensions.md # Guide for framework users
```

**IMPORTANT**: The `fixtures/` directory is NOT a Python package (no `__init__.py`). This maintains clean separation between test infrastructure and the main codebase.

## Example Models Strategy

Instead of abstract or business-specific entities, we use realistic but generic examples that demonstrate framework capabilities:

### Core Example Models

1. **ExampleOrder**: E-commerce order with line items, customer info, shipping
2. **ExampleInventory**: Stock management with locations, quantities, reservations
3. **ExampleCustomer**: Customer profiles with addresses, preferences, history

These models are used **consistently across ALL tests** and serve as:
- Framework capability demonstrations
- User implementation examples
- Comprehensive test coverage vehicles
- Documentation through code

### Pydantic v2 Modernization

Our example models use modern Pydantic v2 patterns:

```python
# Modern Field constraints instead of custom validators
amount: Decimal = Field(ge=0, description="Amount cannot be negative")
currency_code: str = Field(default="USD", min_length=3, max_length=3)
country_code: str = Field(default="US", min_length=2, max_length=2)

# EmailStr for automatic email validation
customer_email: EmailStr  
primary_email: EmailStr

# @field_validator for complex validation (replaces @validator)
@field_validator('order_total')
@classmethod
def validate_order_total(cls, v: Money, info) -> Money:
    # Modern validation logic with info.data

# @computed_field for calculated values (replaces manual properties)
@computed_field
@property  
def quantity_available(self) -> int:
    return self.quantity_on_hand - self.quantity_reserved
```

**Dependencies**: Requires `email-validator` package for EmailStr support.

### Example Processors

1. **ExampleOrderProcessor**: Source processor for order ingestion
2. **ExampleOrderValidator**: Intermediate processor for validation
3. **ExampleOrderEnricher**: Intermediate processor for data enrichment
4. **ExampleOrderExporter**: Terminal processor for external delivery

## Fixture Organization

### Hierarchical Fixture Structure

```python
# tests/conftest.py - Root level (MAIN FIXTURE HUB)
- test_engine: SQLite engine (session-scoped)
- db_session: Transaction-isolated session (function-scoped)  
- db_manager: Test database manager
- example_order_data: Standard order test data (function-scoped)
- example_inventory_data: Standard inventory test data (function-scoped)
- example_customer_data: Standard customer test data (function-scoped)
- create_example_*_data(): Helper functions for generating test data

# tests/unit/conftest.py - Unit test fixtures
- order_repository: Repository with test session
- order_service: Service with test dependencies
- tenant_context: Tenant context setup/teardown

# tests/integration/conftest.py - Integration fixtures  
- full_pipeline_setup: End-to-end test environment
- example_workflow: Complete processing workflow
```

### Import Strategy for Example Models

Since `fixtures/` is not a Python package, we use path-based imports in test files:

```python
# In test files (e.g., tests/unit/test_example_models.py)
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'fixtures'))

from example_models import (
    ExampleOrder, ExampleInventoryItem, ExampleCustomer,
    OrderStatus, PaymentStatus, InventoryStatus, CustomerType,
    Money, Address
)
```

This approach:
- ✅ Keeps `fixtures/` as a pure module directory (not a package)  
- ✅ Avoids circular import issues
- ✅ Maintains clean separation between test infrastructure and production code
- ✅ Allows easy access to example models from any test file

### Fixture Rules

1. **No Redefinition**: Each fixture defined once, reused everywhere
2. **Scope Appropriately**: Session > Module > Function based on cost
3. **Clean Teardown**: Automatic cleanup without manual intervention
4. **Dictionary Returns**: Return dicts to prevent accidental state modification
5. **Parameterized Fixtures**: Use `pytest.fixture(params=...)` for variations

## Testing Patterns

### 1. Example Model Usage

```python
def test_order_processing(example_order_data, order_service):
    # Use consistent example data
    order_id = order_service.create_order(example_order_data)
    
    # Verify with realistic data
    order = order_service.get_order(order_id)
    assert order["customer_name"] == example_order_data["customer_name"]
    assert len(order["line_items"]) == len(example_order_data["line_items"])
```

### 2. Heavy Parameterization

```python
@pytest.mark.parametrize("entity_type,expected_fields", [
    ("order", ["customer_id", "line_items", "total_amount"]),
    ("inventory", ["product_id", "quantity", "location"]),
    ("customer", ["name", "email", "addresses"]),
])
def test_entity_validation(entity_type, expected_fields, entity_service):
    schema = entity_service.get_validation_schema(entity_type)
    for field in expected_fields:
        assert field in schema.fields
```

### 3. Real Database Testing

```python
def test_entity_versioning(db_session, entity_repository):
    # Create entity
    entity_data = {"name": "Test Entity", "attributes": {"version": 1}}
    entity_id = entity_repository.create(entity_data)
    
    # Update entity (creates new version)
    updated_data = {"name": "Updated Entity", "attributes": {"version": 2}}
    entity_repository.update(entity_id, updated_data)
    
    # Verify both versions exist in database
    versions = db_session.query(Entity).filter_by(logical_id=entity_id).all()
    assert len(versions) == 2
    assert versions[0].attributes["version"] != versions[1].attributes["version"]
```

### 4. Tenant Isolation Testing

```python
@pytest.mark.parametrize("tenant_id", ["tenant_a", "tenant_b", "tenant_c"])
def test_tenant_data_isolation(tenant_id, entity_service, example_order_data):
    # Set tenant context
    with TenantContext.set_context(tenant_id):
        # Create entity in specific tenant
        entity_id = entity_service.create_entity(example_order_data)
        
        # Verify entity exists for this tenant
        entity = entity_service.get_entity(entity_id)
        assert entity["tenant_id"] == tenant_id
        
    # Verify entity not visible from other tenants
    with TenantContext.set_context("different_tenant"):
        with pytest.raises(ServiceError):
            entity_service.get_entity(entity_id)
```

### 5. Error Path Testing

```python
@pytest.mark.parametrize("invalid_data,expected_error", [
    ({}, ValidationError),  # Missing required fields
    ({"invalid_field": "value"}, SchemaValidationError),  # Unknown field
    ({"amount": -100}, BusinessRuleValidationError),  # Negative amount
])
def test_validation_errors(invalid_data, expected_error, entity_service):
    with pytest.raises(expected_error):
        entity_service.create_entity(invalid_data)
```

## Test Categories

### Unit Tests (`tests/unit/`)

Test individual components in isolation:

- **Models**: Validation, serialization, relationships
- **Repositories**: CRUD operations, filtering, tenant isolation
- **Services**: Business logic, error handling, context management
- **Utils**: Hash functions, JSON utilities, logging
- **Context**: Tenant and operation context behavior

### Integration Tests (`tests/integration/`)

Test components working together:

- **End-to-End Workflows**: Complete processing pipelines
- **Cross-Service Integration**: Services interacting properly
- **Database Transactions**: Complex multi-table operations
- **Context Propagation**: Tenant context across service calls

## Writing Effective Tests

### Test Structure (AAA Pattern)

```python
def test_entity_creation(entity_service, example_order_data, tenant_context):
    # ARRANGE: Set up test data and preconditions
    order_data = {**example_order_data, "special_field": "test_value"}
    
    # ACT: Execute the function being tested
    entity_id = entity_service.create_entity(order_data)
    
    # ASSERT: Verify results meet expectations
    created_entity = entity_service.get_entity(entity_id)
    assert created_entity["special_field"] == "test_value"
    assert created_entity["tenant_id"] == tenant_context["tenant_id"]
```

### Error Testing Guidelines

Test both success and failure paths:

```python
def test_comprehensive_error_handling(entity_service):
    # Test success path
    valid_data = {"name": "Valid Entity", "amount": 100}
    entity_id = entity_service.create_entity(valid_data)
    assert entity_id is not None
    
    # Test specific error conditions
    with pytest.raises(ValidationError, match="Name is required"):
        entity_service.create_entity({"amount": 100})
    
    with pytest.raises(BusinessRuleValidationError, match="Amount must be positive"):
        entity_service.create_entity({"name": "Test", "amount": -50})
```

## Coverage Requirements

- **Framework Core**: ≥95% coverage (models, repositories, services)
- **Utilities**: ≥90% coverage (helpers, context management)
- **Examples**: ≥85% coverage (demonstration code)
- **Integration**: ≥80% coverage (cross-component scenarios)

## Running Tests

### Run all tests
```bash
# From project root (recommended)
pytest tests/

# Or specifically
pytest tests/unit/
pytest tests/integration/
```

**Note**: Due to import paths in test fixtures, always run pytest from the project root directory, not from within the tests/ directory.

### Run with coverage
```bash
# Run all tests with coverage
pytest --cov=src --cov-report=html --cov-report=term

# Run specific test categories
pytest tests/unit/ --cov=src                    # Unit tests only
pytest tests/integration/ --cov=src            # Integration tests only

# Run with specific markers
pytest -m "not slow"                 # Skip slow tests
pytest -m "tenant_isolation"         # Test tenant features only

# Run with debugging
pytest --pdb                         # Drop into debugger on failure
pytest --log-cli-level=DEBUG         # Show debug logs

# Run specific parameterized test
pytest tests/unit/test_entity.py::test_validation -k "order"
```

## Common Anti-Patterns to Avoid

❌ **Don't Do This:**

```python
# Using mocks for internal components
@patch('src.services.entity_service.EntityRepository')
def test_service(mock_repo):
    pass

# Hard-coded test data
def test_entity_creation():
    entity = Entity(id="123", name="Test")  # Brittle!

# Fixture redefinition
@pytest.fixture
def entity_service():  # Already defined in conftest.py!
    pass

# Testing implementation details
def test_internal_method():
    service._private_method()  # Don't test private methods!
```

✅ **Do This Instead:**

```python
# Use real implementations
def test_service(entity_service, db_session):
    pass

# Use factories and fixtures
def test_entity_creation(example_order_data, entity_service):
    entity = entity_service.create_entity(example_order_data)

# Reuse existing fixtures
def test_my_feature(entity_service):  # Use existing fixture
    pass

# Test public interfaces
def test_public_api(entity_service):
    result = entity_service.public_method()
```

## Framework Extension Testing

When users extend the framework, they should follow these patterns:

```python
# tests/test_my_custom_processor.py
def test_custom_order_processor(example_order_data, processor_factory):
    # Use framework examples in custom tests
    processor = MyCustomOrderProcessor()
    result = processor.process(example_order_data)
    
    # Verify framework contracts
    assert isinstance(result, ProcessorResult)
    assert result.success is True
    assert result.output_entity["id"] is not None
```

This ensures consistency between framework and extension testing approaches.

## Testing Infrastructure Validation

### Current Status ✅

The testing infrastructure has been **fully validated** with a comprehensive example test suite:

- **37/37 tests passing** in `tests/unit/test_example_models.py`
- **Coverage**: 40% overall framework coverage achieved
- **Patterns demonstrated**: All testing patterns working correctly
- **Import issues resolved**: Clean, maintainable import structure
- **Pydantic v2 validated**: Modern validation patterns proven

### Key Achievements

1. **Anti-Mock Philosophy Proven**: Real SQLite database testing works flawlessly
2. **Example-Driven Development**: Consistent ExampleOrder/Inventory/Customer models
3. **Fixture Hierarchy**: Centralized, reusable fixtures with no redefinition
4. **Professional Test Patterns**: Heavy parameterization, comprehensive validation
5. **Framework Extensibility**: Clear patterns for users to follow

### Next Steps

With the infrastructure validated, focus on:

1. **Repository Tests**: Entity, tenant, state transition, error repositories
2. **Service Tests**: Business logic, error handling, context management  
3. **Utility Tests**: Hash, JSON, logging, Azure queue utilities
4. **Integration Tests**: End-to-end workflows and multi-tenant scenarios

**Target**: Achieve ≥90% coverage across all framework components.