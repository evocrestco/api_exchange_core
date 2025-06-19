# Developer Guide

This guide provides comprehensive information for developers working with or contributing to the API Exchange Core framework.

## Table of Contents

1. [Development Environment Setup](#development-environment-setup)
2. [Project Structure](#project-structure)
3. [Core Concepts](#core-concepts)
4. [Development Workflow](#development-workflow)
5. [Testing Philosophy](#testing-philosophy)
6. [Code Standards](#code-standards)
7. [Common Tasks](#common-tasks)
8. [Debugging Tips](#debugging-tips)
9. [Performance Optimization](#performance-optimization)
10. [Troubleshooting](#troubleshooting)

## Development Environment Setup

### Prerequisites

- Python 3.8+ (3.11 recommended)
- PostgreSQL 12+ (for production features)
- Git
- Virtual environment tool (venv, virtualenv, or poetry)

### Initial Setup

1. **Clone the repository**
   ```bash
   git clone https://github.com/evocrest/api_exchange_core.git
   cd api_exchange_core
   ```

2. **Create and activate virtual environment**
   ```bash
   python -m venv .venv
   source .venv/bin/activate  # On Windows: .venv\Scripts\activate
   ```

3. **Install dependencies**
   ```bash
   pip install -r requirements.txt
   pip install -r requirements-dev.txt
   ```

4. **Set up pre-commit hooks**
   ```bash
   pre-commit install
   ```

5. **Configure environment**
   ```bash
   cp .env.example .env
   # Edit .env with your settings
   ```

### IDE Configuration

#### VS Code
- Install Python extension
- Set Python interpreter to virtual environment
- Enable type checking: `"python.analysis.typeCheckingMode": "strict"`

#### PyCharm
- Mark `src` directory as Sources Root
- Configure Python interpreter
- Enable type checking in preferences

## Project Structure

```
api_exchange_core/
├── src/
│   ├── context/           # Context management (tenant, operation)
│   │   ├── tenant_context.py
│   │   ├── operation_context.py
│   │   └── service_decorators.py
│   ├── db/               # Database models and configuration
│   │   ├── db_base.py
│   │   ├── db_entity_models.py
│   │   └── ...
│   ├── repositories/     # Data access layer
│   │   ├── base_repository.py
│   │   ├── entity_repository.py
│   │   └── ...
│   ├── schemas/          # Pydantic models
│   │   ├── entity_schema.py
│   │   ├── processing_error_schema.py
│   │   └── ...
│   ├── services/         # Business logic layer
│   │   ├── base_service.py
│   │   ├── entity_service.py
│   │   └── ...
│   ├── utils/            # Utility functions
│   │   ├── hash_utils.py
│   │   ├── logger.py
│   │   └── ...
│   ├── config.py         # Configuration management
│   ├── constants.py      # System constants
│   ├── exceptions.py     # Custom exceptions
│   └── type_definitions.py  # Type definitions
├── tests/
│   ├── unit/            # Unit tests
│   ├── integration/     # Integration tests
│   └── fixtures/        # Test fixtures and factories
├── examples/            # Example implementations
└── docs/               # Documentation

```

## Core Concepts

### 1. Entity-Centric Architecture

Everything revolves around "entities" - the core data objects being processed:

```python
# Entity lifecycle
entity = Entity(
    id="unique-id",
    tenant_id="customer-1",
    external_id="CRM-12345",
    canonical_type="customer",
    source="salesforce",
    version=1,
    attributes={"name": "John Doe", "email": "john@example.com"}
)
```

### 2. Multi-Tenant Isolation

Every operation is tenant-scoped:

```python
from context.tenant_context import tenant_context

with tenant_context("customer-1"):
    # All operations here are scoped to customer-1
    entity = entity_service.get_entity(entity_id)
```

### 3. Repository Pattern

Data access is abstracted through repositories:

```python
# Don't do this:
session.query(Entity).filter(Entity.id == entity_id).first()

# Do this:
entity = entity_repository.get_by_id(entity_id)
```

### 4. Service Layer

Business logic lives in services:

```python
# Services handle business rules, validation, and orchestration
entity_id = entity_service.create_entity(
    external_id="CRM-12345",
    canonical_type="customer",
    source="salesforce",
    attributes={"name": "John Doe"}
)
```

### 5. Schema Validation

All data is validated using Pydantic:

```python
from schemas import EntityCreate

# This will validate the data
entity_data = EntityCreate(
   tenant_id="customer-1",
   external_id="CRM-12345",
   canonical_type="customer",
   source="salesforce"
)
```

## Development Workflow

### 1. Creating a New Feature

1. **Create/update schema models** (`src/schemas/`)
   ```python
   class NewFeatureCreate(BaseModel):
       name: str
       description: Optional[str] = None
   ```

2. **Update database models** if needed (`src/db/`)
   ```python
   class NewFeature(Base, BaseModel):
       __tablename__ = "new_features"
       # ... columns
   ```

3. **Create/update repository** (`src/repositories/`)
   ```python
   class NewFeatureRepository(BaseRepository):
       def custom_query(self):
           # Custom data access methods
   ```

4. **Create/update service** (`src/services/`)
   ```python
   class NewFeatureService(BaseService):
       def business_logic(self):
           # Business rules and orchestration
   ```

5. **Write tests** (`tests/unit/` and `tests/integration/`)

### 2. Modifying Existing Code

1. Check existing tests to understand current behavior
2. Update tests to reflect new requirements
3. Modify implementation
4. Run tests to ensure no regressions
5. Update documentation

## Testing Philosophy

### NO MOCKS Policy

We follow a strict "NO MOCKS" testing philosophy:

```python
# ❌ Don't do this:
def test_with_mock():
    mock_repo = MagicMock()
    mock_repo.get_by_id.return_value = {"id": "123"}
    
# ✅ Do this:
def test_with_real_objects(db_session):
    repo = EntityRepository(db_session)
    entity = factories.EntityFactory.create()
    result = repo.get_by_id(entity.id)
```

### Test Structure

```python
class TestEntityService:
    """Test entity service operations."""
    
    def test_create_entity_success(self, entity_service, tenant_context):
        """Test successful entity creation."""
        # Arrange
        with tenant_context("test-tenant"):
            # Act
            entity_id = entity_service.create_entity(
                external_id="TEST-123",
                canonical_type="customer",
                source="test"
            )
            
            # Assert
            assert entity_id is not None
            entity = entity_service.get_entity(entity_id)
            assert entity.external_id == "TEST-123"
```

### Test Categories

1. **Unit Tests** (`tests/unit/`)
   - Test individual components in isolation
   - Use SQLite in-memory database
   - Fast execution (< 100ms per test)

2. **Integration Tests** (`tests/integration/`)
   - Test component interactions
   - May use real PostgreSQL
   - Test external integrations

3. **End-to-End Tests** (`tests/e2e/`)
   - Test complete workflows
   - Use production-like environment

## Code Standards

### Python Style

We follow PEP 8 with these tools:
- **black**: Code formatting (line length: 100)
- **isort**: Import sorting
- **flake8**: Linting

### Type Hints

All code must be fully typed:

```python
# ✅ Good
def process_entity(entity_id: str, attributes: Dict[str, Any]) -> EntityRead:
    ...

# ❌ Bad
def process_entity(entity_id, attributes):
    ...
```

### Docstrings

Use Google-style docstrings:

```python
def process_entity(entity_id: str, validate: bool = True) -> EntityRead:
    """
    Process an entity with optional validation.
    
    Args:
        entity_id: The ID of the entity to process
        validate: Whether to validate the entity data
        
    Returns:
        The processed entity
        
    Raises:
        ValidationError: If validation fails
        ServiceError: If processing fails
    """
```

### Error Handling

Use the centralized exception system:

```python
from exceptions import ValidationError, ServiceError, ErrorCode

# Validation errors
if not entity_data.is_valid():
   raise ValidationError(
      "Invalid entity data",
      error_code=ErrorCode.VALIDATION_FAILED,
      details={"field": "email", "error": "Invalid format"}
   )

# Service errors
try:
   result = external_api.call()
except Exception as e:
   raise ServiceError(
      "External API call failed",
      error_code=ErrorCode.EXTERNAL_API_ERROR,
      cause=e
   )
```

### Logging

Use structured logging:

```python
logger.info(
    "Entity processed successfully",
    extra={
        "entity_id": entity_id,
        "tenant_id": tenant_id,
        "processing_time_ms": 123
    }
)
```

## Common Tasks

### Adding a New Entity Type

1. Define the schema:
   ```python
   # src/schemas/order_schema.py
   class OrderCreate(BaseModel):
       order_number: str
       customer_id: str
       total_amount: Decimal
   ```

2. Create a processor:
   ```python
   # src/processors/order_processor.py
   class OrderProcessor(ProcessorInterface):
       def process(self, message: Message) -> ProcessingResult:
           # Processing logic
   ```

### Adding a New External Integration

1. Create an adapter:
   ```python
   # src/adapters/shopify_adapter.py
   class ShopifyAdapter(AbstractAdapter):
       def connect(self):
           self.client = ShopifyClient(self.config)
           
       def fetch_orders(self) -> List[Dict]:
           return self.client.get_orders()
   ```

### Adding Database Migration

```bash
# Using Alembic
alembic revision -m "Add new_column to entities"
# Edit the generated migration file
alembic upgrade head
```

## Debugging Tips

### 1. Enable Debug Logging

```python
# In your .env or environment
LOG_LEVEL=DEBUG
```

### 2. Use Operation Context

```python
from context.operation_context import operation


@operation(name="debug_operation")
def my_function():
    # This will add operation tracking
    pass
```

### 3. Database Query Debugging

```python
# Enable SQLAlchemy query logging
import logging
logging.getLogger('sqlalchemy.engine').setLevel(logging.INFO)
```

### 4. Tenant Context Issues

```python
# Check current tenant
from context.tenant_context import TenantContext

current_tenant = TenantContext.get_current_tenant_id()
print(f"Current tenant: {current_tenant}")
```

## Performance Optimization

### 1. Database Queries

```python
# ❌ N+1 query problem
entities = entity_repository.get_all()
for entity in entities:
    transitions = state_repository.get_by_entity(entity.id)
    
# ✅ Eager loading
entities = entity_repository.get_all_with_transitions()
```

### 2. Batch Processing

```python
# Process in batches
for batch in entity_service.iter_entities(filter_data, batch_size=1000):
    process_batch(batch)
```

### 3. Caching

```python
# Use tenant context cache
tenant = TenantContext.get_tenant(db_manager, tenant_id)  # Cached
```

## Troubleshooting

### Common Issues

1. **Import Errors**
   - Always use absolute imports: `from src.services import EntityService`
   - Check PYTHONPATH includes project root

2. **Tenant Context Errors**
   ```python
   # Error: No tenant context set
   # Solution: Wrap in tenant context
   with tenant_context("tenant-id"):
       # Your code here
   ```

3. **Type Checking Errors**
   ```bash
   # Run mypy to check types
   python -m mypy src/
   ```

4. **Test Failures**
   ```bash
   # Run specific test with verbose output
   pytest tests/unit/test_entity_service.py::test_name -vvs
   ```

### Getting Help

1. Check existing tests for examples
2. Review similar code in the codebase
3. Consult the TECHNICAL.md for architecture details
4. Ask in development chat/forums

## Best Practices

1. **Write tests first** - TDD helps ensure code quality
2. **Keep functions small** - Single responsibility principle
3. **Use type hints** - Helps catch bugs early
4. **Document edge cases** - In tests and docstrings
5. **Handle errors explicitly** - Don't silently fail
6. **Log important events** - With structured data
7. **Review your own code** - Before requesting review
8. **Update documentation** - Keep it in sync with code

## Next Steps

- Read [TECHNICAL.md](TECHNICAL.md) for architecture details
- Check [examples/](examples/) for implementation patterns
- Review [tests/](tests/) for testing patterns
- See [CONTRIBUTING.md](CONTRIBUTING.md) for contribution guidelines