# Technical Architecture

This document provides detailed technical information about the API Exchange Core framework architecture.

## Architecture Overview

The framework follows a layered architecture with clear separation of concerns:

```
┌─────────────────────────────────────────────────────────┐
│                   External Systems                       │
│         (APIs, Databases, Queues, Files, etc.)          │
└─────────────────────────────────────────────────────────┘
                            ↕
┌─────────────────────────────────────────────────────────┐
│                      Adapters Layer                      │
│          (System-specific implementations)               │
└─────────────────────────────────────────────────────────┘
                            ↕
┌─────────────────────────────────────────────────────────┐
│                   Processing Layer                       │
│     (Source, Intermediate, Terminal Processors)          │
└─────────────────────────────────────────────────────────┘
                            ↕
┌─────────────────────────────────────────────────────────┐
│                     Core Services                        │
│  (Entity, State Tracking, Error, Tenant Services)       │
└─────────────────────────────────────────────────────────┘
                            ↕
┌─────────────────────────────────────────────────────────┐
│                   Data Layer                             │
│        (Repositories, Models, Database)                  │
└─────────────────────────────────────────────────────────┘
```

## Core Components

### 1. Entity Management

The framework treats all data as "entities" with the following characteristics:

- **Unique Identification**: Each entity has a logical ID and version
- **Content-Based Versioning**: New versions created when content changes
- **State Tracking**: Full lifecycle tracking from creation to completion
- **Attribute Storage**: Flexible JSON attributes for any data structure

```python
# Entity lifecycle
Created → Processing → Processed → Completed
                ↓
            Failed → Retry → Processing
```

### 2. Processing Framework

Three types of processors handle data transformation:

#### Source Processors
- Ingest data from external systems
- Convert to canonical format
- Create initial entity versions

#### Intermediate Processors
- Transform, validate, enrich data
- Make routing decisions
- Can create multiple output messages

#### Terminal Processors
- Send data to final destinations
- Handle delivery confirmation
- Update entity state to completed

### 3. Messaging System

Standardized message format for inter-component communication:

```python
Message(
    id="unique-message-id",
    correlation_id="request-correlation-id",
    entity_reference=EntityReference(id="entity-id", version=1),
    entity_type="order",
    entity_data={...},  # Actual data payload
    metadata={...},     # Processing metadata
    created_at=datetime.utcnow()
)
```

### 4. Context Management

Two types of context ensure proper isolation and tracking:

#### Tenant Context
- Isolates data between different customers
- Automatically applied to all database queries
- Thread-safe context management

#### Operation Context
- Tracks processing operations
- Provides correlation across components
- Enables detailed audit trails

### 5. Error Handling

Comprehensive error management system:

- **Error Classification**: Validation, business rule, system errors
- **Error Tracking**: All errors stored with full context
- **Retry Logic**: Configurable retry policies per error type
- **Dead Letter Queue**: Failed messages after max retries

### 6. State Transitions

Every entity state change is tracked:

```python
StateTransition(
    entity_id="...",
    from_state="created",
    to_state="processing",
    processor_id="validator",
    timestamp=datetime.utcnow(),
    metadata={...}
)
```

## Database Schema

### Core Tables

- **entities**: Main entity storage with versioning
- **entity_state_transitions**: State change history
- **processing_errors**: Error tracking and analysis
- **tenants**: Multi-tenant configuration
- **tenant_credentials**: Secure credential storage

### Design Principles

- **Soft Deletes**: Nothing is permanently deleted
- **Audit Trail**: Complete history of all changes
- **Tenant Isolation**: Row-level security via tenant_id
- **JSON Flexibility**: Attributes stored as JSON for schema flexibility

## Extension Points

### Custom Processors

```python
class MyProcessor(ProcessorInterface):
    def __init__(self, config: Dict[str, Any]):
        self.config = config
    
    def process(self, message: Message) -> ProcessingResult:
        # Your logic here
        return ProcessingResult(
            success=True,
            output_messages=[...],
            metadata={...}
        )
```

### Custom Adapters

```python
class MyAdapter(AbstractAdapter):
    def connect(self) -> None:
        # Establish connection
    
    def fetch(self) -> List[Dict]:
        # Retrieve data
    
    def send(self, data: Any) -> bool:
        # Send data to system
```

### Custom Canonical Models

```python
class MyEntity(BaseModel):
    id: str
    name: str
    # Your fields
    
    @property
    def entity_type(self) -> str:
        return "my_entity"
```

## Performance Considerations

### Database Optimization
- Indexes on tenant_id, entity_id, state
- Composite indexes for common queries
- JSON GIN indexes for attribute searches (PostgreSQL)

### Processing Optimization
- Batch processing support
- Async processor execution
- Connection pooling for adapters
- Message batching for queues

### Memory Management
- Streaming for large datasets
- Lazy loading of entity attributes
- Configurable batch sizes

## Security Features

### Multi-Tenancy
- Complete data isolation
- Tenant-specific configurations
- Separate credential management

### Credential Management
- Encrypted storage
- Rotation support
- Audit logging

### Data Protection
- PII detection and masking
- Audit trails
- Role-based access control (when integrated)

## Monitoring and Observability

### Metrics
- Processing rates
- Error rates
- Queue depths
- Processing latency

### Logging
- Structured logging with context
- Log levels per component
- Integration with log aggregators

### Tracing
- Correlation IDs across components
- Operation timing
- Dependency tracking

## Testing Support

### Test Infrastructure
- SQLite for fast testing
- Factory-based test data (using Factory Boy)
- Fixture hierarchy with dependency injection
- **Strict NO MOCKS philosophy** - we test with real implementations

### Test Patterns
- Parameterized tests for edge cases
- Property-based testing support
- Integration test helpers
- Performance benchmarks
- 85%+ code coverage requirement

### Testing Best Practices
```python
# We use real objects, not mocks
def test_entity_service(db_session, entity_repository):
    # Real repository with real database
    service = EntityService(entity_repository)
    entity_id = service.create_entity(...)
    
    # Assertions against real data
    entity = service.get_entity(entity_id)
    assert entity.external_id == "expected"
```

## Code Quality Standards

### Type Safety
- Full type hints throughout the codebase
- Mypy strict mode compliance
- Pydantic v2 for runtime validation

### Code Formatting
- Black (line length: 100)
- isort for import organization  
- flake8 for linting
- Pre-commit hooks for consistency

### Documentation
- Google-style docstrings
- Comprehensive inline comments
- Architecture decision records

## Recent Architectural Improvements

### Enhanced Error Handling (2024)
- Centralized exception hierarchy with error codes
- Structured error context and chaining
- Repository and service-level error translation

### Type System Improvements
- Migration to Pydantic v2
- Comprehensive type hints
- Reduced mypy errors by 70%

### Testing Infrastructure
- NO MOCKS philosophy enforcement
- Fixture-based test data management
- Improved test isolation

## Deployment Patterns

### Serverless
- Lambda/Azure Functions compatible
- Minimal cold start
- Stateless processing

### Container-Based
- Docker support
- Kubernetes ready
- Horizontal scaling

### Traditional
- Long-running services
- Background workers
- Scheduled jobs