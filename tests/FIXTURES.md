# Test Fixtures Reference

This document lists all available pytest fixtures organized by category and location. Use existing fixtures whenever possible before creating new ones.

## Database & Core Infrastructure
**Location: `tests/conftest.py`**

| Fixture | Scope | Description |
|---------|-------|-------------|
| `test_engine` | session | Test database engine |
| `db_session` | function | Database session for tests |
| `db_manager` | function | Database manager instance |
| `factory_session` | function | Factory session for test data creation |
| `clean_environment` | function (autouse) | Cleans test environment between tests |

## Tenant Management
**Location: `tests/conftest.py`**

| Fixture | Scope | Description |
|---------|-------|-------------|
| `test_tenant` | function | Single test tenant |
| `tenant_context` | function | Tenant context for operations |
| `multi_tenant_context` | function | Multiple tenants for isolation tests |
| `tenant_context_fixture` | function | Alternative tenant context |
| `tenant_with_config` | function | Tenant with configuration |
| `with_tenant_env` | function | Tenant environment setup |

## Repositories
**Location: `tests/unit/conftest.py`**

| Fixture | Scope | Description |
|---------|-------|-------------|
| `entity_repository` | function | Entity repository with test session |
| `tenant_repository` | function | Tenant repository |
| `state_transition_repository` | function | State transition repository |
| `processing_error_repository` | function | Processing error repository |

## Services
**Location: `tests/unit/conftest.py`**

| Fixture | Scope | Description |
|---------|-------|-------------|
| `entity_service` | function | Entity service with test repository |
| `tenant_service` | function | Tenant service |
| `state_tracking_service` | function | State tracking service |
| `processing_error_service` | function | Processing error service |
| `processing_service` | function | Processing service with real dependencies |
| `processor_context` | function | ProcessorContext for v2 processors |

## Test Data
**Location: `tests/conftest.py` & `tests/unit/conftest.py`**

| Fixture | Scope | Description |
|---------|-------|-------------|
| `example_order_data` | function | Sample order data structure |
| `example_inventory_data` | function | Sample inventory data structure |
| `example_customer_data` | function | Sample customer data structure |
| `test_entities` | function | Pre-created test entities in database |
| `hash_test_data` | function | Test data for hash utilities |
| `json_test_data` | function | Test data for JSON utilities |
| `context_test_data` | function | Test data for context management |

## Domain Examples
**Location: `tests/fixtures/conftest.py`**

| Fixture | Scope | Description |
|---------|-------|-------------|
| `example_order` | function | Example order object |
| `example_inventory` | function | Example inventory object |
| `example_customer` | function | Example customer object |
| `example_order_entity` | function | Order entity persisted in database |
| `example_inventory_entity` | function | Inventory entity persisted in database |
| `example_customer_entity` | function | Customer entity persisted in database |
| `complete_order_workflow` | function | Complete order processing workflow |
| `example_entity_type` | parameterized | Parameterized entity types (order, inventory, customer) |
| `entity_batch_size` | parameterized | Parameterized batch sizes (1, 5, 10) |
| `invalid_order_data_samples` | function | Invalid order data for error testing |
| `edge_case_data_samples` | function | Edge case data samples |
| `multi_tenant_scenario` | function | Multi-tenant test scenario |
| `order_processing_scenario` | function | Order processing scenario |
| `error_recovery_scenario` | function | Error recovery scenario |

## Mock Objects (Use sparingly - NO MOCKS policy)
**Location: `tests/unit/conftest.py`**

| Fixture | Scope | Description |
|---------|-------|-------------|
| `mock_azure_queue_client` | function | Mock Azure Queue client (only for external deps) |
| `mock_http_client` | function | Mock HTTP client (only for external deps) |

## Integration Test Fixtures
**Location: `tests/integration/conftest.py`**

| Fixture | Scope | Description |
|---------|-------|-------------|
| `load_env_for_integration` | session (autouse) | Loads environment variables for integration tests |
| `setup_integration_environment` | session (autouse) | Sets up integration test environment |
| `clean_environment` | function (autouse) | Cleans environment between integration tests |
| `integration_db_session` | function | Database session for integration tests |
| `integration_tenant_context` | function | Tenant context for integration tests |
| `azure_storage_connection_string` | session | Azure Storage connection string |
| `dead_letter_queue_client` | function | Dead letter queue client for testing |
| `queue_message_verifier` | function | Queue message verification utility |
| `output_queue_client` | function | Output queue client for testing |
| `output_queue_verifier` | function | Output queue verification utility |

**Location: `tests/integration/test_hello_world_integration.py`**

| Fixture | Scope | Description |
|---------|-------|-------------|
| `hello_world_processor_handler` | function | Hello world processor handler for testing |
| `test_message` | function | Test message for integration tests |
| `entity_service_for_validation` | function | Entity service for validation in integration tests |

## Component-Specific Fixtures

### Processing (tests/unit/processing/)
**Location: `tests/unit/processing/conftest.py`**

| Fixture | Scope | Description |
|---------|-------|-------------|
| `create_test_entity` | function | Creates test entity for processing tests |
| `create_test_message` | function | Creates test message for processing tests |

**Location: Individual test files**

| Fixture | Location | Description |
|---------|----------|-------------|
| `detection_service` | test_duplicate_detection.py | Duplicate detection service |
| `sample_order_data` | test_duplicate_detection.py | Sample order data |
| `processing_service` | test_processing_service.py | Processing service setup |
| `services` | test_processing_service.py | All processing services |
| `test_processor` | test_processing_service.py | Test processor implementation |
| `test_message` | test_processing_service.py | Test message for processing |
| `attribute_builder` | test_entity_attributes.py | Entity attribute builder |
| `sample_duplicate_result` | test_entity_attributes.py | Sample duplicate detection result |

### Processors v2 (tests/unit/processors/v2/)
**Location: `tests/unit/processors/v2/output_handlers/conftest.py`**

| Fixture | Scope | Description |
|---------|-------|-------------|
| `test_entity` | function | Test entity for output handler tests |
| `test_message` | function | Test message for output handler tests |
| `test_processing_result` | function | Test processing result for output handlers |
| `create_test_entity` | function | Factory function for creating test entities |
| `create_test_message` | function | Factory function for creating test messages |
| `azurite_connection_string` | function | Azurite connection string for storage tests |

**Location: Individual test files**

| Fixture | Location | Description |
|---------|----------|-------------|
| `processor` | test_processor_interface.py | Test processor instances |
| `sample_message` | test_processor_interface.py | Sample message for testing |

### Output Handlers (tests/unit/processors/v2/output_handlers/)
**Location: Individual test files**

| Fixture | Location | Description |
|---------|----------|-------------|
| `handler` | test_base.py | Base output handler for testing |
| `mock_message` | test_base.py | Mock message for base handler tests |
| `mock_result` | test_base.py | Mock result for base handler tests |
| `handler` | test_noop_output.py | NoOp output handler |
| `minimal_handler` | test_noop_output.py | Minimal NoOp handler configuration |
| `mock_message` | test_noop_output.py | Mock message for NoOp tests |
| `mock_result` | test_noop_output.py | Mock result for NoOp tests |
| `temp_dir` | test_file_output.py | Temporary directory for file output tests |
| `handler` | test_file_output.py | File output handler |
| `jsonl_handler` | test_file_output.py | JSONL format file handler |
| `text_handler` | test_file_output.py | Text format file handler |
| `setup_azurite` | test_queue_output.py (autouse) | Sets up Azurite for queue tests |
| `handler` | test_queue_output.py | Queue output handler |
| `minimal_handler` | test_queue_output.py | Minimal queue handler configuration |
| `cleanup_queues` | test_queue_output.py | Cleans up test queues |
| `setup_environment` | test_service_bus_output.py (autouse) | Sets up Service Bus environment |
| `skip_if_no_servicebus` | test_service_bus_output.py | Skips tests if Service Bus SDK not available |
| `handler` | test_service_bus_output.py | Service Bus output handler |
| `topic_handler` | test_service_bus_output.py | Service Bus topic handler |
| `minimal_handler` | test_service_bus_output.py | Minimal Service Bus handler |
| `mock_message` | test_service_bus_output.py | Mock message for Service Bus tests |
| `mock_result` | test_service_bus_output.py | Mock result for Service Bus tests |

### Services (tests/unit/services/)
**Location: Individual test files**

| Fixture | Location | Description |
|---------|----------|-------------|
| `service` | test_base_service.py | Base service for testing |

## Adding New Fixtures

When adding new fixtures:

1. **Check existing fixtures first** - avoid duplication
2. **Place in appropriate conftest.py**:
   - Global fixtures → `tests/conftest.py`
   - Unit test fixtures → `tests/unit/conftest.py`
   - Component-specific → `tests/unit/[component]/conftest.py`
3. **Follow naming conventions**:
   - Descriptive names
   - Use underscores, not camelCase
   - Prefix with component if specific (e.g., `processor_context`)
4. **Update this document** when adding new fixtures
5. **Use function scope unless session/module scope needed**
6. **Follow NO MOCKS policy** - use real implementations

## Common Fixture Combinations

### For Entity Testing
```python
def test_entity_operations(entity_service, tenant_context):
    # Use entity_service to create real entities
    # Use tenant_context for tenant isolation
```

### For Processing Testing
```python
def test_processing(processing_service, processor_context, test_entities):
    # Use processing_service for real processing
    # Use processor_context for v2 processors
    # Use test_entities for pre-created test data
```

### For Multi-Tenant Testing
```python
def test_tenant_isolation(multi_tenant_context, entity_service):
    # Use multi_tenant_context for multiple tenants
    # Use entity_service to create entities in different tenants
```