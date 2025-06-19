"""
Unit test conftest.py - Component-specific fixtures.

This module provides fixtures specific to unit testing:
- Repository fixtures with test sessions
- Service fixtures with mocked dependencies (only when necessary)
- Component-specific test data
"""

from unittest.mock import Mock

import pytest

from api_exchange_core.services.entity_service import EntityService
from api_exchange_core.services.processing_error_service import ProcessingErrorService
from api_exchange_core.services.state_tracking_service import StateTrackingService
from api_exchange_core.services.tenant_service import TenantService

# ==================== LEGACY REPOSITORY FIXTURES ====================
# These are kept for backward compatibility with existing tests


# ==================== SERVICE FIXTURES ====================


@pytest.fixture(scope="function")
def entity_service(db_session):
    """Entity service with test session."""
    return EntityService(session=db_session)


@pytest.fixture(scope="function")
def tenant_service(db_session):
    """Tenant service with test session."""
    return TenantService(session=db_session)


@pytest.fixture(scope="function")
def state_tracking_service(db_session):
    """State tracking service with test session."""
    return StateTrackingService(session=db_session)


@pytest.fixture(scope="function")
def processing_error_service(db_session):
    """Processing error service with test session."""
    return ProcessingErrorService(session=db_session)


@pytest.fixture(scope="function")
def processing_service(db_session):
    """Processing service with session-per-service pattern."""
    from api_exchange_core.processing import ProcessingService
    from api_exchange_core.processing.entity_attributes import EntityAttributeBuilder
    from api_exchange_core.processing.duplicate_detection import DuplicateDetectionService
    from api_exchange_core.services.entity_service import EntityService
    from api_exchange_core.utils.logger import get_logger
    
    # Create ProcessingService manually without using constructor
    processing_service = object.__new__(ProcessingService)
    
    # Set up services with test session
    processing_service.entity_service = EntityService(session=db_session)
    processing_service.duplicate_detection_service = DuplicateDetectionService(
        entity_service=EntityService(session=db_session)
    )
    processing_service.attribute_builder = EntityAttributeBuilder()
    processing_service.logger = get_logger()
    
    # Optional services (not initialized by default)
    processing_service.state_tracking_service = None
    processing_service.error_service = None
    
    return processing_service


@pytest.fixture(scope="function")
def processor_context(processing_service, state_tracking_service, processing_error_service):
    """ProcessorContext with all services for v2 processor testing."""
    from api_exchange_core.processors.v2.processor_interface import ProcessorContext
    
    return ProcessorContext(
        processing_service=processing_service,
        state_tracking_service=state_tracking_service,
        error_service=processing_error_service
    )


# ==================== MOCK FIXTURES (ONLY WHEN NECESSARY) ====================


@pytest.fixture(scope="function")
def mock_azure_queue_client():
    """
    Mock Azure Queue Client for testing queue operations.

    Only use this when testing queue-dependent functionality
    without requiring actual Azure infrastructure.
    """
    mock_client = Mock()
    mock_client.send_message.return_value = Mock(id="test_message_id")
    mock_client.receive_messages.return_value = []
    return mock_client


@pytest.fixture(scope="function")
def mock_http_client():
    """
    Mock HTTP client for testing external API interactions.

    Only use this when testing HTTP-dependent functionality
    without requiring actual external services.
    """
    mock_client = Mock()
    mock_response = Mock()
    mock_response.status_code = 200
    mock_response.json.return_value = {"status": "success"}
    mock_client.get.return_value = mock_response
    mock_client.post.return_value = mock_response
    return mock_client


# ==================== ENTITY FIXTURES ====================


@pytest.fixture(scope="function")
def test_entities(entity_service, tenant_context):
    """
    Create test entities for all test modules.
    
    Creates entities with various prefixes to support different test modules:
    - error_ent_* for processing error repository tests
    - ent_* for state transition repository tests  
    - service_ent_* for service tests
    """
    entities = {}

    # All entity configurations consolidated from different test files
    entity_configs = [
        # Processing error repository entities
        ("error_ent_12345", "test_order_001", "order"),
        ("error_ent_minimal", "test_order_002", "order"),
        ("error_ent_complex", "test_order_003", "order"),
        ("error_ent_bulk_test", "test_order_004", "order"),
        ("error_ent_filter_test", "test_order_005", "order"),
        ("error_ent_isolation_test", "test_order_006", "order"),
        ("error_ent_delete_test", "test_order_007", "order"),
        
        # State transition repository entities
        ("ent_12345", "test_order_101", "order"),
        ("ent_minimal", "test_order_102", "order"),
        ("ent_complex", "test_order_103", "order"),
        ("ent_sequence_test", "test_order_104", "order"),
        ("ent_context_test", "test_order_105", "order"),
        ("ent_get_test", "test_order_106", "order"),
        ("ent_filter_test", "test_order_107", "order"),
        ("ent_workflow_test", "test_order_108", "order"),
        ("ent_shared_id", "test_order_109", "order"),
        ("ent_no_tenant", "test_order_110", "order"),
        ("ent_db_error_test", "test_order_111", "order"),
        
        # Service test entities
        ("service_ent_12345", "test_order_201", "order"),
        ("service_ent_minimal", "test_order_202", "order"),
        ("service_ent_complex", "test_order_203", "order"),
        ("service_ent_bulk_test", "test_order_204", "order"),
        ("service_ent_filter_test", "test_order_205", "order"),
        ("service_ent_isolation_test", "test_order_206", "order"),
        ("service_ent_delete_test", "test_order_207", "order"),
        ("service_ent_workflow_test", "test_order_208", "order"),
        ("service_ent_logging_test", "test_order_209", "order"),
        ("service_ent_convenience_test", "test_order_210", "order"),
        
        # State tracking service entities
        ("entity_record_test", "test_order_301", "order"),
        ("entity_history_test", "test_order_302", "order"),
        ("entity_current_state", "test_order_303", "order"),
        ("entity_stuck_test", "test_order_304", "order"),
        ("entity_stats_test1", "test_order_305", "order"),
        ("entity_stats_test2", "test_order_306", "order"),
        ("entity_processing_time", "test_order_307", "order"),
        ("entity_isolation_test", "test_order_308", "order"),
    ]

    for entity_id_suffix, external_id, canonical_type in entity_configs:
        created_entity_id = entity_service.create_entity(
            external_id=external_id,
            canonical_type=canonical_type,
            source="test_system",
            attributes={"status": "NEW", "test": True},
        )
        entities[entity_id_suffix] = created_entity_id

    return entities


# ==================== COMPONENT-SPECIFIC TEST DATA ====================


@pytest.fixture(scope="function")
def hash_test_data():
    """Test data for hash utility testing."""
    return {
        "simple_dict": {"key": "value", "number": 42},
        "nested_dict": {"level1": {"level2": {"key": "value"}, "list": [1, 2, 3]}},
        "list_data": [{"id": 1, "name": "Item 1"}, {"id": 2, "name": "Item 2"}],
    }


@pytest.fixture(scope="function")
def json_test_data():
    """Test data for JSON utility testing."""
    from datetime import datetime
    from decimal import Decimal

    return {
        "string": "test string",
        "number": 42,
        "decimal": Decimal("99.99"),
        "datetime": datetime(2024, 1, 1, 12, 0, 0),
        "nested": {"list": [1, 2, 3], "bool": True, "none": None},
    }


@pytest.fixture(scope="function")
def context_test_data():
    """Test data for context management testing."""
    return {
        "tenant_ids": ["tenant_1", "tenant_2", "tenant_3"],
        "operation_names": ["test_operation", "another_operation"],
        "operation_metadata": {
            "user_id": "test_user",
            "request_id": "req_123",
            "start_time": "2024-01-01T12:00:00Z",
        },
    }
