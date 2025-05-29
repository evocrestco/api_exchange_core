"""
Unit test conftest.py - Component-specific fixtures.

This module provides fixtures specific to unit testing:
- Repository fixtures with test sessions
- Service fixtures with mocked dependencies (only when necessary)
- Component-specific test data
"""

from unittest.mock import Mock

import pytest

from src.repositories.entity_repository import EntityRepository
from src.repositories.processing_error_repository import ProcessingErrorRepository
from src.repositories.state_transition_repository import StateTransitionRepository
from src.repositories.tenant_repository import TenantRepository
from src.services.entity_service import EntityService
from src.services.processing_error_service import ProcessingErrorService
from src.services.state_tracking_service import StateTrackingService
from src.services.tenant_service import TenantService

# ==================== REPOSITORY FIXTURES ====================


@pytest.fixture(scope="function")
def entity_repository(db_manager):
    """Entity repository with test database manager."""
    return EntityRepository(db_manager)


@pytest.fixture(scope="function")
def tenant_repository(db_manager):
    """Tenant repository with test database manager."""
    return TenantRepository(db_manager)


@pytest.fixture(scope="function")
def state_transition_repository(db_manager):
    """State transition repository with test database manager."""
    return StateTransitionRepository(db_manager)


@pytest.fixture(scope="function")
def processing_error_repository(db_manager):
    """Processing error repository with test database manager."""
    return ProcessingErrorRepository(db_manager)


# ==================== SERVICE FIXTURES ====================


@pytest.fixture(scope="function")
def entity_service(entity_repository):
    """Entity service with test repository."""
    return EntityService(entity_repository)


@pytest.fixture(scope="function")
def tenant_service(tenant_repository):
    """Tenant service with test repository."""
    return TenantService(tenant_repository)


@pytest.fixture(scope="function")
def state_tracking_service(state_transition_repository):
    """State tracking service with test repository."""
    return StateTrackingService(state_transition_repository)


@pytest.fixture(scope="function")
def processing_error_service(processing_error_repository):
    """Processing error service with test repository."""
    return ProcessingErrorService(processing_error_repository)


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
