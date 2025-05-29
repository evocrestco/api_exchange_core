"""
Integration test conftest.py - Cross-component fixtures.

This module provides fixtures for integration testing:
- Full service stack with real dependencies
- End-to-end workflow setups
- Cross-component test scenarios
"""

import pytest

from src.db.db_config import DatabaseManager

# ==================== FULL STACK FIXTURES ====================


@pytest.fixture(scope="function")
def full_service_stack(db_manager):
    """
    Complete service stack for integration testing.

    Provides all services configured with the same database manager
    for testing cross-service interactions.
    """
    from src.repositories.entity_repository import EntityRepository
    from src.repositories.processing_error_repository import ProcessingErrorRepository
    from src.repositories.state_transition_repository import StateTransitionRepository
    from src.repositories.tenant_repository import TenantRepository
    from src.services.entity_service import EntityService
    from src.services.processing_error_service import ProcessingErrorService
    from src.services.state_tracking_service import StateTrackingService
    from src.services.tenant_service import TenantService

    # Create repositories
    entity_repo = EntityRepository(db_manager)
    tenant_repo = TenantRepository(db_manager)
    state_repo = StateTransitionRepository(db_manager)
    error_repo = ProcessingErrorRepository(db_manager)

    # Create services
    entity_service = EntityService(entity_repo)
    tenant_service = TenantService(tenant_repo)
    state_service = StateTrackingService(state_repo)
    error_service = ProcessingErrorService(error_repo)

    return {
        "repositories": {
            "entity": entity_repo,
            "tenant": tenant_repo,
            "state_transition": state_repo,
            "processing_error": error_repo,
        },
        "services": {
            "entity": entity_service,
            "tenant": tenant_service,
            "state_tracking": state_service,
            "processing_error": error_service,
        },
    }


@pytest.fixture(scope="function")
def processing_pipeline_setup(full_service_stack, tenant_context):
    """
    Setup for testing complete processing pipelines.

    Provides services and initial data for end-to-end workflow testing.
    """
    from tests.fixtures.factories import ExampleOrderEntityFactory

    services = full_service_stack["services"]

    # Create test entity for processing
    entity = ExampleOrderEntityFactory.create(tenant_id=tenant_context["id"])

    return {
        "services": services,
        "tenant_id": tenant_context["id"],
        "test_entity": {
            "id": entity.id,
            "logical_id": entity.logical_id,
            "canonical_data": entity.canonical_data.copy(),
        },
    }


# ==================== WORKFLOW FIXTURES ====================


@pytest.fixture(scope="function")
def error_handling_workflow(full_service_stack, tenant_context):
    """
    Setup for testing error handling workflows.

    Creates scenarios with errors for testing error recovery,
    retry logic, and error reporting.
    """
    from tests.fixtures.factories import (
        ConnectionErrorFactory,
        ExampleOrderEntityFactory,
        ValidationErrorFactory,
    )

    services = full_service_stack["services"]

    # Create entity with validation error
    entity_with_error = ExampleOrderEntityFactory.create(tenant_id=tenant_context["id"])
    validation_error = ValidationErrorFactory.create(
        tenant_id=tenant_context["id"], entity_id=entity_with_error.id
    )

    # Create entity with recoverable error
    entity_with_recoverable_error = ExampleOrderEntityFactory.create(tenant_id=tenant_context["id"])
    connection_error = ConnectionErrorFactory.create(
        tenant_id=tenant_context["id"], entity_id=entity_with_recoverable_error.id
    )

    return {
        "services": services,
        "tenant_id": tenant_context["id"],
        "validation_error_scenario": {
            "entity_id": entity_with_error.id,
            "error_id": validation_error.id,
            "is_recoverable": False,
        },
        "connection_error_scenario": {
            "entity_id": entity_with_recoverable_error.id,
            "error_id": connection_error.id,
            "is_recoverable": True,
        },
    }


@pytest.fixture(scope="function")
def multi_tenant_workflow(full_service_stack, multi_tenant_context):
    """
    Setup for testing multi-tenant workflows.

    Creates data across multiple tenants for testing tenant isolation
    and cross-tenant operations.
    """
    from tests.fixtures.factories import ExampleOrderEntityFactory

    services = full_service_stack["services"]

    # Create entities for each tenant
    tenant_entities = {}
    for tenant in multi_tenant_context:
        entity = ExampleOrderEntityFactory.create(tenant_id=tenant["id"])
        tenant_entities[tenant["id"]] = {
            "entity": {
                "id": entity.id,
                "logical_id": entity.logical_id,
                "canonical_data": entity.canonical_data.copy(),
            },
            "tenant_info": tenant,
        }

    return {
        "services": services,
        "tenant_entities": tenant_entities,
        "tenant_list": multi_tenant_context,
    }


# ==================== PERFORMANCE TEST FIXTURES ====================


@pytest.fixture(scope="function")
def performance_test_data(full_service_stack, tenant_context):
    """
    Generate data for performance testing.

    Creates larger datasets for testing performance characteristics
    of repositories and services.
    """
    from tests.fixtures.factories import FixtureDataGenerator

    # Generate multiple orders for performance testing
    entities = FixtureDataGenerator.order_processing_scenario(
        full_service_stack["repositories"]["entity"].db_manager.get_session(),
        tenant_id=tenant_context["id"],
        order_count=50,  # Larger dataset for performance testing
    )

    return {
        "services": full_service_stack["services"],
        "tenant_id": tenant_context["id"],
        "entity_count": len(entities),
        "entities": [
            {"id": entity.id, "logical_id": entity.logical_id, "state": entity.state}
            for entity in entities
        ],
    }


# ==================== UTILITY FIXTURES ====================


@pytest.fixture(scope="function")
def integration_test_context():
    """
    Context information for integration tests.

    Provides metadata and configuration for integration test scenarios.
    """
    return {
        "test_mode": "integration",
        "database_type": "sqlite",
        "enable_logging": False,
        "cleanup_after_test": True,
        "max_entities_per_test": 100,
        "timeout_seconds": 30,
    }
