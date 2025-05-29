"""
Shared fixtures conftest.py - Reusable test infrastructure.

This module provides shared fixtures and test data that can be used
across unit and integration tests. Focuses on example models and
common test scenarios.
"""

from decimal import Decimal

import pytest

from tests.fixtures.example_models import (
    ExampleCustomer,
    ExampleInventoryItem,
    ExampleOrder,
    create_example_customer_data,
    create_example_inventory_data,
    create_example_order_data,
)
from tests.fixtures.factories import (
    CompleteOrderWorkflowFactory,
    ExampleCustomerEntityFactory,
    ExampleInventoryEntityFactory,
    ExampleOrderEntityFactory,
    FixtureDataGenerator,
)

# ==================== EXAMPLE DATA FIXTURES ====================


@pytest.fixture(scope="function")
def example_order_data():
    """
    Standard example order data for testing.

    Returns dictionary of order data that can be used to create
    ExampleOrder instances or test validation logic.
    """
    return create_example_order_data()


@pytest.fixture(scope="function")
def example_inventory_data():
    """
    Standard example inventory data for testing.

    Returns dictionary of inventory data that can be used to create
    ExampleInventoryItem instances or test validation logic.
    """
    return create_example_inventory_data()


@pytest.fixture(scope="function")
def example_customer_data():
    """
    Standard example customer data for testing.

    Returns dictionary of customer data that can be used to create
    ExampleCustomer instances or test validation logic.
    """
    return create_example_customer_data()


# ==================== VALIDATED MODEL FIXTURES ====================


@pytest.fixture(scope="function")
def example_order(example_order_data):
    """
    Validated ExampleOrder model instance.

    Returns a fully validated ExampleOrder that can be used
    in tests that need valid canonical model instances.
    """
    return ExampleOrder(**example_order_data)


@pytest.fixture(scope="function")
def example_inventory(example_inventory_data):
    """
    Validated ExampleInventoryItem model instance.

    Returns a fully validated ExampleInventoryItem that can be used
    in tests that need valid canonical model instances.
    """
    return ExampleInventoryItem(**example_inventory_data)


@pytest.fixture(scope="function")
def example_customer(example_customer_data):
    """
    Validated ExampleCustomer model instance.

    Returns a fully validated ExampleCustomer that can be used
    in tests that need valid canonical model instances.
    """
    return ExampleCustomer(**example_customer_data)


# ==================== ENTITY FIXTURES ====================


@pytest.fixture(scope="function")
def example_order_entity(db_session, tenant_context, factory_session):
    """
    Create example order entity in database.

    Returns entity data as dictionary to prevent accidental modification.
    Uses the current tenant context automatically.
    """
    entity = ExampleOrderEntityFactory.create(tenant_id=tenant_context["id"])
    db_session.add(entity)
    db_session.commit()

    return {
        "id": entity.id,
        "logical_id": entity.logical_id,
        "tenant_id": entity.tenant_id,
        "entity_type": entity.entity_type,
        "external_id": entity.external_id,
        "state": entity.state,
        "version": entity.version,
        "content_hash": entity.content_hash,
        "attributes": entity.attributes.copy() if entity.attributes else {},
        "canonical_data": entity.canonical_data.copy() if entity.canonical_data else {},
    }


@pytest.fixture(scope="function")
def example_inventory_entity(db_session, tenant_context, factory_session):
    """
    Create example inventory entity in database.

    Returns entity data as dictionary to prevent accidental modification.
    """
    entity = ExampleInventoryEntityFactory.create(tenant_id=tenant_context["id"])
    db_session.add(entity)
    db_session.commit()

    return {
        "id": entity.id,
        "logical_id": entity.logical_id,
        "tenant_id": entity.tenant_id,
        "entity_type": entity.entity_type,
        "external_id": entity.external_id,
        "state": entity.state,
        "version": entity.version,
        "content_hash": entity.content_hash,
        "attributes": entity.attributes.copy() if entity.attributes else {},
        "canonical_data": entity.canonical_data.copy() if entity.canonical_data else {},
    }


@pytest.fixture(scope="function")
def example_customer_entity(db_session, tenant_context, factory_session):
    """
    Create example customer entity in database.

    Returns entity data as dictionary to prevent accidental modification.
    """
    entity = ExampleCustomerEntityFactory.create(tenant_id=tenant_context["id"])
    db_session.add(entity)
    db_session.commit()

    return {
        "id": entity.id,
        "logical_id": entity.logical_id,
        "tenant_id": entity.tenant_id,
        "entity_type": entity.entity_type,
        "external_id": entity.external_id,
        "state": entity.state,
        "version": entity.version,
        "content_hash": entity.content_hash,
        "attributes": entity.attributes.copy() if entity.attributes else {},
        "canonical_data": entity.canonical_data.copy() if entity.canonical_data else {},
    }


# ==================== WORKFLOW FIXTURES ====================


@pytest.fixture(scope="function")
def complete_order_workflow(db_session, tenant_context):
    """
    Create complete order processing workflow.

    Includes entity, state transitions, and optionally errors.
    Useful for integration testing of full workflows.
    """
    workflow_data = CompleteOrderWorkflowFactory.create(db_session, tenant_id=tenant_context["id"])

    # Convert to dictionary format for safety
    return {
        "tenant_id": workflow_data["tenant_id"],
        "entity": {
            "id": workflow_data["entity"].id,
            "logical_id": workflow_data["entity"].logical_id,
            "canonical_data": workflow_data["entity"].canonical_data.copy(),
        },
        "transitions": [
            {
                "id": t.id,
                "from_state": t.from_state,
                "to_state": t.to_state,
                "processor_name": t.processor_name,
                "transition_reason": t.transition_reason,
            }
            for t in workflow_data["transitions"]
        ],
        "error": (
            {
                "id": workflow_data["error"].id,
                "error_type_code": workflow_data["error"].error_type_code,
                "error_message": workflow_data["error"].error_message,
                "is_recoverable": workflow_data["error"].is_recoverable,
            }
            if workflow_data["error"]
            else None
        ),
    }


# ==================== PARAMETERIZED FIXTURES ====================


@pytest.fixture(params=["order", "inventory", "customer"])
def example_entity_type(request):
    """
    Parameterized fixture for testing across different entity types.

    Yields tuple of (entity_type_name, factory_class, data_function)
    for comprehensive entity type testing.
    """
    entity_configs = {
        "order": ("order", ExampleOrderEntityFactory, create_example_order_data),
        "inventory": ("inventory", ExampleInventoryEntityFactory, create_example_inventory_data),
        "customer": ("customer", ExampleCustomerEntityFactory, create_example_customer_data),
    }

    return entity_configs[request.param]


@pytest.fixture(params=[1, 5, 10])
def entity_batch_size(request):
    """
    Parameterized fixture for testing different batch sizes.

    Useful for testing pagination, bulk operations, etc.
    """
    return request.param


# ==================== VALIDATION TEST DATA ====================


@pytest.fixture(scope="function")
def invalid_order_data_samples():
    """
    Collection of invalid order data for validation testing.

    Returns list of (invalid_data, expected_error_type) tuples
    for comprehensive validation testing.
    """
    base_data = create_example_order_data()

    return [
        # Missing required fields
        ({}, "missing_required_field"),
        ({"order_id": "test"}, "missing_required_field"),
        # Invalid data types
        ({**base_data, "line_items": "not_a_list"}, "invalid_type"),
        ({**base_data, "order_total": "not_money"}, "invalid_type"),
        # Business rule violations
        ({**base_data, "line_items": []}, "business_rule_violation"),
        (
            {**base_data, "order_total": {"amount": Decimal("-100"), "currency_code": "USD"}},
            "business_rule_violation",
        ),
        # Invalid relationships
        (
            {
                **base_data,
                "subtotal": {"amount": Decimal("50"), "currency_code": "USD"},
                "order_total": {"amount": Decimal("100"), "currency_code": "USD"},
            },
            "calculation_error",
        ),
    ]


@pytest.fixture(scope="function")
def edge_case_data_samples():
    """
    Collection of edge case data for boundary testing.

    Tests boundary conditions, special characters, etc.
    """
    base_data = create_example_order_data()

    return [
        # Boundary values
        ({**base_data, "order_number": "A" * 255}, "max_length_string"),
        ({**base_data, "line_items": [base_data["line_items"][0]] * 100}, "max_line_items"),
        # Special characters
        ({**base_data, "customer_name": "Test Ñoño & Co. (€)"}, "unicode_characters"),
        ({**base_data, "special_instructions": "Line 1\nLine 2\tTabbed"}, "control_characters"),
        # Currency precision
        (
            {**base_data, "order_total": {"amount": Decimal("99.999"), "currency_code": "USD"}},
            "high_precision_decimal",
        ),
    ]


# ==================== SCENARIO GENERATORS ====================


@pytest.fixture(scope="function")
def multi_tenant_scenario(db_session):
    """
    Generate multi-tenant test scenario.

    Creates multiple tenants with data for testing tenant isolation.
    """
    return FixtureDataGenerator.multi_tenant_scenario(db_session)


@pytest.fixture(scope="function")
def order_processing_scenario(db_session, tenant_context):
    """
    Generate order processing scenario with multiple orders.

    Creates several orders in different states for workflow testing.
    """
    return FixtureDataGenerator.order_processing_scenario(
        db_session, tenant_id=tenant_context["id"]
    )


@pytest.fixture(scope="function")
def error_recovery_scenario(db_session, tenant_context):
    """
    Generate error recovery test scenario.

    Creates entity with recoverable error for testing error handling.
    """
    return FixtureDataGenerator.error_recovery_scenario(db_session, tenant_id=tenant_context["id"])
