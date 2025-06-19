"""
Factory Boy factories for generating consistent test data.

These factories create realistic test data using the example models,
ensuring consistency across all tests while allowing customization
for specific test scenarios.
"""

import factory
import factory.fuzzy

from api_exchange_core.db import EntityStateEnum, EntityTypeEnum, ErrorTypeEnum
from api_exchange_core.db import Entity
from api_exchange_core.db.db_error_models import ProcessingError
from api_exchange_core.db import StateTransition
from api_exchange_core.db import Tenant
from tests.fixtures.example_models import (
    create_example_customer_data,
    create_example_inventory_data,
    create_example_order_data,
)

# ==================== BASE FACTORIES ====================


class BaseFactory(factory.alchemy.SQLAlchemyModelFactory):
    """Base factory with common patterns."""

    class Meta:
        abstract = True
        sqlalchemy_session_persistence = "commit"


# ==================== TENANT FACTORIES ====================


class TenantFactory(BaseFactory):
    """Factory for creating test tenants."""

    class Meta:
        model = Tenant

    id = factory.Sequence(lambda n: f"test_tenant_{n}")
    name = factory.Faker("company")
    description = factory.Faker("catch_phrase")
    is_active = True
    config = factory.LazyFunction(
        lambda: {
            "hash_algorithm": "sha256",
            "enable_duplicate_detection": True,
            "max_retry_attempts": 3,
        }
    )


class StandardTenantFactory(TenantFactory):
    """Factory for standard test tenant used across tests."""

    id = "test_tenant"
    name = "Test Tenant"
    description = "Standard tenant for testing"


# ==================== ENTITY FACTORIES ====================


class EntityFactory(BaseFactory):
    """Factory for creating test entities."""

    class Meta:
        model = Entity

    id = factory.Faker("uuid4")
    logical_id = factory.SelfAttribute("id")
    tenant_id = "test_tenant"
    entity_type = EntityTypeEnum.ENTITY_A
    external_id = factory.Sequence(lambda n: f"ext_entity_{n}")
    state = EntityStateEnum.RECEIVED
    version = 1
    content_hash = factory.Faker("sha256")
    attributes = factory.LazyFunction(lambda: {"test_attribute": "test_value"})
    canonical_data = factory.LazyFunction(lambda: {"name": "Test Entity"})


class ExampleOrderEntityFactory(EntityFactory):
    """Factory for order entities using example order model."""

    entity_type = EntityTypeEnum.ENTITY_A  # Orders
    external_id = factory.Sequence(lambda n: f"order_{n}")
    canonical_data = factory.LazyFunction(create_example_order_data)

    @factory.post_generation
    def customize_order_data(obj, create, extracted, **kwargs):
        """Customize order data based on factory parameters."""
        if not create:
            return

        if extracted:
            # Update canonical_data with any provided overrides
            order_data = obj.canonical_data.copy()
            order_data.update(extracted)
            obj.canonical_data = order_data


class ExampleInventoryEntityFactory(EntityFactory):
    """Factory for inventory entities using example inventory model."""

    entity_type = EntityTypeEnum.ENTITY_B  # Inventory
    external_id = factory.Sequence(lambda n: f"inventory_{n}")
    canonical_data = factory.LazyFunction(create_example_inventory_data)


class ExampleCustomerEntityFactory(EntityFactory):
    """Factory for customer entities using example customer model."""

    entity_type = EntityTypeEnum.ENTITY_C  # Customers
    external_id = factory.Sequence(lambda n: f"customer_{n}")
    canonical_data = factory.LazyFunction(create_example_customer_data)


# ==================== STATE TRANSITION FACTORIES ====================


class StateTransitionFactory(BaseFactory):
    """Factory for creating state transitions."""

    class Meta:
        model = StateTransition

    id = factory.Faker("uuid4")
    tenant_id = "test_tenant"
    entity_id = factory.SubFactory(EntityFactory)
    from_state = EntityStateEnum.RECEIVED
    to_state = EntityStateEnum.PROCESSING
    transition_reason = "Automated processing"
    processor_name = "test_processor"
    transition_metadata = factory.LazyFunction(lambda: {"test_meta": "value"})


class OrderProcessingTransitionFactory(StateTransitionFactory):
    """Factory for order processing state transitions."""

    entity_id = factory.SubFactory(ExampleOrderEntityFactory)
    processor_name = "order_processor"
    transition_reason = "Order processing workflow"


# ==================== ERROR FACTORIES ====================


class ProcessingErrorFactory(BaseFactory):
    """Factory for creating processing errors."""

    class Meta:
        model = ProcessingError

    id = factory.Faker("uuid4")
    tenant_id = "test_tenant"
    entity_id = factory.SubFactory(EntityFactory)
    error_type_code = ErrorTypeEnum.VALIDATION
    processor_name = "test_processor"
    error_message = "Test error message"
    error_details = factory.LazyFunction(lambda: {"test_detail": "value"})
    is_recoverable = True
    retry_count = 0


class ValidationErrorFactory(ProcessingErrorFactory):
    """Factory for validation errors."""

    error_type_code = ErrorTypeEnum.VALIDATION
    processor_name = "validation_processor"
    error_message = "Validation failed"
    error_details = factory.LazyFunction(
        lambda: {"field": "test_field", "constraint": "required", "value": None}
    )
    is_recoverable = False


class ConnectionErrorFactory(ProcessingErrorFactory):
    """Factory for connection errors."""

    error_type_code = ErrorTypeEnum.CONNECTION
    processor_name = "external_adapter"
    error_message = "Connection failed"
    error_details = factory.LazyFunction(
        lambda: {"endpoint": "https://api.example.com", "status_code": 503, "retry_after": 60}
    )
    is_recoverable = True


# ==================== SPECIALIZED FACTORIES ====================


class CompleteOrderWorkflowFactory:
    """Factory for creating complete order processing workflow data."""

    @staticmethod
    def create(session, tenant_id="test_tenant", **kwargs):
        """Create a complete order workflow with entity, transitions, and optionally errors."""

        # Create tenant if it doesn't exist
        tenant = session.query(Tenant).filter_by(id=tenant_id).first()
        if not tenant:
            tenant = StandardTenantFactory.create(id=tenant_id)
            session.add(tenant)
            session.commit()

        # Create order entity
        order_entity = ExampleOrderEntityFactory.create(
            tenant_id=tenant_id, customize_order_data=kwargs.get("order_overrides", {})
        )
        session.add(order_entity)
        session.commit()

        # Create state transitions
        transitions = []
        states = [
            (EntityStateEnum.RECEIVED, EntityStateEnum.PROCESSING),
            (EntityStateEnum.PROCESSING, EntityStateEnum.VALIDATED),
            (EntityStateEnum.VALIDATED, EntityStateEnum.COMPLETED),
        ]

        for from_state, to_state in states:
            transition = OrderProcessingTransitionFactory.create(
                tenant_id=tenant_id,
                entity_id=order_entity.id,
                from_state=from_state,
                to_state=to_state,
            )
            transitions.append(transition)
            session.add(transition)

        # Create error if requested
        error = None
        if kwargs.get("include_error", False):
            error = ValidationErrorFactory.create(tenant_id=tenant_id, entity_id=order_entity.id)
            session.add(error)

        session.commit()

        return {
            "tenant_id": tenant_id,
            "entity": order_entity,
            "transitions": transitions,
            "error": error,
        }


# ==================== FACTORY CONFIGURATION ====================


def configure_factories(session):
    """Configure all factories to use the provided session."""
    factories = [
        TenantFactory,
        StandardTenantFactory,
        EntityFactory,
        ExampleOrderEntityFactory,
        ExampleInventoryEntityFactory,
        ExampleCustomerEntityFactory,
        StateTransitionFactory,
        OrderProcessingTransitionFactory,
        ProcessingErrorFactory,
        ValidationErrorFactory,
        ConnectionErrorFactory,
    ]

    for factory_class in factories:
        factory_class._meta.sqlalchemy_session = session


# ==================== FIXTURE DATA GENERATORS ====================


class FixtureDataGenerator:
    """Helper class for generating structured test data."""

    @staticmethod
    def multi_tenant_scenario(session, tenant_count=3):
        """Generate multi-tenant test scenario."""
        tenants = []
        for i in range(tenant_count):
            tenant = TenantFactory.create(id=f"tenant_{i}")
            session.add(tenant)
            tenants.append(tenant)

        session.commit()
        return tenants

    @staticmethod
    def order_processing_scenario(session, tenant_id="test_tenant", order_count=5):
        """Generate order processing test scenario."""
        entities = []
        for i in range(order_count):
            entity = ExampleOrderEntityFactory.create(
                tenant_id=tenant_id,
                external_id=f"order_{i}",
                customize_order_data={
                    "order_number": f"ORD-2024-{i:03d}",
                    "customer_name": f"Customer {i}",
                },
            )
            session.add(entity)
            entities.append(entity)

        session.commit()
        return entities

    @staticmethod
    def error_recovery_scenario(session, tenant_id="test_tenant"):
        """Generate error recovery test scenario."""
        # Create entity with error
        entity = ExampleOrderEntityFactory.create(tenant_id=tenant_id)
        session.add(entity)

        # Create recoverable error
        error = ConnectionErrorFactory.create(
            tenant_id=tenant_id, entity_id=entity.id, retry_count=2
        )
        session.add(error)

        session.commit()
        return {"entity": entity, "error": error}


# ==================== PARAMETER GENERATORS ====================


def generate_entity_type_parameters():
    """Generate parameterized test data for different entity types."""
    return [
        (EntityTypeEnum.ENTITY_A, ExampleOrderEntityFactory, create_example_order_data),
        (EntityTypeEnum.ENTITY_B, ExampleInventoryEntityFactory, create_example_inventory_data),
        (EntityTypeEnum.ENTITY_C, ExampleCustomerEntityFactory, create_example_customer_data),
    ]


def generate_error_type_parameters():
    """Generate parameterized test data for different error types."""
    return [
        (ErrorTypeEnum.VALIDATION, ValidationErrorFactory, False),  # Not recoverable
        (ErrorTypeEnum.CONNECTION, ConnectionErrorFactory, True),  # Recoverable
        (ErrorTypeEnum.MAPPING, ProcessingErrorFactory, True),  # Recoverable
    ]


def generate_state_transition_parameters():
    """Generate parameterized test data for state transitions."""
    return [
        (EntityStateEnum.RECEIVED, EntityStateEnum.PROCESSING, "start_processing"),
        (EntityStateEnum.PROCESSING, EntityStateEnum.VALIDATED, "validation_complete"),
        (EntityStateEnum.VALIDATED, EntityStateEnum.COMPLETED, "processing_complete"),
        (EntityStateEnum.PROCESSING, EntityStateEnum.VALIDATION_ERROR, "validation_failed"),
    ]
