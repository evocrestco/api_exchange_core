"""
Root conftest.py - Core database and session fixtures.

This module provides the fundamental testing infrastructure:
- SQLite database engine for fast, isolated testing
- Session management with automatic rollback
- Basic tenant setup for multi-tenant testing
"""

import os
import sys

import pytest

# Add the project root to the Python path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from decimal import Decimal

from sqlalchemy import JSON, create_engine, event
from sqlalchemy.dialects import postgresql
from sqlalchemy.orm import scoped_session, sessionmaker
from sqlalchemy.pool import StaticPool

# Replace PostgreSQL's JSONB with standard JSON type for SQLite compatibility
postgresql.JSONB = JSON

from src.context.tenant_context import TenantContext
from src.db.db_config import Base, DatabaseManager
from src.db.db_tenant_models import Tenant

# ==================== DATABASE FIXTURES ====================


@pytest.fixture(scope="session")
def test_engine():
    """
    Create database engine for testing.
    
    Uses SQLite by default for speed, but switches to PostgreSQL when
    APP_ENV=production (for testing features like pgcrypto that require PostgreSQL).
    """
    app_env = os.getenv('APP_ENV', 'development')
    
    if app_env == 'production':
        # Use PostgreSQL for production testing (pgcrypto, etc.)
        from dotenv import load_dotenv
        load_dotenv()
        
        db_url = f"postgresql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"
        engine = create_engine(db_url, echo=False)
    else:
        # Use in-memory SQLite for speed
        engine = create_engine(
            "sqlite:///:memory:",
            poolclass=StaticPool,
            connect_args={
                "check_same_thread": False,
            },
            echo=False,  # Set to True for SQL debugging
        )

        # Enable foreign key constraints in SQLite
        @event.listens_for(engine, "connect")
        def set_sqlite_pragma(dbapi_connection, connection_record):
            cursor = dbapi_connection.cursor()
            cursor.execute("PRAGMA foreign_keys=ON")
            cursor.close()

    # Create all tables
    Base.metadata.create_all(engine)

    return engine


@pytest.fixture(scope="session")
def postgres_engine():
    """
    Create PostgreSQL database engine for tests requiring PostgreSQL-specific features.
    
    This fixture is used for tests that need PostgreSQL features like pgcrypto.
    It always uses PostgreSQL regardless of APP_ENV setting.
    """
    from dotenv import load_dotenv
    load_dotenv(override=True)
    
    db_url = f"postgresql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"
    engine = create_engine(db_url, echo=False)
    
    # Create all tables
    Base.metadata.create_all(engine)
    
    return engine


@pytest.fixture(scope="function")
def postgres_db_session(postgres_engine):
    """
    Create PostgreSQL database session with automatic rollback.
    
    This fixture is used for tests that require PostgreSQL-specific features.
    Each test gets a fresh session that automatically rolls back all changes.
    """
    # Create connection and transaction
    connection = postgres_engine.connect()
    transaction = connection.begin()

    # Create session bound to this specific connection
    Session = sessionmaker(bind=connection)
    session = Session()

    # Start a savepoint for nested transaction support
    nested = connection.begin_nested()

    # Configure session events to restart savepoint on commit
    @event.listens_for(session, "after_transaction_end")
    def restart_savepoint(session, transaction):
        if transaction.nested and not transaction._parent.nested:
            # Restart savepoint after a commit
            nonlocal nested
            nested = connection.begin_nested()

    yield session

    # Cleanup - rollback everything
    session.close()
    if nested.is_active:
        nested.rollback()
    transaction.rollback()
    connection.close()


@pytest.fixture(scope="function")
def db_session(test_engine):
    """
    Create database session with automatic rollback.

    Each test gets a fresh session that automatically rolls back
    all changes at the end, ensuring test isolation.
    """
    # Create connection and transaction
    connection = test_engine.connect()
    transaction = connection.begin()

    # Create session bound to this specific connection
    Session = sessionmaker(bind=connection)
    session = Session()

    # Start a savepoint for nested transaction support
    nested = connection.begin_nested()

    # Configure session events to restart savepoint on commit
    @event.listens_for(session, "after_transaction_end")
    def restart_savepoint(session, transaction):
        if transaction.nested and not transaction._parent.nested:
            # Restart savepoint after a commit
            nonlocal nested
            nested = connection.begin_nested()

    yield session

    # Cleanup - rollback everything
    session.close()
    if nested.is_active:
        nested.rollback()
    transaction.rollback()
    connection.close()


@pytest.fixture(scope="function")
def db_manager(db_session):
    """
    Create test database manager.

    Provides the same interface as production DatabaseManager
    but uses the test session for isolation.
    """

    class TestDatabaseManager(DatabaseManager):
        def __init__(self, session):
            self._session = session

        def get_session(self):
            return self._session

        def close_session(self, session=None):
            # Don't actually close in tests - handled by fixture cleanup
            pass

    return TestDatabaseManager(db_session)


# ==================== TENANT FIXTURES ====================


@pytest.fixture(scope="function")
def test_tenant(db_session):
    """
    Create standard test tenant.

    Returns tenant data as dictionary to prevent accidental modification.
    """
    # Create tenant directly without factory
    tenant = Tenant(
        tenant_id="test_tenant",
        customer_name="Test Tenant",
        is_active=True,
        tenant_config={
            "hash_algorithm": {"value": "sha256", "updated_at": "2024-01-01T12:00:00Z"},
            "enable_duplicate_detection": {"value": True, "updated_at": "2024-01-01T12:00:00Z"},
            "max_retry_attempts": {"value": 3, "updated_at": "2024-01-01T12:00:00Z"},
        },
    )
    db_session.add(tenant)
    db_session.commit()

    # Return as dictionary to prevent direct object modification
    return {
        "id": tenant.tenant_id,
        "name": tenant.customer_name,
        "is_active": tenant.is_active,
        "config": tenant.tenant_config.copy() if tenant.tenant_config else {},
    }


@pytest.fixture(scope="function")
def tenant_context(test_tenant):
    """
    Set up tenant context for tests.

    Automatically sets and clears tenant context, ensuring
    clean state before and after each test.
    """
    # Clear any existing context
    TenantContext.clear_current_tenant()

    # Set test tenant context
    TenantContext.set_current_tenant(test_tenant["id"])

    yield test_tenant

    # Clean up context
    TenantContext.clear_current_tenant()


@pytest.fixture(scope="function")
def multi_tenant_context(db_session):
    """
    Create multiple tenants for testing tenant isolation.

    Returns list of tenant dictionaries for parameterized testing.
    """
    tenant_data = []

    for i in range(3):
        tenant = Tenant(
            tenant_id=f"test_tenant_{i}",
            customer_name=f"Test Tenant {i}",
            is_active=True,
            tenant_config={
                "test_config": {"value": f"value_{i}", "updated_at": "2024-01-01T12:00:00Z"}
            },
        )
        db_session.add(tenant)
        tenant_data.append(
            {
                "id": tenant.tenant_id,
                "name": tenant.customer_name,
                "is_active": tenant.is_active,
                "config": tenant.tenant_config.copy() if tenant.tenant_config else {},
            }
        )

    db_session.commit()

    # Ensure clean context
    TenantContext.clear_current_tenant()

    yield tenant_data

    # Clean up context
    TenantContext.clear_current_tenant()


@pytest.fixture(scope="function")
def postgres_test_tenant(postgres_db_session):
    """
    Create standard test tenant in PostgreSQL.

    Returns tenant data as dictionary to prevent accidental modification.
    """
    # Create tenant directly without factory
    tenant = Tenant(
        tenant_id="test_tenant",
        customer_name="Test Tenant",
        is_active=True,
        tenant_config={
            "hash_algorithm": {"value": "sha256", "updated_at": "2024-01-01T12:00:00Z"},
            "enable_duplicate_detection": {"value": True, "updated_at": "2024-01-01T12:00:00Z"},
            "max_retry_attempts": {"value": 3, "updated_at": "2024-01-01T12:00:00Z"},
        },
    )
    postgres_db_session.add(tenant)
    postgres_db_session.commit()

    # Return as dictionary to prevent direct object modification
    return {
        "id": tenant.tenant_id,
        "name": tenant.customer_name,
        "is_active": tenant.is_active,
        "config": tenant.tenant_config.copy() if tenant.tenant_config else {},
    }


@pytest.fixture(scope="function")
def postgres_multi_tenant_context(postgres_db_session):
    """
    Create multiple tenants in PostgreSQL for testing tenant isolation.

    Returns list of tenant dictionaries for parameterized testing.
    """
    tenant_data = []

    for i in range(3):
        tenant = Tenant(
            tenant_id=f"test_tenant_{i}",
            customer_name=f"Test Tenant {i}",
            is_active=True,
            tenant_config={
                "test_config": {"value": f"value_{i}", "updated_at": "2024-01-01T12:00:00Z"}
            },
        )
        postgres_db_session.add(tenant)
        tenant_data.append(
            {
                "id": tenant.tenant_id,
                "name": tenant.customer_name,
                "is_active": tenant.is_active,
                "config": tenant.tenant_config.copy() if tenant.tenant_config else {},
            }
        )

    postgres_db_session.commit()

    # Ensure clean context
    TenantContext.clear_current_tenant()

    yield tenant_data

    # Clean up context
    TenantContext.clear_current_tenant()


@pytest.fixture(scope="function")
def tenant_context_fixture(test_tenant):
    """
    Alias for test_tenant to match test naming conventions.

    Some tests use 'tenant_context_fixture' as the parameter name.
    """
    return test_tenant


@pytest.fixture(scope="function")
def tenant_with_config(db_session):
    """
    Create a tenant with specific configuration for testing config retrieval.

    Returns tuple of (tenant_id, config_key, config_value).
    """
    import uuid

    from src.schemas.tenant_schema import TenantConfigValue

    tenant_id = f"config-tenant-{uuid.uuid4()}"
    config_key = "test_setting"
    config_value = "test_config_value"

    tenant = Tenant(
        tenant_id=tenant_id,
        customer_name="Config Test Tenant",
        is_active=True,
        tenant_config={config_key: {"value": config_value, "updated_at": "2024-01-01T12:00:00Z"}},
    )
    db_session.add(tenant)
    db_session.commit()

    # Ensure clean context
    TenantContext.clear_current_tenant()

    yield (tenant_id, config_key, config_value)

    # Clean up context
    TenantContext.clear_current_tenant()


# ==================== EXAMPLE DATA HELPERS ====================


def create_example_order_data(**overrides):
    """Create example order data for testing."""
    base_data = {
        "order_id": "ord_12345",
        "external_order_id": "EXT_ORD_98765",
        "order_number": "ON-2024-001",
        "customer_id": "cust_98765",
        "customer_name": "John Doe",
        "customer_email": "john.doe@example.com",
        "billing_address": {
            "street_line_1": "123 Main St",
            "city": "Anytown",
            "state_province": "CA",
            "postal_code": "12345",
            "country_code": "US",
        },
        "shipping_address": {
            "street_line_1": "123 Main St",
            "city": "Anytown",
            "state_province": "CA",
            "postal_code": "12345",
            "country_code": "US",
        },
        "line_items": [
            {
                "line_number": 1,
                "product_id": "prod_123",
                "product_name": "Example Widget",
                "product_sku": "WDG-001",
                "quantity": 2,
                "unit_price": {"amount": Decimal("25.00"), "currency_code": "USD"},
                "line_total": {"amount": Decimal("50.00"), "currency_code": "USD"},
            }
        ],
        "subtotal": {"amount": Decimal("50.00"), "currency_code": "USD"},
        "tax_total": {"amount": Decimal("4.50"), "currency_code": "USD"},
        "shipping_total": {"amount": Decimal("5.99"), "currency_code": "USD"},
        "discount_total": {"amount": Decimal("0.00"), "currency_code": "USD"},
        "order_total": {"amount": Decimal("60.49"), "currency_code": "USD"},
        "source_system": "test_system",
    }

    # Apply any overrides
    base_data.update(overrides)
    return base_data


def create_example_inventory_data(**overrides):
    """Create example inventory data for testing."""
    base_data = {
        "inventory_id": "inv_12345",
        "product_id": "prod_123",
        "product_sku": "WDG-001",
        "product_name": "Example Widget",
        "location_id": "loc_001",
        "location_name": "Main Warehouse",
        "warehouse_id": "wh_001",
        "quantity_on_hand": 100,
        "quantity_reserved": 15,
        "quantity_allocated": 0,
        "quantity_on_order": 50,
        "reorder_point": 25,
        "source_system": "test_system",
    }

    base_data.update(overrides)
    return base_data


def create_example_customer_data(**overrides):
    """Create example customer data for testing."""
    base_data = {
        "customer_id": "cust_98765",
        "external_customer_id": "EXT_CUST_123",
        "first_name": "John",
        "last_name": "Doe",
        "display_name": "John Doe",
        "primary_email": "john.doe@example.com",
        "contact_info": {"phone": "+1-555-123-4567", "email": "john.doe@example.com"},
        "addresses": [
            {
                "street_line_1": "123 Main St",
                "city": "Anytown",
                "state_province": "CA",
                "postal_code": "12345",
                "country_code": "US",
            }
        ],
        "source_system": "test_system",
    }

    base_data.update(overrides)
    return base_data


# ==================== EXAMPLE DATA FIXTURES ====================


@pytest.fixture(scope="function")
def example_order_data():
    """Standard example order data for testing."""
    return create_example_order_data()


@pytest.fixture(scope="function")
def example_inventory_data():
    """Standard example inventory data for testing."""
    return create_example_inventory_data()


@pytest.fixture(scope="function")
def example_customer_data():
    """Standard example customer data for testing."""
    return create_example_customer_data()


# ==================== ENVIRONMENT FIXTURES ====================


@pytest.fixture(scope="function", autouse=True)
def clean_environment():
    """
    Clean environment variables before and after each test.

    Ensures tests don't interfere with each other through
    environment variable pollution.
    """
    # Store original values
    original_tenant_id = os.environ.get("TENANT_ID")
    original_db_url = os.environ.get("DATABASE_URL")

    # Clear test-related environment variables
    env_vars_to_clear = ["TENANT_ID", "DATABASE_URL", "AZURE_STORAGE_CONNECTION_STRING"]
    for var in env_vars_to_clear:
        if var in os.environ:
            del os.environ[var]

    yield

    # Restore original values
    if original_tenant_id:
        os.environ["TENANT_ID"] = original_tenant_id
    if original_db_url:
        os.environ["DATABASE_URL"] = original_db_url

    # Clear any test values that might have been set
    for var in env_vars_to_clear:
        if var in os.environ:
            del os.environ[var]


@pytest.fixture(scope="function")
def with_tenant_env(test_tenant):
    """
    Set TENANT_ID environment variable for tests that need it.

    Some components read tenant ID from environment variables.
    This fixture provides that capability with automatic cleanup.
    """
    original_value = os.environ.get("TENANT_ID")

    # Set test tenant ID
    os.environ["TENANT_ID"] = test_tenant["id"]

    yield test_tenant["id"]

    # Restore original value
    if original_value:
        os.environ["TENANT_ID"] = original_value
    elif "TENANT_ID" in os.environ:
        del os.environ["TENANT_ID"]


# ==================== UTILITY FIXTURES ====================


@pytest.fixture(scope="function")
def factory_session(db_session):
    """
    Provide session for any inline data creation in tests.

    This is a convenience fixture for tests that need to create
    test data directly without complex factory dependencies.
    """
    return db_session


# ==================== PYTEST CONFIGURATION ====================


def pytest_configure(config):
    """Configure pytest with custom markers."""
    config.addinivalue_line(
        "markers", "slow: marks tests as slow (deselect with '-m \"not slow\"')"
    )
    config.addinivalue_line("markers", "integration: marks tests as integration tests")
    config.addinivalue_line("markers", "unit: marks tests as unit tests")
    config.addinivalue_line("markers", "tenant_isolation: marks tests that verify tenant isolation")
    config.addinivalue_line("markers", "example: marks tests that demonstrate framework usage")


def pytest_collection_modifyitems(config, items):
    """Automatically mark tests based on their location."""
    for item in items:
        # Mark unit tests
        if "unit" in str(item.fspath):
            item.add_marker(pytest.mark.unit)

        # Mark integration tests
        if "integration" in str(item.fspath):
            item.add_marker(pytest.mark.integration)

        # Mark tenant isolation tests
        if "tenant" in item.name.lower() or "isolation" in item.name.lower():
            item.add_marker(pytest.mark.tenant_isolation)

        # Mark example tests
        if "example" in item.name.lower() or "examples" in str(item.fspath):
            item.add_marker(pytest.mark.example)
