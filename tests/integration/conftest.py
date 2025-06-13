"""
Integration test configuration.

This module provides fixtures and configuration specific to integration tests.
It builds on the base conftest.py but adds integration-specific setup.
"""

import json
import os
import time
from pathlib import Path

import pytest

# Azure Storage imports for real queue operations
from azure.storage.queue import QueueClient, QueueMessage

# Integration tests need to import all models for proper database setup
from src.db.db_config import import_all_models

# Import base fixtures from root conftest
from tests.conftest import *  # noqa: F403, F401


@pytest.fixture(scope="session", autouse=True)
def setup_integration_environment():
    """
    Set up environment for ALL integration tests.
    
    This fixture:
    1. Saves the current environment
    2. Loads the integration .env file
    3. Ensures all integration tests use the same environment
    4. Restores the original environment when done
    """
    # Save original environment
    original_env = dict(os.environ)
    
    try:
        # Load integration test .env file
        env_file = Path(__file__).parent / ".env"
        print(f"DEBUG: Loading integration .env from: {env_file}")
        
        if env_file.exists():
            # Manual .env loading to avoid dotenv dependency conflicts
            with open(env_file) as f:
                for line in f:
                    line = line.strip()
                    if line and not line.startswith('#') and '=' in line:
                        key, value = line.split('=', 1)
                        os.environ[key] = value
                        print(f"DEBUG: Set {key}={value[:20]}...")  # Show first 20 chars
        
        print(f"DEBUG: Integration environment loaded - TENANT_ID={os.getenv('TENANT_ID')}")
        print(f"DEBUG: Integration environment loaded - DB_NAME={os.getenv('DB_NAME')}")
        
        yield
        
    finally:
        # Restore original environment
        print("DEBUG: Restoring original environment...")
        os.environ.clear()
        os.environ.update(original_env)


@pytest.fixture(scope="session", autouse=True)
def setup_models_and_testing_mode():
    """
    Set up models and testing mode for integration tests.
    
    This fixture runs once per test session and ensures:
    - All database models are imported
    - Testing mode is set
    """
    # Import all models to ensure they're registered
    import_all_models()
    
    # Set integration test environment markers
    os.environ["TESTING_MODE"] = "integration"
    
    yield
    
    # Cleanup
    if "TESTING_MODE" in os.environ:
        del os.environ["TESTING_MODE"]


@pytest.fixture(scope="function", autouse=True)
def clean_environment():
    """
    Override the base clean_environment fixture for integration tests.
    
    Integration tests need to preserve environment variables loaded from .env file,
    so we don't clear them like the base fixture does.
    """
    # Just yield without clearing - integration tests need the env vars
    yield
    # No cleanup - we want to keep the env vars for the whole test session


# ==================== POSTGRESQL FIXTURES ====================


@pytest.fixture(scope="session")
def postgres_engine():
    """
    Create PostgreSQL database engine for integration tests.
    
    This fixture uses the environment variables set by setup_integration_environment.
    """
    # Use environment variables that should already be set by setup_integration_environment
    db_url = f"postgresql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"
    
    if not all([os.getenv('DB_USER'), os.getenv('DB_PASSWORD'), os.getenv('DB_HOST'), os.getenv('DB_PORT'), os.getenv('DB_NAME')]):
        raise ValueError("PostgreSQL environment variables not set. Check integration .env file.")
    
    from sqlalchemy import create_engine
    from src.db.db_config import Base
    
    engine = create_engine(db_url, echo=False)
    
    # Create all tables
    Base.metadata.create_all(engine)
    
    yield engine


@pytest.fixture(scope="function")
def postgres_db_session(postgres_engine):
    """
    Create PostgreSQL database session with automatic rollback.
    
    This fixture is used for integration tests that require PostgreSQL-specific features.
    Each test gets a fresh session that automatically rolls back all changes.
    """
    from sqlalchemy.orm import sessionmaker
    from sqlalchemy import event
    
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
def postgres_test_tenant(postgres_db_session):
    """
    Create standard test tenant in PostgreSQL.

    Returns tenant data as dictionary to prevent accidental modification.
    """
    from src.db.db_tenant_models import Tenant
    
    tenant_id = "test_tenant"
    
    # Check if tenant already exists
    existing_tenant = postgres_db_session.query(Tenant).filter(
        Tenant.tenant_id == tenant_id
    ).first()
    
    if existing_tenant:
        # Return existing tenant data
        return {
            "id": existing_tenant.tenant_id,
            "name": existing_tenant.customer_name,
            "is_active": existing_tenant.is_active,
            "config": existing_tenant.tenant_config.copy() if existing_tenant.tenant_config else {},
        }
    
    # Create tenant if it doesn't exist
    tenant = Tenant(
        tenant_id=tenant_id,
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
    from src.db.db_tenant_models import Tenant
    from src.context.tenant_context import TenantContext
    
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
def integration_db_session(db_session):
    """
    Database session specifically for integration tests.
    
    This is an alias for the base db_session fixture but makes it clear
    that integration tests are using the database.
    """
    return db_session


@pytest.fixture(scope="function") 
def integration_tenant_context(tenant_context):
    """
    Tenant context specifically for integration tests.
    
    This is an alias for the base tenant_context fixture.
    """
    return tenant_context


# Integration-specific markers
pytest_markers = [
    "integration: marks tests as integration tests",
    "database: marks tests that require database",
    "processor: marks tests that test processor functionality",
    "queue: marks tests that involve queue operations",
]


def pytest_configure(config):
    """Configure pytest with integration-specific markers."""
    for marker in pytest_markers:
        config.addinivalue_line("markers", marker)


def pytest_collection_modifyitems(config, items):
    """Automatically mark integration tests."""
    for item in items:
        # Mark all tests in integration folder
        if "integration" in str(item.fspath):
            item.add_marker(pytest.mark.integration)
            item.add_marker(pytest.mark.database)
        
        # Mark processor tests
        if "processor" in item.name.lower() or "hello_world" in item.name.lower():
            item.add_marker(pytest.mark.processor)
        
        # Mark queue-related tests
        if "queue" in item.name.lower() or "output" in item.name.lower():
            item.add_marker(pytest.mark.queue)


# ==================== AZURE STORAGE QUEUE FIXTURES ====================


@pytest.fixture(scope="session")
def azure_storage_connection_string():
    """Get Azure Storage connection string from environment."""
    connection_string = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
    if not connection_string:
        pytest.skip("AZURE_STORAGE_CONNECTION_STRING not configured")
    return connection_string


@pytest.fixture(scope="function")
def dead_letter_queue_client(azure_storage_connection_string):
    """
    Create real dead letter queue client for testing.
    
    Uses real Azure Storage (Azurite) - no mocks.
    Auto-creates queue if it doesn't exist.
    Cleans up test messages after each test.
    """
    queue_name = "test-dead-letter"
    
    # Create real Azure Storage queue client
    queue_client = QueueClient.from_connection_string(
        conn_str=azure_storage_connection_string,
        queue_name=queue_name
    )
    
    # Create queue if it doesn't exist
    try:
        queue_client.create_queue()
    except Exception:
        # Queue already exists
        pass
    
    # Track messages sent during test for cleanup
    sent_messages = []
    original_send = queue_client.send_message
    
    def tracked_send_message(content, **kwargs):
        result = original_send(content, **kwargs)
        sent_messages.append(result)
        return result
    
    queue_client.send_message = tracked_send_message
    queue_client._test_sent_messages = sent_messages
    
    yield queue_client
    
    # Cleanup: clear all messages from the queue
    try:
        queue_client.clear_messages()
    except Exception:
        # Queue might not exist or already cleared
        pass


@pytest.fixture(scope="function") 
def queue_message_verifier(dead_letter_queue_client):
    """
    Helper for verifying messages in real Azure Storage queues.
    
    Provides utilities to check queue contents and verify message structure.
    """
    def verify_dlq_message(expected_external_id, expected_error_message, timeout_seconds=5):
        """
        Verify a message exists in the dead letter queue with expected content.
        
        Args:
            expected_external_id: Expected external_id in the message
            expected_error_message: Expected error message content
            timeout_seconds: How long to wait for message to appear
            
        Returns:
            The parsed message content if found
            
        Raises:
            AssertionError: If message not found or content doesn't match
        """
        start_time = time.time()
        
        while time.time() - start_time < timeout_seconds:
            # Peek at messages in queue (don't dequeue them)
            messages = dead_letter_queue_client.peek_messages(max_messages=32)
            
            for message in messages:
                try:
                    content = json.loads(message.content)
                    
                    # Check if this is our expected message
                    if (content.get("original_message", {}).get("external_id") == expected_external_id and
                        expected_error_message in content.get("failure_info", {}).get("error_message", "")):
                        return content
                        
                except (json.JSONDecodeError, KeyError):
                    # Skip malformed messages
                    continue
            
            # Wait a bit before checking again
            time.sleep(0.1)
        
        # Message not found - get current queue contents for debugging
        current_messages = []
        try:
            messages = dead_letter_queue_client.peek_messages(max_messages=32)
            for msg in messages:
                try:
                    current_messages.append(json.loads(msg.content))
                except json.JSONDecodeError:
                    current_messages.append({"raw_content": msg.content})
        except Exception:
            current_messages = ["Failed to read queue contents"]
        
        raise AssertionError(
            f"Message with external_id='{expected_external_id}' and error_message containing "
            f"'{expected_error_message}' not found in dead letter queue within {timeout_seconds} seconds. "
            f"Current queue contents: {current_messages}"
        )
    
    def get_all_dlq_messages():
        """Get all messages currently in the dead letter queue."""
        messages = []
        try:
            queue_messages = dead_letter_queue_client.peek_messages(max_messages=32)
            for msg in queue_messages:
                try:
                    messages.append(json.loads(msg.content))
                except json.JSONDecodeError:
                    messages.append({"raw_content": msg.content, "parse_error": True})
        except Exception as e:
            messages.append({"error": f"Failed to read queue: {str(e)}"})
        return messages
    
    def assert_dlq_empty():
        """Assert that the dead letter queue is empty."""
        messages = get_all_dlq_messages()
        assert len(messages) == 0, f"Expected empty dead letter queue, but found {len(messages)} messages: {messages}"
    
    # Create verifier class instance  
    class QueueVerifier:
        def verify_dlq_message(self, expected_external_id, expected_error_message, timeout_seconds=5):
            return verify_dlq_message(expected_external_id, expected_error_message, timeout_seconds)
        
        def get_all_dlq_messages(self):
            return get_all_dlq_messages()
        
        def assert_dlq_empty(self):
            return assert_dlq_empty()
    
    return QueueVerifier()


@pytest.fixture(scope="function")
def output_queue_client(azure_storage_connection_string):
    """
    Create real output queue client for testing processor pipeline output.
    
    Uses real Azure Storage (Azurite) - no mocks.
    Auto-creates queue if it doesn't exist.
    Cleans up test messages after each test.
    """
    queue_name = "test-output-queue"
    
    # Create real Azure Storage queue client
    queue_client = QueueClient.from_connection_string(
        conn_str=azure_storage_connection_string,
        queue_name=queue_name
    )
    
    # Create queue if it doesn't exist
    try:
        queue_client.create_queue()
    except Exception:
        # Queue already exists
        pass
    
    # Track messages sent during test for cleanup
    sent_messages = []
    original_send = queue_client.send_message
    
    def tracked_send_message(content, **kwargs):
        result = original_send(content, **kwargs)
        sent_messages.append(result)
        return result
    
    queue_client.send_message = tracked_send_message
    queue_client._test_sent_messages = sent_messages
    
    yield queue_client
    
    # Cleanup: clear all messages from the queue
    try:
        queue_client.clear_messages()
    except Exception:
        # Queue might not exist or already cleared
        pass


@pytest.fixture(scope="function") 
def output_queue_verifier(output_queue_client):
    """
    Helper for verifying messages in real Azure Storage output queues.
    
    Provides utilities to check queue contents and verify message structure.
    """
    def verify_output_message(expected_external_id, timeout_seconds=5):
        """
        Verify a message exists in the output queue with expected content.
        
        Args:
            expected_external_id: Expected external_id in the message
            timeout_seconds: How long to wait for message to appear
            
        Returns:
            The parsed message content if found
            
        Raises:
            AssertionError: If message not found or content doesn't match
        """
        start_time = time.time()
        
        while time.time() - start_time < timeout_seconds:
            # Peek at messages in queue (don't dequeue them)
            messages = output_queue_client.peek_messages(max_messages=32)
            
            for message in messages:
                try:
                    content = json.loads(message.content)
                    
                    # Check if this is our expected message
                    if content.get("entity_reference", {}).get("external_id") == expected_external_id:
                        return content
                        
                except (json.JSONDecodeError, KeyError):
                    # Skip malformed messages
                    continue
            
            # Wait a bit before checking again
            time.sleep(0.1)
        
        # Message not found - get current queue contents for debugging
        current_messages = []
        try:
            messages = output_queue_client.peek_messages(max_messages=32)
            for msg in messages:
                try:
                    current_messages.append(json.loads(msg.content))
                except json.JSONDecodeError:
                    current_messages.append({"raw_content": msg.content})
        except Exception:
            current_messages = ["Failed to read queue contents"]
        
        raise AssertionError(
            f"Message with external_id='{expected_external_id}' not found in output queue "
            f"within {timeout_seconds} seconds. Current queue contents: {current_messages}"
        )
    
    def get_all_output_messages():
        """Get all messages currently in the output queue."""
        messages = []
        try:
            queue_messages = output_queue_client.peek_messages(max_messages=32)
            for msg in queue_messages:
                try:
                    messages.append(json.loads(msg.content))
                except json.JSONDecodeError:
                    messages.append({"raw_content": msg.content, "parse_error": True})
        except Exception as e:
            messages.append({"error": f"Failed to read queue: {str(e)}"})
        return messages
    
    def assert_output_queue_empty():
        """Assert that the output queue is empty."""
        messages = get_all_output_messages()
        assert len(messages) == 0, f"Expected empty output queue, but found {len(messages)} messages: {messages}"
    
    def get_message_count():
        """Get the current number of messages in the output queue."""
        try:
            messages = output_queue_client.peek_messages(max_messages=32)
            return len(list(messages))
        except Exception:
            return 0
    
    # Create verifier class instance  
    class OutputQueueVerifier:
        def verify_output_message(self, expected_external_id, timeout_seconds=5):
            return verify_output_message(expected_external_id, timeout_seconds)
        
        def get_all_output_messages(self):
            return get_all_output_messages()
        
        def assert_output_queue_empty(self):
            return assert_output_queue_empty()
            
        def get_message_count(self):
            return get_message_count()
    
    return OutputQueueVerifier()