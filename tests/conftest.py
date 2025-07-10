"""
Test fixtures for V2 framework unit tests.

This module provides shared test fixtures including database setup,
model factories, and common test utilities.
"""

import pytest
from sqlalchemy.orm import Session

from api_exchange_core.db import (
    DatabaseConfig,
    DatabaseManager,
    import_all_models,
)
from api_exchange_core.db.db_config import Base, initialize_db


@pytest.fixture(scope="session")
def db_config() -> DatabaseConfig:
    """Create SQLite in-memory database configuration for testing."""
    return DatabaseConfig(
        db_type="sqlite",
        database=":memory:",
        host="localhost",  # Required but not used for SQLite
        username="test",   # Required but not used for SQLite
        password="test",   # Required but not used for SQLite
        echo=False,
        development_mode=True,
    )


@pytest.fixture(scope="session")
def db_manager(db_config: DatabaseConfig) -> DatabaseManager:
    """Create and initialize database manager with all models."""
    # Import all models to ensure they're registered with SQLAlchemy
    import_all_models()
    
    # Initialize the database
    manager = initialize_db(db_config)
    
    return manager


@pytest.fixture(scope="function")
def db_session(db_manager: DatabaseManager) -> Session:
    """
    Create a database session for each test.
    
    This fixture creates a fresh database session for each test function,
    ensuring test isolation. The session is automatically closed after
    each test.
    """
    session = db_manager.get_session()
    
    # Create all tables for this test
    Base.metadata.create_all(db_manager.engine)
    
    yield session
    
    # Clean up after test
    session.rollback()
    session.close()
    
    # Drop all tables to ensure clean state
    Base.metadata.drop_all(db_manager.engine)


@pytest.fixture
def sample_tenant_id() -> str:
    """Standard tenant ID for testing."""
    return "test-tenant-123"


@pytest.fixture
def sample_pipeline_id() -> str:
    """Standard pipeline ID for testing."""
    return "test-pipeline-456"