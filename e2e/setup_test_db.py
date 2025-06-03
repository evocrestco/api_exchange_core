"""
Setup script for test harness database.

This script creates the schema and test data for the test harness.
The database itself is created by Docker Compose.
"""

import os
import sys
import time

# Add parent directories to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

from src.db.db_config import DatabaseConfig, DatabaseManager
from src.db.db_base import Base
# Import all models to ensure they're registered with Base.metadata
from src.db import db_entity_models, db_error_models, db_state_transition_models, db_tenant_models
from src.services.tenant_service import TenantService
from src.repositories.tenant_repository import TenantRepository
from src.schemas.tenant_schema import TenantCreate


def wait_for_db(db_manager, max_attempts=30):
    """Wait for database to be ready."""
    print("Waiting for database to be ready...")
    for attempt in range(max_attempts):
        try:
            with db_manager.get_session() as session:
                from sqlalchemy import text
                session.execute(text("SELECT 1"))
            print("Database is ready!")
            return True
        except Exception as e:
            if attempt < max_attempts - 1:
                print(f"  Attempt {attempt + 1}/{max_attempts} failed, retrying...")
                time.sleep(1)
            else:
                print(f"Database not ready after {max_attempts} attempts: {e}")
                return False
    return False


def create_schema():
    """Create database schema for test harness."""
    print("\nCreating database schema...")
    
    # Connect to test database using environment variables
    db_config = DatabaseConfig(
        db_type="postgres",
        host=os.getenv("DB_HOST", "localhost"),
        port=os.getenv("DB_PORT", "5432"),
        database=os.getenv("DB_NAME", "e2e_test"),
        username=os.getenv("DB_USER", "test_user"),
        password=os.getenv("DB_PASSWORD", "test_password")
    )
    
    db_manager = DatabaseManager(db_config)
    
    try:
        # Wait for database to be ready
        if not wait_for_db(db_manager):
            raise Exception("Database not available")
        
        # Drop all existing tables (clean slate)
        print("Dropping existing tables...")
        Base.metadata.drop_all(bind=db_manager.engine)
        
        # Import all models first (to resolve relationships)
        print("Importing models...")
        from src.db.db_config import import_all_models
        import_all_models()
        
        # Create all tables
        print("Creating tables...")
        Base.metadata.create_all(bind=db_manager.engine)
        print("Created all tables")
        
        # Create tenants using the TenantService
        tenant_repository = TenantRepository(db_manager=db_manager)
        tenant_service = TenantService(tenant_repository=tenant_repository)
        
        test_tenants = [
            TenantCreate(
                tenant_id=os.getenv("TENANT_ID", "e2e_test_tenant"),
                customer_name="E2E Test Tenant",
                tenant_config={"test": True},
                is_active=True
            ),
            TenantCreate(
                tenant_id="test_tenant_1",
                customer_name="Test Tenant 1",
                tenant_config={"test": True, "index": 1},
                is_active=True
            ),
            TenantCreate(
                tenant_id="test_tenant_2",
                customer_name="Test Tenant 2",
                tenant_config={"test": True, "index": 2},
                is_active=True
            ),
            TenantCreate(
                tenant_id="test_tenant_3",
                customer_name="Test Tenant 3",
                tenant_config={"test": True, "index": 3},
                is_active=True
            ),
        ]
        
        for tenant_data in test_tenants:
            tenant_service.create_tenant(tenant_data)
            print(f"Created tenant: {tenant_data.tenant_id}")
        
        print("Test tenants created successfully")
            
    finally:
        db_manager.close()


def main():
    """Main setup function."""
    print("=== Test Harness Database Setup ===\n")
    
    try:
        create_schema()
        print("\n✅ Test database setup completed successfully!")
        print("\nConnection Details:")
        print(f"  Database: {os.getenv('DB_NAME', 'e2e_test')}")
        print(f"  Username: {os.getenv('DB_USER', 'test_user')}")
        print(f"  Host: {os.getenv('DB_HOST', 'localhost')}")
        print(f"  Port: {os.getenv('DB_PORT', '5432')}")
        print("\nTest tenants created:")
        print(f"  - {os.getenv('TENANT_ID', 'e2e_test_tenant')} (default)")
        print("  - test_tenant_1")
        print("  - test_tenant_2")
        print("  - test_tenant_3")
        
    except Exception as e:
        print(f"\n❌ Setup failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()