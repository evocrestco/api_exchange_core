#!/usr/bin/env python3
"""
Setup script for integration test database.

This script:
1. Creates/recreates the test database
2. Creates all necessary tables
3. Seeds initial test data
4. Can be run to reset the database to a clean state
"""

import os
import sys
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from src.db.db_config import Base, import_all_models
from src.db.db_tenant_models import Tenant


def get_db_config():
    """Get database configuration from environment."""
    # Load integration test .env file
    env_file = Path(__file__).parent / ".env"
    if env_file.exists():
        with open(env_file) as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith('#') and '=' in line:
                    key, value = line.split('=', 1)
                    os.environ[key] = value
    
    return {
        'host': os.getenv('DB_HOST', 'localhost'),
        'port': os.getenv('DB_PORT', '15432'),
        'database': os.getenv('DB_NAME', 'testdb'),
        'user': os.getenv('DB_USER', 'testuser'),
        'password': os.getenv('DB_PASSWORD', 'testpassword')
    }


def create_database_if_not_exists(config):
    """Create the database if it doesn't exist."""
    # Connect to PostgreSQL server (not specific database)
    conn = psycopg2.connect(
        host=config['host'],
        port=config['port'],
        user=config['user'],
        password=config['password'],
        database='postgres'  # Connect to default postgres database
    )
    conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    cursor = conn.cursor()
    
    # Check if database exists
    cursor.execute(
        "SELECT 1 FROM pg_database WHERE datname = %s",
        (config['database'],)
    )
    exists = cursor.fetchone()
    
    if not exists:
        print(f"Creating database '{config['database']}'...")
        cursor.execute(f"CREATE DATABASE {config['database']}")
    else:
        print(f"Database '{config['database']}' already exists.")
    
    cursor.close()
    conn.close()


def drop_all_tables(engine):
    """Drop all tables in the database."""
    print("Dropping all existing tables...")
    Base.metadata.drop_all(engine)


def create_all_tables(engine):
    """Create all tables in the database."""
    print("Creating all tables...")
    # Import all models to ensure they're registered
    import_all_models()
    Base.metadata.create_all(engine)


def seed_test_data(engine):
    """Seed initial test data."""
    Session = sessionmaker(bind=engine)
    session = Session()
    
    try:
        print("Seeding test data...")
        
        # Create test tenants
        tenants = [
            Tenant(
                tenant_id="test_tenant",
                customer_name="Test Tenant",
                is_active=True,
                tenant_config={
                    "hash_algorithm": {"value": "sha256", "updated_at": "2024-01-01T12:00:00Z"},
                    "enable_duplicate_detection": {"value": True, "updated_at": "2024-01-01T12:00:00Z"},
                    "max_retry_attempts": {"value": 3, "updated_at": "2024-01-01T12:00:00Z"},
                }
            ),
            Tenant(
                tenant_id="integration_test_tenant",
                customer_name="Integration Test Tenant",
                is_active=True,
                tenant_config={
                    "test_mode": {"value": True, "updated_at": "2024-01-01T12:00:00Z"},
                }
            ),
            Tenant(
                tenant_id="e2e_test_tenant",
                customer_name="E2E Test Tenant",
                is_active=True,
                tenant_config={
                    "e2e_mode": {"value": True, "updated_at": "2024-01-01T12:00:00Z"},
                }
            )
        ]
        
        for tenant in tenants:
            # Check if tenant already exists
            existing = session.query(Tenant).filter_by(tenant_id=tenant.tenant_id).first()
            if not existing:
                session.add(tenant)
                print(f"  Created tenant: {tenant.tenant_id}")
            else:
                print(f"  Tenant already exists: {tenant.tenant_id}")
        
        session.commit()
        print("Test data seeded successfully!")
        
    except Exception as e:
        session.rollback()
        print(f"Error seeding test data: {e}")
        raise
    finally:
        session.close()


def setup_database(reset=False):
    """
    Set up the integration test database.
    
    Args:
        reset: If True, drop and recreate all tables
    """
    config = get_db_config()
    
    print(f"Setting up integration test database...")
    print(f"  Host: {config['host']}")
    print(f"  Port: {config['port']}")
    print(f"  Database: {config['database']}")
    print(f"  User: {config['user']}")
    
    # Create database if it doesn't exist
    create_database_if_not_exists(config)
    
    # Create engine for the specific database
    db_url = f"postgresql://{config['user']}:{config['password']}@{config['host']}:{config['port']}/{config['database']}"
    engine = create_engine(db_url, echo=False)
    
    if reset:
        # Drop all tables for a clean slate
        drop_all_tables(engine)
    
    # Create all tables
    create_all_tables(engine)
    
    # Seed test data
    seed_test_data(engine)
    
    print("\nDatabase setup complete!")
    
    # Test the connection
    try:
        from sqlalchemy import text
        with engine.connect() as conn:
            result = conn.execute(text("SELECT COUNT(*) FROM tenant"))
            count = result.scalar()
            print(f"Tenant table has {count} records.")
    except Exception as e:
        print(f"Error testing connection: {e}")
    
    engine.dispose()


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Setup integration test database")
    parser.add_argument(
        "--reset", 
        action="store_true", 
        help="Drop and recreate all tables (WARNING: This will delete all data!)"
    )
    
    args = parser.parse_args()
    
    if args.reset:
        response = input("WARNING: This will drop all tables and delete all data. Continue? (yes/no): ")
        if response.lower() != 'yes':
            print("Aborted.")
            sys.exit(0)
    
    setup_database(reset=args.reset)