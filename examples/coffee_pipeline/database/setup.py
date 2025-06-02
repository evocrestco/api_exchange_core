"""
Database setup script for Coffee Pipeline example.
Creates schema and seeds initial data including tenants.
"""
import os
import sys

# Add src to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', '..'))

from src.db.db_config import DatabaseConfig, DatabaseManager, import_all_models
from src.db.db_tenant_models import Tenant


def setup_database():
    """Create all tables and seed initial data."""
    # Create database config for coffee pipeline
    db_config = DatabaseConfig(
        db_type="postgres",
        host="localhost",
        port="5432",
        database="coffee_pipeline",
        username="coffee_admin",
        password="pretentious_password_123"
    )
    
    # Create database manager
    db_manager = DatabaseManager(db_config)
    
    # Import all models to register them
    import_all_models()
    
    # Create all tables
    db_manager.create_tables()
    print("Database schema created successfully!")
    
    # Get session for data seeding
    session = db_manager.get_session()
    
    try:
        # Check if coffee shop tenant already exists
        existing_tenant = session.query(Tenant).filter_by(tenant_id="coffee_shop").first()
        
        if not existing_tenant:
            # Create coffee shop tenant
            coffee_tenant = Tenant(
                tenant_id="coffee_shop",
                customer_name="Pretentious Coffee Shop",
                tenant_config={
                    "max_pretentiousness_score": 10.0,
                    "default_complexity_multiplier": 1.5,
                    "enable_barista_eye_rolls": True
                },
                is_active=True
            )
            session.add(coffee_tenant)
            session.commit()
            print("Created coffee shop tenant!")
        else:
            print("Coffee shop tenant already exists.")
            
    except Exception as e:
        session.rollback()
        print(f"Error seeding data: {e}")
        raise
    finally:
        db_manager.close_session(session)
    
    print("Database setup completed successfully!")


if __name__ == "__main__":
    setup_database()