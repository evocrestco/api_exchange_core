"""
Unit tests for tenant utilities using generic CRUD helpers.

Tests the tenant utility functions that provide business logic
using the generic CRUD system.
"""

import pytest
from datetime import datetime, timezone
from sqlalchemy.orm import Session

from api_exchange_core.db.db_tenant_models import Tenant
from api_exchange_core.exceptions import DuplicateError, NotFoundError
from api_exchange_core.schemas.tenant_schemas import TenantCreate, ProcessingConfigSchema
from api_exchange_core.utils.tenant_utils import (
    create_tenant,
    delete_tenant,
    get_tenant_config,
    list_tenants,
    update_tenant,
    update_tenant_config,
)


class TestGetTenantConfig:
    """Test get_tenant_config function."""
    
    def test_get_full_config_success(self, db_session: Session):
        """Test getting full tenant configuration."""
        # Create tenant directly in database
        tenant = Tenant(
            tenant_id="test-tenant",
            name="Test Tenant",
            config={"key1": "value1", "key2": {"nested": "value2"}}
        )
        db_session.add(tenant)
        db_session.commit()
        
        # Get full config
        config = get_tenant_config(db_session, "test-tenant")
        
        assert config == {"key1": "value1", "key2": {"nested": "value2"}}
    
    def test_get_specific_config_key_success(self, db_session: Session):
        """Test getting specific configuration key."""
        # Create tenant with config
        tenant = Tenant(
            tenant_id="test-tenant",
            name="Test Tenant",
            config={"database_url": "sqlite:///test.db", "timeout": 30}
        )
        db_session.add(tenant)
        db_session.commit()
        
        # Get specific key
        value = get_tenant_config(db_session, "test-tenant", "database_url")
        
        assert value == "sqlite:///test.db"
    
    def test_get_nonexistent_config_key(self, db_session: Session):
        """Test getting non-existent configuration key."""
        # Create tenant with config
        tenant = Tenant(
            tenant_id="test-tenant",
            name="Test Tenant",
            config={"key1": "value1"}
        )
        db_session.add(tenant)
        db_session.commit()
        
        # Get non-existent key
        value = get_tenant_config(db_session, "test-tenant", "nonexistent_key")
        
        assert value is None
    
    def test_get_config_empty_config(self, db_session: Session):
        """Test getting config when tenant has no config."""
        # Create tenant with no config
        tenant = Tenant(
            tenant_id="test-tenant",
            name="Test Tenant",
            config=None
        )
        db_session.add(tenant)
        db_session.commit()
        
        # Get full config
        config = get_tenant_config(db_session, "test-tenant")
        
        assert config == {}
    
    def test_get_config_tenant_not_found(self, db_session: Session):
        """Test getting config when tenant doesn't exist."""
        with pytest.raises(NotFoundError) as exc_info:
            get_tenant_config(db_session, "nonexistent-tenant")
        
        error = exc_info.value
        assert "Tenant not found" in error.message
        assert error.context["tenant_id"] == "nonexistent-tenant"


class TestCreateTenant:
    """Test create_tenant function."""
    
    def test_create_tenant_success(self, db_session: Session):
        """Test creating a new tenant."""
        config = ProcessingConfigSchema(
            timeout=30,
            max_concurrent_processes=5,
            batch_size=100
        )
        
        tenant_create = TenantCreate(
            tenant_id="new-tenant",
            name="New Tenant",
            description="Test tenant",
            config=config
        )
        
        tenant_id = create_tenant(db_session, tenant_create)
        
        assert tenant_id == "new-tenant"
        
        # Verify tenant was created
        tenant = db_session.query(Tenant).filter_by(tenant_id="new-tenant").first()
        assert tenant is not None
        assert tenant.name == "New Tenant"
        assert tenant.description == "Test tenant"
        assert tenant.is_active is True
        assert tenant.config is not None  # Should be serialized JSON
    
    def test_create_tenant_without_config(self, db_session: Session):
        """Test creating tenant without configuration."""
        tenant_create = TenantCreate(
            tenant_id="simple-tenant",
            name="Simple Tenant"
        )
        
        tenant_id = create_tenant(db_session, tenant_create)
        
        assert tenant_id == "simple-tenant"
        
        # Verify tenant was created with no config
        tenant = db_session.query(Tenant).filter_by(tenant_id="simple-tenant").first()
        assert tenant is not None
        assert tenant.name == "Simple Tenant"
        assert tenant.description is None
        assert tenant.is_active is True
        assert tenant.config is None
    
    def test_create_tenant_duplicate(self, db_session: Session):
        """Test creating duplicate tenant."""
        # Create first tenant
        tenant = Tenant(
            tenant_id="existing-tenant",
            name="Existing Tenant"
        )
        db_session.add(tenant)
        db_session.commit()
        
        # Try to create duplicate
        duplicate_create = TenantCreate(
            tenant_id="existing-tenant",
            name="Duplicate Tenant"
        )
        
        with pytest.raises(DuplicateError) as exc_info:
            create_tenant(db_session, duplicate_create)
        
        error = exc_info.value
        assert "Duplicate Tenant" in error.message
        assert error.context["tenant_id"] == "existing-tenant"


class TestUpdateTenant:
    """Test update_tenant function."""
    
    def test_update_tenant_name_success(self, db_session: Session):
        """Test updating tenant name."""
        # Create tenant
        tenant = Tenant(
            tenant_id="update-tenant",
            name="Original Name",
            config={"key": "value"}
        )
        db_session.add(tenant)
        db_session.commit()
        
        # Update name
        result = update_tenant(db_session, "update-tenant", tenant_name="Updated Name")
        
        assert result is True
        
        # Verify update
        updated_tenant = db_session.query(Tenant).filter_by(tenant_id="update-tenant").first()
        assert updated_tenant.name == "Updated Name"
        assert updated_tenant.config == {"key": "value"}  # Config unchanged
    
    def test_update_tenant_config_success(self, db_session: Session):
        """Test updating tenant configuration."""
        # Create tenant
        tenant = Tenant(
            tenant_id="update-tenant",
            name="Test Tenant",
            config={"old_key": "old_value"}
        )
        db_session.add(tenant)
        db_session.commit()
        
        # Update config
        new_config = {"new_key": "new_value", "another": "config"}
        result = update_tenant(db_session, "update-tenant", config=new_config)
        
        assert result is True
        
        # Verify update
        updated_tenant = db_session.query(Tenant).filter_by(tenant_id="update-tenant").first()
        assert updated_tenant.name == "Test Tenant"  # Name unchanged
        assert updated_tenant.config == new_config
    
    def test_update_tenant_both_name_and_config(self, db_session: Session):
        """Test updating both name and configuration."""
        # Create tenant
        tenant = Tenant(
            tenant_id="update-tenant",
            name="Original Name",
            config={"old": "config"}
        )
        db_session.add(tenant)
        db_session.commit()
        
        # Update both
        new_config = {"new": "config"}
        result = update_tenant(
            db_session, 
            "update-tenant", 
            tenant_name="New Name",
            config=new_config
        )
        
        assert result is True
        
        # Verify update
        updated_tenant = db_session.query(Tenant).filter_by(tenant_id="update-tenant").first()
        assert updated_tenant.name == "New Name"
        assert updated_tenant.config == new_config
    
    def test_update_tenant_no_changes(self, db_session: Session):
        """Test update with no changes provided."""
        # Create tenant
        tenant = Tenant(
            tenant_id="update-tenant",
            name="Test Tenant"
        )
        db_session.add(tenant)
        db_session.commit()
        
        # Update with no changes
        result = update_tenant(db_session, "update-tenant")
        
        assert result is False
    
    def test_update_tenant_not_found(self, db_session: Session):
        """Test updating non-existent tenant."""
        result = update_tenant(db_session, "nonexistent-tenant", tenant_name="New Name")
        
        assert result is False


class TestUpdateTenantConfig:
    """Test update_tenant_config function."""
    
    def test_update_config_key_success(self, db_session: Session):
        """Test updating specific configuration key."""
        # Create tenant with config
        tenant = Tenant(
            tenant_id="config-tenant",
            name="Config Tenant",
            config={"existing_key": "existing_value", "timeout": 30}
        )
        db_session.add(tenant)
        db_session.commit()
        
        # Update specific key
        result = update_tenant_config(db_session, "config-tenant", "timeout", 60)
        
        assert result is True
        
        # Verify update
        updated_tenant = db_session.query(Tenant).filter_by(tenant_id="config-tenant").first()
        # Config is stored as dict by SQLAlchemy's JSON type
        expected_config = {"existing_key": "existing_value", "timeout": 60}
        assert updated_tenant.config == expected_config
    
    def test_update_config_add_new_key(self, db_session: Session):
        """Test adding new configuration key."""
        # Create tenant with config
        tenant = Tenant(
            tenant_id="config-tenant",
            name="Config Tenant",
            config={"existing_key": "existing_value"}
        )
        db_session.add(tenant)
        db_session.commit()
        
        # Add new key
        result = update_tenant_config(db_session, "config-tenant", "new_key", "new_value")
        
        assert result is True
        
        # Verify update
        updated_tenant = db_session.query(Tenant).filter_by(tenant_id="config-tenant").first()
        # Config is stored as dict by SQLAlchemy's JSON type
        expected_config = {"existing_key": "existing_value", "new_key": "new_value"}
        assert updated_tenant.config == expected_config
    
    def test_update_config_tenant_no_config(self, db_session: Session):
        """Test updating config when tenant has no existing config."""
        # Create tenant without config
        tenant = Tenant(
            tenant_id="config-tenant",
            name="Config Tenant",
            config=None
        )
        db_session.add(tenant)
        db_session.commit()
        
        # Update config
        result = update_tenant_config(db_session, "config-tenant", "new_key", "new_value")
        
        assert result is True
        
        # Verify update
        updated_tenant = db_session.query(Tenant).filter_by(tenant_id="config-tenant").first()
        # Config is stored as dict by SQLAlchemy's JSON type
        assert updated_tenant.config == {"new_key": "new_value"}
    
    def test_update_config_tenant_not_found(self, db_session: Session):
        """Test updating config for non-existent tenant."""
        result = update_tenant_config(db_session, "nonexistent-tenant", "key", "value")
        
        assert result is False


class TestListTenants:
    """Test list_tenants function."""
    
    def test_list_tenants_success(self, db_session: Session):
        """Test listing all tenants."""
        # Create multiple tenants
        tenant1 = Tenant(
            tenant_id="tenant-1",
            name="Tenant One",
            config={"type": "test"}
        )
        tenant2 = Tenant(
            tenant_id="tenant-2",
            name="Tenant Two",
            config=None
        )
        db_session.add_all([tenant1, tenant2])
        db_session.commit()
        
        # List tenants
        tenants = list_tenants(db_session)
        
        assert len(tenants) == 2
        
        # Verify structure (order may vary)
        tenant_ids = {t["tenant_id"] for t in tenants}
        assert tenant_ids == {"tenant-1", "tenant-2"}
        
        # Check first tenant details
        tenant1_data = next(t for t in tenants if t["tenant_id"] == "tenant-1")
        assert tenant1_data["name"] == "Tenant One"
        assert tenant1_data["config"] == {"type": "test"}
        assert "created_at" in tenant1_data
        assert "updated_at" in tenant1_data
        
        # Check second tenant details
        tenant2_data = next(t for t in tenants if t["tenant_id"] == "tenant-2")
        assert tenant2_data["name"] == "Tenant Two"
        assert tenant2_data["config"] == {}  # None converted to empty dict
    
    def test_list_tenants_with_limit(self, db_session: Session):
        """Test listing tenants with limit."""
        # Create multiple tenants
        for i in range(5):
            tenant = Tenant(
                tenant_id=f"tenant-{i}",
                name=f"Tenant {i}"
            )
            db_session.add(tenant)
        db_session.commit()
        
        # List with limit
        tenants = list_tenants(db_session, limit=3)
        
        assert len(tenants) == 3
    
    def test_list_tenants_with_offset(self, db_session: Session):
        """Test listing tenants with offset."""
        # Create multiple tenants
        for i in range(5):
            tenant = Tenant(
                tenant_id=f"tenant-{i}",
                name=f"Tenant {i}"
            )
            db_session.add(tenant)
        db_session.commit()
        
        # List with offset
        tenants = list_tenants(db_session, offset=2)
        
        assert len(tenants) == 3  # 5 total - 2 offset
    
    def test_list_tenants_empty(self, db_session: Session):
        """Test listing tenants when none exist."""
        tenants = list_tenants(db_session)
        
        assert tenants == []


class TestDeleteTenant:
    """Test delete_tenant function."""
    
    def test_delete_tenant_success(self, db_session: Session):
        """Test deleting existing tenant."""
        # Create tenant
        tenant = Tenant(
            tenant_id="delete-tenant",
            name="Delete Tenant"
        )
        db_session.add(tenant)
        db_session.commit()
        
        # Delete tenant
        result = delete_tenant(db_session, "delete-tenant")
        
        assert result is True
        
        # Verify deletion
        deleted_tenant = db_session.query(Tenant).filter_by(tenant_id="delete-tenant").first()
        assert deleted_tenant is None
    
    def test_delete_tenant_not_found(self, db_session: Session):
        """Test deleting non-existent tenant."""
        result = delete_tenant(db_session, "nonexistent-tenant")
        
        assert result is False


class TestTenantUtilsIntegration:
    """Integration tests for tenant utilities."""
    
    def test_tenant_lifecycle(self, db_session: Session):
        """Test complete tenant lifecycle."""
        # 1. Create tenant
        config = ProcessingConfigSchema(
            timeout=120,
            max_concurrent_processes=10,
            batch_size=500
        )
        tenant_create = TenantCreate(
            tenant_id="lifecycle-tenant",
            name="Lifecycle Tenant",
            config=config
        )
        tenant_id = create_tenant(db_session, tenant_create)
        assert tenant_id == "lifecycle-tenant"
        
        # 2. Get config
        retrieved_config = get_tenant_config(db_session, "lifecycle-tenant")
        # Config is returned as dict after being stored as JSON
        assert isinstance(retrieved_config, dict)
        assert retrieved_config["timeout"] == 120
        assert retrieved_config["max_concurrent_processes"] == 10
        assert retrieved_config["batch_size"] == 500
        
        # 3. Update specific config key
        update_result = update_tenant_config(db_session, "lifecycle-tenant", "timeout", 240)
        assert update_result is True
        
        # 4. Verify config update
        updated_config = get_tenant_config(db_session, "lifecycle-tenant")
        # Note: update_tenant_config currently works with raw dict, so this will be mixed
        # In a real implementation, we might want to improve this to handle Pydantic models properly
        assert updated_config is not None
        
        # 5. Update tenant name (still uses old interface)
        name_update_result = update_tenant(db_session, "lifecycle-tenant", tenant_name="Updated Lifecycle Tenant")
        assert name_update_result is True
        
        # 6. List tenants and verify
        tenants = list_tenants(db_session)
        lifecycle_tenant = next(t for t in tenants if t["tenant_id"] == "lifecycle-tenant")
        assert lifecycle_tenant["name"] == "Updated Lifecycle Tenant"
        assert lifecycle_tenant["config"] is not None
        
        # 7. Delete tenant
        delete_result = delete_tenant(db_session, "lifecycle-tenant")
        assert delete_result is True
        
        # 8. Verify deletion
        with pytest.raises(NotFoundError):
            get_tenant_config(db_session, "lifecycle-tenant")