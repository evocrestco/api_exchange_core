"""
Unit tests for CRUD helper functions.

Tests the generic CRUD operations that replace the old utility duplication.
"""

import pytest
from sqlalchemy.orm import Session

from api_exchange_core.db import Tenant
from api_exchange_core.exceptions import BaseError, NotFoundError
from api_exchange_core.utils.crud_helpers import (
    create_record,
    delete_record,
    get_record,
    list_records,
    update_record,
)


class TestCreateRecord:
    """Test create_record function."""
    
    def test_create_simple_record(self, db_session: Session, sample_tenant_id: str):
        """Test creating a simple record."""
        tenant_data = {
            "tenant_id": sample_tenant_id,  # Set explicitly since it's the primary key
            "name": "Test Tenant",
            "config": {"description": "A test tenant"},
        }
        
        tenant = create_record(db_session, Tenant, tenant_data)
        
        assert tenant.tenant_id == sample_tenant_id
        assert tenant.name == "Test Tenant"
        assert tenant.config == {"description": "A test tenant"}
        assert tenant.created_at is not None
        assert tenant.updated_at is not None
    
    def test_create_record_without_tenant_id(self, db_session: Session):
        """Test creating a record without tenant_id parameter."""
        tenant_data = {
            "tenant_id": "explicit-tenant-id",
            "name": "Test Tenant",
            "config": {"type": "test"},
        }
        
        tenant = create_record(db_session, Tenant, tenant_data)
        
        assert tenant.tenant_id == "explicit-tenant-id"
        assert tenant.name == "Test Tenant"
    
    def test_create_record_auto_timestamps(self, db_session: Session, sample_tenant_id: str):
        """Test that timestamps are automatically added."""
        tenant_data = {
            "name": "Test Tenant",
        }
        
        tenant = create_record(db_session, Tenant, tenant_data, tenant_id=sample_tenant_id)
        
        assert tenant.created_at is not None
        assert tenant.updated_at is not None
        # Timestamps should be very close (within same second)
        assert abs((tenant.created_at - tenant.updated_at).total_seconds()) < 1
    
    def test_create_record_database_error(self, db_session: Session):
        """Test handling of database errors during creation."""
        # Try to create a record with invalid data (missing required field)
        invalid_data = {
            "description": "Missing required name field",
        }
        
        with pytest.raises(BaseError) as exc_info:
            create_record(db_session, Tenant, invalid_data)
        
        assert "Failed to create Tenant" in str(exc_info.value)


class TestGetRecord:
    """Test get_record function."""
    
    def test_get_existing_record(self, db_session: Session, sample_tenant_id: str):
        """Test retrieving an existing record."""
        # Create a tenant first
        tenant_data = {
            "name": "Test Tenant",
            "is_active": True,
        }
        created_tenant = create_record(db_session, Tenant, tenant_data, tenant_id=sample_tenant_id)
        db_session.commit()
        
        # Retrieve by ID
        retrieved_tenant = get_record(db_session, Tenant, {"id": created_tenant.id})
        
        assert retrieved_tenant is not None
        assert retrieved_tenant.id == created_tenant.id
        assert retrieved_tenant.name == "Test Tenant"
    
    def test_get_record_by_multiple_filters(self, db_session: Session, sample_tenant_id: str):
        """Test retrieving a record with multiple filter criteria."""
        # Create a tenant
        tenant_data = {
            "name": "Test Tenant",
            "is_active": True,
        }
        create_record(db_session, Tenant, tenant_data, tenant_id=sample_tenant_id)
        db_session.commit()
        
        # Retrieve by tenant_id and name
        retrieved_tenant = get_record(
            db_session, Tenant, {"tenant_id": sample_tenant_id, "name": "Test Tenant"}
        )
        
        assert retrieved_tenant is not None
        assert retrieved_tenant.tenant_id == sample_tenant_id
        assert retrieved_tenant.name == "Test Tenant"
    
    def test_get_nonexistent_record(self, db_session: Session):
        """Test retrieving a non-existent record returns None."""
        result = get_record(db_session, Tenant, {"id": "nonexistent-id"})
        assert result is None
    
    def test_get_record_empty_filters(self, db_session: Session):
        """Test get_record with empty filters returns first record."""
        # Create two tenants
        tenant1_data = {"name": "Tenant 1", "is_active": True}
        tenant2_data = {"name": "Tenant 2", "is_active": True}
        
        create_record(db_session, Tenant, tenant1_data, tenant_id="tenant-1")
        create_record(db_session, Tenant, tenant2_data, tenant_id="tenant-2")
        db_session.commit()
        
        # Get with empty filters should return first record
        result = get_record(db_session, Tenant, {})
        
        assert result is not None
        assert result.name in ["Tenant 1", "Tenant 2"]


class TestListRecords:
    """Test list_records function."""
    
    def test_list_all_records(self, db_session: Session):
        """Test listing all records without filters."""
        # Create multiple tenants
        tenant1_data = {"name": "Tenant 1", "is_active": True}
        tenant2_data = {"name": "Tenant 2", "is_active": False}
        
        create_record(db_session, Tenant, tenant1_data, tenant_id="tenant-1")
        create_record(db_session, Tenant, tenant2_data, tenant_id="tenant-2")
        db_session.commit()
        
        # List all
        all_tenants = list_records(db_session, Tenant)
        
        assert len(all_tenants) == 2
        tenant_names = [t.name for t in all_tenants]
        assert "Tenant 1" in tenant_names
        assert "Tenant 2" in tenant_names
    
    def test_list_records_with_filters(self, db_session: Session):
        """Test listing records with filter criteria."""
        # Create tenants with different statuses
        tenant1_data = {"name": "Active Tenant", "is_active": True}
        tenant2_data = {"name": "Inactive Tenant", "is_active": False}
        
        create_record(db_session, Tenant, tenant1_data, tenant_id="tenant-1")
        create_record(db_session, Tenant, tenant2_data, tenant_id="tenant-2")
        db_session.commit()
        
        # List only active tenants
        active_tenants = list_records(db_session, Tenant, {"is_active": True})
        
        assert len(active_tenants) == 1
        assert active_tenants[0].name == "Active Tenant"
        assert active_tenants[0].is_active is True
    
    def test_list_records_with_ordering(self, db_session: Session):
        """Test listing records with custom ordering."""
        # Create tenants in reverse alphabetical order
        tenant1_data = {"name": "Zebra Tenant", "is_active": True}
        tenant2_data = {"name": "Alpha Tenant", "is_active": True}
        
        create_record(db_session, Tenant, tenant1_data, tenant_id="tenant-1")
        create_record(db_session, Tenant, tenant2_data, tenant_id="tenant-2")
        db_session.commit()
        
        # List with alphabetical ordering
        ordered_tenants = list_records(db_session, Tenant, order_by="name")
        
        assert len(ordered_tenants) == 2
        assert ordered_tenants[0].name == "Alpha Tenant"
        assert ordered_tenants[1].name == "Zebra Tenant"
    
    def test_list_records_with_limit(self, db_session: Session):
        """Test listing records with limit."""
        # Create multiple tenants
        for i in range(5):
            tenant_data = {"name": f"Tenant {i}", "is_active": True}
            create_record(db_session, Tenant, tenant_data, tenant_id=f"tenant-{i}")
        db_session.commit()
        
        # List with limit
        limited_tenants = list_records(db_session, Tenant, limit=3)
        
        assert len(limited_tenants) == 3
    
    def test_list_empty_table(self, db_session: Session):
        """Test listing records from empty table."""
        result = list_records(db_session, Tenant)
        assert result == []


class TestUpdateRecord:
    """Test update_record function."""
    
    def test_update_existing_record(self, db_session: Session, sample_tenant_id: str):
        """Test updating an existing record."""
        # Create a tenant
        tenant_data = {
            "name": "Original Name",
            "description": "Original Description",
            "is_active": True,
        }
        tenant = create_record(db_session, Tenant, tenant_data, tenant_id=sample_tenant_id)
        db_session.commit()
        
        # Update the tenant
        update_data = {
            "name": "Updated Name",
            "description": "Updated Description",
            "is_active": False,
        }
        
        updated_tenant = update_record(db_session, Tenant, tenant.id, update_data)
        
        assert updated_tenant.name == "Updated Name"
        assert updated_tenant.description == "Updated Description"
        assert updated_tenant.is_active is False
        assert updated_tenant.updated_at > updated_tenant.created_at
    
    def test_update_nonexistent_record(self, db_session: Session):
        """Test updating a non-existent record raises error."""
        update_data = {"name": "Updated Name"}
        
        with pytest.raises(NotFoundError) as exc_info:
            update_record(db_session, Tenant, "nonexistent-id", update_data)
        
        assert "not found" in str(exc_info.value)
    
    def test_update_partial_fields(self, db_session: Session, sample_tenant_id: str):
        """Test updating only some fields leaves others unchanged."""
        # Create a tenant
        tenant_data = {
            "name": "Original Name",
            "description": "Original Description",
            "is_active": True,
        }
        tenant = create_record(db_session, Tenant, tenant_data, tenant_id=sample_tenant_id)
        db_session.commit()
        
        # Update only the name
        update_data = {"name": "Updated Name"}
        
        updated_tenant = update_record(db_session, Tenant, tenant.id, update_data)
        
        assert updated_tenant.name == "Updated Name"
        assert updated_tenant.description == "Original Description"  # Unchanged
        assert updated_tenant.is_active is True  # Unchanged


class TestDeleteRecord:
    """Test delete_record function."""
    
    def test_delete_existing_record(self, db_session: Session, sample_tenant_id: str):
        """Test deleting an existing record."""
        # Create a tenant
        tenant_data = {
            "name": "To Be Deleted",
            "is_active": True,
        }
        tenant = create_record(db_session, Tenant, tenant_data, tenant_id=sample_tenant_id)
        db_session.commit()
        
        # Delete the tenant
        result = delete_record(db_session, Tenant, tenant.id)
        
        assert result is True
        
        # Verify it's deleted
        deleted_tenant = get_record(db_session, Tenant, {"id": tenant.id})
        assert deleted_tenant is None
    
    def test_delete_nonexistent_record(self, db_session: Session):
        """Test deleting a non-existent record returns False."""
        result = delete_record(db_session, Tenant, "nonexistent-id")
        assert result is False