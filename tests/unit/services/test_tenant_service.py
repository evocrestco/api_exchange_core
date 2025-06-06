"""
Comprehensive tests for TenantService.

Tests the tenant service business logic layer using real SQLite database,
following the anti-mock philosophy with real database operations and proper
tenant isolation with schema validation.

Following README_TESTING.md requirements:
- NO MOCKS except for external services
- Real SQLite database testing with automatic rollback
- Heavy parameterization for multiple scenarios
- Tenant isolation testing
- ≥90% coverage target
- Example-driven using realistic test data
"""

import os

# Import models and schemas using our established path pattern
import sys
import uuid

from typing import Any, Dict

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "..", "src"))

from src.context.tenant_context import TenantContext
from src.exceptions import ErrorCode, RepositoryError, ServiceError, ValidationError
from src.schemas.tenant_schema import TenantCreate, TenantUpdate


class TestTenantServiceCreate:
    """Test tenant creation functionality."""

    def test_create_tenant_success(self, tenant_service):
        """Test successful tenant creation."""
        service = tenant_service

        tenant_data = TenantCreate(
            tenant_id=f"create-test-{uuid.uuid4()}",
            customer_name="Test Customer Corp",
            primary_contact_email="contact@testcustomer.com",
            is_active=True,
        )

        result = service.create_tenant(tenant_data)

        assert result is not None
        assert result.tenant_id == tenant_data.tenant_id
        assert result.customer_name == tenant_data.customer_name
        assert result.primary_contact_email == tenant_data.primary_contact_email
        assert result.is_active == tenant_data.is_active

    @pytest.mark.parametrize(
        "customer_name,contact_email,is_active",
        [
            ("Small Business LLC", "owner@smallbiz.com", True),
            ("Enterprise Corp", "admin@enterprise.com", False),
            ("Startup Inc", "founder@startup.io", True),
            ("Global Conglomerate", "contact@global.com", True),
            ("Testing Co", None, False),  # No email
        ],
    )
    def test_create_tenant_various_types(
        self, tenant_service, customer_name, contact_email, is_active
    ):
        """Test creating tenants with various configurations."""
        service = tenant_service

        tenant_data = TenantCreate(
            tenant_id=f"param-test-{uuid.uuid4()}",
            customer_name=customer_name,
            primary_contact_email=contact_email,
            is_active=is_active,
        )

        result = service.create_tenant(tenant_data)

        assert result.tenant_id == tenant_data.tenant_id
        assert result.customer_name == customer_name
        assert result.primary_contact_email == contact_email
        assert result.is_active == is_active

    def test_create_tenant_with_full_details(self, tenant_service):
        """Test creating tenant with all optional fields."""
        service = tenant_service

        tenant_data = TenantCreate(
            tenant_id=f"full-test-{uuid.uuid4()}",
            customer_name="Full Details Corp",
            primary_contact_name="John Doe",
            primary_contact_email="john@fulldetails.com",
            primary_contact_phone="555-123-4567",
            address_line1="123 Main St",
            address_line2="Suite 100",
            city="Anytown",
            state="CA",
            postal_code="12345",
            country="USA",
            notes="Test tenant with complete information",
            is_active=True,
        )

        result = service.create_tenant(tenant_data)

        assert result.tenant_id == tenant_data.tenant_id
        assert result.primary_contact_name == "John Doe"
        assert result.address_line1 == "123 Main St"
        assert result.city == "Anytown"
        assert result.notes == "Test tenant with complete information"

    def test_create_tenant_duplicate_fails(self, tenant_service):
        """Test that creating duplicate tenant fails."""
        service = tenant_service

        tenant_id = f"duplicate-test-{uuid.uuid4()}"
        tenant_data = TenantCreate(tenant_id=tenant_id, customer_name="Original Customer")

        # Create first tenant
        service.create_tenant(tenant_data)

        # Attempt to create duplicate
        duplicate_data = TenantCreate(tenant_id=tenant_id, customer_name="Duplicate Customer")

        with pytest.raises(ValidationError) as exc_info:
            service.create_tenant(duplicate_data)
        assert "already exists" in str(exc_info.value)

    def test_create_tenant_from_dict_success(self, tenant_service):
        """Test creating tenant from dictionary data."""
        service = tenant_service

        tenant_id = f"dict-test-{uuid.uuid4()}"

        result = service.create_tenant_from_dict(
            tenant_id=tenant_id,
            customer_name="Dict Customer",
            primary_contact_email="dict@customer.com",
            is_active=True,
        )

        assert result.tenant_id == tenant_id
        assert result.customer_name == "Dict Customer"
        assert result.primary_contact_email == "dict@customer.com"
        assert result.is_active is True

    def test_create_tenant_from_dict_validation_error(self, tenant_service):
        """Test that create_tenant_from_dict validates input."""
        service = tenant_service

        with pytest.raises((ValueError, ServiceError)):
            service.create_tenant_from_dict(
                tenant_id="", customer_name="Invalid Customer"  # Invalid empty tenant_id
            )


class TestTenantServiceRead:
    """Test tenant retrieval functionality."""

    def test_get_tenant_success(self, tenant_service):
        """Test successful tenant retrieval."""
        service = tenant_service

        # Create tenant first
        tenant_data = TenantCreate(
            tenant_id=f"get-test-{uuid.uuid4()}", customer_name="Get Test Customer"
        )
        created = service.create_tenant(tenant_data)

        # Retrieve tenant
        result = service.get_tenant(created.tenant_id)

        assert result.tenant_id == created.tenant_id
        assert result.customer_name == created.customer_name

    def test_get_tenant_not_found(self, tenant_service):
        """Test getting non-existent tenant."""
        service = tenant_service

        with pytest.raises(RepositoryError) as exc_info:
            service.get_tenant(f"nonexistent-{uuid.uuid4()}")
        assert exc_info.value.error_code == ErrorCode.NOT_FOUND

    def test_get_tenants_empty_list(self, tenant_service):
        """Test getting all tenants when none exist."""
        service = tenant_service

        result = service.get_tenants()

        assert isinstance(result, list)
        assert len(result) == 0

    def test_get_tenants_with_data(self, tenant_service):
        """Test getting all tenants when some exist."""
        service = tenant_service

        # Create multiple tenants
        tenant_ids = []
        for i in range(3):
            tenant_data = TenantCreate(
                tenant_id=f"list-test-{i}-{uuid.uuid4()}",
                customer_name=f"Customer {i}",
                is_active=True,
            )
            created = service.create_tenant(tenant_data)
            tenant_ids.append(created.tenant_id)

        result = service.get_tenants()

        assert len(result) >= 3  # May have other tenants from other tests
        result_ids = [t.tenant_id for t in result]
        for tenant_id in tenant_ids:
            assert tenant_id in result_ids

    def test_get_tenants_include_inactive(self, tenant_service):
        """Test getting tenants including inactive ones."""
        service = tenant_service

        # Create active tenant
        active_data = TenantCreate(
            tenant_id=f"active-{uuid.uuid4()}", customer_name="Active Customer", is_active=True
        )
        active = service.create_tenant(active_data)

        # Create inactive tenant
        inactive_data = TenantCreate(
            tenant_id=f"inactive-{uuid.uuid4()}", customer_name="Inactive Customer", is_active=False
        )
        inactive = service.create_tenant(inactive_data)

        # Get all tenants including inactive
        all_tenants = service.get_tenants(include_inactive=True)
        all_ids = [t.tenant_id for t in all_tenants]

        assert active.tenant_id in all_ids
        assert inactive.tenant_id in all_ids


class TestTenantServiceUpdate:
    """Test tenant update functionality."""

    def test_update_tenant_success(self, tenant_service):
        """Test successful tenant update."""
        service = tenant_service

        # Create tenant
        tenant_data = TenantCreate(
            tenant_id=f"update-test-{uuid.uuid4()}",
            customer_name="Original Name",
            primary_contact_email="original@email.com",
        )
        created = service.create_tenant(tenant_data)

        # Set tenant context for update
        TenantContext.set_current_tenant(created.tenant_id)

        # Update tenant
        update_data = TenantUpdate(
            customer_name="Updated Name", primary_contact_email="updated@email.com"
        )

        result = service.update_tenant(update_data)

        assert result.tenant_id == created.tenant_id
        assert result.customer_name == "Updated Name"
        assert result.primary_contact_email == "updated@email.com"

    def test_update_tenant_partial_fields(self, tenant_service):
        """Test updating only some fields."""
        service = tenant_service

        # Create tenant
        tenant_data = TenantCreate(
            tenant_id=f"partial-update-{uuid.uuid4()}",
            customer_name="Original Name",
            primary_contact_email="original@email.com",
            city="Original City",
        )
        created = service.create_tenant(tenant_data)

        # Set tenant context
        TenantContext.set_current_tenant(created.tenant_id)

        # Update only customer name
        update_data = TenantUpdate(customer_name="New Name")

        result = service.update_tenant(update_data)

        assert result.customer_name == "New Name"
        assert result.primary_contact_email == "original@email.com"  # Unchanged
        assert result.city == "Original City"  # Unchanged

    def test_update_tenant_from_dict_success(self, tenant_service):
        """Test updating tenant from dictionary data."""
        service = tenant_service

        # Create tenant
        tenant_data = TenantCreate(
            tenant_id=f"dict-update-{uuid.uuid4()}", customer_name="Original Name"
        )
        created = service.create_tenant(tenant_data)

        # Set tenant context
        TenantContext.set_current_tenant(created.tenant_id)

        # Update from dict
        result = service.update_tenant_from_dict(
            customer_name="Dict Updated Name", primary_contact_email="dict@updated.com"
        )

        assert result.customer_name == "Dict Updated Name"
        assert result.primary_contact_email == "dict@updated.com"

    def test_update_tenant_not_found(self, tenant_service):
        """Test updating non-existent tenant."""
        service = tenant_service

        # Set context to non-existent tenant
        TenantContext.set_current_tenant(f"nonexistent-{uuid.uuid4()}")

        update_data = TenantUpdate(customer_name="Should Fail")

        with pytest.raises(RepositoryError) as exc_info:
            service.update_tenant(update_data)
        assert exc_info.value.error_code == ErrorCode.NOT_FOUND


class TestTenantServiceConfig:
    """Test tenant configuration functionality."""

    def test_update_tenant_config_success(self, tenant_service):
        """Test successful config update."""
        service = tenant_service

        # Create tenant
        tenant_data = TenantCreate(
            tenant_id=f"config-test-{uuid.uuid4()}", customer_name="Config Test Customer"
        )
        created = service.create_tenant(tenant_data)

        # Update config
        result = service.update_tenant_config(created.tenant_id, "test_setting", "test_value")

        assert result is True

    def test_update_tenant_config_not_found(self, tenant_service):
        """Test config update for non-existent tenant."""
        service = tenant_service

        result = service.update_tenant_config(
            f"nonexistent-{uuid.uuid4()}", "test_setting", "test_value"
        )

        assert result is False

    @pytest.mark.parametrize(
        "config_key,config_value",
        [
            ("string_setting", "string_value"),
            ("number_setting", 42),
            ("boolean_setting", True),
            ("list_setting", ["item1", "item2"]),
            ("dict_setting", {"nested": "value"}),
        ],
    )
    def test_update_tenant_config_various_types(self, tenant_service, config_key, config_value):
        """Test config updates with various data types."""
        service = tenant_service

        # Create tenant
        tenant_data = TenantCreate(
            tenant_id=f"config-types-{uuid.uuid4()}", customer_name="Config Types Customer"
        )
        created = service.create_tenant(tenant_data)

        # Update config with various types
        result = service.update_tenant_config(created.tenant_id, config_key, config_value)

        assert result is True


class TestTenantServiceActivation:
    """Test tenant activation/deactivation functionality."""

    def test_activate_tenant_success(self, tenant_service):
        """Test successful tenant activation."""
        service = tenant_service

        # Create inactive tenant
        tenant_data = TenantCreate(
            tenant_id=f"activate-test-{uuid.uuid4()}",
            customer_name="Activate Test Customer",
            is_active=False,
        )
        created = service.create_tenant(tenant_data)

        # Activate tenant
        result = service.activate_tenant(created.tenant_id)

        assert result is True

        # Verify tenant is now active
        retrieved = service.get_tenant(created.tenant_id)
        assert retrieved.is_active is True

    def test_activate_tenant_not_found(self, tenant_service):
        """Test activating non-existent tenant."""
        service = tenant_service

        result = service.activate_tenant(f"nonexistent-{uuid.uuid4()}")

        assert result is False

    def test_deactivate_tenant_success(self, tenant_service):
        """Test successful tenant deactivation."""
        service = tenant_service

        # Create active tenant
        tenant_data = TenantCreate(
            tenant_id=f"deactivate-test-{uuid.uuid4()}",
            customer_name="Deactivate Test Customer",
            is_active=True,
        )
        created = service.create_tenant(tenant_data)

        # Deactivate tenant
        result = service.deactivate_tenant(created.tenant_id)

        assert result is True

        # Verify tenant is now inactive
        retrieved = service.get_tenant(created.tenant_id)
        assert retrieved.is_active is False

    def test_deactivate_tenant_not_found(self, tenant_service):
        """Test deactivating non-existent tenant."""
        service = tenant_service

        result = service.deactivate_tenant(f"nonexistent-{uuid.uuid4()}")

        assert result is False

    def test_activation_cycle(self, tenant_service):
        """Test complete activation/deactivation cycle."""
        service = tenant_service

        # Create tenant
        tenant_data = TenantCreate(
            tenant_id=f"cycle-test-{uuid.uuid4()}",
            customer_name="Cycle Test Customer",
            is_active=True,
        )
        created = service.create_tenant(tenant_data)

        # Deactivate
        result1 = service.deactivate_tenant(created.tenant_id)
        assert result1 is True

        retrieved1 = service.get_tenant(created.tenant_id)
        assert retrieved1.is_active is False

        # Reactivate
        result2 = service.activate_tenant(created.tenant_id)
        assert result2 is True

        retrieved2 = service.get_tenant(created.tenant_id)
        assert retrieved2.is_active is True


class TestTenantServiceErrorHandling:
    """Test error handling and edge cases."""

    def test_duplicate_tenant_validation_error(self, tenant_service):
        """Test that duplicate tenant creation raises ValueError."""
        service = tenant_service

        tenant_data = TenantCreate(
            tenant_id=f"duplicate-test-{uuid.uuid4()}", customer_name="Duplicate Test Customer"
        )

        # First creation should succeed
        service.create_tenant(tenant_data)

        # Second creation with same ID should raise ValidationError
        with pytest.raises(ValidationError) as exc_info:
            service.create_tenant(tenant_data)
        assert "already exists" in str(exc_info.value)
        assert exc_info.value.error_code == ErrorCode.VALIDATION_FAILED

    def test_invalid_tenant_update(self, tenant_service):
        """Test updating non-existent tenant raises appropriate error."""
        service = tenant_service

        # Set a non-existent tenant in context
        TenantContext.set_current_tenant("nonexistent-tenant-id")

        update_data = TenantUpdate(customer_name="Updated Name")

        # Should raise RepositoryError when tenant doesn't exist
        with pytest.raises(RepositoryError) as exc_info:
            service.update_tenant(update_data)
        assert exc_info.value.error_code == ErrorCode.NOT_FOUND

    def test_tenant_context_integration(self, tenant_service):
        """Test integration with TenantContext."""
        service = tenant_service

        # Create tenant
        tenant_data = TenantCreate(
            tenant_id=f"context-test-{uuid.uuid4()}", customer_name="Context Test Customer"
        )
        created = service.create_tenant(tenant_data)

        # Clear context to test context dependency
        TenantContext.clear_current_tenant()

        # Operations requiring context should fail appropriately
        update_data = TenantUpdate(customer_name="Should Fail")

        with pytest.raises((ValueError, ServiceError)):
            service.update_tenant(update_data)

    def test_cache_clearing_behavior(self, tenant_service, db_session):
        """Test that service operations properly clear tenant cache."""
        service = tenant_service

        # Create tenant
        tenant_data = TenantCreate(
            tenant_id=f"cache-test-{uuid.uuid4()}", customer_name="Cache Test Customer"
        )
        created = service.create_tenant(tenant_data)

        # Populate cache by getting tenant via TenantContext
        TenantContext.get_tenant(db_session, created.tenant_id)

        # Update tenant - should clear cache
        TenantContext.set_current_tenant(created.tenant_id)
        update_data = TenantUpdate(customer_name="Updated Name")
        service.update_tenant(update_data)

        # Cache should be cleared (difficult to test directly without mocking)
        # But operation should succeed
        updated = service.get_tenant(created.tenant_id)
        assert updated.customer_name == "Updated Name"


class TestTenantServiceIntegration:
    """Test realistic integration scenarios."""

    def test_complete_tenant_lifecycle(self, tenant_service):
        """Test complete tenant management lifecycle."""
        service = tenant_service

        # 1. Create tenant
        tenant_data = TenantCreate(
            tenant_id=f"lifecycle-{uuid.uuid4()}",
            customer_name="Lifecycle Customer",
            primary_contact_email="contact@lifecycle.com",
            is_active=True,
        )
        created = service.create_tenant(tenant_data)
        assert created.is_active is True

        # 2. Update tenant details
        TenantContext.set_current_tenant(created.tenant_id)
        update_data = TenantUpdate(
            customer_name="Updated Lifecycle Customer", primary_contact_phone="555-987-6543"
        )
        updated = service.update_tenant(update_data)
        assert updated.customer_name == "Updated Lifecycle Customer"
        assert updated.primary_contact_phone == "555-987-6543"

        # 3. Update configuration
        config_result = service.update_tenant_config(created.tenant_id, "max_connections", 100)
        assert config_result is True

        # 4. Deactivate tenant
        deactivate_result = service.deactivate_tenant(created.tenant_id)
        assert deactivate_result is True

        # 5. Verify final state
        final = service.get_tenant(created.tenant_id)
        assert final.customer_name == "Updated Lifecycle Customer"
        assert final.is_active is False

    def test_multi_tenant_operations(self, tenant_service):
        """Test operations across multiple tenants."""
        service = tenant_service

        # Create multiple tenants
        tenant_ids = []
        for i in range(3):
            tenant_data = TenantCreate(
                tenant_id=f"multi-{i}-{uuid.uuid4()}",
                customer_name=f"Multi Customer {i}",
                is_active=i % 2 == 0,  # Alternating active/inactive
            )
            created = service.create_tenant(tenant_data)
            tenant_ids.append(created.tenant_id)

        # Get all tenants
        all_tenants = service.get_tenants(include_inactive=True)
        multi_tenants = [t for t in all_tenants if t.tenant_id in tenant_ids]

        assert len(multi_tenants) == 3

        # Verify active/inactive states
        active_count = sum(1 for t in multi_tenants if t.is_active)
        inactive_count = sum(1 for t in multi_tenants if not t.is_active)

        assert active_count == 2  # Tenants 0 and 2
        assert inactive_count == 1  # Tenant 1

    def test_tenant_operations_with_context_switching(self, tenant_service):
        """Test operations with context switching between tenants."""
        service = tenant_service

        # Create two tenants
        tenant1_data = TenantCreate(
            tenant_id=f"switch1-{uuid.uuid4()}", customer_name="Switch Customer 1"
        )
        tenant1 = service.create_tenant(tenant1_data)

        tenant2_data = TenantCreate(
            tenant_id=f"switch2-{uuid.uuid4()}", customer_name="Switch Customer 2"
        )
        tenant2 = service.create_tenant(tenant2_data)

        # Update tenant 1
        TenantContext.set_current_tenant(tenant1.tenant_id)
        update1 = TenantUpdate(customer_name="Updated Customer 1")
        result1 = service.update_tenant(update1)
        assert result1.customer_name == "Updated Customer 1"

        # Switch context and update tenant 2
        TenantContext.set_current_tenant(tenant2.tenant_id)
        update2 = TenantUpdate(customer_name="Updated Customer 2")
        result2 = service.update_tenant(update2)
        assert result2.customer_name == "Updated Customer 2"

        # Verify both tenants have correct state
        final1 = service.get_tenant(tenant1.tenant_id)
        final2 = service.get_tenant(tenant2.tenant_id)

        assert final1.customer_name == "Updated Customer 1"
        assert final2.customer_name == "Updated Customer 2"
