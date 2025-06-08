"""
Comprehensive tests for TenantRepository.

Tests the tenant repository data access layer using real SQLite database,
following the anti-mock philosophy with real database operations and
proper tenant data management with schema validation.
"""

import os

# Import models and schemas using our established path pattern
import sys
import uuid
from datetime import datetime
from typing import Any, Dict

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "..", "src"))

from src.db.db_tenant_models import Tenant
from src.exceptions import ErrorCode, RepositoryError
from src.repositories.tenant_repository import TenantRepository
from src.schemas.tenant_schema import TenantConfigUpdate, TenantCreate, TenantRead, TenantUpdate

# ==================== TENANT REPOSITORY TESTS ====================


class TestTenantRepositoryCreate:
    """Test tenant creation operations."""

    def test_create_tenant_success(self, tenant_repository, db_session):
        """Test successful tenant creation."""
        # Arrange
        tenant_data = TenantCreate(
            tenant_id="test_tenant_001",
            customer_name="Test Customer Corp",
            is_active=True,
            primary_contact_name="John Doe",
            primary_contact_email="john.doe@testcorp.com",
            primary_contact_phone="+1-555-0123",
            address_line1="123 Business Ave",
            city="Business City",
            state="BC",
            postal_code="12345",
            country="US",
            tenant_config={
                "max_entities": {"value": 10000, "updated_at": None},
                "enable_analytics": {"value": True, "updated_at": None},
                "processing_timeout": {"value": 300, "updated_at": None},
            },
        )

        # Act
        created_tenant = tenant_repository.create(tenant_data)

        # Assert
        assert isinstance(created_tenant, TenantRead)
        assert created_tenant.tenant_id == "test_tenant_001"
        assert created_tenant.customer_name == "Test Customer Corp"
        assert created_tenant.is_active is True
        assert created_tenant.primary_contact_name == "John Doe"
        assert created_tenant.primary_contact_email == "john.doe@testcorp.com"
        assert created_tenant.primary_contact_phone == "+1-555-0123"
        assert created_tenant.address_line1 == "123 Business Ave"
        assert created_tenant.city == "Business City"
        assert created_tenant.state == "BC"
        assert created_tenant.postal_code == "12345"
        assert created_tenant.country == "US"
        assert created_tenant.tenant_config["max_entities"].value == 10000
        assert created_tenant.tenant_config["enable_analytics"].value is True
        assert created_tenant.tenant_config["processing_timeout"].value == 300
        assert isinstance(created_tenant.created_at, datetime)
        assert isinstance(created_tenant.updated_at, datetime)

    def test_create_tenant_minimal_data(self, tenant_repository, db_session):
        """Test creating tenant with minimal required data."""
        # Arrange
        tenant_data = TenantCreate(tenant_id="minimal_tenant", customer_name="Minimal Corp")

        # Act
        created_tenant = tenant_repository.create(tenant_data)

        # Assert
        assert isinstance(created_tenant, TenantRead)
        assert created_tenant.tenant_id == "minimal_tenant"
        assert created_tenant.customer_name == "Minimal Corp"
        assert created_tenant.is_active is True  # Default value
        assert created_tenant.primary_contact_name is None
        assert created_tenant.primary_contact_email is None
        assert created_tenant.tenant_config == {}

    def test_create_tenant_with_complex_config(self, tenant_repository, db_session):
        """Test creating tenant with complex configuration."""
        # Arrange
        complex_config = {
            "processing": {
                "value": {"max_concurrent": 5, "timeout_seconds": 120, "retry_attempts": 3},
                "updated_at": None,
            },
            "notifications": {
                "value": {
                    "email_enabled": True,
                    "webhook_url": "https://api.example.com/webhook",
                    "alert_levels": ["error", "warning"],
                },
                "updated_at": None,
            },
            "features": {
                "value": {
                    "advanced_analytics": False,
                    "custom_processors": True,
                    "api_rate_limit": 1000,
                },
                "updated_at": None,
            },
        }

        tenant_data = TenantCreate(
            tenant_id="complex_tenant",
            customer_name="Complex Corp",
            is_active=True,
            tenant_config=complex_config,
        )

        # Act
        created_tenant = tenant_repository.create(tenant_data)

        # Assert
        assert created_tenant.tenant_config["processing"].value["max_concurrent"] == 5
        assert (
            created_tenant.tenant_config["notifications"].value["webhook_url"]
            == "https://api.example.com/webhook"
        )
        assert created_tenant.tenant_config["features"].value["advanced_analytics"] is False

    def test_create_tenant_duplicate_id_fails(self, tenant_repository, db_session):
        """Test that creating tenant with duplicate ID fails."""
        # Arrange - Create first tenant
        tenant_data1 = TenantCreate(tenant_id="duplicate_test", customer_name="First Corp")
        tenant_repository.create(tenant_data1)

        # Arrange - Try to create second tenant with same ID
        tenant_data2 = TenantCreate(
            tenant_id="duplicate_test", customer_name="Second Corp"  # Same ID
        )

        # Act & Assert
        with pytest.raises(RepositoryError):
            tenant_repository.create(tenant_data2)

    def test_create_tenant_invalid_email_fails(self, tenant_repository, db_session):
        """Test that creating tenant with invalid email fails."""
        # Arrange
        with pytest.raises(ValueError, match="Invalid email address"):
            TenantCreate(
                tenant_id="invalid_email_test",
                customer_name="Invalid Email Corp",
                primary_contact_email="invalid-email",  # Missing @ symbol
            )


class TestTenantRepositoryRead:
    """Test tenant read operations."""

    def test_get_by_id_success(self, tenant_repository, db_session):
        """Test successful retrieval of tenant by ID."""
        # Arrange - Create a tenant first
        tenant_data = TenantCreate(
            tenant_id="get_test_001",
            customer_name="Get Test Corp",
            primary_contact_name="Jane Smith",
        )
        created_tenant = tenant_repository.create(tenant_data)

        # Act
        retrieved_tenant = tenant_repository.get_by_id("get_test_001")

        # Assert
        assert isinstance(retrieved_tenant, TenantRead)
        assert retrieved_tenant.tenant_id == "get_test_001"
        assert retrieved_tenant.customer_name == "Get Test Corp"
        assert retrieved_tenant.primary_contact_name == "Jane Smith"
        assert retrieved_tenant.id == created_tenant.id

    def test_get_by_id_not_found(self, tenant_repository, db_session):
        """Test retrieval of non-existent tenant."""
        # Act & Assert
        with pytest.raises(
            RepositoryError, match="Tenant not found: tenant_id=nonexistent"
        ) as exc_info:
            tenant_repository.get_by_id("nonexistent")
        assert exc_info.value.error_code == ErrorCode.NOT_FOUND



class TestTenantRepositoryUpdate:
    """Test tenant update operations."""

    def test_update_tenant_success(self, tenant_repository, db_session):
        """Test successful tenant update."""
        # Arrange - Create a tenant first
        tenant_data = TenantCreate(
            tenant_id="update_test_001",
            customer_name="Original Corp",
            primary_contact_name="Original Name",
        )
        created_tenant = tenant_repository.create(tenant_data)

        # Arrange - Update data
        update_data = TenantUpdate(
            customer_name="Updated Corp",
            primary_contact_name="Updated Name",
            primary_contact_email="updated@corp.com",
        )

        # Act
        updated_tenant = tenant_repository.update("update_test_001", update_data)

        # Assert
        assert isinstance(updated_tenant, TenantRead)
        assert updated_tenant.tenant_id == "update_test_001"
        assert updated_tenant.customer_name == "Updated Corp"
        assert updated_tenant.primary_contact_name == "Updated Name"
        assert updated_tenant.primary_contact_email == "updated@corp.com"
        assert updated_tenant.id == created_tenant.id  # ID should remain the same

    def test_update_tenant_partial(self, tenant_repository, db_session):
        """Test partial tenant update (only some fields)."""
        # Arrange - Create a tenant first
        tenant_data = TenantCreate(
            tenant_id="partial_update_001",
            customer_name="Partial Corp",
            primary_contact_name="Partial Name",
            city="Original City",
        )
        created_tenant = tenant_repository.create(tenant_data)

        # Arrange - Update only customer name
        update_data = TenantUpdate(customer_name="Partially Updated Corp")

        # Act
        updated_tenant = tenant_repository.update("partial_update_001", update_data)

        # Assert
        assert updated_tenant.customer_name == "Partially Updated Corp"
        assert updated_tenant.primary_contact_name == "Partial Name"  # Unchanged
        assert updated_tenant.city == "Original City"  # Unchanged

    def test_update_tenant_not_found(self, tenant_repository, db_session):
        """Test updating non-existent tenant."""
        # Arrange
        update_data = TenantUpdate(customer_name="Nonexistent Corp")

        # Act & Assert
        with pytest.raises(
            RepositoryError, match="Tenant not found: tenant_id=nonexistent"
        ) as exc_info:
            tenant_repository.update("nonexistent", update_data)
        assert exc_info.value.error_code == ErrorCode.NOT_FOUND

    def test_update_tenant_invalid_email_fails(self, tenant_repository, db_session):
        """Test that updating tenant with invalid email fails."""
        # Arrange - Create a tenant first
        tenant_data = TenantCreate(tenant_id="email_update_test", customer_name="Email Test Corp")
        tenant_repository.create(tenant_data)

        # Act & Assert
        with pytest.raises(ValueError, match="Invalid email address"):
            TenantUpdate(primary_contact_email="invalid-email")  # Missing @ symbol


class TestTenantRepositoryConfigUpdate:
    """Test tenant configuration update operations."""

    def test_update_config_success(self, tenant_repository, db_session):
        """Test successful configuration update."""
        # Arrange - Create a tenant first
        tenant_data = TenantCreate(
            tenant_id="config_test_001",
            customer_name="Config Test Corp",
            tenant_config={"existing_key": {"value": "existing_value", "updated_at": None}},
        )
        tenant_repository.create(tenant_data)

        # Arrange - Config update
        config_update = TenantConfigUpdate(key="new_setting", value="new_value")

        # Act
        updated_tenant = tenant_repository.update_config("config_test_001", config_update)

        # Assert
        assert isinstance(updated_tenant, TenantRead)
        assert "new_setting" in updated_tenant.tenant_config
        assert updated_tenant.tenant_config["new_setting"].value == "new_value"
        assert "existing_key" in updated_tenant.tenant_config  # Existing config preserved
        assert updated_tenant.tenant_config["existing_key"].value == "existing_value"

    def test_update_config_overwrite_existing(self, tenant_repository, db_session):
        """Test overwriting existing configuration key."""
        # Arrange - Create a tenant with existing config
        tenant_data = TenantCreate(
            tenant_id="config_overwrite_001",
            customer_name="Config Overwrite Corp",
            tenant_config={"setting_to_update": {"value": "old_value", "updated_at": None}},
        )
        tenant_repository.create(tenant_data)

        # Arrange - Config update for existing key
        config_update = TenantConfigUpdate(key="setting_to_update", value="new_value")

        # Act
        updated_tenant = tenant_repository.update_config("config_overwrite_001", config_update)

        # Assert
        assert updated_tenant.tenant_config["setting_to_update"].value == "new_value"

    def test_update_config_tenant_not_found(self, tenant_repository, db_session):
        """Test configuration update for non-existent tenant."""
        # Arrange
        config_update = TenantConfigUpdate(key="some_key", value="some_value")

        # Act & Assert
        with pytest.raises(
            RepositoryError, match="Tenant not found: tenant_id=nonexistent"
        ) as exc_info:
            tenant_repository.update_config("nonexistent", config_update)
        assert exc_info.value.error_code == ErrorCode.NOT_FOUND

    def test_update_config_complex_value(self, tenant_repository, db_session):
        """Test configuration update with complex value types."""
        # Arrange - Create a tenant
        tenant_data = TenantCreate(
            tenant_id="complex_config_001", customer_name="Complex Config Corp"
        )
        tenant_repository.create(tenant_data)

        # Arrange - Complex config value
        complex_value = {
            "nested": {"array": [1, 2, 3], "boolean": True, "string": "test"},
            "number": 42,
        }
        config_update = TenantConfigUpdate(key="complex_setting", value=complex_value)

        # Act
        updated_tenant = tenant_repository.update_config("complex_config_001", config_update)

        # Assert
        assert updated_tenant.tenant_config["complex_setting"].value == complex_value
        assert updated_tenant.tenant_config["complex_setting"].value["nested"]["array"] == [1, 2, 3]
        assert updated_tenant.tenant_config["complex_setting"].value["number"] == 42


class TestTenantRepositoryErrorHandling:
    """Test repository error handling and edge cases."""

    def test_repository_error_handling(self, tenant_repository, db_session):
        """Test that repository properly handles and converts database errors."""
        # This test ensures proper error handling and conversion
        # by attempting operations that should fail with proper error types

        # Test 1: Create tenant, then try to create duplicate (should fail with RepositoryError)
        tenant_data = TenantCreate(tenant_id="error_test_001", customer_name="Error Test Corp")
        tenant_repository.create(tenant_data)

        duplicate_data = TenantCreate(
            tenant_id="error_test_001", customer_name="Duplicate Corp"  # Same ID
        )

        with pytest.raises(RepositoryError, match="Duplicate Tenant"):
            tenant_repository.create(duplicate_data)

    def test_repository_validates_schemas(self, tenant_repository, db_session):
        """Test that repository validates schemas properly."""
        # Test that invalid schema data raises validation errors

        # This should pass validation at schema level, not repository level
        with pytest.raises(ValueError, match="Invalid email address"):
            TenantCreate(
                tenant_id="validation_test",
                customer_name="Validation Test Corp",
                primary_contact_email="invalid-email",  # Missing @ symbol
            )
