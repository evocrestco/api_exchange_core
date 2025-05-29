"""
Comprehensive tests for EntityService.

Tests the entity service business logic layer using real SQLite database,
following the anti-mock philosophy with real database operations and
entity repository integration.
"""

import os

# Import models and schemas using our established path pattern
import sys
import uuid
from datetime import datetime
from typing import Any, Dict

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "..", "src"))

from schemas.entity_schema import EntityCreate
from src.exceptions import ErrorCode, RepositoryError, ServiceError, ValidationError
from src.repositories.entity_repository import EntityRepository
from src.schemas.entity_schema import EntityRead
from src.services.entity_service import EntityService
from src.utils.hash_config import HashConfig

# ==================== ENTITY SERVICE TESTS ====================


class TestEntityServiceCreate:
    """Test entity creation operations."""

    def test_create_entity_success(self, entity_service, tenant_context):
        """Test successful entity creation."""
        # Act
        entity_id = entity_service.create_entity(
            external_id="service_order_001",
            canonical_type="order",
            source="test_system",
            content={"status": "RECEIVED", "priority": "normal"},
            attributes={"created_by": "test"},
        )

        # Assert
        assert entity_id is not None
        assert isinstance(entity_id, str)

        # Verify entity was created correctly
        created_entity = entity_service.get_entity(entity_id)
        assert created_entity.external_id == "service_order_001"
        assert created_entity.canonical_type == "order"
        assert created_entity.source == "test_system"
        assert created_entity.version == 1
        assert created_entity.attributes["created_by"] == "test"

    def test_create_entity_with_custom_version(self, entity_service, tenant_context):
        """Test creating entity with custom version."""
        # Act
        entity_id = entity_service.create_entity(
            external_id="service_order_002",
            canonical_type="order",
            source="test_system",
            version=5,
            attributes={"initial_version": 5},
        )

        # Assert
        created_entity = entity_service.get_entity(entity_id)
        assert created_entity.version == 5
        assert created_entity.attributes["initial_version"] == 5

    def test_create_entity_with_hash_config(self, entity_service, tenant_context):
        """Test creating entity with custom hash configuration."""
        # Arrange
        hash_config = HashConfig(
            include_fields=["status", "priority"], exclude_fields=["timestamp"]
        )
        content = {
            "status": "PROCESSING",
            "priority": "high",
            "timestamp": "2023-01-01T00:00:00Z",  # Should be excluded
        }

        # Act
        entity_id = entity_service.create_entity(
            external_id="service_order_003",
            canonical_type="order",
            source="test_system",
            content=content,
            hash_config=hash_config,
        )

        # Assert
        created_entity = entity_service.get_entity(entity_id)
        assert created_entity.external_id == "service_order_003"
        # Hash should be calculated without timestamp field
        assert created_entity.content_hash is not None

    @pytest.mark.parametrize(
        "canonical_type,source",
        [
            ("order", "shopify"),
            ("product", "internal_catalog"),
            ("customer", "crm_system"),
            ("inventory", "warehouse_api"),
        ],
    )
    def test_create_entity_various_types(
        self, entity_service, tenant_context, canonical_type, source
    ):
        """Test creating entities of various types."""
        # Act
        entity_id = entity_service.create_entity(
            external_id=f"service_{canonical_type}_001",
            canonical_type=canonical_type,
            source=source,
            attributes={"type": canonical_type, "created_by": "test"},
        )

        # Assert
        created_entity = entity_service.get_entity(entity_id)
        assert created_entity.canonical_type == canonical_type
        assert created_entity.source == source
        assert created_entity.attributes["type"] == canonical_type


class TestEntityServiceVersioning:
    """Test entity versioning operations."""

    def test_create_new_version_success(self, entity_service, tenant_context):
        """Test creating new version of existing entity."""
        # Arrange - Create initial entity
        initial_id = entity_service.create_entity(
            external_id="service_order_004",
            canonical_type="order",
            source="test_system",
            content={"status": "RECEIVED"},
            version=1,
        )

        # Act - Create new version
        new_entity_id, new_version = entity_service.create_new_version(
            external_id="service_order_004",
            source="test_system",
            content={"status": "PROCESSING", "updated": True},
        )

        # Assert
        assert new_entity_id != initial_id
        assert new_version == 2

        # Verify new entity has correct version and content
        new_entity = entity_service.get_entity(new_entity_id)
        assert new_entity.version == 2
        assert new_entity.external_id == "service_order_004"

        # Verify original entity still exists unchanged
        original_entity = entity_service.get_entity(initial_id)
        assert original_entity.version == 1

    def test_create_new_version_with_attributes(self, entity_service, tenant_context):
        """Test creating new version with different attributes."""
        # Arrange
        initial_id = entity_service.create_entity(
            external_id="service_order_005",
            canonical_type="order",
            source="test_system",
            attributes={"status": "pending", "priority": "normal"},
        )

        # Act
        new_entity_id, new_version = entity_service.create_new_version(
            external_id="service_order_005",
            source="test_system",
            attributes={
                "status": "completed",
                "priority": "high",
                "completed_at": "2023-01-01T12:00:00Z",
            },
        )

        # Assert
        new_entity = entity_service.get_entity(new_entity_id)
        assert new_entity.version == 2
        assert new_entity.attributes["status"] == "completed"
        assert new_entity.attributes["priority"] == "high"
        assert new_entity.attributes["completed_at"] == "2023-01-01T12:00:00Z"

    def test_get_max_version_single_entity(self, entity_service, tenant_context):
        """Test getting max version for entity with single version."""
        # Arrange
        entity_service.create_entity(
            external_id="service_order_006", canonical_type="order", source="test_system", version=1
        )

        # Act
        max_version = entity_service.get_max_version("service_order_006", "test_system")

        # Assert
        assert max_version == 1

    def test_get_max_version_multiple_versions(self, entity_service, tenant_context):
        """Test getting max version for entity with multiple versions."""
        # Arrange - Create multiple versions
        external_id = "service_order_007"
        entity_service.create_entity(
            external_id=external_id, canonical_type="order", source="test_system", version=1
        )
        entity_service.create_new_version(external_id, "test_system", content={"v": 2})
        entity_service.create_new_version(external_id, "test_system", content={"v": 3})

        # Act
        max_version = entity_service.get_max_version(external_id, "test_system")

        # Assert
        assert max_version == 3

    def test_get_max_version_nonexistent_entity(self, entity_service, tenant_context):
        """Test getting max version for non-existent entity."""
        # Act
        max_version = entity_service.get_max_version("nonexistent_order", "test_system")

        # Assert
        assert max_version == 0


class TestEntityServiceRead:
    """Test entity read operations."""

    def test_get_entity_existing(self, entity_service, tenant_context):
        """Test retrieving existing entity by ID."""
        # Arrange
        entity_id = entity_service.create_entity(
            external_id="service_order_008",
            canonical_type="order",
            source="test_system",
            attributes={"status": "active"},
        )

        # Act
        retrieved_entity = entity_service.get_entity(entity_id)

        # Assert
        assert retrieved_entity is not None
        assert isinstance(retrieved_entity, EntityRead)
        assert retrieved_entity.id == entity_id
        assert retrieved_entity.external_id == "service_order_008"
        assert retrieved_entity.canonical_type == "order"
        assert retrieved_entity.source == "test_system"
        assert retrieved_entity.attributes["status"] == "active"
        assert isinstance(retrieved_entity.created_at, datetime)
        assert isinstance(retrieved_entity.updated_at, datetime)

    def test_get_entity_nonexistent(self, entity_service, tenant_context):
        """Test retrieving non-existent entity by ID."""
        # Arrange
        nonexistent_id = str(uuid.uuid4())

        # Act & Assert
        with pytest.raises(ServiceError):
            entity_service.get_entity(nonexistent_id)

    def test_get_entity_by_external_id_existing(self, entity_service, tenant_context):
        """Test retrieving entity by external ID."""
        # Arrange
        entity_id = entity_service.create_entity(
            external_id="service_order_009",
            canonical_type="order",
            source="test_system",
            attributes={"priority": "urgent"},
        )

        # Act
        retrieved_entity = entity_service.get_entity_by_external_id(
            external_id="service_order_009", source="test_system"
        )

        # Assert
        assert retrieved_entity is not None
        assert retrieved_entity.id == entity_id
        assert retrieved_entity.external_id == "service_order_009"
        assert retrieved_entity.attributes["priority"] == "urgent"

    def test_get_entity_by_external_id_nonexistent(self, entity_service, tenant_context):
        """Test retrieving non-existent entity by external ID."""
        # Act
        result = entity_service.get_entity_by_external_id(
            external_id="nonexistent_order", source="test_system"
        )

        # Assert
        assert result is None

    def test_get_entity_by_content_hash_existing(self, entity_service, tenant_context):
        """Test retrieving entity by content hash."""
        # Arrange
        content = {"unique_data": "test_hash_content"}
        entity_id = entity_service.create_entity(
            external_id="service_order_010",
            canonical_type="order",
            source="test_system",
            content=content,
        )

        # Get the created entity to find its content hash
        created_entity = entity_service.get_entity(entity_id)

        # Act
        retrieved_entity = entity_service.get_entity_by_content_hash(
            content_hash=created_entity.content_hash, source="test_system"
        )

        # Assert
        assert retrieved_entity is not None
        assert retrieved_entity.id == entity_id
        assert retrieved_entity.content_hash == created_entity.content_hash

    def test_get_entity_by_content_hash_nonexistent(self, entity_service, tenant_context):
        """Test retrieving entity by non-existent content hash."""
        # Act
        result = entity_service.get_entity_by_content_hash(
            content_hash="nonexistent_hash", source="test_system"
        )

        # Assert
        assert result is None


class TestEntityServiceExistence:
    """Test entity existence checking operations."""

    def test_check_entity_existence_exists(self, entity_service, tenant_context):
        """Test checking existence of existing entity."""
        # Arrange
        entity_service.create_entity(
            external_id="service_order_011", canonical_type="order", source="test_system"
        )

        # Act
        exists = entity_service.check_entity_existence("service_order_011", "test_system")

        # Assert
        assert exists is True

    def test_check_entity_existence_not_exists(self, entity_service, tenant_context):
        """Test checking existence of non-existent entity."""
        # Act
        exists = entity_service.check_entity_existence("nonexistent_order", "test_system")

        # Assert
        assert exists is False


class TestEntityServiceDelete:
    """Test entity deletion operations."""

    def test_delete_entity_success(self, entity_service, tenant_context):
        """Test successful entity deletion."""
        # Arrange
        entity_id = entity_service.create_entity(
            external_id="service_order_012", canonical_type="order", source="test_system"
        )

        # Verify entity exists
        entity = entity_service.get_entity(entity_id)
        assert entity is not None

        # Act
        success = entity_service.delete_entity(entity_id)

        # Assert
        assert success is True

        # Verify entity no longer exists
        with pytest.raises(ServiceError):
            entity_service.get_entity(entity_id)

    def test_delete_entity_nonexistent(self, entity_service, tenant_context):
        """Test deleting non-existent entity."""
        # Arrange
        nonexistent_id = str(uuid.uuid4())

        # Act
        success = entity_service.delete_entity(nonexistent_id)

        # Assert
        assert success is False


class TestEntityServiceTenantIsolation:
    """Test multi-tenant data isolation in service layer."""

    def test_tenant_isolation_service_operations(self, entity_service, multi_tenant_context):
        """Test that service operations respect tenant isolation."""
        # Arrange - Create entities for different tenants
        entity_data = {
            "external_id": "shared_service_order_001",
            "canonical_type": "order",
            "source": "shared_system",
        }

        tenant_entities = {}
        for i, tenant_data in enumerate(multi_tenant_context):
            # Set tenant context for this operation
            from src.context.tenant_context import TenantContext

            TenantContext.set_current_tenant(tenant_data["id"])

            entity_id = entity_service.create_entity(
                **entity_data, attributes={"tenant_specific": f"data_{i}"}
            )
            tenant_entities[tenant_data["id"]] = entity_id

        # Act & Assert - Verify each tenant can only access their own entity
        for tenant_data in multi_tenant_context:
            TenantContext.set_current_tenant(tenant_data["id"])

            try:
                # Should find entity for current tenant
                entity = entity_service.get_entity_by_external_id(
                    external_id="shared_service_order_001", source="shared_system"
                )
                assert entity is not None
                assert entity.tenant_id == tenant_data["id"]
                assert entity.id == tenant_entities[tenant_data["id"]]

                # Should NOT find entities from other tenants
                for other_tenant_data in multi_tenant_context:
                    if other_tenant_data["id"] != tenant_data["id"]:
                        with pytest.raises(ServiceError):
                            entity_service.get_entity(tenant_entities[other_tenant_data["id"]])

            finally:
                TenantContext.clear_current_tenant()


class TestEntityServiceErrorHandling:
    """Test error handling and edge cases."""

    def test_create_entity_invalid_tenant(self, entity_service, tenant_context):
        """Test creating entity with invalid tenant context."""
        from src.context.tenant_context import TenantContext

        # Store original tenant
        original_tenant = TenantContext.get_current_tenant_id()

        try:
            # Set invalid tenant context
            TenantContext.set_current_tenant("nonexistent_tenant")

            # Act & Assert
            with pytest.raises(ServiceError):
                entity_service.create_entity(
                    external_id="service_order_013", canonical_type="order", source="test_system"
                )

        finally:
            # Restore original tenant
            if original_tenant:
                TenantContext.set_current_tenant(original_tenant)
            else:
                TenantContext.clear_current_tenant()

    def test_service_error_conversion(self, entity_service, tenant_context):
        """Test that repository errors are converted to service errors."""
        # This test verifies the _handle_repo_error method works correctly
        # We'll test by trying to get a non-existent entity

        # Act & Assert
        with pytest.raises(ServiceError) as exc_info:
            entity_service.get_entity(str(uuid.uuid4()))

        # Verify error details
        assert exc_info.value.context.get("operation_name") is not None
        assert exc_info.value.context.get("tenant_id") is not None

    @pytest.mark.parametrize(
        "invalid_input",
        [
            {"external_id": "", "canonical_type": "order", "source": "test"},  # Empty external_id
            {"external_id": "test", "canonical_type": "", "source": "test"},  # Empty canonical_type
            {"external_id": "test", "canonical_type": "order", "source": ""},  # Empty source
        ],
    )
    def test_create_entity_validation_errors(self, entity_service, tenant_context, invalid_input):
        """Test creating entity with invalid input data."""
        # Act & Assert
        with pytest.raises((ServiceError, ValidationError, ValueError)):
            entity_service.create_entity(**invalid_input)
