"""
Comprehensive tests for EntityRepository.

Tests the entity repository data access layer using real SQLite database,
following the anti-mock philosophy with real database operations and
tenant isolation.
"""

import os

# Import models and schemas using our established path pattern
import sys
import uuid
from datetime import datetime
from typing import Any, Dict

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "..", "src"))

from schemas.entity_schema import EntityCreate, EntityFilter
from src.exceptions import RepositoryError
from src.repositories.entity_repository import EntityRepository

# ==================== ENTITY REPOSITORY TESTS ====================


class TestEntityRepositoryCreate:
    """Test entity creation operations."""

    def test_create_entity_success(self, entity_repository, tenant_context):
        """Test successful entity creation."""
        # Arrange
        entity_data = EntityCreate(
            external_id="test_order_001",
            tenant_id=tenant_context["id"],
            canonical_type="order",
            source="test_system",
            version=1,
            content_hash="abc123",
            attributes={"status": "RECEIVED", "priority": "normal"},
        )

        # Act
        entity_id = entity_repository.create(entity_data)

        # Assert
        assert entity_id is not None
        assert isinstance(entity_id, str)

        # Verify entity was created in database
        created_entity = entity_repository.get_by_id(entity_id)
        assert created_entity is not None
        assert created_entity.external_id == "test_order_001"
        assert created_entity.tenant_id == tenant_context["id"]
        assert created_entity.canonical_type == "order"
        assert created_entity.source == "test_system"
        assert created_entity.version == 1
        assert created_entity.content_hash == "abc123"
        assert created_entity.attributes["status"] == "RECEIVED"
        assert created_entity.attributes["priority"] == "normal"

    @pytest.mark.parametrize(
        "canonical_type,source,version",
        [
            ("order", "shopify", 1),
            ("product", "internal", 2),
            ("customer", "external_api", 5),
            ("inventory", "warehouse_system", 10),
        ],
    )
    def test_create_entity_various_types(
        self, entity_repository, tenant_context, canonical_type, source, version
    ):
        """Test creating entities of various types."""
        # Arrange
        entity_data = EntityCreate(
            external_id=f"test_{canonical_type}_001",
            tenant_id=tenant_context["id"],
            canonical_type=canonical_type,
            source=source,
            version=version,
            content_hash=f"hash_{canonical_type}",
            attributes={"type": canonical_type, "created_by": "test"},
        )

        # Act
        entity_id = entity_repository.create(entity_data)

        # Assert
        created_entity = entity_repository.get_by_id(entity_id)
        assert created_entity.canonical_type == canonical_type
        assert created_entity.source == source
        assert created_entity.version == version
        assert created_entity.content_hash == f"hash_{canonical_type}"

    def test_create_entity_with_null_attributes(self, entity_repository, tenant_context):
        """Test creating entity with null attributes."""
        # Arrange
        entity_data = EntityCreate(
            external_id="test_order_002",
            tenant_id=tenant_context["id"],
            canonical_type="order",
            source="test_system",
            version=1,
            content_hash="def456",
        )

        # Act
        entity_id = entity_repository.create(entity_data)

        # Assert
        created_entity = entity_repository.get_by_id(entity_id)
        assert created_entity.attributes is None or created_entity.attributes == {}

    def test_create_entity_with_complex_attributes(self, entity_repository, tenant_context):
        """Test creating entity with complex nested attributes."""
        # Arrange
        complex_attributes = {
            "status": "PROCESSING",
            "metadata": {
                "source_timestamp": "2023-01-01T00:00:00Z",
                "processing_rules": ["rule1", "rule2"],
                "nested": {"level": 2, "data": {"key": "value"}},
            },
            "tags": ["urgent", "priority"],
            "numeric_data": 123.45,
        }

        entity_data = EntityCreate(
            external_id="test_order_003",
            tenant_id=tenant_context["id"],
            canonical_type="order",
            source="test_system",
            version=1,
            content_hash="ghi789",
            attributes=complex_attributes,
        )

        # Act
        entity_id = entity_repository.create(entity_data)

        # Assert
        created_entity = entity_repository.get_by_id(entity_id)
        assert created_entity.attributes == complex_attributes
        assert created_entity.attributes["metadata"]["nested"]["data"]["key"] == "value"
        assert created_entity.attributes["numeric_data"] == 123.45


class TestEntityRepositoryRead:
    """Test entity read operations."""

    def test_get_by_id_existing_entity(self, entity_repository, tenant_context):
        """Test retrieving entity by ID."""
        # Arrange - Create entity first
        entity_data = EntityCreate(
            external_id="test_order_004",
            tenant_id=tenant_context["id"],
            canonical_type="order",
            source="test_system",
            version=1,
            content_hash="jkl012",
            attributes={"status": "RECEIVED"},
        )
        entity_id = entity_repository.create(entity_data)

        # Act
        retrieved_entity = entity_repository.get_by_id(entity_id)

        # Assert
        assert retrieved_entity is not None
        assert retrieved_entity.id == entity_id
        assert retrieved_entity.external_id == "test_order_004"
        assert retrieved_entity.tenant_id == tenant_context["id"]
        assert retrieved_entity.canonical_type == "order"
        assert retrieved_entity.source == "test_system"
        assert retrieved_entity.version == 1
        assert retrieved_entity.content_hash == "jkl012"
        assert retrieved_entity.attributes["status"] == "RECEIVED"
        assert isinstance(retrieved_entity.created_at, datetime)
        assert isinstance(retrieved_entity.updated_at, datetime)

    def test_get_by_id_nonexistent_entity(self, entity_repository, tenant_context):
        """Test retrieving non-existent entity by ID."""
        # Arrange
        nonexistent_id = str(uuid.uuid4())

        # Act
        result = entity_repository.get_by_id(nonexistent_id)

        # Assert
        assert result is None

    def test_get_by_external_id_existing_entity(self, entity_repository, tenant_context):
        """Test retrieving entity by external ID."""
        # Arrange - Create entity first
        entity_data = EntityCreate(
            external_id="test_order_005",
            tenant_id=tenant_context["id"],
            canonical_type="order",
            source="test_system",
            version=1,
            content_hash="mno345",
            attributes={"status": "PROCESSING"},
        )
        entity_id = entity_repository.create(entity_data)

        # Act
        retrieved_entity = entity_repository.get_by_external_id(
            external_id="test_order_005", source="test_system"
        )

        # Assert
        assert retrieved_entity is not None
        assert retrieved_entity.id == entity_id
        assert retrieved_entity.external_id == "test_order_005"
        assert retrieved_entity.source == "test_system"

    def test_get_by_external_id_nonexistent_entity(self, entity_repository, tenant_context):
        """Test retrieving non-existent entity by external ID."""
        # Act
        result = entity_repository.get_by_external_id(
            external_id="nonexistent_order", source="test_system"
        )

        # Assert
        assert result is None

    def test_get_by_content_hash_existing_entity(self, entity_repository, tenant_context):
        """Test retrieving entity by content hash."""
        # Arrange - Create entity first
        unique_hash = "unique_hash_123"
        entity_data = EntityCreate(
            external_id="test_order_006",
            tenant_id=tenant_context["id"],
            canonical_type="order",
            source="test_system",
            version=1,
            content_hash=unique_hash,
            attributes={"status": "COMPLETED"},
        )
        entity_id = entity_repository.create(entity_data)

        # Act
        retrieved_entity = entity_repository.get_by_content_hash(
            content_hash=unique_hash, source="test_system"
        )

        # Assert
        assert retrieved_entity is not None
        assert retrieved_entity.id == entity_id
        assert retrieved_entity.content_hash == unique_hash
        assert retrieved_entity.source == "test_system"


class TestEntityRepositoryVersioning:
    """Test entity versioning operations."""

    def test_get_max_version_single_entity(self, entity_repository, tenant_context):
        """Test getting max version for entity with single version."""
        # Arrange - Create entity
        entity_data = EntityCreate(
            external_id="test_order_007",
            tenant_id=tenant_context["id"],
            canonical_type="order",
            source="test_system",
            version=1,
            content_hash="pqr678",
            attributes={"status": "RECEIVED"},
        )
        entity_repository.create(entity_data)

        # Act
        max_version = entity_repository.get_max_version(
            external_id="test_order_007", source="test_system"
        )

        # Assert
        assert max_version == 1

    def test_get_max_version_multiple_versions(self, entity_repository, tenant_context):
        """Test getting max version for entity with multiple versions."""
        # Arrange - Create multiple versions of same entity
        external_id = "test_order_008"

        for version in [1, 2, 3, 5]:  # Test non-sequential versions
            entity_data = EntityCreate(
                external_id=external_id,
                tenant_id=tenant_context["id"],
                canonical_type="order",
                source="test_system",
                version=version,
                content_hash=f"hash_v{version}",
                attributes={"status": "RECEIVED", "version": version},
            )
            entity_repository.create(entity_data)

        # Act
        max_version = entity_repository.get_max_version(
            external_id=external_id, source="test_system"
        )

        # Assert
        assert max_version == 5

    def test_get_max_version_nonexistent_entity(self, entity_repository, tenant_context):
        """Test getting max version for non-existent entity."""
        # Act
        max_version = entity_repository.get_max_version(
            external_id="nonexistent_order", source="test_system"
        )

        # Assert
        assert max_version == 0

    def test_create_new_version_success(self, entity_repository, tenant_context):
        """Test creating new version of existing entity."""
        # Arrange - Create initial entity
        external_id = "test_order_009"
        initial_entity = EntityCreate(
            external_id=external_id,
            tenant_id=tenant_context["id"],
            canonical_type="order",
            source="test_system",
            version=1,
            content_hash="initial_hash",
            attributes={"status": "RECEIVED"},
        )
        entity_repository.create(initial_entity)

        # Act - Create new version
        new_entity_id, new_version = entity_repository.create_new_version(
            external_id=external_id,
            source="test_system",
            content_hash="updated_hash",
            attributes={"status": "PROCESSING", "updated": True},
        )

        # Assert
        assert new_entity_id is not None
        assert new_version == 2

        # Verify new entity was created
        new_entity = entity_repository.get_by_id(new_entity_id)
        assert new_entity.external_id == external_id
        assert new_entity.version == 2
        assert new_entity.content_hash == "updated_hash"
        assert new_entity.attributes["status"] == "PROCESSING"
        assert new_entity.attributes["updated"] is True

        # Verify max version is now 2
        max_version = entity_repository.get_max_version(external_id, "test_system")
        assert max_version == 2


class TestEntityRepositoryTenantIsolation:
    """Test multi-tenant data isolation."""

    def test_tenant_isolation_create_and_retrieve(self, entity_repository, multi_tenant_context):
        """Test that entities are isolated by tenant."""
        # Arrange - Create entities for different tenants
        entity_data_template = {
            "external_id": "shared_order_001",
            "canonical_type": "order",
            "source": "shared_system",
            "version": 1,
            "content_hash": "shared_hash",
        }

        tenant_entities = {}
        for i, tenant_data in enumerate(multi_tenant_context):
            # Set tenant context for this operation
            from src.context.tenant_context import TenantContext

            TenantContext.set_current_tenant(tenant_data["id"])

            entity_data = EntityCreate(
                **entity_data_template,
                tenant_id=tenant_data["id"],
                attributes={"status": "RECEIVED", "tenant_specific": f"data_{i}"},
            )
            entity_id = entity_repository.create(entity_data)
            tenant_entities[tenant_data["id"]] = entity_id

        # Act & Assert - Verify each tenant can only see their own entity
        for tenant_data in multi_tenant_context:
            # Set tenant context
            TenantContext.set_current_tenant(tenant_data["id"])

            try:
                # Should find entity for current tenant
                entity = entity_repository.get_by_external_id(
                    external_id="shared_order_001", source="shared_system"
                )
                assert entity is not None
                assert entity.tenant_id == tenant_data["id"]
                assert entity.id == tenant_entities[tenant_data["id"]]

                # Should NOT find entities from other tenants with same external_id
                for other_tenant_data in multi_tenant_context:
                    if other_tenant_data["id"] != tenant_data["id"]:
                        other_entity = entity_repository.get_by_id(
                            tenant_entities[other_tenant_data["id"]]
                        )
                        # Entity exists in database but not accessible through tenant-aware queries
                        assert (
                            other_entity is None
                        ), f"Tenant isolation failed: {tenant_data['id']} can see entity from {other_tenant_data['id']}"

            finally:
                TenantContext.clear_current_tenant()


class TestEntityRepositoryErrorHandling:
    """Test error handling and edge cases."""

    def test_create_with_invalid_tenant(self, entity_repository, tenant_context):
        """Test creating entity with invalid tenant ID in context."""
        from src.context.tenant_context import TenantContext

        # Arrange - Set tenant context to non-existent tenant
        TenantContext.set_current_tenant("nonexistent_tenant")

        try:
            entity_data = EntityCreate(
                external_id="test_order_010",
                tenant_id="any_value",  # This will be ignored - context is used
                canonical_type="order",
                source="test_system",
                version=1,
                content_hash="error_hash",
                attributes={"status": "RECEIVED"},
            )

            # Act & Assert - Should raise RepositoryError for invalid tenant
            with pytest.raises(RepositoryError, match="Invalid tenant or reference"):
                entity_repository.create(entity_data)

        finally:
            # Restore original tenant context
            TenantContext.set_current_tenant(tenant_context["id"])

    def test_delete_existing_entity(self, entity_repository, tenant_context):
        """Test deleting an existing entity."""
        # Arrange - Create entity first
        entity_data = EntityCreate(
            external_id="test_order_011",
            tenant_id=tenant_context["id"],
            canonical_type="order",
            source="test_system",
            version=1,
            content_hash="delete_hash",
            attributes={"status": "RECEIVED"},
        )
        entity_id = entity_repository.create(entity_data)

        # Verify entity exists
        assert entity_repository.get_by_id(entity_id) is not None

        # Act
        success = entity_repository.delete(entity_id)

        # Assert
        assert success is True
        assert entity_repository.get_by_id(entity_id) is None

    def test_delete_nonexistent_entity(self, entity_repository, tenant_context):
        """Test deleting non-existent entity."""
        # Arrange
        nonexistent_id = str(uuid.uuid4())

        # Act
        success = entity_repository.delete(nonexistent_id)

        # Assert
        assert success is False

    @pytest.mark.parametrize(
        "invalid_data",
        [
            # Missing required fields
            {"external_id": "test", "canonical_type": "order"},  # Missing tenant_id
            {"tenant_id": "test", "canonical_type": "order"},  # Missing external_id
            {"external_id": "test", "tenant_id": "test"},  # Missing canonical_type
        ],
    )
    def test_create_with_missing_required_fields(self, entity_repository, invalid_data):
        """Test creating entity with missing required fields."""
        # Act & Assert
        with pytest.raises((RepositoryError, ValueError)):
            entity_repository.create(EntityCreate(**invalid_data))
