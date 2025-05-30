"""
Extended tests for EntityRepository covering error paths and edge cases.

This module tests the remaining uncovered lines in entity_repository.py,
focusing on error handling and specific conditional branches.
"""

import pytest

from src.exceptions import ErrorCode, RepositoryError
from src.repositories.entity_repository import EntityRepository
from src.schemas.entity_schema import EntityCreate, EntityUpdate, EntityFilter


class TestEntityRepositoryErrorPaths:
    """Test error handling paths in EntityRepository."""

    def test_require_by_id_not_found(self, entity_repository, tenant_context):
        """Test require_by_id raises RepositoryError when entity doesn't exist."""
        with pytest.raises(RepositoryError) as exc_info:
            entity_repository.require_by_id("non-existent-id")
        
        # Verify the exception contains expected context
        assert exc_info.value.error_code == ErrorCode.NOT_FOUND
        assert "non-existent-id" in str(exc_info.value)
        assert tenant_context["id"] in str(exc_info.value)

    def test_require_by_external_id_not_found(self, entity_repository, tenant_context):
        """Test require_by_external_id raises RepositoryError when entity doesn't exist."""
        with pytest.raises(RepositoryError) as exc_info:
            entity_repository.require_by_external_id("non-existent-ext-id", "test_source")
        
        assert exc_info.value.error_code == ErrorCode.NOT_FOUND
        assert "non-existent-ext-id" in str(exc_info.value)
        assert "test_source" in str(exc_info.value)

    def test_get_by_external_id_all_versions(self, entity_repository, tenant_context):
        """Test get_by_external_id with all_versions=True."""
        # Create an entity
        entity_data = EntityCreate(
            tenant_id=tenant_context["id"],
            external_id="multi-version-001",
            canonical_type="test_type",
            source="test_source",
            content_hash="hash1",
            attributes={"name": "Original"}
        )
        entity1_id = entity_repository.create(entity_data)
        
        # Create a new version
        entity2_id, version = entity_repository.create_new_version(
            external_id="multi-version-001",
            source="test_source",
            content_hash="hash2",
            attributes={"name": "Updated"}
        )
        
        # Get all versions
        all_versions = entity_repository.get_by_external_id(
            external_id="multi-version-001",
            source="test_source",
            all_versions=True
        )
        
        assert len(all_versions) == 2
        assert all_versions[0].version == 1
        assert all_versions[1].version == 2
        assert all_versions[0].attributes["name"] == "Original"
        assert all_versions[1].attributes["name"] == "Updated"

    def test_get_by_external_id_specific_version(self, entity_repository, tenant_context):
        """Test get_by_external_id with specific version."""
        # Create entity with multiple versions
        entity_data = EntityCreate(
            tenant_id=tenant_context["id"],
            external_id="version-test-001",
            canonical_type="test_type",
            source="test_source",
            content_hash="hash1",
            attributes={"data": "v1"}
        )
        entity1_id = entity_repository.create(entity_data)
        
        entity2_id, version2 = entity_repository.create_new_version(
            external_id="version-test-001",
            source="test_source",
            content_hash="hash2",
            attributes={"data": "v2"}
        )
        
        # Get specific version
        v1_entity = entity_repository.get_by_external_id(
            external_id="version-test-001",
            source="test_source",
            version=1
        )
        
        assert v1_entity is not None
        assert v1_entity.version == 1
        assert v1_entity.attributes["data"] == "v1"
        
        # Test non-existent version
        v3_entity = entity_repository.get_by_external_id(
            external_id="version-test-001",
            source="test_source",
            version=3
        )
        assert v3_entity is None

    def test_get_by_content_hash_not_found(self, entity_repository, tenant_context):
        """Test get_by_content_hash returns None when no match."""
        result = entity_repository.get_by_content_hash(
            content_hash="non-existent-hash",
            source="test_source"
        )
        assert result is None

    def test_list_entities_with_filters(self, entity_repository, tenant_context):
        """Test list_entities with various filter combinations."""
        # Create test entities
        entity_ids = []
        for i in range(5):
            entity_data = EntityCreate(
                tenant_id=tenant_context["id"],
                external_id=f"list-test-{i:03d}",
                canonical_type="customer" if i % 2 == 0 else "order",
                source="source_a" if i < 3 else "source_b",
                content_hash=f"hash{i}",
                attributes={"index": i}
            )
            entity_ids.append(entity_repository.create(entity_data))
        
        # Test canonical_type filter  
        customers, total = entity_repository.list(EntityFilter(canonical_type="customer"))
        assert len(customers) == 3  # indices 0, 2, 4
        
        # Test source filter
        source_a_entities, total = entity_repository.list(EntityFilter(source="source_a"))
        assert len(source_a_entities) == 3  # indices 0, 1, 2
        
        # Test combined filters
        customer_source_a, total = entity_repository.list(
            EntityFilter(canonical_type="customer", source="source_a")
        )
        assert len(customer_source_a) == 2  # indices 0, 2

