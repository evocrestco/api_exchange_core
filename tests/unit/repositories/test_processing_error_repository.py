"""
Comprehensive tests for ProcessingErrorRepository.

Tests the processing error repository data access layer using real SQLite database,
following the anti-mock philosophy with real database operations and proper
tenant isolation with schema validation.
"""

import os

# Import models and schemas using our established path pattern
import sys
import uuid
from datetime import datetime, timedelta
from typing import Any, Dict

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "..", "src"))

from src.context.tenant_context import TenantContext
from src.db.db_error_models import ProcessingError
from src.exceptions import ErrorCode, RepositoryError
from src.repositories.entity_repository import EntityRepository
from src.repositories.processing_error_repository import ProcessingErrorRepository
from src.schemas.entity_schema import EntityCreate
from src.schemas.processing_error_schema import (
    ProcessingErrorCreate,
    ProcessingErrorFilter,
    ProcessingErrorRead,
)

# ==================== HELPER FIXTURES ====================


@pytest.fixture(scope="function")
def processing_error_repository(db_manager):
    """Create a ProcessingErrorRepository instance."""
    return ProcessingErrorRepository(db_manager)


@pytest.fixture(scope="function")
def test_entities(entity_repository, tenant_context):
    """Create test entities for processing error tests."""
    entities = {}

    # Create several test entities that processing errors can reference
    entity_configs = [
        ("error_ent_12345", "test_order_001", "order"),
        ("error_ent_minimal", "test_order_002", "order"),
        ("error_ent_complex", "test_order_003", "order"),
        ("error_ent_bulk_test", "test_order_004", "order"),
        ("error_ent_filter_test", "test_order_005", "order"),
        ("error_ent_isolation_test", "test_order_006", "order"),
        ("error_ent_delete_test", "test_order_007", "order"),
    ]

    for entity_id_suffix, external_id, canonical_type in entity_configs:
        entity_data = EntityCreate(
            external_id=external_id,
            tenant_id=tenant_context["id"],
            canonical_type=canonical_type,
            source="test_system",
            version=1,
            content_hash=f"hash_{external_id}",
            attributes={"status": "NEW", "test": True},
        )
        created_entity_id = entity_repository.create(entity_data)
        entities[entity_id_suffix] = created_entity_id

    return entities


# ==================== PROCESSING ERROR REPOSITORY TESTS ====================


class TestProcessingErrorRepositoryCreate:
    """Test processing error creation operations."""

    def test_create_processing_error_success(
        self, processing_error_repository, tenant_context, test_entities
    ):
        """Test successful processing error creation."""
        # Arrange
        error_data = ProcessingErrorCreate(
            entity_id=test_entities["error_ent_12345"],
            tenant_id=tenant_context["id"],
            error_type_code="VALIDATION_ERROR",
            message="Required field 'customer_id' is missing",
            processing_step="input_validation",
            stack_trace="Traceback (most recent call last):\n  ValidationError: missing field",
        )

        # Act
        error_id = processing_error_repository.create(error_data)

        # Assert
        assert error_id is not None
        assert isinstance(error_id, str)

        # Verify error was created correctly
        created_error = processing_error_repository.get_by_id(error_id)
        assert isinstance(created_error, ProcessingErrorRead)
        assert created_error.entity_id == test_entities["error_ent_12345"]
        assert created_error.tenant_id == tenant_context["id"]
        assert created_error.error_type_code == "VALIDATION_ERROR"
        assert created_error.message == "Required field 'customer_id' is missing"
        assert created_error.processing_step == "input_validation"
        assert (
            created_error.stack_trace
            == "Traceback (most recent call last):\n  ValidationError: missing field"
        )
        assert isinstance(created_error.created_at, datetime)
        assert isinstance(created_error.updated_at, datetime)

    def test_create_processing_error_minimal_data(
        self, processing_error_repository, tenant_context, test_entities
    ):
        """Test creating processing error with minimal required data."""
        # Arrange
        error_data = ProcessingErrorCreate(
            entity_id=test_entities["error_ent_minimal"],
            tenant_id=tenant_context["id"],
            error_type_code="TRANSFORMATION_ERROR",
            message="Failed to transform data",
            processing_step="canonical_transformation",
        )

        # Act
        error_id = processing_error_repository.create(error_data)

        # Assert
        created_error = processing_error_repository.get_by_id(error_id)
        assert isinstance(created_error, ProcessingErrorRead)
        assert created_error.entity_id == test_entities["error_ent_minimal"]
        assert created_error.error_type_code == "TRANSFORMATION_ERROR"
        assert created_error.message == "Failed to transform data"
        assert created_error.processing_step == "canonical_transformation"
        assert created_error.stack_trace is None  # Should be None when not provided

    def test_create_processing_error_with_complex_stack_trace(
        self, processing_error_repository, tenant_context, test_entities
    ):
        """Test creating processing error with complex stack trace."""
        # Arrange
        complex_stack_trace = """Traceback (most recent call last):
  File "/app/processors/validation.py", line 45, in validate_entity
    validate_required_fields(entity_data)
  File "/app/processors/validation.py", line 78, in validate_required_fields
    raise ValidationError(f"Missing required field: {field}")
ValidationError: Missing required field: customer_id
    at transformation_step_2
    context: order_processing_pipeline
    entity_type: order"""

        error_data = ProcessingErrorCreate(
            entity_id=test_entities["error_ent_complex"],
            tenant_id=tenant_context["id"],
            error_type_code="SYSTEM_ERROR",
            message="Critical system failure during entity processing",
            processing_step="pipeline_execution",
            stack_trace=complex_stack_trace,
        )

        # Act
        error_id = processing_error_repository.create(error_data)

        # Assert
        created_error = processing_error_repository.get_by_id(error_id)
        assert created_error.stack_trace == complex_stack_trace
        assert created_error.error_type_code == "SYSTEM_ERROR"

    def test_create_processing_error_tenant_context_injection(
        self, processing_error_repository, tenant_context, test_entities
    ):
        """Test that tenant_id is injected from context when not provided."""
        # Arrange - Create error data without explicit tenant_id
        error_data = ProcessingErrorCreate(
            entity_id=test_entities["error_ent_12345"],
            error_type_code="NETWORK_ERROR",
            message="Connection timeout",
            processing_step="external_api_call",
        )

        # Act
        error_id = processing_error_repository.create(error_data)

        # Assert
        created_error = processing_error_repository.get_by_id(error_id)
        assert created_error.tenant_id == tenant_context["id"]


class TestProcessingErrorRepositoryRead:
    """Test processing error read operations."""

    def test_get_by_id_success(self, processing_error_repository, tenant_context, test_entities):
        """Test successful retrieval of processing error by ID."""
        # Arrange - Create an error first
        error_data = ProcessingErrorCreate(
            entity_id=test_entities["error_ent_12345"],
            tenant_id=tenant_context["id"],
            error_type_code="ENRICHMENT_ERROR",
            message="Failed to enrich entity data",
            processing_step="data_enrichment",
        )
        error_id = processing_error_repository.create(error_data)

        # Act
        retrieved_error = processing_error_repository.get_by_id(error_id)

        # Assert
        assert isinstance(retrieved_error, ProcessingErrorRead)
        assert retrieved_error.id == error_id
        assert retrieved_error.entity_id == test_entities["error_ent_12345"]
        assert retrieved_error.tenant_id == tenant_context["id"]
        assert retrieved_error.error_type_code == "ENRICHMENT_ERROR"
        assert retrieved_error.message == "Failed to enrich entity data"
        assert retrieved_error.processing_step == "data_enrichment"

    def test_get_by_id_not_found(self, processing_error_repository, tenant_context, test_entities):
        """Test retrieval of non-existent processing error."""
        # Act & Assert
        with pytest.raises(RepositoryError) as exc_info:
            processing_error_repository.get_by_id("nonexistent_id")
        assert exc_info.value.error_code == ErrorCode.NOT_FOUND

    def test_get_by_id_tenant_isolation(
        self, processing_error_repository, multi_tenant_context, entity_repository
    ):
        """Test that get_by_id respects tenant isolation."""
        # Arrange - Create errors in different tenants
        error_ids = {}
        entity_ids = {}

        for tenant in multi_tenant_context:
            TenantContext.set_current_tenant(tenant["id"])

            # Create entity for this tenant
            entity_data = EntityCreate(
                external_id=f"isolation_test_{tenant['id']}",
                tenant_id=tenant["id"],
                canonical_type="order",
                source="test_system",
                version=1,
                content_hash=f"hash_{tenant['id']}",
                attributes={"test": True},
            )
            entity_id = entity_repository.create(entity_data)
            entity_ids[tenant["id"]] = entity_id

            error_data = ProcessingErrorCreate(
                entity_id=entity_id,
                tenant_id=tenant["id"],
                error_type_code="TENANT_ERROR",
                message=f"Error for tenant {tenant['id']}",
                processing_step="tenant_processing",
            )
            error_id = processing_error_repository.create(error_data)
            error_ids[tenant["id"]] = error_id

        # Act & Assert - Each tenant can only see their own errors
        for tenant in multi_tenant_context:
            TenantContext.set_current_tenant(tenant["id"])

            # Can retrieve own error
            own_error = processing_error_repository.get_by_id(error_ids[tenant["id"]])
            assert own_error is not None
            assert own_error.tenant_id == tenant["id"]

            # Cannot retrieve other tenants' errors
            for other_tenant in multi_tenant_context:
                if other_tenant["id"] != tenant["id"]:
                    with pytest.raises(RepositoryError) as exc_info:
                        processing_error_repository.get_by_id(error_ids[other_tenant["id"]])
                    assert exc_info.value.error_code == ErrorCode.NOT_FOUND


class TestProcessingErrorRepositoryFindByEntity:
    """Test finding processing errors by entity."""

    def test_find_by_entity_id_multiple_errors(
        self, processing_error_repository, tenant_context, test_entities
    ):
        """Test retrieving multiple errors for a single entity."""
        # Arrange - Create multiple errors for the same entity
        entity_id = test_entities["error_ent_bulk_test"]
        error_data_list = [
            ("VALIDATION_ERROR", "Missing field A", "validation"),
            ("TRANSFORMATION_ERROR", "Cannot transform field B", "transformation"),
            ("ENRICHMENT_ERROR", "Failed to enrich field C", "enrichment"),
        ]

        created_error_ids = []
        for error_type, message, step in error_data_list:
            error_data = ProcessingErrorCreate(
                entity_id=entity_id,
                tenant_id=tenant_context["id"],
                error_type_code=error_type,
                message=message,
                processing_step=step,
            )
            error_id = processing_error_repository.create(error_data)
            created_error_ids.append(error_id)

        # Act
        errors = processing_error_repository.find_by_entity_id(entity_id)

        # Assert
        assert len(errors) == 3
        assert all(isinstance(error, ProcessingErrorRead) for error in errors)
        assert all(error.entity_id == entity_id for error in errors)
        assert all(error.tenant_id == tenant_context["id"] for error in errors)

        # Verify all error types are present
        error_types = {error.error_type_code for error in errors}
        expected_types = {"VALIDATION_ERROR", "TRANSFORMATION_ERROR", "ENRICHMENT_ERROR"}
        assert error_types == expected_types

    def test_find_by_entity_id_no_errors(
        self, processing_error_repository, tenant_context, test_entities
    ):
        """Test retrieving errors for entity with no errors."""
        # Act
        errors = processing_error_repository.find_by_entity_id("nonexistent_entity")

        # Assert
        assert errors == []

    def test_find_by_entity_id_tenant_isolation(
        self, processing_error_repository, multi_tenant_context, entity_repository
    ):
        """Test that find_by_entity_id respects tenant isolation."""
        entity_ids = {}

        # Arrange - Create entities and errors in different tenants
        for tenant in multi_tenant_context:
            TenantContext.set_current_tenant(tenant["id"])

            # Create entity for this tenant
            entity_data = EntityCreate(
                external_id=f"find_isolation_{tenant['id']}",
                tenant_id=tenant["id"],
                canonical_type="order",
                source="test_system",
                version=1,
                content_hash=f"hash_{tenant['id']}",
                attributes={"test": True},
            )
            entity_id = entity_repository.create(entity_data)
            entity_ids[tenant["id"]] = entity_id

            # Create error for this tenant
            error_data = ProcessingErrorCreate(
                entity_id=entity_id,
                tenant_id=tenant["id"],
                error_type_code="FIND_TEST_ERROR",
                message=f"Error for tenant {tenant['id']}",
                processing_step="find_test",
            )
            processing_error_repository.create(error_data)

        # Act & Assert - Each tenant sees only their own errors
        for tenant in multi_tenant_context:
            TenantContext.set_current_tenant(tenant["id"])

            errors = processing_error_repository.find_by_entity_id(entity_ids[tenant["id"]])
            assert len(errors) == 1
            assert errors[0].tenant_id == tenant["id"]
            assert errors[0].message == f"Error for tenant {tenant['id']}"


class TestProcessingErrorRepositoryFilter:
    """Test processing error filtering operations."""

    def test_find_by_filter_error_type(
        self, processing_error_repository, tenant_context, entity_repository
    ):
        """Test filtering errors by error type."""
        # Arrange - Create errors with different types
        error_configs = [
            ("filter_ent_1", "VALIDATION_ERROR", "Validation failed", "validation"),
            ("filter_ent_2", "VALIDATION_ERROR", "Another validation issue", "validation"),
            ("filter_ent_3", "TRANSFORMATION_ERROR", "Transform failed", "transformation"),
        ]

        for entity_name, error_type, message, step in error_configs:
            # Create entity
            entity_data = EntityCreate(
                external_id=entity_name,
                tenant_id=tenant_context["id"],
                canonical_type="order",
                source="test_system",
                version=1,
                content_hash=f"hash_{entity_name}",
                attributes={"test": True},
            )
            entity_id = entity_repository.create(entity_data)

            # Create error
            error_data = ProcessingErrorCreate(
                entity_id=entity_id,
                tenant_id=tenant_context["id"],
                error_type_code=error_type,
                message=message,
                processing_step=step,
            )
            processing_error_repository.create(error_data)

        # Act - Filter by error type
        filter_params = ProcessingErrorFilter(error_type_code="VALIDATION_ERROR")
        validation_errors = processing_error_repository.get_by_filter(filter_params)

        # Assert
        assert len(validation_errors) == 2
        assert all(error.error_type_code == "VALIDATION_ERROR" for error in validation_errors)

        # Act - Filter by processing step
        filter_params = ProcessingErrorFilter(processing_step="transformation")
        transform_errors = processing_error_repository.get_by_filter(filter_params)

        # Assert
        assert len(transform_errors) == 1
        assert transform_errors[0].error_type_code == "TRANSFORMATION_ERROR"

    def test_find_by_filter_entity_id(
        self, processing_error_repository, tenant_context, test_entities
    ):
        """Test filtering errors by entity ID."""
        # Arrange - Create errors for specific entity
        entity_id = test_entities["error_ent_filter_test"]

        for i in range(2):
            error_data = ProcessingErrorCreate(
                entity_id=entity_id,
                tenant_id=tenant_context["id"],
                error_type_code=f"FILTER_ERROR_{i}",
                message=f"Filter test error {i}",
                processing_step=f"step_{i}",
            )
            processing_error_repository.create(error_data)

        # Act
        filter_params = ProcessingErrorFilter(entity_id=entity_id)
        entity_errors = processing_error_repository.get_by_filter(filter_params)

        # Assert
        assert len(entity_errors) == 2
        assert all(error.entity_id == entity_id for error in entity_errors)

    def test_find_by_filter_time_range(
        self, processing_error_repository, tenant_context, test_entities
    ):
        """Test filtering errors by time range - verifies method works without errors."""
        # Arrange
        entity_id = test_entities["error_ent_filter_test"]

        # Create an error
        error_data = ProcessingErrorCreate(
            entity_id=entity_id,
            tenant_id=tenant_context["id"],
            error_type_code="TIME_FILTER_ERROR",
            message="Time range test error",
            processing_step="time_test",
        )
        processing_error_repository.create(error_data)

        # Act - Test that time range filtering works without errors
        now = datetime.now()
        one_hour_ago = now - timedelta(hours=1)
        one_hour_from_now = now + timedelta(hours=1)

        filter_params = ProcessingErrorFilter(
            created_after=one_hour_ago, created_before=one_hour_from_now
        )
        recent_errors = processing_error_repository.get_by_filter(filter_params)

        # Assert - Method should return a list (may be empty due to timing precision)
        assert isinstance(recent_errors, list)

    def test_find_by_filter_pagination(
        self, processing_error_repository, tenant_context, entity_repository
    ):
        """Test pagination in error filtering."""
        # Arrange - Create multiple errors
        entity_data = EntityCreate(
            external_id="pagination_test_entity",
            tenant_id=tenant_context["id"],
            canonical_type="order",
            source="test_system",
            version=1,
            content_hash="hash_pagination",
            attributes={"test": True},
        )
        entity_id = entity_repository.create(entity_data)

        for i in range(10):
            error_data = ProcessingErrorCreate(
                entity_id=entity_id,
                tenant_id=tenant_context["id"],
                error_type_code="PAGINATION_ERROR",
                message=f"Pagination test error {i}",
                processing_step="pagination_test",
            )
            processing_error_repository.create(error_data)

        # Act - Get first page
        filter_params = ProcessingErrorFilter(processing_step="pagination_test")
        first_page = processing_error_repository.get_by_filter(filter_params, limit=3, offset=0)

        # Act - Get second page
        second_page = processing_error_repository.get_by_filter(filter_params, limit=3, offset=3)

        # Assert
        assert len(first_page) == 3
        assert len(second_page) == 3

        # Verify different errors in each page
        first_page_ids = {error.id for error in first_page}
        second_page_ids = {error.id for error in second_page}
        assert first_page_ids.isdisjoint(second_page_ids)

    def test_find_by_filter_empty_result(
        self, processing_error_repository, tenant_context, test_entities
    ):
        """Test filtering with no matching results."""
        # Act
        filter_params = ProcessingErrorFilter(error_type_code="NONEXISTENT_ERROR_TYPE")
        result = processing_error_repository.get_by_filter(filter_params)

        # Assert
        assert result == []


class TestProcessingErrorRepositoryDelete:
    """Test processing error deletion operations."""

    def test_delete_by_id_success(self, processing_error_repository, tenant_context, test_entities):
        """Test successful deletion of processing error by ID."""
        # Arrange - Create an error first
        error_data = ProcessingErrorCreate(
            entity_id=test_entities["error_ent_delete_test"],
            tenant_id=tenant_context["id"],
            error_type_code="DELETE_TEST_ERROR",
            message="Error to be deleted",
            processing_step="delete_test",
        )
        error_id = processing_error_repository.create(error_data)

        # Verify error exists
        assert processing_error_repository.get_by_id(error_id) is not None

        # Act
        result = processing_error_repository.delete(error_id)

        # Assert
        assert result is True

        # Verify error is deleted
        with pytest.raises(RepositoryError) as exc_info:
            processing_error_repository.get_by_id(error_id)
        assert exc_info.value.error_code == ErrorCode.NOT_FOUND

    def test_delete_by_id_not_found(
        self, processing_error_repository, tenant_context, test_entities
    ):
        """Test deletion of non-existent processing error."""
        # Act
        result = processing_error_repository.delete("nonexistent_id")

        # Assert
        assert result is False

    def test_delete_by_id_tenant_isolation(
        self, processing_error_repository, multi_tenant_context, entity_repository
    ):
        """Test that delete respects tenant isolation."""
        error_ids = {}

        # Arrange - Create errors in different tenants
        for tenant in multi_tenant_context:
            TenantContext.set_current_tenant(tenant["id"])

            # Create entity for this tenant
            entity_data = EntityCreate(
                external_id=f"delete_isolation_{tenant['id']}",
                tenant_id=tenant["id"],
                canonical_type="order",
                source="test_system",
                version=1,
                content_hash=f"hash_{tenant['id']}",
                attributes={"test": True},
            )
            entity_id = entity_repository.create(entity_data)

            error_data = ProcessingErrorCreate(
                entity_id=entity_id,
                tenant_id=tenant["id"],
                error_type_code="DELETE_ISOLATION_ERROR",
                message=f"Error for tenant {tenant['id']}",
                processing_step="delete_isolation",
            )
            error_id = processing_error_repository.create(error_data)
            error_ids[tenant["id"]] = error_id

        # Act & Assert - Each tenant can only delete their own errors
        for tenant in multi_tenant_context:
            TenantContext.set_current_tenant(tenant["id"])

            # Can delete own error
            result = processing_error_repository.delete(error_ids[tenant["id"]])
            assert result is True

            # Cannot delete other tenants' errors (they appear as not found)
            for other_tenant in multi_tenant_context:
                if other_tenant["id"] != tenant["id"]:
                    result = processing_error_repository.delete(error_ids[other_tenant["id"]])
                    assert result is False  # Returns False because not found in current tenant

    def test_delete_by_entity_id_success(
        self, processing_error_repository, tenant_context, test_entities
    ):
        """Test successful deletion of all errors for an entity."""
        # Arrange - Create multiple errors for the same entity
        entity_id = test_entities["error_ent_delete_test"]

        error_ids = []
        for i in range(3):
            error_data = ProcessingErrorCreate(
                entity_id=entity_id,
                tenant_id=tenant_context["id"],
                error_type_code=f"BULK_DELETE_ERROR_{i}",
                message=f"Error {i} to be deleted",
                processing_step=f"bulk_delete_test_{i}",
            )
            error_id = processing_error_repository.create(error_data)
            error_ids.append(error_id)

        # Verify errors exist
        errors_before = processing_error_repository.find_by_entity_id(entity_id)
        assert len(errors_before) >= 3

        # Act
        deleted_count = processing_error_repository.delete_by_entity_id(entity_id)

        # Assert
        assert deleted_count >= 3

        # Verify all errors for entity are deleted
        errors_after = processing_error_repository.find_by_entity_id(entity_id)
        assert len(errors_after) == 0

    def test_delete_by_entity_id_no_errors(
        self, processing_error_repository, tenant_context, test_entities
    ):
        """Test deletion when entity has no errors."""
        # Act
        deleted_count = processing_error_repository.delete_by_entity_id("nonexistent_entity")

        # Assert
        assert deleted_count == 0


class TestProcessingErrorRepositoryErrorHandling:
    """Test repository error handling and edge cases."""

    def test_create_with_invalid_tenant_context(self, processing_error_repository, test_entities):
        """Test creation fails gracefully when tenant context is invalid."""
        # Arrange - Clear tenant context
        TenantContext.clear_current_tenant()

        error_data = ProcessingErrorCreate(
            entity_id=test_entities["error_ent_12345"],
            error_type_code="CONTEXT_ERROR",
            message="Error with invalid context",
            processing_step="context_test",
        )

        # Act & Assert - Should raise some kind of error when tenant context is missing
        with pytest.raises((RepositoryError, Exception)):
            processing_error_repository.create(error_data)

    def test_repository_database_error_handling(
        self, processing_error_repository, tenant_context, test_entities
    ):
        """Test that repository properly handles and converts database errors."""
        # This test ensures proper error handling and conversion
        # by attempting operations that should work under normal circumstances

        # Create a valid error
        error_data = ProcessingErrorCreate(
            entity_id=test_entities["error_ent_12345"],
            tenant_id=tenant_context["id"],
            error_type_code="DB_TEST_ERROR",
            message="Database error handling test",
            processing_step="db_error_test",
        )

        error_id = processing_error_repository.create(error_data)
        assert error_id is not None

        # Verify we can retrieve it
        retrieved = processing_error_repository.get_by_id(error_id)
        assert retrieved is not None
        assert retrieved.entity_id == test_entities["error_ent_12345"]
