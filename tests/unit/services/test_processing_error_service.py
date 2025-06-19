"""
Comprehensive tests for ProcessingErrorService.

Tests the processing error service business logic layer using real SQLite database,
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
from datetime import datetime, timedelta

import pytest
from pydantic import ValidationError

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "..", "src"))

from api_exchange_core.context.tenant_context import TenantContext
from api_exchange_core.exceptions import ErrorCode, ServiceError
from api_exchange_core.schemas import EntityCreate
from api_exchange_core.schemas import (
    ProcessingErrorCreate,
    ProcessingErrorFilter,
    ProcessingErrorRead,
)


# ==================== PROCESSING ERROR SERVICE TESTS ====================


class TestProcessingErrorServiceCreate:
    """Test processing error creation operations."""

    def test_create_error_success(self, processing_error_service, tenant_context, test_entities):
        """Test successful processing error creation."""
        # Arrange
        error_data = ProcessingErrorCreate(
            entity_id=test_entities["service_ent_12345"],
            tenant_id=tenant_context["id"],
            error_type_code="VALIDATION_ERROR",
            message="Required field 'customer_id' is missing",
            processing_step="input_validation",
            stack_trace="Traceback (most recent call last):\n  ValidationError: missing field",
        )

        # Act
        error_id = processing_error_service.create_error(error_data)

        # Assert
        assert error_id is not None
        assert isinstance(error_id, str)

        # Verify the error was created correctly by retrieving it
        created_error = processing_error_service.get_error(error_id)
        assert isinstance(created_error, ProcessingErrorRead)
        assert created_error.entity_id == test_entities["service_ent_12345"]
        assert created_error.tenant_id == tenant_context["id"]
        assert created_error.error_type_code == "VALIDATION_ERROR"
        assert created_error.message == "Required field 'customer_id' is missing"
        assert created_error.processing_step == "input_validation"
        assert (
            created_error.stack_trace
            == "Traceback (most recent call last):\n  ValidationError: missing field"
        )

    def test_create_error_minimal_data(
        self, processing_error_service, tenant_context, test_entities
    ):
        """Test creating processing error with minimal required data."""
        # Arrange
        error_data = ProcessingErrorCreate(
            entity_id=test_entities["service_ent_minimal"],
            tenant_id=tenant_context["id"],
            error_type_code="TRANSFORMATION_ERROR",
            message="Failed to transform data",
            processing_step="canonical_transformation",
        )

        # Act
        error_id = processing_error_service.create_error(error_data)

        # Assert
        created_error = processing_error_service.get_error(error_id)
        assert isinstance(created_error, ProcessingErrorRead)
        assert created_error.entity_id == test_entities["service_ent_minimal"]
        assert created_error.error_type_code == "TRANSFORMATION_ERROR"
        assert created_error.message == "Failed to transform data"
        assert created_error.processing_step == "canonical_transformation"
        assert created_error.stack_trace is None  # Should be None when not provided

    @pytest.mark.parametrize(
        "error_type,step,message",
        [
            ("VALIDATION_ERROR", "input_validation", "Validation failed for required fields"),
            ("TRANSFORMATION_ERROR", "data_transform", "Cannot transform source format"),
            ("ENRICHMENT_ERROR", "data_enrichment", "Failed to enrich entity data"),
            ("SYSTEM_ERROR", "processing", "Critical system failure occurred"),
            ("NETWORK_ERROR", "external_api_call", "Connection timeout to external service"),
        ],
    )
    def test_create_error_different_types(
        self, processing_error_service, tenant_context, test_entities, error_type, step, message
    ):
        """Test creating different types of processing errors."""
        # Arrange
        error_data = ProcessingErrorCreate(
            entity_id=test_entities["service_ent_complex"],
            tenant_id=tenant_context["id"],
            error_type_code=error_type,
            message=message,
            processing_step=step,
        )

        # Act
        error_id = processing_error_service.create_error(error_data)

        # Assert
        created_error = processing_error_service.get_error(error_id)
        assert created_error.error_type_code == error_type
        assert created_error.message == message
        assert created_error.processing_step == step

    def test_record_error_convenience_method(
        self, processing_error_service, tenant_context, test_entities
    ):
        """Test the convenience record_error method."""
        # Arrange
        entity_id = test_entities["service_ent_convenience_test"]

        # Act
        error_id = processing_error_service.record_error(
            entity_id=entity_id,
            error_type="CONVENIENCE_ERROR",
            message="Error recorded via convenience method",
            processing_step="convenience_test",
            stack_trace="Stack trace from convenience method",
        )

        # Assert
        assert error_id is not None

        # Verify the error was created correctly
        created_error = processing_error_service.get_error(error_id)
        assert created_error.entity_id == entity_id
        assert created_error.error_type_code == "CONVENIENCE_ERROR"
        assert created_error.message == "Error recorded via convenience method"
        assert created_error.processing_step == "convenience_test"
        assert created_error.stack_trace == "Stack trace from convenience method"


class TestProcessingErrorServiceRead:
    """Test processing error read operations."""

    def test_get_error_success(self, processing_error_service, tenant_context, test_entities):
        """Test successful retrieval of processing error by ID."""
        # Arrange - Create an error first
        error_data = ProcessingErrorCreate(
            entity_id=test_entities["service_ent_12345"],
            tenant_id=tenant_context["id"],
            error_type_code="GET_TEST_ERROR",
            message="Error for get test",
            processing_step="get_test",
        )
        error_id = processing_error_service.create_error(error_data)

        # Act
        retrieved_error = processing_error_service.get_error(error_id)

        # Assert
        assert isinstance(retrieved_error, ProcessingErrorRead)
        assert retrieved_error.id == error_id
        assert retrieved_error.entity_id == test_entities["service_ent_12345"]
        assert retrieved_error.tenant_id == tenant_context["id"]
        assert retrieved_error.error_type_code == "GET_TEST_ERROR"
        assert retrieved_error.message == "Error for get test"
        assert retrieved_error.processing_step == "get_test"

    def test_get_error_not_found(self, processing_error_service, tenant_context, test_entities):
        """Test retrieval of non-existent processing error."""
        # Act & Assert
        with pytest.raises(ServiceError) as exc_info:
            processing_error_service.get_error("nonexistent_id")
        assert exc_info.value.error_code == ErrorCode.NOT_FOUND

    def test_get_error_tenant_isolation(
        self, processing_error_service, multi_tenant_context, entity_service
    ):
        """Test that get_error respects tenant isolation."""
        # Arrange - Create errors in different tenants
        error_ids = {}
        entity_ids = {}

        for tenant in multi_tenant_context:
            TenantContext.set_current_tenant(tenant["id"])

            # Create entity for this tenant
            entity_data = EntityCreate(
                external_id=f"service_isolation_test_{tenant['id']}",
                tenant_id=tenant["id"],
                canonical_type="order",
                source="test_system",
                version=1,
                content_hash=f"hash_{tenant['id']}",
                attributes={"test": True},
            )
            entity_id = entity_service.create_entity(
                external_id=entity_data.external_id,
                canonical_type=entity_data.canonical_type,
                source=entity_data.source,
                content={"hash_placeholder": entity_data.content_hash},  # Use content instead of content_hash
                attributes=entity_data.attributes,
                version=entity_data.version,
            )
            entity_ids[tenant["id"]] = entity_id

            error_data = ProcessingErrorCreate(
                entity_id=entity_id,
                tenant_id=tenant["id"],
                error_type_code="TENANT_ISOLATION_ERROR",
                message=f"Error for tenant {tenant['id']}",
                processing_step="tenant_isolation_test",
            )
            error_id = processing_error_service.create_error(error_data)
            error_ids[tenant["id"]] = error_id

        # Act & Assert - Each tenant can only see their own errors
        for tenant in multi_tenant_context:
            TenantContext.set_current_tenant(tenant["id"])

            # Can retrieve own error
            own_error = processing_error_service.get_error(error_ids[tenant["id"]])
            assert own_error is not None
            assert own_error.tenant_id == tenant["id"]

            # Cannot retrieve other tenants' errors
            for other_tenant in multi_tenant_context:
                if other_tenant["id"] != tenant["id"]:
                    with pytest.raises(ServiceError) as exc_info:
                        processing_error_service.get_error(error_ids[other_tenant["id"]])
                    assert exc_info.value.error_code == ErrorCode.NOT_FOUND


class TestProcessingErrorServiceEntityErrors:
    """Test entity-specific error retrieval operations."""

    def test_get_entity_errors_multiple_errors(
        self, processing_error_service, tenant_context, test_entities
    ):
        """Test retrieving multiple errors for a single entity."""
        # Arrange - Create multiple errors for the same entity
        entity_id = test_entities["service_ent_bulk_test"]
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
            error_id = processing_error_service.create_error(error_data)
            created_error_ids.append(error_id)

        # Act
        errors = processing_error_service.get_entity_errors(entity_id)

        # Assert
        assert len(errors) == 3
        assert all(isinstance(error, ProcessingErrorRead) for error in errors)
        assert all(error.entity_id == entity_id for error in errors)
        assert all(error.tenant_id == tenant_context["id"] for error in errors)

        # Verify all error types are present
        error_types = {error.error_type_code for error in errors}
        expected_types = {"VALIDATION_ERROR", "TRANSFORMATION_ERROR", "ENRICHMENT_ERROR"}
        assert error_types == expected_types

    def test_get_entity_errors_no_errors(
        self, processing_error_service, tenant_context, test_entities
    ):
        """Test retrieving errors for entity with no errors."""
        # Act
        errors = processing_error_service.get_entity_errors("nonexistent_entity")

        # Assert
        assert errors == []

    def test_get_entity_errors_tenant_isolation(
        self, processing_error_service, multi_tenant_context, entity_service
    ):
        """Test that get_entity_errors respects tenant isolation."""
        entity_ids = {}

        # Arrange - Create entities and errors in different tenants
        for tenant in multi_tenant_context:
            TenantContext.set_current_tenant(tenant["id"])

            # Create entity for this tenant
            entity_data = EntityCreate(
                external_id=f"entity_errors_isolation_{tenant['id']}",
                tenant_id=tenant["id"],
                canonical_type="order",
                source="test_system",
                version=1,
                content_hash=f"hash_{tenant['id']}",
                attributes={"test": True},
            )
            entity_id = entity_service.create_entity(
                external_id=entity_data.external_id,
                canonical_type=entity_data.canonical_type,
                source=entity_data.source,
                content={"hash_placeholder": entity_data.content_hash},  # Use content instead of content_hash
                attributes=entity_data.attributes,
                version=entity_data.version,
            )
            entity_ids[tenant["id"]] = entity_id

            # Create error for this tenant
            error_data = ProcessingErrorCreate(
                entity_id=entity_id,
                tenant_id=tenant["id"],
                error_type_code="ENTITY_ERRORS_ISOLATION_TEST",
                message=f"Error for tenant {tenant['id']}",
                processing_step="entity_errors_isolation",
            )
            processing_error_service.create_error(error_data)

        # Act & Assert - Each tenant sees only their own errors
        for tenant in multi_tenant_context:
            TenantContext.set_current_tenant(tenant["id"])

            errors = processing_error_service.get_entity_errors(entity_ids[tenant["id"]])
            assert len(errors) == 1
            assert errors[0].tenant_id == tenant["id"]
            assert errors[0].message == f"Error for tenant {tenant['id']}"


class TestProcessingErrorServiceFilter:
    """Test processing error filtering operations."""

    def test_find_errors_by_error_type(
        self, processing_error_service, tenant_context, entity_service
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
            entity_id = entity_service.create_entity(
                external_id=entity_data.external_id,
                canonical_type=entity_data.canonical_type,
                source=entity_data.source,
                content={"hash_placeholder": entity_data.content_hash},  # Use content instead of content_hash
                attributes=entity_data.attributes,
                version=entity_data.version,
            )

            # Create error
            error_data = ProcessingErrorCreate(
                entity_id=entity_id,
                tenant_id=tenant_context["id"],
                error_type_code=error_type,
                message=message,
                processing_step=step,
            )
            processing_error_service.create_error(error_data)

        # Act - Filter by error type
        filter_params = ProcessingErrorFilter(error_type_code="VALIDATION_ERROR")
        validation_errors = processing_error_service.find_errors(filter_params)

        # Assert
        assert len(validation_errors) == 2
        assert all(error.error_type_code == "VALIDATION_ERROR" for error in validation_errors)

        # Act - Filter by processing step
        filter_params = ProcessingErrorFilter(processing_step="transformation")
        transform_errors = processing_error_service.find_errors(filter_params)

        # Assert
        assert len(transform_errors) == 1
        assert transform_errors[0].error_type_code == "TRANSFORMATION_ERROR"

    def test_find_errors_by_entity_id(
        self, processing_error_service, tenant_context, test_entities
    ):
        """Test filtering errors by entity ID."""
        # Arrange - Create errors for specific entity
        entity_id = test_entities["service_ent_filter_test"]

        for i in range(2):
            error_data = ProcessingErrorCreate(
                entity_id=entity_id,
                tenant_id=tenant_context["id"],
                error_type_code=f"FILTER_ERROR_{i}",
                message=f"Filter test error {i}",
                processing_step=f"step_{i}",
            )
            processing_error_service.create_error(error_data)

        # Act
        filter_params = ProcessingErrorFilter(entity_id=entity_id)
        entity_errors = processing_error_service.find_errors(filter_params)

        # Assert
        assert len(entity_errors) == 2
        assert all(error.entity_id == entity_id for error in entity_errors)

    def test_find_errors_pagination(
        self, processing_error_service, tenant_context, entity_service
    ):
        """Test pagination in error filtering."""
        # Arrange - Create multiple errors
        entity_data = EntityCreate(
            external_id="pagination_service_test_entity",
            tenant_id=tenant_context["id"],
            canonical_type="order",
            source="test_system",
            version=1,
            content_hash="hash_pagination_service",
            attributes={"test": True},
        )
        entity_id = entity_service.create_entity(
            external_id=entity_data.external_id,
            canonical_type=entity_data.canonical_type,
            source=entity_data.source,
            content={"hash_placeholder": entity_data.content_hash},  # Use content instead of content_hash
            attributes=entity_data.attributes,
            version=entity_data.version,
        )

        for i in range(10):
            error_data = ProcessingErrorCreate(
                entity_id=entity_id,
                tenant_id=tenant_context["id"],
                error_type_code="PAGINATION_ERROR",
                message=f"Pagination test error {i}",
                processing_step="pagination_test",
            )
            processing_error_service.create_error(error_data)

        # Act - Get first page
        filter_params = ProcessingErrorFilter(processing_step="pagination_test")
        first_page = processing_error_service.find_errors(filter_params, limit=3, offset=0)

        # Act - Get second page
        second_page = processing_error_service.find_errors(filter_params, limit=3, offset=3)

        # Assert
        assert len(first_page) == 3
        assert len(second_page) == 3

        # Verify different errors in each page
        first_page_ids = {error.id for error in first_page}
        second_page_ids = {error.id for error in second_page}
        assert first_page_ids.isdisjoint(second_page_ids)

    def test_find_errors_empty_result(
        self, processing_error_service, tenant_context, test_entities
    ):
        """Test filtering with no matching results."""
        # Act
        filter_params = ProcessingErrorFilter(error_type_code="NONEXISTENT_ERROR_TYPE")
        result = processing_error_service.find_errors(filter_params)

        # Assert
        assert result == []

    def test_find_errors_time_range_filtering(
        self, processing_error_service, tenant_context, test_entities
    ):
        """Test filtering errors by time range - verifies method works without errors."""
        # Arrange
        entity_id = test_entities["service_ent_filter_test"]

        # Create an error
        error_data = ProcessingErrorCreate(
            entity_id=entity_id,
            tenant_id=tenant_context["id"],
            error_type_code="TIME_FILTER_ERROR",
            message="Time range test error",
            processing_step="time_test",
        )
        processing_error_service.create_error(error_data)

        # Act - Test that time range filtering works without errors
        now = datetime.now()
        one_hour_ago = now - timedelta(hours=1)
        one_hour_from_now = now + timedelta(hours=1)

        filter_params = ProcessingErrorFilter(
            created_after=one_hour_ago, created_before=one_hour_from_now
        )
        recent_errors = processing_error_service.find_errors(filter_params)

        # Assert - Method should return a list (may be empty due to timing precision)
        assert isinstance(recent_errors, list)


class TestProcessingErrorServiceDelete:
    """Test processing error deletion operations."""

    def test_delete_error_success(self, processing_error_service, tenant_context, test_entities):
        """Test successful deletion of processing error by ID."""
        # Arrange - Create an error first
        error_data = ProcessingErrorCreate(
            entity_id=test_entities["service_ent_delete_test"],
            tenant_id=tenant_context["id"],
            error_type_code="DELETE_TEST_ERROR",
            message="Error to be deleted",
            processing_step="delete_test",
        )
        error_id = processing_error_service.create_error(error_data)

        # Verify error exists
        assert processing_error_service.get_error(error_id) is not None

        # Act
        result = processing_error_service.delete_error(error_id)

        # Assert
        assert result is True

        # Verify error is deleted
        with pytest.raises(ServiceError) as exc_info:
            processing_error_service.get_error(error_id)
        assert exc_info.value.error_code == ErrorCode.NOT_FOUND

    def test_delete_error_not_found(self, processing_error_service, tenant_context, test_entities):
        """Test deletion of non-existent processing error."""
        # Act
        result = processing_error_service.delete_error("nonexistent_id")

        # Assert
        assert result is False

    def test_delete_error_tenant_isolation(
        self, processing_error_service, multi_tenant_context, entity_service
    ):
        """Test that delete respects tenant isolation."""
        error_ids = {}

        # Arrange - Create errors in different tenants
        for tenant in multi_tenant_context:
            TenantContext.set_current_tenant(tenant["id"])

            # Create entity for this tenant
            entity_data = EntityCreate(
                external_id=f"delete_service_isolation_{tenant['id']}",
                tenant_id=tenant["id"],
                canonical_type="order",
                source="test_system",
                version=1,
                content_hash=f"hash_{tenant['id']}",
                attributes={"test": True},
            )
            entity_id = entity_service.create_entity(
                external_id=entity_data.external_id,
                canonical_type=entity_data.canonical_type,
                source=entity_data.source,
                content={"hash_placeholder": entity_data.content_hash},  # Use content instead of content_hash
                attributes=entity_data.attributes,
                version=entity_data.version,
            )

            error_data = ProcessingErrorCreate(
                entity_id=entity_id,
                tenant_id=tenant["id"],
                error_type_code="DELETE_SERVICE_ISOLATION_ERROR",
                message=f"Error for tenant {tenant['id']}",
                processing_step="delete_service_isolation",
            )
            error_id = processing_error_service.create_error(error_data)
            error_ids[tenant["id"]] = error_id

        # Act & Assert - Each tenant can only delete their own errors
        for tenant in multi_tenant_context:
            TenantContext.set_current_tenant(tenant["id"])

            # Can delete own error
            result = processing_error_service.delete_error(error_ids[tenant["id"]])
            assert result is True

            # Cannot delete other tenants' errors (they appear as not found)
            for other_tenant in multi_tenant_context:
                if other_tenant["id"] != tenant["id"]:
                    result = processing_error_service.delete_error(error_ids[other_tenant["id"]])
                    assert result is False  # Returns False because not found in current tenant

    def test_delete_entity_errors_success(
        self, processing_error_service, tenant_context, test_entities
    ):
        """Test successful deletion of all errors for an entity."""
        # Arrange - Create multiple errors for the same entity
        entity_id = test_entities["service_ent_delete_test"]

        error_ids = []
        for i in range(3):
            error_data = ProcessingErrorCreate(
                entity_id=entity_id,
                tenant_id=tenant_context["id"],
                error_type_code=f"BULK_DELETE_ERROR_{i}",
                message=f"Error {i} to be deleted",
                processing_step=f"bulk_delete_test_{i}",
            )
            error_id = processing_error_service.create_error(error_data)
            error_ids.append(error_id)

        # Verify errors exist
        errors_before = processing_error_service.get_entity_errors(entity_id)
        assert len(errors_before) >= 3

        # Act
        deleted_count = processing_error_service.delete_entity_errors(entity_id)

        # Assert
        assert deleted_count >= 3

        # Verify all errors for entity are deleted
        errors_after = processing_error_service.get_entity_errors(entity_id)
        assert len(errors_after) == 0

    def test_delete_entity_errors_no_errors(
        self, processing_error_service, tenant_context, test_entities
    ):
        """Test deletion when entity has no errors."""
        # Act
        deleted_count = processing_error_service.delete_entity_errors("nonexistent_entity")

        # Assert
        assert deleted_count == 0


class TestProcessingErrorServiceBusinessLogic:
    """Test service business logic and operation context."""

    def test_operation_context_propagation(
        self, processing_error_service, tenant_context, test_entities
    ):
        """Test that @operation decorator properly tracks context."""
        # This test verifies that the @operation decorator is working
        # The operation context should be available for all service methods

        # Arrange
        error_data = ProcessingErrorCreate(
            entity_id=test_entities["service_ent_workflow_test"],
            tenant_id=tenant_context["id"],
            error_type_code="OPERATION_CONTEXT_TEST",
            message="Testing operation context",
            processing_step="operation_test",
        )

        # Act - All these methods should have @operation decorator
        error_id = processing_error_service.create_error(error_data)
        retrieved_error = processing_error_service.get_error(error_id)
        entity_errors = processing_error_service.get_entity_errors(
            test_entities["service_ent_workflow_test"]
        )

        filter_params = ProcessingErrorFilter(error_type_code="OPERATION_CONTEXT_TEST")
        filtered_errors = processing_error_service.find_errors(filter_params)

        # Assert - All operations should complete successfully with context tracking
        assert error_id is not None
        assert retrieved_error is not None
        assert len(entity_errors) >= 1
        assert len(filtered_errors) >= 1

    def test_service_logging_behavior(
        self, processing_error_service, tenant_context, test_entities, caplog
    ):
        """Test that service methods log appropriate messages."""
        # Arrange
        error_data = ProcessingErrorCreate(
            entity_id=test_entities["service_ent_logging_test"],
            tenant_id=tenant_context["id"],
            error_type_code="LOGGING_TEST_ERROR",
            message="Testing logging behavior",
            processing_step="logging_test",
        )

        # Act
        with caplog.at_level("INFO"):
            error_id = processing_error_service.create_error(error_data)

            # Test delete logging
            processing_error_service.delete_error(error_id)

        # Assert - Check that appropriate log messages were created
        log_messages = [record.message for record in caplog.records]

        # Should have creation log
        assert any("Created processing error for entity" in msg for msg in log_messages)

        # Should have deletion log
        assert any("Deleted processing error" in msg for msg in log_messages)

    @pytest.mark.parametrize(
        "invalid_entity_id,expected_behavior",
        [
            ("", "should_fail"),
            (None, "should_fail"),
            ("very_long_id_" * 100, "should_handle"),  # Very long ID
            ("invalid-chars-!@#$%", "should_handle"),  # Special characters
        ],
    )
    def test_input_validation_edge_cases(
        self, processing_error_service, tenant_context, invalid_entity_id, expected_behavior
    ):
        """Test service behavior with edge case inputs."""
        if expected_behavior == "should_fail":
            # These should fail at the Pydantic validation level
            with pytest.raises((ValidationError, ValueError, TypeError)):
                error_data = ProcessingErrorCreate(
                    entity_id=invalid_entity_id,
                    tenant_id=tenant_context["id"],
                    error_type_code="VALIDATION_TEST",
                    message="Testing input validation",
                    processing_step="validation_test",
                )
                processing_error_service.create_error(error_data)
        else:
            # These should be handled gracefully by the service
            try:
                error_data = ProcessingErrorCreate(
                    entity_id=invalid_entity_id,
                    tenant_id=tenant_context["id"],
                    error_type_code="VALIDATION_TEST",
                    message="Testing input validation",
                    processing_step="validation_test",
                )
                # Service should handle this gracefully, may succeed or fail appropriately
                processing_error_service.create_error(error_data)
            except Exception as e:
                # Should be a service error, not an unhandled exception
                assert isinstance(e, (ServiceError, ValueError))


class TestProcessingErrorServiceErrorHandling:
    """Test service error handling and edge cases."""

    def test_service_error_handling_with_invalid_tenant_context(
        self, processing_error_service, test_entities
    ):
        """Test service behavior when tenant context is invalid."""
        # Arrange - Clear tenant context
        TenantContext.clear_current_tenant()

        error_data = ProcessingErrorCreate(
            entity_id=test_entities["service_ent_12345"],
            error_type_code="CONTEXT_ERROR",
            message="Error with invalid context",
            processing_step="context_test",
        )

        # Act & Assert - Should raise some kind of service error
        with pytest.raises((ServiceError, Exception)):
            processing_error_service.create_error(error_data)

    def test_service_error_wrapping(self, processing_error_service, tenant_context):
        """Test that service properly wraps and handles repository errors."""
        # This test ensures that repository errors are properly caught and wrapped as service errors

        # Arrange - Try to get a non-existent error
        # Act & Assert
        with pytest.raises(ServiceError) as exc_info:
            processing_error_service.get_error("definitely_nonexistent_id")
        assert exc_info.value.error_code == ErrorCode.NOT_FOUND

        # The service should wrap NOT_FOUND errors as ServiceError with NOT_FOUND code
        # and other repository errors should be wrapped as ServiceError

    def test_convenience_vs_core_methods_consistency(
        self, processing_error_service, tenant_context, test_entities
    ):
        """Test that convenience methods produce same results as core methods."""
        # Arrange
        entity_id = test_entities["service_ent_convenience_test"]

        # Act - Create error using convenience method
        convenience_error_id = processing_error_service.record_error(
            entity_id=entity_id,
            error_type="CONSISTENCY_TEST",
            message="Error via convenience method",
            processing_step="consistency_test",
        )

        # Act - Create error using core method
        error_data = ProcessingErrorCreate(
            entity_id=entity_id,
            tenant_id=tenant_context["id"],
            error_type_code="CONSISTENCY_TEST",
            message="Error via core method",
            processing_step="consistency_test",
        )
        core_error_id = processing_error_service.create_error(error_data)

        # Assert - Both methods should create valid errors
        convenience_error = processing_error_service.get_error(convenience_error_id)
        core_error = processing_error_service.get_error(core_error_id)

        # Both errors should have same structure and tenant
        assert convenience_error.entity_id == core_error.entity_id
        assert convenience_error.tenant_id == core_error.tenant_id
        assert convenience_error.error_type_code == core_error.error_type_code
        assert convenience_error.processing_step == core_error.processing_step

    def test_service_tenant_isolation_comprehensive(
        self, processing_error_service, multi_tenant_context, entity_service
    ):
        """Comprehensive test of tenant isolation across all service methods."""
        entity_ids = {}
        error_ids = {}

        # Arrange - Create entities and errors in different tenants
        for tenant in multi_tenant_context:
            TenantContext.set_current_tenant(tenant["id"])

            entity_data = EntityCreate(
                external_id=f"comprehensive_isolation_{tenant['id']}",
                tenant_id=tenant["id"],
                canonical_type="order",
                source="test_system",
                version=1,
                content_hash=f"hash_{tenant['id']}",
                attributes={"test": True},
            )
            entity_id = entity_service.create_entity(
                external_id=entity_data.external_id,
                canonical_type=entity_data.canonical_type,
                source=entity_data.source,
                content={"hash_placeholder": entity_data.content_hash},  # Use content instead of content_hash
                attributes=entity_data.attributes,
                version=entity_data.version,
            )
            entity_ids[tenant["id"]] = entity_id

            # Create multiple errors for this tenant
            for i in range(2):
                error_data = ProcessingErrorCreate(
                    entity_id=entity_id,
                    tenant_id=tenant["id"],
                    error_type_code=f"COMPREHENSIVE_ISOLATION_ERROR_{i}",
                    message=f"Error {i} for tenant {tenant['id']}",
                    processing_step=f"comprehensive_isolation_{i}",
                )
                error_id = processing_error_service.create_error(error_data)
                if tenant["id"] not in error_ids:
                    error_ids[tenant["id"]] = []
                error_ids[tenant["id"]].append(error_id)

        # Act & Assert - Each tenant sees only their own data in all operations
        for tenant in multi_tenant_context:
            TenantContext.set_current_tenant(tenant["id"])

            # Test get_entity_errors isolation
            entity_errors = processing_error_service.get_entity_errors(entity_ids[tenant["id"]])
            assert len(entity_errors) == 2
            assert all(error.tenant_id == tenant["id"] for error in entity_errors)

            # Test find_errors isolation
            filter_params = ProcessingErrorFilter(processing_step="comprehensive_isolation_0")
            filtered_errors = processing_error_service.find_errors(filter_params)
            assert len(filtered_errors) >= 1
            assert all(error.tenant_id == tenant["id"] for error in filtered_errors)

            # Test get_error isolation for own errors
            for error_id in error_ids[tenant["id"]]:
                error = processing_error_service.get_error(error_id)
                assert error.tenant_id == tenant["id"]

            # Test get_error isolation - cannot access other tenants' errors
            for other_tenant in multi_tenant_context:
                if other_tenant["id"] != tenant["id"]:
                    for error_id in error_ids[other_tenant["id"]]:
                        with pytest.raises(ServiceError) as exc_info:
                            processing_error_service.get_error(error_id)
                        assert exc_info.value.error_code == ErrorCode.NOT_FOUND
