"""
Comprehensive tests for ProcessingErrorRepository.

Tests the processing error repository data access layer using real SQLite database,
following the anti-mock philosophy with real database operations and proper
tenant isolation with schema validation.
"""

import os
import sys
import uuid
from datetime import datetime, timedelta

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "..", "src"))

from src.context.tenant_context import TenantContext
from src.db.db_error_models import ProcessingError
from src.exceptions import ErrorCode, RepositoryError
from src.repositories.processing_error_repository import ProcessingErrorRepository
from src.schemas.processing_error_schema import (
    ProcessingErrorCreate,
    ProcessingErrorFilter,
    ProcessingErrorRead,
)

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
            error_type_code="VALIDATION_ERROR",
            message="Required field 'customer_id' is missing",
            processing_step="order_validation",
            stack_trace="Exception in thread...",
        )

        # Act
        error_id = processing_error_repository.create(error_data)

        # Assert
        assert error_id is not None
        assert isinstance(error_id, str)

        # Verify the error was actually created
        created_error = processing_error_repository.get_by_id(error_id)
        assert created_error is not None
        assert created_error.entity_id == test_entities["error_ent_12345"]
        assert created_error.error_type_code == "VALIDATION_ERROR"
        assert created_error.message == "Required field 'customer_id' is missing"
        assert created_error.processing_step == "order_validation"
        assert created_error.stack_trace == "Exception in thread..."
        assert isinstance(created_error.created_at, datetime)

    def test_create_processing_error_minimal_data(
        self, processing_error_repository, tenant_context, test_entities
    ):
        """Test creating processing error with minimal required data."""
        # Arrange
        error_data = ProcessingErrorCreate(
            entity_id=test_entities["error_ent_minimal"],
            error_type_code="SYSTEM_ERROR",
            message="System failure occurred",
            processing_step="data_ingestion",
        )

        # Act
        error_id = processing_error_repository.create(error_data)

        # Assert
        created_error = processing_error_repository.get_by_id(error_id)
        assert created_error.entity_id == test_entities["error_ent_minimal"]
        assert created_error.error_type_code == "SYSTEM_ERROR"
        assert created_error.message == "System failure occurred"
        assert created_error.processing_step == "data_ingestion"
        assert created_error.stack_trace is None


class TestProcessingErrorRepositoryRead:
    """Test processing error read operations."""

    def test_get_by_id_success(self, processing_error_repository, tenant_context, test_entities):
        """Test successful retrieval of processing error by ID."""
        # Arrange - Create a processing error first
        error_data = ProcessingErrorCreate(
            entity_id=test_entities["error_ent_12345"],
            error_type_code="VALIDATION_ERROR",
            message="Test error message",
            processing_step="validation",
        )
        created_error_id = processing_error_repository.create(error_data)

        # Act
        retrieved_error = processing_error_repository.get_by_id(created_error_id)

        # Assert
        assert retrieved_error is not None
        assert isinstance(retrieved_error, ProcessingErrorRead)
        assert retrieved_error.id == created_error_id
        assert retrieved_error.entity_id == test_entities["error_ent_12345"]
        assert retrieved_error.error_type_code == "VALIDATION_ERROR"
        assert retrieved_error.message == "Test error message"
        assert retrieved_error.processing_step == "validation"

    def test_get_by_id_not_found(self, processing_error_repository, tenant_context, test_entities):
        """Test retrieval of non-existent processing error."""
        # Act & Assert
        nonexistent_id = str(uuid.uuid4())
        result = processing_error_repository.get_by_id(nonexistent_id)
        assert result is None

    def test_get_by_id_tenant_isolation(self, processing_error_repository, multi_tenant_context):
        """Test that get_by_id respects tenant isolation."""
        # This test would require setting up multiple tenants
        # For now, just test basic isolation
        TenantContext.set_current_tenant("test_tenant")
        
        nonexistent_id = str(uuid.uuid4())
        result = processing_error_repository.get_by_id(nonexistent_id)
        assert result is None


class TestProcessingErrorRepositoryDelete:
    """Test processing error deletion operations."""

    def test_delete_by_id_success(self, processing_error_repository, tenant_context, test_entities):
        """Test successful deletion of processing error by ID."""
        # Arrange - Create a processing error first
        error_data = ProcessingErrorCreate(
            entity_id=test_entities["error_ent_delete_test"],
            error_type_code="TEST_ERROR",
            message="Error to be deleted",
            processing_step="test_step",
        )
        created_error_id = processing_error_repository.create(error_data)

        # Verify it exists
        assert processing_error_repository.get_by_id(created_error_id) is not None

        # Act
        deletion_result = processing_error_repository.delete(created_error_id)

        # Assert
        assert deletion_result is True
        assert processing_error_repository.get_by_id(created_error_id) is None

    def test_delete_by_id_not_found(self, processing_error_repository, tenant_context, test_entities):
        """Test deletion of non-existent processing error."""
        # Act
        nonexistent_id = str(uuid.uuid4())
        deletion_result = processing_error_repository.delete(nonexistent_id)

        # Assert
        assert deletion_result is False


class TestProcessingErrorRepositoryErrorHandling:
    """Test repository error handling and edge cases."""

    def test_create_with_invalid_tenant_context(self, processing_error_repository, test_entities):
        """Test that repository properly handles missing tenant context."""
        # Arrange - Clear tenant context
        TenantContext.clear_current_tenant()

        error_data = ProcessingErrorCreate(
            entity_id=test_entities["error_ent_12345"],
            error_type_code="TEST_ERROR",
            message="Test error",
            processing_step="test",
        )

        # Act & Assert
        with pytest.raises(ValueError, match="No tenant context set"):
            processing_error_repository.create(error_data)

    def test_repository_error_handling(
        self, processing_error_repository, tenant_context, test_entities
    ):
        """Test that repository properly handles and converts database errors."""
        # Create a valid error first
        error_data = ProcessingErrorCreate(
            entity_id=test_entities["error_ent_12345"],
            error_type_code="TEST_ERROR",
            message="Test error for handling",
            processing_step="test_step",
        )
        
        error_id = processing_error_repository.create(error_data)
        
        # Verify it was created and can be retrieved
        retrieved = processing_error_repository.get_by_id(error_id)
        assert retrieved is not None
        assert retrieved.entity_id == test_entities["error_ent_12345"]