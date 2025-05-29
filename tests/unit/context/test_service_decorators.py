"""Tests for service decorators using real implementations."""

import pytest
from pydantic import BaseModel

from src.context.service_decorators import handle_repository_errors
from src.exceptions import (
    ErrorCode,
    RepositoryError,
    ServiceError,
)
from src.repositories.base_repository import BaseRepository
from src.services.base_service import BaseService


# Dummy schema for testing
class DummySchema(BaseModel):
    id: str
    name: str


class ServiceForTesting(BaseService):
    """Test service that uses the decorator."""

    @handle_repository_errors()
    def method_that_succeeds(self, value):
        """Method that returns successfully."""
        return value * 2

    @handle_repository_errors()
    def method_that_raises_repo_error(self):
        """Method that raises a RepositoryError."""
        raise RepositoryError("Database connection failed", error_code=ErrorCode.DATABASE_ERROR)

    @handle_repository_errors()
    def method_that_raises_not_found(self, entity_id):
        """Method that raises a NOT_FOUND error."""
        raise RepositoryError(
            "Entity not found", error_code=ErrorCode.NOT_FOUND, entity_id=entity_id
        )

    @handle_repository_errors("custom_operation")
    def method_with_custom_name(self):
        """Method with custom operation name."""
        raise RepositoryError("Test error")

    @handle_repository_errors()
    def method_that_raises_unexpected(self):
        """Method that raises an unexpected error."""
        raise ValueError("Unexpected error")


class PlainService:
    """Service without base class or handler methods."""

    @handle_repository_errors()
    def method_that_raises(self):
        """Method that raises but has no handler."""
        raise RepositoryError("Test error")


class TestHandleRepositoryErrors:
    """Test handle_repository_errors decorator with real services."""

    def test_successful_execution(self, db_manager):
        """Test decorator with successful method execution."""
        service = ServiceForTesting(repository=None, read_schema_class=DummySchema)
        result = service.method_that_succeeds(5)
        assert result == 10

    def test_repository_error_handled(self, db_manager):
        """Test that RepositoryError is converted to ServiceError."""
        service = ServiceForTesting(repository=None, read_schema_class=DummySchema)

        # The base service handler converts RepositoryError to ServiceError
        with pytest.raises(ServiceError) as exc_info:
            service.method_that_raises_repo_error()

        assert exc_info.value.error_code == ErrorCode.INTERNAL_ERROR
        assert "Database connection failed" in str(exc_info.value)

    def test_not_found_error_handled(self, db_manager):
        """Test that NOT_FOUND errors are handled specially."""
        service = ServiceForTesting(repository=None, read_schema_class=DummySchema)

        # The base service handler converts NOT_FOUND to ServiceError with NOT_FOUND code
        with pytest.raises(ServiceError) as exc_info:
            service.method_that_raises_not_found("123")

        assert exc_info.value.error_code == ErrorCode.NOT_FOUND
        assert "Entity not found" in str(exc_info.value)

    def test_custom_operation_name(self, db_manager):
        """Test decorator with custom operation name."""
        service = ServiceForTesting(repository=None, read_schema_class=DummySchema)

        with pytest.raises(ServiceError) as exc_info:
            service.method_with_custom_name()

        # The operation name should be included in the error context
        assert exc_info.value.context.get("operation") == "custom_operation"

    def test_unexpected_error_handled(self, db_manager):
        """Test that unexpected errors are wrapped in ServiceError."""
        service = ServiceForTesting(repository=None, read_schema_class=DummySchema)

        with pytest.raises(ServiceError) as exc_info:
            service.method_that_raises_unexpected()

        assert exc_info.value.error_code == ErrorCode.INTERNAL_ERROR
        assert "Unexpected error" in str(exc_info.value)

    def test_no_handler_reraises(self):
        """Test that errors are re-raised when no handler exists."""
        service = PlainService()

        # Without a handler, the original exception is re-raised
        with pytest.raises(RepositoryError) as exc_info:
            service.method_that_raises()

        assert "Test error" in str(exc_info.value)

    def test_preserve_function_metadata(self):
        """Test that decorator preserves function metadata."""
        service = ServiceForTesting(repository=None, read_schema_class=DummySchema)

        # Check metadata is preserved
        assert service.method_that_succeeds.__name__ == "method_that_succeeds"
        assert "returns successfully" in service.method_that_succeeds.__doc__
