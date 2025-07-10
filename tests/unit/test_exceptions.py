"""
Unit tests for the exception system.

Tests all exception classes, factory functions, and error handling patterns.
"""

import pytest
from unittest.mock import patch

from api_exchange_core.exceptions import (
    BaseError,
    DuplicateError,
    ErrorCode,
    NotFoundError,
    ValidationError,
    duplicate,
    not_found,
    permission_denied,
    validation_failed,
    get_correlation_id,
    set_correlation_id,
    clear_correlation_id,
)


class TestBaseError:
    """Test BaseError class."""
    
    def test_basic_error_creation(self):
        """Test creating a basic error."""
        error = BaseError("Test error message")
        
        assert error.message == "Test error message"
        assert error.error_code == ErrorCode.INTERNAL_ERROR
        assert error.status_code == 500
        assert error.cause is None
        assert error.error_id is not None
        assert error.timestamp is not None
        assert isinstance(error.context, dict)
        assert error.context["error_id"] == error.error_id
    
    def test_error_with_custom_code_and_status(self):
        """Test error with custom error code and status."""
        error = BaseError(
            "Custom error",
            error_code=ErrorCode.NOT_FOUND,
            status_code=404
        )
        
        assert error.error_code == ErrorCode.NOT_FOUND
        assert error.status_code == 404
    
    def test_error_with_cause(self):
        """Test error with underlying cause."""
        original_error = ValueError("Original error")
        error = BaseError("Wrapped error", cause=original_error)
        
        assert error.cause == original_error
        assert "cause" in error.context
        assert error.context["cause"]["type"] == "ValueError"
        assert error.context["cause"]["message"] == "Original error"
    
    def test_error_with_context(self):
        """Test error with additional context."""
        error = BaseError(
            "Context error",
            tenant_id="test-tenant",
            operation="test_operation",
            resource_id="123"
        )
        
        assert error.context["tenant_id"] == "test-tenant"
        assert error.context["operation"] == "test_operation"
        assert error.context["resource_id"] == "123"
    
    def test_error_with_correlation_id(self):
        """Test error includes correlation ID when available."""
        set_correlation_id("test-correlation-123")
        
        try:
            error = BaseError("Test error")
            assert error.context["correlation_id"] == "test-correlation-123"
        finally:
            clear_correlation_id()
    
    def test_add_context_fluent_interface(self):
        """Test adding context using fluent interface."""
        error = BaseError("Test error").add_context(
            user_id="user123",
            action="delete_resource"
        )
        
        assert error.context["user_id"] == "user123"
        assert error.context["action"] == "delete_resource"
    
    def test_to_dict(self):
        """Test converting error to dictionary."""
        error = BaseError(
            "Test error",
            error_code=ErrorCode.VALIDATION_FAILED,
            status_code=400,
            field="email"
        )
        
        result = error.to_dict()
        
        assert result["error"]["code"] == "2000"
        assert result["error"]["message"] == "Test error"
        assert result["error"]["id"] == error.error_id
        assert result["error"]["timestamp"] == error.timestamp
        assert result["error"]["context"]["field"] == "email"
    
    def test_to_dict_with_cause(self):
        """Test converting error with cause to dictionary."""
        original_error = ValueError("Original error")
        error = BaseError("Wrapped error", cause=original_error)
        
        result = error.to_dict(include_cause=True)
        
        assert "cause" in result["error"]
        assert result["error"]["cause"]["type"] == "ValueError"
        assert result["error"]["cause"]["message"] == "Original error"
    
    def test_error_chain(self):
        """Test error chain property."""
        error1 = ValueError("First error")
        error2 = BaseError("Second error", cause=error1)
        error3 = BaseError("Third error", cause=error2)
        
        chain = error3.error_chain
        
        assert len(chain) == 3
        assert chain[0] == error3
        assert chain[1] == error2
        assert chain[2] == error1


class TestNotFoundError:
    """Test NotFoundError class."""
    
    def test_not_found_error_creation(self):
        """Test creating a NotFoundError."""
        error = NotFoundError("Resource not found", resource_type="Tenant")
        
        assert error.message == "Resource not found"
        assert error.error_code == ErrorCode.NOT_FOUND
        assert error.status_code == 404
        assert error.context["resource_type"] == "Tenant"
    
    def test_not_found_error_without_resource_type(self):
        """Test NotFoundError without resource type."""
        error = NotFoundError("Something not found")
        
        assert error.message == "Something not found"
        assert error.error_code == ErrorCode.NOT_FOUND
        assert error.status_code == 404
        assert "resource_type" not in error.context


class TestDuplicateError:
    """Test DuplicateError class."""
    
    def test_duplicate_error_creation(self):
        """Test creating a DuplicateError."""
        error = DuplicateError("Duplicate resource", resource_type="Pipeline")
        
        assert error.message == "Duplicate resource"
        assert error.error_code == ErrorCode.DUPLICATE
        assert error.status_code == 409
        assert error.context["resource_type"] == "Pipeline"
    
    def test_duplicate_error_without_resource_type(self):
        """Test DuplicateError without resource type."""
        error = DuplicateError("Duplicate found")
        
        assert error.message == "Duplicate found"
        assert error.error_code == ErrorCode.DUPLICATE
        assert error.status_code == 409
        assert "resource_type" not in error.context


class TestValidationError:
    """Test ValidationError class."""
    
    def test_validation_error_with_field(self):
        """Test ValidationError with field context."""
        error = ValidationError("Invalid email format", field="email")
        
        assert error.message == "Invalid email format"
        assert error.error_code == ErrorCode.VALIDATION_FAILED
        assert error.status_code == 400
        assert error.context["field"] == "email"
    
    def test_validation_error_without_field(self):
        """Test ValidationError without field context."""
        error = ValidationError("General validation error")
        
        assert error.message == "General validation error"
        assert error.error_code == ErrorCode.VALIDATION_FAILED
        assert error.status_code == 400
        assert "field" not in error.context


class TestFactoryFunctions:
    """Test factory functions for common error patterns."""
    
    def test_not_found_factory(self):
        """Test not_found factory function."""
        error = not_found("Tenant", tenant_id="test-123", name="Test Tenant")
        
        assert isinstance(error, NotFoundError)
        assert error.message == "Tenant not found: tenant_id=test-123, name=Test Tenant"
        assert error.error_code == ErrorCode.NOT_FOUND
        assert error.status_code == 404
        assert error.context["resource_type"] == "Tenant"
        assert error.context["tenant_id"] == "test-123"
        assert error.context["name"] == "Test Tenant"
    
    def test_not_found_factory_without_identifiers(self):
        """Test not_found factory without identifiers."""
        error = not_found("Pipeline")
        
        assert isinstance(error, NotFoundError)
        assert error.message == "Pipeline not found"
        assert error.context["resource_type"] == "Pipeline"
    
    def test_duplicate_factory(self):
        """Test duplicate factory function."""
        error = duplicate("Tenant", tenant_id="test-123", name="Test Tenant")
        
        assert isinstance(error, DuplicateError)
        assert error.message == "Duplicate Tenant: tenant_id=test-123, name=Test Tenant"
        assert error.error_code == ErrorCode.DUPLICATE
        assert error.status_code == 409
        assert error.context["resource_type"] == "Tenant"
        assert error.context["tenant_id"] == "test-123"
        assert error.context["name"] == "Test Tenant"
    
    def test_duplicate_factory_without_identifiers(self):
        """Test duplicate factory without identifiers."""
        error = duplicate("Pipeline")
        
        assert isinstance(error, DuplicateError)
        assert error.message == "Duplicate Pipeline"
        assert error.context["resource_type"] == "Pipeline"
    
    def test_validation_failed_factory(self):
        """Test validation_failed factory function."""
        error = validation_failed("email", "invalid-email", "Must be valid email format")
        
        assert isinstance(error, ValidationError)
        assert error.message == "Validation failed for email: Must be valid email format"
        assert error.error_code == ErrorCode.VALIDATION_FAILED
        assert error.status_code == 400
        assert error.context["field"] == "email"
        assert error.context["value"] == "invalid-email"
        assert error.context["reason"] == "Must be valid email format"
    
    def test_permission_denied_factory(self):
        """Test permission_denied factory function."""
        error = permission_denied("delete", "pipeline", user_id="user123")
        
        assert isinstance(error, BaseError)
        assert error.message == "Permission denied: delete on pipeline"
        assert error.error_code == ErrorCode.PERMISSION_DENIED
        assert error.status_code == 403
        assert error.context["action"] == "delete"
        assert error.context["resource"] == "pipeline"
        assert error.context["user_id"] == "user123"


class TestCorrelationId:
    """Test correlation ID management."""
    
    def test_set_and_get_correlation_id(self):
        """Test setting and getting correlation ID."""
        correlation_id = "test-correlation-456"
        set_correlation_id(correlation_id)
        
        assert get_correlation_id() == correlation_id
        
        clear_correlation_id()
        assert get_correlation_id() is None
    
    def test_correlation_id_in_different_threads(self):
        """Test that correlation ID is thread-local."""
        import threading
        
        results = {}
        
        def thread_function(thread_id):
            set_correlation_id(f"correlation-{thread_id}")
            results[thread_id] = get_correlation_id()
        
        # Create two threads with different correlation IDs
        thread1 = threading.Thread(target=thread_function, args=(1,))
        thread2 = threading.Thread(target=thread_function, args=(2,))
        
        thread1.start()
        thread2.start()
        thread1.join()
        thread2.join()
        
        # Each thread should have its own correlation ID
        assert results[1] == "correlation-1"
        assert results[2] == "correlation-2"
    
    def test_clear_correlation_id(self):
        """Test clearing correlation ID."""
        set_correlation_id("test-correlation")
        assert get_correlation_id() is not None
        
        clear_correlation_id()
        assert get_correlation_id() is None


class TestErrorLogging:
    """Test error logging behavior."""
    
    @patch('api_exchange_core.utils.logger.get_logger')
    def test_error_logging_by_status_code(self, mock_get_logger):
        """Test that errors are logged with appropriate levels based on status code."""
        mock_logger = mock_get_logger.return_value
        
        # Test 500 error (should log as ERROR)
        error_500 = BaseError("Server error", status_code=500)
        mock_logger.error.assert_called_once()
        mock_logger.reset_mock()
        
        # Test 400 error (should log as WARNING)
        error_400 = BaseError("Client error", status_code=400)
        mock_logger.warning.assert_called_once()
        mock_logger.reset_mock()
        
        # Test 200 status (should log as INFO)
        error_200 = BaseError("Success message", status_code=200)
        mock_logger.info.assert_called_once()
    
    @patch('api_exchange_core.utils.logger.get_logger')
    def test_error_logging_includes_context(self, mock_get_logger):
        """Test that error logging includes relevant context."""
        mock_logger = mock_get_logger.return_value
        
        error = BaseError(
            "Test error",
            error_code=ErrorCode.VALIDATION_FAILED,
            tenant_id="test-tenant",
            operation="test_operation"
        )
        
        # Check that the logger was called with extra context
        call_args = mock_logger.error.call_args
        assert call_args is not None
        
        extra_data = call_args[1]['extra']
        assert extra_data['error_code'] == ErrorCode.VALIDATION_FAILED.value
        assert extra_data['status_code'] == 500
        assert extra_data['context']['tenant_id'] == "test-tenant"
        assert extra_data['context']['operation'] == "test_operation"