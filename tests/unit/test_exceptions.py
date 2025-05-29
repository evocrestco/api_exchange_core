"""
Test the consolidated exception system.
"""

from unittest.mock import MagicMock, patch


from src.exceptions import (
    BaseError,
    ErrorCode,
    ErrorTelemetry,
    ExternalServiceError,
    RepositoryError,
    ServiceError,
    ValidationError,
    clear_correlation_id,
    duplicate,
    get_correlation_id,
    not_found,
    permission_denied,
    set_correlation_id,
    validation_failed,
)


class TestBaseError:
    """Test BaseError functionality."""

    def test_base_error_creation(self):
        """Test creating a base error with all attributes."""
        error = BaseError(
            "Test error",
            error_code=ErrorCode.INTERNAL_ERROR,
            status_code=500,
            custom_field="custom_value",
        )

        assert error.message == "Test error"
        assert error.error_code == ErrorCode.INTERNAL_ERROR
        assert error.status_code == 500
        assert error.context["custom_field"] == "custom_value"
        assert error.error_id is not None
        assert error.timestamp is not None

    def test_error_with_cause(self):
        """Test error chaining with cause."""
        original = ValueError("Original error")
        error = BaseError("Wrapped error", cause=original)

        assert error.cause == original
        assert error.context["cause"]["type"] == "ValueError"
        assert error.context["cause"]["message"] == "Original error"
        assert len(error.context["cause"]["traceback"]) > 0

    def test_error_chain(self):
        """Test getting full error chain."""
        error1 = ValueError("Root cause")
        error2 = BaseError("Middle error", cause=error1)
        error3 = BaseError("Top error", cause=error2)

        chain = error3.error_chain
        assert len(chain) == 3
        assert chain[0] == error3
        assert chain[1] == error2
        assert chain[2] == error1

    def test_add_context(self):
        """Test adding context fluently."""
        error = BaseError("Test error")
        error.add_context(user_id="123", action="create")

        assert error.context["user_id"] == "123"
        assert error.context["action"] == "create"

    def test_to_dict(self):
        """Test converting to dictionary."""
        error = BaseError(
            "Test error", error_code=ErrorCode.NOT_FOUND, status_code=404, resource_id="123"
        )

        result = error.to_dict()

        assert result["error"]["id"] == error.error_id
        assert result["error"]["code"] == "3000"
        assert result["error"]["message"] == "Test error"
        assert result["error"]["timestamp"] == error.timestamp
        assert result["error"]["context"]["resource_id"] == "123"

    def test_to_dict_with_cause(self):
        """Test converting to dictionary with cause."""
        original = ValueError("Original")
        error = BaseError("Wrapped", cause=original)

        # Without cause
        result = error.to_dict(include_cause=False)
        assert "cause" not in result["error"]

        # With cause but no traceback
        result = error.to_dict(include_cause=True, include_traceback=False)
        assert result["error"]["cause"]["type"] == "ValueError"
        assert result["error"]["cause"]["message"] == "Original"
        assert "traceback" not in result["error"]["cause"]

        # With full traceback
        result = error.to_dict(include_cause=True, include_traceback=True)
        assert "traceback" in result["error"]["cause"]


class TestCorrelationId:
    """Test correlation ID management."""

    def setup_method(self):
        """Clear correlation ID before each test."""
        clear_correlation_id()

    def test_correlation_id_flow(self):
        """Test setting and getting correlation ID."""
        # Initially None
        assert get_correlation_id() is None

        # Set correlation ID
        set_correlation_id("test-correlation-123")
        assert get_correlation_id() == "test-correlation-123"

        # Error should capture it
        error = BaseError("Test error")
        assert error.context["correlation_id"] == "test-correlation-123"

        # Clear it
        clear_correlation_id()
        assert get_correlation_id() is None

    def test_error_without_correlation_id(self):
        """Test error creation without correlation ID."""
        clear_correlation_id()
        error = BaseError("Test error")
        assert "correlation_id" not in error.context


class TestLayerSpecificErrors:
    """Test layer-specific error classes."""

    def test_repository_error(self):
        """Test RepositoryError defaults."""
        error = RepositoryError("Database connection failed")

        assert error.error_code == ErrorCode.DATABASE_ERROR
        assert error.status_code == 500

    def test_service_error_with_operation(self):
        """Test ServiceError with operation context."""
        error = ServiceError("Processing failed", operation="process_order")

        assert error.error_code == ErrorCode.INTERNAL_ERROR
        assert error.status_code == 500
        assert error.context["operation"] == "process_order"

    def test_validation_error_with_field(self):
        """Test ValidationError with field."""
        error = ValidationError("Invalid email format", field="email")

        assert error.error_code == ErrorCode.VALIDATION_FAILED
        assert error.status_code == 400
        assert error.context["field"] == "email"

    def test_external_service_error(self):
        """Test ExternalServiceError."""
        error = ExternalServiceError("API timeout", service_name="payment_gateway")

        assert error.error_code == ErrorCode.EXTERNAL_API_ERROR
        assert error.status_code == 502
        assert error.context["service_name"] == "payment_gateway"


class TestFactoryFunctions:
    """Test error factory functions."""

    def test_not_found_factory(self):
        """Test not_found factory."""
        error = not_found("Entity", entity_id="123", tenant_id="abc")

        assert error.error_code == ErrorCode.NOT_FOUND
        assert error.status_code == 404
        assert error.message == "Entity not found: entity_id=123, tenant_id=abc"
        assert error.context["resource_type"] == "Entity"
        assert error.context["entity_id"] == "123"
        assert error.context["tenant_id"] == "abc"

    def test_duplicate_factory(self):
        """Test duplicate factory."""
        error = duplicate("Order", order_id="123", source="api")

        assert error.error_code == ErrorCode.DUPLICATE
        assert error.status_code == 409
        assert error.message == "Duplicate Order: order_id=123, source=api"

    def test_validation_failed_factory(self):
        """Test validation_failed factory."""
        error = validation_failed(field="age", value=-5, reason="Must be positive")

        assert error.error_code == ErrorCode.VALIDATION_FAILED
        assert error.status_code == 400
        assert error.message == "Validation failed for age: Must be positive"
        assert error.context["field"] == "age"
        assert error.context["value"] == "-5"
        assert error.context["reason"] == "Must be positive"

    def test_permission_denied_factory(self):
        """Test permission_denied factory."""
        error = permission_denied(action="delete", resource="order", user_id="123")

        assert error.error_code == ErrorCode.PERMISSION_DENIED
        assert error.status_code == 403
        assert error.message == "Permission denied: delete on order"
        assert error.context["action"] == "delete"
        assert error.context["resource"] == "order"
        assert error.context["user_id"] == "123"


class TestErrorLogging:
    """Test error logging behavior."""

    @patch("logging.getLogger")
    def test_error_logging_levels(self, mock_get_logger):
        """Test that errors log at appropriate levels."""
        mock_logger = MagicMock()
        mock_get_logger.return_value = mock_logger

        # 5xx error - should log as error
        error = BaseError("Server error", status_code=500)
        mock_logger.error.assert_called_once()

        # 4xx error - should log as warning
        error = BaseError("Client error", status_code=400)
        mock_logger.warning.assert_called_once()

        # Other - should log as info
        error = BaseError("Redirect", status_code=302)
        mock_logger.info.assert_called_once()

    @patch("logging.getLogger")
    def test_error_logging_with_correlation(self, mock_get_logger):
        """Test that correlation ID is included in logs."""
        mock_logger = MagicMock()
        mock_get_logger.return_value = mock_logger

        set_correlation_id("test-123")
        error = BaseError("Test error")

        # Check that correlation ID was logged
        call_args = mock_logger.error.call_args
        extra_data = call_args[1]["extra"]
        assert extra_data["correlation_id"] == "test-123"

        clear_correlation_id()


class TestErrorTelemetry:
    """Test telemetry integration."""

    @patch("logging.getLogger")
    def test_error_telemetry(self, mock_get_logger):
        """Test error telemetry tracking."""
        mock_logger = MagicMock()
        mock_get_logger.return_value = mock_logger

        error = BaseError("Test error", error_code=ErrorCode.NOT_FOUND)
        ErrorTelemetry.track_error(error, {"endpoint": "/api/test"})

        # Should log telemetry
        mock_logger.debug.assert_called()
        call_args = mock_logger.debug.call_args
        assert "Telemetry: ErrorCode.NOT_FOUND" in call_args[0][0]
