"""
Consolidated exception system with error codes, context, and correlation support.

This module provides a unified exception hierarchy for the entire application,
with automatic logging, telemetry support, and correlation ID tracking.
"""

import traceback
import uuid
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Dict, List, Optional

# Thread-local storage for correlation ID
import threading

# Removed logger import to avoid circular dependency - calling code should handle logging

_thread_local = threading.local()


class ErrorCode(str, Enum):
    """Standardized error codes for API responses."""

    # System errors (1xxx)
    INTERNAL_ERROR = "1000"
    DATABASE_ERROR = "1001"
    CONNECTION_ERROR = "1002"
    CONFIGURATION_ERROR = "1003"
    TIMEOUT_ERROR = "1004"

    # Validation errors (2xxx)
    VALIDATION_FAILED = "2000"
    INVALID_FORMAT = "2001"
    MISSING_REQUIRED = "2002"
    TYPE_MISMATCH = "2003"
    CONSTRAINT_VIOLATION = "2004"

    # Resource errors (3xxx)
    NOT_FOUND = "3000"
    DUPLICATE = "3001"
    CONFLICT = "3002"
    LOCKED = "3003"
    EXPIRED = "3004"
    LIMIT_EXCEEDED = "3005"

    # Business logic errors (4xxx)
    BUSINESS_RULE_VIOLATION = "4000"
    INVALID_STATE_TRANSITION = "4001"
    QUOTA_EXCEEDED = "4002"
    PERMISSION_DENIED = "4003"
    PRECONDITION_FAILED = "4004"

    # External service errors (5xxx)
    ADAPTER_ERROR = "5000"
    QUEUE_ERROR = "5001"
    EXTERNAL_API_ERROR = "5002"
    INTEGRATION_ERROR = "5003"
    DOWNSTREAM_ERROR = "5004"


class BaseError(Exception):
    """Base exception with context, error codes, logging, and error chaining."""

    def __init__(
        self,
        message: str,
        error_code: ErrorCode = ErrorCode.INTERNAL_ERROR,
        status_code: int = 500,
        cause: Optional[Exception] = None,
        **context: Any,
    ):
        """
        Initialize base error with rich context.

        Args:
            message: Human-readable error message
            error_code: Standardized error code from ErrorCode enum
            status_code: HTTP status code for API responses
            cause: Original exception that caused this error
            **context: Additional context information
        """
        self.message = message
        self.error_code = error_code
        self.status_code = status_code
        self.cause = cause
        self.timestamp = datetime.now(timezone.utc).isoformat()
        self.error_id = str(uuid.uuid4())
        self.context = context

        # Add correlation ID if available
        correlation_id = get_correlation_id()
        if correlation_id:
            self.context["correlation_id"] = correlation_id

        # Add error ID to context
        self.context["error_id"] = self.error_id

        # Add cause to context if present
        if cause:
            self.context["cause"] = {
                "type": type(cause).__name__,
                "message": str(cause),
                "traceback": traceback.format_exception(type(cause), cause, cause.__traceback__),
            }

        # Log the error (using lazy import to avoid circular dependencies)
        self._log_error()

        # Call parent constructor
        super().__init__(message)

    def _log_error(self) -> None:
        """Log error with appropriate level based on status code."""
        # Import logger here to avoid circular dependency at module load time
        from .utils.logger import get_logger
        logger = get_logger()

        log_data = {
            "error_id": self.error_id,
            "error_code": self.error_code.value,
            "status_code": self.status_code,
            "error_message": self.message,  # Changed from 'message' to avoid conflict
            "timestamp": self.timestamp,
            "context": {k: v for k, v in self.context.items() if k not in ["cause", "traceback"]},
        }

        # Add correlation ID to log data if present
        if "correlation_id" in self.context:
            log_data["correlation_id"] = self.context["correlation_id"]

        if self.status_code >= 500:
            logger.error(
                f"Error {self.error_code}: {self.message}", extra=log_data, exc_info=self.cause
            )
        elif self.status_code >= 400:
            logger.warning(f"Client error {self.error_code}: {self.message}", extra=log_data)
        else:
            logger.info(f"Error {self.error_code}: {self.message}", extra=log_data)

    def to_dict(
        self, include_cause: bool = False, include_traceback: bool = False
    ) -> Dict[str, Any]:
        """
        Convert to dict for API responses.

        Args:
            include_cause: Include cause information (useful for debugging)
            include_traceback: Include full traceback (only in debug mode)

        Returns:
            Dictionary representation suitable for JSON serialization
        """
        result: Dict[str, Any] = {
            "error": {
                "id": self.error_id,
                "code": self.error_code.value,
                "message": self.message,
                "timestamp": self.timestamp,
                "context": {
                    k: v
                    for k, v in self.context.items()
                    if k not in ["cause", "error_id", "correlation_id"]
                },
            }
        }

        # Include correlation ID if present
        if "correlation_id" in self.context:
            result["error"]["correlation_id"] = self.context["correlation_id"]

        if include_cause and "cause" in self.context:
            result["error"]["cause"] = {
                "type": self.context["cause"]["type"],
                "message": self.context["cause"]["message"],
            }
            if include_traceback:
                result["error"]["cause"]["traceback"] = self.context["cause"]["traceback"]

        return result

    def add_context(self, **kwargs: Any) -> "BaseError":
        """
        Add additional context to the error (fluent interface).

        Returns:
            Self for method chaining
        """
        self.context.update(kwargs)
        return self

    @property
    def error_chain(self) -> List[Exception]:
        """Get the full chain of errors."""
        chain: List[Exception] = [self]
        current = self.cause
        while current:
            chain.append(current)
            current = getattr(current, "cause", None)
        return chain


# Layer-specific base exceptions
class RepositoryError(BaseError):
    """Repository layer errors."""

    def __init__(
        self,
        message: str,
        error_code: ErrorCode = ErrorCode.DATABASE_ERROR,
        status_code: int = 500,
        cause: Optional[Exception] = None,
        **context,
    ):
        """Initialize repository error with database context."""
        super().__init__(message, error_code, status_code, cause, **context)


class ServiceError(BaseError):
    """Service layer errors."""

    def __init__(
        self,
        message: str,
        error_code: ErrorCode = ErrorCode.INTERNAL_ERROR,
        operation: Optional[str] = None,
        cause: Optional[Exception] = None,
        **context,
    ):
        """Initialize service error with operation context."""
        if operation:
            context["operation"] = operation
        super().__init__(message, error_code, 500, cause, **context)


class ValidationError(BaseError):
    """Validation errors."""

    def __init__(
        self,
        message: str,
        field: Optional[str] = None,
        error_code: ErrorCode = ErrorCode.VALIDATION_FAILED,
        cause: Optional[Exception] = None,
        **context,
    ):
        """Initialize validation error with field context."""
        if field:
            context["field"] = field
        super().__init__(message, error_code, 400, cause, **context)


class ExternalServiceError(BaseError):
    """External service integration errors."""

    def __init__(
        self,
        message: str,
        service_name: str,
        error_code: ErrorCode = ErrorCode.EXTERNAL_API_ERROR,
        cause: Optional[Exception] = None,
        **context,
    ):
        """Initialize external service error with service context."""
        context["service_name"] = service_name
        super().__init__(message, error_code, 502, cause, **context)


# Factory functions for common error patterns
def not_found(
    resource_type: str, cause: Optional[Exception] = None, **identifiers
) -> RepositoryError:
    """
    Factory for not found errors.

    Args:
        resource_type: Type of resource (e.g., 'Entity', 'Tenant')
        cause: Original exception if any
        **identifiers: Resource identifiers (e.g., entity_id='123')

    Returns:
        Configured RepositoryError instance with 404 status
    """
    id_parts = [f"{k}={v}" for k, v in identifiers.items()]
    message = f"{resource_type} not found"
    if id_parts:
        message += f": {', '.join(id_parts)}"

    return RepositoryError(
        message,
        error_code=ErrorCode.NOT_FOUND,
        status_code=404,
        cause=cause,
        resource_type=resource_type,
        **identifiers,
    )


def duplicate(
    resource_type: str, cause: Optional[Exception] = None, **identifiers
) -> RepositoryError:
    """
    Factory for duplicate resource errors.

    Args:
        resource_type: Type of resource (e.g., 'Entity', 'Tenant')
        cause: Original exception if any
        **identifiers: Resource identifiers

    Returns:
        Configured RepositoryError instance with 409 status
    """
    id_parts = [f"{k}={v}" for k, v in identifiers.items()]
    message = f"Duplicate {resource_type}"
    if id_parts:
        message += f": {', '.join(id_parts)}"

    return RepositoryError(
        message,
        error_code=ErrorCode.DUPLICATE,
        status_code=409,
        cause=cause,
        resource_type=resource_type,
        **identifiers,
    )


def validation_failed(
    field: str, value: Any, reason: str, cause: Optional[Exception] = None
) -> ValidationError:
    """
    Factory for validation errors.

    Args:
        field: Field that failed validation
        value: The invalid value
        reason: Why validation failed
        cause: Original exception if any

    Returns:
        Configured ValidationError instance
    """
    return ValidationError(
        f"Validation failed for {field}: {reason}",
        field=field,
        error_code=ErrorCode.VALIDATION_FAILED,
        cause=cause,
        value=str(value),
        reason=reason,
    )


def permission_denied(
    action: str, resource: str, cause: Optional[Exception] = None, **context
) -> BaseError:
    """
    Factory for permission denied errors.

    Args:
        action: Action that was denied (e.g., 'read', 'update')
        resource: Resource being accessed
        cause: Original exception if any
        **context: Additional context

    Returns:
        Configured BaseError instance
    """
    return BaseError(
        f"Permission denied: {action} on {resource}",
        error_code=ErrorCode.PERMISSION_DENIED,
        status_code=403,
        cause=cause,
        action=action,
        resource=resource,
        **context,
    )


# Correlation ID management
def set_correlation_id(correlation_id: str) -> None:
    """Set the correlation ID for the current thread."""
    _thread_local.correlation_id = correlation_id


def get_correlation_id() -> Optional[str]:
    """Get the current thread's correlation ID."""
    return getattr(_thread_local, 'correlation_id', None)


def clear_correlation_id() -> None:
    """Clear the current thread's correlation ID."""
    if hasattr(_thread_local, 'correlation_id'):
        delattr(_thread_local, 'correlation_id')


# Telemetry integration (optional)
class ErrorTelemetry:
    """Optional telemetry integration for errors."""

    @staticmethod
    def track_error(error: BaseError, additional_properties: Optional[Dict[str, Any]] = None):
        """
        Send error telemetry to monitoring system.

        This is a placeholder for integration with your telemetry provider
        (e.g., Azure Application Insights, Datadog, New Relic).
        """
        properties = {
            "error_id": error.error_id,
            "error_code": error.error_code.value,
            "status_code": error.status_code,
            "timestamp": error.timestamp,
            **error.context,
        }
        if additional_properties:
            properties.update(additional_properties)

        # Example telemetry calls (uncomment and configure as needed):
        # telemetry_client.track_exception(
        #     error,
        #     properties=properties,
        #     measurements={'duration_ms': properties.get('duration_ms', 0)}
        # )

        # For now, just log it (using lazy import to avoid circular dependencies)
        from .utils.logger import get_logger
        logger = get_logger()
        logger.debug(f"Telemetry: {error.error_code} - {error.message}", extra=properties)


# ==================== CREDENTIAL-SPECIFIC EXCEPTIONS ====================


class CredentialError(BaseError):
    """Base exception for credential-related errors."""

    def __init__(self, message: str = "Credential error", **kwargs):
        super().__init__(
            message=message, error_code=ErrorCode.INTEGRATION_ERROR, status_code=500, **kwargs
        )


class CredentialNotFoundError(BaseError):
    """Raised when a requested credential is not found."""

    def __init__(self, message: str = "Credential not found", **kwargs):
        super().__init__(message=message, error_code=ErrorCode.NOT_FOUND, status_code=404, **kwargs)


class CredentialExpiredError(BaseError):
    """Raised when attempting to use expired credentials."""

    def __init__(self, message: str = "Credential has expired", **kwargs):
        super().__init__(message=message, error_code=ErrorCode.EXPIRED, status_code=401, **kwargs)


class TenantIsolationViolationError(BaseError):
    """Raised when tenant isolation is violated in credential operations."""

    def __init__(self, message: str = "Tenant isolation violation detected", **kwargs):
        super().__init__(
            message=message, error_code=ErrorCode.PERMISSION_DENIED, status_code=403, **kwargs
        )


class TokenNotAvailableError(BaseError):
    """Raised when no valid tokens are available and cannot generate new ones."""

    def __init__(self, message: str = "No valid tokens available", **kwargs):
        super().__init__(
            message=message,
            error_code=ErrorCode.LIMIT_EXCEEDED,
            status_code=503,  # Service Temporarily Unavailable
            **kwargs,
        )
