"""
Error message schema for the API Exchange.

This module defines Pydantic models for error messages sent to error queues,
providing a standardized structure for error reporting and handling.
"""

import uuid
from datetime import UTC, datetime, timedelta
from enum import Enum
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field, field_validator

from src.constants import RecoveryStrategy

# ==================== ENUMS ====================


class ErrorTypeEnum(str, Enum):
    """Classification of different error types."""

    # General error categories
    VALIDATION_ERROR = "validation_error"
    CONNECTION_ERROR = "connection_error"
    AUTHENTICATION_ERROR = "authentication_error"
    AUTHORIZATION_ERROR = "authorization_error"
    RESOURCE_ERROR = "resource_error"
    TIMEOUT_ERROR = "timeout_error"
    SERVICE_UNAVAILABLE = "service_unavailable"

    # Data errors
    FORMAT_ERROR = "format_error"
    SCHEMA_ERROR = "schema_error"
    DESERIALIZATION_ERROR = "deserialization_error"
    MISSING_FIELD_ERROR = "missing_field_error"
    INVALID_TYPE_ERROR = "invalid_type_error"
    CONSTRAINT_ERROR = "constraint_error"
    BUSINESS_RULE_ERROR = "business_rule_error"

    # Processing errors
    ENTITY_NOT_FOUND = "entity_not_found"
    DUPLICATE_ENTITY = "duplicate_entity"
    STATE_TRANSITION_ERROR = "state_transition_error"
    PROCESSING_ERROR = "processing_error"

    # System errors
    CONFIGURATION_ERROR = "configuration_error"
    ENVIRONMENT_ERROR = "environment_error"
    DEPENDENCY_ERROR = "dependency_error"
    UNEXPECTED_ERROR = "unexpected_error"


class RecoverabilityEnum(str, Enum):
    """Classification of error recoverability."""

    # Error is not recoverable, manual intervention required
    NOT_RECOVERABLE = "not_recoverable"

    # Error is automatically recoverable
    AUTO_RECOVERABLE = "auto_recoverable"

    # Error requires conditional recovery logic
    CONDITIONAL = "conditional"

    # Recoverability is unknown
    UNKNOWN = "unknown"


class SeverityEnum(str, Enum):
    """Classification of error severity."""

    CRITICAL = "critical"  # Service is unusable
    ERROR = "error"  # Functionality is impaired
    WARNING = "warning"  # Potential issue, but operation continues
    INFO = "info"  # Informational only


# ==================== COMPONENT MODELS ====================


class ValidationErrorDetail(BaseModel):
    """Details about a specific validation error."""

    field: Optional[str] = None
    error_type: str
    message: str
    constraint: Optional[str] = None
    value: Optional[Any] = None
    expected_type: Optional[str] = None
    actual_type: Optional[str] = None
    expected_value: Optional[Any] = None
    context: Optional[Dict[str, Any]] = None


class EntityErrorContext(BaseModel):
    """Context information about an entity involved in an error."""

    entity_id: Optional[str] = None
    entity_type: Optional[str] = None
    external_id: Optional[str] = None
    current_state: Optional[str] = None
    attributes: Optional[Dict[str, Any]] = None
    version: Optional[int] = None


class ProcessingContext(BaseModel):
    """Context information about the processing environment."""

    processor_name: str
    operation_name: Optional[str] = None
    source_queue: Optional[str] = None
    destination_queue: Optional[str] = None
    message_id: Optional[str] = None
    correlation_id: Optional[str] = None
    operation_timestamp: datetime = Field(default_factory=lambda: datetime.now(UTC))


class SystemContext(BaseModel):
    """System-level context information."""

    host: Optional[str] = None
    environment: Optional[str] = None
    service_version: Optional[str] = None
    dependencies: Optional[Dict[str, str]] = None
    memory_usage_mb: Optional[float] = None
    queue_depth: Optional[Dict[str, int]] = None


class RecoveryInfo(BaseModel):
    """Information for error recovery."""

    recoverability: RecoverabilityEnum = RecoverabilityEnum.UNKNOWN
    retry_count: int = 0
    max_retries: Optional[int] = None
    retry_after_seconds: Optional[int] = None
    next_retry_time: Optional[datetime] = None
    recovery_strategy: Optional[str] = None
    recovery_action: Optional[str] = None
    manual_intervention_required: bool = False
    error_handled: bool = False


class ExceptionDetail(BaseModel):
    """Details about an exception that occurred."""

    exception_type: str
    exception_message: str
    stack_trace: Optional[List[str]] = None
    inner_exceptions: Optional[List["ExceptionDetail"]] = None


# ==================== MAIN ERROR MESSAGE MODEL ====================


class ErrorMessage(BaseModel):
    """
    Standardized error message format for the API Exchange.

    This model defines the structure of error messages sent to error queues,
    providing consistent error reporting and facilitating error handling.
    """

    # Core error information
    error_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    error_type: ErrorTypeEnum
    error_message: str
    severity: SeverityEnum = SeverityEnum.ERROR
    timestamp: datetime = Field(default_factory=lambda: datetime.now(UTC))

    # Tenant information
    tenant_id: Optional[str] = None

    # Context information
    entity_context: Optional[EntityErrorContext] = None
    processing_context: ProcessingContext
    system_context: Optional[SystemContext] = None

    # Error details
    validation_errors: Optional[List[ValidationErrorDetail]] = None
    exception_details: Optional[ExceptionDetail] = None
    original_data: Optional[Any] = None

    # Recovery information
    recovery_info: RecoveryInfo = Field(default_factory=RecoveryInfo)

    # Additional metadata
    metadata: Dict[str, Any] = Field(default_factory=dict)

    @field_validator("original_data")
    def truncate_large_data(cls, v):
        """Truncate very large data to prevent excessive message sizes."""
        if v is not None:
            data_str = str(v)
            if len(data_str) > 10000:  # Limit to 10KB
                return data_str[:10000] + "... [truncated]"
        return v


# ==================== FACTORY FUNCTIONS ====================


def create_validation_error(
    message: str,
    processor_name: str,
    tenant_id: Optional[str] = None,
    entity_context: Optional[Dict[str, Any]] = None,
    validation_errors: Optional[List[Dict[str, Any]]] = None,
    original_data: Optional[Any] = None,
    severity: SeverityEnum = SeverityEnum.ERROR,
) -> ErrorMessage:
    """
    Create a validation error message.

    Args:
        message: Main error message
        processor_name: Name of the processor where the error occurred
        tenant_id: Optional tenant ID
        entity_context: Optional entity context information
        validation_errors: List of validation error details
        original_data: The data that failed validation
        severity: Error severity

    Returns:
        Formatted ErrorMessage
    """
    # Convert entity context dictionary to EntityErrorContext model
    entity_error_context = None
    if entity_context:
        entity_error_context = EntityErrorContext(**entity_context)

    # Convert validation errors dictionaries to ValidationErrorDetail models
    validation_error_details = None
    if validation_errors:
        validation_error_details = [ValidationErrorDetail(**error) for error in validation_errors]

    # Create the error message
    return ErrorMessage(
        error_type=ErrorTypeEnum.VALIDATION_ERROR,
        error_message=message,
        tenant_id=tenant_id,
        severity=severity,
        entity_context=entity_error_context,
        processing_context=ProcessingContext(processor_name=processor_name),
        validation_errors=validation_error_details,
        original_data=original_data,
        recovery_info=RecoveryInfo(
            recoverability=RecoverabilityEnum.NOT_RECOVERABLE, manual_intervention_required=True
        ),
    )


def create_connection_error(
    message: str,
    processor_name: str,
    tenant_id: Optional[str] = None,
    dependency_name: Optional[str] = None,
    exception: Optional[Exception] = None,
    retry_after_seconds: Optional[int] = 60,
    severity: SeverityEnum = SeverityEnum.ERROR,
) -> ErrorMessage:
    """
    Create a connection error message.

    Args:
        message: Main error message
        processor_name: Name of the processor where the error occurred
        tenant_id: Optional tenant ID
        dependency_name: Name of the dependency that failed
        exception: Optional exception that caused the error
        retry_after_seconds: Suggested retry delay in seconds
        severity: Error severity

    Returns:
        Formatted ErrorMessage
    """
    # Create exception details if available
    exception_details = None
    if exception:
        exception_details = ExceptionDetail(
            exception_type=type(exception).__name__,
            exception_message=str(exception),
            stack_trace=None,  # Stack trace can be added if needed
        )

    # Create system context with dependency information
    system_ctx = None
    if dependency_name:
        system_ctx = SystemContext(dependencies={dependency_name: "unavailable"})

    # Create the error message
    return ErrorMessage(
        error_type=ErrorTypeEnum.CONNECTION_ERROR,
        error_message=message,
        tenant_id=tenant_id,
        severity=severity,
        processing_context=ProcessingContext(processor_name=processor_name),
        system_context=system_ctx,
        exception_details=exception_details,
        recovery_info=RecoveryInfo(
            recoverability=RecoverabilityEnum.AUTO_RECOVERABLE,
            retry_after_seconds=retry_after_seconds,
            next_retry_time=(
                datetime.now(UTC) + timedelta(seconds=retry_after_seconds)
                if retry_after_seconds
                else None
            ),
            recovery_strategy=RecoveryStrategy.RETRY.value,
        ),
    )


def create_processing_error(
    message: str,
    processor_name: str,
    tenant_id: Optional[str] = None,
    entity_context: Optional[Dict[str, Any]] = None,
    exception: Optional[Exception] = None,
    original_data: Optional[Any] = None,
    recoverability: RecoverabilityEnum = RecoverabilityEnum.CONDITIONAL,
    severity: SeverityEnum = SeverityEnum.ERROR,
) -> ErrorMessage:
    """
    Create a processing error message.

    Args:
        message: Main error message
        processor_name: Name of the processor where the error occurred
        tenant_id: Optional tenant ID
        entity_context: Optional entity context information
        exception: Optional exception that caused the error
        original_data: The data that was being processed
        recoverability: Whether the error is recoverable
        severity: Error severity

    Returns:
        Formatted ErrorMessage
    """
    # Convert entity context dictionary to EntityErrorContext model
    entity_error_context = None
    if entity_context:
        entity_error_context = EntityErrorContext(**entity_context)

    # Create exception details if available
    exception_details = None
    if exception:
        exception_details = ExceptionDetail(
            exception_type=type(exception).__name__,
            exception_message=str(exception),
            stack_trace=None,  # Stack trace can be added if needed
        )

    # Create the error message
    return ErrorMessage(
        error_type=ErrorTypeEnum.PROCESSING_ERROR,
        error_message=message,
        tenant_id=tenant_id,
        severity=severity,
        entity_context=entity_error_context,
        processing_context=ProcessingContext(processor_name=processor_name),
        exception_details=exception_details,
        original_data=original_data,
        recovery_info=RecoveryInfo(
            recoverability=recoverability,
            manual_intervention_required=recoverability != RecoverabilityEnum.AUTO_RECOVERABLE,
            recovery_strategy=(
                RecoveryStrategy.RETRY.value
                if recoverability == RecoverabilityEnum.AUTO_RECOVERABLE
                else RecoveryStrategy.MANUAL_REVIEW.value
            ),
        ),
    )


def create_system_error(
    message: str,
    processor_name: str,
    exception: Exception,
    tenant_id: Optional[str] = None,
    severity: SeverityEnum = SeverityEnum.CRITICAL,
) -> ErrorMessage:
    """
    Create a system-level error message.

    Args:
        message: Main error message
        processor_name: Name of the processor where the error occurred
        exception: Exception that caused the error
        tenant_id: Optional tenant ID
        severity: Error severity

    Returns:
        Formatted ErrorMessage
    """
    # Create exception details
    exception_details = ExceptionDetail(
        exception_type=type(exception).__name__,
        exception_message=str(exception),
        stack_trace=None,  # Stack trace can be added if needed
    )

    # Create the error message
    return ErrorMessage(
        error_type=ErrorTypeEnum.UNEXPECTED_ERROR,
        error_message=message,
        tenant_id=tenant_id,
        severity=severity,
        processing_context=ProcessingContext(processor_name=processor_name),
        exception_details=exception_details,
        recovery_info=RecoveryInfo(
            recoverability=RecoverabilityEnum.NOT_RECOVERABLE,
            manual_intervention_required=True,
            recovery_strategy=RecoveryStrategy.SYSTEM_RESTART.value,
        ),
    )


def error_from_exception(
    exception: Exception,
    processor_name: str,
    tenant_id: Optional[str] = None,
    entity_context: Optional[Dict[str, Any]] = None,
    original_data: Optional[Any] = None,
) -> ErrorMessage:
    """
    Create an error message from an exception.

    This function attempts to determine the appropriate error type
    based on the exception class and create an appropriate error message.

    Args:
        exception: Exception that occurred
        processor_name: Name of the processor where the error occurred
        tenant_id: Optional tenant ID
        entity_context: Optional entity context information
        original_data: The data that was being processed

    Returns:
        Formatted ErrorMessage
    """
    exception_type = type(exception).__name__
    message = str(exception)

    # Determine error type based on exception class
    if "Validation" in exception_type or "Schema" in exception_type:
        return create_validation_error(
            message=message,
            processor_name=processor_name,
            tenant_id=tenant_id,
            entity_context=entity_context,
            original_data=original_data,
        )
    elif "Timeout" in exception_type or "Connection" in exception_type:
        return create_connection_error(
            message=message, processor_name=processor_name, tenant_id=tenant_id, exception=exception
        )
    elif "NotFound" in exception_type:
        return create_processing_error(
            message=message,
            processor_name=processor_name,
            tenant_id=tenant_id,
            entity_context=entity_context,
            exception=exception,
            original_data=original_data,
            recoverability=RecoverabilityEnum.NOT_RECOVERABLE,
        )
    else:
        # Default to general processing error
        return create_processing_error(
            message=message,
            processor_name=processor_name,
            tenant_id=tenant_id,
            entity_context=entity_context,
            exception=exception,
            original_data=original_data,
        )
