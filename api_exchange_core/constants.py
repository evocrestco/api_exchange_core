"""
Constants and enums for the API Exchange Core framework.

This module centralizes all magic strings and constants used throughout
the framework to ensure consistency and maintainability.
"""

from enum import Enum


class QueueName(str, Enum):
    """Standard queue names used in the framework."""

    METRICS = "metrics-queue"
    LOGS = "logs-queue"
    ERROR = "error-queue"
    PROCESSING = "processing-queue"
    RETRY = "retry-queue"
    DLQ = "dead-letter-queue"


class OperationStatus(str, Enum):
    """Status values for operations."""

    SUCCESS = "success"
    ERROR = "error"
    IN_PROGRESS = "in_progress"
    PENDING = "pending"
    CANCELLED = "cancelled"


class RecoveryStrategy(str, Enum):
    """Error recovery strategies."""

    RETRY = "retry"
    MANUAL_REVIEW = "manual_review"
    SYSTEM_RESTART = "system_restart"
    SKIP = "skip"
    COMPENSATE = "compensate"


class QueueOperation(str, Enum):
    """Queue operation types for metrics."""

    SEND = "send"
    RECEIVE = "receive"
    PEEK = "peek"
    DELETE = "delete"
    UPDATE = "update"


class DependencyStatus(str, Enum):
    """Status of external dependencies."""

    AVAILABLE = "available"
    UNAVAILABLE = "unavailable"
    DEGRADED = "degraded"
    UNKNOWN = "unknown"


class LogLevel(str, Enum):
    """Standard logging levels."""

    DEBUG = "DEBUG"
    INFO = "INFO"
    WARNING = "WARNING"
    ERROR = "ERROR"
    CRITICAL = "CRITICAL"


class EnvironmentVariable(str, Enum):
    """Standard environment variable names."""

    AZURE_STORAGE_CONNECTION = "AzureWebJobsStorage"
    DATABASE_URL = "DATABASE_URL"
    APP_ENV = "APP_ENV"
    LOG_LEVEL = "LOG_LEVEL"
    TENANT_ID = "TENANT_ID"
    API_KEY = "API_KEY"
    FEATURE_FLAGS = "FEATURE_FLAGS"
    ENVIRONMENT = "ENVIRONMENT"
    DEBUG = "DEBUG"
    FUNCTION_NAME = "FUNCTION_NAME"
    INVOCATION_ID = "INVOCATION_ID"


class LogContextKey(str, Enum):
    """Standard keys for logging context."""

    OPERATION_ID = "operation_id"
    CORRELATION_ID = "correlation_id"
    TENANT_ID = "tenant_id"
    ENTITY_ID = "entity_id"
    DURATION_MS = "duration_ms"
    STATUS = "status"
    ERROR_CODE = "error_code"
    SOURCE_MODULE = "source_module"
    OPERATION = "operation"
    USER_ID = "user_id"
    REQUEST_ID = "request_id"


class ProcessingStep(str, Enum):
    """Standard processing steps in the pipeline."""

    RECEIVED = "received"
    VALIDATION = "validation"
    TRANSFORMATION = "transformation"
    ENRICHMENT = "enrichment"
    ROUTING = "routing"
    DELIVERY = "delivery"
    COMPLETED = "completed"
    ERROR_HANDLING = "error_handling"


class SourceSystem(str, Enum):
    """Common source system identifiers."""

    INTERNAL = "internal"
    EXTERNAL_API = "external_api"
    FILE_IMPORT = "file_import"
    MANUAL_ENTRY = "manual_entry"
    SCHEDULED_JOB = "scheduled_job"
    WEBHOOK = "webhook"
    MESSAGE_QUEUE = "message_queue"


class ErrorCategory(str, Enum):
    """Categories of errors for classification."""

    VALIDATION = "validation"
    TRANSFORMATION = "transformation"
    NETWORK = "network"
    AUTHENTICATION = "authentication"
    AUTHORIZATION = "authorization"
    RATE_LIMIT = "rate_limit"
    SYSTEM = "system"
    BUSINESS_LOGIC = "business_logic"


class FeatureFlag(str, Enum):
    """Feature flags for runtime configuration."""

    ENHANCED_LOGGING = "enhanced_logging"
    ASYNC_PROCESSING = "async_processing"
    BATCH_OPTIMIZATION = "batch_optimization"
    CACHE_ENABLED = "cache_enabled"
    METRICS_COLLECTION = "metrics_collection"
    RETRY_ENABLED = "retry_enabled"
    CIRCUIT_BREAKER = "circuit_breaker"


# Numeric constants
class Limits:
    """System limits and thresholds."""

    MAX_RETRY_ATTEMPTS = 3
    DEFAULT_BATCH_SIZE = 100
    MAX_BATCH_SIZE = 1000
    DEFAULT_PAGE_SIZE = 50
    MAX_PAGE_SIZE = 500
    DEFAULT_TIMEOUT_SECONDS = 30
    MAX_TIMEOUT_SECONDS = 300
    DEFAULT_CACHE_TTL_SECONDS = 300
    MAX_QUEUE_MESSAGE_SIZE_KB = 64


# Time-related constants (in seconds)
class Timeouts:
    """Timeout values in seconds."""

    DATABASE_QUERY = 30
    EXTERNAL_API_CALL = 60
    QUEUE_OPERATION = 10
    CACHE_OPERATION = 5
    HEALTH_CHECK = 10
    SHUTDOWN_GRACE_PERIOD = 30
