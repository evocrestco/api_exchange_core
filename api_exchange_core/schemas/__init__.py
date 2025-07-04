"""
Validation schemas package.

This package contains the validation schemas used for input/output validation
in the application. These schemas define the expected structure of data for
API requests and responses.

Note: Adapter-specific and canonical schemas are not included in the open-source
release and are available as part of commercial add-ons.
"""

from .entity_schema import (
    EntityBase,
    EntityCreate,
    EntityFilter,
    EntityRead,
    EntityReference,
    EntityUpdate,
)
from .error_message_schema import (
    EntityErrorContext,
    ErrorMessage,
    ErrorTypeEnum,
    ExceptionDetail,
    ProcessingContext,
    RecoverabilityEnum,
    RecoveryInfo,
    SeverityEnum,
    SystemContext,
    ValidationErrorDetail,
    create_connection_error,
    create_processing_error,
    create_system_error,
    create_validation_error,
    error_from_exception,
)
from .metric_model import (
    Metric,
    QueueMetric,
)
from .pipeline_state_schema import (
    EntityQuery,
    PipelineHistoryResponse,
    PipelineStateHistoryCreate,
    PipelineStateHistoryRead,
    PipelineStateQuery,
    ProcessorMetricsRead,
    ProcessorMetricsResponse,
)
from .pipeline_state_schema import StateTransitionCreate as PipelineStateTransitionCreate
from .pipeline_state_schema import StateTransitionRead as PipelineStateTransitionRead
from .pipeline_state_schema import (
    StatusSummaryRead,
)
from .processing_error_schema import (
    ProcessingErrorBase,
    ProcessingErrorCreate,
    ProcessingErrorFilter,
    ProcessingErrorRead,
)
from .state_transition_schema import (
    EntityStateHistory,
    StateTransitionBase,
    StateTransitionCreate,
    StateTransitionFilter,
    StateTransitionRead,
    StateTransitionStats,
)
from .tenant_schema import (
    TenantConfigUpdate,
    TenantConfigValue,
    TenantCreate,
    TenantUpdate,
)

# Note: Canonical schemas are intentionally not imported here as they are part of
# commercial add-ons and not included in the open-source release.

__all__ = [
    # Entity schemas
    "EntityBase",
    "EntityCreate",
    "EntityRead",
    "EntityUpdate",
    "EntityFilter",
    "EntityReference",
    # Processing error schemas
    "ProcessingErrorBase",
    "ProcessingErrorCreate",
    "ProcessingErrorRead",
    "ProcessingErrorFilter",
    # State transition schemas
    "StateTransitionBase",
    "StateTransitionCreate",
    "StateTransitionRead",
    "StateTransitionFilter",
    "StateTransitionStats",
    "EntityStateHistory",
    # Tenant schemas
    "TenantConfigValue",
    "TenantCreate",
    "TenantUpdate",
    "TenantConfigUpdate",
    # Metric schemas
    "Metric",
    "QueueMetric",
    # Error message schemas
    "ErrorMessage",
    "ErrorTypeEnum",
    "RecoverabilityEnum",
    "SeverityEnum",
    "ValidationErrorDetail",
    "EntityErrorContext",
    "ProcessingContext",
    "SystemContext",
    "RecoveryInfo",
    "ExceptionDetail",
    "create_validation_error",
    "create_connection_error",
    "create_processing_error",
    "create_system_error",
    "error_from_exception",
    # Pipeline state schemas
    "EntityQuery",
    "PipelineHistoryResponse",
    "PipelineStateHistoryCreate",
    "PipelineStateHistoryRead",
    "PipelineStateQuery",
    "ProcessorMetricsRead",
    "ProcessorMetricsResponse",
    "PipelineStateTransitionCreate",
    "PipelineStateTransitionRead",
    "StatusSummaryRead",
]
