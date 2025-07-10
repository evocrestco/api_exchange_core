"""
Simplified SQLAlchemy models for V2 framework.

This module provides a common entry point for all simplified models.
"""

# Import models
from .db_api_token_models import APIToken

# Import base definitions
from .db_base import (
    JSON,
    EncryptedBinary,
    TimestampMixin,
    UUIDMixin,
    utc_now,
)

# Import configuration
from .db_config import (
    Base,
    DatabaseConfig,
    DatabaseManager,
    get_development_config,
    get_production_config,
    import_all_models,
)
from .db_credential_models import ExternalCredential
from .db_pipeline_definition_models import PipelineDefinition, PipelineStepDefinition
from .db_pipeline_tracking_models import PipelineExecution, PipelineMessage, PipelineStep
from .db_tenant_models import Tenant

# Re-export all models for easy access
__all__ = [
    # Base definitions
    "Base",
    "JSON",
    "EncryptedBinary",
    "TimestampMixin",
    "UUIDMixin",
    "utc_now",
    # Configuration
    "DatabaseConfig",
    "DatabaseManager",
    "import_all_models",
    "get_production_config",
    "get_development_config",
    # Models
    "APIToken",
    "ExternalCredential",
    "PipelineDefinition",
    "PipelineStepDefinition",
    "PipelineExecution",
    "PipelineStep",
    "PipelineMessage",
    "Tenant",
]
