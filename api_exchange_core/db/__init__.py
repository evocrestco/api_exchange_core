"""
SQLAlchemy models for the Entity Integration System.

This module provides a common entry point for all entity models,
importing and re-exporting them from their respective modules.
"""

# Import base definitions
from .db_base import (
    JSON,
    BaseModel,
    EntityStateEnum,
    EntityTypeEnum,
    ErrorTypeEnum,
    RefTypeEnum,
    EncryptedBinary,
)

# Import configuration
from .db_config import Base, DatabaseConfig, DatabaseManager, import_all_models

# Import models
from .db_api_token_models import APIToken, APITokenUsageLog, TokenCoordination
from .db_credential_models import ExternalCredential
from .db_entity_models import Entity
from .db_error_models import ProcessingError
from .db_state_transition_models import StateTransition, TransitionTypeEnum
from .db_tenant_models import Tenant, TenantNotFoundError

# Re-export all models for easy access
__all__ = [
    # Base definitions
    "Base",
    "BaseModel",
    "JSON",
    "EncryptedBinary",
    "EntityTypeEnum",
    "EntityStateEnum",
    "ErrorTypeEnum",
    "RefTypeEnum",
    # Configuration
    "DatabaseConfig",
    "DatabaseManager",
    "import_all_models",
    # Models
    "APIToken",
    "APITokenUsageLog",
    "TokenCoordination",
    "ExternalCredential",
    "Entity",
    "ProcessingError",
    "StateTransition",
    "TransitionTypeEnum",
    "Tenant",
    "TenantNotFoundError",
]
