"""
SQLAlchemy models for the Entity Integration System.

This module provides a common entry point for all entity models,
importing and re-exporting them from their respective modules.
"""

# Import enums from centralized location
from ..enums import TransitionTypeEnum

# Import models
from .db_api_token_models import APIToken, APITokenUsageLog, TokenCoordination

# Import base definitions
from .db_base import (
    JSON,
    BaseModel,
    EncryptedBinary,
    EntityStateEnum,
    EntityTypeEnum,
    ErrorTypeEnum,
    RefTypeEnum,
)

# Import configuration
from .db_config import Base, DatabaseConfig, DatabaseManager, import_all_models
from .db_credential_models import ExternalCredential
from .db_entity_models import Entity
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
    "TransitionTypeEnum",
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
    "Tenant",
    "TenantNotFoundError",
]
