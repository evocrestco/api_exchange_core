"""
SQLAlchemy models for the Entity Integration System.

This module provides a common entry point for all entity models,
importing and re-exporting them from their respective modules.
"""

# Import base definitions
from src.db.db_base import (
    JSON,
    BaseModel,
    EntityStateEnum,
    EntityTypeEnum,
    ErrorTypeEnum,
    RefTypeEnum,
)
from src.db.db_error_models import ProcessingError
from src.db.db_state_transition_models import StateTransition

# Re-export all models for easy access
__all__ = [
    # Base definitions
    "BaseModel",
    "JSON",
    "EntityTypeEnum",
    "EntityStateEnum",
    "ErrorTypeEnum",
    "RefTypeEnum",
    # Tracking models
    "StateTransition",
    "ProcessingError",
]
