"""
Enums used across the api_exchange_core package.

This module contains enum definitions that are used by multiple modules
to avoid circular import issues.
"""

import enum


class TransitionTypeEnum(enum.Enum):
    """Types of state transitions."""
    
    NORMAL = "NORMAL"
    ERROR = "ERROR"
    RETRY = "RETRY"
    MANUAL = "MANUAL"
    TIMEOUT = "TIMEOUT"