"""
Base output handler interface and common functionality.

Defines the abstract OutputHandler interface that all concrete handlers must implement,
along with common result and error classes used throughout the output handler system.
"""

import math
import time
from abc import ABC, abstractmethod
from datetime import datetime, UTC
from enum import Enum
from typing import Any, Dict, Optional, TYPE_CHECKING

from pydantic import BaseModel, Field

from src.utils.logger import get_logger

if TYPE_CHECKING:
    from src.processors.v2.message import Message
    from src.processors.processing_result import ProcessingResult


def calculate_exponential_backoff(
    retry_count: int,
    base_delay: int = 1,
    max_delay: int = 300,
    multiplier: float = 2.0,
    jitter: bool = True
) -> int:
    """
    Calculate exponential backoff delay with jitter.
    
    Args:
        retry_count: Current retry attempt (0-based)
        base_delay: Base delay in seconds (default: 1)
        max_delay: Maximum delay in seconds (default: 300 = 5 minutes)
        multiplier: Exponential multiplier (default: 2.0)
        jitter: Whether to add randomization to prevent thundering herd (default: True)
        
    Returns:
        Delay in seconds before next retry
        
    Example:
        retry_count=0: ~1s
        retry_count=1: ~2s  
        retry_count=2: ~4s
        retry_count=3: ~8s
        retry_count=4: ~16s
        retry_count=5: ~32s
        retry_count=6: ~64s
        retry_count=7: ~128s
        retry_count=8: ~256s
        retry_count=9: 300s (capped at max_delay)
    """
    if retry_count < 0:
        return base_delay
        
    # Calculate exponential delay: base_delay * (multiplier ^ retry_count)
    delay = base_delay * (multiplier ** retry_count)
    
    # Cap at maximum delay
    delay = min(delay, max_delay)
    
    # Add jitter to prevent thundering herd effect
    # Jitter adds ±25% randomization
    if jitter:
        import random
        jitter_range = delay * 0.25
        delay = delay + random.uniform(-jitter_range, jitter_range)
        
    # Ensure minimum delay and return as integer
    # But don't go below the base_delay to maintain backoff progression
    return max(int(delay), base_delay)


class OutputHandlerStatus(str, Enum):
    """Status values for output handler execution results."""
    
    SUCCESS = "success"           # Output delivered successfully
    FAILED = "failed"            # Output failed and should not retry
    RETRYABLE_ERROR = "retryable_error"  # Output failed but can be retried
    SKIPPED = "skipped"          # Output was skipped (e.g., condition not met)
    PARTIAL_SUCCESS = "partial_success"  # Some outputs succeeded, some failed


class OutputHandlerError(Exception):
    """
    Exception raised by output handlers for processing errors.
    
    Provides structured error information including retry capability
    and error categorization for proper handling by the framework.
    """
    
    def __init__(
        self,
        message: str,
        error_code: Optional[str] = None,
        can_retry: bool = False,
        retry_after_seconds: Optional[int] = None,
        error_details: Optional[Dict[str, Any]] = None,
        original_exception: Optional[Exception] = None
    ):
        super().__init__(message)
        self.message = message
        self.error_code = error_code or "OUTPUT_HANDLER_ERROR"
        self.can_retry = can_retry
        self.retry_after_seconds = retry_after_seconds
        self.error_details = error_details or {}
        self.original_exception = original_exception
    
    def calculate_retry_delay(self, retry_count: int, base_delay: Optional[int] = None) -> int:
        """
        Calculate retry delay using exponential backoff.
        
        Args:
            retry_count: Current retry attempt (0-based)
            base_delay: Override base delay, otherwise uses retry_after_seconds or default
            
        Returns:
            Delay in seconds before next retry
        """
        # Use provided base_delay, or fall back to retry_after_seconds, or default to 1
        effective_base_delay = base_delay or self.retry_after_seconds or 1
        
        return calculate_exponential_backoff(
            retry_count=retry_count,
            base_delay=effective_base_delay,
            max_delay=300,  # 5 minutes max
            multiplier=2.0,
            jitter=True
        )


class OutputHandlerResult(BaseModel):
    """
    Result of output handler execution.
    
    Contains detailed information about the output operation including
    success status, error information, and execution metadata.
    """
    
    status: OutputHandlerStatus = Field(description="Status of the output operation")
    success: bool = Field(description="Whether the output was delivered successfully")
    
    # Output metadata
    handler_name: str = Field(description="Name of the handler that processed the output")
    destination: str = Field(description="Target destination (queue name, file path, etc.)")
    
    # Timing information
    execution_duration_ms: float = Field(description="Time taken to execute output in milliseconds")
    completed_at: datetime = Field(default_factory=lambda: datetime.now(UTC), description="When execution completed")
    
    # Error information (populated only on failure)
    error_message: Optional[str] = Field(default=None, description="Human-readable error message")
    error_code: Optional[str] = Field(default=None, description="Machine-readable error code")
    error_details: Dict[str, Any] = Field(default_factory=dict, description="Additional error context")
    
    # Retry information
    can_retry: bool = Field(default=False, description="Whether this operation can be retried")
    retry_after_seconds: Optional[int] = Field(default=None, description="Suggested delay before retry")
    
    # Additional metadata
    metadata: Dict[str, Any] = Field(default_factory=dict, description="Handler-specific metadata")
    
    @classmethod
    def create_success(
        cls,
        handler_name: str,
        destination: str,
        execution_duration_ms: float,
        metadata: Optional[Dict[str, Any]] = None
    ) -> "OutputHandlerResult":
        """Create a successful output result."""
        return cls(
            status=OutputHandlerStatus.SUCCESS,
            success=True,
            handler_name=handler_name,
            destination=destination,
            execution_duration_ms=execution_duration_ms,
            metadata=metadata or {}
        )
    
    @classmethod
    def create_failure(
        cls,
        handler_name: str,
        destination: str,
        execution_duration_ms: float,
        error_message: str,
        error_code: Optional[str] = None,
        can_retry: bool = False,
        retry_after_seconds: Optional[int] = None,
        error_details: Optional[Dict[str, Any]] = None,
        metadata: Optional[Dict[str, Any]] = None
    ) -> "OutputHandlerResult":
        """Create a failed output result."""
        status = OutputHandlerStatus.RETRYABLE_ERROR if can_retry else OutputHandlerStatus.FAILED
        
        return cls(
            status=status,
            success=False,
            handler_name=handler_name,
            destination=destination,
            execution_duration_ms=execution_duration_ms,
            error_message=error_message,
            error_code=error_code,
            can_retry=can_retry,
            retry_after_seconds=retry_after_seconds,
            error_details=error_details or {},
            metadata=metadata or {}
        )
    
    @classmethod
    def create_skipped(
        cls,
        handler_name: str,
        destination: str,
        execution_duration_ms: float,
        reason: str,
        metadata: Optional[Dict[str, Any]] = None
    ) -> "OutputHandlerResult":
        """Create a skipped output result."""
        return cls(
            status=OutputHandlerStatus.SKIPPED,
            success=True,  # Skipped is considered successful
            handler_name=handler_name,
            destination=destination,
            execution_duration_ms=execution_duration_ms,
            metadata={
                **(metadata or {}),
                "skip_reason": reason
            }
        )


class OutputHandler(ABC):
    """
    Abstract base class for all output handlers.
    
    Output handlers are responsible for delivering processed messages to their
    target destinations (queues, files, HTTP endpoints, databases, etc.).
    
    Each handler implements the `handle()` method to perform the actual output
    operation and returns an OutputHandlerResult indicating success or failure.
    
    Handlers should be stateless and thread-safe to allow reuse across multiple
    processor executions.
    """
    
    def __init__(self, destination: str, config: Optional[Dict[str, Any]] = None):
        """
        Initialize the output handler.
        
        Args:
            destination: Target destination identifier (queue name, file path, URL, etc.)
            config: Handler-specific configuration options
        """
        self.destination = destination
        self.config = config or {}
        self.logger = get_logger()
        self._handler_name = self.__class__.__name__
    
    @abstractmethod
    def handle(self, message: "Message", result: "ProcessingResult") -> OutputHandlerResult:
        """
        Handle output delivery for the processed message.
        
        This method must be implemented by concrete handlers to perform the actual
        output operation (sending to queue, writing to file, HTTP POST, etc.).
        
        Args:
            message: The original message that was processed
            result: The processing result containing output data
            
        Returns:
            OutputHandlerResult indicating success/failure and execution details
            
        Raises:
            OutputHandlerError: For recoverable errors that may be retried
            Exception: For unrecoverable errors that should fail immediately
        """
        pass
    
    def validate_configuration(self) -> bool:
        """
        Validate handler configuration.
        
        Called before handling to ensure the handler is properly configured.
        Default implementation always returns True.
        
        Returns:
            True if configuration is valid, False otherwise
        """
        return True
    
    def get_handler_info(self) -> Dict[str, Any]:
        """
        Get handler metadata and configuration information.
        
        Returns:
            Dictionary with handler information for logging and debugging
        """
        return {
            "handler_name": self._handler_name,
            "destination": self.destination,
            "config_keys": list(self.config.keys()),
            "supports_retry": self.supports_retry()
        }
    
    def supports_retry(self) -> bool:
        """
        Indicate whether this handler supports retry operations.
        
        Default implementation returns True. Override to customize retry behavior.
        
        Returns:
            True if handler supports retries, False otherwise
        """
        return True
    
    def _execute_with_timing(self, operation) -> tuple[Any, float]:
        """
        Execute an operation and measure execution time.
        
        Helper method for handlers to consistently measure execution duration.
        
        Args:
            operation: Callable to execute
            
        Returns:
            Tuple of (operation_result, duration_in_milliseconds)
        """
        start_time = time.time()
        try:
            result = operation()
            duration_ms = (time.time() - start_time) * 1000
            return result, duration_ms
        except Exception as e:
            duration_ms = (time.time() - start_time) * 1000
            raise e
    
    def _create_success_result(
        self, 
        execution_duration_ms: float,
        metadata: Optional[Dict[str, Any]] = None
    ) -> OutputHandlerResult:
        """Create a success result for this handler."""
        return OutputHandlerResult.create_success(
            handler_name=self._handler_name,
            destination=self.destination,
            execution_duration_ms=execution_duration_ms,
            metadata=metadata
        )
    
    def _create_failure_result(
        self,
        execution_duration_ms: float,
        error_message: str,
        error_code: Optional[str] = None,
        can_retry: bool = False,
        retry_after_seconds: Optional[int] = None,
        error_details: Optional[Dict[str, Any]] = None,
        metadata: Optional[Dict[str, Any]] = None
    ) -> OutputHandlerResult:
        """Create a failure result for this handler."""
        return OutputHandlerResult.create_failure(
            handler_name=self._handler_name,
            destination=self.destination,
            execution_duration_ms=execution_duration_ms,
            error_message=error_message,
            error_code=error_code,
            can_retry=can_retry,
            retry_after_seconds=retry_after_seconds,
            error_details=error_details,
            metadata=metadata
        )
    
    def _create_failure_result_with_backoff(
        self,
        execution_duration_ms: float,
        error_message: str,
        retry_count: int,
        error_code: Optional[str] = None,
        base_delay: Optional[int] = None,
        error_details: Optional[Dict[str, Any]] = None,
        metadata: Optional[Dict[str, Any]] = None
    ) -> OutputHandlerResult:
        """
        Create a failure result with exponential backoff retry delay.
        
        Args:
            execution_duration_ms: How long the operation took
            error_message: Human-readable error description
            retry_count: Current retry attempt (0-based)
            error_code: Machine-readable error code
            base_delay: Base delay for exponential backoff (default: handler-specific)
            error_details: Additional error context
            metadata: Handler-specific metadata
            
        Returns:
            OutputHandlerResult with exponentially calculated retry delay
        """
        # Calculate exponential backoff delay
        retry_delay = calculate_exponential_backoff(
            retry_count=retry_count,
            base_delay=base_delay or 1,
            max_delay=300,  # 5 minutes max
            multiplier=2.0,
            jitter=True
        )
        
        # Create enhanced error details with backoff info
        enhanced_details = {
            **(error_details or {}),
            "retry_count": retry_count,
            "calculated_backoff_delay": retry_delay,
            "backoff_algorithm": "exponential_with_jitter"
        }
        
        return OutputHandlerResult.create_failure(
            handler_name=self._handler_name,
            destination=self.destination,
            execution_duration_ms=execution_duration_ms,
            error_message=error_message,
            error_code=error_code,
            can_retry=True,  # Always retryable when using backoff
            retry_after_seconds=retry_delay,
            error_details=enhanced_details,
            metadata=metadata
        )
    
    def _create_skipped_result(
        self,
        execution_duration_ms: float,
        reason: str,
        metadata: Optional[Dict[str, Any]] = None
    ) -> OutputHandlerResult:
        """Create a skipped result for this handler."""
        return OutputHandlerResult.create_skipped(
            handler_name=self._handler_name,
            destination=self.destination,
            execution_duration_ms=execution_duration_ms,
            reason=reason,
            metadata=metadata
        )
    
    def __repr__(self) -> str:
        """String representation of the handler."""
        return f"{self._handler_name}(destination='{self.destination}')"
    
    def __str__(self) -> str:
        """Human-readable string representation."""
        return f"{self._handler_name} -> {self.destination}"