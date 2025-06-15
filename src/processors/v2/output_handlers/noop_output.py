"""
No-operation output handler.

Provides a handler implementation for processors that don't produce any output
or need to explicitly indicate no downstream routing is required.
"""

from typing import TYPE_CHECKING, Any, Dict, Optional

from .base import OutputHandler, OutputHandlerResult

if TYPE_CHECKING:
    from src.processors.processing_result import ProcessingResult
    from src.processors.v2.message import Message


class NoOpOutputHandler(OutputHandler):
    """
    No-operation output handler for processors with no output.

    This handler is used when a processor completes successfully but doesn't
    need to route any output to downstream systems. It provides a clean way
    to indicate intentional completion without output.

    Use cases:
    - Terminal processors that only store data without forwarding
    - Processors that perform side effects (notifications, logging, metrics)
    - Conditional processors that sometimes produce no output
    - Testing scenarios where output routing should be disabled

    Configuration:
        - reason: Optional explanation for why no output is produced
        - metadata: Additional metadata to include in the result

    Example:
        handler = NoOpOutputHandler(
            destination="terminal",
            config={
                "reason": "Final processor in pipeline - no downstream routing needed",
                "metadata": {"processor_type": "terminal", "side_effects": ["notification_sent"]}
            }
        )
    """

    def __init__(self, destination: str = "noop", config: Optional[Dict[str, Any]] = None):
        """
        Initialize the no-operation output handler.

        Args:
            destination: Logical destination name for logging (default: "noop")
            config: Configuration dictionary with reason and metadata
        """
        super().__init__(destination, config)

        # Extract configuration
        self.reason = self.config.get("reason", "No output required")
        self.metadata = self.config.get("metadata", {})

    def validate_configuration(self) -> bool:
        """Validate no-op handler configuration (always valid)."""
        return True

    def supports_retry(self) -> bool:
        """No-op handlers don't support retry (always succeed)."""
        return False

    def handle(self, message: "Message", result: "ProcessingResult") -> OutputHandlerResult:
        """
        Handle no-operation output (always succeeds immediately).

        Args:
            message: Original message that was processed
            result: Processing result containing output data

        Returns:
            OutputHandlerResult indicating successful no-operation
        """

        def _execute_noop():
            # Log the no-op operation for debugging/auditing
            self.logger.debug(
                "No-op output handler executed - no downstream routing",
                extra={
                    "handler_name": self._handler_name,
                    "destination": self.destination,
                    "message_id": message.message_id,
                    "correlation_id": message.correlation_id,
                    "reason": self.reason,
                    "entities_created": len(result.entities_created or []),
                    "entities_updated": len(result.entities_updated or []),
                },
            )

            # Prepare result metadata
            result_metadata = {
                "reason": self.reason,
                "no_output_produced": True,
                "processing_completed": True,
                "message_id": message.message_id,
                "correlation_id": message.correlation_id,
                "entities_affected": len(result.entities_created or [])
                + len(result.entities_updated or []),
                **self.metadata,
            }

            return result_metadata

        # Execute with timing (for consistency with other handlers)
        operation_metadata, duration_ms = self._execute_with_timing(_execute_noop)

        return self._create_success_result(
            execution_duration_ms=duration_ms, metadata=operation_metadata
        )

    def get_handler_info(self) -> Dict[str, Any]:
        """Get no-op handler information."""
        base_info = super().get_handler_info()
        base_info.update(
            {
                "handler_type": "no_operation",
                "reason": self.reason,
                "produces_output": False,
                "side_effects": False,
                "always_succeeds": True,
                "metadata_keys": list(self.metadata.keys()),
            }
        )
        return base_info
