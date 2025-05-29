"""
Processor execution framework for managing processor lifecycle and error handling.

This module provides the execution framework that handles processor invocation,
error management, retry logic, and result processing.
"""

import time
from typing import Any, Dict, Optional

from src.context.operation_context import operation
from src.context.tenant_context import tenant_aware
from src.exceptions import ServiceError, ValidationError
from src.processors.message import Message
from src.processors.processing_result import ProcessingResult, ProcessingStatus
from src.processors.processor_interface import ProcessorInterface
from src.utils.logger import get_logger


class ProcessorExecutor:
    """
    Framework for executing processors with error handling and retry logic.

    Handles the common concerns around processor execution including:
    - Tenant context management
    - Error handling and classification
    - Retry logic and backoff
    - Performance monitoring
    - Result validation
    """

    def __init__(self):
        """Initialize the processor executor."""
        self.logger = get_logger()

    @tenant_aware
    @operation(name="processor_execute")
    def execute_processor(
        self,
        processor: ProcessorInterface,
        message: Message,
        execution_context: Optional[Dict[str, Any]] = None,
    ) -> ProcessingResult:
        """
        Execute a processor with comprehensive error handling and monitoring.

        Args:
            processor: The processor instance to execute
            message: The message to process
            execution_context: Optional context for execution (timeouts, etc.)

        Returns:
            ProcessingResult with execution outcome

        Raises:
            ServiceError: If execution fails in an unrecoverable way
        """
        start_time = time.time()
        execution_context = execution_context or {}

        try:
            # Log execution start
            self.logger.info(
                f"Starting processor execution: {processor.__class__.__name__}",
                extra={
                    "processor_class": processor.__class__.__name__,
                    "message_id": message.message_id,
                    "correlation_id": message.correlation_id,
                    "entity_external_id": message.entity_reference.external_id,
                    "entity_source": message.entity_reference.source,
                    "retry_count": message.retry_count,
                },
            )

            # Validate message
            if not processor.validate_message(message):
                return ProcessingResult.create_failure(
                    error_message="Message validation failed",
                    error_code="INVALID_MESSAGE",
                    can_retry=False,
                    processing_duration_ms=(time.time() - start_time) * 1000,
                )

            # Execute the processor
            result = processor.process(message)

            # Calculate processing duration
            duration_ms = (time.time() - start_time) * 1000
            result.processing_duration_ms = duration_ms

            # Add processor info to result
            result.processor_info = processor.get_processor_info()

            # Mark message as processed
            message.mark_processed()

            # Log execution completion
            self.logger.info(
                f"Processor execution completed: {processor.__class__.__name__}",
                extra={
                    "processor_class": processor.__class__.__name__,
                    "message_id": message.message_id,
                    "correlation_id": message.correlation_id,
                    "status": result.status.value,
                    "success": result.success,
                    "duration_ms": duration_ms,
                    "output_message_count": len(result.output_messages),
                    "entities_created": len(result.entities_created),
                    "entities_updated": len(result.entities_updated),
                },
            )

            return result

        except ValidationError as e:
            duration_ms = (time.time() - start_time) * 1000
            self.logger.warning(
                f"Validation error in processor: {processor.__class__.__name__}",
                extra={
                    "processor_class": processor.__class__.__name__,
                    "message_id": message.message_id,
                    "error": str(e),
                    "duration_ms": duration_ms,
                },
            )

            return ProcessingResult.create_failure(
                error_message=f"Validation error: {str(e)}",
                error_code="VALIDATION_ERROR",
                error_details={"validation_details": getattr(e, "details", {})},
                can_retry=False,
                processing_duration_ms=duration_ms,
            )

        except ServiceError as e:
            duration_ms = (time.time() - start_time) * 1000
            can_retry = processor.can_retry(e)

            self.logger.error(
                f"Service error in processor: {processor.__class__.__name__}",
                extra={
                    "processor_class": processor.__class__.__name__,
                    "message_id": message.message_id,
                    "error": str(e),
                    "error_code": getattr(e, "error_code", None),
                    "can_retry": can_retry,
                    "duration_ms": duration_ms,
                },
                exc_info=True,
            )

            return ProcessingResult.create_failure(
                error_message=f"Service error: {str(e)}",
                error_code="SERVICE_ERROR",
                error_details={"service_error_code": getattr(e, "error_code", None)},
                can_retry=can_retry,
                retry_after_seconds=self._calculate_retry_delay(message.retry_count),
                processing_duration_ms=duration_ms,
            )

        except Exception as e:
            duration_ms = (time.time() - start_time) * 1000
            can_retry = processor.can_retry(e)

            self.logger.error(
                f"Unexpected error in processor: {processor.__class__.__name__}",
                extra={
                    "processor_class": processor.__class__.__name__,
                    "message_id": message.message_id,
                    "error": str(e),
                    "error_type": type(e).__name__,
                    "can_retry": can_retry,
                    "duration_ms": duration_ms,
                },
                exc_info=True,
            )

            return ProcessingResult.create_failure(
                error_message=f"Unexpected error: {str(e)}",
                error_code="UNEXPECTED_ERROR",
                error_details={
                    "error_type": type(e).__name__,
                    "error_details": str(e),
                },
                can_retry=can_retry,
                retry_after_seconds=self._calculate_retry_delay(message.retry_count),
                processing_duration_ms=duration_ms,
            )

    def _calculate_retry_delay(self, retry_count: int) -> int:
        """
        Calculate retry delay using exponential backoff.

        Args:
            retry_count: Number of previous retry attempts

        Returns:
            Delay in seconds before next retry attempt
        """
        # Exponential backoff: 2^retry_count seconds, max 300 seconds (5 minutes)
        delay = min(2**retry_count, 300)
        return delay

    def can_process_message(
        self,
        processor: ProcessorInterface,
        message: Message,
    ) -> bool:
        """
        Check if a processor can handle a specific message.

        Args:
            processor: The processor to check
            message: The message to validate

        Returns:
            True if processor can handle the message, False otherwise
        """
        try:
            # Check if message can be retried
            if not message.can_retry():
                self.logger.warning(
                    "Message has exceeded retry limit",
                    extra={
                        "message_id": message.message_id,
                        "retry_count": message.retry_count,
                        "max_retries": message.max_retries,
                    },
                )
                return False

            # Check processor validation
            if not processor.validate_message(message):
                self.logger.debug(
                    f"Processor rejected message: {processor.__class__.__name__}",
                    extra={
                        "processor_class": processor.__class__.__name__,
                        "message_id": message.message_id,
                        "message_type": message.message_type.value,
                    },
                )
                return False

            return True

        except Exception as e:
            self.logger.error(
                "Error checking if processor can handle message",
                extra={
                    "processor_class": processor.__class__.__name__,
                    "message_id": message.message_id,
                    "error": str(e),
                },
                exc_info=True,
            )
            return False

    def validate_processing_result(self, result: ProcessingResult) -> bool:
        """
        Validate that a processing result is well-formed.

        Args:
            result: The processing result to validate

        Returns:
            True if result is valid, False otherwise
        """
        try:
            # Check basic result structure
            if not isinstance(result, ProcessingResult):
                self.logger.error("Result is not a ProcessingResult instance")
                return False

            # Check status consistency
            if result.success and result.status in [
                ProcessingStatus.FAILED,
                ProcessingStatus.ERROR,
            ]:
                self.logger.error(
                    "Inconsistent result: success=True but status indicates failure",
                    extra={"status": result.status.value},
                )
                return False

            if not result.success and result.status == ProcessingStatus.SUCCESS:
                self.logger.error("Inconsistent result: success=False but status is SUCCESS")
                return False

            # Check error information
            if not result.success and not result.error_message:
                self.logger.warning("Failed result missing error message")

            # Validate output messages
            for i, message in enumerate(result.output_messages):
                if not isinstance(message, Message):
                    self.logger.error(
                        f"Output message {i} is not a Message instance",
                        extra={"message_type": type(message).__name__},
                    )
                    return False

            return True

        except Exception as e:
            self.logger.error(
                f"Error validating processing result: {str(e)}",
                exc_info=True,
            )
            return False
