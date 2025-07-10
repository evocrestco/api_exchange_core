"""
Simple processor handler for the V2 framework.

This module provides a lightweight handler that manages pipeline execution
tracking and output routing for processors.
"""

import time
from typing import Any, Dict, Optional

from ..utils.logger import get_logger
from .message import Message
from .processing_result import ProcessingResult
from .simple_processor_interface import SimpleProcessorInterface


class SimpleProcessorHandler:
    """
    Handler for processor execution with pipeline tracking.

    This handler provides:
    - Pipeline execution tracking via pipeline_id
    - Output message routing
    - Error handling and logging
    - Processing metrics

    Unlike the old ProcessorHandler, this doesn't depend on entity persistence.
    """

    def __init__(
        self,
        processor: SimpleProcessorInterface,
        enable_pipeline_tracking: bool = True,
        enable_metrics: bool = True,
    ):
        """
        Initialize the processor handler.

        Args:
            processor: The processor to handle
            enable_pipeline_tracking: Whether to track pipeline execution
            enable_metrics: Whether to collect processing metrics
        """
        self.processor = processor
        self.enable_pipeline_tracking = enable_pipeline_tracking
        self.enable_metrics = enable_metrics
        self.logger = get_logger()

    def process_message(
        self, message: Message, context: Optional[Dict[str, Any]] = None
    ) -> ProcessingResult:
        """
        Process a message through the processor with full tracking.

        Args:
            message: Message to process
            context: Additional processing context

        Returns:
            ProcessingResult with success/failure status and output messages
        """
        context = context or {}
        processor_name = self.processor.get_processor_name()
        start_time = time.time()

        # Set up logging context
        log_context = {
            "processor_name": processor_name,
            "pipeline_id": message.pipeline_id,
            "message_id": message.message_id,
            "correlation_id": message.correlation_id,
            "tenant_id": message.tenant_id,
        }

        self.logger.info(f"Starting processing: {processor_name}", extra=log_context)

        try:
            # Validate message
            if not self.processor.validate_message(message):
                return ProcessingResult.failure_result(
                    error_message="Message validation failed", error_code="INVALID_MESSAGE"
                )

            # Track pipeline execution start (if enabled)
            if self.enable_pipeline_tracking:
                self._track_pipeline_start(message, processor_name, context)

            # Process the message
            result = self.processor.process(message, context)

            # Calculate processing duration
            processing_duration_ms = int((time.time() - start_time) * 1000)
            if result.processing_duration_ms is None:
                result.processing_duration_ms = processing_duration_ms

            # Track pipeline execution completion (if enabled)
            if self.enable_pipeline_tracking:
                self._track_pipeline_completion(message, processor_name, result, context)

            # Log result
            result_context = {
                **log_context,
                "success": result.success,
                "status": result.status,
                "processing_duration_ms": processing_duration_ms,
                "records_processed": result.records_processed,
                "output_messages_count": len(result.output_messages),
            }

            if result.success:
                self.logger.info(
                    f"Processing completed successfully: {processor_name}", extra=result_context
                )
            else:
                result_context.update(
                    {
                        "error_message": result.error_message,
                        "error_code": result.error_code,
                    }
                )
                self.logger.error(f"Processing failed: {processor_name}", extra=result_context)

            return result

        except Exception as e:
            processing_duration_ms = int((time.time() - start_time) * 1000)

            # Track pipeline execution failure (if enabled)
            if self.enable_pipeline_tracking:
                self._track_pipeline_failure(message, processor_name, str(e), context)

            # Log error
            error_context = {
                **log_context,
                "processing_duration_ms": processing_duration_ms,
                "error_type": type(e).__name__,
                "error_message": str(e),
            }

            self.logger.error(
                f"Processing failed with exception: {processor_name}",
                extra=error_context,
                exc_info=True,
            )

            return ProcessingResult.failure_result(
                error_message=f"Processing failed: {str(e)}",
                error_code="PROCESSING_EXCEPTION",
                processing_duration_ms=processing_duration_ms,
            )

    def _track_pipeline_start(
        self, message: Message, processor_name: str, context: Dict[str, Any]
    ) -> None:
        """
        Track the start of pipeline execution.

        Args:
            message: Message being processed
            processor_name: Name of the processor
            context: Processing context
        """
        # TODO: Implement pipeline tracking
        # This could log to a pipeline tracking service, database, or queue
        # For now, we'll just log it
        self.logger.debug(
            f"Pipeline step started: {processor_name}",
            extra={
                "pipeline_id": message.pipeline_id,
                "processor_name": processor_name,
                "message_id": message.message_id,
                "tenant_id": message.tenant_id,
            },
        )

    def _track_pipeline_completion(
        self,
        message: Message,
        processor_name: str,
        result: ProcessingResult,
        context: Dict[str, Any],
    ) -> None:
        """
        Track the completion of pipeline execution.

        Args:
            message: Message that was processed
            processor_name: Name of the processor
            result: Processing result
            context: Processing context
        """
        # TODO: Implement pipeline tracking
        # This could log to a pipeline tracking service, database, or queue
        # For now, we'll just log it
        self.logger.debug(
            f"Pipeline step completed: {processor_name}",
            extra={
                "pipeline_id": message.pipeline_id,
                "processor_name": processor_name,
                "message_id": message.message_id,
                "tenant_id": message.tenant_id,
                "success": result.success,
                "status": result.status,
                "processing_duration_ms": result.processing_duration_ms,
                "records_processed": result.records_processed,
            },
        )

    def _track_pipeline_failure(
        self, message: Message, processor_name: str, error_message: str, context: Dict[str, Any]
    ) -> None:
        """
        Track the failure of pipeline execution.

        Args:
            message: Message that was processed
            processor_name: Name of the processor
            error_message: Error message
            context: Processing context
        """
        # TODO: Implement pipeline tracking
        # This could log to a pipeline tracking service, database, or queue
        # For now, we'll just log it
        self.logger.debug(
            f"Pipeline step failed: {processor_name}",
            extra={
                "pipeline_id": message.pipeline_id,
                "processor_name": processor_name,
                "message_id": message.message_id,
                "tenant_id": message.tenant_id,
                "error_message": error_message,
            },
        )
