"""
Processor Handler v2 - Simplified infrastructure wrapper.

Key changes from v1:
- No automatic to_canonical() calling
- Just handles infrastructure concerns (error handling, state tracking, DLQ)
- Processor controls business logic flow
- No tenant decorators - that's handled at repository layer
"""

import os
import time
from typing import Any, Dict, Optional

from src.context.operation_context import operation
from src.context.tenant_context import tenant_context
from src.processors.processing_result import ProcessingResult, ProcessingStatus
from src.processors.v2.message import Message
from src.processors.v2.processor_interface import ProcessorContext, ProcessorInterface
from src.utils.logger import get_logger


class ProcessorHandler:
    """
    Infrastructure wrapper for processors.

    Handles:
    - Error handling and retry logic
    - Dead letter queue routing
    - High-level state tracking
    - Metrics and logging

    Does NOT handle:
    - Business logic
    - Data transformation
    - When to persist entities
    - Tenant scoping (that's at repository layer)
    """

    def __init__(
        self,
        processor: ProcessorInterface,
        processing_service,
        config: Optional[Dict[str, Any]] = None,
        state_tracking_service=None,
        error_service=None,
        dead_letter_queue_client=None,
        output_queue_client=None,
    ):
        self.processor = processor
        self.processing_service = processing_service
        self.config = config or {}
        self.state_tracking_service = state_tracking_service
        self.error_service = error_service
        self.dead_letter_queue_client = dead_letter_queue_client
        self.output_queue_client = output_queue_client
        self.logger = get_logger()

    @operation(name="processor_v2_execute")
    def execute(self, message: Message) -> ProcessingResult:
        """
        Execute processor with infrastructure support.

        This method:
        1. Sets up tenant context from environment
        2. Creates context for processor
        3. Calls processor.process()
        4. Handles errors and DLQ routing
        5. Records metrics
        """
        # Get tenant ID from environment
        tenant_id = os.getenv("TENANT_ID")
        if not tenant_id:
            return self._create_failure_result(
                error_message="TENANT_ID environment variable is required",
                error_code="MISSING_TENANT_ID",
                can_retry=False,
                duration_ms=0,
            )

        # Execute within tenant context
        with tenant_context(tenant_id):
            return self._execute_with_tenant_context(message)

    def _execute_with_tenant_context(self, message: Message) -> ProcessingResult:
        """Execute processor within tenant context."""
        start_time = time.time()

        try:
            # Check if entity exists in message
            if message.entity_reference.id is None:
                self.logger.info(
                    "Entity not persisted yet, will be handled by processor",
                    extra={
                        "processor_class": self.processor.__class__.__name__,
                        "external_id": message.entity_reference.external_id,
                        "canonical_type": message.entity_reference.canonical_type,
                    },
                )

            # Log execution start
            self.logger.info(
                f"Starting processor v2 execution: {self.processor.__class__.__name__}",
                extra={
                    "processor_class": self.processor.__class__.__name__,
                    "message_id": message.message_id,
                    "external_id": message.entity_reference.external_id,
                },
            )

            # Validate message
            if not self.processor.validate_message(message):
                return self._create_failure_result(
                    error_message="Message validation failed",
                    error_code="INVALID_MESSAGE",
                    can_retry=False,
                    duration_ms=(time.time() - start_time) * 1000,
                )

            # Create context with services
            context = ProcessorContext(
                processing_service=self.processing_service,
                state_tracking_service=self.state_tracking_service,
                error_service=self.error_service,
            )

            # Note: We don't create stub entities here anymore since processors
            # may handle their own entity creation. The entity_id will be obtained
            # from the message.entity_reference if it exists, or from ProcessingResult if the
            # processor uses the new approach.
            entity_id = message.entity_reference.id if message.entity_reference else None

            # Execute processor - let it control everything
            result = self.processor.process(message, context)

            # Add timing and metadata
            duration_ms = (time.time() - start_time) * 1000
            result.processing_duration_ms = duration_ms
            result.processor_info = self.processor.get_processor_info()

            # Get entity_id from result if processor created entities
            if result.entities_created and not entity_id:
                entity_id = result.entities_created[0]  # Use first created entity

            # Update entity with processor results if needed
            if result.has_entity_data() and entity_id:
                self._update_entity_with_result_data(entity_id, result, context)

            # Handle result
            if result.success:
                self.logger.info(
                    "Processor v2 execution completed successfully",
                    extra={
                        "processor_class": self.processor.__class__.__name__,
                        "message_id": message.message_id,
                        "duration_ms": duration_ms,
                        "output_messages": len(result.output_messages),
                        "output_handlers": len(result.output_handlers),
                    },
                )

                # Record processing result to entity if entity exists
                if entity_id:
                    self._record_processing_result(entity_id, result)

                # Process output handlers if any are configured
                if result.output_handlers:
                    self._process_output_handlers(message, result)
            else:
                # Handle failure - route to DLQ if not retryable
                if not result.can_retry and self.dead_letter_queue_client:
                    self._send_to_dead_letter_queue(message, result)
                    result.status = ProcessingStatus.DEAD_LETTERED

                # Record processing result to entity even on failure if entity exists
                if entity_id:
                    self._record_processing_result(entity_id, result)

                self.logger.error(
                    "Processor v2 execution failed",
                    extra={
                        "processor_class": self.processor.__class__.__name__,
                        "message_id": message.message_id,
                        "error_code": result.error_code,
                        "error_message": result.error_message,
                        "can_retry": result.can_retry,
                    },
                )

            return result

        except Exception as e:
            # Handle unexpected errors
            duration_ms = (time.time() - start_time) * 1000
            can_retry = self.processor.can_retry(e)

            self.logger.error(
                "Unexpected error in processor v2",
                extra={
                    "processor_class": self.processor.__class__.__name__,
                    "message_id": message.message_id,
                    "error": str(e),
                    "error_type": type(e).__name__,
                    "can_retry": can_retry,
                },
                exc_info=True,
            )

            result = self._create_failure_result(
                error_message=f"Unexpected error: {str(e)}",
                error_code="UNEXPECTED_ERROR",
                can_retry=can_retry,
                duration_ms=duration_ms,
            )

            # Record processing result to entity even on unexpected error if entity exists
            entity_id = getattr(message.entity_reference, "id", None) if message.entity_reference else None
            if entity_id:
                self._record_processing_result(entity_id, result)

            # Send to DLQ if not retryable
            if not can_retry and self.dead_letter_queue_client:
                self._send_to_dead_letter_queue(message, result)
                result.status = ProcessingStatus.DEAD_LETTERED

            return result

    def _create_failure_result(
        self, error_message: str, error_code: str, can_retry: bool, duration_ms: float
    ) -> ProcessingResult:
        """Create a failure result."""
        return ProcessingResult.create_failure(
            error_message=error_message,
            error_code=error_code,
            can_retry=can_retry,
            processing_duration_ms=duration_ms,
        )

    def _send_to_dead_letter_queue(self, message: Message, result: ProcessingResult) -> bool:
        """Send message to dead letter queue."""
        try:
            import json
            from datetime import datetime

            dead_letter_message = {
                "original_message": {
                    "message_id": message.message_id,
                    "external_id": message.entity_reference.external_id,
                    "payload": message.payload,
                },
                "failure_info": {
                    "error_code": result.error_code,
                    "error_message": result.error_message,
                    "processor": self.processor.__class__.__name__,
                    "failed_at": datetime.utcnow().isoformat(),
                },
            }

            self.dead_letter_queue_client.send_message(json.dumps(dead_letter_message))

            self.logger.info(f"Message routed to dead letter queue: {message.message_id}")
            return True

        except Exception as e:
            self.logger.error(f"Failed to send to DLQ: {e}", exc_info=True)
            return False

    def _process_output_handlers(self, message: Message, result: "ProcessingResult") -> None:
        """
        Process all output handlers configured in the processing result.

        Executes each output handler sequentially and logs the results.
        If any handler fails, logs the error but continues processing other handlers.

        Args:
            message: Original message that was processed
            result: Processing result containing output handlers
        """
        if not result.output_handlers:
            return

        self.logger.info(
            f"Processing {len(result.output_handlers)} output handlers",
            extra={
                "processor_class": self.processor.__class__.__name__,
                "message_id": message.message_id,
                "output_handler_count": len(result.output_handlers),
            },
        )

        successful_handlers = 0
        failed_handlers = 0
        handler_results = []

        for i, handler in enumerate(result.output_handlers):
            try:
                # Get handler info for logging
                handler_info = handler.get_handler_info()
                handler_name = handler_info.get("handler_name", handler.__class__.__name__)
                destination = handler_info.get("destination", "unknown")

                self.logger.debug(
                    f"Executing output handler {i+1}/{len(result.output_handlers)}: {handler_name}",
                    extra={
                        "handler_name": handler_name,
                        "destination": destination,
                        "message_id": message.message_id,
                        "handler_index": i,
                    },
                )

                # Execute the handler
                handler_result = handler.handle(message, result)
                handler_results.append(handler_result)

                if handler_result.success:
                    successful_handlers += 1
                    self.logger.info(
                        f"Output handler executed successfully: {handler_name} -> {destination}",
                        extra={
                            "handler_name": handler_name,
                            "destination": destination,
                            "message_id": message.message_id,
                            "execution_duration_ms": handler_result.execution_duration_ms,
                            "handler_status": handler_result.status.value,
                        },
                    )
                else:
                    failed_handlers += 1
                    self.logger.error(
                        f"Output handler failed: {handler_name} -> {destination}",
                        extra={
                            "handler_name": handler_name,
                            "destination": destination,
                            "message_id": message.message_id,
                            "error_message": handler_result.error_message,
                            "error_code": handler_result.error_code,
                            "can_retry": handler_result.can_retry,
                            "execution_duration_ms": handler_result.execution_duration_ms,
                        },
                    )

                    # If handler supports retry and suggests retry, log that information
                    if handler_result.can_retry and handler_result.retry_after_seconds:
                        self.logger.info(
                            f"Handler suggests retry after {handler_result.retry_after_seconds} seconds",
                            extra={
                                "handler_name": handler_name,
                                "retry_after_seconds": handler_result.retry_after_seconds,
                                "message_id": message.message_id,
                            },
                        )

            except Exception as e:
                failed_handlers += 1
                handler_name = getattr(handler, "__class__", {}).get("__name__", "unknown")

                self.logger.error(
                    f"Unexpected error executing output handler: {handler_name}",
                    extra={
                        "handler_name": handler_name,
                        "message_id": message.message_id,
                        "error": str(e),
                        "error_type": type(e).__name__,
                        "handler_index": i,
                    },
                    exc_info=True,
                )

                # Create a failure result for this handler
                from src.processors.v2.output_handlers.base import (
                    OutputHandlerResult,
                    OutputHandlerStatus,
                )

                error_result = OutputHandlerResult(
                    status=OutputHandlerStatus.FAILED,
                    success=False,
                    handler_name=handler_name,
                    destination="unknown",
                    execution_duration_ms=0.0,
                    error_message=f"Unexpected error: {str(e)}",
                    error_code="HANDLER_EXECUTION_ERROR",
                    can_retry=False,
                )
                handler_results.append(error_result)

        # Log overall output handler execution summary
        self.logger.info(
            "Output handler processing completed",
            extra={
                "processor_class": self.processor.__class__.__name__,
                "message_id": message.message_id,
                "total_handlers": len(result.output_handlers),
                "successful_handlers": successful_handlers,
                "failed_handlers": failed_handlers,
                "success_rate": (successful_handlers / len(result.output_handlers)) * 100,
            },
        )

        # Store handler results in processing result metadata for potential debugging/monitoring
        if not hasattr(result, "processing_metadata"):
            result.processing_metadata = {}

        result.processing_metadata["output_handler_results"] = [
            {
                "handler_name": hr.handler_name,
                "destination": hr.destination,
                "success": hr.success,
                "status": hr.status.value,
                "execution_duration_ms": hr.execution_duration_ms,
                "error_code": hr.error_code,
                "error_message": hr.error_message,
            }
            for hr in handler_results
        ]

    def _record_processing_result(
        self, entity_id: str, processing_result: ProcessingResult
    ) -> None:
        """
        Record processing result to entity's processing history.

        Args:
            entity_id: ID of the entity to record result for
            processing_result: ProcessingResult to record
        """
        try:
            # Get EntityService from processing_service
            if hasattr(self.processing_service, "entity_service"):
                entity_service = self.processing_service.entity_service
                entity_service.add_processing_result(entity_id, processing_result)

                self.logger.debug(
                    f"Recorded processing result for entity {entity_id}",
                    extra={
                        "entity_id": entity_id,
                        "processor_class": self.processor.__class__.__name__,
                        "success": processing_result.success,
                        "status": processing_result.status.value,
                        "processing_duration_ms": processing_result.processing_duration_ms,
                    },
                )
            else:
                self.logger.warning(
                    "Cannot record processing result - entity service not available",
                    extra={
                        "entity_id": entity_id,
                        "processor_class": self.processor.__class__.__name__,
                    },
                )

        except Exception as e:
            # Don't fail the main processing if recording result fails
            self.logger.error(
                f"Failed to record processing result for entity {entity_id}: {str(e)}",
                extra={
                    "entity_id": entity_id,
                    "processor_class": self.processor.__class__.__name__,
                    "error": str(e),
                },
                exc_info=True,
            )

    def _ensure_entity_exists(self, message: Message, context: ProcessorContext) -> Optional[str]:
        """
        Ensure entity exists for source processors.

        Creates a stub entity if one doesn't exist, returns entity_id.

        Args:
            message: Message with entity information
            context: Processor context

        Returns:
            Entity ID if entity exists or was created, None otherwise
        """
        if message.entity_reference and hasattr(message.entity_reference, "id") and message.entity_reference.id:
            return message.entity_reference.id

        # For source processors, create stub entity
        if hasattr(message.entity_reference, "external_id") and hasattr(message.entity_reference, "canonical_type"):
            try:
                entity_id = context.persist_entity(
                    external_id=message.entity_reference.external_id,
                    canonical_type=message.entity_reference.canonical_type,
                    source=getattr(message.entity_reference, "source", "unknown"),
                    data={"status": "processing"},
                    metadata={"created_by": "processor_handler", "stage": "stub"},
                )

                # Update message entity reference with the ID
                message.entity_reference.id = entity_id

                self.logger.debug(
                    "Created stub entity for source processor",
                    extra={
                        "entity_id": entity_id,
                        "external_id": message.entity_reference.external_id,
                        "canonical_type": message.entity_reference.canonical_type,
                        "processor_class": self.processor.__class__.__name__,
                    },
                )

                return entity_id

            except Exception as e:
                self.logger.error(
                    f"Failed to create stub entity: {str(e)}",
                    extra={
                        "external_id": getattr(message.entity_reference, "external_id", "unknown"),
                        "canonical_type": getattr(message.entity_reference, "canonical_type", "unknown"),
                        "processor_class": self.processor.__class__.__name__,
                        "error": str(e),
                    },
                    exc_info=True,
                )
                return None

        return None

    def _update_entity_with_result_data(
        self, entity_id: str, result: ProcessingResult, context: ProcessorContext
    ) -> None:
        """
        Update entity with data from ProcessingResult.

        Args:
            entity_id: ID of entity to update
            result: ProcessingResult with entity data
            context: Processor context
        """
        try:
            # Update entity attributes with the processor result data
            if hasattr(context.processing_service, "entity_service"):
                entity_service = context.processing_service.entity_service

                # Merge existing metadata with new metadata
                update_attributes = result.entity_data.copy()
                if result.entity_metadata:
                    update_attributes.update(result.entity_metadata)

                # Add processing metadata
                update_attributes.update(
                    {
                        "processed_by": self.processor.__class__.__name__,
                        "processed_at": result.completed_at.isoformat(),
                        "processing_duration_ms": result.processing_duration_ms,
                        "processing_status": "completed" if result.success else "failed",
                    }
                )

                entity_service.update_entity_attributes(entity_id, update_attributes)

                self.logger.debug(
                    "Updated entity with processor result data",
                    extra={
                        "entity_id": entity_id,
                        "processor_class": self.processor.__class__.__name__,
                        "data_keys": list(result.entity_data.keys()) if result.entity_data else [],
                        "success": result.success,
                    },
                )

        except Exception as e:
            # Don't fail the main processing if entity update fails
            self.logger.error(
                f"Failed to update entity with result data: {str(e)}",
                extra={
                    "entity_id": entity_id,
                    "processor_class": self.processor.__class__.__name__,
                    "error": str(e),
                },
                exc_info=True,
            )
