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
from typing import Any, Dict, List, Optional

from ...context.operation_context import operation
from ...context.tenant_context import tenant_context
from ...exceptions import ErrorCode, ValidationError
from ...utils.logger import get_logger
from ..processing_result import ProcessingResult, ProcessingStatus
from .message import Message
from .processor_interface import ProcessorContext, ProcessorInterface


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
        dead_letter_queue_client=None,
        output_queue_client=None,
        db_manager=None,
    ):
        self.processor = processor
        self.processing_service = processing_service
        self.config = config or {}
        self.dead_letter_queue_client = dead_letter_queue_client
        self.output_queue_client = output_queue_client
        self.db_manager = db_manager  # Store db_manager for creating new sessions
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
        try:
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

        except Exception as e:
            import traceback

            self.logger.error(f"CRITICAL ERROR in ProcessorHandler.execute: {str(e)}")
            self.logger.error(f"Exception type: {type(e)}")
            self.logger.error(f"Message type: {type(message)}")
            self.logger.error(f"Message content: {message}")
            self.logger.error(f"Full traceback: {traceback.format_exc()}")

            # Re-raise so the Azure Function can handle it
            raise

    def _execute_with_tenant_context(self, message: Message) -> ProcessingResult:
        """Execute processor within tenant context."""
        start_time = time.time()

        # Create services - much simpler now with logging-based state/error tracking
        if self.db_manager:
            # Create ProcessingService with db_manager
            from ...processing import ProcessingService

            processing_service = ProcessingService(db_manager=self.db_manager)

            # Create logging-based services (no sessions needed!)
            from ...services.logging_processing_error_service import LoggingProcessingErrorService
            from ...services.logging_state_tracking_service import LoggingStateTrackingService

            state_tracking_service = LoggingStateTrackingService(db_manager=self.db_manager)
            error_service = LoggingProcessingErrorService()
            entity_service = None  # ProcessingService handles entity operations

            self.logger.debug(
                "Created services with logging-based state tracking and error handling",
                extra={
                    "processor_class": self.processor.__class__.__name__,
                    "state_tracking": "logging",
                    "error_handling": "logging",
                },
            )
        else:
            # Fallback to the shared services (old behavior)
            self.logger.warning(
                "No db_manager provided - using shared ProcessingService. "
                "Consider providing db_manager to ProcessorHandler."
            )
            processing_service = self.processing_service

            # Use logging services even in fallback mode
            from ...services.logging_processing_error_service import LoggingProcessingErrorService
            from ...services.logging_state_tracking_service import LoggingStateTrackingService

            state_tracking_service = LoggingStateTrackingService(db_manager=self.db_manager)
            error_service = LoggingProcessingErrorService()
            entity_service = None  # noqa: F841 - Placeholder for future fallback mode

        try:
            # Check if entity exists in message
            if message.entity_reference is None or message.entity_reference.id is None:
                self.logger.info(
                    "Entity not persisted yet, will be handled by processor",
                    extra={
                        "processor_class": self.processor.__class__.__name__,
                        "external_id": (
                            getattr(message.entity_reference, "external_id", "unknown")
                            if message.entity_reference
                            else "unknown"
                        ),
                        "canonical_type": (
                            getattr(message.entity_reference, "canonical_type", "unknown")
                            if message.entity_reference
                            else "unknown"
                        ),
                    },
                )

            # Log execution start
            self.logger.info(
                f"Starting processor v2 execution: {self.processor.__class__.__name__}",
                extra={
                    "processor_class": self.processor.__class__.__name__,
                    "message_id": message.message_id,
                    "external_id": (
                        getattr(message.entity_reference, "external_id", "unknown")
                        if message.entity_reference
                        else "unknown"
                    ),
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

            # Create enhanced context with services and output capabilities
            # Pass the new services to the context
            context = self._create_enhanced_context(
                processing_service=processing_service,
                state_tracking_service=state_tracking_service,
                error_service=error_service,
            )

            # Note: We don't create stub entities here anymore since processors
            # may handle their own entity creation. The entity_id will be obtained
            # from the message.entity_reference if it exists, or from ProcessingResult if the
            # processor uses the new approach.
            entity_id = message.entity_reference.id if message.entity_reference else None

            # Execute processor - let it control everything
            result = self.processor.process(message, context)

            # Commit ProcessingService transaction if processor succeeded
            # ProcessingService uses shared session pattern and doesn't commit its own transactions
            if result.success:
                processing_service.session.commit()
            else:
                processing_service.session.rollback()

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

                # Record state transitions for existing entities and newly created entities
                if state_tracking_service:
                    self._record_success_state_transitions(
                        entity_id, message, result, state_tracking_service
                    )

                # Legacy: Processing results now tracked via logging instead of database

                # Process output handlers if any are configured
                if result.output_handlers:
                    self._process_output_handlers(message, result)
            else:
                # Handle failure - route to DLQ if not retryable
                if not result.can_retry and self.dead_letter_queue_client:
                    self._send_to_dead_letter_queue(message, result)
                    result.status = ProcessingStatus.DEAD_LETTERED

                # Record state transitions for existing entities and newly created entities 
                if state_tracking_service:
                    self._record_failure_state_transitions(
                        entity_id, message, result, state_tracking_service
                    )

                # Record processing error if entity exists and error service available
                if entity_id and error_service:
                    self._record_processing_error(entity_id, result, error_service)

                # Legacy: Processing results now tracked via logging instead of database

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
            entity_id = (
                getattr(message.entity_reference, "id", None) if message.entity_reference else None
            )
            if entity_id:
                # Record failed state transition for unexpected errors
                if state_tracking_service:
                    external_id = (
                        message.entity_reference.external_id if message.entity_reference else None
                    )
                    self._record_state_transition(
                        entity_id,
                        "processing",
                        "error",
                        result,
                        state_tracking_service,
                        external_id,
                    )

                # Record processing error for unexpected errors
                if error_service:
                    self._record_processing_error(entity_id, result, error_service)

                # Create a minimal context for _record_processing_result
                # Legacy: Processing results now tracked via logging instead of database

            # Send to DLQ if not retryable
            if not can_retry and self.dead_letter_queue_client:
                self._send_to_dead_letter_queue(message, result)
                result.status = ProcessingStatus.DEAD_LETTERED

            return result

        finally:
            # Clean up the ProcessingService session if we created a new one
            if self.db_manager and processing_service != self.processing_service:
                try:
                    processing_service.session.close()
                    self.logger.debug(
                        "Closed ProcessingService session",
                        extra={
                            "processor_class": self.processor.__class__.__name__,
                            "session_id": id(processing_service.session),
                        },
                    )
                except Exception as e:
                    self.logger.warning(
                        "Failed to close ProcessingService session",
                        extra={
                            "error": str(e),
                            "processor_class": self.processor.__class__.__name__,
                        },
                    )

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
                    "external_id": (
                        getattr(message.entity_reference, "external_id", "unknown")
                        if message.entity_reference
                        else "unknown"
                    ),
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
                from .output_handlers.base import (
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

    # Legacy method removed - processing results now tracked via logging instead of database

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
        if (
            message.entity_reference
            and hasattr(message.entity_reference, "id")
            and message.entity_reference.id
        ):
            return message.entity_reference.id

        # For source processors, create stub entity
        if hasattr(message.entity_reference, "external_id") and hasattr(
            message.entity_reference, "canonical_type"
        ):
            try:
                entity_id = context.create_entity(
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
                        "canonical_type": getattr(
                            message.entity_reference, "canonical_type", "unknown"
                        ),
                        "processor_class": self.processor.__class__.__name__,
                        "error": str(e),
                    },
                    exc_info=True,
                )
                return None

        return None

    def _create_enhanced_context(
        self, processing_service=None, state_tracking_service=None, error_service=None
    ) -> ProcessorContext:
        """
        Create an enhanced ProcessorContext that includes send_output functionality.

        Args:
            processing_service: Optional ProcessingService to use. If None, uses self.processing_service
            state_tracking_service: StateTrackingService to use (required)
            error_service: ProcessingErrorService to use (required)

        Returns:
            ProcessorContext with send_output capability
        """
        # Use provided services (no fallback to shared services - they're removed)
        processing_service = processing_service or self.processing_service
        # state_tracking_service and error_service must be provided explicitly
        # (no more shared services that cause session conflicts)

        # Create a custom ProcessorContext class that has access to this handler
        handler = self

        class EnhancedProcessorContext(ProcessorContext):
            """ProcessorContext with send_output capability."""

            def send_output(
                self,
                message: Message,
                handler_type: str,
                destinations: Optional[List[str]] = None,
                **handler_params,
            ) -> Dict[str, Dict[str, Any]]:
                """
                Send a message to one or more outputs using the specified handler type.

                Implements the actual send_output functionality by accessing the
                ProcessorHandler's output handlers.
                """
                from .output_handlers.queue_output import QueueOutputHandler
                from .output_handlers.service_bus_output import ServiceBusOutputHandler

                results = {}

                # Handle queue output
                if handler_type == "queue":
                    if not destinations:
                        raise ValidationError(
                            "destinations required for queue handler",
                            error_code=ErrorCode.MISSING_REQUIRED,
                            field="destinations",
                        )

                    # Send to each destination and track results
                    for destination in destinations:
                        try:
                            # Create handler for this destination
                            queue_handler = QueueOutputHandler(
                                destination=destination, **handler_params
                            )

                            # Execute handler
                            handler_result = queue_handler.handle(message)

                            results[destination] = {
                                "success": handler_result.success,
                                "status": handler_result.status,
                                "message_id": handler_result.output_message_id,
                            }

                            if not handler_result.success:
                                results[destination].update(
                                    {
                                        "error": handler_result.error_message,
                                        "error_code": handler_result.error_code,
                                    }
                                )

                        except Exception as e:
                            results[destination] = {
                                "success": False,
                                "status": "failed",
                                "error": str(e),
                                "error_code": "HANDLER_EXCEPTION",
                            }

                # Handle service bus output
                elif handler_type == "service_bus":
                    topic = handler_params.get("topic")
                    if not topic:
                        raise ValidationError(
                            "topic required for service_bus handler",
                            error_code=ErrorCode.MISSING_REQUIRED,
                            field="topic",
                        )

                    try:
                        sb_handler = ServiceBusOutputHandler(topic=topic, **handler_params)

                        handler_result = sb_handler.handle(message)

                        results[topic] = {
                            "success": handler_result.success,
                            "status": handler_result.status,
                            "message_id": handler_result.output_message_id,
                        }

                        if not handler_result.success:
                            results[topic].update(
                                {
                                    "error": handler_result.error_message,
                                    "error_code": handler_result.error_code,
                                }
                            )

                    except Exception as e:
                        results[topic] = {
                            "success": False,
                            "status": "failed",
                            "error": str(e),
                            "error_code": "HANDLER_EXCEPTION",
                        }

                else:
                    raise ValidationError(
                        f"Unsupported handler type: {handler_type}",
                        error_code=ErrorCode.INVALID_FORMAT,
                        field="handler_type",
                        value=handler_type,
                    )

                # Log output results
                handler.logger.info(
                    "send_output completed",
                    extra={
                        "handler_type": handler_type,
                        "destinations": (
                            destinations
                            if handler_type == "queue"
                            else [handler_params.get("topic")]
                        ),
                        "success_count": sum(1 for r in results.values() if r["success"]),
                        "failure_count": sum(1 for r in results.values() if not r["success"]),
                    },
                )

                return results

        # Create and return enhanced context
        return EnhancedProcessorContext(
            processing_service=processing_service,
            state_tracking_service=state_tracking_service,
            error_service=error_service,
        )

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

    def _record_state_transition(
        self,
        entity_id: str,
        from_state: str,
        to_state: str,
        processing_result: "ProcessingResult",
        state_tracking_service=None,
        external_id: str = None,
    ) -> None:
        """
        Record state transition for entity processing.

        Args:
            entity_id: ID of the entity
            from_state: Previous state
            to_state: New state
            processing_result: ProcessingResult with context
            state_tracking_service: StateTrackingService to use. If None, uses self.state_tracking_service
        """
        # Use provided service (required - no fallback to prevent session conflicts)
        service = state_tracking_service
        if not service:
            self.logger.warning(
                "No state tracking service provided - state transition not recorded"
            )
            return

        try:
            processor_data = {
                "processor_name": self.processor.__class__.__name__,
                "processing_duration_ms": processing_result.processing_duration_ms,
                "success": processing_result.success,
            }

            if not processing_result.success:
                processor_data.update(
                    {
                        "error_code": processing_result.error_code,
                        "error_message": processing_result.error_message,
                        "can_retry": processing_result.can_retry,
                    }
                )

            from ...schemas import PipelineStateTransitionCreate

            transition_data = PipelineStateTransitionCreate(
                entity_id=entity_id,
                from_state=from_state,
                to_state=to_state,
                actor=self.processor.__class__.__name__,
                processor_data=processor_data,
                external_id=external_id,
            )
            service.record_transition(transition_data)

            self.logger.debug(
                f"Recorded state transition for entity {entity_id}: {from_state} -> {to_state}",
                extra={
                    "entity_id": entity_id,
                    "from_state": from_state,
                    "to_state": to_state,
                    "processor_class": self.processor.__class__.__name__,
                },
            )

        except Exception as e:
            # Don't fail the main processing if state tracking fails
            self.logger.error(
                f"Failed to record state transition for entity {entity_id}: {str(e)}",
                extra={
                    "entity_id": entity_id,
                    "from_state": from_state,
                    "to_state": to_state,
                    "processor_class": self.processor.__class__.__name__,
                    "error": str(e),
                },
                exc_info=True,
            )

    def _record_processing_error(
        self, entity_id: str, processing_result: "ProcessingResult", error_service=None
    ) -> None:
        """
        Record processing error for failed entity processing.

        Args:
            entity_id: ID of the entity
            processing_result: ProcessingResult with error information
            error_service: ProcessingErrorService to use (required)
        """
        # Use provided service (required - no fallback to prevent session conflicts)
        service = error_service
        if not service:
            self.logger.warning("No error service provided - processing error not recorded")
            return

        try:
            from ...schemas.processing_error_schema import ProcessingErrorCreate

            error_data = ProcessingErrorCreate(
                entity_id=entity_id,
                error_type=processing_result.error_code or "UNKNOWN_ERROR",
                message=processing_result.error_message or "Unknown error occurred",
                processing_step=self.processor.__class__.__name__,
                stack_trace=None,  # Could add stack trace from exception if available
            )

            error_id = service.create_error(error_data)

            self.logger.debug(
                f"Recorded processing error for entity {entity_id}",
                extra={
                    "entity_id": entity_id,
                    "error_id": error_id,
                    "error_code": processing_result.error_code,
                    "processor_class": self.processor.__class__.__name__,
                },
            )

        except Exception as e:
            # Don't fail the main processing if error recording fails
            self.logger.error(
                f"Failed to record processing error for entity {entity_id}: {str(e)}",
                extra={
                    "entity_id": entity_id,
                    "processor_class": self.processor.__class__.__name__,
                    "error": str(e),
                },
                exc_info=True,
            )

    def _record_success_state_transitions(
        self,
        entity_id: Optional[str],
        message: Message,
        result: ProcessingResult,
        state_tracking_service,
    ) -> None:
        """
        Record state transitions for successful processing.
        
        Handles both existing entities (from message) and newly created entities (from result).
        
        Args:
            entity_id: ID of existing entity from message (if any)
            message: Original message
            result: Processing result
            state_tracking_service: State tracking service
        """
        entities_to_track = []
        
        # Track existing entity if present
        if entity_id:
            external_id = (
                message.entity_reference.external_id if message.entity_reference else None
            )
            entities_to_track.append((entity_id, external_id, "processing"))
        
        # Track newly created entities (source operations)
        if result.entities_created:
            for created_entity_id in result.entities_created:
                # Skip if this is the same as the existing entity
                if created_entity_id != entity_id:
                    # For newly created entities, we don't have external_id easily available
                    # The processor should set it in entity metadata if needed
                    entities_to_track.append((created_entity_id, None, "started"))
        
        # Record transitions for all entities
        for track_entity_id, track_external_id, from_state in entities_to_track:
            self._record_state_transition(
                track_entity_id,
                from_state,
                "completed",
                result,
                state_tracking_service,
                track_external_id,
            )

    def _record_failure_state_transitions(
        self,
        entity_id: Optional[str],
        message: Message,
        result: ProcessingResult,
        state_tracking_service,
    ) -> None:
        """
        Record state transitions for failed processing.
        
        Handles both existing entities (from message) and newly created entities (from result).
        
        Args:
            entity_id: ID of existing entity from message (if any)
            message: Original message
            result: Processing result
            state_tracking_service: State tracking service
        """
        entities_to_track = []
        
        # Track existing entity if present
        if entity_id:
            external_id = (
                message.entity_reference.external_id if message.entity_reference else None
            )
            entities_to_track.append((entity_id, external_id, "processing"))
        
        # Track newly created entities (source operations can still fail after creating entities)
        if result.entities_created:
            for created_entity_id in result.entities_created:
                # Skip if this is the same as the existing entity
                if created_entity_id != entity_id:
                    entities_to_track.append((created_entity_id, None, "started"))
        
        # Record transitions for all entities
        for track_entity_id, track_external_id, from_state in entities_to_track:
            self._record_state_transition(
                track_entity_id,
                from_state,
                "failed",
                result,
                state_tracking_service,
                track_external_id,
            )
