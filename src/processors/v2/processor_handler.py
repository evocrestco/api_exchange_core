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
from src.processors.v2.message import Message
from src.processors.processing_result import ProcessingResult, ProcessingStatus
from src.processors.v2.processor_interface import ProcessorInterface, ProcessorContext
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
        output_queue_client=None
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
                duration_ms=0
            )
        
        # Execute within tenant context
        with tenant_context(tenant_id):
            return self._execute_with_tenant_context(message)
    
    def _execute_with_tenant_context(self, message: Message) -> ProcessingResult:
        """Execute processor within tenant context."""
        start_time = time.time()
        
        try:
            # Check if entity exists in message
            if message.entity.id is None:
                self.logger.info(
                    f"Entity not persisted yet, will be handled by processor",
                    extra={
                        "processor_class": self.processor.__class__.__name__,
                        "external_id": message.entity.external_id,
                        "canonical_type": message.entity.canonical_type
                    }
                )
            
            # Log execution start
            self.logger.info(
                f"Starting processor v2 execution: {self.processor.__class__.__name__}",
                extra={
                    "processor_class": self.processor.__class__.__name__,
                    "message_id": message.message_id,
                    "external_id": message.entity.external_id
                }
            )
            
            # Validate message
            if not self.processor.validate_message(message):
                return self._create_failure_result(
                    error_message="Message validation failed",
                    error_code="INVALID_MESSAGE",
                    can_retry=False,
                    duration_ms=(time.time() - start_time) * 1000
                )
            
            # Create context with services
            context = ProcessorContext(
                processing_service=self.processing_service,
                state_tracking_service=self.state_tracking_service,
                error_service=self.error_service
            )
            
            # Execute processor - let it control everything
            result = self.processor.process(message, context)
            
            # Add timing and metadata
            duration_ms = (time.time() - start_time) * 1000
            result.processing_duration_ms = duration_ms
            result.processor_info = self.processor.get_processor_info()
            
            # Handle result
            if result.success:
                self.logger.info(
                    f"Processor v2 execution completed successfully",
                    extra={
                        "processor_class": self.processor.__class__.__name__,
                        "message_id": message.message_id,
                        "duration_ms": duration_ms,
                        "output_messages": len(result.output_messages),
                        "output_handlers": len(result.output_handlers)
                    }
                )
                
                # Process output handlers if any are configured
                if result.output_handlers:
                    self._process_output_handlers(message, result)
                
                message.mark_processed()
            else:
                # Handle failure - route to DLQ if not retryable
                if not result.can_retry and self.dead_letter_queue_client:
                    self._send_to_dead_letter_queue(message, result)
                    result.status = ProcessingStatus.DEAD_LETTERED
                
                self.logger.error(
                    f"Processor v2 execution failed",
                    extra={
                        "processor_class": self.processor.__class__.__name__,
                        "message_id": message.message_id,
                        "error_code": result.error_code,
                        "error_message": result.error_message,
                        "can_retry": result.can_retry
                    }
                )
            
            return result
            
        except Exception as e:
            # Handle unexpected errors
            duration_ms = (time.time() - start_time) * 1000
            can_retry = self.processor.can_retry(e)
            
            self.logger.error(
                f"Unexpected error in processor v2",
                extra={
                    "processor_class": self.processor.__class__.__name__,
                    "message_id": message.message_id,
                    "error": str(e),
                    "error_type": type(e).__name__,
                    "can_retry": can_retry
                },
                exc_info=True
            )
            
            result = self._create_failure_result(
                error_message=f"Unexpected error: {str(e)}",
                error_code="UNEXPECTED_ERROR",
                can_retry=can_retry,
                duration_ms=duration_ms
            )
            
            # Send to DLQ if not retryable
            if not can_retry and self.dead_letter_queue_client:
                self._send_to_dead_letter_queue(message, result)
                result.status = ProcessingStatus.DEAD_LETTERED
            
            return result
    
    def _create_failure_result(
        self,
        error_message: str,
        error_code: str,
        can_retry: bool,
        duration_ms: float
    ) -> ProcessingResult:
        """Create a failure result."""
        return ProcessingResult.create_failure(
            error_message=error_message,
            error_code=error_code,
            can_retry=can_retry,
            processing_duration_ms=duration_ms
        )
    
    def _send_to_dead_letter_queue(self, message: Message, result: ProcessingResult) -> bool:
        """Send message to dead letter queue."""
        try:
            import json
            from datetime import datetime
            
            dead_letter_message = {
                "original_message": {
                    "message_id": message.message_id,
                    "external_id": message.entity.external_id,
                    "payload": message.payload
                },
                "failure_info": {
                    "error_code": result.error_code,
                    "error_message": result.error_message,
                    "processor": self.processor.__class__.__name__,
                    "failed_at": datetime.utcnow().isoformat()
                }
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
                "output_handler_count": len(result.output_handlers)
            }
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
                        "handler_index": i
                    }
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
                            "handler_status": handler_result.status.value
                        }
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
                            "execution_duration_ms": handler_result.execution_duration_ms
                        }
                    )
                    
                    # If handler supports retry and suggests retry, log that information
                    if handler_result.can_retry and handler_result.retry_after_seconds:
                        self.logger.info(
                            f"Handler suggests retry after {handler_result.retry_after_seconds} seconds",
                            extra={
                                "handler_name": handler_name,
                                "retry_after_seconds": handler_result.retry_after_seconds,
                                "message_id": message.message_id
                            }
                        )
                
            except Exception as e:
                failed_handlers += 1
                handler_name = getattr(handler, '__class__', {}).get('__name__', 'unknown')
                
                self.logger.error(
                    f"Unexpected error executing output handler: {handler_name}",
                    extra={
                        "handler_name": handler_name,
                        "message_id": message.message_id,
                        "error": str(e),
                        "error_type": type(e).__name__,
                        "handler_index": i
                    },
                    exc_info=True
                )
                
                # Create a failure result for this handler
                from src.processors.v2.output_handlers.base import OutputHandlerResult, OutputHandlerStatus
                error_result = OutputHandlerResult(
                    status=OutputHandlerStatus.FAILED,
                    success=False,
                    handler_name=handler_name,
                    destination="unknown",
                    execution_duration_ms=0.0,
                    error_message=f"Unexpected error: {str(e)}",
                    error_code="HANDLER_EXECUTION_ERROR",
                    can_retry=False
                )
                handler_results.append(error_result)
        
        # Log overall output handler execution summary
        self.logger.info(
            f"Output handler processing completed",
            extra={
                "processor_class": self.processor.__class__.__name__,
                "message_id": message.message_id,
                "total_handlers": len(result.output_handlers),
                "successful_handlers": successful_handlers,
                "failed_handlers": failed_handlers,
                "success_rate": (successful_handlers / len(result.output_handlers)) * 100
            }
        )
        
        # Store handler results in processing result metadata for potential debugging/monitoring
        if not hasattr(result, 'processing_metadata'):
            result.processing_metadata = {}
        
        result.processing_metadata["output_handler_results"] = [
            {
                "handler_name": hr.handler_name,
                "destination": hr.destination,
                "success": hr.success,
                "status": hr.status.value,
                "execution_duration_ms": hr.execution_duration_ms,
                "error_code": hr.error_code,
                "error_message": hr.error_message
            }
            for hr in handler_results
        ]
    
