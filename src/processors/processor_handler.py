"""
Clean processor handler that unifies execution, persistence, and state tracking.

This module provides a single, clean handler for processors that combines:
- Processor execution with error handling and retries
- Entity persistence for processors with mapper interface
- State transition tracking
- Processing error recording
"""

import time
from typing import Any, Dict, Optional

from src.context.operation_context import operation
from src.context.tenant_context import tenant_aware
from src.db.db_base import EntityStateEnum
from src.db.db_state_transition_models import TransitionTypeEnum
from src.exceptions import ValidationError, ServiceError
from src.processing.processor_config import ProcessorConfig
from src.processing.processing_service import ProcessingService
from src.processors.message import Message, MessageType, EntityReference
from src.processors.processing_result import ProcessingResult, ProcessingStatus
from src.processors.processor_interface import ProcessorInterface
from src.services.processing_error_service import ProcessingErrorService
from src.services.state_tracking_service import StateTrackingService
from src.utils.logger import get_logger


class ProcessorHandler:
    """
    Unified handler for processor execution with full framework integration.
    
    Handles:
    - Processor execution with error handling
    - Entity persistence (for mappers)
    - State transition tracking
    - Error recording
    - Retry logic
    """
    
    def __init__(
        self,
        processor: ProcessorInterface,
        config: ProcessorConfig,
        processing_service: ProcessingService,
        state_tracking_service: Optional[StateTrackingService] = None,
        error_service: Optional[ProcessingErrorService] = None,
    ):
        """
        Initialize the processor handler.
        
        Args:
            processor: The processor instance to execute
            config: Processor configuration
            processing_service: Service for entity persistence
            state_tracking_service: Optional service for state tracking
            error_service: Optional service for error recording
        """
        # Ensure all models are imported before any database operations
        from src.db.db_config import import_all_models
        import_all_models()
        
        self.processor = processor
        self.config = config
        self.processing_service = processing_service
        self.state_tracking_service = state_tracking_service
        self.error_service = error_service
        self.logger = get_logger()
    
    @tenant_aware
    @operation(name="processor_handler_execute")
    def execute(
        self,
        message: Message,
        execution_context: Optional[Dict[str, Any]] = None,
    ) -> ProcessingResult:
        """
        Execute processor with full framework integration.
        
        Args:
            message: The message to process
            execution_context: Optional execution context
            
        Returns:
            ProcessingResult with execution outcome
        """
        start_time = time.time()
        entity_id = None
        
        try:
            # Log execution start
            self.logger.info(
                f"Starting processor execution: {self.processor.__class__.__name__}",
                extra={
                    "processor_class": self.processor.__class__.__name__,
                    "processor_name": self.config.processor_name,
                    "message_id": message.message_id,
                    "correlation_id": message.correlation_id,
                    "external_id": message.entity_reference.external_id,
                    "source": message.entity_reference.source,
                },
            )
            
            # Validate entity_id requirement for non-source processors
            if not self.config.is_source_processor and not message.entity_reference.entity_id:
                return self._create_failure_result(
                    error_message="Missing entity_id for non-source processor",
                    error_code="MISSING_ENTITY_ID",
                    can_retry=False,
                    duration_ms=(time.time() - start_time) * 1000,
                    message=message,
                    entity_id=None,
                    routing_info={"dead_letter": True, "reason": "missing_entity_id"},
                )
            
            # For non-source processors with entity_id, record state transition
            if (self.config.enable_state_tracking and 
                self.state_tracking_service and 
                message.entity_reference.entity_id):
                self._record_state_transition(
                    entity_id=message.entity_reference.entity_id,
                    from_state=EntityStateEnum.RECEIVED,
                    to_state=EntityStateEnum.PROCESSING,
                    message=message,
                    metadata={"processor": self.config.processor_name},
                )
            
            # Validate message
            if not self.processor.validate_message(message):
                return self._create_failure_result(
                    error_message="Message validation failed",
                    error_code="INVALID_MESSAGE",
                    can_retry=False,
                    duration_ms=(time.time() - start_time) * 1000,
                    message=message,
                    entity_id=entity_id,
                )
            
            # Execute processor
            result = self.processor.process(message)
            
            # Calculate duration
            duration_ms = (time.time() - start_time) * 1000
            result.processing_duration_ms = duration_ms
            
            # Add processor info
            result.processor_info = self.processor.get_processor_info()
            
            # Handle successful result
            if result.success:
                # Persist entity only for source processors
                if self.config.is_source_processor and hasattr(self.processor, 'to_canonical'):
                    entity_id = self._persist_entity(message, result)
                
                # Record successful state transition (only if we have entity_id)
                if (self.config.enable_state_tracking and 
                    self.state_tracking_service and 
                    (entity_id or message.entity_reference.entity_id)):
                    self._record_state_transition(
                        entity_id=entity_id or message.entity_reference.entity_id,
                        from_state=EntityStateEnum.PROCESSING,
                        to_state=EntityStateEnum.COMPLETED,
                        message=message,
                        metadata={
                            "processor": self.config.processor_name,
                            "duration_ms": duration_ms,
                        },
                    )
                
                # Mark message as processed
                message.mark_processed()
                
                # Log completion
                self.logger.info(
                    f"Processor execution completed successfully: {self.processor.__class__.__name__}",
                    extra={
                        "processor_class": self.processor.__class__.__name__,
                        "message_id": message.message_id,
                        "duration_ms": duration_ms,
                        "entity_id": entity_id,
                        "entities_created": len(result.entities_created),
                        "entities_updated": len(result.entities_updated),
                    },
                )
            else:
                # Handle processor failure
                self._handle_processing_failure(
                    result=result,
                    message=message,
                    entity_id=entity_id,
                )
            
            return result
            
        except ValidationError as e:
            return self._handle_validation_error(e, message, start_time, entity_id)
        except ServiceError as e:
            return self._handle_service_error(e, message, start_time, entity_id)
        except Exception as e:
            return self._handle_unexpected_error(e, message, start_time, entity_id)
    
    def _persist_entity(self, message: Message, result: ProcessingResult) -> Optional[str]:
        """
        Persist entity using ProcessingService.
        
        Args:
            message: The message being processed
            result: The processing result
            
        Returns:
            Entity ID if persisted, None otherwise
        """
        try:
            # Transform to canonical format
            canonical_data = self.processor.to_canonical(
                external_data=message.payload,
                metadata=message.metadata or {},
            )
            
            # Process entity
            entity_result = self.processing_service.process_entity(
                external_id=message.entity_reference.external_id,
                canonical_type=message.entity_reference.canonical_type,
                source=message.entity_reference.source,
                content=canonical_data,
                config=self.config,
                custom_attributes={
                    "processor_execution": {
                        "message_id": message.message_id,
                        "correlation_id": message.correlation_id,
                        "processing_duration_ms": result.processing_duration_ms,
                    }
                },
                source_metadata=message.metadata,
            )
            
            # Update result with entity info
            if entity_result.is_new_entity:
                result.entities_created.append(entity_result.entity_id)
            else:
                result.entities_updated.append(entity_result.entity_id)
            
            result.processing_metadata.update({
                "entity_id": entity_result.entity_id,
                "entity_version": entity_result.entity_version,
                "content_changed": entity_result.content_changed,
                "is_new_entity": entity_result.is_new_entity,
            })
            
            return entity_result.entity_id
            
        except Exception as e:
            self.logger.error(
                f"Entity persistence failed: {type(e).__name__}: {e}",
                extra={
                    "processor_class": self.processor.__class__.__name__,
                    "message_id": message.message_id,
                    "error": str(e),
                    "error_type": type(e).__name__,
                },
                exc_info=True  # This will log the full traceback
            )
            # Don't fail the processing if persistence fails, but log it properly
            return None
    
    def _record_state_transition(
        self,
        entity_id: Optional[str],
        from_state: EntityStateEnum,
        to_state: EntityStateEnum,
        message: Message,
        metadata: Dict[str, Any],
    ) -> None:
        """Record state transition if service is available."""
        if not self.state_tracking_service:
            return
            
        try:
            self.state_tracking_service.record_transition(
                entity_id=entity_id or "pending",
                from_state=from_state,
                to_state=to_state,
                actor=self.config.processor_name,
                transition_type=TransitionTypeEnum.NORMAL,
                processor_data={
                    "processor_name": self.config.processor_name,
                    "processor_version": self.config.processor_version,
                    **metadata,
                    "message_id": message.message_id,
                    "correlation_id": message.correlation_id,
                },
            )
        except Exception as e:
            self.logger.warning(
                f"Failed to record state transition: {e}",
                extra={"error": str(e)},
            )
    
    def _handle_processing_failure(
        self,
        result: ProcessingResult,
        message: Message,
        entity_id: Optional[str],
    ) -> None:
        """Handle processor failure."""
        # Record error if service is available
        if self.error_service:
            try:
                self.error_service.record_error(
                    entity_id=entity_id,
                    processor_name=self.config.processor_name,
                    error_type=result.error_code or "PROCESSING_FAILURE",
                    error_message=result.error_message or "Processing failed",
                    error_details=result.error_details,
                    can_retry=result.can_retry,
                    retry_count=message.retry_count,
                )
            except Exception as e:
                self.logger.warning(
                    f"Failed to record processing error: {e}",
                    extra={"error": str(e)},
                )
        
        # Record failed state transition
        if self.config.enable_state_tracking and self.state_tracking_service:
            self._record_state_transition(
                entity_id=entity_id,
                from_state=EntityStateEnum.PROCESSING,
                to_state=EntityStateEnum.SYSTEM_ERROR,
                message=message,
                metadata={
                    "processor": self.config.processor_name,
                    "error_code": result.error_code,
                    "error_message": result.error_message,
                },
            )
    
    def _handle_validation_error(
        self,
        error: ValidationError,
        message: Message,
        start_time: float,
        entity_id: Optional[str],
    ) -> ProcessingResult:
        """Handle validation errors."""
        duration_ms = (time.time() - start_time) * 1000
        
        self.logger.warning(
            f"Validation error in processor: {self.processor.__class__.__name__}",
            extra={
                "processor_class": self.processor.__class__.__name__,
                "message_id": message.message_id,
                "error": str(error),
                "duration_ms": duration_ms,
            },
        )
        
        result = self._create_failure_result(
            error_message=f"Validation error: {str(error)}",
            error_code="VALIDATION_ERROR",
            error_details={"validation_details": getattr(error, "details", {})},
            can_retry=False,
            duration_ms=duration_ms,
            message=message,
            entity_id=entity_id,
        )
        
        self._handle_processing_failure(result, message, entity_id)
        return result
    
    def _handle_service_error(
        self,
        error: ServiceError,
        message: Message,
        start_time: float,
        entity_id: Optional[str],
    ) -> ProcessingResult:
        """Handle service errors."""
        duration_ms = (time.time() - start_time) * 1000
        can_retry = self.processor.can_retry(error)
        
        self.logger.error(
            f"Service error in processor: {self.processor.__class__.__name__}",
            extra={
                "processor_class": self.processor.__class__.__name__,
                "message_id": message.message_id,
                "error": str(error),
                "error_code": getattr(error, "error_code", None),
                "can_retry": can_retry,
                "duration_ms": duration_ms,
            },
            exc_info=True,
        )
        
        result = self._create_failure_result(
            error_message=f"Service error: {str(error)}",
            error_code="SERVICE_ERROR",
            error_details={"service_error_code": getattr(error, "error_code", None)},
            can_retry=can_retry,
            retry_after_seconds=self._calculate_retry_delay(message.retry_count),
            duration_ms=duration_ms,
            message=message,
            entity_id=entity_id,
        )
        
        self._handle_processing_failure(result, message, entity_id)
        return result
    
    def _handle_unexpected_error(
        self,
        error: Exception,
        message: Message,
        start_time: float,
        entity_id: Optional[str],
    ) -> ProcessingResult:
        """Handle unexpected errors."""
        duration_ms = (time.time() - start_time) * 1000
        can_retry = self.processor.can_retry(error)
        
        self.logger.error(
            f"Unexpected error in processor: {self.processor.__class__.__name__}",
            extra={
                "processor_class": self.processor.__class__.__name__,
                "message_id": message.message_id,
                "error": str(error),
                "error_type": type(error).__name__,
                "can_retry": can_retry,
                "duration_ms": duration_ms,
            },
            exc_info=True,
        )
        
        result = self._create_failure_result(
            error_message=f"Unexpected error: {str(error)}",
            error_code="UNEXPECTED_ERROR",
            error_details={
                "error_type": type(error).__name__,
                "error_details": str(error),
            },
            can_retry=can_retry,
            retry_after_seconds=self._calculate_retry_delay(message.retry_count),
            duration_ms=duration_ms,
            message=message,
            entity_id=entity_id,
        )
        
        self._handle_processing_failure(result, message, entity_id)
        return result
    
    def _create_failure_result(
        self,
        error_message: str,
        error_code: str,
        can_retry: bool,
        duration_ms: float,
        message: Message,
        entity_id: Optional[str],
        error_details: Optional[Dict[str, Any]] = None,
        retry_after_seconds: Optional[int] = None,
        routing_info: Optional[Dict[str, Any]] = None,
    ) -> ProcessingResult:
        """Create a failure result with consistent formatting."""
        return ProcessingResult.create_failure(
            error_message=error_message,
            error_code=error_code,
            error_details=error_details or {},
            can_retry=can_retry,
            retry_after_seconds=retry_after_seconds,
            routing_info=routing_info or message.routing_info,
            processing_duration_ms=duration_ms,
        )
    
    def _calculate_retry_delay(self, retry_count: int) -> int:
        """Calculate retry delay with exponential backoff."""
        # 2^retry_count seconds, max 300 seconds (5 minutes)
        return min(2**retry_count, 300)
    
    def handle_message(self, message) -> dict:
        """
        Handle a message using the configured processor.
        
        This provides compatibility with existing code that expects
        a dictionary result format.
        
        Args:
            message: Message to process (can be dict or Message object)
            
        Returns:
            Dictionary with processing results
        """
        # Convert dict to Message if needed
        if isinstance(message, dict):
            message = self._convert_dict_to_message(message)
        elif not isinstance(message, Message):
            raise ValueError(f"Unsupported message type: {type(message)}")
        
        # Execute processor
        result = self.execute(message)
        
        # Convert result to dict
        return self._convert_result_to_dict(result, message)
    
    def _convert_dict_to_message(self, message_dict: dict) -> Message:
        """Convert dictionary message to Message object."""
        entity_ref_data = message_dict.get("entity_reference", {})
        
        return Message.create_entity_message(
            external_id=entity_ref_data.get("external_id", "unknown"),
            canonical_type=entity_ref_data.get("canonical_type", "unknown"),
            source=entity_ref_data.get("source", "unknown"),
            tenant_id=entity_ref_data.get("tenant_id", "unknown"),
            payload=message_dict.get("payload", {}),
            entity_id=entity_ref_data.get("entity_id"),
            version=entity_ref_data.get("version"),
            correlation_id=message_dict.get("correlation_id"),
            metadata=message_dict.get("metadata", {}),
        )
    
    def _convert_result_to_dict(self, result: ProcessingResult, original_message: Message) -> dict:
        """Convert ProcessingResult to dictionary format."""
        return {
            "success": result.success,
            "status": result.status.value,
            "output_messages": [self._message_to_dict(msg) for msg in result.output_messages],
            "routing_info": result.routing_info,
            "error_message": result.error_message,
            "error_code": result.error_code,
            "processing_metadata": result.processing_metadata,
            "entities_created": result.entities_created,
            "entities_updated": result.entities_updated,
            "processing_duration_ms": result.processing_duration_ms,
            "can_retry": result.can_retry,
            "retry_after_seconds": result.retry_after_seconds,
            "processor_info": result.processor_info,
            "original_message_id": original_message.message_id,
            "correlation_id": original_message.correlation_id,
        }
    
    def _message_to_dict(self, message: Message) -> dict:
        """Convert Message object to dictionary format."""
        return {
            "message_id": message.message_id,
            "correlation_id": message.correlation_id,
            "message_type": message.message_type.value,
            "entity_reference": {
                "entity_id": message.entity_reference.entity_id,
                "external_id": message.entity_reference.external_id,
                "canonical_type": message.entity_reference.canonical_type,
                "source": message.entity_reference.source,
                "tenant_id": message.entity_reference.tenant_id,
                "version": message.entity_reference.version,
            },
            "payload": message.payload,
            "metadata": message.metadata,
            "routing_info": message.routing_info,
            "created_at": message.created_at.isoformat(),
            "processed_at": message.processed_at.isoformat() if message.processed_at else None,
        }