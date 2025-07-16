"""
Simple processor handler for the V2 framework.

This module provides a lightweight handler that manages pipeline execution
tracking and output routing for processors.
"""

import time
import threading
import json
import copy
from datetime import datetime, timezone
from typing import Any, Dict, Optional

from ..utils.logger import get_logger
from .message import Message
from .processing_result import ProcessingResult
from .simple_processor_interface import SimpleProcessorInterface
from ..db.db_config import get_db_manager
from ..db.db_pipeline_tracking_models import PipelineExecution, PipelineStep, PipelineMessage


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
        enable_message_storage: bool = False,
        message_sanitization_rules: Optional[Dict[str, Any]] = None,
    ):
        """
        Initialize the processor handler.

        Args:
            processor: The processor to handle
            enable_pipeline_tracking: Whether to track pipeline execution
            enable_metrics: Whether to collect processing metrics
            enable_message_storage: Whether to store input/output messages for debugging
            message_sanitization_rules: Rules for sanitizing sensitive data in messages
        """
        self.processor = processor
        self.enable_pipeline_tracking = enable_pipeline_tracking
        self.enable_metrics = enable_metrics
        self.enable_message_storage = enable_message_storage
        self.message_sanitization_rules = message_sanitization_rules or {}
        self.logger = get_logger()
        self._execution_id = None
        # Remove self._step_id as it will be local to each process_message call

    def process_message(self, message: Message, context: Optional[Dict[str, Any]] = None) -> ProcessingResult:
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
        step_id = None  # Local step_id for this message processing

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
                return ProcessingResult.failure_result(error_message="Message validation failed", error_code="INVALID_MESSAGE")

            # Track pipeline execution start (if enabled)
            if self.enable_pipeline_tracking:
                step_id = self._track_pipeline_start(message, processor_name, context)
                
                # Store input message (if enabled)
                if self.enable_message_storage and step_id:
                    self._store_input_message(message, step_id, context)

            # Process the message
            result = self.processor.process(message, context)

            # Calculate processing duration
            processing_duration_ms = int((time.time() - start_time) * 1000)
            if result.processing_duration_ms is None:
                result.processing_duration_ms = processing_duration_ms

            # Track pipeline execution completion (if enabled)
            if self.enable_pipeline_tracking:
                self._track_pipeline_completion(message, processor_name, result, context, step_id)
                
                # Store output messages (if enabled)
                if self.enable_message_storage and step_id and result.output_messages:
                    self._store_output_messages(result.output_messages, step_id, context)

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
                self.logger.info(f"Processing completed successfully: {processor_name}", extra=result_context)
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
                self._track_pipeline_failure(message, processor_name, str(e), context, step_id)

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

    def _track_pipeline_start(self, message: Message, processor_name: str, context: Dict[str, Any]) -> str:
        """
        Track the start of pipeline execution.

        Args:
            message: Message being processed
            processor_name: Name of the processor
            context: Processing context
            
        Returns:
            str: The step_id for this processing step
        """
        try:
            db_manager = get_db_manager()
            session = db_manager.get_session()
            
            try:
                # Check if pipeline execution already exists
                existing_execution = session.query(PipelineExecution).filter_by(
                    pipeline_id=message.pipeline_id
                ).first()
                
                if not existing_execution:
                    # Create new pipeline execution
                    execution = PipelineExecution(
                        pipeline_id=message.pipeline_id,
                        tenant_id=message.tenant_id,
                        correlation_id=message.correlation_id,
                        status="started",
                        started_at=datetime.now(timezone.utc),
                        trigger_type=context.get("trigger_type", "queue"),
                        trigger_source=context.get("trigger_source", "unknown"),
                        step_count=0,
                        message_count=1,
                        error_count=0,
                        context=context
                    )
                    session.add(execution)
                    session.flush()
                    self._execution_id = execution.id
                else:
                    self._execution_id = existing_execution.id
                    # Update message count
                    existing_execution.message_count += 1
                
                # Create pipeline step
                step = PipelineStep(
                    execution_id=self._execution_id,
                    pipeline_id=message.pipeline_id,
                    tenant_id=message.tenant_id,
                    step_name=processor_name,
                    processor_name=processor_name,
                    function_name=context.get("function_name", processor_name),
                    message_id=message.message_id,
                    correlation_id=message.correlation_id,
                    started_at=datetime.now(timezone.utc),
                    status="processing",
                    context=context
                )
                session.add(step)
                session.flush()
                step_id = step.id
                
                # Update execution step count
                if existing_execution:
                    existing_execution.step_count += 1
                else:
                    execution.step_count += 1
                
                session.commit()
                self.logger.debug(f"Pipeline step created | step_id={step_id} | processor={processor_name} | message_id={message.message_id}")
                
                return step_id
                
            except Exception as e:
                session.rollback()
                self.logger.error(f"Failed to track pipeline start: {str(e)}")
                raise
            finally:
                session.close()
                
        except Exception as e:
            self.logger.error(f"Error in pipeline tracking: {str(e)}")
            # Continue processing even if tracking fails
            return None

    def _track_pipeline_completion(
        self,
        message: Message,
        processor_name: str,
        result: ProcessingResult,
        context: Dict[str, Any],
        step_id: str = None,
    ) -> None:
        """
        Track the completion of pipeline execution.

        Args:
            message: Message that was processed
            processor_name: Name of the processor
            result: Processing result
            context: Processing context
        """
        try:
            db_manager = get_db_manager()
            session = db_manager.get_session()
            
            try:
                # Update step completion
                if step_id:
                    step = session.query(PipelineStep).filter_by(id=step_id).first()
                    if step:
                        step.completed_at = datetime.now(timezone.utc)
                        step.duration_ms = result.processing_duration_ms
                        step.status = "completed" if result.success else "failed"
                        step.output_count = len(result.output_messages)
                        step.output_queues = [msg.destination_queue for msg in result.output_messages if hasattr(msg, 'destination_queue')]
                        
                        if not result.success:
                            step.error_message = result.error_message
                            step.error_type = result.error_code
                        
                        self.logger.debug(f"Pipeline step completed | step_id={step_id} | processor={processor_name} | status={step.status} | duration={result.processing_duration_ms}ms")
                    else:
                        self.logger.warning(f"Pipeline step not found | step_id={step_id}")
                else:
                    self.logger.warning(f"No step_id to update | message_id={message.message_id}")
                
                # Update execution completion (if this is the last step)
                if self._execution_id and len(result.output_messages) == 0:
                    # This might be the final step - mark execution as complete
                    execution = session.query(PipelineExecution).filter_by(id=self._execution_id).first()
                    if execution:
                        execution.completed_at = datetime.now(timezone.utc)
                        execution.status = "completed" if result.success else "failed"
                        if execution.started_at:
                            execution.duration_ms = int((datetime.now(timezone.utc) - execution.started_at).total_seconds() * 1000)
                        
                        if not result.success:
                            execution.error_message = result.error_message
                            execution.error_step = processor_name
                            execution.error_count += 1
                
                session.commit()
                
            except Exception as e:
                session.rollback()
                self.logger.error(f"Failed to track pipeline completion: {str(e)}")
                raise
            finally:
                session.close()
                
        except Exception as e:
            self.logger.error(f"Error in pipeline completion tracking: {str(e)}")
            # Continue processing even if tracking fails

    def _track_pipeline_failure(self, message: Message, processor_name: str, error_message: str, context: Dict[str, Any], step_id: str = None) -> None:
        """
        Track the failure of pipeline execution.

        Args:
            message: Message that was processed
            processor_name: Name of the processor
            error_message: Error message
            context: Processing context
        """
        try:
            db_manager = get_db_manager()
            session = db_manager.get_session()
            
            try:
                # Update step failure
                if step_id:
                    step = session.query(PipelineStep).filter_by(id=step_id).first()
                    if step:
                        step.completed_at = datetime.now(timezone.utc)
                        step.status = "failed"
                        step.error_message = error_message
                        step.error_type = "PROCESSING_EXCEPTION"
                        if step.started_at:
                            step.duration_ms = int((datetime.now(timezone.utc) - step.started_at).total_seconds() * 1000)
                
                # Update execution failure
                if self._execution_id:
                    execution = session.query(PipelineExecution).filter_by(id=self._execution_id).first()
                    if execution:
                        execution.completed_at = datetime.now(timezone.utc)
                        execution.status = "failed"
                        execution.error_message = error_message
                        execution.error_step = processor_name
                        execution.error_count += 1
                        if execution.started_at:
                            execution.duration_ms = int((datetime.now(timezone.utc) - execution.started_at).total_seconds() * 1000)
                
                session.commit()
                
            except Exception as e:
                session.rollback()
                self.logger.error(f"Failed to track pipeline failure: {str(e)}")
                raise
            finally:
                session.close()
                
        except Exception as e:
            self.logger.error(f"Error in pipeline failure tracking: {str(e)}")
            # Continue processing even if tracking fails

    def _sanitize_message(self, message_data: Dict[str, Any]) -> tuple[Dict[str, Any], bool]:
        """
        Sanitize message data to remove sensitive information.
        
        Args:
            message_data: The message data to sanitize
            
        Returns:
            tuple: (sanitized_data, was_sanitized)
        """
        if not self.message_sanitization_rules:
            return message_data, False
            
        sanitized_data = copy.deepcopy(message_data)
        was_sanitized = False
        
        # Apply sanitization rules
        for field_path, action in self.message_sanitization_rules.items():
            if self._apply_sanitization_rule(sanitized_data, field_path, action):
                was_sanitized = True
                
        return sanitized_data, was_sanitized
    
    def _apply_sanitization_rule(self, data: Dict[str, Any], field_path: str, action: str) -> bool:
        """
        Apply a single sanitization rule to data.
        
        Args:
            data: The data to modify
            field_path: Dot-separated path to the field (e.g., "user.password")
            action: The action to take ("mask", "remove", "hash")
            
        Returns:
            bool: Whether any sanitization was applied
        """
        keys = field_path.split('.')
        current = data
        
        # Navigate to the parent of the target field
        for key in keys[:-1]:
            if isinstance(current, dict) and key in current:
                current = current[key]
            else:
                return False
                
        # Apply the action to the final key
        final_key = keys[-1]
        if isinstance(current, dict) and final_key in current:
            if action == "remove":
                del current[final_key]
            elif action == "mask":
                current[final_key] = "***MASKED***"
            elif action == "hash":
                current[final_key] = f"***HASH:{hash(str(current[final_key]))}***"
            return True
            
        return False

    def _store_input_message(self, message: Message, step_id: str, context: Dict[str, Any]) -> None:
        """
        Store the input message for debugging purposes.
        
        Args:
            message: The input message
            step_id: The pipeline step ID
            context: Processing context
        """
        try:
            # Convert message to dict for storage
            message_data = {
                "message_id": message.message_id,
                "pipeline_id": message.pipeline_id,
                "tenant_id": message.tenant_id,
                "correlation_id": message.correlation_id,
                "payload": message.payload,
                "context": message.context,
                "created_at": message.created_at.isoformat() if message.created_at else None,
            }
            
            # Sanitize the message data
            sanitized_data, was_sanitized = self._sanitize_message(message_data)
            
            # Calculate message size
            message_size = len(json.dumps(sanitized_data, default=str))
            
            db_manager = get_db_manager()
            session = db_manager.get_session()
            
            try:
                pipeline_message = PipelineMessage(
                    step_id=step_id,
                    execution_id=self._execution_id,
                    tenant_id=message.tenant_id,
                    message_id=message.message_id,
                    message_type="input",
                    message_payload=sanitized_data,
                    message_size_bytes=message_size,
                    source_queue=context.get("source_queue"),
                    target_queue=None,
                    is_sanitized=was_sanitized,
                    sanitization_rules=self.message_sanitization_rules if was_sanitized else None,
                    context=context
                )
                session.add(pipeline_message)
                session.commit()
                
                self.logger.debug(f"Stored input message | step_id={step_id} | message_id={message.message_id} | size={message_size} | sanitized={was_sanitized}")
                
            except Exception as e:
                session.rollback()
                self.logger.error(f"Failed to store input message: {str(e)}")
                raise
            finally:
                session.close()
                
        except Exception as e:
            self.logger.error(f"Error storing input message: {str(e)}")
            # Continue processing even if message storage fails

    def _store_output_messages(self, output_messages: list, step_id: str, context: Dict[str, Any]) -> None:
        """
        Store the output messages for debugging purposes.
        
        Args:
            output_messages: List of output messages
            step_id: The pipeline step ID
            context: Processing context
        """
        try:
            db_manager = get_db_manager()
            session = db_manager.get_session()
            
            try:
                for output_message in output_messages:
                    # Convert message to dict for storage
                    created_at = getattr(output_message, 'created_at', None)
                    message_data = {
                        "message_id": getattr(output_message, 'message_id', None),
                        "pipeline_id": getattr(output_message, 'pipeline_id', None),
                        "tenant_id": getattr(output_message, 'tenant_id', None),
                        "correlation_id": getattr(output_message, 'correlation_id', None),
                        "payload": getattr(output_message, 'payload', None),
                        "context": getattr(output_message, 'context', {}),
                        "created_at": created_at.isoformat() if created_at else None,
                    }
                    
                    # Sanitize the message data
                    sanitized_data, was_sanitized = self._sanitize_message(message_data)
                    
                    # Calculate message size
                    message_size = len(json.dumps(sanitized_data, default=str))
                    
                    pipeline_message = PipelineMessage(
                        step_id=step_id,
                        execution_id=self._execution_id,
                        tenant_id=getattr(output_message, 'tenant_id', None),
                        message_id=getattr(output_message, 'message_id', None),
                        message_type="output",
                        message_payload=sanitized_data,
                        message_size_bytes=message_size,
                        source_queue=None,
                        target_queue=getattr(output_message, 'destination_queue', None),
                        is_sanitized=was_sanitized,
                        sanitization_rules=self.message_sanitization_rules if was_sanitized else None,
                        context=context
                    )
                    session.add(pipeline_message)
                    
                    self.logger.debug(f"Stored output message | step_id={step_id} | message_id={getattr(output_message, 'message_id', None)} | size={message_size} | sanitized={was_sanitized}")
                
                session.commit()
                
            except Exception as e:
                session.rollback()
                self.logger.error(f"Failed to store output messages: {str(e)}")
                raise
            finally:
                session.close()
                
        except Exception as e:
            self.logger.error(f"Error storing output messages: {str(e)}")
            # Continue processing even if message storage fails
