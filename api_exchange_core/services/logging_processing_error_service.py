"""
Logging-based processing error service.

Replaces database-based ProcessingErrorService with structured logging that can be
consumed by Loki/ELK for monitoring and analysis. Much simpler and more reliable.
"""

import uuid
from datetime import datetime, UTC

from ..context.operation_context import operation
from ..utils.logger import get_logger
from ..schemas.processing_error_schema import ProcessingErrorCreate


class LoggingProcessingErrorService:
    """
    Logging-based processing error service.
    
    Records processing errors as structured log events instead of database records.
    Can be consumed by Loki/ELK for monitoring, alerting, and analysis.
    """

    def __init__(self):
        """Initialize the logging processing error service."""
        self.logger = get_logger()

    @operation(name="log_processing_error")
    def create_error(self, error_data: ProcessingErrorCreate) -> str:
        """
        Record a processing error as a structured log event.

        Args:
            error_data: Validated error data

        Returns:
            ID of the logged error (for compatibility)
        """
        # Generate ID for compatibility with existing code
        error_id = str(uuid.uuid4())
        
        # Get tenant context
        from ..context.tenant_context import TenantContext
        tenant_id = TenantContext.get_current_tenant_id()
        
        # Create structured log event
        log_data = {
            "event_type": "processing_error",
            "error_id": error_id,
            "entity_id": error_data.entity_id,
            "tenant_id": tenant_id,
            "error_type": error_data.error_type_code,
            "error_message": error_data.message,
            "processing_step": error_data.processing_step,
            "stack_trace": error_data.stack_trace,
            "timestamp": datetime.now(UTC).isoformat(),
        }
        
        # Remove None values to keep logs clean
        log_data = {k: v for k, v in log_data.items() if v is not None}
        
        # Log the processing error event
        self.logger.error(
            f"Processing error in {error_data.processing_step}: {error_data.message}",
            extra=log_data,
            exc_info=False  # Don't duplicate stack trace in log record
        )
        
        return error_id

    @operation(name="log_processing_error_simple")
    def record_error(
        self, 
        entity_id: str, 
        error_type: str, 
        message: str, 
        processing_step: str, 
        stack_trace: str = None
    ) -> str:
        """
        Convenience method for recording an error without requiring a schema object.

        Args:
            entity_id: ID of the entity
            error_type: Type of error
            message: Error message
            processing_step: Processing step where error occurred
            stack_trace: Optional stack trace

        Returns:
            ID of the logged error
        """
        # Create ProcessingErrorCreate schema from parameters
        error_data = ProcessingErrorCreate(
            entity_id=entity_id,
            error_type_code=error_type,
            message=message,
            processing_step=processing_step,
            stack_trace=stack_trace,
        )
        
        return self.create_error(error_data)

    def get_error(self, error_id: str):
        """
        Get processing error (not supported in logging mode).
        
        Args:
            error_id: ID of the error
            
        Returns:
            None - use Loki/ELK queries instead
        """
        self.logger.warning(
            "get_error not supported in logging mode - use Loki/ELK queries",
            extra={"error_id": error_id, "suggestion": "query logs with error_id filter"}
        )
        return None

    def get_entity_errors(self, entity_id: str):
        """
        Get entity errors (not supported in logging mode).
        
        Args:
            entity_id: ID of the entity
            
        Returns:
            Empty list - use Loki/ELK queries instead
        """
        self.logger.warning(
            "get_entity_errors not supported in logging mode - use Loki/ELK queries",
            extra={"entity_id": entity_id, "suggestion": "query logs with entity_id filter"}
        )
        return []

    def find_errors(self, filter_params, limit=None, offset=None):
        """
        Find processing errors (not supported in logging mode).
        
        Returns:
            Empty list - use Loki/ELK queries instead
        """
        self.logger.warning(
            "find_errors not supported in logging mode - use Loki/ELK queries",
            extra={"suggestion": "use Loki/ELK queries with appropriate filters"}
        )
        return []

    def delete_error(self, error_id: str) -> bool:
        """
        Delete processing error (not supported in logging mode).
        
        Args:
            error_id: ID of the error
            
        Returns:
            False - logs cannot be deleted
        """
        self.logger.warning(
            "delete_error not supported in logging mode - logs are immutable",
            extra={"error_id": error_id, "suggestion": "use log retention policies instead"}
        )
        return False

    def delete_entity_errors(self, entity_id: str) -> int:
        """
        Delete entity errors (not supported in logging mode).
        
        Args:
            entity_id: ID of the entity
            
        Returns:
            0 - logs cannot be deleted
        """
        self.logger.warning(
            "delete_entity_errors not supported in logging mode - logs are immutable",
            extra={"entity_id": entity_id, "suggestion": "use log retention policies instead"}
        )
        return 0