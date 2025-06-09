"""
Token Cleanup Processor for cleaning up expired access tokens.

This processor is designed to run as an Azure Functions timer trigger
to periodically clean up expired access tokens from the database.
"""

import json
import os
from datetime import datetime
from typing import Any, Dict, Optional

from src.context.tenant_context import TenantContext
from src.db.db_config import get_production_config, import_all_models
from src.processors.v2.processor_interface import ProcessorInterface
from src.processors.v2.message import Message
from src.repositories.credential_repository import CredentialRepository
from src.services.credential_service import CredentialService
from src.utils.logger import get_logger
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker


class TokenCleanupProcessor(ProcessorInterface):
    """
    Processor for cleaning up expired access tokens.
    
    This processor:
    1. Connects to the database
    2. Cleans up expired tokens based on configurable age
    3. Returns metrics about the cleanup operation
    
    Configuration can be provided via:
    - Environment variables
    - Message payload
    - Default values
    """
    
    # Default configuration
    DEFAULT_CLEANUP_AGE_MINUTES = 40
    
    def __init__(self, cleanup_age_minutes: Optional[int] = None):
        """
        Initialize the token cleanup processor.
        
        Args:
            cleanup_age_minutes: How old tokens must be before deletion (default: 40)
        """
        self.cleanup_age_minutes = cleanup_age_minutes or self.DEFAULT_CLEANUP_AGE_MINUTES
        self.logger = get_logger()
        
        # Import all models for SQLAlchemy relationships
        import_all_models()
    
    def get_processor_name(self) -> str:
        """Get the processor name for logging and identification."""
        return "TokenCleanupProcessor"
    
    def get_processor_version(self) -> str:
        """Get the processor version."""
        return "1.0.0"
    
    def is_source_processor(self) -> bool:
        """This is a maintenance processor, not a source processor."""
        return False
    
    def process(self, message: Message) -> Message:
        """
        Process token cleanup operation.
        
        Args:
            message: Timer trigger message (payload may contain configuration)
            
        Returns:
            Message with cleanup metrics and results
        """
        start_time = datetime.utcnow()
        
        self.logger.info(
            "Starting token cleanup operation",
            extra={
                "processor": self.get_processor_name(),
                "cleanup_age_minutes": self.cleanup_age_minutes,
                "start_time": start_time.isoformat()
            }
        )
        
        try:
            # Parse configuration from message if provided
            config = self._parse_configuration(message)
            cleanup_age_minutes = config.get("cleanup_age_minutes")
            if cleanup_age_minutes is None:
                cleanup_age_minutes = self.cleanup_age_minutes
            
            # Setup database connection
            db_config = get_production_config()
            engine = create_engine(db_config.get_connection_string())
            Session = sessionmaker(bind=engine)
            
            total_deleted = 0
            
            with Session() as session:
                # Create credential service (no tenant context needed for cleanup)
                credential_repo = CredentialRepository(session)
                credential_service = CredentialService(credential_repo)
                
                # Perform cleanup
                deleted_count = credential_service.cleanup_expired_tokens(
                    cleanup_age_minutes=cleanup_age_minutes
                )
                total_deleted += deleted_count
                
                # Commit the transaction
                session.commit()
            
            end_time = datetime.utcnow()
            duration = (end_time - start_time).total_seconds()
            
            # Prepare result
            result_data = {
                "status": "success",
                "cleanup_age_minutes": cleanup_age_minutes,
                "tokens_deleted": total_deleted,
                "start_time": start_time.isoformat(),
                "end_time": end_time.isoformat(),
                "duration_seconds": duration,
                "processor": self.get_processor_name(),
                "version": self.get_processor_version()
            }
            
            self.logger.info(
                "Token cleanup completed successfully",
                extra=result_data
            )
            
            # Return success message
            return Message(
                entity=None,
                message_type=message.message_type,
                payload=result_data,
                metadata={
                    "processor": self.get_processor_name(),
                    "operation": "token_cleanup",
                    "status": "success",
                    "processed_at": end_time.isoformat()
                }
            )
            
        except Exception as e:
            end_time = datetime.utcnow()
            duration = (end_time - start_time).total_seconds()
            
            error_data = {
                "status": "error",
                "error": str(e),
                "error_type": type(e).__name__,
                "cleanup_age_minutes": self.cleanup_age_minutes,
                "start_time": start_time.isoformat(),
                "end_time": end_time.isoformat(),
                "duration_seconds": duration,
                "processor": self.get_processor_name(),
                "version": self.get_processor_version()
            }
            
            self.logger.error(
                "Token cleanup failed",
                extra=error_data,
                exc_info=True
            )
            
            # Return error message
            return Message(
                entity=None,
                message_type=message.message_type,
                payload=error_data,
                metadata={
                    "processor": self.get_processor_name(),
                    "operation": "token_cleanup",
                    "status": "error",
                    "processed_at": end_time.isoformat()
                }
            )
    
    def _parse_configuration(self, message: Message) -> Dict[str, Any]:
        """
        Parse configuration from message payload and environment variables.
        
        Priority order:
        1. Message payload
        2. Environment variables
        3. Default values
        
        Args:
            message: Input message that may contain configuration
            
        Returns:
            Configuration dictionary
        """
        config = {}
        
        # Try to get configuration from message payload
        if message.payload:
            try:
                if isinstance(message.payload, dict):
                    config.update(message.payload)
                elif isinstance(message.payload, str):
                    payload_data = json.loads(message.payload)
                    if isinstance(payload_data, dict):
                        config.update(payload_data)
            except (json.JSONDecodeError, TypeError) as e:
                self.logger.warning(
                    "Failed to parse configuration from message payload",
                    extra={
                        "error": str(e),
                        "payload_type": type(message.payload).__name__
                    }
                )
        
        # Override with environment variables if present
        env_cleanup_age = os.getenv("TOKEN_CLEANUP_AGE_MINUTES")
        if env_cleanup_age:
            try:
                config["cleanup_age_minutes"] = int(env_cleanup_age)
            except ValueError:
                self.logger.warning(
                    "Invalid TOKEN_CLEANUP_AGE_MINUTES environment variable",
                    extra={"value": env_cleanup_age}
                )
        
        return config


def create_timer_function_handler(cleanup_age_minutes: Optional[int] = None):
    """
    Factory function to create an Azure Functions timer handler.
    
    Args:
        cleanup_age_minutes: Optional cleanup age override
        
    Returns:
        Function that can be used as Azure Functions timer trigger
        
    Example usage in Azure Functions:
        from src.processors.infrastructure.token_cleanup_processor import create_timer_function_handler
        
        # Create handler with default settings
        timer_handler = create_timer_function_handler()
        
        @app.timer_trigger(schedule="0 */20 * * * *")  # Every 20 minutes
        def token_cleanup_timer(timer: func.TimerRequest) -> None:
            timer_handler(timer)
    """
    processor = TokenCleanupProcessor(cleanup_age_minutes=cleanup_age_minutes)
    
    def timer_handler(timer_request) -> Dict[str, Any]:
        """
        Azure Functions timer handler.
        
        Args:
            timer_request: Azure Functions timer request object
            
        Returns:
            Dictionary with cleanup results
        """
        # Create message from timer trigger  
        from src.processors.v2.message import MessageType
        message = Message(
            entity=None,  # System processor doesn't work with entities
            message_type=MessageType.CONTROL_MESSAGE,
            payload={
                "trigger_type": "timer",
                "past_due": getattr(timer_request, 'past_due', False),
                "schedule_status": getattr(timer_request, 'schedule_status', {})
            },
            metadata={
                "source": "azure_functions_timer",
                "timestamp": datetime.utcnow().isoformat()
            }
        )
        
        # Process the cleanup
        result_message = processor.process(message)
        
        # Return the payload for Azure Functions
        return result_message.payload
    
    return timer_handler