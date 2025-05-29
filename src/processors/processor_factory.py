"""
Processor factory and handler creation patterns.

This module provides factory patterns and helper functions for creating
and configuring processors with proper dependency injection.
"""

from typing import Any, Optional, Type, TypeVar

from src.exceptions import ErrorCode
from src.processing.processing_service import ProcessingService
from src.processing.processor_config import ProcessorConfig
from src.processors.processor_executor import ProcessorExecutor
from src.processors.processor_interface import ProcessorInterface
from src.repositories.entity_repository import EntityRepository
from src.services.entity_service import EntityService
from src.utils.logger import get_logger

T = TypeVar("T", bound=ProcessorInterface)


class ProcessorFactory:
    """
    Factory for creating processors with proper dependency injection.

    Handles the common pattern of injecting Core services into processors
    and configuring them with appropriate settings.
    """

    def __init__(
        self,
        entity_service: EntityService,
        entity_repository: EntityRepository,
        processing_service: ProcessingService,
    ):
        """
        Initialize the processor factory with Core services.

        Args:
            entity_service: Service for entity operations
            entity_repository: Repository for direct entity access
            processing_service: Service for processing workflows
        """
        self.entity_service = entity_service
        self.entity_repository = entity_repository
        self.processing_service = processing_service
        self.logger = get_logger()

    def create_processor(
        self,
        processor_class: Type[T],
        config: ProcessorConfig,
        **kwargs: Any,
    ) -> T:
        """
        Create a processor instance with injected dependencies.

        Args:
            processor_class: The processor class to instantiate
            config: Configuration for the processor
            **kwargs: Additional arguments for processor constructor

        Returns:
            Configured processor instance

        Raises:
            ServiceError: If processor creation fails
        """
        try:
            # Standard dependencies available to all processors
            dependencies = {
                "entity_service": self.entity_service,
                "entity_repository": self.entity_repository,
                "processing_service": self.processing_service,
                "config": config,
                "logger": self.logger,
                **kwargs,
            }

            # Create processor instance
            processor = processor_class(**dependencies)

            self.logger.info(
                f"Created processor: {processor_class.__name__}",
                extra={
                    "processor_class": processor_class.__name__,
                    "processor_name": config.processor_name,
                    "processor_version": config.processor_version,
                },
            )

            return processor

        except Exception as e:
            self.logger.error(
                f"Failed to create processor: {processor_class.__name__}",
                extra={
                    "processor_class": processor_class.__name__,
                    "error": str(e),
                },
                exc_info=True,
            )
            from src.exceptions import ServiceError

            raise ServiceError(
                f"Failed to create processor {processor_class.__name__}: {str(e)}",
                error_code=ErrorCode.INTERNAL_ERROR,
            ) from e


class ProcessorHandler:
    """
    Handler for managing processor execution with Core service integration.

    Provides the execution pattern that was used in the old unified processor
    but updated to work with the new Core services and unified processor interface.
    """

    def __init__(
        self,
        processor: ProcessorInterface,
        config: ProcessorConfig,
        executor: Optional[ProcessorExecutor] = None,
    ):
        """
        Initialize the processor handler.

        Args:
            processor: The processor instance to manage
            config: Configuration for processing behavior
            executor: Optional custom processor executor
        """
        self.processor = processor
        self.config = config
        self.executor = executor or ProcessorExecutor()
        self.logger = get_logger()

    def handle_message(self, message) -> dict:
        """
        Handle a message using the configured processor.

        This provides the main entry point for processing messages, similar
        to the old unified processor handler pattern.

        Args:
            message: Message to process (can be dict or Message object)

        Returns:
            Dictionary with processing results for framework integration
        """
        from src.processors.message import Message

        try:
            # Convert dict to Message if needed
            if isinstance(message, dict):
                message = self._convert_dict_to_message(message)
            elif not isinstance(message, Message):
                raise ValueError(f"Unsupported message type: {type(message)}")

            # Execute processor
            result = self.executor.execute_processor(
                processor=self.processor,
                message=message,
                execution_context={"config": self.config},
            )

            # Convert result to dict format for framework compatibility
            return self._convert_result_to_dict(result, message)

        except Exception as e:
            self.logger.error(
                "Error handling message in processor handler",
                extra={
                    "processor_class": self.processor.__class__.__name__,
                    "error": str(e),
                },
                exc_info=True,
            )

            # Return error result in expected format
            return {
                "success": False,
                "error": str(e),
                "processor_class": self.processor.__class__.__name__,
                "can_retry": True,
            }

    def _convert_dict_to_message(self, message_dict: dict):
        """
        Convert dictionary message to Message object.

        Handles compatibility with existing message formats.
        """
        from src.processors.message import Message

        # Extract entity reference information
        entity_ref_data = message_dict.get("entity_reference", {})

        # Create Message from dict
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

    def _convert_result_to_dict(self, result, original_message) -> dict:
        """
        Convert ProcessingResult to dictionary format.

        Maintains compatibility with existing framework expectations.
        """
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

    def _message_to_dict(self, message) -> dict:
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


def create_processor_handler(
    processor_class: Type[ProcessorInterface],
    config: ProcessorConfig,
    entity_service: EntityService,
    entity_repository: EntityRepository,
    processing_service: ProcessingService,
    **processor_kwargs: Any,
) -> ProcessorHandler:
    """
    Create a complete processor handler with all dependencies.

    This is the main factory function for creating processor handlers,
    similar to the old create_source_handler/create_intermediate_handler
    pattern but unified.

    Args:
        processor_class: The processor class to instantiate
        config: Configuration for the processor
        entity_service: Entity service dependency
        entity_repository: Entity repository dependency
        processing_service: Processing service dependency
        **processor_kwargs: Additional processor constructor arguments

    Returns:
        Configured ProcessorHandler ready for message processing
    """
    # Create factory and processor
    factory = ProcessorFactory(
        entity_service=entity_service,
        entity_repository=entity_repository,
        processing_service=processing_service,
    )

    processor = factory.create_processor(
        processor_class=processor_class,
        config=config,
        **processor_kwargs,
    )

    # Create handler
    return ProcessorHandler(
        processor=processor,
        config=config,
    )


# ProcessorError is replaced by ServiceError from src.exceptions
