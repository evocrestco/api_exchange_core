"""
Processor factory and handler creation patterns.

This module provides factory patterns and helper functions for creating
and configuring processors with proper dependency injection.
"""

from typing import Any, Optional, Type, TypeVar

from src.exceptions import ErrorCode
from src.processing.processing_service import ProcessingService
from src.processing.processor_config import ProcessorConfig
from src.processors.processor_interface import ProcessorInterface
from src.processors.processor_handler import ProcessorHandler
from src.repositories.entity_repository import EntityRepository
from src.services.entity_service import EntityService
from src.services.processing_error_service import ProcessingErrorService
from src.services.state_tracking_service import StateTrackingService
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


def create_processor_handler(
    processor_class: Type[ProcessorInterface],
    config: ProcessorConfig,
    entity_service: EntityService,
    entity_repository: EntityRepository,
    processing_service: ProcessingService,
    state_tracking_service: Optional[StateTrackingService] = None,
    error_service: Optional[ProcessingErrorService] = None,
    **processor_kwargs: Any,
) -> ProcessorHandler:
    """
    Create a complete processor handler with all dependencies.

    This is the main factory function for creating processor handlers with
    full framework integration including entity persistence, state tracking,
    and error recording.

    Args:
        processor_class: The processor class to instantiate
        config: Configuration for the processor
        entity_service: Entity service dependency
        entity_repository: Entity repository dependency
        processing_service: Processing service dependency
        state_tracking_service: Optional state tracking service
        error_service: Optional error recording service
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

    # Create handler with full framework integration
    return ProcessorHandler(
        processor=processor,
        config=config,
        processing_service=processing_service,
        state_tracking_service=state_tracking_service,
        error_service=error_service,
    )


# ProcessorError is replaced by ServiceError from src.exceptions