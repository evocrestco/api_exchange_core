"""
Processor Factory v2 - Creates processor handlers with simplified configuration.

Key improvements from v1:
- db_manager is optional - creates from environment if not provided
- Better error handling with clear missing field messages
- Handles all service creation with sensible defaults
"""

import os
from typing import Any, Dict, Optional

from pydantic import ValidationError

from src.db.db_config import DatabaseConfig, DatabaseManager, init_db
from src.processing.duplicate_detection import DuplicateDetectionService
from src.processing.entity_attributes import EntityAttributeBuilder
from src.processing.processing_service import ProcessingService
from src.processors.v2.processor_handler import ProcessorHandler
from src.processors.v2.processor_interface import ProcessorInterface
from src.repositories.entity_repository import EntityRepository
from src.services.entity_service import EntityService
from src.utils.logger import get_logger


def create_db_manager() -> DatabaseManager:
    """
    Create DatabaseManager from environment variables.

    Returns:
        Configured DatabaseManager

    Raises:
        ValueError: If required environment variables are missing with clear field names
    """
    try:
        db_config = DatabaseConfig(
            db_type=os.getenv("DB_TYPE", "postgres"),
            database=os.getenv("DB_NAME"),
            host=os.getenv("DB_HOST"),
            port=os.getenv("DB_PORT", "5432"),
            username=os.getenv("DB_USER"),
            password=os.getenv("DB_PASSWORD"),
        )
        return DatabaseManager(db_config)

    except ValidationError as e:
        # Extract missing fields for clear error message
        missing_fields = []
        for error in e.errors():
            if error["type"] == "missing":
                field_name = error["loc"][0]
                env_var_name = _field_to_env_var(field_name)
                missing_fields.append(f"{field_name} (env: {env_var_name})")

        if missing_fields:
            raise ValueError(
                f"Cannot create database manager. Missing required environment variables for fields: "
                f"{', '.join(missing_fields)}. Please set these environment variables."
            ) from e
        else:
            # Re-raise if it's not a missing field error
            raise ValueError(f"Database configuration validation failed: {e}") from e


def create_processor_handler(
    processor: ProcessorInterface,
    db_manager: Optional[DatabaseManager] = None,
    config: Optional[Dict[str, Any]] = None,
    state_tracking_service=None,
    error_service=None,
    dead_letter_queue_client=None,
) -> ProcessorHandler:
    """
    Create a processor handler with all required services.

    Args:
        processor: The processor implementation
        db_manager: Optional database manager. If not provided, will create from environment
        config: Optional configuration dict
        state_tracking_service: Optional state tracking service
        error_service: Optional error service
        dead_letter_queue_client: Optional DLQ client

    Returns:
        Configured ProcessorHandler

    Raises:
        ValueError: If db_manager is None and required environment variables are missing
    """
    logger = get_logger()

    # Create db_manager from environment if not provided
    if db_manager is None:
        db_manager = create_db_manager()
        logger.info(f"Created database manager from environment: {db_manager.config}")

    # Initialize database and import all models
    init_db(db_manager)
    logger.info("Database initialized with all models imported")

    # Create session from db_manager
    session = db_manager.get_session()

    # Create repositories using session
    entity_repository = EntityRepository(session)
    entity_service = EntityService(entity_repository)
    duplicate_detection_service = DuplicateDetectionService(entity_repository)
    attribute_builder = EntityAttributeBuilder()
    processing_service = ProcessingService(
        entity_service, duplicate_detection_service, attribute_builder
    )

    # Create and return the processor handler
    return ProcessorHandler(
        processor=processor,
        processing_service=processing_service,
        config=config or {},
        state_tracking_service=state_tracking_service,
        error_service=error_service,
        dead_letter_queue_client=dead_letter_queue_client,
    )


def _field_to_env_var(field_name: str) -> str:
    """Convert DatabaseConfig field name to environment variable name."""
    field_to_env = {
        "db_type": "DB_TYPE",
        "database": "DB_NAME",
        "host": "DB_HOST",
        "port": "DB_PORT",
        "username": "DB_USER",
        "password": "DB_PASSWORD",
    }
    return field_to_env.get(field_name, f"DB_{field_name.upper()}")
