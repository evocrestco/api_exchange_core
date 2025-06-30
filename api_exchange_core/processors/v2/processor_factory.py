"""
Processor Factory v2 - Creates processor handlers with simplified configuration.

Key improvements from v1:
- db_manager is optional - creates from environment if not provided
- Better error handling with clear missing field messages
- Handles all service creation with sensible defaults
"""

import os
from typing import Any, Dict, Optional

from pydantic import ValidationError as PydanticValidationError

from ...db.db_config import DatabaseConfig, DatabaseManager, init_db, initialize_db, set_db_manager
from ...exceptions import ErrorCode, ServiceError, ValidationError
from ...processing.processing_service import ProcessingService
from ...utils.logger import get_logger
from .processor_handler import ProcessorHandler
from .processor_interface import ProcessorInterface


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

    except PydanticValidationError as e:
        # Extract missing fields for clear error message
        missing_fields = []
        for error in e.errors():
            if error["type"] == "missing":
                field_name = error["loc"][0]
                env_var_name = _field_to_env_var(field_name)
                missing_fields.append(f"{field_name} (env: {env_var_name})")

        if missing_fields:
            raise ServiceError(
                f"Cannot create database manager. Missing required environment variables for fields: "
                f"{', '.join(missing_fields)}. Please set these environment variables.",
                error_code=ErrorCode.CONFIGURATION_ERROR,
                operation="create_db_manager",
                cause=e,
            )
        else:
            # Re-raise if it's not a missing field error
            raise ServiceError(
                f"Database configuration validation failed: {e}",
                error_code=ErrorCode.CONFIGURATION_ERROR,
                operation="create_db_manager",
                cause=e,
            )


def create_processor_handler(
    processor: ProcessorInterface,
    config: Optional[Dict[str, Any]] = None,
    dead_letter_queue_client=None,
) -> ProcessorHandler:
    """
    Create a processor handler with all required services.

    The processor framework handles all infrastructure setup including database initialization.

    Args:
        processor: The processor implementation
        config: Optional configuration dict
        dead_letter_queue_client: Optional DLQ client

    Returns:
        Configured ProcessorHandler

    Raises:
        ValueError: If global db_manager is not initialized and required environment variables are missing
    """
    logger = get_logger()

    # Initialize global db_manager if not already done, or if connection is closed
    try:
        from ...db.db_config import get_db_manager
        existing_db_manager = get_db_manager()
        
        # Check if the connection is still valid
        try:
            # Test the connection by executing a simple query
            from sqlalchemy import text
            with existing_db_manager.engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            logger.debug("Using existing global database manager")
            # Create ProcessingService (uses global db_manager)
            processing_service = ProcessingService()
            
            # Create and return the processor handler
            return ProcessorHandler(
                processor=processor,
                processing_service=processing_service,
                config=config or {},
                dead_letter_queue_client=dead_letter_queue_client,
            )
        except Exception as e:
            logger.warning(f"Existing database connection is invalid ({e}), recreating...")
            # Fall through to recreate the db_manager
            
    except ServiceError:
        logger.warning("Global db_manager not initialized")
        # Fall through to create new db_manager
        
    # Create new db_manager (either not initialized or connection is closed)
    logger.warning("Initializing DB")
    db_manager = create_db_manager()
    logger.info(f"Created database manager from environment: {db_manager.config}")
    
    # Initialize database and import all models
    init_db(db_manager)
    logger.info("Database initialized with all models imported")
    
    # Set as global db_manager
    from ...db.db_config import set_db_manager
    set_db_manager(db_manager)
    logger.info("Global database manager initialized")

    # Create ProcessingService (uses global db_manager)
    processing_service = ProcessingService()

    # Create and return the processor handler
    # Note: All services now use global db_manager
    return ProcessorHandler(
        processor=processor,
        processing_service=processing_service,
        config=config or {},
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
