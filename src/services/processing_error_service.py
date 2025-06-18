"""
Pythonic service for processing error operations with direct SQLAlchemy access.

This module provides business logic for managing processing errors
using SQLAlchemy directly - simple, explicit, and efficient.
"""

import uuid
import logging
from datetime import datetime
from typing import List, Optional

from sqlalchemy import and_, exists
from sqlalchemy.exc import IntegrityError
from pydantic import ValidationError as PydanticValidationError

from src.context.operation_context import operation
from src.db.db_entity_models import Entity
from src.db.db_error_models import ProcessingError
from src.db.db_tenant_models import Tenant
from src.exceptions import ErrorCode, ServiceError, ValidationError
from src.schemas.processing_error_schema import (
    ProcessingErrorCreate,
    ProcessingErrorFilter,
    ProcessingErrorRead,
)
from src.services.base_service import SessionManagedService


class ProcessingErrorService(SessionManagedService):
    """
    Pythonic service for processing error management with direct SQLAlchemy access.
    
    Uses SQLAlchemy directly - simple, explicit, and efficient.
    """

    def __init__(self, session=None, logger: Optional[logging.Logger] = None):
        """
        Initialize the processing error service with its own session.

        Args:
            session: Optional existing session (for testing or coordination)
            logger: Optional logger instance
        """
        super().__init__(session=session, logger=logger)

    @operation()
    def create_error(self, error_data: ProcessingErrorCreate) -> str:
        """
        Create a new processing error.

        Args:
            error_data: Validated error data

        Returns:
            ID of the created error

        Raises:
            ServiceError: If there's an error during creation
        """
        try:
            tenant_id = self._get_current_tenant_id()
            
            # Explicit entity validation - Pythonic approach
            if error_data.entity_id and not self.session.query(exists().where(
                and_(
                    Entity.id == error_data.entity_id,
                    Entity.tenant_id == tenant_id
                )
            )).scalar():
                raise ServiceError(
                    f"Invalid entity: {error_data.entity_id}",
                    error_code=ErrorCode.CONSTRAINT_VIOLATION,
                    operation="create_error",
                    entity_id=error_data.entity_id,
                    tenant_id=tenant_id,
                )
            
            # Create processing error using validated schema data
            processing_error = ProcessingError(
                id=str(uuid.uuid4()),
                entity_id=error_data.entity_id,
                error_type_code=error_data.error_type_code,
                message=error_data.message,
                processing_step=error_data.processing_step,
                stack_trace=error_data.stack_trace,
                tenant_id=tenant_id,
                created_at=datetime.utcnow(),
                updated_at=datetime.utcnow(),
            )

            self.session.add(processing_error)
            # Transaction managed by caller
            
            self.logger.info(
                f"Created processing error for entity {error_data.entity_id}: "
                f"{error_data.error_type_code}",
                extra={
                    "entity_id": error_data.entity_id,
                    "error_type": error_data.error_type_code,
                    "processing_step": error_data.processing_step,
                },
            )
            
            return processing_error.id
            
        except PydanticValidationError as e:
            # Handle Pydantic validation errors
            raise ValidationError(
                f"Invalid processing error data: {str(e)}",
                details={"validation_errors": e.errors()},
            ) from e
        except IntegrityError as e:
            # Transaction managed by caller
            # Check if it's a foreign key constraint (entity validation)
            if "foreign key constraint" in str(e).lower():
                raise ServiceError(
                    f"Invalid entity: {error_data.entity_id}",
                    error_code=ErrorCode.CONSTRAINT_VIOLATION,
                    operation="create_error",
                    entity_id=error_data.entity_id,
                    tenant_id=tenant_id,
                    cause=e,
                ) from e
            else:
                raise ServiceError(
                    f"Processing error creation failed due to data integrity constraints",
                    error_code=ErrorCode.INVALID_DATA,
                    operation="create_error",
                    entity_id=error_data.entity_id,
                    tenant_id=tenant_id,
                    cause=e,
                ) from e
        except Exception as e:
            # Transaction managed by caller
            if isinstance(e, ServiceError):
                raise
            self._handle_service_exception("create_error", e)

    @operation()
    def get_error(self, error_id: str) -> ProcessingErrorRead:
        """
        Get a processing error by ID.

        Args:
            error_id: ID of the error to retrieve

        Returns:
            ProcessingError data

        Raises:
            ServiceError: If the error doesn't exist or there's an error during retrieval
        """
        try:
            tenant_id = self._get_current_tenant_id()
            
            # Query processing error directly with SQLAlchemy
            processing_error = (
                self.session.query(ProcessingError)
                .filter(
                    ProcessingError.id == error_id,
                    ProcessingError.tenant_id == tenant_id,
                )
                .first()
            )

            if processing_error is None:
                raise ServiceError(
                    f"Processing error not found: error_id={error_id}",
                    error_code=ErrorCode.NOT_FOUND,
                    entity_id=error_id,
                    tenant_id=tenant_id,
                )

            # Convert to ProcessingErrorRead
            return ProcessingErrorRead.model_validate(processing_error)

        except Exception as e:
            if isinstance(e, ServiceError):
                raise
            self._handle_service_exception("get_error", e, error_id)

    @operation()
    def get_entity_errors(self, entity_id: str) -> List[ProcessingErrorRead]:
        """
        Get all errors for a specific entity.

        Args:
            entity_id: ID of the entity

        Returns:
            List of processing errors

        Raises:
            ServiceError: If there's an error during retrieval
        """
        try:
            tenant_id = self._get_current_tenant_id()
            
            # Query processing errors for entity directly with SQLAlchemy
            processing_errors = (
                self.session.query(ProcessingError)
                .filter(
                    ProcessingError.entity_id == entity_id,
                    ProcessingError.tenant_id == tenant_id,
                )
                .order_by(ProcessingError.created_at.desc())
                .all()
            )

            # Convert to ProcessingErrorRead objects
            return [ProcessingErrorRead.model_validate(error) for error in processing_errors]
            
        except Exception as e:
            self._handle_service_exception("get_entity_errors", e, entity_id)

    @operation()
    def record_error(
        self, 
        entity_id: str, 
        error_type: str, 
        message: str, 
        processing_step: str, 
        stack_trace: Optional[str] = None
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
            ID of the created error

        Raises:
            ServiceError: If there's an error during creation
        """
        try:
            # Create ProcessingErrorCreate schema from parameters
            error_data = ProcessingErrorCreate(
                entity_id=entity_id,
                error_type_code=error_type,
                message=message,
                processing_step=processing_step,
                stack_trace=stack_trace,
            )
            
            return self.create_error(error_data)
            
        except Exception as e:
            if isinstance(e, ServiceError):
                raise
            self._handle_service_exception("record_error", e, entity_id)

    @operation()
    def find_errors(
        self, 
        filter_params: ProcessingErrorFilter, 
        limit: Optional[int] = None, 
        offset: Optional[int] = None
    ) -> List[ProcessingErrorRead]:
        """
        Find processing errors based on filter criteria.

        Args:
            filter_params: Filter parameters
            limit: Maximum number of errors to return
            offset: Number of errors to skip

        Returns:
            List of processing errors matching the criteria

        Raises:
            ServiceError: If there's an error during retrieval
        """
        try:
            tenant_id = self._get_current_tenant_id()
            
            # Build base query
            query = self.session.query(ProcessingError).filter(
                ProcessingError.tenant_id == tenant_id
            )
            
            # Apply filters
            if filter_params.entity_id:
                query = query.filter(ProcessingError.entity_id == filter_params.entity_id)
            
            if filter_params.error_type_code:
                query = query.filter(ProcessingError.error_type_code == filter_params.error_type_code)
            
            if filter_params.processing_step:
                query = query.filter(ProcessingError.processing_step == filter_params.processing_step)
            
            # Apply date range filters if available
            if hasattr(filter_params, 'created_after') and filter_params.created_after:
                query = query.filter(ProcessingError.created_at >= filter_params.created_after)
            
            if hasattr(filter_params, 'created_before') and filter_params.created_before:
                query = query.filter(ProcessingError.created_at <= filter_params.created_before)
            
            # Order by creation time (newest first)
            query = query.order_by(ProcessingError.created_at.desc())
            
            # Apply pagination
            if offset:
                query = query.offset(offset)
            if limit:
                query = query.limit(limit)
            
            # Execute query and convert to schema objects
            processing_errors = query.all()
            return [ProcessingErrorRead.model_validate(error) for error in processing_errors]
            
        except Exception as e:
            self._handle_service_exception("find_errors", e)

    @operation()
    def delete_error(self, error_id: str) -> bool:
        """
        Delete a processing error by ID.

        Args:
            error_id: ID of the error to delete

        Returns:
            True if deleted, False if not found

        Raises:
            ServiceError: If there's an error during deletion
        """
        try:
            tenant_id = self._get_current_tenant_id()
            
            # Find the error
            processing_error = (
                self.session.query(ProcessingError)
                .filter(
                    ProcessingError.id == error_id,
                    ProcessingError.tenant_id == tenant_id,
                )
                .first()
            )
            
            if not processing_error:
                return False
            
            # Delete the error
            self.session.delete(processing_error)
            # Transaction managed by caller
            
            self.logger.info(f"Deleted processing error: id={error_id}")
            
            return True
            
        except Exception as e:
            # Transaction managed by caller
            if isinstance(e, ServiceError):
                raise
            self._handle_service_exception("delete_error", e, error_id)

    @operation()
    def delete_entity_errors(self, entity_id: str) -> int:
        """
        Delete all processing errors for an entity.

        Args:
            entity_id: ID of the entity

        Returns:
            Number of errors deleted

        Raises:
            ServiceError: If there's an error during deletion
        """
        try:
            tenant_id = self._get_current_tenant_id()
            
            # Count errors before deletion
            error_count = (
                self.session.query(ProcessingError)
                .filter(
                    ProcessingError.entity_id == entity_id,
                    ProcessingError.tenant_id == tenant_id,
                )
                .count()
            )
            
            if error_count == 0:
                return 0
            
            # Delete all errors for the entity
            self.session.query(ProcessingError).filter(
                ProcessingError.entity_id == entity_id,
                ProcessingError.tenant_id == tenant_id,
            ).delete()
            # Transaction managed by caller
            
            self.logger.info(f"Deleted {error_count} processing errors for entity: {entity_id}")
            
            return error_count
            
        except Exception as e:
            # Transaction managed by caller
            if isinstance(e, ServiceError):
                raise
            self._handle_service_exception("delete_entity_errors", e, entity_id)