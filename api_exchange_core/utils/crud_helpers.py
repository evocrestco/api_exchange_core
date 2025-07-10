"""
Generic CRUD helpers to eliminate utility function duplication.

This module provides generic functions that work with any SQLAlchemy model,
eliminating the need for separate utility files for each domain object.
"""

from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Type, TypeVar

from sqlalchemy.orm import Session

from ..exceptions import BaseError, ErrorCode, not_found
from ..utils.logger import get_logger

T = TypeVar("T")


def create_record(
    session: Session, model_class: Type[T], data: Dict[str, Any], tenant_id: Optional[str] = None
) -> T:
    """
    Generic create operation for any model.

    Args:
        session: Database session
        model_class: SQLAlchemy model class
        data: Data dictionary
        tenant_id: Optional tenant ID to add

    Returns:
        Created record instance

    Raises:
        BaseError: If creation fails
    """
    logger = get_logger()

    try:
        # Add tenant_id if provided and model has tenant_id field and it's not already set
        if tenant_id and hasattr(model_class, "tenant_id") and "tenant_id" not in data:
            data["tenant_id"] = tenant_id

        # Add timestamps if model has them
        if hasattr(model_class, "created_at"):
            data["created_at"] = datetime.now(timezone.utc)
        if hasattr(model_class, "updated_at"):
            data["updated_at"] = datetime.now(timezone.utc)

        # Create record
        record = model_class(**data)
        session.add(record)
        session.commit()

        logger.info(
            f"Created {model_class.__name__}",
            extra={
                "model": model_class.__name__,
                "record_id": getattr(record, "id", None),
                "tenant_id": tenant_id,
            },
        )

        return record

    except Exception as e:
        session.rollback()
        logger.error(
            f"Failed to create {model_class.__name__}: {str(e)}",
            extra={"model": model_class.__name__, "error": str(e), "tenant_id": tenant_id},
        )
        raise BaseError(
            f"Failed to create {model_class.__name__}: {str(e)}",
            error_code=ErrorCode.DATABASE_ERROR,
            cause=e,
        )


def get_record(
    session: Session, model_class: Type[T], filters: Dict[str, Any], tenant_id: Optional[str] = None
) -> Optional[T]:
    """
    Generic get operation for any model.

    Args:
        session: Database session
        model_class: SQLAlchemy model class
        filters: Filter conditions
        tenant_id: Optional tenant ID filter

    Returns:
        Record instance or None
    """
    query = session.query(model_class)

    # Add tenant filter if provided and model has tenant_id
    if tenant_id and hasattr(model_class, "tenant_id"):
        query = query.filter(model_class.tenant_id == tenant_id)  # type: ignore[attr-defined]

    # Apply filters
    for key, value in filters.items():
        if hasattr(model_class, key) and value is not None:
            query = query.filter(getattr(model_class, key) == value)

    return query.first()


def get_record_by_id(
    session: Session, model_class: Type[T], record_id: str, tenant_id: Optional[str] = None
) -> Optional[T]:
    """
    Generic get by ID operation.

    Args:
        session: Database session
        model_class: SQLAlchemy model class
        record_id: Record ID
        tenant_id: Optional tenant ID filter

    Returns:
        Record instance or None
    """
    return get_record(session, model_class, {"id": record_id}, tenant_id)


def update_record(
    session: Session,
    model_class: Type[T],
    record_id: str,
    data: Dict[str, Any],
    tenant_id: Optional[str] = None,
) -> Optional[T]:
    """
    Generic update operation for any model.

    Args:
        session: Database session
        model_class: SQLAlchemy model class
        record_id: Record ID to update
        data: Update data dictionary
        tenant_id: Optional tenant ID filter

    Returns:
        Updated record instance or None if not found

    Raises:
        BaseError: If update fails
    """
    logger = get_logger()

    # Find record
    record = get_record_by_id(session, model_class, record_id, tenant_id)
    if not record:
        raise not_found(model_class.__name__, record_id=record_id, tenant_id=tenant_id)

    try:

        # Update fields
        for key, value in data.items():
            if hasattr(record, key) and value is not None:
                setattr(record, key, value)

        # Update timestamp if model has it
        if hasattr(record, "updated_at"):
            record.updated_at = datetime.now(timezone.utc)

        session.commit()

        logger.info(
            f"Updated {model_class.__name__}",
            extra={"model": model_class.__name__, "record_id": record_id, "tenant_id": tenant_id},
        )

        return record

    except Exception as e:
        session.rollback()
        logger.error(
            f"Failed to update {model_class.__name__}: {str(e)}",
            extra={
                "model": model_class.__name__,
                "record_id": record_id,
                "error": str(e),
                "tenant_id": tenant_id,
            },
        )
        raise BaseError(
            f"Failed to update {model_class.__name__}: {str(e)}",
            error_code=ErrorCode.DATABASE_ERROR,
            cause=e,
        )


def delete_record(
    session: Session, model_class: Type[T], record_id: str, tenant_id: Optional[str] = None
) -> bool:
    """
    Generic delete operation for any model.

    Args:
        session: Database session
        model_class: SQLAlchemy model class
        record_id: Record ID to delete
        tenant_id: Optional tenant ID filter

    Returns:
        True if deleted, False if not found

    Raises:
        BaseError: If delete fails
    """
    logger = get_logger()

    try:
        # Find record
        record = get_record_by_id(session, model_class, record_id, tenant_id)
        if not record:
            return False

        session.delete(record)
        session.commit()

        logger.info(
            f"Deleted {model_class.__name__}",
            extra={"model": model_class.__name__, "record_id": record_id, "tenant_id": tenant_id},
        )

        return True

    except Exception as e:
        session.rollback()
        logger.error(
            f"Failed to delete {model_class.__name__}: {str(e)}",
            extra={
                "model": model_class.__name__,
                "record_id": record_id,
                "error": str(e),
                "tenant_id": tenant_id,
            },
        )
        raise BaseError(
            f"Failed to delete {model_class.__name__}: {str(e)}",
            error_code=ErrorCode.DATABASE_ERROR,
            cause=e,
        )


def list_records(
    session: Session,
    model_class: Type[T],
    filters: Optional[Dict[str, Any]] = None,
    tenant_id: Optional[str] = None,
    limit: Optional[int] = None,
    offset: Optional[int] = None,
    order_by: Optional[str] = None,
) -> List[T]:
    """
    Generic list operation for any model.

    Args:
        session: Database session
        model_class: SQLAlchemy model class
        filters: Optional filter conditions
        tenant_id: Optional tenant ID filter
        limit: Optional limit
        offset: Optional offset
        order_by: Optional order by field

    Returns:
        List of record instances
    """
    query = session.query(model_class)

    # Add tenant filter if provided and model has tenant_id
    if tenant_id and hasattr(model_class, "tenant_id"):
        query = query.filter(model_class.tenant_id == tenant_id)  # type: ignore[attr-defined]

    # Apply filters
    if filters:
        for key, value in filters.items():
            if hasattr(model_class, key) and value is not None:
                query = query.filter(getattr(model_class, key) == value)

    # Apply ordering
    if order_by and hasattr(model_class, order_by):
        query = query.order_by(getattr(model_class, order_by))
    elif hasattr(model_class, "created_at"):
        query = query.order_by(model_class.created_at.desc())  # type: ignore[attr-defined]

    # Apply pagination
    if offset:
        query = query.offset(offset)
    if limit:
        query = query.limit(limit)

    return query.all()


def count_records(
    session: Session,
    model_class: Type[T],
    filters: Optional[Dict[str, Any]] = None,
    tenant_id: Optional[str] = None,
) -> int:
    """
    Generic count operation for any model.

    Args:
        session: Database session
        model_class: SQLAlchemy model class
        filters: Optional filter conditions
        tenant_id: Optional tenant ID filter

    Returns:
        Count of records
    """
    query = session.query(model_class)

    # Add tenant filter if provided and model has tenant_id
    if tenant_id and hasattr(model_class, "tenant_id"):
        query = query.filter(model_class.tenant_id == tenant_id)  # type: ignore[attr-defined]

    # Apply filters
    if filters:
        for key, value in filters.items():
            if hasattr(model_class, key) and value is not None:
                query = query.filter(getattr(model_class, key) == value)

    return query.count()


def record_exists(
    session: Session, model_class: Type[T], filters: Dict[str, Any], tenant_id: Optional[str] = None
) -> bool:
    """
    Check if a record exists with the given filters.

    Args:
        session: Database session
        model_class: SQLAlchemy model class
        filters: Filter conditions
        tenant_id: Optional tenant ID filter

    Returns:
        True if record exists, False otherwise
    """
    return get_record(session, model_class, filters, tenant_id) is not None
