"""
Simple credential utilities using generic CRUD helpers and Pydantic schemas.

This module provides credential operations using the generic CRUD system
and Pydantic schemas for type safety and validation.
"""

from datetime import datetime, timezone
from typing import Any, Dict

from sqlalchemy.orm import Session

from ..db.db_credential_models import ExternalCredential
from ..exceptions import CredentialNotFoundError
from ..schemas.credential_schemas import (
    ExternalCredentialCreate,
    ExternalCredentialRead,
    ExternalCredentialUpdate,
    deserialize_credentials,
    serialize_credentials,
)
from ..utils.crud_helpers import create_record, delete_record, get_record, update_record


def get_credentials(session: Session, tenant_id: str, system_name: str) -> ExternalCredentialRead:
    """
    Get credentials for a specific external system using generic CRUD.

    Args:
        session: Database session
        tenant_id: Tenant identifier
        system_name: Name of the external system

    Returns:
        ExternalCredentialRead schema with typed credential data

    Raises:
        CredentialNotFoundError: If credentials not found
    """
    # Use generic CRUD to find credential
    credential = get_record(session, ExternalCredential, {"system_name": system_name}, tenant_id)

    if not credential:
        raise CredentialNotFoundError(
            f"Credentials not found for system: {system_name}",
            tenant_id=tenant_id,
            system_name=system_name,
        )

    # Check if credentials are expired (handle timezone-naive comparison for SQLite)
    if credential.expires_at:
        now_utc = datetime.now(timezone.utc)
        expires_at = credential.expires_at

        # Handle both timezone-aware and timezone-naive datetimes
        if expires_at.tzinfo is None:
            # SQLite returns naive datetime, compare with naive
            now_naive = now_utc.replace(tzinfo=None)
            is_expired = expires_at < now_naive
        else:
            # Compare timezone-aware datetimes
            is_expired = expires_at < now_utc

        if is_expired:
            raise CredentialNotFoundError(
                f"Credentials expired for system: {system_name}",
                tenant_id=tenant_id,
                system_name=system_name,
            )

    # Deserialize credential data and return as Pydantic schema
    try:
        # Assume credential_data is stored as JSON string
        credential_data = deserialize_credentials(credential.credential_data)  # type: ignore[arg-type]
    except Exception as e:
        raise CredentialNotFoundError(
            f"Failed to deserialize credentials for system: {system_name}",
            tenant_id=tenant_id,
            system_name=system_name,
            error=str(e),
        )

    return ExternalCredentialRead(
        id=credential.id,  # type: ignore[arg-type]
        system_name=credential.system_name,  # type: ignore[arg-type]
        credential_data=credential_data,
        expires_at=credential.expires_at,  # type: ignore[arg-type]
        context=credential.context,  # type: ignore[arg-type]
        created_at=credential.created_at,  # type: ignore[arg-type]
        updated_at=credential.updated_at,  # type: ignore[arg-type]
    )


def store_credentials(
    session: Session,
    tenant_id: str,
    credential_create: ExternalCredentialCreate,
) -> str:
    """
    Store new credentials using generic CRUD and Pydantic validation.

    Args:
        session: Database session
        tenant_id: Tenant identifier
        credential_create: ExternalCredentialCreate schema with validated data

    Returns:
        Credential ID
    """
    # Serialize credential data to JSON string
    serialized_data = serialize_credentials(credential_create.credential_data)

    # Check if credentials already exist using generic CRUD
    existing = get_record(
        session, ExternalCredential, {"system_name": credential_create.system_name}, tenant_id
    )

    if existing:
        # Update existing using generic CRUD
        update_data = {
            "credential_data": serialized_data,
            "expires_at": credential_create.expires_at,
            "context": credential_create.context,
        }
        updated = update_record(session, ExternalCredential, existing.id, update_data, tenant_id)  # type: ignore[arg-type]
        return updated.id  # type: ignore[return-value,union-attr]
    else:
        # Create new using generic CRUD
        new_credential_data = {
            "system_name": credential_create.system_name,
            "credential_data": serialized_data,
            "expires_at": credential_create.expires_at,
            "context": credential_create.context,
        }
        new_credential = create_record(session, ExternalCredential, new_credential_data, tenant_id)
        return new_credential.id  # type: ignore[return-value]


def update_credentials(
    session: Session,
    tenant_id: str,
    system_name: str,
    credential_update: ExternalCredentialUpdate,
) -> bool:
    """
    Update existing credentials using generic CRUD and Pydantic validation.

    Args:
        session: Database session
        tenant_id: Tenant identifier
        system_name: Name of the external system
        credential_update: ExternalCredentialUpdate schema with validated data

    Returns:
        True if updated, False if not found
    """
    # Find existing credential using generic CRUD
    existing = get_record(session, ExternalCredential, {"system_name": system_name}, tenant_id)

    if not existing:
        return False

    # Prepare update data, only include fields that are provided
    update_data: Dict[str, Any] = {}

    if credential_update.credential_data is not None:
        update_data["credential_data"] = serialize_credentials(credential_update.credential_data)

    if credential_update.expires_at is not None:
        update_data["expires_at"] = credential_update.expires_at

    if credential_update.context is not None:
        update_data["context"] = credential_update.context

    # Update using generic CRUD
    updated = update_record(session, ExternalCredential, existing.id, update_data, tenant_id)  # type: ignore[arg-type]
    return updated is not None


def delete_credentials(session: Session, tenant_id: str, system_name: str) -> bool:
    """
    Delete credentials using generic CRUD.

    Args:
        session: Database session
        tenant_id: Tenant identifier
        system_name: Name of the external system

    Returns:
        True if deleted, False if not found
    """
    # Find existing credential using generic CRUD
    existing = get_record(session, ExternalCredential, {"system_name": system_name}, tenant_id)

    if not existing:
        return False

    # Delete using generic CRUD
    return delete_record(session, ExternalCredential, existing.id, tenant_id)  # type: ignore[arg-type]
