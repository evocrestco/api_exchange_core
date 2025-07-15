"""
Simple API token utilities using generic CRUD helpers.

This module provides API token operations using the generic CRUD system,
eliminating code duplication.
"""

from datetime import datetime, timezone
from typing import Any, Dict, Optional, Tuple

from sqlalchemy.orm import Session

from ..db.db_api_token_models import APIToken
from ..exceptions import TokenNotAvailableError
from ..utils.crud_helpers import create_record, delete_record, list_records
from ..utils.encryption_utils import encrypt_token, decrypt_token
from ..utils.logger import get_logger


def get_valid_token(session: Session, tenant_id: str, api_provider: str, operation: str = "api_call") -> Tuple[str, str]:
    """
    Get a valid API token for the specified provider and tenant.

    Args:
        session: Database session
        tenant_id: Tenant identifier
        api_provider: API provider name
        operation: Description of operation needing token

    Returns:
        Tuple of (token_value, token_id)

    Raises:
        TokenNotAvailableError: If no valid tokens available
    """
    logger = get_logger()

    # Use generic CRUD to find valid tokens
    all_tokens = list_records(
        session,
        APIToken,
        filters={"api_provider": api_provider, "is_active": True},
        tenant_id=tenant_id,
    )

    # Filter for non-expired tokens (handle timezone-naive comparison for SQLite)
    now_utc = datetime.now(timezone.utc)
    now_naive = now_utc.replace(tzinfo=None)

    valid_tokens = []
    for token in all_tokens:
        # Handle both timezone-aware and timezone-naive datetimes
        token_expires = token.expires_at
        if token_expires.tzinfo is None:
            # SQLite returns naive datetime, compare with naive
            valid_tokens.append(token) if token_expires > now_naive else None
        else:
            # Compare timezone-aware datetimes
            valid_tokens.append(token) if token_expires > now_utc else None

    if not valid_tokens:
        raise TokenNotAvailableError(
            f"No valid tokens available for {api_provider}",
            tenant_id=tenant_id,
            api_provider=api_provider,
        )

    # Use first valid token
    valid_token = valid_tokens[0]

    # Decrypt token value for use
    decrypted_token = decrypt_token(session, valid_token.token_value, tenant_id, api_provider)
    if not decrypted_token:
        raise TokenNotAvailableError(
            f"Failed to decrypt token for {api_provider}",
            tenant_id=tenant_id,
            api_provider=api_provider,
        )

    # Token retrieved successfully - no automatic usage logging

    logger.info(
        f"Token retrieved for {api_provider}",
        extra={
            "tenant_id": tenant_id,
            "api_provider": api_provider,
            "operation": operation,
            "token_id": valid_token.id,
        },
    )

    return decrypted_token, valid_token.id  # type: ignore[return-value]


def store_token(
    session: Session,
    tenant_id: str,
    api_provider: str,
    token_value: str,
    expires_at: datetime,
    token_metadata: Optional[Dict[str, Any]] = None,
) -> str:
    """
    Store a new API token using generic CRUD.

    Args:
        session: Database session
        tenant_id: Tenant identifier
        api_provider: API provider name
        token_value: The token value
        expires_at: Token expiration datetime
        token_metadata: Optional metadata

    Returns:
        Token ID
    """
    # Encrypt token value for secure storage
    encrypted_token = encrypt_token(session, token_value, tenant_id, api_provider)
    
    token_data = {
        "api_provider": api_provider,
        "token_value": encrypted_token,
        "expires_at": expires_at,
        "is_active": True,
        "context": token_metadata or {},
    }

    new_token = create_record(session, APIToken, token_data, tenant_id)
    return new_token.id  # type: ignore[return-value]


def cleanup_expired_tokens(session: Session, tenant_id: Optional[str] = None, api_provider: Optional[str] = None) -> int:
    """
    Clean up expired tokens using generic CRUD.

    Args:
        session: Database session
        tenant_id: Optional tenant filter
        api_provider: Optional provider filter

    Returns:
        Number of tokens cleaned up
    """
    logger = get_logger()

    # Get all tokens for tenant/provider
    filters = {}
    if api_provider:
        filters["api_provider"] = api_provider

    all_tokens = list_records(session, APIToken, filters, tenant_id)

    # Find expired tokens (handle timezone-naive comparison for SQLite)
    now_utc = datetime.now(timezone.utc)
    now_naive = now_utc.replace(tzinfo=None)

    expired_tokens = []
    for token in all_tokens:
        # Handle both timezone-aware and timezone-naive datetimes
        token_expires = token.expires_at
        if token_expires.tzinfo is None:
            # SQLite returns naive datetime, compare with naive
            if token_expires < now_naive:
                expired_tokens.append(token)
        else:
            # Compare timezone-aware datetimes
            if token_expires < now_utc:
                expired_tokens.append(token)

    # Delete expired tokens using generic CRUD
    deleted_count = 0
    for token in expired_tokens:
        if delete_record(session, APIToken, token.id, tenant_id):  # type: ignore[arg-type]
            deleted_count += 1

    logger.info(
        f"Cleaned up {deleted_count} expired tokens",
        extra={
            "tenant_id": tenant_id,
            "api_provider": api_provider,
            "deleted_count": deleted_count,
        },
    )

    return deleted_count


def get_token_statistics(session: Session, tenant_id: str, api_provider: Optional[str] = None) -> Dict[str, Any]:
    """
    Get token statistics for a tenant using generic CRUD.

    Args:
        session: Database session
        tenant_id: Tenant identifier
        api_provider: Optional provider filter

    Returns:
        Statistics dictionary
    """
    filters = {}
    if api_provider:
        filters["api_provider"] = api_provider

    # Get all tokens using generic CRUD
    all_tokens = list_records(session, APIToken, filters, tenant_id)

    # Calculate stats (handle timezone-naive comparison for SQLite)
    now_utc = datetime.now(timezone.utc)
    now_naive = now_utc.replace(tzinfo=None)

    total_tokens = len(all_tokens)
    active_tokens = len([t for t in all_tokens if t.is_active])

    # Count expired tokens with timezone handling
    expired_count = 0
    for token in all_tokens:
        token_expires = token.expires_at
        if token_expires.tzinfo is None:
            # SQLite returns naive datetime, compare with naive
            if token_expires < now_naive:
                expired_count += 1
        else:
            # Compare timezone-aware datetimes
            if token_expires < now_utc:
                expired_count += 1

    expired_tokens = expired_count

    return {
        "total_tokens": total_tokens,
        "active_tokens": active_tokens,
        "expired_tokens": expired_tokens,
        "tenant_id": tenant_id,
        "api_provider": api_provider,
    }
