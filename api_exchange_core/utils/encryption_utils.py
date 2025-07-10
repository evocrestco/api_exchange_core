"""
Simple encryption utilities for token and credential storage.

Handles cross-database encryption (PostgreSQL pgcrypto, SQLite plaintext for testing).
"""

from typing import Optional

from sqlalchemy import text
from sqlalchemy.orm import Session


def encrypt_value(session: Session, value: str, tenant_id: str, key_suffix: str = "") -> bytes:
    """
    Encrypt a value using database-specific encryption.

    Args:
        session: Database session
        value: Value to encrypt
        tenant_id: Tenant ID for key isolation
        key_suffix: Additional key suffix for different data types

    Returns:
        Encrypted bytes
    """
    if session.bind.dialect.name == "postgresql":
        # Use tenant-specific key with pgcrypto
        encryption_key = f"{tenant_id}_{key_suffix}" if key_suffix else tenant_id

        result = session.execute(
            text("SELECT pgp_sym_encrypt(:data, :key)"), {"data": value, "key": encryption_key}
        ).scalar()

        return result
    else:
        # SQLite for testing - return as-is
        return value.encode() if isinstance(value, str) else value


def decrypt_value(
    session: Session, encrypted_value: bytes, tenant_id: str, key_suffix: str = ""
) -> Optional[str]:
    """
    Decrypt a value using database-specific decryption.

    Args:
        session: Database session
        encrypted_value: Encrypted bytes
        tenant_id: Tenant ID for key isolation
        key_suffix: Additional key suffix for different data types

    Returns:
        Decrypted string or None
    """
    if not encrypted_value:
        return None

    if session.bind.dialect.name == "postgresql":
        # Use same tenant-specific key for decryption
        encryption_key = f"{tenant_id}_{key_suffix}" if key_suffix else tenant_id

        result = session.execute(
            text("SELECT pgp_sym_decrypt(:data, :key)"),
            {"data": encrypted_value, "key": encryption_key},
        ).scalar()

        return result
    else:
        # SQLite for testing
        if isinstance(encrypted_value, bytes):
            return encrypted_value.decode()
        return encrypted_value


def encrypt_token(session: Session, token: str, tenant_id: str, api_provider: str) -> bytes:
    """Encrypt an API token with tenant and provider isolation."""
    return encrypt_value(session, token, tenant_id, f"token_{api_provider}")


def decrypt_token(
    session: Session, encrypted: bytes, tenant_id: str, api_provider: str
) -> Optional[str]:
    """Decrypt an API token."""
    return decrypt_value(session, encrypted, tenant_id, f"token_{api_provider}")


def encrypt_credential(
    session: Session, credential: str, tenant_id: str, system_name: str
) -> bytes:
    """Encrypt credentials with tenant and system isolation."""
    return encrypt_value(session, credential, tenant_id, f"cred_{system_name}")


def decrypt_credential(
    session: Session, encrypted: bytes, tenant_id: str, system_name: str
) -> Optional[str]:
    """Decrypt credentials."""
    return decrypt_value(session, encrypted, tenant_id, f"cred_{system_name}")
