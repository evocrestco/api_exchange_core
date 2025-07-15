"""
Simplified base models for V2 framework.

Keeps necessary cross-database compatibility (SQLite/PostgreSQL)
but removes Java-style patterns and business logic.
"""

import json
import uuid
from datetime import UTC, datetime

from pydantic_core import to_jsonable_python
from sqlalchemy import Column, DateTime, String, Text
from sqlalchemy.dialects.postgresql import BYTEA, JSONB
from sqlalchemy.types import TypeDecorator


def utc_now():
    """Return current UTC time with timezone info attached."""
    return datetime.now(UTC)


class JSON(TypeDecorator):
    """Cross-database JSON type for SQLite/PostgreSQL compatibility."""

    impl = Text
    cache_ok = True

    def load_dialect_impl(self, dialect):
        if dialect.name == "postgresql":
            return dialect.type_descriptor(JSONB())
        else:
            return dialect.type_descriptor(Text())

    def process_bind_param(self, value, dialect):
        if value is None:
            return None
        if dialect.name == "postgresql":
            return value
        else:
            return json.dumps(to_jsonable_python(value))

    def process_result_value(self, value, dialect):
        if value is None:
            return None
        if dialect.name == "postgresql":
            return value
        else:
            import json

            return json.loads(value)


class EncryptedBinary(TypeDecorator):
    """
    Cross-database encrypted binary type.
    Uses BYTEA for PostgreSQL and Text for SQLite.
    """

    impl = Text
    cache_ok = True

    def load_dialect_impl(self, dialect):
        if dialect.name == "postgresql":
            return dialect.type_descriptor(BYTEA())
        else:
            return dialect.type_descriptor(Text())

    def process_bind_param(self, value, dialect):
        if dialect.name == "postgresql":
            return value
        else:
            # SQLite: ensure it's a string for TEXT column
            if isinstance(value, bytes):
                return value.decode("utf-8", errors="ignore")
            return value

    def process_result_value(self, value, dialect):
        # Just pass through - encryption/decryption handled in utils
        return value


class TimestampMixin:
    """Simple mixin for created_at/updated_at timestamps."""

    created_at = Column(DateTime(timezone=True), default=utc_now, nullable=False)
    updated_at = Column(DateTime(timezone=True), default=utc_now, onupdate=utc_now, nullable=False)


class UUIDMixin:
    """Simple mixin for UUID primary keys."""

    id = Column(String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
