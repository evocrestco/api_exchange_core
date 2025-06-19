import enum
import json
import uuid
from datetime import UTC, datetime
from typing import List, Optional, Type, TypeVar

from sqlalchemy import Column, DateTime, String, Text
from sqlalchemy.dialects.postgresql import BYTEA, JSONB
from sqlalchemy.ext.declarative import declared_attr
from sqlalchemy.orm import Session
from sqlalchemy.types import TypeDecorator

from .db_config import Base
from ..utils.json_utils import dumps

BaseModelT = TypeVar("BaseModelT", bound="BaseModel")


def utc_now():
    """Return current UTC time with timezone info attached."""
    return datetime.now(UTC)


T = TypeVar("T", bound=Base)


class JSON(TypeDecorator):
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
            return dumps(value)

    def process_result_value(self, value, dialect):
        if value is None:
            return None
        if dialect.name == "postgresql":
            return value
        else:
            return json.loads(value)


class EncryptedBinary(TypeDecorator):
    """
    Cross-database encrypted binary type.

    Uses BYTEA for PostgreSQL (where pgcrypto returns bytea)
    and Text for SQLite (fallback for testing).
    """

    impl = Text
    cache_ok = True

    def load_dialect_impl(self, dialect):
        if dialect.name == "postgresql":
            return dialect.type_descriptor(BYTEA())
        else:
            # For SQLite, use Text - encryption won't work but tests can run
            return dialect.type_descriptor(Text())

    def process_bind_param(self, value, dialect):
        # Value is already encrypted/processed by the model methods
        if dialect.name == "postgresql":
            # PostgreSQL: pgcrypto returns bytea, pass through
            return value
        else:
            # SQLite: ensure it's a string for TEXT column
            if isinstance(value, bytes):
                return value.decode("utf-8", errors="ignore")
            return value

    def process_result_value(self, value, dialect):
        # Value processing is handled by the model methods
        # Just pass through - the model's get_credentials/get_access_token handles decryption
        return value


class BaseModel:

    @declared_attr  # type: ignore[arg-type]
    def __tablename__(cls) -> str:  # noqa
        return cls.__name__.lower()  # type: ignore[attr-defined, no-any-return]

    id = Column(String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
    created_at = Column(DateTime, default=lambda: datetime.now(UTC), nullable=False)
    updated_at = Column(
        DateTime,
        default=lambda: datetime.now(UTC),
        onupdate=lambda: datetime.now(UTC),
        nullable=False,
    )

    @classmethod
    def get_by_id(cls: Type[BaseModelT], session: Session, id: str) -> Optional[BaseModelT]:
        return session.query(cls).filter(cls.id == id).first()

    @classmethod
    def get_all(
        cls: Type[BaseModelT], session: Session, limit: int = 100, offset: int = 0
    ) -> List[BaseModelT]:
        return session.query(cls).order_by(cls.created_at.desc()).limit(limit).offset(offset).all()


class EntityStateEnum(enum.Enum):
    # Standard processing states
    RECEIVED = "RECEIVED"
    TRANSFORMED_TO_CANONICAL = "TRANSFORMED_TO_CANONICAL"
    PROCESSING = "PROCESSING"
    VALIDATED = "VALIDATED"
    ENRICHED = "ENRICHED"
    TRANSFORMING_FOR_TARGET = "TRANSFORMING_FOR_TARGET"
    DELIVERED_TO_TARGET = "DELIVERED_TO_TARGET"
    COMPLETED = "COMPLETED"

    # Error states
    VALIDATION_ERROR = "VALIDATION_ERROR"
    TRANSFORMATION_ERROR = "TRANSFORMATION_ERROR"
    DELIVERY_ERROR = "DELIVERY_ERROR"
    SYSTEM_ERROR = "SYSTEM_ERROR"

    # New update-specific states
    UPDATE_RECEIVED = "UPDATE_RECEIVED"
    UPDATE_PROCESSING = "UPDATE_PROCESSING"
    UPDATE_VALIDATED = "UPDATE_VALIDATED"
    UPDATE_DELIVERED = "UPDATE_DELIVERED"
    UPDATE_COMPLETED = "UPDATE_COMPLETED"
    UPDATE_ERROR = "UPDATE_ERROR"

    # Duplicate handling states
    DUPLICATE_DETECTED = "DUPLICATE_DETECTED"
    MANUALLY_RESOLVED = "MANUALLY_RESOLVED"

    # Hold states
    ON_HOLD = "ON_HOLD"
    PENDING_REVIEW = "PENDING_REVIEW"


class ErrorTypeEnum(enum.Enum):
    VALIDATION = "VALIDATION"
    MAPPING = "MAPPING"
    CONNECTION = "CONNECTION"
    AUTHENTICATION = "AUTHENTICATION"
    SYSTEM = "SYSTEM"
    BUSINESS_RULE = "BUSINESS_RULE"
    DATA_INTEGRITY = "DATA_INTEGRITY"
    TIMEOUT = "TIMEOUT"
    CUSTOM = "CUSTOM"

    # New error types
    DUPLICATE = "DUPLICATE"
    CONCURRENCY = "CONCURRENCY"
    VERSION_CONFLICT = "VERSION_CONFLICT"
    HASH_MISMATCH = "HASH_MISMATCH"


class EntityTypeEnum(enum.Enum):
    """Generic entity types that can be extended by implementations."""

    ENTITY_A = "entity_a"
    ENTITY_B = "entity_b"
    ENTITY_C = "entity_c"
    CUSTOM = "custom"


class RefTypeEnum(enum.Enum):
    SOURCE = "SOURCE"
    TARGET = "TARGET"
