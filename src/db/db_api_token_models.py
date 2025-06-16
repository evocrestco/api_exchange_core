"""
Generic API token management models for serverless environments.

These models provide a reusable pattern for any API that requires token pooling
and coordination across serverless functions (Azure Functions, AWS Lambda, etc).
"""

import json
from datetime import datetime, timedelta
from typing import Any, Dict, Optional

from sqlalchemy import (
    Column,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    String,
    Text,
    UniqueConstraint,
    text,
)
from sqlalchemy.orm import relationship

from src.db.db_base import Base, BaseModel, EncryptedBinary


class APIToken(Base, BaseModel):
    """
    Generic API token model for serverless coordination.

    This model handles:
    - Token storage with encryption
    - Expiration tracking
    - Usage counting for API limits
    - Atomic operations via PostgreSQL advisory locks
    - Tenant isolation
    - Configurable for any API provider
    """

    __tablename__ = "api_tokens"

    # Tenant isolation
    tenant_id = Column(
        String(100), ForeignKey("tenant.tenant_id", ondelete="CASCADE"), nullable=False
    )

    # API provider identification
    api_provider = Column(String(50), nullable=False)  # e.g., "api_provider_a", "shopify", etc.

    # Token identification - using hash of token for uniqueness without storing plaintext
    token_hash = Column(String(64), nullable=False)

    # pgcrypto encrypted token storage
    _encrypted_token = Column("encrypted_token", EncryptedBinary, nullable=False)

    # Token lifecycle management
    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)
    expires_at = Column(DateTime, nullable=False)
    last_used_at = Column(DateTime, nullable=True)

    # Usage tracking for API limits
    usage_count = Column(Integer, nullable=False, default=0)
    is_active = Column(String(10), nullable=False, default="active")

    # Token generation metadata
    generated_by = Column(String(100), nullable=True)  # Which function/process generated this
    generation_context = Column(Text, nullable=True)  # JSON metadata about generation

    # Relationships
    tenant = relationship("Tenant", backref="api_tokens")

    # Constraints and indexes for performance
    __table_args__ = (
        UniqueConstraint("token_hash", name="uq_api_token_hash"),
        Index("ix_api_token_tenant_provider", "tenant_id", "api_provider"),
        Index("ix_api_token_tenant_active", "tenant_id", "api_provider", "is_active"),
        Index("ix_api_token_expires_at", "expires_at"),
        Index("ix_api_token_last_used", "last_used_at"),
        Index("ix_api_token_created_at", "created_at"),
    )

    def set_token(self, token: str, session) -> None:
        """Encrypt and store token using pgcrypto with tenant-specific key."""
        # TODO: Replace database-specific logic with proper encryption abstraction layer
        if session.bind.dialect.name == "postgresql":
            # Use tenant_id + api_provider as part of the encryption key for additional isolation
            encryption_key = f"api_token_key_{self.tenant_id}_{self.api_provider}"

            # Encrypt using pgcrypto with AES
            encrypted_value = session.execute(
                text("SELECT pgp_sym_encrypt(:data, :key)"), {"data": token, "key": encryption_key}
            ).scalar()

            self._encrypted_token = encrypted_value
        else:
            # For SQLite (testing), store unencrypted
            self._encrypted_token = token

    def get_token(self, session) -> str:
        """Decrypt and return token using pgcrypto."""
        if not self._encrypted_token:
            return ""

        # TODO: Replace database-specific logic with proper encryption abstraction layer
        if session.bind.dialect.name == "postgresql":
            # Use same tenant-specific key for decryption
            encryption_key = f"api_token_key_{self.tenant_id}_{self.api_provider}"

            # Decrypt using pgcrypto
            decrypted_value = session.execute(
                text("SELECT pgp_sym_decrypt(:data, :key)"),
                {"data": self._encrypted_token, "key": encryption_key},
            ).scalar()

            return decrypted_value if decrypted_value else ""
        else:
            # For SQLite (testing), return unencrypted value
            return self._encrypted_token

    def is_expired(self) -> bool:
        """Check if token has expired."""
        return datetime.utcnow() > self.expires_at

    def is_valid(self) -> bool:
        """Check if token is valid and usable."""
        return (
            self.is_active == "active"
            and not self.is_expired()
            and self._encrypted_token is not None
        )

    def mark_used(self, session) -> None:
        """Mark token as used and increment usage counter."""
        self.last_used_at = datetime.utcnow()
        self.usage_count += 1

    def set_generation_context(self, context: Dict[str, Any]) -> None:
        """Set metadata about how this token was generated."""
        self.generation_context = json.dumps(context) if context else None

    def get_generation_context(self) -> Dict[str, Any]:
        """Get metadata about how this token was generated."""
        if not self.generation_context:
            return {}
        try:
            return json.loads(self.generation_context)
        except (json.JSONDecodeError, TypeError):
            return {}

    def deactivate(self, session) -> None:
        """Deactivate token (soft delete for audit purposes)."""
        self.is_active = "inactive"

    @classmethod
    def create_token_hash(cls, token: str) -> str:
        """Create a hash of the token for uniqueness checking without storing plaintext."""
        import hashlib

        return hashlib.sha256(token.encode()).hexdigest()

    @classmethod
    def calculate_expiry(cls, hours_valid: int) -> datetime:
        """Calculate expiry time for new tokens."""
        return datetime.utcnow() + timedelta(hours=hours_valid)


class APITokenUsageLog(Base, BaseModel):
    """
    Audit log for API token usage.

    This model provides detailed tracking for:
    - Which tokens were used when
    - What operations they were used for
    - Performance monitoring
    - Compliance and debugging
    """

    __tablename__ = "api_token_usage_log"

    # Tenant isolation
    tenant_id = Column(
        String(100), ForeignKey("tenant.tenant_id", ondelete="CASCADE"), nullable=False
    )

    # API provider identification
    api_provider = Column(String(50), nullable=False)

    # Reference to the token (but don't prevent token deletion)
    token_id = Column(String(100), nullable=False)  # Store ID as string, not FK
    token_hash = Column(String(64), nullable=False)  # For correlation even if token deleted

    # Usage details
    used_at = Column(DateTime, nullable=False, default=datetime.utcnow)
    operation = Column(String(100), nullable=False)  # e.g., "list_orders", "get_order_details"
    endpoint = Column(String(200), nullable=True)  # Specific API endpoint called

    # Request/response metadata
    request_duration_ms = Column(Integer, nullable=True)
    response_status = Column(Integer, nullable=True)  # HTTP status code
    success = Column(
        String(10), nullable=False, default="unknown"
    )  # "success", "failed", "unknown"

    # Context information
    function_name = Column(String(100), nullable=True)  # Which Azure Function used the token
    correlation_id = Column(String(100), nullable=True)  # For tracing across operations
    usage_context = Column(Text, nullable=True)  # JSON metadata about the usage

    # Relationships
    tenant = relationship("Tenant", backref="api_token_usage_logs")

    # Indexes for performance and querying
    __table_args__ = (
        Index("ix_api_usage_tenant_provider", "tenant_id", "api_provider"),
        Index("ix_api_usage_token_id", "token_id"),
        Index("ix_api_usage_used_at", "used_at"),
        Index("ix_api_usage_operation", "operation"),
        Index("ix_api_usage_tenant_operation", "tenant_id", "api_provider", "operation"),
        Index("ix_api_usage_correlation_id", "correlation_id"),
    )

    def set_usage_context(self, context: Dict[str, Any]) -> None:
        """Set metadata about this token usage."""
        self.usage_context = json.dumps(context) if context else None

    def get_usage_context(self) -> Dict[str, Any]:
        """Get metadata about this token usage."""
        if not self.usage_context:
            return {}
        try:
            return json.loads(self.usage_context)
        except (json.JSONDecodeError, TypeError):
            return {}

    @classmethod
    def create_usage_record(
        cls,
        tenant_id: str,
        api_provider: str,
        token_id: str,
        token_hash: str,
        operation: str,
        **kwargs,
    ) -> "APITokenUsageLog":
        """Create a new usage log record."""
        return cls(
            tenant_id=tenant_id,
            api_provider=api_provider,
            token_id=token_id,
            token_hash=token_hash,
            operation=operation,
            used_at=datetime.utcnow(),
            **kwargs,
        )


class TokenCoordination(Base):
    """
    Token coordination table for Azure Function coordination.

    This table provides atomic coordination for token creation across
    multiple isolated Azure Functions using database-level locking.
    """

    __tablename__ = "token_coordination"

    # Composite primary key for tenant + API provider coordination
    tenant_id = Column(
        String(255), ForeignKey("tenant.tenant_id"), nullable=False, primary_key=True
    )
    api_provider = Column(String(255), nullable=False, primary_key=True)

    # Lock ownership and timing
    locked_by = Column(String(255), nullable=False)  # Function instance identifier
    locked_at = Column(DateTime, nullable=False, default=datetime.utcnow)
    expires_at = Column(DateTime, nullable=False)

    # Coordination attempt tracking for monitoring
    attempt_count = Column(Integer, nullable=False, default=1)
    last_attempt_at = Column(DateTime, nullable=False, default=datetime.utcnow)

    # Relationships
    tenant = relationship("Tenant", backref="token_coordinations")

    # Indexes for performance
    __table_args__ = (
        Index("ix_token_coordination_expires_at", "expires_at"),
        Index("ix_token_coordination_locked_at", "locked_at"),
        Index("ix_token_coordination_attempt_count", "attempt_count"),
    )

    @classmethod
    def create_coordination_lock(
        cls, tenant_id: str, api_provider: str, locked_by: str, lock_duration_seconds: int = 10
    ) -> "TokenCoordination":
        """Create a new coordination lock."""
        return cls(
            tenant_id=tenant_id,
            api_provider=api_provider,
            locked_by=locked_by,
            locked_at=datetime.utcnow(),
            expires_at=datetime.utcnow() + timedelta(seconds=lock_duration_seconds),
            attempt_count=1,
            last_attempt_at=datetime.utcnow(),
        )
