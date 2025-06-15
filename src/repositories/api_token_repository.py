"""
Generic API token repository for serverless environments.

This repository provides atomic token operations using PostgreSQL advisory locks
to coordinate token access across multiple serverless function instances.
"""

from datetime import datetime, timedelta
from typing import Any, Callable, Dict, List, Optional, Tuple

from sqlalchemy import and_, func, or_, text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from src.context.tenant_context import tenant_aware
from src.db.db_api_token_models import APIToken, APITokenUsageLog, TokenCoordination
from src.exceptions import (
    ErrorCode,
    RepositoryError,
    TenantIsolationViolationError,
    ValidationError,
)
from src.repositories.base_repository import BaseRepository
from src.utils.logger import get_logger


class APITokenRepository(BaseRepository[APIToken]):
    """
    Repository for generic API token management in serverless environments.

    This repository provides:
    - Atomic token operations using PostgreSQL advisory locks
    - Token lifecycle management (create, use, expire, cleanup)
    - Tenant isolation with paranoid validation
    - Usage tracking and audit logging
    - Coordination across multiple serverless function instances
    - Configurable for any API provider with token limits
    """

    def __init__(
        self,
        session: Session,
        api_provider: str,
        max_tokens: int = 25,
        token_validity_hours: int = 1,
    ):
        """
        Initialize repository for specific API provider.

        Args:
            session: Database session
            api_provider: API provider identifier (e.g., "api_provider_a", "shopify")
            max_tokens: Maximum tokens allowed per tenant (default: 25)
            token_validity_hours: Token validity in hours (default: 1)
        """
        super().__init__(session, APIToken)
        self.logger = get_logger()
        self.api_provider = api_provider
        self.max_tokens = max_tokens
        self.token_validity_hours = token_validity_hours

    @tenant_aware
    def get_valid_token(
        self, operation: str = "api_call", buffer_minutes: int = 5
    ) -> Optional[Tuple[str, str]]:
        """
        Get a valid token for API operations (shared access).

        This method:
        1. Finds tokens with sufficient remaining validity time
        2. Marks token as used (lightweight update)
        3. Logs usage

        Multiple Azure Functions can safely share the same token.
        Coordination happens during token creation, not retrieval.

        Args:
            operation: Description of operation needing token
            buffer_minutes: Minimum minutes before expiry required (default: 5)

        Returns:
            Tuple of (token_value, token_id) or None if no tokens available

        Raises:
            RepositoryError: If database operation fails
        """
        tenant_id = self._get_current_tenant_id()

        try:
            # Calculate buffer time to avoid tokens that expire too soon
            buffer_expiry = datetime.utcnow() + timedelta(minutes=buffer_minutes)

            # Step 1: Try to get an existing valid token (no locks needed)
            valid_tokens = (
                self.session.query(APIToken)
                .filter(
                    and_(
                        APIToken.tenant_id == tenant_id,
                        APIToken.api_provider == self.api_provider,
                        APIToken.is_active == "active",
                        APIToken.expires_at > buffer_expiry,  # Must have buffer time remaining
                    )
                )
                .order_by(APIToken.created_at.desc())
                .limit(1)
                .all()
            )  # Get newest token

            if valid_tokens:
                # Found existing token - use it (multiple functions can share)
                token = valid_tokens[0]
            else:
                # Step 2: No tokens available - return None so service can coordinate creation
                self.logger.info(
                    "No valid tokens available with sufficient buffer time",
                    extra={
                        "api_provider": self.api_provider,
                        "tenant_id": tenant_id,
                        "operation": operation,
                        "buffer_minutes": buffer_minutes,
                    },
                )
                return None

            # Mark token as used (this row is now locked for us)
            token.mark_used(self.session)

            # Log usage
            self._log_token_usage(token=token, operation=operation, success="pending")

            # Get decrypted token value
            token_value = token.get_token(self.session)

            self.logger.info(
                "Token retrieved successfully",
                extra={
                    "api_provider": self.api_provider,
                    "token_id": token.id,
                    "tenant_id": tenant_id,
                    "operation": operation,
                    "usage_count": token.usage_count,
                    "expires_at": token.expires_at.isoformat(),
                    "buffer_minutes": buffer_minutes,
                    "token_preview": (
                        token_value[:20] + "..." if len(token_value) > 20 else token_value
                    ),
                },
            )

            return (token_value, token.id)

        except Exception as e:
            self.logger.error(
                "Failed to get valid token",
                extra={
                    "api_provider": self.api_provider,
                    "tenant_id": tenant_id,
                    "operation": operation,
                    "error": str(e),
                    "error_type": type(e).__name__,
                },
            )
            raise RepositoryError(
                message="Failed to retrieve valid token",
                error_code=ErrorCode.DATABASE_ERROR,
                details={
                    "api_provider": self.api_provider,
                    "operation": operation,
                    "error": str(e),
                },
                cause=e,
            ) from e

    @tenant_aware
    def store_new_token(
        self, token: str, generated_by: str, generation_context: Optional[Dict[str, Any]] = None
    ) -> str:
        """
        Store a new token with atomic coordination.

        Args:
            token: The token value to store
            generated_by: Identifier of what generated this token
            generation_context: Metadata about token generation

        Returns:
            Token ID

        Raises:
            ValidationError: If token already exists or limits exceeded
            RepositoryError: If database operation fails
        """
        tenant_id = self._get_current_tenant_id()

        # Validate token
        if not token or not isinstance(token, str):
            raise ValidationError(
                message="Token must be a non-empty string",
                error_code=ErrorCode.INVALID_FORMAT,
                details={"token_type": type(token).__name__},
            )

        # Create token hash for uniqueness checking
        token_hash = APIToken.create_token_hash(token)

        try:
            # Check token count limit for tenant and API provider
            # Note: We can't use FOR UPDATE with COUNT(), so we do a regular count
            # Race conditions on token limits are handled by the database constraints
            active_count = (
                self.session.query(func.count(APIToken.id))
                .filter(
                    and_(
                        APIToken.tenant_id == tenant_id,
                        APIToken.api_provider == self.api_provider,
                        APIToken.is_active == "active",
                        APIToken.expires_at > datetime.utcnow(),
                    )
                )
                .scalar()
            )

            if active_count >= self.max_tokens:
                self.logger.warning(
                    "Token limit reached, cannot store new token",
                    extra={
                        "api_provider": self.api_provider,
                        "tenant_id": tenant_id,
                        "active_count": active_count,
                        "limit": self.max_tokens,
                    },
                )
                raise ValidationError(
                    message=f"Token limit reached ({active_count}/{self.max_tokens})",
                    error_code=ErrorCode.LIMIT_EXCEEDED,
                    details={
                        "api_provider": self.api_provider,
                        "active_count": active_count,
                        "limit": self.max_tokens,
                    },
                )

            # Create new token record
            api_token = APIToken(
                tenant_id=tenant_id,
                api_provider=self.api_provider,
                token_hash=token_hash,
                expires_at=APIToken.calculate_expiry(self.token_validity_hours),
                generated_by=generated_by,
                is_active="active",
            )

            # Set encrypted token
            api_token.set_token(token, self.session)

            # Set generation context
            if generation_context:
                api_token.set_generation_context(generation_context)

            # Save to database - let database enforce uniqueness constraints
            self.session.add(api_token)
            self.session.flush()  # Get the ID

            self.logger.info(
                "Token stored successfully",
                extra={
                    "api_provider": self.api_provider,
                    "token_id": api_token.id,
                    "tenant_id": tenant_id,
                    "generated_by": generated_by,
                    "expires_at": api_token.expires_at.isoformat(),
                    "active_tokens_after": active_count + 1,
                    "coordination": "database_constraints",
                    "token_hash": token_hash,
                    "token_preview": token[:20] + "..." if len(token) > 20 else token,
                },
            )

            return api_token.id

        except (ValidationError, TenantIsolationViolationError):
            raise
        except IntegrityError as e:
            # Handle race condition where token was created by another process
            self.logger.warning(
                "Token creation race condition detected",
                extra={
                    "api_provider": self.api_provider,
                    "tenant_id": tenant_id,
                    "generated_by": generated_by,
                    "error": str(e),
                },
            )
            raise ValidationError(
                message="Token already exists (race condition)",
                error_code=ErrorCode.DUPLICATE,
                details={"error": str(e)},
            ) from e
        except Exception as e:
            self.logger.error(
                "Failed to store new token",
                extra={
                    "api_provider": self.api_provider,
                    "tenant_id": tenant_id,
                    "generated_by": generated_by,
                    "error": str(e),
                    "error_type": type(e).__name__,
                },
            )
            raise RepositoryError(
                message="Failed to store token",
                error_code=ErrorCode.DATABASE_ERROR,
                details={
                    "api_provider": self.api_provider,
                    "generated_by": generated_by,
                    "error": str(e),
                },
                cause=e,
            ) from e

    @tenant_aware
    def get_or_create_token_with_coordination(
        self,
        operation: str = "api_call",
        buffer_minutes: int = 5,
        token_generator: Optional[Callable[[], str]] = None,
    ) -> Optional[Tuple[str, str]]:
        """
        Get existing token or create new one with coordination table pattern.

        This implements the coordination table pattern:
        1. Try to get existing token (no locks)
        2. If none available, try to acquire coordination lock
        3. If lock acquired, double-check if token exists
        4. If still none, create new token and release lock
        5. If lock not acquired, wait and retry get_valid_token

        Args:
            operation: Description of operation needing token
            buffer_minutes: Minimum minutes before expiry required
            token_generator: Function to generate new token if needed

        Returns:
            Tuple of (token_value, token_id) or None if can't create
        """
        import uuid

        # Step 1: Try to get existing token first (fast path)
        self.logger.info(
            "get_or_create_token_with_coordination: Checking for existing tokens",
            extra={
                "api_provider": self.api_provider,
                "operation": operation,
                "buffer_minutes": buffer_minutes,
            },
        )

        existing_token = self.get_valid_token(operation, buffer_minutes)
        if existing_token:
            self.logger.info(
                "get_or_create_token_with_coordination: Found existing token, returning it",
                extra={
                    "api_provider": self.api_provider,
                    "token_id": existing_token[1],
                    "operation": operation,
                },
            )
            return existing_token

        # Step 2: No token available, coordinate creation
        self.logger.info(
            "get_or_create_token_with_coordination: No existing tokens, will coordinate creation",
            extra={"api_provider": self.api_provider, "operation": operation},
        )

        if not token_generator:
            return None

        # Generate unique identifier for this function instance
        function_instance_id = f"func_{uuid.uuid4().hex[:8]}_{operation}"

        # Step 3: Try to acquire coordination lock
        lock_acquired = self.try_acquire_coordination_lock(
            locked_by=function_instance_id,
            lock_duration_seconds=30,  # Long enough for token creation
        )

        try:
            if lock_acquired:
                # Step 4: We got the lock - double-check if token exists
                self.logger.info(
                    "get_or_create_token_with_coordination: Lock acquired, double-checking for tokens",
                    extra={
                        "api_provider": self.api_provider,
                        "function_instance": function_instance_id,
                        "operation": operation,
                    },
                )

                double_check = self.get_valid_token(operation, buffer_minutes)
                if double_check:
                    # Another function created a token while we were acquiring lock
                    self.logger.info(
                        "get_or_create_token_with_coordination: Found token during double-check",
                        extra={
                            "api_provider": self.api_provider,
                            "function_instance": function_instance_id,
                            "token_id": double_check[1],
                            "operation": operation,
                        },
                    )
                    return double_check

                # Step 5: Still no token, create one
                self.logger.info(
                    "get_or_create_token_with_coordination: Generating new token",
                    extra={
                        "api_provider": self.api_provider,
                        "function_instance": function_instance_id,
                        "operation": operation,
                    },
                )

                new_token = token_generator()
                if not new_token:
                    return None

                token_id = self.store_new_token(
                    token=new_token,
                    generated_by=function_instance_id,
                    generation_context={
                        "trigger": "coordination_table_lock",
                        "operation": operation,
                        "coordination_method": "coordination_table",
                    },
                )

                if token_id:
                    self.logger.info(
                        "Created new token via coordination",
                        extra={
                            "api_provider": self.api_provider,
                            "function_instance": function_instance_id,
                            "operation": operation,
                            "token_id": token_id,
                        },
                    )
                    return (new_token, token_id)
                else:
                    return None
            else:
                # Step 6: Lock not acquired - another function is creating token
                # Just fail immediately and let the service layer handle retry logic
                self.logger.warning(
                    "Could not acquire coordination lock and no token available",
                    extra={
                        "api_provider": self.api_provider,
                        "function_instance": function_instance_id,
                        "operation": operation,
                    },
                )
                return None

        finally:
            # Always attempt to release our lock if we acquired it
            if lock_acquired:
                self.release_coordination_lock(function_instance_id)

    @tenant_aware
    def cleanup_expired_tokens(self, force_cleanup: bool = False) -> int:
        """
        Clean up expired tokens using row-level locking coordination.

        Args:
            force_cleanup: Unused (kept for compatibility)

        Returns:
            Number of tokens cleaned up
        """
        tenant_id = self._get_current_tenant_id()

        try:
            # Atomically claim expired tokens for cleanup using row-level locking
            if self.session.bind.dialect.name == "postgresql":
                # PostgreSQL: Use FOR UPDATE SKIP LOCKED to claim expired tokens
                expired_ids = self.session.execute(
                    text(
                        """
                        SELECT id
                        FROM api_tokens 
                        WHERE tenant_id = :tenant_id 
                          AND api_provider = :api_provider 
                          AND (expires_at <= :now OR is_active = 'inactive')
                        FOR UPDATE SKIP LOCKED
                    """
                    ),
                    {
                        "tenant_id": tenant_id,
                        "api_provider": self.api_provider,
                        "now": datetime.utcnow(),
                    },
                ).fetchall()

                if not expired_ids:
                    return 0

                # Get the full token objects for the locked rows
                expired_tokens = (
                    self.session.query(APIToken)
                    .filter(APIToken.id.in_([row.id for row in expired_ids]))
                    .all()
                )
            else:
                # SQLite: Regular query (no row locking support)
                expired_tokens = (
                    self.session.query(APIToken)
                    .filter(
                        and_(
                            APIToken.tenant_id == tenant_id,
                            APIToken.api_provider == self.api_provider,
                            or_(
                                APIToken.expires_at <= datetime.utcnow(),
                                APIToken.is_active == "inactive",
                            ),
                        )
                    )
                    .all()
                )

                if not expired_tokens:
                    return 0

            # Deactivate tokens (soft delete for audit purposes)
            count = 0
            for token in expired_tokens:
                token.deactivate(self.session)
                count += 1

            # Also cleanup expired coordination locks
            coordination_cleanup_count = self.cleanup_expired_coordination_locks()

            self.logger.info(
                "Cleaned up expired tokens and coordination locks",
                extra={
                    "api_provider": self.api_provider,
                    "tenant_id": tenant_id,
                    "tokens_cleaned": count,
                    "coordination_locks_cleaned": coordination_cleanup_count,
                    "coordination": "coordination_table",
                },
            )

            return count

        except Exception as e:
            self.logger.error(
                "Failed to cleanup expired tokens",
                extra={
                    "api_provider": self.api_provider,
                    "tenant_id": tenant_id,
                    "error": str(e),
                    "error_type": type(e).__name__,
                },
            )
            raise RepositoryError(
                message="Failed to cleanup expired tokens",
                error_code=ErrorCode.DATABASE_ERROR,
                details={"api_provider": self.api_provider, "error": str(e)},
                cause=e,
            ) from e

    @tenant_aware
    def get_token_stats(self) -> Dict[str, Any]:
        """
        Get token statistics for monitoring and debugging.

        Returns:
            Dictionary with token pool statistics
        """
        tenant_id = self._get_current_tenant_id()

        try:
            # Count tokens by status for this API provider
            active_count = (
                self.session.query(func.count(APIToken.id))
                .filter(
                    and_(
                        APIToken.tenant_id == tenant_id,
                        APIToken.api_provider == self.api_provider,
                        APIToken.is_active == "active",
                        APIToken.expires_at > datetime.utcnow(),
                    )
                )
                .scalar()
            )

            expired_count = (
                self.session.query(func.count(APIToken.id))
                .filter(
                    and_(
                        APIToken.tenant_id == tenant_id,
                        APIToken.api_provider == self.api_provider,
                        APIToken.expires_at <= datetime.utcnow(),
                    )
                )
                .scalar()
            )

            inactive_count = (
                self.session.query(func.count(APIToken.id))
                .filter(
                    and_(
                        APIToken.tenant_id == tenant_id,
                        APIToken.api_provider == self.api_provider,
                        APIToken.is_active == "inactive",
                    )
                )
                .scalar()
            )

            # Get oldest active token
            oldest_active = (
                self.session.query(APIToken.created_at)
                .filter(
                    and_(
                        APIToken.tenant_id == tenant_id,
                        APIToken.api_provider == self.api_provider,
                        APIToken.is_active == "active",
                        APIToken.expires_at > datetime.utcnow(),
                    )
                )
                .order_by(APIToken.created_at.asc())
                .first()
            )

            return {
                "api_provider": self.api_provider,
                "tenant_id": tenant_id,
                "active_tokens": active_count,
                "expired_tokens": expired_count,
                "inactive_tokens": inactive_count,
                "total_tokens": active_count + expired_count + inactive_count,
                "available_slots": max(0, self.max_tokens - active_count),
                "oldest_active_token": oldest_active[0].isoformat() if oldest_active else None,
                "max_tokens_allowed": self.max_tokens,
                "token_validity_hours": self.token_validity_hours,
                "generated_at": datetime.utcnow().isoformat(),
            }

        except Exception as e:
            self.logger.error(
                "Failed to get token statistics",
                extra={"api_provider": self.api_provider, "tenant_id": tenant_id, "error": str(e)},
            )
            raise RepositoryError(
                message="Failed to get token statistics",
                error_code=ErrorCode.DATABASE_ERROR,
                details={"api_provider": self.api_provider, "error": str(e)},
                cause=e,
            ) from e

    def _log_token_usage(
        self, token: APIToken, operation: str, success: str = "unknown", **kwargs
    ) -> None:
        """
        Log token usage for audit and monitoring.

        Args:
            token: The token being used
            operation: Operation description
            success: "success", "failed", or "unknown"
            **kwargs: Additional usage metadata
        """
        try:
            usage_log = APITokenUsageLog.create_usage_record(
                tenant_id=token.tenant_id,
                api_provider=token.api_provider,
                token_id=token.id,
                token_hash=token.token_hash,
                operation=operation,
                success=success,
                **kwargs,
            )

            self.session.add(usage_log)
            self.session.flush()

        except Exception as e:
            # Don't fail the main operation if logging fails
            self.logger.warning(
                "Failed to log token usage",
                extra={
                    "api_provider": self.api_provider,
                    "token_id": token.id,
                    "operation": operation,
                    "error": str(e),
                },
            )

    @tenant_aware
    def try_acquire_coordination_lock(
        self, locked_by: str, lock_duration_seconds: int = 10
    ) -> bool:
        """
        Try to acquire a coordination lock for token creation.

        Uses atomic INSERT with ON CONFLICT to ensure only one function
        can hold the coordination lock at a time.

        Args:
            locked_by: Identifier for the function instance acquiring the lock
            lock_duration_seconds: How long the lock should be held

        Returns:
            True if lock acquired, False if lock already exists
        """
        tenant_id = self._get_current_tenant_id()

        import time

        attempt_time = time.time()
        self.logger.info(
            "Attempting to acquire coordination lock",
            extra={
                "api_provider": self.api_provider,
                "tenant_id": tenant_id,
                "locked_by": locked_by,
                "lock_duration_seconds": lock_duration_seconds,
                "timestamp": attempt_time,
                "timestamp_ms": int(attempt_time * 1000),
            },
        )

        try:
            if self.session.bind.dialect.name == "postgresql":
                # Simple row lock approach: ensure row exists, then lock it

                # Step 1: Ensure coordination row exists (first use creates it)
                self.session.execute(
                    text(
                        """
                        INSERT INTO token_coordination 
                        (tenant_id, api_provider, locked_by, locked_at, expires_at, attempt_count, last_attempt_at)
                        VALUES (:tenant_id, :api_provider, '', '1970-01-01', '1970-01-01', 0, '1970-01-01')
                        ON CONFLICT (tenant_id, api_provider) DO NOTHING
                    """
                    ),
                    {"tenant_id": tenant_id, "api_provider": self.api_provider},
                )

                # Step 2: Try to acquire row lock with NOWAIT
                try:
                    self.session.execute(
                        text(
                            """
                            SELECT 1 FROM token_coordination 
                            WHERE tenant_id = :tenant_id 
                              AND api_provider = :api_provider
                            FOR UPDATE NOWAIT
                        """
                        ),
                        {"tenant_id": tenant_id, "api_provider": self.api_provider},
                    )

                    # If we get here, we have the row lock!
                    # Update coordination metadata for monitoring
                    self.session.execute(
                        text(
                            """
                            UPDATE token_coordination 
                            SET locked_by = :locked_by,
                                locked_at = :locked_at,
                                expires_at = :expires_at,
                                attempt_count = attempt_count + 1,
                                last_attempt_at = :last_attempt_at
                            WHERE tenant_id = :tenant_id 
                              AND api_provider = :api_provider
                        """
                        ),
                        {
                            "tenant_id": tenant_id,
                            "api_provider": self.api_provider,
                            "locked_by": locked_by,
                            "locked_at": datetime.utcnow(),
                            "expires_at": datetime.utcnow()
                            + timedelta(seconds=lock_duration_seconds),
                            "last_attempt_at": datetime.utcnow(),
                        },
                    )

                    # Lock is held until transaction ends
                    success_time = time.time()
                    elapsed_ms = int((success_time - attempt_time) * 1000)
                    self.logger.info(
                        "Successfully acquired coordination lock",
                        extra={
                            "api_provider": self.api_provider,
                            "tenant_id": tenant_id,
                            "locked_by": locked_by,
                            "timestamp": success_time,
                            "timestamp_ms": int(success_time * 1000),
                            "elapsed_ms": elapsed_ms,
                            "attempt_count": self.session.execute(
                                text(
                                    "SELECT attempt_count FROM token_coordination WHERE tenant_id = :tenant_id AND api_provider = :api_provider"
                                ),
                                {"tenant_id": tenant_id, "api_provider": self.api_provider},
                            ).scalar(),
                        },
                    )
                    return True

                except Exception as e:
                    # NOWAIT failed - someone else has the lock
                    failed_time = time.time()
                    elapsed_ms = int((failed_time - attempt_time) * 1000)
                    self.logger.info(
                        "Failed to acquire coordination lock - another process has it",
                        extra={
                            "api_provider": self.api_provider,
                            "tenant_id": tenant_id,
                            "locked_by": locked_by,
                            "timestamp": failed_time,
                            "timestamp_ms": int(failed_time * 1000),
                            "elapsed_ms": elapsed_ms,
                            "error": str(e),
                        },
                    )
                    self.session.rollback()
                    return False
            else:
                # SQLite fallback - always acquire lock (for testing)
                coordination = TokenCoordination.create_coordination_lock(
                    tenant_id=tenant_id,
                    api_provider=self.api_provider,
                    locked_by=locked_by,
                    lock_duration_seconds=lock_duration_seconds,
                )
                self.session.add(coordination)
                self.session.commit()
                return True

        except Exception as e:
            self.session.rollback()
            self.logger.error(
                "Failed to acquire coordination lock",
                extra={
                    "api_provider": self.api_provider,
                    "tenant_id": tenant_id,
                    "locked_by": locked_by,
                    "error": str(e),
                },
            )
            return False

    @tenant_aware
    def release_coordination_lock(self, locked_by: str) -> bool:
        """
        Release a coordination lock held by this function instance.

        Args:
            locked_by: Identifier for the function instance releasing the lock

        Returns:
            True if lock was released, False if no lock was held by this instance
        """
        tenant_id = self._get_current_tenant_id()

        try:
            # Clear the lock ownership but keep the row and history for monitoring
            # Note: The actual row lock is released when transaction commits/rollbacks
            # This just clears the ownership metadata
            updated_count = self.session.execute(
                text(
                    """
                    UPDATE token_coordination 
                    SET locked_by = '',
                        expires_at = '1970-01-01'
                    WHERE tenant_id = :tenant_id 
                      AND api_provider = :api_provider
                      AND locked_by = :locked_by
                """
                ),
                {"tenant_id": tenant_id, "api_provider": self.api_provider, "locked_by": locked_by},
            ).rowcount

            self.session.commit()

            if updated_count > 0:
                self.logger.info(
                    "Released coordination lock",
                    extra={
                        "api_provider": self.api_provider,
                        "tenant_id": tenant_id,
                        "locked_by": locked_by,
                    },
                )
                return True
            else:
                self.logger.warning(
                    "No coordination lock found to release",
                    extra={
                        "api_provider": self.api_provider,
                        "tenant_id": tenant_id,
                        "locked_by": locked_by,
                    },
                )
                return False

        except Exception as e:
            self.session.rollback()
            self.logger.error(
                "Failed to release coordination lock",
                extra={
                    "api_provider": self.api_provider,
                    "tenant_id": tenant_id,
                    "locked_by": locked_by,
                    "error": str(e),
                },
            )
            return False

    @tenant_aware
    def cleanup_expired_coordination_locks(self) -> int:
        """
        Clean up expired coordination locks.

        This should be called periodically to prevent stale locks from
        blocking coordination indefinitely.

        Returns:
            Number of expired locks cleaned up
        """
        tenant_id = self._get_current_tenant_id()

        try:
            deleted_count = (
                self.session.query(TokenCoordination)
                .filter(
                    and_(
                        TokenCoordination.tenant_id == tenant_id,
                        TokenCoordination.api_provider == self.api_provider,
                        TokenCoordination.expires_at < datetime.utcnow(),
                    )
                )
                .delete()
            )

            self.session.commit()

            if deleted_count > 0:
                self.logger.info(
                    "Cleaned up expired coordination locks",
                    extra={
                        "api_provider": self.api_provider,
                        "tenant_id": tenant_id,
                        "expired_locks_cleaned": deleted_count,
                    },
                )

            return deleted_count

        except Exception as e:
            self.session.rollback()
            self.logger.error(
                "Failed to cleanup expired coordination locks",
                extra={"api_provider": self.api_provider, "tenant_id": tenant_id, "error": str(e)},
            )
            return 0

    @tenant_aware
    def get_coordination_metrics(self) -> Dict[str, Any]:
        """
        Get coordination metrics for monitoring system strain.

        The attempt_count tracks successful lock acquisitions (i.e., how many times
        token generation was triggered). This shows coordination effectiveness -
        lower counts mean better token reuse.

        Returns:
            Dictionary with coordination metrics including successful lock counts
        """
        tenant_id = self._get_current_tenant_id()

        try:
            # Get current active locks
            active_locks = (
                self.session.query(TokenCoordination)
                .filter(
                    and_(
                        TokenCoordination.tenant_id == tenant_id,
                        TokenCoordination.api_provider == self.api_provider,
                        TokenCoordination.expires_at > datetime.utcnow(),
                    )
                )
                .count()
            )

            # Get total attempt count from all coordination records (including expired)
            total_attempts = (
                self.session.query(func.sum(TokenCoordination.attempt_count))
                .filter(
                    and_(
                        TokenCoordination.tenant_id == tenant_id,
                        TokenCoordination.api_provider == self.api_provider,
                    )
                )
                .scalar()
                or 0
            )

            # Get max attempt count (indicates hottest contention)
            max_attempts = (
                self.session.query(func.max(TokenCoordination.attempt_count))
                .filter(
                    and_(
                        TokenCoordination.tenant_id == tenant_id,
                        TokenCoordination.api_provider == self.api_provider,
                    )
                )
                .scalar()
                or 0
            )

            return {
                "api_provider": self.api_provider,
                "tenant_id": tenant_id,
                "active_coordination_locks": active_locks,
                "total_coordination_attempts": total_attempts,
                "max_coordination_attempts": max_attempts,
                "timestamp": datetime.utcnow().isoformat(),
            }

        except Exception as e:
            self.logger.error(
                "Failed to get coordination metrics",
                extra={"api_provider": self.api_provider, "tenant_id": tenant_id, "error": str(e)},
            )
            return {
                "api_provider": self.api_provider,
                "tenant_id": tenant_id,
                "error": str(e),
                "timestamp": datetime.utcnow().isoformat(),
            }
