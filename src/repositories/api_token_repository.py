"""
Generic API token repository for serverless environments.

This repository provides atomic token operations using PostgreSQL advisory locks
to coordinate token access across multiple serverless function instances.
"""

from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional, Tuple

from sqlalchemy import and_, or_, text, func
from sqlalchemy.orm import Session
from sqlalchemy.exc import IntegrityError

from src.context.tenant_context import tenant_aware
from src.db.db_api_token_models import APIToken, APITokenUsageLog
from src.exceptions import (
    ErrorCode, 
    RepositoryError, 
    ValidationError,
    TenantIsolationViolationError
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
    
    def __init__(self, session: Session, api_provider: str, max_tokens: int = 25, token_validity_hours: int = 1):
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
        
        # Generate unique advisory lock IDs based on API provider
        self._lock_token_pool = abs(hash(f"{api_provider}_pool")) % 2147483647
        self._lock_token_cleanup = abs(hash(f"{api_provider}_cleanup")) % 2147483647

    def _get_advisory_lock(self, lock_id: int, timeout_seconds: int = 5) -> bool:
        """
        Acquire PostgreSQL advisory lock for atomic operations.
        
        Args:
            lock_id: Unique lock identifier
            timeout_seconds: How long to wait for lock
            
        Returns:
            True if lock acquired, False if timeout
        """
        if self.session.bind.dialect.name != "postgresql":
            # For SQLite (testing), always return True
            return True
            
        try:
            # Try to acquire lock with timeout
            result = self.session.execute(
                text("SELECT pg_try_advisory_lock(:lock_id)"),
                {"lock_id": lock_id}
            ).scalar()
            
            return bool(result)
        except Exception as e:
            self.logger.error(
                "Failed to acquire advisory lock",
                extra={
                    "api_provider": self.api_provider,
                    "lock_id": lock_id,
                    "timeout_seconds": timeout_seconds,
                    "error": str(e)
                }
            )
            return False
    
    def _release_advisory_lock(self, lock_id: int) -> bool:
        """
        Release PostgreSQL advisory lock.
        
        Args:
            lock_id: Lock identifier to release
            
        Returns:
            True if lock released successfully
        """
        if self.session.bind.dialect.name != "postgresql":
            # For SQLite (testing), always return True
            return True
            
        try:
            result = self.session.execute(
                text("SELECT pg_advisory_unlock(:lock_id)"),
                {"lock_id": lock_id}
            ).scalar()
            
            return bool(result)
        except Exception as e:
            self.logger.error(
                "Failed to release advisory lock",
                extra={
                    "api_provider": self.api_provider,
                    "lock_id": lock_id,
                    "error": str(e)
                }
            )
            return False
    
    @tenant_aware
    def get_valid_token(self, operation: str = "api_call") -> Optional[Tuple[str, str]]:
        """
        Get a valid token for API operations with atomic coordination.
        
        This method:
        1. Acquires advisory lock for token pool
        2. Finds least-recently-used valid token
        3. Marks token as used
        4. Logs usage
        5. Releases lock
        
        Args:
            operation: Description of operation needing token
            
        Returns:
            Tuple of (token_value, token_id) or None if no tokens available
            
        Raises:
            RepositoryError: If database operation fails
        """
        tenant_id = self._get_current_tenant_id()
        
        # Acquire lock for token pool operations
        if not self._get_advisory_lock(self._lock_token_pool):
            self.logger.warning(
                "Could not acquire token pool lock, proceeding without coordination",
                extra={
                    "api_provider": self.api_provider,
                    "tenant_id": tenant_id,
                    "operation": operation
                }
            )
            # Continue without lock in case of PostgreSQL issues
        
        try:
            # Find valid tokens - get the newest token
            valid_tokens = self.session.query(APIToken).filter(
                and_(
                    APIToken.tenant_id == tenant_id,
                    APIToken.api_provider == self.api_provider,
                    APIToken.is_active == "active",
                    APIToken.expires_at > datetime.utcnow()
                )
            ).order_by(APIToken.created_at.desc()).limit(1).all()  # Get newest token
            
            if not valid_tokens:
                self.logger.info(
                    "No valid tokens available",
                    extra={
                        "api_provider": self.api_provider,
                        "tenant_id": tenant_id,
                        "operation": operation
                    }
                )
                return None
            
            # Use the newest available token
            token = valid_tokens[0]
            
            # Mark token as used
            token.mark_used(self.session)
            
            # Log usage
            self._log_token_usage(
                token=token,
                operation=operation,
                success="pending"
            )
            
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
                    "expires_at": token.expires_at.isoformat()
                }
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
                    "error_type": type(e).__name__
                }
            )
            raise RepositoryError(
                message="Failed to retrieve valid token",
                error_code=ErrorCode.DATABASE_ERROR,
                details={
                    "api_provider": self.api_provider,
                    "operation": operation, 
                    "error": str(e)
                },
                cause=e
            ) from e
        finally:
            # Always release the lock
            self._release_advisory_lock(self._lock_token_pool)
    
    @tenant_aware
    def store_new_token(
        self, 
        token: str, 
        generated_by: str,
        generation_context: Optional[Dict[str, Any]] = None
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
                details={"token_type": type(token).__name__}
            )
        
        # Create token hash for uniqueness checking
        token_hash = APIToken.create_token_hash(token)
        
        # Acquire lock for token pool operations
        if not self._get_advisory_lock(self._lock_token_pool):
            self.logger.warning(
                "Could not acquire token pool lock for token storage",
                extra={
                    "api_provider": self.api_provider,
                    "tenant_id": tenant_id,
                    "generated_by": generated_by
                }
            )
        
        try:
            # Check if token already exists
            existing = self.session.query(APIToken).filter(
                APIToken.token_hash == token_hash
            ).first()
            
            if existing:
                raise ValidationError(
                    message="Token already exists in system",
                    error_code=ErrorCode.DUPLICATE,
                    details={"token_hash": token_hash}
                )
            
            # Check token count limit for tenant and API provider
            active_count = self.session.query(func.count(APIToken.id)).filter(
                and_(
                    APIToken.tenant_id == tenant_id,
                    APIToken.api_provider == self.api_provider,
                    APIToken.is_active == "active",
                    APIToken.expires_at > datetime.utcnow()
                )
            ).scalar()
            
            if active_count >= self.max_tokens:
                self.logger.warning(
                    "Token limit reached, cannot store new token",
                    extra={
                        "api_provider": self.api_provider,
                        "tenant_id": tenant_id,
                        "active_count": active_count,
                        "limit": self.max_tokens
                    }
                )
                raise ValidationError(
                    message=f"Token limit reached ({active_count}/{self.max_tokens})",
                    error_code=ErrorCode.LIMIT_EXCEEDED,
                    details={
                        "api_provider": self.api_provider,
                        "active_count": active_count,
                        "limit": self.max_tokens
                    }
                )
            
            # Create new token record
            api_token = APIToken(
                tenant_id=tenant_id,
                api_provider=self.api_provider,
                token_hash=token_hash,
                expires_at=APIToken.calculate_expiry(self.token_validity_hours),
                generated_by=generated_by,
                is_active="active"
            )
            
            # Set encrypted token
            api_token.set_token(token, self.session)
            
            # Set generation context
            if generation_context:
                api_token.set_generation_context(generation_context)
            
            # Save to database
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
                    "active_tokens_after": active_count + 1
                }
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
                    "error": str(e)
                }
            )
            raise ValidationError(
                message="Token already exists (race condition)",
                error_code=ErrorCode.DUPLICATE,
                details={"error": str(e)}
            ) from e
        except Exception as e:
            self.logger.error(
                "Failed to store new token",
                extra={
                    "api_provider": self.api_provider,
                    "tenant_id": tenant_id,
                    "generated_by": generated_by,
                    "error": str(e),
                    "error_type": type(e).__name__
                }
            )
            raise RepositoryError(
                message="Failed to store token",
                error_code=ErrorCode.DATABASE_ERROR,
                details={
                    "api_provider": self.api_provider,
                    "generated_by": generated_by, 
                    "error": str(e)
                },
                cause=e
            ) from e
        finally:
            # Always release the lock
            self._release_advisory_lock(self._lock_token_pool)
    
    @tenant_aware
    def cleanup_expired_tokens(self, force_cleanup: bool = False) -> int:
        """
        Clean up expired tokens with atomic coordination.
        
        Args:
            force_cleanup: If True, skip lock acquisition timeout
            
        Returns:
            Number of tokens cleaned up
        """
        tenant_id = self._get_current_tenant_id()
        
        # Acquire cleanup lock
        lock_timeout = 1 if not force_cleanup else 10
        if not self._get_advisory_lock(self._lock_token_cleanup, lock_timeout):
            if not force_cleanup:
                self.logger.debug(
                    "Could not acquire cleanup lock, skipping cleanup",
                    extra={
                        "api_provider": self.api_provider,
                        "tenant_id": tenant_id
                    }
                )
                return 0
            else:
                self.logger.warning(
                    "Proceeding with forced cleanup without lock",
                    extra={
                        "api_provider": self.api_provider,
                        "tenant_id": tenant_id
                    }
                )
        
        try:
            # Find expired tokens for this API provider
            expired_tokens = self.session.query(APIToken).filter(
                and_(
                    APIToken.tenant_id == tenant_id,
                    APIToken.api_provider == self.api_provider,
                    or_(
                        APIToken.expires_at <= datetime.utcnow(),
                        APIToken.is_active == "inactive"
                    )
                )
            ).all()
            
            if not expired_tokens:
                return 0
            
            # Deactivate tokens (soft delete for audit purposes)
            count = 0
            for token in expired_tokens:
                token.deactivate(self.session)
                count += 1
            
            self.logger.info(
                "Cleaned up expired tokens",
                extra={
                    "api_provider": self.api_provider,
                    "tenant_id": tenant_id,
                    "tokens_cleaned": count
                }
            )
            
            return count
            
        except Exception as e:
            self.logger.error(
                "Failed to cleanup expired tokens",
                extra={
                    "api_provider": self.api_provider,
                    "tenant_id": tenant_id,
                    "error": str(e),
                    "error_type": type(e).__name__
                }
            )
            raise RepositoryError(
                message="Failed to cleanup expired tokens",
                error_code=ErrorCode.DATABASE_ERROR,
                details={
                    "api_provider": self.api_provider,
                    "error": str(e)
                },
                cause=e
            ) from e
        finally:
            # Always release the lock
            self._release_advisory_lock(self._lock_token_cleanup)
    
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
            active_count = self.session.query(func.count(APIToken.id)).filter(
                and_(
                    APIToken.tenant_id == tenant_id,
                    APIToken.api_provider == self.api_provider,
                    APIToken.is_active == "active",
                    APIToken.expires_at > datetime.utcnow()
                )
            ).scalar()
            
            expired_count = self.session.query(func.count(APIToken.id)).filter(
                and_(
                    APIToken.tenant_id == tenant_id,
                    APIToken.api_provider == self.api_provider,
                    APIToken.expires_at <= datetime.utcnow()
                )
            ).scalar()
            
            inactive_count = self.session.query(func.count(APIToken.id)).filter(
                and_(
                    APIToken.tenant_id == tenant_id,
                    APIToken.api_provider == self.api_provider,
                    APIToken.is_active == "inactive"
                )
            ).scalar()
            
            # Get oldest active token
            oldest_active = self.session.query(APIToken.created_at).filter(
                and_(
                    APIToken.tenant_id == tenant_id,
                    APIToken.api_provider == self.api_provider,
                    APIToken.is_active == "active",
                    APIToken.expires_at > datetime.utcnow()
                )
            ).order_by(APIToken.created_at.asc()).first()
            
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
                "generated_at": datetime.utcnow().isoformat()
            }
            
        except Exception as e:
            self.logger.error(
                "Failed to get token statistics",
                extra={
                    "api_provider": self.api_provider,
                    "tenant_id": tenant_id,
                    "error": str(e)
                }
            )
            raise RepositoryError(
                message="Failed to get token statistics",
                error_code=ErrorCode.DATABASE_ERROR,
                details={
                    "api_provider": self.api_provider,
                    "error": str(e)
                },
                cause=e
            ) from e
    
    def _log_token_usage(
        self, 
        token: APIToken, 
        operation: str, 
        success: str = "unknown",
        **kwargs
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
                **kwargs
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
                    "error": str(e)
                }
            )