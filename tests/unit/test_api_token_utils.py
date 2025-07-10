"""
Unit tests for API token utilities.

Tests the API token utility functions that provide business logic
using the generic CRUD helpers.
"""

import pytest
from datetime import datetime, timezone, timedelta
from sqlalchemy.orm import Session

from api_exchange_core.db.db_api_token_models import APIToken
from api_exchange_core.exceptions import TokenNotAvailableError
from api_exchange_core.utils.api_token_utils import (
    cleanup_expired_tokens,
    get_token_statistics,
    get_valid_token,
    store_token,
)


class TestGetValidToken:
    """Test get_valid_token function."""
    
    def test_get_valid_token_success(self, db_session: Session):
        """Test getting a valid token."""
        # Create a valid token
        future_date = datetime.now(timezone.utc) + timedelta(days=30)
        token = APIToken(
            tenant_id="test-tenant",
            api_provider="azure",
            token_value=b"valid_token_123",
            expires_at=future_date,
            is_active=True
        )
        db_session.add(token)
        db_session.commit()
        
        # Get the token
        token_value, token_id = get_valid_token(
            db_session, "test-tenant", "azure", "test_operation"
        )
        
        # SQLite may return EncryptedBinary as string instead of bytes
        assert token_value in [b"valid_token_123", "valid_token_123"]
        assert token_id == token.id
        
        # No automatic usage logging - tokens can be reused
    
    def test_get_valid_token_no_tokens(self, db_session: Session):
        """Test getting token when none exist."""
        with pytest.raises(TokenNotAvailableError) as exc_info:
            get_valid_token(db_session, "test-tenant", "nonexistent", "test_op")
        
        error = exc_info.value
        assert "No valid tokens available for nonexistent" in error.message
        assert error.context["tenant_id"] == "test-tenant"
        assert error.context["api_provider"] == "nonexistent"
    
    def test_get_valid_token_all_expired(self, db_session: Session):
        """Test getting token when all tokens are expired."""
        # Create an expired token
        past_date = datetime.now(timezone.utc) - timedelta(days=1)
        token = APIToken(
            tenant_id="test-tenant",
            api_provider="azure",
            token_value=b"expired_token",
            expires_at=past_date,
            is_active=True
        )
        db_session.add(token)
        db_session.commit()
        
        with pytest.raises(TokenNotAvailableError) as exc_info:
            get_valid_token(db_session, "test-tenant", "azure")
        
        error = exc_info.value
        assert "No valid tokens available for azure" in error.message
    
    def test_get_valid_token_inactive_tokens(self, db_session: Session):
        """Test getting token when all tokens are inactive."""
        # Create an inactive token
        future_date = datetime.now(timezone.utc) + timedelta(days=30)
        token = APIToken(
            tenant_id="test-tenant",
            api_provider="azure",
            token_value=b"inactive_token",
            expires_at=future_date,
            is_active=False
        )
        db_session.add(token)
        db_session.commit()
        
        with pytest.raises(TokenNotAvailableError):
            get_valid_token(db_session, "test-tenant", "azure")
    
    def test_get_valid_token_tenant_isolation(self, db_session: Session):
        """Test that tenant isolation works correctly."""
        # Create token for different tenant
        future_date = datetime.now(timezone.utc) + timedelta(days=30)
        token = APIToken(
            tenant_id="other-tenant",
            api_provider="azure",
            token_value=b"other_tenant_token",
            expires_at=future_date,
            is_active=True
        )
        db_session.add(token)
        db_session.commit()
        
        # Try to get token for different tenant
        with pytest.raises(TokenNotAvailableError):
            get_valid_token(db_session, "test-tenant", "azure")


class TestStoreToken:
    """Test store_token function."""
    
    def test_store_token_success(self, db_session: Session):
        """Test storing a new token."""
        future_date = datetime.now(timezone.utc) + timedelta(days=30)
        
        token_id = store_token(
            db_session,
            "test-tenant",
            "azure",
            "new_token_value",
            future_date,
            {"scope": "read"}
        )
        
        # Verify token was stored
        stored_token = db_session.query(APIToken).filter_by(id=token_id).first()
        assert stored_token is not None
        assert stored_token.tenant_id == "test-tenant"
        assert stored_token.api_provider == "azure"
        assert stored_token.token_value in ["new_token_value", b"new_token_value"]
        assert stored_token.expires_at.replace(tzinfo=None) == future_date.replace(tzinfo=None)
        assert stored_token.is_active is True
        assert stored_token.context == {"scope": "read"}
    
    def test_store_token_minimal(self, db_session: Session):
        """Test storing token with minimal parameters."""
        future_date = datetime.now(timezone.utc) + timedelta(days=30)
        
        token_id = store_token(
            db_session,
            "test-tenant",
            "azure",
            "minimal_token",
            future_date
        )
        
        stored_token = db_session.query(APIToken).filter_by(id=token_id).first()
        assert stored_token is not None
        assert stored_token.context == {}


class TestCleanupExpiredTokens:
    """Test cleanup_expired_tokens function."""
    
    def test_cleanup_expired_tokens_success(self, db_session: Session):
        """Test cleaning up expired tokens."""
        past_date = datetime.now(timezone.utc) - timedelta(days=1)
        future_date = datetime.now(timezone.utc) + timedelta(days=30)
        
        # Create expired token
        expired_token = APIToken(
            tenant_id="test-tenant",
            api_provider="azure",
            token_value=b"expired_token",
            expires_at=past_date,
            is_active=True
        )
        
        # Create valid token
        valid_token = APIToken(
            tenant_id="test-tenant",
            api_provider="azure",
            token_value=b"valid_token",
            expires_at=future_date,
            is_active=True
        )
        
        db_session.add_all([expired_token, valid_token])
        db_session.commit()
        
        expired_id = expired_token.id
        valid_id = valid_token.id
        
        # Cleanup expired tokens
        deleted_count = cleanup_expired_tokens(db_session, "test-tenant")
        
        assert deleted_count == 1
        
        # Verify expired token was deleted
        assert db_session.query(APIToken).filter_by(id=expired_id).first() is None
        
        # Verify valid token still exists
        assert db_session.query(APIToken).filter_by(id=valid_id).first() is not None
    
    def test_cleanup_expired_tokens_with_provider_filter(self, db_session: Session):
        """Test cleanup with provider filter."""
        past_date = datetime.now(timezone.utc) - timedelta(days=1)
        
        # Create expired tokens for different providers
        azure_token = APIToken(
            tenant_id="test-tenant",
            api_provider="azure",
            token_value=b"expired_azure",
            expires_at=past_date,
            is_active=True
        )
        
        aws_token = APIToken(
            tenant_id="test-tenant",
            api_provider="aws",
            token_value=b"expired_aws",
            expires_at=past_date,
            is_active=True
        )
        
        db_session.add_all([azure_token, aws_token])
        db_session.commit()
        
        # Cleanup only Azure tokens
        deleted_count = cleanup_expired_tokens(
            db_session, "test-tenant", "azure"
        )
        
        assert deleted_count == 1
        
        # Verify Azure token was deleted, AWS token remains
        assert db_session.query(APIToken).filter_by(
            tenant_id="test-tenant", api_provider="azure"
        ).first() is None
        
        assert db_session.query(APIToken).filter_by(
            tenant_id="test-tenant", api_provider="aws"
        ).first() is not None
    
    def test_cleanup_no_expired_tokens(self, db_session: Session):
        """Test cleanup when no tokens are expired."""
        future_date = datetime.now(timezone.utc) + timedelta(days=30)
        
        token = APIToken(
            tenant_id="test-tenant",
            api_provider="azure",
            token_value=b"valid_token",
            expires_at=future_date,
            is_active=True
        )
        db_session.add(token)
        db_session.commit()
        
        deleted_count = cleanup_expired_tokens(db_session, "test-tenant")
        
        assert deleted_count == 0
        assert db_session.query(APIToken).filter_by(tenant_id="test-tenant").count() == 1


class TestGetTokenStatistics:
    """Test get_token_statistics function."""
    
    def test_get_token_statistics_success(self, db_session: Session):
        """Test getting token statistics."""
        past_date = datetime.now(timezone.utc) - timedelta(days=1)
        future_date = datetime.now(timezone.utc) + timedelta(days=30)
        
        # Create various tokens
        tokens = [
            # Active, valid token
            APIToken(
                tenant_id="test-tenant",
                api_provider="azure",
                token_value=b"active_valid",
                expires_at=future_date,
                is_active=True
            ),
            # Active, expired token
            APIToken(
                tenant_id="test-tenant",
                api_provider="azure",
                token_value=b"active_expired",
                expires_at=past_date,
                is_active=True
            ),
            # Inactive, valid token
            APIToken(
                tenant_id="test-tenant",
                api_provider="azure",
                token_value=b"inactive_valid",
                expires_at=future_date,
                is_active=False
            ),
        ]
        
        db_session.add_all(tokens)
        db_session.commit()
        
        stats = get_token_statistics(db_session, "test-tenant", "azure")
        
        assert stats["total_tokens"] == 3
        assert stats["active_tokens"] == 2  # 2 active tokens
        assert stats["expired_tokens"] == 1  # 1 expired token
        assert stats["tenant_id"] == "test-tenant"
        assert stats["api_provider"] == "azure"
    
    def test_get_token_statistics_all_providers(self, db_session: Session):
        """Test getting statistics for all providers."""
        future_date = datetime.now(timezone.utc) + timedelta(days=30)
        
        # Create tokens for different providers
        tokens = [
            APIToken(
                tenant_id="test-tenant",
                api_provider="azure",
                token_value=b"azure_token",
                expires_at=future_date,
                is_active=True
            ),
            APIToken(
                tenant_id="test-tenant",
                api_provider="aws",
                token_value=b"aws_token",
                expires_at=future_date,
                is_active=True
            ),
        ]
        
        db_session.add_all(tokens)
        db_session.commit()
        
        # Get stats for all providers (no provider filter)
        stats = get_token_statistics(db_session, "test-tenant")
        
        assert stats["total_tokens"] == 2
        assert stats["active_tokens"] == 2
        assert stats["expired_tokens"] == 0
        assert stats["tenant_id"] == "test-tenant"
        assert stats["api_provider"] is None
    
    def test_get_token_statistics_empty(self, db_session: Session):
        """Test statistics when no tokens exist."""
        stats = get_token_statistics(db_session, "empty-tenant", "azure")
        
        assert stats["total_tokens"] == 0
        assert stats["active_tokens"] == 0
        assert stats["expired_tokens"] == 0
        assert stats["tenant_id"] == "empty-tenant"
        assert stats["api_provider"] == "azure"
    
    def test_get_token_statistics_tenant_isolation(self, db_session: Session):
        """Test that statistics respect tenant isolation."""
        future_date = datetime.now(timezone.utc) + timedelta(days=30)
        
        # Create tokens for different tenants
        tokens = [
            APIToken(
                tenant_id="tenant-1",
                api_provider="azure",
                token_value=b"tenant1_token",
                expires_at=future_date,
                is_active=True
            ),
            APIToken(
                tenant_id="tenant-2",
                api_provider="azure",
                token_value=b"tenant2_token",
                expires_at=future_date,
                is_active=True
            ),
        ]
        
        db_session.add_all(tokens)
        db_session.commit()
        
        # Get stats for tenant-1 only
        stats = get_token_statistics(db_session, "tenant-1", "azure")
        
        assert stats["total_tokens"] == 1
        assert stats["active_tokens"] == 1
        assert stats["tenant_id"] == "tenant-1"


class TestAPITokenUtilsIntegration:
    """Integration tests for API token utilities."""
    
    def test_token_lifecycle(self, db_session: Session):
        """Test complete token lifecycle: store, use, cleanup."""
        future_date = datetime.now(timezone.utc) + timedelta(days=30)
        
        # 1. Store a token
        token_id = store_token(
            db_session,
            "test-tenant",
            "azure",
            "lifecycle_token",
            future_date,
            {"purpose": "testing"}
        )
        
        # 2. Use the token
        token_value, retrieved_id = get_valid_token(
            db_session, "test-tenant", "azure", "lifecycle_test"
        )
        
        assert retrieved_id == token_id
        assert token_value in ["lifecycle_token", b"lifecycle_token"]
        
        # 3. No automatic usage logging - tokens can be reused
        
        # 4. Get statistics
        stats = get_token_statistics(db_session, "test-tenant", "azure")
        assert stats["total_tokens"] == 1
        assert stats["active_tokens"] == 1
        assert stats["expired_tokens"] == 0
        
        # 5. Simulate token expiration and cleanup
        stored_token = db_session.query(APIToken).filter_by(id=token_id).first()
        stored_token.expires_at = datetime.now(timezone.utc) - timedelta(days=1)
        db_session.commit()
        
        deleted_count = cleanup_expired_tokens(db_session, "test-tenant")
        assert deleted_count == 1
        
        # 6. Verify token is gone
        assert db_session.query(APIToken).filter_by(id=token_id).first() is None