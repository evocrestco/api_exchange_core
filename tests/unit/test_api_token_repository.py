"""
Tests for generic API token repository.
"""

import pytest
from datetime import datetime, timedelta
from unittest.mock import Mock

from src.db.db_api_token_models import APIToken, APITokenUsageLog
from src.repositories.api_token_repository import APITokenRepository
from src.context.tenant_context import tenant_context
from src.exceptions import ValidationError, RepositoryError


class TestAPITokenRepository:
    """Test APITokenRepository functionality."""
    
    @pytest.fixture
    def test_api_provider_repo(self, db_session):
        """Create repository configured for test API provider (25 tokens, 1 hour)."""
        return APITokenRepository(
            session=db_session,
            api_provider="test_api_provider",
            max_tokens=25,
            token_validity_hours=1
        )
    
    @pytest.fixture  
    def shopify_repo(self, db_session):
        """Create repository configured for Shopify (10 tokens, 2 hours)."""
        return APITokenRepository(
            session=db_session,
            api_provider="shopify",
            max_tokens=10,
            token_validity_hours=2
        )
    
    def test_store_new_token_success(self, test_api_provider_repo, test_tenant):
        """Test storing a new token successfully."""
        with tenant_context(test_tenant["id"]):
            token_id = test_api_provider_repo.store_new_token(
                token="test_token_123456",
                generated_by="test_function",
                generation_context={"reason": "initial_token"}
            )
            
            assert token_id is not None
            
            # Verify token was stored correctly
            stored_token = test_api_provider_repo.session.query(APIToken).filter(
                APIToken.id == token_id
            ).first()
            
            assert stored_token is not None
            assert stored_token.tenant_id == test_tenant["id"]
            assert stored_token.api_provider == "test_api_provider"
            assert stored_token.is_active == "active"
            assert stored_token.generated_by == "test_function"
            assert stored_token.get_token(test_api_provider_repo.session) == "test_token_123456"
    
    def test_store_duplicate_token_fails(self, test_api_provider_repo, test_tenant):
        """Test that storing duplicate token fails."""
        with tenant_context(test_tenant["id"]):
            token = "duplicate_token_123"
            
            # Store first token
            test_api_provider_repo.store_new_token(
                token=token,
                generated_by="first_function"
            )
            
            # Try to store same token again
            with pytest.raises(ValidationError) as exc_info:
                test_api_provider_repo.store_new_token(
                    token=token,
                    generated_by="second_function"
                )
            
            assert "already exists" in str(exc_info.value)
    
    def test_token_limit_enforcement(self, shopify_repo, test_tenant):
        """Test that token limits are enforced."""
        with tenant_context(test_tenant["id"]):
            # Store tokens up to the limit (10 for shopify_repo)
            for i in range(10):
                shopify_repo.store_new_token(
                    token=f"shopify_token_{i}",
                    generated_by="limit_test"
                )
            
            # Try to store one more token (should fail)
            with pytest.raises(ValidationError) as exc_info:
                shopify_repo.store_new_token(
                    token="shopify_token_overflow",
                    generated_by="limit_test"
                )
            
            assert "limit reached" in str(exc_info.value).lower()
            assert "10/10" in str(exc_info.value)
    
    def test_get_valid_token_success(self, test_api_provider_repo, test_tenant):
        """Test retrieving a valid token."""
        with tenant_context(test_tenant["id"]):
            # Store a token
            stored_token_id = test_api_provider_repo.store_new_token(
                token="valid_token_123",
                generated_by="test_function"
            )
            
            # Retrieve the token
            result = test_api_provider_repo.get_valid_token("test_operation")
            
            assert result is not None
            token_value, token_id = result
            assert token_value == "valid_token_123"
            assert token_id == stored_token_id
            
            # Verify usage was tracked
            stored_token = test_api_provider_repo.session.query(APIToken).filter(
                APIToken.id == stored_token_id
            ).first()
            assert stored_token.usage_count == 1
            assert stored_token.last_used_at is not None
    
    def test_get_valid_token_no_tokens(self, test_api_provider_repo, test_tenant):
        """Test retrieving token when none are available."""
        with tenant_context(test_tenant["id"]):
            result = test_api_provider_repo.get_valid_token("test_operation")
            assert result is None
    
    def test_get_valid_token_newest_first(self, test_api_provider_repo, test_tenant):
        """Test that newest token is returned."""
        with tenant_context(test_tenant["id"]):
            # Store multiple tokens
            token1_id = test_api_provider_repo.store_new_token(
                token="token_1",
                generated_by="test"
            )
            token2_id = test_api_provider_repo.store_new_token(
                token="token_2", 
                generated_by="test"
            )
            token3_id = test_api_provider_repo.store_new_token(
                token="token_3",
                generated_by="test"
            )
            
            # Should get the newest token (token_3)
            result = test_api_provider_repo.get_valid_token("operation_1")
            token_value, token_id = result
            assert token_id == token3_id
            assert token_value == "token_3"
    
    def test_cleanup_expired_tokens(self, test_api_provider_repo, test_tenant):
        """Test cleanup of expired tokens."""
        with tenant_context(test_tenant["id"]):
            # Create expired token
            expired_token = APIToken(
                tenant_id=test_tenant["id"],
                api_provider="test_api_provider",
                token_hash=APIToken.create_token_hash("expired_token"),
                expires_at=datetime.utcnow() - timedelta(hours=1),  # Expired
                is_active="active"
            )
            expired_token.set_token("expired_token", test_api_provider_repo.session)
            
            # Create valid token  
            valid_token = APIToken(
                tenant_id=test_tenant["id"],
                api_provider="test_api_provider",
                token_hash=APIToken.create_token_hash("valid_token"),
                expires_at=datetime.utcnow() + timedelta(hours=1),  # Valid
                is_active="active"
            )
            valid_token.set_token("valid_token", test_api_provider_repo.session)
            
            test_api_provider_repo.session.add(expired_token)
            test_api_provider_repo.session.add(valid_token)
            test_api_provider_repo.session.flush()
            
            # Cleanup
            cleaned_count = test_api_provider_repo.cleanup_expired_tokens()
            
            assert cleaned_count == 1
            
            # Verify expired token was deactivated
            test_api_provider_repo.session.refresh(expired_token)
            test_api_provider_repo.session.refresh(valid_token)
            
            assert expired_token.is_active == "inactive"
            assert valid_token.is_active == "active"
    
    def test_get_token_stats(self, test_api_provider_repo, test_tenant):
        """Test token statistics generation."""
        with tenant_context(test_tenant["id"]):
            # Create various tokens
            # Active token
            test_api_provider_repo.store_new_token("active_token", "test")
            
            # Expired token
            expired_token = APIToken(
                tenant_id=test_tenant["id"],
                api_provider="test_api_provider",
                token_hash=APIToken.create_token_hash("expired_token"),
                expires_at=datetime.utcnow() - timedelta(hours=1),
                is_active="active"
            )
            expired_token.set_token("expired_token", test_api_provider_repo.session)
            test_api_provider_repo.session.add(expired_token)
            
            # Inactive token
            inactive_token = APIToken(
                tenant_id=test_tenant["id"],
                api_provider="test_api_provider", 
                token_hash=APIToken.create_token_hash("inactive_token"),
                expires_at=datetime.utcnow() + timedelta(hours=1),
                is_active="inactive"
            )
            inactive_token.set_token("inactive_token", test_api_provider_repo.session)
            test_api_provider_repo.session.add(inactive_token)
            
            test_api_provider_repo.session.flush()
            
            # Get stats
            stats = test_api_provider_repo.get_token_stats()
            
            assert stats["api_provider"] == "test_api_provider"
            assert stats["tenant_id"] == test_tenant["id"]
            assert stats["active_tokens"] == 1
            assert stats["expired_tokens"] == 1
            assert stats["inactive_tokens"] == 1
            assert stats["total_tokens"] == 3
            assert stats["available_slots"] == 24  # 25 - 1 active
            assert stats["max_tokens_allowed"] == 25
    
    def test_different_api_providers_isolated(self, test_api_provider_repo, shopify_repo, test_tenant):
        """Test that different API providers are properly isolated."""
        with tenant_context(test_tenant["id"]):
            # Store tokens for different providers
            test_api_token_id = test_api_provider_repo.store_new_token(
                token="test_token_123",
                generated_by="test_api_test"
            )
            
            shopify_token_id = shopify_repo.store_new_token(
                token="shopify_token_456", 
                generated_by="shopify_test"
            )
            
            # test API provider repo should only see test API tokens
            test_api_result = test_api_provider_repo.get_valid_token("test_api_operation")
            assert test_api_result is not None
            assert test_api_result[0] == "test_token_123"
            
            # Shopify repo should only see Shopify tokens
            shopify_result = shopify_repo.get_valid_token("shopify_operation")
            assert shopify_result is not None
            assert shopify_result[0] == "shopify_token_456"
            
            # Stats should be isolated
            test_api_stats = test_api_provider_repo.get_token_stats()
            shopify_stats = shopify_repo.get_token_stats()
            
            assert test_api_stats["active_tokens"] == 1
            assert shopify_stats["active_tokens"] == 1
            assert test_api_stats["api_provider"] == "test_api_provider"
            assert shopify_stats["api_provider"] == "shopify"
    
    def test_tenant_isolation(self, test_api_provider_repo, test_tenant):
        """Test that tenants are properly isolated."""
        # Store token for first tenant
        with tenant_context(test_tenant["id"]):
            test_api_provider_repo.store_new_token(
                token="tenant1_token",
                generated_by="tenant1_test"
            )
        
        # Try to access from different tenant
        with tenant_context("different_tenant"):
            result = test_api_provider_repo.get_valid_token("tenant2_operation")
            assert result is None  # Should not see tenant1's token
    
    def test_usage_logging(self, test_api_provider_repo, test_tenant):
        """Test that token usage is properly logged."""
        with tenant_context(test_tenant["id"]):
            # Store and use a token
            token_id = test_api_provider_repo.store_new_token(
                token="logged_token",
                generated_by="logging_test"
            )
            
            test_api_provider_repo.get_valid_token("test_logging_operation")
            
            # Check usage log was created
            usage_logs = test_api_provider_repo.session.query(APITokenUsageLog).filter(
                APITokenUsageLog.token_id == token_id
            ).all()
            
            assert len(usage_logs) == 1
            log = usage_logs[0]
            assert log.api_provider == "test_api_provider"
            assert log.operation == "test_logging_operation"
            assert log.success == "pending"
    
    def test_invalid_token_validation(self, test_api_provider_repo, test_tenant):
        """Test validation of invalid tokens."""
        with tenant_context(test_tenant["id"]):
            # Test empty token
            with pytest.raises(ValidationError):
                test_api_provider_repo.store_new_token(
                    token="",
                    generated_by="validation_test"
                )
            
            # Test None token
            with pytest.raises(ValidationError):
                test_api_provider_repo.store_new_token(
                    token=None,
                    generated_by="validation_test"
                )
            
            # Test non-string token
            with pytest.raises(ValidationError):
                test_api_provider_repo.store_new_token(
                    token=12345,
                    generated_by="validation_test"
                )