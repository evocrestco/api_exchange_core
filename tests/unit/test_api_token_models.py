"""
Tests for generic API token models.
"""

import pytest
from datetime import datetime, timedelta

from src.db.db_api_token_models import APIToken, APITokenUsageLog
from src.context.tenant_context import tenant_context


class TestAPIToken:
    """Test APIToken model functionality."""
    
    def test_token_creation(self, db_session, test_tenant):
        """Test basic token creation."""
        with tenant_context(test_tenant["id"]):
            token = APIToken(
                tenant_id=test_tenant["id"],
                api_provider="test_api",
                token_hash=APIToken.create_token_hash("test_token_123"),
                expires_at=datetime.utcnow() + timedelta(hours=1)
            )
            
            token.set_token("test_token_123", db_session)
            
            db_session.add(token)
            db_session.flush()
            
            assert token.id is not None
            assert token.tenant_id == test_tenant["id"]
            assert token.api_provider == "test_api"
            assert token.is_active == "active"
            assert token.usage_count == 0
            
            # Test token retrieval
            retrieved_token = token.get_token(db_session)
            assert retrieved_token == "test_token_123"
    
    def test_token_hash_uniqueness(self):
        """Test token hash generation for uniqueness."""
        token1 = "test_token_123"
        token2 = "test_token_456"
        token3 = "test_token_123"  # Same as token1
        
        hash1 = APIToken.create_token_hash(token1)
        hash2 = APIToken.create_token_hash(token2)
        hash3 = APIToken.create_token_hash(token3)
        
        assert hash1 != hash2  # Different tokens have different hashes
        assert hash1 == hash3  # Same tokens have same hashes
        assert len(hash1) == 64  # SHA256 hash length
    
    def test_token_expiration_check(self, db_session, test_tenant):
        """Test token expiration logic."""
        with tenant_context(test_tenant["id"]):
            # Create expired token
            expired_token = APIToken(
                tenant_id=test_tenant["id"],
                api_provider="test_api",
                token_hash=APIToken.create_token_hash("expired_token"),
                expires_at=datetime.utcnow() - timedelta(hours=1),  # Expired 1 hour ago
                is_active="active"
            )
            expired_token.set_token("expired_token", db_session)
            
            # Create valid token
            valid_token = APIToken(
                tenant_id=test_tenant["id"],
                api_provider="test_api", 
                token_hash=APIToken.create_token_hash("valid_token"),
                expires_at=datetime.utcnow() + timedelta(hours=1),  # Expires in 1 hour
                is_active="active"
            )
            valid_token.set_token("valid_token", db_session)
            
            assert expired_token.is_expired() is True
            assert expired_token.is_valid() is False
            
            assert valid_token.is_expired() is False
            assert valid_token.is_valid() is True
    
    def test_token_usage_tracking(self, db_session, test_tenant):
        """Test token usage counting and timestamp updates."""
        with tenant_context(test_tenant["id"]):
            token = APIToken(
                tenant_id=test_tenant["id"],
                api_provider="test_api",
                token_hash=APIToken.create_token_hash("usage_token"),
                expires_at=datetime.utcnow() + timedelta(hours=1)
            )
            token.set_token("usage_token", db_session)
            
            db_session.add(token)
            db_session.flush()
            
            # Initial state
            assert token.usage_count == 0
            assert token.last_used_at is None
            
            # Mark as used
            before_use = datetime.utcnow()
            token.mark_used(db_session)
            after_use = datetime.utcnow()
            
            assert token.usage_count == 1
            assert token.last_used_at is not None
            assert before_use <= token.last_used_at <= after_use
            
            # Use again
            token.mark_used(db_session)
            assert token.usage_count == 2
    
    def test_token_deactivation(self, db_session, test_tenant):
        """Test token deactivation (soft delete)."""
        with tenant_context(test_tenant["id"]):
            token = APIToken(
                tenant_id=test_tenant["id"],
                api_provider="test_api",
                token_hash=APIToken.create_token_hash("deactivate_token"),
                expires_at=datetime.utcnow() + timedelta(hours=1)
            )
            token.set_token("deactivate_token", db_session)
            
            db_session.add(token)
            db_session.flush()
            
            # Initially active
            assert token.is_active == "active"
            assert token.is_valid() is True
            
            # Deactivate
            token.deactivate(db_session)
            
            assert token.is_active == "inactive"
            assert token.is_valid() is False  # Not valid even if not expired
    
    def test_generation_context(self, db_session, test_tenant):
        """Test token generation context metadata."""
        with tenant_context(test_tenant["id"]):
            token = APIToken(
                tenant_id=test_tenant["id"],
                api_provider="test_api",
                token_hash=APIToken.create_token_hash("context_token"),
                expires_at=datetime.utcnow() + timedelta(hours=1)
            )
            token.set_token("context_token", db_session)
            
            # Set generation context
            context = {
                "generated_by": "test_function",
                "reason": "no_valid_tokens",
                "timestamp": datetime.utcnow().isoformat()
            }
            token.set_generation_context(context)
            
            db_session.add(token)
            db_session.flush()
            
            # Retrieve context
            retrieved_context = token.get_generation_context()
            assert retrieved_context == context
            assert retrieved_context["generated_by"] == "test_function"
    
    def test_calculate_expiry(self):
        """Test expiry calculation utility."""
        before_calc = datetime.utcnow()
        expiry_1h = APIToken.calculate_expiry(1)
        expiry_24h = APIToken.calculate_expiry(24)
        after_calc = datetime.utcnow()
        
        # Check that expiry times are reasonable
        expected_1h_min = before_calc + timedelta(hours=1)
        expected_1h_max = after_calc + timedelta(hours=1)
        
        assert expected_1h_min <= expiry_1h <= expected_1h_max
        
        # 24 hour expiry should be later than 1 hour expiry
        assert expiry_24h > expiry_1h
        
        # Difference should be approximately 23 hours
        diff = expiry_24h - expiry_1h
        assert timedelta(hours=22, minutes=59) <= diff <= timedelta(hours=23, minutes=1)


class TestAPITokenUsageLog:
    """Test APITokenUsageLog model functionality."""
    
    def test_usage_log_creation(self, db_session, test_tenant):
        """Test basic usage log creation."""
        with tenant_context(test_tenant["id"]):
            usage_log = APITokenUsageLog.create_usage_record(
                tenant_id=test_tenant["id"],
                api_provider="test_api",
                token_id="token_123",
                token_hash="hash_123",
                operation="list_orders",
                endpoint="/api/orders",
                response_status=200,
                success="success"
            )
            
            db_session.add(usage_log)
            db_session.flush()
            
            assert usage_log.id is not None
            assert usage_log.tenant_id == test_tenant["id"]
            assert usage_log.api_provider == "test_api"
            assert usage_log.token_id == "token_123"
            assert usage_log.operation == "list_orders"
            assert usage_log.response_status == 200
            assert usage_log.success == "success"
    
    def test_usage_context_metadata(self, db_session, test_tenant):
        """Test usage context metadata handling."""
        with tenant_context(test_tenant["id"]):
            usage_log = APITokenUsageLog.create_usage_record(
                tenant_id=test_tenant["id"],
                api_provider="test_api", 
                token_id="token_123",
                token_hash="hash_123",
                operation="get_order_details"
            )
            
            # Set usage context
            context = {
                "function_name": "process_order",
                "correlation_id": "corr_123",
                "request_size": 1024,
                "custom_metadata": {"order_id": "ord_456"}
            }
            usage_log.set_usage_context(context)
            
            db_session.add(usage_log)
            db_session.flush()
            
            # Retrieve context
            retrieved_context = usage_log.get_usage_context()
            assert retrieved_context == context
            assert retrieved_context["function_name"] == "process_order"
            assert retrieved_context["custom_metadata"]["order_id"] == "ord_456"
    
    def test_usage_log_empty_context(self, db_session, test_tenant):
        """Test usage log with no context."""
        with tenant_context(test_tenant["id"]):
            usage_log = APITokenUsageLog.create_usage_record(
                tenant_id=test_tenant["id"],
                api_provider="test_api",
                token_id="token_123", 
                token_hash="hash_123",
                operation="test_operation"
            )
            
            # No context set
            context = usage_log.get_usage_context()
            assert context == {}
    
    def test_usage_log_performance_tracking(self, db_session, test_tenant):
        """Test performance tracking fields."""
        with tenant_context(test_tenant["id"]):
            usage_log = APITokenUsageLog.create_usage_record(
                tenant_id=test_tenant["id"],
                api_provider="test_api",
                token_id="token_123",
                token_hash="hash_123", 
                operation="performance_test",
                request_duration_ms=1500,
                response_status=200,
                success="success",
                correlation_id="perf_test_123"
            )
            
            db_session.add(usage_log)
            db_session.flush()
            
            assert usage_log.request_duration_ms == 1500
            assert usage_log.response_status == 200
            assert usage_log.success == "success"
            assert usage_log.correlation_id == "perf_test_123"