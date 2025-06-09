"""
Tests for token management functionality .

Following NO MOCKS policy - tests use real implementations and PostgreSQL.
Tests cover:
- ExternalAccessToken model 
- CredentialRepository token methods
- CredentialService token methods
- TokenManagementConfig schema validation
"""

import pytest
import os
from datetime import datetime, timedelta

from src.db.db_config import import_all_models
from src.db.db_credential_models import ExternalAccessToken
from src.repositories.credential_repository import CredentialRepository
from src.services.credential_service import CredentialService
from src.context.tenant_context import TenantContext
from src.schemas.tenant_schema import TokenManagementConfig, get_token_management_config, set_token_management_config
from pydantic import ValidationError as PydanticValidationError
from src.exceptions import (
    ValidationError, 
    ServiceError,
    RepositoryError
)

# Initialize models properly
import_all_models()


class TestExternalAccessTokenModel:
    """Test ExternalAccessToken model ."""

    @pytest.mark.skipif(os.getenv("DB_TYPE", "sqlite") == "sqlite", reason="Encryption tests require PostgreSQL")
    def test_set_and_get_access_token(self, db_session, test_tenant):
        """Test token encryption and decryption."""
        TenantContext.set_current_tenant(test_tenant["id"])
        
        # Create token record
        expires_at = datetime.utcnow() + timedelta(hours=1)
        token_record = ExternalAccessToken(
            tenant_id=test_tenant["id"],
            system_name="test_system",
            expires_at=expires_at
        )
        
        # Set access token (should encrypt)
        test_token = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.test_payload.signature"
        token_record.set_access_token(test_token, db_session)
        
        # Save to database
        db_session.add(token_record)
        db_session.flush()
        
        # Verify encrypted data exists
        assert token_record._encrypted_access_token is not None
        assert token_record._encrypted_access_token != test_token  # Should be encrypted
        
        # Retrieve and decrypt token
        decrypted_token = token_record.get_access_token(db_session)
        assert decrypted_token == test_token
        
        TenantContext.clear_current_tenant()

    def test_is_expired_and_is_valid(self, db_session, test_tenant):
        """Test token expiration checks."""
        TenantContext.set_current_tenant(test_tenant["id"])
        
        # Create expired token
        expired_token = ExternalAccessToken(
            tenant_id=test_tenant["id"],
            system_name="test_system",
            expires_at=datetime.utcnow() - timedelta(minutes=10)
        )
        
        assert expired_token.is_expired() is True
        assert expired_token.is_valid() is False
        
        # Create valid token
        valid_token = ExternalAccessToken(
            tenant_id=test_tenant["id"],
            system_name="test_system",
            expires_at=datetime.utcnow() + timedelta(hours=1)
        )
        
        assert valid_token.is_expired() is False
        assert valid_token.is_valid() is True
        
        TenantContext.clear_current_tenant()

    @pytest.mark.skipif(os.getenv("DB_TYPE", "sqlite") == "sqlite", reason="Encryption tests require PostgreSQL")
    def test_tenant_specific_encryption(self, db_session, test_tenant, multi_tenant_context):
        """Test that different tenants use different encryption keys."""
        test_token = "shared_token_value"
        
        # Create token for first tenant
        TenantContext.set_current_tenant(test_tenant["id"])
        token1 = ExternalAccessToken(
            tenant_id=test_tenant["id"],
            system_name="test_system",
            expires_at=datetime.utcnow() + timedelta(hours=1)
        )
        token1.set_access_token(test_token, db_session)
        db_session.add(token1)
        db_session.flush()
        
        # Create token for second tenant
        tenant2_id = multi_tenant_context[1]["id"]
        TenantContext.set_current_tenant(tenant2_id)
        token2 = ExternalAccessToken(
            tenant_id=tenant2_id,
            system_name="test_system",
            expires_at=datetime.utcnow() + timedelta(hours=1)
        )
        token2.set_access_token(test_token, db_session)
        db_session.add(token2)
        db_session.flush()
        
        # Encrypted values should be different (different tenant keys)
        assert token1._encrypted_access_token != token2._encrypted_access_token
        
        # But decrypted values should be the same
        TenantContext.set_current_tenant(test_tenant["id"])
        assert token1.get_access_token(db_session) == test_token
        
        TenantContext.set_current_tenant(tenant2_id)
        assert token2.get_access_token(db_session) == test_token
        
        TenantContext.clear_current_tenant()


class TestCredentialRepositoryTokenMethods:
    """Test CredentialRepository token management methods."""

    @pytest.fixture(scope="function")
    def credential_repo(self, db_session):
        """Create credential repository with database session."""
        return CredentialRepository(db_session)

    def test_create_access_token_success(self, credential_repo, db_session, test_tenant):
        """Test successful access token creation."""
        TenantContext.set_current_tenant(test_tenant["id"])
        
        test_token = "test_access_token_123"
        expires_at = datetime.utcnow() + timedelta(hours=1)
        
        # Create token
        token_record = credential_repo.create_access_token(
            system_name="test_system",
            access_token=test_token,
            expires_at=expires_at
        )
        db_session.commit()
        
        # Verify creation
        assert token_record.id is not None
        assert token_record.tenant_id == test_tenant["id"]
        assert token_record.system_name == "test_system"
        assert token_record.expires_at == expires_at
        
        # Verify encryption/decryption
        decrypted_token = token_record.get_access_token(db_session)
        assert decrypted_token == test_token
        
        TenantContext.clear_current_tenant()

    def test_create_access_token_validation_errors(self, credential_repo, test_tenant):
        """Test validation errors in token creation."""
        TenantContext.set_current_tenant(test_tenant["id"])
        
        expires_at = datetime.utcnow() + timedelta(hours=1)
        
        # Test empty system_name
        with pytest.raises(ValidationError, match="system_name must be a non-empty string"):
            credential_repo.create_access_token("", "token", expires_at)
        
        # Test empty access_token
        with pytest.raises(ValidationError, match="access_token must be a non-empty string"):
            credential_repo.create_access_token("system", "", expires_at)
        
        # Test invalid expires_at
        with pytest.raises(ValidationError, match="expires_at must be a datetime object"):
            credential_repo.create_access_token("system", "token", "not_datetime")
        
        TenantContext.clear_current_tenant()

    def test_get_newest_valid_token(self, credential_repo, db_session, test_tenant):
        """Test retrieving newest valid token."""
        TenantContext.set_current_tenant(test_tenant["id"])
        
        now = datetime.utcnow()
        system_name = "test_system"
        
        # Create multiple tokens with different expiration times
        token1 = credential_repo.create_access_token(
            system_name=system_name,
            access_token="token1",
            expires_at=now + timedelta(minutes=30)
        )
        
        token2 = credential_repo.create_access_token(
            system_name=system_name,
            access_token="token2", 
            expires_at=now + timedelta(minutes=60)  # Newest/latest expiry
        )
        
        token3 = credential_repo.create_access_token(
            system_name=system_name,
            access_token="token3",
            expires_at=now + timedelta(minutes=45)
        )
        db_session.commit()
        
        # Get newest valid token (should be token2 with latest expiry)
        min_expires_at = now + timedelta(minutes=20)
        newest_token = credential_repo.get_newest_valid_token(
            system_name=system_name,
            min_expires_at=min_expires_at
        )
        
        assert newest_token is not None
        assert newest_token.id == token2.id
        assert newest_token.get_access_token(db_session) == "token2"
        
        TenantContext.clear_current_tenant()

    def test_get_newest_valid_token_no_valid_tokens(self, credential_repo, test_tenant):
        """Test getting token when no valid tokens exist."""
        TenantContext.set_current_tenant(test_tenant["id"])
        
        now = datetime.utcnow()
        
        # Request token that expires well in the future
        min_expires_at = now + timedelta(hours=2)
        token = credential_repo.get_newest_valid_token(
            system_name="nonexistent_system",
            min_expires_at=min_expires_at
        )
        
        assert token is None
        
        TenantContext.clear_current_tenant()

    def test_delete_expired_tokens(self, credential_repo, db_session, test_tenant):
        """Test deleting expired tokens across tenants."""
        TenantContext.set_current_tenant(test_tenant["id"])
        
        now = datetime.utcnow()
        cutoff_time = now - timedelta(minutes=30)
        
        # Create expired tokens (before cutoff)
        expired_token1 = credential_repo.create_access_token(
            system_name="system1",
            access_token="expired1",
            expires_at=now - timedelta(hours=2)
        )
        
        expired_token2 = credential_repo.create_access_token(
            system_name="system2", 
            access_token="expired2",
            expires_at=now - timedelta(minutes=45)
        )
        
        # Create valid token (after cutoff)
        valid_token = credential_repo.create_access_token(
            system_name="system3",
            access_token="valid",
            expires_at=now + timedelta(hours=1)
        )
        db_session.commit()
        
        # Delete expired tokens
        deleted_count = credential_repo.delete_expired_tokens(cutoff_time)
        db_session.commit()
        
        # Should have deleted 2 expired tokens
        assert deleted_count == 2
        
        # Verify expired tokens are gone
        remaining_tokens = db_session.query(ExternalAccessToken).filter(
            ExternalAccessToken.tenant_id == test_tenant["id"]
        ).all()
        
        assert len(remaining_tokens) == 1
        assert remaining_tokens[0].id == valid_token.id
        
        TenantContext.clear_current_tenant()


class TestCredentialServiceTokenMethods:
    """Test CredentialService token management methods."""

    @pytest.fixture(scope="function")
    def credential_repo(self, db_session):
        """Create credential repository with database session."""
        return CredentialRepository(db_session)

    @pytest.fixture(scope="function")  
    def credential_service(self, credential_repo):
        """Create credential service with repository."""
        return CredentialService(credential_repo)

    def test_store_access_token_success(self, credential_service, db_session, test_tenant):
        """Test successful access token storage via service."""
        TenantContext.set_current_tenant(test_tenant["id"])
        
        test_token = "service_test_token_456"
        expires_at = datetime.utcnow() + timedelta(hours=1)
        
        # Store token
        token_id = credential_service.store_access_token(
            system_name="service_test_system",
            access_token=test_token,
            expires_at=expires_at
        )
        db_session.commit()
        
        # Verify storage
        assert token_id is not None
        
        # Retrieve and verify
        token_data = credential_service.get_valid_access_token(
            system_name="service_test_system",
            buffer_minutes=5
        )
        
        assert token_data is not None
        assert token_data["access_token"] == test_token
        assert token_data["expires_at"] == expires_at
        
        TenantContext.clear_current_tenant()

    def test_get_valid_access_token_with_buffer(self, credential_service, db_session, test_tenant):
        """Test getting valid token with buffer time consideration."""
        TenantContext.set_current_tenant(test_tenant["id"])
        
        now = datetime.utcnow()
        system_name = "buffer_test_system"
        
        # Store token that expires in 15 minutes
        short_lived_token = "short_lived_token"
        credential_service.store_access_token(
            system_name=system_name,
            access_token=short_lived_token,
            expires_at=now + timedelta(minutes=15)
        )
        
        # Store token that expires in 45 minutes
        long_lived_token = "long_lived_token"
        credential_service.store_access_token(
            system_name=system_name,
            access_token=long_lived_token,
            expires_at=now + timedelta(minutes=45)
        )
        db_session.commit()
        
        # Request with 20 minute buffer - should get long-lived token
        token_data = credential_service.get_valid_access_token(
            system_name=system_name,
            buffer_minutes=20
        )
        
        assert token_data is not None
        assert token_data["access_token"] == long_lived_token
        
        # Request with 10 minute buffer - should get either token (newest = long-lived)
        token_data = credential_service.get_valid_access_token(
            system_name=system_name,
            buffer_minutes=10
        )
        
        assert token_data is not None
        assert token_data["access_token"] == long_lived_token
        
        TenantContext.clear_current_tenant()

    def test_get_valid_access_token_none_available(self, credential_service, test_tenant):
        """Test getting token when none are available."""
        TenantContext.set_current_tenant(test_tenant["id"])
        
        # Request token for system that has no tokens
        token_data = credential_service.get_valid_access_token(
            system_name="nonexistent_system",
            buffer_minutes=20
        )
        
        assert token_data is None
        
        TenantContext.clear_current_tenant()

    def test_cleanup_expired_tokens_service(self, credential_service, db_session, test_tenant):
        """Test token cleanup via service method."""
        TenantContext.set_current_tenant(test_tenant["id"])
        
        now = datetime.utcnow()
        
        # Create tokens with different expiration times
        credential_service.store_access_token(
            system_name="cleanup_system1",
            access_token="old_token1",
            expires_at=now - timedelta(hours=2)  # Very old
        )
        
        credential_service.store_access_token(
            system_name="cleanup_system2",
            access_token="old_token2", 
            expires_at=now - timedelta(minutes=45)  # Moderately old
        )
        
        credential_service.store_access_token(
            system_name="cleanup_system3",
            access_token="fresh_token",
            expires_at=now + timedelta(hours=1)  # Fresh
        )
        db_session.commit()
        
        # Cleanup tokens older than 40 minutes
        deleted_count = credential_service.cleanup_expired_tokens(cleanup_age_minutes=40)
        db_session.commit()
        
        # Should delete 2 old tokens
        assert deleted_count == 2
        
        # Verify only fresh token remains
        remaining_token = credential_service.get_valid_access_token(
            system_name="cleanup_system3",
            buffer_minutes=5
        )
        assert remaining_token is not None
        assert remaining_token["access_token"] == "fresh_token"
        
        TenantContext.clear_current_tenant()


class TestTokenManagementConfig:
    """Test TokenManagementConfig schema and utility functions."""

    def test_token_management_config_defaults(self):
        """Test default configuration values."""
        config = TokenManagementConfig()
        
        assert config.refresh_buffer_minutes == 20
        assert config.cleanup_frequency_minutes == 20
        assert config.cleanup_age_minutes == 40

    def test_token_management_config_custom_values(self):
        """Test custom configuration values."""
        config = TokenManagementConfig(
            refresh_buffer_minutes=15,
            cleanup_frequency_minutes=30,
            cleanup_age_minutes=60
        )
        
        assert config.refresh_buffer_minutes == 15
        assert config.cleanup_frequency_minutes == 30
        assert config.cleanup_age_minutes == 60

    def test_token_management_config_validation(self):
        """Test configuration validation."""
        # Test minimum values are enforced by Pydantic
        with pytest.raises(PydanticValidationError):
            TokenManagementConfig(refresh_buffer_minutes=2)  # Below minimum of 5
        
        with pytest.raises(PydanticValidationError):
            TokenManagementConfig(cleanup_age_minutes=20)  # Below minimum of 30
        
        # Test cleanup_age must be greater than refresh_buffer
        with pytest.raises(ValueError, match="cleanup_age_minutes must be greater than refresh_buffer_minutes"):
            TokenManagementConfig(
                refresh_buffer_minutes=35,
                cleanup_age_minutes=35  # Invalid: equal to refresh_buffer
            )

    def test_get_token_management_config_from_tenant_config(self):
        """Test extracting token config from tenant config."""
        # Test with TenantConfigValue wrapper
        tenant_config = {
            "token_management": {
                "value": {
                    "refresh_buffer_minutes": 15,
                    "cleanup_frequency_minutes": 25,
                    "cleanup_age_minutes": 50
                },
                "updated_at": "2025-01-01T00:00:00"
            }
        }
        
        config = get_token_management_config(tenant_config)
        assert config.refresh_buffer_minutes == 15
        assert config.cleanup_frequency_minutes == 25
        assert config.cleanup_age_minutes == 50
        
        # Test with direct values (no wrapper)
        tenant_config = {
            "token_management": {
                "refresh_buffer_minutes": 10,
                "cleanup_frequency_minutes": 15,
                "cleanup_age_minutes": 35
            }
        }
        
        config = get_token_management_config(tenant_config)
        assert config.refresh_buffer_minutes == 10
        assert config.cleanup_frequency_minutes == 15
        assert config.cleanup_age_minutes == 35
        
        # Test with missing config (should use defaults)
        tenant_config = {}
        config = get_token_management_config(tenant_config)
        assert config.refresh_buffer_minutes == 20  # Default

    def test_set_token_management_config_in_tenant_config(self):
        """Test setting token config in tenant config."""
        existing_config = {
            "some_other_setting": "value"
        }
        
        token_config = TokenManagementConfig(
            refresh_buffer_minutes=25,
            cleanup_frequency_minutes=35,
            cleanup_age_minutes=65
        )
        
        updated_config = set_token_management_config(existing_config, token_config)
        
        # Should preserve existing settings
        assert updated_config["some_other_setting"] == "value"
        
        # Should add token management config with TenantConfigValue wrapper
        assert "token_management" in updated_config
        token_setting = updated_config["token_management"]
        assert hasattr(token_setting, 'value')
        assert hasattr(token_setting, 'updated_at')
        
        # Verify config values
        assert token_setting.value["refresh_buffer_minutes"] == 25
        assert token_setting.value["cleanup_frequency_minutes"] == 35
        assert token_setting.value["cleanup_age_minutes"] == 65