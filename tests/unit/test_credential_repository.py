"""
Tests for CredentialRepository with pgcrypto encryption.

Following NO MOCKS policy - tests use real PostgreSQL with pgcrypto.
"""

import pytest
from datetime import datetime, timedelta

from src.db.db_config import import_all_models
from src.db.db_credential_models import ExternalCredential
from src.repositories.credential_repository import CredentialRepository
from src.context.tenant_context import TenantContext
from src.exceptions import ValidationError, CredentialNotFoundError

# Initialize models properly
import_all_models()


class TestCredentialRepository:
    """Test CredentialRepository with real PostgreSQL and pgcrypto."""

    @pytest.fixture(scope="function")
    def credential_repo(self, postgres_db_session):
        """Create credential repository with database session."""
        return CredentialRepository(postgres_db_session)

    @pytest.fixture(scope="function")
    def test_credentials(self):
        """Standard test credential data."""
        return {
            "api_key": "test_api_key_123",
            "api_secret": "super_secret_value",
            "endpoint": "https://api.example.com"
        }

    def test_create_credential_success(self, credential_repo, postgres_db_session, postgres_test_tenant, test_credentials):
        """Test successful credential creation."""
        TenantContext.set_current_tenant(postgres_test_tenant["id"])
        
        try:
            credential = credential_repo.create_credential(
                system_name="test_system",
                auth_type="api_token",
                credentials=test_credentials,
                expires_at=datetime.utcnow() + timedelta(days=30)
            )
            
            postgres_db_session.commit()
            
            # Verify credential was created
            assert credential.id is not None
            assert credential.tenant_id == postgres_test_tenant["id"]
            assert credential.system_name == "test_system"
            assert credential.auth_type == "api_token"
            assert credential.is_active == "active"
            assert credential.expires_at is not None
            
            # Verify credentials are encrypted and can be decrypted
            retrieved_creds = credential.get_credentials(postgres_db_session)
            assert retrieved_creds == test_credentials
            
        finally:
            TenantContext.clear_current_tenant()

    def test_create_credential_validation_errors(self, credential_repo, postgres_test_tenant):
        """Test credential creation with invalid parameters."""
        TenantContext.set_current_tenant(postgres_test_tenant["id"])
        
        try:
            # Test empty system_name
            with pytest.raises(ValidationError) as exc_info:
                credential_repo.create_credential("", "api_token", {"key": "value"})
            assert "system_name must be a non-empty string" in str(exc_info.value)
            
            # Test empty auth_type
            with pytest.raises(ValidationError) as exc_info:
                credential_repo.create_credential("test_system", "", {"key": "value"})
            assert "auth_type must be a non-empty string" in str(exc_info.value)
            
            # Test empty credentials
            with pytest.raises(ValidationError) as exc_info:
                credential_repo.create_credential("test_system", "api_token", {})
            assert "credentials must be a non-empty dictionary" in str(exc_info.value)
            
        finally:
            TenantContext.clear_current_tenant()

    def test_get_by_system_name_success(self, credential_repo, postgres_db_session, postgres_test_tenant, test_credentials):
        """Test getting credential by system name."""
        TenantContext.set_current_tenant(postgres_test_tenant["id"])
        
        try:
            # Create credential first
            created_credential = credential_repo.create_credential(
                system_name="test_system",
                auth_type="api_token", 
                credentials=test_credentials
            )
            postgres_db_session.commit()
            
            # Retrieve credential
            retrieved_credential = credential_repo.get_by_system_name("test_system")
            
            assert retrieved_credential is not None
            assert retrieved_credential.id == created_credential.id
            assert retrieved_credential.system_name == "test_system"
            assert retrieved_credential.tenant_id == postgres_test_tenant["id"]
            
            # Verify credentials can be decrypted
            retrieved_creds = retrieved_credential.get_credentials(postgres_db_session)
            assert retrieved_creds == test_credentials
            
        finally:
            TenantContext.clear_current_tenant()

    def test_get_by_system_name_not_found(self, credential_repo, postgres_test_tenant):
        """Test getting non-existent credential."""
        TenantContext.set_current_tenant(postgres_test_tenant["id"])
        
        try:
            result = credential_repo.get_by_system_name("nonexistent_system")
            assert result is None
        finally:
            TenantContext.clear_current_tenant()

    def test_update_credentials_success(self, credential_repo, postgres_db_session, postgres_test_tenant, test_credentials):
        """Test updating existing credentials."""
        TenantContext.set_current_tenant(postgres_test_tenant["id"])
        
        try:
            # Create credential first
            credential = credential_repo.create_credential(
                system_name="test_system",
                auth_type="api_token",
                credentials=test_credentials
            )
            postgres_db_session.commit()
            
            # Update with new credentials
            new_credentials = {
                "api_key": "updated_api_key_456",
                "api_secret": "updated_secret_value"
            }
            new_expiry = datetime.utcnow() + timedelta(days=60)
            
            updated_credential = credential_repo.update_credentials(
                system_name="test_system",
                credentials=new_credentials,
                expires_at=new_expiry
            )
            postgres_db_session.commit()
            
            # Verify update
            assert updated_credential.id == credential.id
            assert updated_credential.expires_at.replace(microsecond=0) == new_expiry.replace(microsecond=0)
            
            # Verify new credentials can be decrypted
            retrieved_creds = updated_credential.get_credentials(postgres_db_session)
            assert retrieved_creds == new_credentials
            
        finally:
            TenantContext.clear_current_tenant()

    def test_update_credentials_not_found(self, credential_repo, postgres_test_tenant):
        """Test updating non-existent credential."""
        TenantContext.set_current_tenant(postgres_test_tenant["id"])
        
        try:
            with pytest.raises(CredentialNotFoundError):
                credential_repo.update_credentials(
                    system_name="nonexistent_system",
                    credentials={"key": "value"}
                )
        finally:
            TenantContext.clear_current_tenant()

    def test_delete_credential_success(self, credential_repo, postgres_db_session, postgres_test_tenant, test_credentials):
        """Test deleting existing credential.""" 
        TenantContext.set_current_tenant(postgres_test_tenant["id"])
        
        try:
            # Create credential first
            credential_repo.create_credential(
                system_name="test_system",
                auth_type="api_token",
                credentials=test_credentials
            )
            postgres_db_session.commit()
            
            # Verify credential exists
            assert credential_repo.get_by_system_name("test_system") is not None
            
            # Delete credential
            result = credential_repo.delete_credential("test_system")
            postgres_db_session.commit()
            
            # Verify deletion
            assert result is True
            assert credential_repo.get_by_system_name("test_system") is None
            
        finally:
            TenantContext.clear_current_tenant()

    def test_delete_credential_not_found(self, credential_repo, postgres_test_tenant):
        """Test deleting non-existent credential."""
        TenantContext.set_current_tenant(postgres_test_tenant["id"])
        
        try:
            result = credential_repo.delete_credential("nonexistent_system")
            assert result is False
        finally:
            TenantContext.clear_current_tenant()

    def test_tenant_isolation(self, credential_repo, postgres_db_session, postgres_multi_tenant_context):
        """Test that credentials are properly isolated by tenant."""
        tenant1, tenant2, tenant3 = postgres_multi_tenant_context
        
        # Create credentials for different tenants
        TenantContext.set_current_tenant(tenant1["id"])
        credential_repo.create_credential(
            system_name="shared_system",
            auth_type="api_token",
            credentials={"tenant1_key": "tenant1_value"}
        )
        
        TenantContext.set_current_tenant(tenant2["id"])
        credential_repo.create_credential(
            system_name="shared_system", 
            auth_type="api_token",
            credentials={"tenant2_key": "tenant2_value"}
        )
        
        postgres_db_session.commit()
        
        # Verify tenant1 only sees their credential
        TenantContext.set_current_tenant(tenant1["id"])
        tenant1_cred = credential_repo.get_by_system_name("shared_system")
        assert tenant1_cred is not None
        assert tenant1_cred.tenant_id == tenant1["id"]
        tenant1_creds = tenant1_cred.get_credentials(postgres_db_session)
        assert "tenant1_key" in tenant1_creds
        
        # Verify tenant2 only sees their credential
        TenantContext.set_current_tenant(tenant2["id"])
        tenant2_cred = credential_repo.get_by_system_name("shared_system")
        assert tenant2_cred is not None
        assert tenant2_cred.tenant_id == tenant2["id"]
        tenant2_creds = tenant2_cred.get_credentials(postgres_db_session)
        assert "tenant2_key" in tenant2_creds
        
        # Verify tenant3 sees no credentials
        TenantContext.set_current_tenant(tenant3["id"])
        tenant3_cred = credential_repo.get_by_system_name("shared_system")
        assert tenant3_cred is None
        
        TenantContext.clear_current_tenant()