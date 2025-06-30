"""
Tests for CredentialService  encryption.

Following NO MOCKS policy - tests use real implementations and PostgreSQL.
"""

import pytest
from datetime import datetime, timedelta

from api_exchange_core.db import import_all_models
from api_exchange_core.db import ExternalCredential
from api_exchange_core.services.credential_service import CredentialService
from api_exchange_core.context.tenant_context import TenantContext
from api_exchange_core.exceptions import (
    ValidationError, 
    CredentialNotFoundError, 
    CredentialExpiredError,
    ServiceError
)

# Initialize models properly
import_all_models()


class TestCredentialService:
    """Test CredentialService  encryption."""

    @pytest.fixture(scope="function")
    def credential_service(self, db_manager):
        """Create credential service with global database manager."""
        return CredentialService()

    @pytest.fixture(scope="function")
    def test_credentials(self):
        """Standard test credential data."""
        return {
            "api_key": "service_test_api_key_123",
            "api_secret": "service_super_secret_value",
            "endpoint": "https://api.example.com/v2"
        }

    def test_store_credentials_success(self, credential_service, db_manager, test_tenant, test_credentials):
        """Test successful credential storage."""
        TenantContext.set_current_tenant(test_tenant["id"])
        
        try:
            credential_id = credential_service.store_credentials(
                system_name="test_system",
                auth_type="api_token",
                credentials=test_credentials,
                expires_at=datetime.utcnow() + timedelta(days=30)
            )
            
            
            # Verify credential was created
            assert credential_id is not None
            
            # Verify we can retrieve credentials
            retrieved_creds = credential_service.get_credentials("test_system")
            assert retrieved_creds == test_credentials
            
        finally:
            TenantContext.clear_current_tenant()

    def test_store_credentials_validation_errors(self, credential_service, test_tenant):
        """Test credential storage with invalid parameters."""
        TenantContext.set_current_tenant(test_tenant["id"])
        
        try:
            # Test empty system_name
            with pytest.raises(ValidationError) as exc_info:
                credential_service.store_credentials("", "api_token", {"key": "value"})
            assert "system_name must be a non-empty string" in str(exc_info.value)
            
            # Test None system_name
            with pytest.raises(ValidationError) as exc_info:
                credential_service.store_credentials(None, "api_token", {"key": "value"})
            assert "system_name must be a non-empty string" in str(exc_info.value)
            
        finally:
            TenantContext.clear_current_tenant()

    def test_store_credentials_duplicate_system(self, credential_service, db_manager, test_tenant, test_credentials):
        """Test storing credentials for system that already exists."""
        TenantContext.set_current_tenant(test_tenant["id"])
        
        try:
            # Store initial credential
            credential_service.store_credentials(
                system_name="duplicate_system",
                auth_type="api_token",
                credentials=test_credentials
            )
            
            # Attempt to store another credential for same system
            with pytest.raises(ValidationError) as exc_info:
                credential_service.store_credentials(
                    system_name="duplicate_system",
                    auth_type="oauth",
                    credentials={"token": "different_token"}
                )
            assert "already exist" in str(exc_info.value)
            assert "Use update_credentials instead" in str(exc_info.value)
            
        finally:
            TenantContext.clear_current_tenant()

    def test_get_credentials_success(self, credential_service, db_manager, test_tenant, test_credentials):
        """Test successful credential retrieval."""
        TenantContext.set_current_tenant(test_tenant["id"])
        
        try:
            # Store credential first
            credential_service.store_credentials(
                system_name="get_test_system",
                auth_type="api_token",
                credentials=test_credentials
            )
            
            # Retrieve credentials
            retrieved_creds = credential_service.get_credentials("get_test_system")
            
            assert retrieved_creds == test_credentials
            assert "api_key" in retrieved_creds
            assert "api_secret" in retrieved_creds
            assert "endpoint" in retrieved_creds
            
        finally:
            TenantContext.clear_current_tenant()

    def test_get_credentials_not_found(self, credential_service, test_tenant):
        """Test getting non-existent credentials."""
        TenantContext.set_current_tenant(test_tenant["id"])
        
        try:
            with pytest.raises(CredentialNotFoundError) as exc_info:
                credential_service.get_credentials("nonexistent_system")
            assert "No credentials found for system 'nonexistent_system'" in str(exc_info.value)
        finally:
            TenantContext.clear_current_tenant()

    def test_get_credentials_expired(self, credential_service, db_manager, test_tenant, test_credentials):
        """Test accessing expired credentials."""
        TenantContext.set_current_tenant(test_tenant["id"])
        
        try:
            # Store credential with past expiration
            past_time = datetime.utcnow() - timedelta(hours=1)
            credential_service.store_credentials(
                system_name="expired_system",
                auth_type="api_token",
                credentials=test_credentials,
                expires_at=past_time
            )
            
            # Attempt to get expired credentials
            with pytest.raises(CredentialExpiredError) as exc_info:
                credential_service.get_credentials("expired_system")
            assert "have expired" in str(exc_info.value)
            
        finally:
            TenantContext.clear_current_tenant()

    def test_update_credentials_success(self, credential_service, db_manager, test_tenant, test_credentials):
        """Test successful credential update."""
        TenantContext.set_current_tenant(test_tenant["id"])
        
        try:
            # Store initial credential
            credential_service.store_credentials(
                system_name="update_test_system",
                auth_type="api_token",
                credentials=test_credentials
            )
            
            # Update with new credentials
            new_credentials = {
                "api_key": "updated_key_456",
                "api_secret": "updated_secret",
                "endpoint": "https://api.newversion.com"
            }
            new_expiry = datetime.utcnow() + timedelta(days=60)
            
            credential_service.update_credentials(
                system_name="update_test_system",
                credentials=new_credentials,
                expires_at=new_expiry
            )
            
            # Verify update
            retrieved_creds = credential_service.get_credentials("update_test_system")
            assert retrieved_creds == new_credentials
            assert retrieved_creds["api_key"] == "updated_key_456"
            
        finally:
            TenantContext.clear_current_tenant()

    def test_update_credentials_not_found(self, credential_service, test_tenant):
        """Test updating non-existent credentials."""
        TenantContext.set_current_tenant(test_tenant["id"])
        
        try:
            with pytest.raises(CredentialNotFoundError):
                credential_service.update_credentials(
                    system_name="nonexistent_system",
                    credentials={"key": "value"}
                )
        finally:
            TenantContext.clear_current_tenant()

    def test_delete_credentials_success(self, credential_service, db_manager, test_tenant, test_credentials):
        """Test successful credential deletion."""
        TenantContext.set_current_tenant(test_tenant["id"])
        
        try:
            # Store credential first
            credential_service.store_credentials(
                system_name="delete_test_system",
                auth_type="api_token",
                credentials=test_credentials
            )
            
            # Verify credential exists
            retrieved_creds = credential_service.get_credentials("delete_test_system")
            assert retrieved_creds == test_credentials
            
            # Delete credential
            result = credential_service.delete_credentials("delete_test_system")
            
            # Verify deletion
            assert result is True
            
            # Verify credential no longer exists
            with pytest.raises(CredentialNotFoundError):
                credential_service.get_credentials("delete_test_system")
            
        finally:
            TenantContext.clear_current_tenant()

    def test_delete_credentials_not_found(self, credential_service, test_tenant):
        """Test deleting non-existent credentials."""
        TenantContext.set_current_tenant(test_tenant["id"])
        
        try:
            result = credential_service.delete_credentials("nonexistent_system")
            assert result is False
        finally:
            TenantContext.clear_current_tenant()

    def test_tenant_isolation(self, credential_service, db_manager, multi_tenant_context):
        """Test that credentials are properly isolated by tenant."""
        tenant1, tenant2, tenant3 = multi_tenant_context
        
        # Store credentials for different tenants
        TenantContext.set_current_tenant(tenant1["id"])
        credential_service.store_credentials(
            system_name="shared_system",
            auth_type="api_token",
            credentials={"tenant1_key": "tenant1_value"}
        )
        
        TenantContext.set_current_tenant(tenant2["id"])
        credential_service.store_credentials(
            system_name="shared_system",
            auth_type="api_token", 
            credentials={"tenant2_key": "tenant2_value"}
        )
        
        session = db_manager.get_session()
        session.flush()  # Ensure records are persisted
        
        # Verify tenant1 only sees their credentials
        TenantContext.set_current_tenant(tenant1["id"])
        tenant1_creds = credential_service.get_credentials("shared_system")
        assert "tenant1_key" in tenant1_creds
        assert "tenant2_key" not in tenant1_creds
        
        # Verify tenant2 only sees their credentials
        TenantContext.set_current_tenant(tenant2["id"])
        tenant2_creds = credential_service.get_credentials("shared_system")
        assert "tenant2_key" in tenant2_creds
        assert "tenant1_key" not in tenant2_creds
        
        # Verify tenant3 sees no credentials
        TenantContext.set_current_tenant(tenant3["id"])
        with pytest.raises(CredentialNotFoundError):
            credential_service.get_credentials("shared_system")
        
        TenantContext.clear_current_tenant()

    def test_inactive_credentials_access(self, credential_service, db_manager, test_tenant, test_credentials):
        """Test accessing inactive credentials."""
        TenantContext.set_current_tenant(test_tenant["id"])
        
        try:
            # Store credential first
            credential_service.store_credentials(
                system_name="inactive_test_system",
                auth_type="api_token",
                credentials=test_credentials
            )
            
            # Manually deactivate credential
            from sqlalchemy import and_
            tenant_id = TenantContext.get_current_tenant_id()
            # Get the session from the credential service to query the database
            session = credential_service.session
            credential = session.query(ExternalCredential).filter(
                and_(
                    ExternalCredential.tenant_id == tenant_id,
                    ExternalCredential.system_name == "inactive_test_system"
                )
            ).first()
            credential.is_active = "inactive"
            
            # Attempt to get inactive credentials
            with pytest.raises(CredentialExpiredError) as exc_info:
                credential_service.get_credentials("inactive_test_system")
            assert "are not active" in str(exc_info.value)
            assert "status: inactive" in str(exc_info.value)
            
        finally:
            TenantContext.clear_current_tenant()

    def test_full_credential_lifecycle(self, credential_service, db_manager, test_tenant):
        """Test complete credential lifecycle: create, read, update, delete."""
        TenantContext.set_current_tenant(test_tenant["id"])
        
        try:
            system_name = "lifecycle_system"
            
            # 1. CREATE
            initial_creds = {
                "api_key": "initial_key",
                "api_secret": "initial_secret"
            }
            credential_id = credential_service.store_credentials(
                system_name=system_name,
                auth_type="api_token",
                credentials=initial_creds,
                expires_at=datetime.utcnow() + timedelta(days=30)
            )
            assert credential_id is not None
            
            # 2. READ
            retrieved_creds = credential_service.get_credentials(system_name)
            assert retrieved_creds == initial_creds
            
            # 3. UPDATE
            updated_creds = {
                "api_key": "updated_key",
                "api_secret": "updated_secret",
                "new_field": "new_value"
            }
            credential_service.update_credentials(
                system_name=system_name,
                credentials=updated_creds,
                expires_at=datetime.utcnow() + timedelta(days=60)
            )
            
            # Verify update
            retrieved_updated = credential_service.get_credentials(system_name)
            assert retrieved_updated == updated_creds
            assert retrieved_updated["new_field"] == "new_value"
            
            # 4. DELETE
            deleted = credential_service.delete_credentials(system_name)
            assert deleted is True
            
            # Verify deletion
            with pytest.raises(CredentialNotFoundError):
                credential_service.get_credentials(system_name)
            
        finally:
            TenantContext.clear_current_tenant()

    def test_credential_service_with_token_management(self, db_manager, test_tenant):
        """Test creating CredentialService with token management using consistent pattern."""
        TenantContext.set_current_tenant(test_tenant["id"])
        
        try:
            # Create API token repository and service (consistent pattern)
            from api_exchange_core.repositories.api_token_repository import APITokenRepository
            from api_exchange_core.services.api_token_service import APITokenService
            
            api_token_repo = APITokenRepository(
                api_provider="test_api",
                max_tokens=5,
                token_validity_hours=2
            )
            api_token_service = APITokenService(token_repository=api_token_repo)
            credential_service = CredentialService(
                api_token_service=api_token_service
            )
            
            # Verify service has API token management configured
            assert credential_service.api_token_service is not None
            assert credential_service.api_token_service.token_repository.api_provider == "test_api"
            assert credential_service.api_token_service.token_repository.max_tokens == 5
            assert credential_service.api_token_service.token_repository.token_validity_hours == 2
            
        finally:
            TenantContext.clear_current_tenant()

    def test_store_access_token_success(self, db_manager, test_tenant):
        """Test storing access token via API token service."""
        TenantContext.set_current_tenant(test_tenant["id"])
        
        try:
            # Create API token repository and service (consistent pattern)
            from api_exchange_core.repositories.api_token_repository import APITokenRepository
            from api_exchange_core.services.api_token_service import APITokenService
            
            api_token_repo = APITokenRepository(
                api_provider="test_api_provider"
            )
            api_token_service = APITokenService(token_repository=api_token_repo)
            credential_service = CredentialService(
                api_token_service=api_token_service
            )
            
            # Store access token
            expires_at = datetime.utcnow() + timedelta(hours=1)
            token_id = credential_service.store_access_token(
                system_name="test_api_provider",
                access_token="test_access_token_123",
                expires_at=expires_at
            )
            
            # Verify token was stored
            assert token_id is not None
            
            # Verify we can retrieve it
            token_data = credential_service.get_valid_access_token("test_api_provider")
            assert token_data is not None
            assert token_data["access_token"] == "test_access_token_123"
            assert "token_id" in token_data
            
        finally:
            TenantContext.clear_current_tenant()

    def test_get_valid_access_token_not_found(self, db_manager, test_tenant):
        """Test getting access token when none exists."""
        TenantContext.set_current_tenant(test_tenant["id"])
        
        try:
            # Create API token repository and service (consistent pattern)
            from api_exchange_core.repositories.api_token_repository import APITokenRepository
            from api_exchange_core.services.api_token_service import APITokenService
            
            api_token_repo = APITokenRepository(
                api_provider="test_api_provider"
            )
            api_token_service = APITokenService(token_repository=api_token_repo)
            credential_service = CredentialService(
                api_token_service=api_token_service
            )
            
            # Try to get token when none exists
            token_data = credential_service.get_valid_access_token("test_api_provider")
            assert token_data is None
            
        finally:
            TenantContext.clear_current_tenant()

    def test_access_token_without_token_service(self, credential_service, test_tenant):
        """Test token methods fail when no API token service configured."""
        TenantContext.set_current_tenant(test_tenant["id"])
        
        try:
            # This service doesn't have API token management configured
            expires_at = datetime.utcnow() + timedelta(hours=1)
            
            # Store should fail
            with pytest.raises(ServiceError) as exc_info:
                credential_service.store_access_token(
                    system_name="test_api_provider",
                    access_token="test_token",
                    expires_at=expires_at
                )
            assert "API token management not configured" in str(exc_info.value)
            
            # Get should fail
            with pytest.raises(ServiceError) as exc_info:
                credential_service.get_valid_access_token("test_api_provider")
            assert "API token management not configured" in str(exc_info.value)
            
        finally:
            TenantContext.clear_current_tenant()

    def test_token_multi_tenant_isolation(self, db_manager, multi_tenant_context):
        """Test that tokens are isolated between tenants."""
        tenant1 = multi_tenant_context[0]
        tenant2 = multi_tenant_context[1]
        
        # Create API token repository and service (consistent pattern)
        from api_exchange_core.repositories.api_token_repository import APITokenRepository
        from api_exchange_core.services.api_token_service import APITokenService
        
        api_token_repo = APITokenRepository(
            api_provider="test_api_provider"
        )
        api_token_service = APITokenService(token_repository=api_token_repo)
        credential_service = CredentialService(
            api_token_service=api_token_service
        )
        
        expires_at = datetime.utcnow() + timedelta(hours=1)
        
        # Store token for tenant 1
        TenantContext.set_current_tenant(tenant1["id"])
        try:
            credential_service.store_access_token(
                system_name="test_api_provider",
                access_token="tenant1_token",
                expires_at=expires_at
            )
        finally:
            TenantContext.clear_current_tenant()
        
        # Store token for tenant 2
        TenantContext.set_current_tenant(tenant2["id"])
        try:
            credential_service.store_access_token(
                system_name="test_api_provider",
                access_token="tenant2_token",
                expires_at=expires_at
            )
        finally:
            TenantContext.clear_current_tenant()
        
        # Verify tenant 1 only sees their token
        TenantContext.set_current_tenant(tenant1["id"])
        try:
            token_data = credential_service.get_valid_access_token("test_api_provider")
            assert token_data["access_token"] == "tenant1_token"
        finally:
            TenantContext.clear_current_tenant()
        
        # Verify tenant 2 only sees their token
        TenantContext.set_current_tenant(tenant2["id"])
        try:
            token_data = credential_service.get_valid_access_token("test_api_provider")
            assert token_data["access_token"] == "tenant2_token"
        finally:
            TenantContext.clear_current_tenant()