"""
Unit tests for credential utilities using Pydantic schemas.

Tests the credential utility functions that provide business logic
using Pydantic schemas for type safety and validation.
"""

import pytest
from datetime import datetime, timezone, timedelta
from sqlalchemy.orm import Session

from api_exchange_core.db.db_credential_models import ExternalCredential
from api_exchange_core.exceptions import CredentialNotFoundError
from api_exchange_core.schemas.credential_schemas import (
    APIKeyCredentials,
    AzureServicePrincipalCredentials,
    BasicAuthCredentials,
    ExternalCredentialCreate,
    ExternalCredentialUpdate,
    OAuthCredentials,
    serialize_credentials,
)
from api_exchange_core.utils.credential_utils import (
    delete_credentials,
    get_credentials,
    store_credentials,
    update_credentials,
)


class TestGetCredentials:
    """Test get_credentials function with Pydantic schemas."""
    
    def test_get_oauth_credentials_success(self, db_session: Session):
        """Test getting OAuth credentials."""
        # Create OAuth credentials using schema
        oauth_creds = OAuthCredentials(
            access_token="oauth_access_123",
            refresh_token="oauth_refresh_456",
            expires_in=3600,
            scope="read write"
        )
        
        # Store directly in database for test
        future_date = datetime.now(timezone.utc) + timedelta(days=30)
        credential = ExternalCredential(
            tenant_id="test-tenant",
            system_name="oauth_system",
            credential_data=serialize_credentials(oauth_creds),
            expires_at=future_date
        )
        db_session.add(credential)
        db_session.commit()
        
        # Get the credentials
        result = get_credentials(db_session, "test-tenant", "oauth_system")
        
        assert result.id == credential.id
        assert result.system_name == "oauth_system"
        assert isinstance(result.credential_data, OAuthCredentials)
        assert result.credential_data.access_token == "oauth_access_123"
        assert result.credential_data.refresh_token == "oauth_refresh_456"
        assert result.credential_data.expires_in == 3600
        assert result.credential_data.scope == "read write"
    
    def test_get_api_key_credentials_success(self, db_session: Session):
        """Test getting API key credentials."""
        # Create API key credentials using schema
        api_creds = APIKeyCredentials(
            api_key="api_key_123",
            secret_key="secret_456",
            key_id="key_id_789"
        )
        
        # Store directly in database for test
        credential = ExternalCredential(
            tenant_id="test-tenant",
            system_name="api_system",
            credential_data=serialize_credentials(api_creds)
        )
        db_session.add(credential)
        db_session.commit()
        
        # Get the credentials
        result = get_credentials(db_session, "test-tenant", "api_system")
        
        assert isinstance(result.credential_data, APIKeyCredentials)
        assert result.credential_data.api_key == "api_key_123"
        assert result.credential_data.secret_key == "secret_456"
        assert result.credential_data.key_id == "key_id_789"
    
    def test_get_credentials_not_found(self, db_session: Session):
        """Test getting credentials when they don't exist."""
        with pytest.raises(CredentialNotFoundError) as exc_info:
            get_credentials(db_session, "test-tenant", "nonexistent_system")
        
        error = exc_info.value
        assert "Credentials not found for system: nonexistent_system" in error.message
        assert error.context["tenant_id"] == "test-tenant"
        assert error.context["system_name"] == "nonexistent_system"
    
    def test_get_credentials_expired(self, db_session: Session):
        """Test getting expired credentials."""
        oauth_creds = OAuthCredentials(access_token="expired_token")
        
        # Create an expired credential
        past_date = datetime.now(timezone.utc) - timedelta(days=1)
        credential = ExternalCredential(
            tenant_id="test-tenant",
            system_name="expired_system",
            credential_data=serialize_credentials(oauth_creds),
            expires_at=past_date
        )
        db_session.add(credential)
        db_session.commit()
        
        with pytest.raises(CredentialNotFoundError) as exc_info:
            get_credentials(db_session, "test-tenant", "expired_system")
        
        error = exc_info.value
        assert "Credentials expired for system: expired_system" in error.message


class TestStoreCredentials:
    """Test store_credentials function with Pydantic schemas."""
    
    def test_store_oauth_credentials_new(self, db_session: Session):
        """Test storing new OAuth credentials."""
        oauth_creds = OAuthCredentials(
            access_token="new_access_123",
            refresh_token="new_refresh_456",
            token_type="Bearer",
            expires_in=7200
        )
        
        future_date = datetime.now(timezone.utc) + timedelta(days=30)
        credential_create = ExternalCredentialCreate(
            system_name="new_oauth_system",
            credential_data=oauth_creds,
            expires_at=future_date,
            context={"provider": "azure", "tenant": "contoso"}
        )
        
        cred_id = store_credentials(db_session, "test-tenant", credential_create)
        
        # Verify credential was stored
        stored_cred = db_session.query(ExternalCredential).filter_by(id=cred_id).first()
        assert stored_cred is not None
        assert stored_cred.tenant_id == "test-tenant"
        assert stored_cred.system_name == "new_oauth_system"
        assert stored_cred.expires_at.replace(tzinfo=None) == future_date.replace(tzinfo=None)
        assert stored_cred.context == {"provider": "azure", "tenant": "contoso"}
        
        # Verify we can deserialize the stored data back to OAuth credentials
        retrieved = get_credentials(db_session, "test-tenant", "new_oauth_system")
        assert isinstance(retrieved.credential_data, OAuthCredentials)
        assert retrieved.credential_data.access_token == "new_access_123"
    
    def test_store_api_key_credentials_new(self, db_session: Session):
        """Test storing new API key credentials."""
        api_creds = APIKeyCredentials(
            api_key="store_key_123",
            secret_key="store_secret_456"
        )
        
        credential_create = ExternalCredentialCreate(
            system_name="new_api_system",
            credential_data=api_creds
        )
        
        cred_id = store_credentials(db_session, "test-tenant", credential_create)
        
        # Verify credential was stored
        retrieved = get_credentials(db_session, "test-tenant", "new_api_system")
        assert isinstance(retrieved.credential_data, APIKeyCredentials)
        assert retrieved.credential_data.api_key == "store_key_123"
        assert retrieved.credential_data.secret_key == "store_secret_456"
    
    def test_store_credentials_update_existing(self, db_session: Session):
        """Test updating existing credentials via store_credentials."""
        # First create initial credentials
        initial_creds = APIKeyCredentials(api_key="initial_key")
        credential_create = ExternalCredentialCreate(
            system_name="update_system",
            credential_data=initial_creds
        )
        initial_id = store_credentials(db_session, "test-tenant", credential_create)
        
        # Now store new credentials for same system (should update)
        updated_creds = APIKeyCredentials(
            api_key="updated_key",
            secret_key="new_secret"
        )
        updated_create = ExternalCredentialCreate(
            system_name="update_system",  # Same system name
            credential_data=updated_creds,
            context={"updated": True}
        )
        updated_id = store_credentials(db_session, "test-tenant", updated_create)
        
        # Should return same ID (updated, not created new)
        assert updated_id == initial_id
        
        # Verify the credential was updated
        retrieved = get_credentials(db_session, "test-tenant", "update_system")
        assert isinstance(retrieved.credential_data, APIKeyCredentials)
        assert retrieved.credential_data.api_key == "updated_key"
        assert retrieved.credential_data.secret_key == "new_secret"
        assert retrieved.context == {"updated": True}


class TestUpdateCredentials:
    """Test update_credentials function with Pydantic schemas."""
    
    def test_update_credentials_success(self, db_session: Session):
        """Test updating existing credentials."""
        # Create initial credential
        initial_creds = OAuthCredentials(access_token="initial_token")
        credential_create = ExternalCredentialCreate(
            system_name="update_test",
            credential_data=initial_creds
        )
        store_credentials(db_session, "test-tenant", credential_create)
        
        # Update credentials
        updated_creds = OAuthCredentials(
            access_token="updated_token",
            refresh_token="new_refresh"
        )
        new_date = datetime.now(timezone.utc) + timedelta(days=60)
        credential_update = ExternalCredentialUpdate(
            credential_data=updated_creds,
            expires_at=new_date,
            context={"updated": True}
        )
        
        result = update_credentials(
            db_session,
            "test-tenant",
            "update_test",
            credential_update
        )
        
        assert result is True
        
        # Verify update
        retrieved = get_credentials(db_session, "test-tenant", "update_test")
        assert isinstance(retrieved.credential_data, OAuthCredentials)
        assert retrieved.credential_data.access_token == "updated_token"
        assert retrieved.credential_data.refresh_token == "new_refresh"
        assert retrieved.context == {"updated": True}
    
    def test_update_credentials_partial(self, db_session: Session):
        """Test partial update of credentials."""
        # Create initial credential with context
        initial_creds = APIKeyCredentials(api_key="initial_key")
        credential_create = ExternalCredentialCreate(
            system_name="partial_update",
            credential_data=initial_creds,
            context={"original": True}
        )
        store_credentials(db_session, "test-tenant", credential_create)
        
        # Update only the expiry date, leave credentials and context unchanged
        new_date = datetime.now(timezone.utc) + timedelta(days=90)
        credential_update = ExternalCredentialUpdate(
            expires_at=new_date
            # credential_data and context not provided - should remain unchanged
        )
        
        result = update_credentials(
            db_session,
            "test-tenant", 
            "partial_update",
            credential_update
        )
        
        assert result is True
        
        # Verify original credentials and context preserved, but expiry updated
        retrieved = get_credentials(db_session, "test-tenant", "partial_update")
        assert isinstance(retrieved.credential_data, APIKeyCredentials)
        assert retrieved.credential_data.api_key == "initial_key"  # Unchanged
        assert retrieved.context == {"original": True}  # Unchanged
        assert retrieved.expires_at.replace(tzinfo=None) == new_date.replace(tzinfo=None)  # Updated
    
    def test_update_credentials_not_found(self, db_session: Session):
        """Test updating non-existent credentials."""
        credential_update = ExternalCredentialUpdate(
            credential_data=APIKeyCredentials(api_key="test")
        )
        
        result = update_credentials(
            db_session,
            "test-tenant",
            "nonexistent_system",
            credential_update
        )
        
        assert result is False


class TestDeleteCredentials:
    """Test delete_credentials function."""
    
    def test_delete_credentials_success(self, db_session: Session):
        """Test deleting existing credentials."""
        # Create credential
        creds = BasicAuthCredentials(username="test_user", password="test_pass")
        credential_create = ExternalCredentialCreate(
            system_name="delete_system",
            credential_data=creds
        )
        cred_id = store_credentials(db_session, "test-tenant", credential_create)
        
        # Delete credentials
        result = delete_credentials(db_session, "test-tenant", "delete_system")
        
        assert result is True
        
        # Verify deletion
        deleted_cred = db_session.query(ExternalCredential).filter_by(id=cred_id).first()
        assert deleted_cred is None
    
    def test_delete_credentials_not_found(self, db_session: Session):
        """Test deleting non-existent credentials."""
        result = delete_credentials(db_session, "test-tenant", "nonexistent_system")
        
        assert result is False


class TestPydanticValidation:
    """Test Pydantic schema validation."""
    
    def test_oauth_credentials_validation(self):
        """Test OAuth credentials validation."""
        # Valid OAuth credentials
        valid_oauth = OAuthCredentials(
            access_token="valid_token",
            refresh_token="valid_refresh",
            token_type="Bearer",
            expires_in=3600
        )
        assert valid_oauth.access_token == "valid_token"
        assert valid_oauth.token_type == "Bearer"
        
        # Invalid token type should raise validation error
        with pytest.raises(ValueError, match="Token type must be one of"):
            OAuthCredentials(
                access_token="token",
                token_type="InvalidType"
            )
        
        # Empty access token should raise validation error
        with pytest.raises(ValueError):
            OAuthCredentials(access_token="")
    
    def test_api_key_credentials_validation(self):
        """Test API key credentials validation."""
        # Valid API key credentials
        valid_api_key = APIKeyCredentials(
            api_key="valid_key",
            secret_key="valid_secret"
        )
        assert valid_api_key.api_key == "valid_key"
        
        # API key with whitespace should be automatically stripped due to str_strip_whitespace=True
        api_key_with_spaces = APIKeyCredentials(api_key="  key_with_spaces  ")
        assert api_key_with_spaces.api_key == "key_with_spaces"  # Whitespace stripped
    
    def test_azure_service_principal_validation(self):
        """Test Azure Service Principal credentials validation."""
        # Valid Azure SP credentials with proper GUID format
        valid_azure = AzureServicePrincipalCredentials(
            tenant_id="12345678-1234-1234-1234-123456789012",
            client_id="87654321-4321-4321-4321-210987654321",
            client_secret="valid_secret"
        )
        assert valid_azure.tenant_id == "12345678-1234-1234-1234-123456789012"
        
        # Invalid GUID format should raise validation error
        with pytest.raises(ValueError, match="Must be a valid GUID format"):
            AzureServicePrincipalCredentials(
                tenant_id="invalid-guid",
                client_id="87654321-4321-4321-4321-210987654321",
                client_secret="secret"
            )


class TestCredentialUtilsIntegration:
    """Integration tests for credential utilities with Pydantic schemas."""
    
    def test_credential_lifecycle_with_schemas(self, db_session: Session):
        """Test complete credential lifecycle using Pydantic schemas."""
        # 1. Store OAuth credentials
        oauth_creds = OAuthCredentials(
            access_token="lifecycle_access",
            refresh_token="lifecycle_refresh",
            expires_in=3600
        )
        initial_date = datetime.now(timezone.utc) + timedelta(days=30)
        credential_create = ExternalCredentialCreate(
            system_name="lifecycle_system",
            credential_data=oauth_creds,
            expires_at=initial_date,
            context={"phase": "initial"}
        )
        
        cred_id = store_credentials(db_session, "test-tenant", credential_create)
        
        # 2. Get credentials and verify schema type
        retrieved = get_credentials(db_session, "test-tenant", "lifecycle_system")
        assert retrieved.id == cred_id
        assert isinstance(retrieved.credential_data, OAuthCredentials)
        assert retrieved.credential_data.access_token == "lifecycle_access"
        assert retrieved.context == {"phase": "initial"}
        
        # 3. Update to different credential type (API key)
        api_creds = APIKeyCredentials(
            api_key="new_api_key",
            secret_key="new_secret"
        )
        updated_date = datetime.now(timezone.utc) + timedelta(days=60)
        credential_update = ExternalCredentialUpdate(
            credential_data=api_creds,
            expires_at=updated_date,
            context={"phase": "updated"}
        )
        
        update_result = update_credentials(
            db_session,
            "test-tenant",
            "lifecycle_system",
            credential_update
        )
        assert update_result is True
        
        # 4. Get updated credentials and verify new schema type
        updated_retrieved = get_credentials(db_session, "test-tenant", "lifecycle_system")
        assert isinstance(updated_retrieved.credential_data, APIKeyCredentials)
        assert updated_retrieved.credential_data.api_key == "new_api_key"
        assert updated_retrieved.context == {"phase": "updated"}
        
        # 5. Delete credentials
        delete_result = delete_credentials(db_session, "test-tenant", "lifecycle_system")
        assert delete_result is True
        
        # 6. Verify deletion
        with pytest.raises(CredentialNotFoundError):
            get_credentials(db_session, "test-tenant", "lifecycle_system")