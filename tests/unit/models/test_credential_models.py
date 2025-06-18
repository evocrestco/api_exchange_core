"""Simple test to verify credential model basics."""

import pytest
from datetime import datetime, timedelta

from src.db.db_credential_models import ExternalCredential


class TestExternalCredential:
    """External credential tests."""

    def test_credential_creation(self, db_session, test_tenant):
        """Test basic credential creation."""
        credential = ExternalCredential(
            tenant_id=test_tenant["id"],
            system_name="test_system",
            auth_type="api_token",
            is_active="active"
        )
        
        # Set credentials
        test_credentials = {
            "api_key": "secret_api_key_123",
            "api_secret": "super_secret_value"
        }
        credential.set_credentials(test_credentials, db_session)
        
        db_session.add(credential)
        db_session.commit()
        
        # Verify credential was created
        assert credential.id is not None
        assert credential.tenant_id == test_tenant["id"]
        assert credential.system_name == "test_system"
        assert credential.auth_type == "api_token"
        assert credential.is_active == "active"
        
        # Verify credentials can be retrieved
        decrypted_creds = credential.get_credentials(db_session)
        assert decrypted_creds == test_credentials

    def test_expiration_check(self, db_session, test_tenant):
        """Test expiration checking."""
        # Create expired credential
        past_time = datetime.utcnow() - timedelta(hours=1)
        credential = ExternalCredential(
            tenant_id=test_tenant["id"],
            system_name="test_system",
            auth_type="api_token",
            expires_at=past_time
        )
        
        # Set credentials
        test_credentials = {"api_key": "test_key"}
        credential.set_credentials(test_credentials, db_session)
        
        db_session.add(credential)
        db_session.commit()
        
        assert credential.is_expired()

    def test_no_expiration(self, db_session, test_tenant):
        """Test credential without expiration."""
        credential = ExternalCredential(
            tenant_id=test_tenant["id"],
            system_name="test_system",
            auth_type="api_token"
        )
        
        # Set credentials
        test_credentials = {"api_key": "test_key"}
        credential.set_credentials(test_credentials, db_session)
        
        db_session.add(credential)
        db_session.commit()
        
        assert not credential.is_expired()