"""
Unit tests for encryption utilities.

Tests database-specific encryption (SQLite plaintext for testing, PostgreSQL pgcrypto).
Note: We use SQLite in tests, so PostgreSQL paths are tested with mocks.
"""

import pytest
from unittest.mock import Mock, patch, MagicMock
from sqlalchemy.orm import Session

from api_exchange_core.utils.encryption_utils import (
    encrypt_value,
    decrypt_value,
    encrypt_token,
    decrypt_token,
    encrypt_credential,
    decrypt_credential
)


class TestEncryptValueSQLite:
    """Test encrypt_value function with SQLite."""
    
    def test_encrypt_value_basic(self, db_session: Session):
        """Test encrypting value with SQLite (returns plaintext)."""
        # Test data
        value = "test_secret_value"
        tenant_id = "test-tenant"
        
        # Encrypt
        encrypted = encrypt_value(db_session, value, tenant_id)
        
        # SQLite returns as bytes
        assert isinstance(encrypted, bytes)
        assert encrypted == b"test_secret_value"
    
    def test_encrypt_value_with_suffix(self, db_session: Session):
        """Test encrypting value with key suffix in SQLite."""
        # Test data
        value = "another_secret"
        tenant_id = "test-tenant"
        key_suffix = "token_api"
        
        # Encrypt with suffix
        encrypted = encrypt_value(db_session, value, tenant_id, key_suffix)
        
        # SQLite ignores key suffix but still returns bytes
        assert encrypted == b"another_secret"
    
    def test_encrypt_empty_value(self, db_session: Session):
        """Test encrypting empty string."""
        encrypted = encrypt_value(db_session, "", "test-tenant")
        assert encrypted == b""
    
    def test_encrypt_unicode_value(self, db_session: Session):
        """Test encrypting unicode value."""
        value = "unicode_🔐_secret"
        encrypted = encrypt_value(db_session, value, "test-tenant")
        assert encrypted == value.encode('utf-8')
    
    def test_encrypt_bytes_value(self, db_session: Session):
        """Test that bytes are handled properly."""
        # If value is already bytes, it should still work
        value = b"already_bytes"
        encrypted = encrypt_value(db_session, value.decode(), "test-tenant")
        assert encrypted == value


class TestDecryptValueSQLite:
    """Test decrypt_value function with SQLite."""
    
    def test_decrypt_value_basic(self, db_session: Session):
        """Test decrypting value with SQLite."""
        # Test data
        encrypted = b"test_secret_value"
        tenant_id = "test-tenant"
        
        # Decrypt
        decrypted = decrypt_value(db_session, encrypted, tenant_id)
        
        assert decrypted == "test_secret_value"
    
    def test_decrypt_value_with_suffix(self, db_session: Session):
        """Test decrypting value with key suffix in SQLite."""
        # Test data
        encrypted = b"another_secret"
        tenant_id = "test-tenant"
        key_suffix = "token_api"
        
        # Decrypt with suffix
        decrypted = decrypt_value(db_session, encrypted, tenant_id, key_suffix)
        
        # SQLite ignores key suffix
        assert decrypted == "another_secret"
    
    def test_decrypt_none_value(self, db_session: Session):
        """Test decrypting None value."""
        decrypted = decrypt_value(db_session, None, "test-tenant")
        assert decrypted is None
    
    def test_decrypt_empty_bytes(self, db_session: Session):
        """Test decrypting empty bytes."""
        decrypted = decrypt_value(db_session, b"", "test-tenant")
        assert decrypted is None
    
    def test_decrypt_string_value(self, db_session: Session):
        """Test decrypting string value (not bytes) in SQLite."""
        # Sometimes encrypted values might be stored as strings
        encrypted = "string_value"
        decrypted = decrypt_value(db_session, encrypted, "test-tenant")
        assert decrypted == "string_value"
    
    def test_decrypt_unicode_value(self, db_session: Session):
        """Test decrypting unicode value."""
        encrypted = "unicode_🔐_secret".encode('utf-8')
        decrypted = decrypt_value(db_session, encrypted, "test-tenant")
        assert decrypted == "unicode_🔐_secret"


class TestPostgreSQLPaths:
    """Test PostgreSQL code paths using mocks."""
    
    def test_encrypt_value_postgresql_path(self):
        """Test that PostgreSQL encryption path is called correctly."""
        # Create mock session
        mock_session = Mock()
        mock_session.bind = Mock()
        mock_session.bind.dialect = Mock()
        mock_session.bind.dialect.name = "postgresql"
        
        # Mock execute result
        mock_result = Mock()
        mock_result.scalar.return_value = b"\\x01234567890encrypted"
        mock_session.execute.return_value = mock_result
        
        # Test encryption
        value = "postgres_secret"
        tenant_id = "pg-tenant"
        
        encrypted = encrypt_value(mock_session, value, tenant_id)
        
        # Verify pgcrypto was called
        mock_session.execute.assert_called_once()
        call_args = mock_session.execute.call_args
        
        # Verify SQL and parameters
        sql_text = str(call_args[0][0])
        assert "pgp_sym_encrypt" in sql_text
        
        params = call_args[0][1]
        assert params["data"] == "postgres_secret"
        assert params["key"] == "pg-tenant"
        
        assert encrypted == b"\\x01234567890encrypted"
    
    def test_decrypt_value_postgresql_path(self):
        """Test that PostgreSQL decryption path is called correctly."""
        # Create mock session
        mock_session = Mock()
        mock_session.bind = Mock()
        mock_session.bind.dialect = Mock()
        mock_session.bind.dialect.name = "postgresql"
        
        # Mock execute result
        mock_result = Mock()
        mock_result.scalar.return_value = "decrypted_secret"
        mock_session.execute.return_value = mock_result
        
        # Test decryption
        encrypted = b"\\x01234567890encrypted"
        tenant_id = "pg-tenant"
        
        decrypted = decrypt_value(mock_session, encrypted, tenant_id)
        
        # Verify pgcrypto was called
        mock_session.execute.assert_called_once()
        call_args = mock_session.execute.call_args
        
        # Verify SQL and parameters
        sql_text = str(call_args[0][0])
        assert "pgp_sym_decrypt" in sql_text
        
        params = call_args[0][1]
        assert params["data"] == encrypted
        assert params["key"] == "pg-tenant"
        
        assert decrypted == "decrypted_secret"
    
    def test_postgresql_key_suffix_handling(self):
        """Test that PostgreSQL correctly handles key suffixes."""
        # Create mock session
        mock_session = Mock()
        mock_session.bind = Mock()
        mock_session.bind.dialect = Mock()
        mock_session.bind.dialect.name = "postgresql"
        
        # Mock execute result
        mock_result = Mock()
        mock_result.scalar.return_value = b"\\xencrypted"
        mock_session.execute.return_value = mock_result
        
        # Test with suffix
        value = "test_value"
        tenant_id = "tenant"
        key_suffix = "api_key"
        
        encrypt_value(mock_session, value, tenant_id, key_suffix)
        
        # Verify combined key
        call_args = mock_session.execute.call_args
        params = call_args[0][1]
        assert params["key"] == "tenant_api_key"


class TestTokenEncryption:
    """Test token-specific encryption functions."""
    
    def test_encrypt_token_sqlite(self, db_session: Session):
        """Test encrypting API token with SQLite."""
        token = "sk-1234567890abcdef"
        tenant_id = "test-tenant"
        api_provider = "openai"
        
        encrypted = encrypt_token(db_session, token, tenant_id, api_provider)
        
        # SQLite returns plaintext as bytes
        assert encrypted == b"sk-1234567890abcdef"
    
    def test_decrypt_token_sqlite(self, db_session: Session):
        """Test decrypting API token with SQLite."""
        encrypted = b"sk-1234567890abcdef"
        tenant_id = "test-tenant"
        api_provider = "openai"
        
        decrypted = decrypt_token(db_session, encrypted, tenant_id, api_provider)
        
        assert decrypted == "sk-1234567890abcdef"
    
    def test_token_round_trip(self, db_session: Session):
        """Test full round trip for token encryption/decryption."""
        original_token = "test-api-token-12345"
        tenant_id = "test-tenant"
        api_provider = "azure"
        
        # Encrypt and decrypt
        encrypted = encrypt_token(db_session, original_token, tenant_id, api_provider)
        decrypted = decrypt_token(db_session, encrypted, tenant_id, api_provider)
        
        assert decrypted == original_token


class TestCredentialEncryption:
    """Test credential-specific encryption functions."""
    
    def test_encrypt_credential_sqlite(self, db_session: Session):
        """Test encrypting credentials with SQLite."""
        credential = "password123"
        tenant_id = "test-tenant"
        system_name = "salesforce"
        
        encrypted = encrypt_credential(db_session, credential, tenant_id, system_name)
        
        # SQLite returns plaintext as bytes
        assert encrypted == b"password123"
    
    def test_decrypt_credential_sqlite(self, db_session: Session):
        """Test decrypting credentials with SQLite."""
        encrypted = b"password123"
        tenant_id = "test-tenant"
        system_name = "salesforce"
        
        decrypted = decrypt_credential(db_session, encrypted, tenant_id, system_name)
        
        assert decrypted == "password123"
    
    def test_credential_round_trip(self, db_session: Session):
        """Test full round trip for credential encryption/decryption."""
        original_cred = "super_secret_password"
        tenant_id = "test-tenant"
        system_name = "database"
        
        # Encrypt and decrypt
        encrypted = encrypt_credential(db_session, original_cred, tenant_id, system_name)
        decrypted = decrypt_credential(db_session, encrypted, tenant_id, system_name)
        
        assert decrypted == original_cred


class TestEncryptionIntegration:
    """Integration tests for encryption utilities."""
    
    def test_multiple_values_round_trip(self, db_session: Session):
        """Test encryption/decryption of multiple values."""
        test_cases = [
            ("simple_password", "tenant1", ""),
            ("complex!@#$%^&*()_+", "tenant2", "suffix1"),
            ("unicode_密码_🔐", "tenant3", "api_key"),
            ("very_long_value_" * 100, "tenant4", "long"),
        ]
        
        for value, tenant_id, key_suffix in test_cases:
            # Encrypt and decrypt
            encrypted = encrypt_value(db_session, value, tenant_id, key_suffix)
            decrypted = decrypt_value(db_session, encrypted, tenant_id, key_suffix)
            
            # Verify round trip
            assert decrypted == value
            
            # Verify encrypted is bytes
            assert isinstance(encrypted, bytes)
    
    def test_empty_value_handling(self, db_session: Session):
        """Test handling of empty values."""
        # Empty string encrypts to empty bytes
        encrypted = encrypt_value(db_session, "", "tenant")
        assert encrypted == b""
        
        # Empty bytes are treated as None by decrypt_value
        # This is by design in the encryption utils
        decrypted = decrypt_value(db_session, encrypted, "tenant")
        assert decrypted is None
        
        # None handling in decrypt
        assert decrypt_value(db_session, None, "tenant") is None
        assert decrypt_value(db_session, b"", "tenant") is None
    
    def test_api_specific_functions(self, db_session: Session):
        """Test that API-specific functions work correctly."""
        tenant_id = "test-tenant"
        
        # Test token encryption
        token = "api-token-12345"
        encrypted_token = encrypt_token(db_session, token, tenant_id, "openai")
        decrypted_token = decrypt_token(db_session, encrypted_token, tenant_id, "openai")
        assert decrypted_token == token
        
        # Test credential encryption
        cred = "database_password"
        encrypted_cred = encrypt_credential(db_session, cred, tenant_id, "postgres")
        decrypted_cred = decrypt_credential(db_session, encrypted_cred, tenant_id, "postgres")
        assert decrypted_cred == cred
    
    def test_tenant_and_suffix_combinations(self, db_session: Session):
        """Test various tenant and suffix combinations."""
        value = "test_secret"
        
        # Different tenants
        enc1 = encrypt_value(db_session, value, "tenant1", "suffix")
        enc2 = encrypt_value(db_session, value, "tenant2", "suffix")
        
        # In SQLite, both encrypt to same value (no real encryption)
        assert enc1 == enc2 == b"test_secret"
        
        # Different suffixes
        enc3 = encrypt_value(db_session, value, "tenant1", "suffix1")
        enc4 = encrypt_value(db_session, value, "tenant1", "suffix2")
        
        # In SQLite, suffix doesn't matter
        assert enc3 == enc4 == b"test_secret"
        
        # But all decrypt correctly
        assert decrypt_value(db_session, enc1, "tenant1", "suffix") == value
        assert decrypt_value(db_session, enc2, "tenant2", "suffix") == value
        assert decrypt_value(db_session, enc3, "tenant1", "suffix1") == value
        assert decrypt_value(db_session, enc4, "tenant1", "suffix2") == value