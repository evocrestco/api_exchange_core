"""
PostgreSQL credential models with pgcrypto encryption.
"""

import json
from datetime import datetime
from typing import Any, Dict, Optional

from sqlalchemy import Column, DateTime, ForeignKey, Index, String, Text, UniqueConstraint, text
from sqlalchemy.orm import relationship

from src.db.db_base import Base, BaseModel, EncryptedBinary


class ExternalCredential(Base, BaseModel):
    """Secure credential model using PostgreSQL pgcrypto encryption."""
    
    __tablename__ = "external_credentials"
    
    # Tenant isolation
    tenant_id = Column(
        String(100), 
        ForeignKey('tenant.tenant_id', ondelete='CASCADE'), 
        nullable=False
    )
    
    # System identification
    system_name = Column(String(100), nullable=False)
    auth_type = Column(String(50), nullable=False)
    
    # pgcrypto encrypted storage
    _encrypted_credentials = Column('encrypted_credentials', EncryptedBinary, nullable=False)
    
    # Optional fields
    expires_at = Column(DateTime, nullable=True)
    is_active = Column(String(10), nullable=False, default="active")
    
    # Relationships
    tenant = relationship("Tenant", backref="external_credentials")
    
    # Constraints
    __table_args__ = (
        UniqueConstraint('tenant_id', 'system_name', name='uq_credential_tenant_system'),
        Index('ix_credential_tenant_id', 'tenant_id'),
        Index('ix_credential_system_name', 'system_name'),
    )
    
    def set_credentials(self, credentials: Dict[str, Any], session) -> None:
        """Encrypt and store credentials using pgcrypto with tenant-specific key."""
        credentials_json = json.dumps(credentials)
        
        # TODO: Replace database-specific logic with proper encryption abstraction layer
        if session.bind.dialect.name == "postgresql":
            # Use tenant_id as part of the encryption key for additional isolation
            encryption_key = f"credential_key_{self.tenant_id}"
            
            # Encrypt using pgcrypto with AES
            encrypted_value = session.execute(
                text("SELECT pgp_sym_encrypt(:data, :key)"),
                {"data": credentials_json, "key": encryption_key}
            ).scalar()
            
            self._encrypted_credentials = encrypted_value
        else:
            # For SQLite (testing), store unencrypted JSON
            self._encrypted_credentials = credentials_json
    
    def get_credentials(self, session) -> Dict[str, Any]:
        """Decrypt and return credentials using pgcrypto."""
        if not self._encrypted_credentials:
            return {}
        
        # TODO: Replace database-specific logic with proper encryption abstraction layer
        if session.bind.dialect.name == "postgresql":
            # Use same tenant-specific key for decryption
            encryption_key = f"credential_key_{self.tenant_id}"
            
            # Decrypt using pgcrypto
            decrypted_value = session.execute(
                text("SELECT pgp_sym_decrypt(:data, :key)"),
                {"data": self._encrypted_credentials, "key": encryption_key}
            ).scalar()
            
            return json.loads(decrypted_value) if decrypted_value else {}
        else:
            # For SQLite (testing), parse unencrypted JSON
            return json.loads(self._encrypted_credentials)
    
    def is_expired(self) -> bool:
        """Check if credential has expired."""
        if not self.expires_at:
            return False
        return datetime.utcnow() > self.expires_at


class ExternalAccessToken(Base, BaseModel):
    """Secure access token model using PostgreSQL pgcrypto encryption."""
    
    __tablename__ = "external_access_tokens"
    
    # Tenant isolation
    tenant_id = Column(
        String(100), 
        ForeignKey('tenant.tenant_id', ondelete='CASCADE'), 
        nullable=False
    )
    
    # System identification
    system_name = Column(String(100), nullable=False)
    
    # pgcrypto encrypted token storage  
    _encrypted_access_token = Column('encrypted_access_token', EncryptedBinary, nullable=False)
    
    # Token metadata
    expires_at = Column(DateTime, nullable=False)
    
    # Relationships
    tenant = relationship("Tenant", backref="external_access_tokens")
    
    # Constraints and indexes
    __table_args__ = (
        Index('ix_token_tenant_system', 'tenant_id', 'system_name'),
        Index('ix_token_expires_at', 'expires_at'),
        Index('ix_token_tenant_id', 'tenant_id'),
        Index('ix_token_system_name', 'system_name'),
    )
    
    def set_access_token(self, access_token: str, session) -> None:
        """Encrypt and store access token using pgcrypto with tenant-specific key."""
        # TODO: Replace database-specific logic with proper encryption abstraction layer
        # Check if we're using PostgreSQL with pgcrypto
        if session.bind.dialect.name == "postgresql":
            # Use tenant_id as part of the encryption key for additional isolation
            encryption_key = f"token_key_{self.tenant_id}"
            
            # Encrypt using pgcrypto with AES
            encrypted_value = session.execute(
                text("SELECT pgp_sym_encrypt(:data, :key)"),
                {"data": access_token, "key": encryption_key}
            ).scalar()
            
            self._encrypted_access_token = encrypted_value
        else:
            # For SQLite (testing), store unencrypted
            # This is acceptable since SQLite is only used for unit tests
            self._encrypted_access_token = access_token
    
    def get_access_token(self, session) -> str:
        """Decrypt and return access token using pgcrypto."""
        if not self._encrypted_access_token:
            return ""
        
        # TODO: Replace database-specific logic with proper encryption abstraction layer
        if session.bind.dialect.name == "postgresql":
            # Use same tenant-specific key for decryption
            encryption_key = f"token_key_{self.tenant_id}"
            
            # Decrypt using pgcrypto
            decrypted_value = session.execute(
                text("SELECT pgp_sym_decrypt(:data, :key)"),
                {"data": self._encrypted_access_token, "key": encryption_key}
            ).scalar()
            
            return decrypted_value if decrypted_value else ""
        else:
            # For SQLite (testing), return unencrypted value
            return self._encrypted_access_token
    
    def is_expired(self) -> bool:
        """Check if token has expired."""
        return datetime.utcnow() > self.expires_at
    
    def is_valid(self) -> bool:
        """Check if token is valid (not expired)."""
        return not self.is_expired()