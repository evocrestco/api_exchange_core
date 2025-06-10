"""
Service for managing external system credentials with comprehensive security and audit logging.

This service provides high-level operations for credential management while ensuring
proper tenant isolation, security validation, and comprehensive audit trails.
"""

from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional

from src.context.service_decorators import handle_repository_errors
from src.context.tenant_context import TenantContext, tenant_aware
from src.db.db_credential_models import ExternalCredential
from src.exceptions import (
    ErrorCode, 
    ServiceError, 
    ValidationError,
    CredentialError, 
    CredentialNotFoundError,
    CredentialExpiredError,
    TenantIsolationViolationError
)
from src.repositories.credential_repository import CredentialRepository
from src.schemas.credential_schema import CredentialRead, CredentialCreate, CredentialUpdate, CredentialFilter
from src.services.base_service import BaseService
from src.utils.logger import get_logger


class CredentialService(BaseService[CredentialCreate, CredentialRead, CredentialUpdate, CredentialFilter]):
    """
    Service for managing external system credentials.
    
    This service provides:
    - High-level credential operations with business logic
    - Comprehensive audit logging for security compliance
    - Automatic tenant isolation and validation
    - Error handling with proper exception translation
    - Token lifecycle management
    """

    def __init__(self, credential_repository: CredentialRepository):
        super().__init__(credential_repository, CredentialRead)
        self.credential_repository = credential_repository

    @tenant_aware
    @handle_repository_errors("get_credentials")
    def get_credentials(self, system_name: str) -> Dict[str, Any]:
        """
        Get decrypted credentials for a specific external system.
        
        Args:
            system_name: Name of the external system
            
        Returns:
            Dictionary containing decrypted credential data
            
        Raises:
            CredentialNotFoundError: If credential doesn't exist
            CredentialExpiredError: If credential has expired
            ServiceError: If operation fails
        """
        tenant_id = TenantContext.get_current_tenant_id()
        
        self.logger.info(
            "Credential access attempt",
            extra={
                "tenant_id": tenant_id,
                "system_name": system_name,
                "operation": "get_credentials"
            }
        )
        
        # Get credential from repository
        credential = self.credential_repository.get_by_system_name(system_name)
        if not credential:
            self.logger.warning(
                "Credential not found",
                extra={
                    "tenant_id": tenant_id,
                    "system_name": system_name
                }
            )
            raise CredentialNotFoundError(
                f"No credentials found for system '{system_name}'"
            )
        
        # Check if credential is expired
        if credential.is_expired():
            self.logger.warning(
                "Attempted access to expired credential",
                extra={
                    "credential_id": credential.id,
                    "system_name": system_name,
                    "expires_at": credential.expires_at.isoformat() if credential.expires_at else None
                }
            )
            raise CredentialExpiredError(
                f"Credentials for system '{system_name}' have expired"
            )
        
        # Check if credential is active
        if credential.is_active != "active":
            self.logger.warning(
                "Attempted access to inactive credential",
                extra={
                    "credential_id": credential.id,
                    "system_name": system_name,
                    "status": credential.is_active
                }
            )
            raise CredentialExpiredError(
                f"Credentials for system '{system_name}' are not active (status: {credential.is_active})"
            )
        
        # Log successful access
        self.logger.info(
            "Credential access granted",
            extra={
                "credential_id": credential.id,
                "system_name": system_name,
                "auth_type": credential.auth_type,
                "expires_at": credential.expires_at.isoformat() if credential.expires_at else None
            }
        )
        
        # Get decrypted credentials
        credentials = credential.get_credentials(self.credential_repository.session)
        if not credentials:
            self.logger.error(
                "Failed to decrypt credential data",
                extra={
                    "credential_id": credential.id,
                    "system_name": system_name
                }
            )
            raise ServiceError(
                message="Failed to decrypt credential data",
                error_code=ErrorCode.INTEGRATION_ERROR,
                details={"system_name": system_name}
            )
        
        return credentials

    @tenant_aware
    @handle_repository_errors("store_credentials")
    def store_credentials(
        self,
        system_name: str,
        auth_type: str,
        credentials: Dict[str, Any],
        expires_at: Optional[datetime] = None,
        metadata: Optional[Dict[str, Any]] = None
    ) -> str:
        """
        Store new credentials for an external system.
        
        Args:
            system_name: Name of the external system
            auth_type: Type of authentication (e.g., 'api_token', 'oauth')
            credentials: Dictionary containing credential data to encrypt
            expires_at: Optional expiration datetime
            metadata: Optional non-sensitive metadata
            
        Returns:
            ID of the created credential
            
        Raises:
            ValidationError: If parameters are invalid
            ServiceError: If operation fails
        """
        tenant_id = TenantContext.get_current_tenant_id()
        
        self.logger.info(
            "Storing new credentials",
            extra={
                "tenant_id": tenant_id,
                "system_name": system_name,
                "auth_type": auth_type,
                "expires_at": expires_at.isoformat() if expires_at else None,
                "has_metadata": metadata is not None,
                "credential_keys": list(credentials.keys()) if credentials else []
            }
        )
        
        # Validate system_name
        if not system_name or not isinstance(system_name, str):
            raise ValidationError(
                message="system_name must be a non-empty string",
                error_code=ErrorCode.INVALID_FORMAT,
                details={"system_name": system_name}
            )
        
        # Check if credential already exists
        existing = self.credential_repository.get_by_system_name(system_name)
        if existing:
            self.logger.warning(
                "Attempted to store credential for existing system",
                extra={
                    "tenant_id": tenant_id,
                    "system_name": system_name,
                    "existing_credential_id": existing.id
                }
            )
            raise ValidationError(
                message=f"Credentials for system '{system_name}' already exist. Use update_credentials instead.",
                error_code=ErrorCode.DUPLICATE,
                details={"system_name": system_name}
            )
        
        # Create new credential
        try:
            credential = self.credential_repository.create_credential(
                system_name=system_name,
                auth_type=auth_type,
                credentials=credentials,
                expires_at=expires_at
            )
            
            self.logger.info(
                "Credentials stored successfully",
                extra={
                    "credential_id": credential.id,
                    "tenant_id": tenant_id,
                    "system_name": system_name,
                    "auth_type": auth_type
                }
            )
            
            return credential.id
            
        except Exception as e:
            self.logger.error(
                "Failed to store credentials",
                extra={
                    "tenant_id": tenant_id,
                    "system_name": system_name,
                    "auth_type": auth_type,
                    "error": str(e),
                    "error_type": type(e).__name__
                }
            )
            raise

    @tenant_aware
    @handle_repository_errors("update_credentials")
    def update_credentials(
        self,
        system_name: str,
        credentials: Dict[str, Any],
        expires_at: Optional[datetime] = None
    ) -> None:
        """
        Update existing credentials for an external system.
        
        Args:
            system_name: Name of the external system
            credentials: New credential data to encrypt
            expires_at: Optional new expiration datetime
            
        Raises:
            CredentialNotFoundError: If credential doesn't exist
            ServiceError: If operation fails
        """
        tenant_id = TenantContext.get_current_tenant_id()
        
        self.logger.info(
            "Updating credentials",
            extra={
                "tenant_id": tenant_id,
                "system_name": system_name,
                "expires_at": expires_at.isoformat() if expires_at else None,
                "credential_keys": list(credentials.keys()) if credentials else []
            }
        )
        
        # Update credential
        credential = self.credential_repository.update_credentials(
            system_name=system_name,
            credentials=credentials,
            expires_at=expires_at
        )
        
        self.logger.info(
            "Credentials updated successfully",
            extra={
                "credential_id": credential.id,
                "tenant_id": tenant_id,
                "system_name": system_name
            }
        )

    @tenant_aware
    @handle_repository_errors("delete_credentials")
    def delete_credentials(self, system_name: str) -> bool:
        """
        Delete credentials for an external system.
        
        Args:
            system_name: Name of the external system
            
        Returns:
            True if credential was deleted, False if not found
            
        Raises:
            ServiceError: If operation fails
        """
        tenant_id = TenantContext.get_current_tenant_id()
        
        self.logger.info(
            "Deleting credentials",
            extra={
                "tenant_id": tenant_id,
                "system_name": system_name
            }
        )
        
        deleted = self.credential_repository.delete_credential(system_name)
        
        if deleted:
            self.logger.info(
                "Credentials deleted successfully",
                extra={
                    "tenant_id": tenant_id,
                    "system_name": system_name
                }
            )
        else:
            self.logger.debug(
                "No credentials found to delete",
                extra={
                    "tenant_id": tenant_id,
                    "system_name": system_name
                }
            )
        
        return deleted

