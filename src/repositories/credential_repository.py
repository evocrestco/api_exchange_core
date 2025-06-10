"""
Repository for managing external system credentials with enhanced security.

This repository provides secure, tenant-isolated access to external system
credentials using PostgreSQL's pgcrypto for encryption.
"""

from datetime import datetime
from typing import Dict, Any, List, Optional

from sqlalchemy import and_, or_, text
from sqlalchemy.orm import Session

from src.context.tenant_context import tenant_aware
from src.db.db_credential_models import ExternalCredential
from src.exceptions import (
    ErrorCode, 
    RepositoryError, 
    ValidationError,
    CredentialNotFoundError,
    TenantIsolationViolationError
)
from src.repositories.base_repository import BaseRepository
from src.utils.logger import get_logger


class CredentialRepository(BaseRepository[ExternalCredential]):
    """
    Repository for managing external system credentials with enhanced security.
    
    This repository provides:
    - Automatic tenant isolation for all operations
    - Enhanced security validation with paranoid checks
    - pgcrypto integration for transparent encryption
    - Comprehensive audit logging
    """

    def __init__(self, session: Session):
        super().__init__(session, ExternalCredential)
        self.logger = get_logger()

    def _validate_tenant_isolation(self, credential: ExternalCredential, expected_tenant_id: str) -> None:
        """
        Paranoid validation to ensure tenant isolation is maintained.
        
        Args:
            credential: The credential object to validate
            expected_tenant_id: The expected tenant ID
            
        Raises:
            TenantIsolationViolationError: If tenant isolation is violated
        """
        if credential.tenant_id != expected_tenant_id:
            self.logger.error(
                "SECURITY VIOLATION: Credential tenant mismatch detected",
                extra={
                    "expected_tenant_id": expected_tenant_id,
                    "actual_tenant_id": credential.tenant_id,
                    "credential_id": credential.id,
                    "system_name": credential.system_name
                }
            )
            raise TenantIsolationViolationError(
                f"Credential belongs to tenant {credential.tenant_id}, "
                f"but operation requested for tenant {expected_tenant_id}"
            )

    @tenant_aware
    def get_by_system_name(self, system_name: str) -> Optional[ExternalCredential]:
        """
        Get credential by system name for the current tenant.
        
        Args:
            system_name: Name of the external system
            
        Returns:
            ExternalCredential object or None if not found
            
        Raises:
            RepositoryError: If database operation fails
            TenantIsolationViolationError: If tenant isolation is violated
        """
        try:
            tenant_id = self._get_current_tenant_id()
            
            self.logger.debug(
                "Getting credential by system name",
                extra={
                    "tenant_id": tenant_id,
                    "system_name": system_name
                }
            )
            
            # Query with explicit tenant filtering
            credential = self.session.query(ExternalCredential).filter(
                and_(
                    ExternalCredential.tenant_id == tenant_id,
                    ExternalCredential.system_name == system_name
                )
            ).first()
            
            if credential:
                # Paranoid check: verify tenant isolation
                self._validate_tenant_isolation(credential, tenant_id)
                
                self.logger.debug(
                    "Credential found and validated",
                    extra={
                        "credential_id": credential.id,
                        "system_name": system_name,
                        "is_active": credential.is_active,
                        "expires_at": credential.expires_at.isoformat() if credential.expires_at else None
                    }
                )
            else:
                self.logger.debug(
                    "No credential found for system",
                    extra={
                        "tenant_id": tenant_id,
                        "system_name": system_name
                    }
                )
            
            return credential
            
        except TenantIsolationViolationError:
            raise
        except Exception as e:
            self.logger.error(
                "Failed to get credential by system name",
                extra={
                    "system_name": system_name,
                    "error": str(e),
                    "error_type": type(e).__name__
                }
            )
            raise RepositoryError(
                message="Failed to retrieve credential",
                error_code=ErrorCode.DATABASE_ERROR,
                details={"system_name": system_name, "error": str(e)},
                cause=e
            ) from e

    @tenant_aware
    def create_credential(
        self, 
        system_name: str, 
        auth_type: str, 
        credentials: Dict[str, Any],
        expires_at: Optional[datetime] = None
    ) -> ExternalCredential:
        """
        Create a new credential for the current tenant.
        
        Args:
            system_name: Name of the external system
            auth_type: Type of authentication (e.g., 'api_token', 'oauth')
            credentials: Dictionary containing credential data to encrypt
            expires_at: Optional expiration datetime
            
        Returns:
            Created ExternalCredential object
            
        Raises:
            ValidationError: If parameters are invalid
            RepositoryError: If creation fails
        """
        try:
            tenant_id = self._get_current_tenant_id()
            
            # Validate input parameters
            if not system_name or not isinstance(system_name, str):
                raise ValidationError(
                    message="system_name must be a non-empty string",
                    error_code=ErrorCode.INVALID_FORMAT,
                    details={"system_name": system_name}
                )
            
            if not auth_type or not isinstance(auth_type, str):
                raise ValidationError(
                    message="auth_type must be a non-empty string",
                    error_code=ErrorCode.INVALID_FORMAT,
                    details={"auth_type": auth_type}
                )
            
            if not credentials or not isinstance(credentials, dict):
                raise ValidationError(
                    message="credentials must be a non-empty dictionary",
                    error_code=ErrorCode.INVALID_FORMAT,
                    details={"credentials_type": type(credentials).__name__}
                )
            
            self.logger.info(
                "Creating new credential",
                extra={
                    "tenant_id": tenant_id,
                    "system_name": system_name,
                    "auth_type": auth_type,
                    "expires_at": expires_at.isoformat() if expires_at else None,
                    "credential_keys": list(credentials.keys())
                }
            )
            
            # Create credential object
            credential = ExternalCredential(
                tenant_id=tenant_id,
                system_name=system_name,
                auth_type=auth_type,
                expires_at=expires_at,
                is_active="active"
            )
            
            # Set credentials (triggers encryption)
            credential.set_credentials(credentials, self.session)
            
            # Save to database
            self.session.add(credential)
            self.session.flush()  # Get the ID
            
            self.logger.info(
                "Credential created successfully",
                extra={
                    "credential_id": credential.id,
                    "tenant_id": tenant_id,
                    "system_name": system_name
                }
            )
            
            return credential
            
        except (ValidationError, TenantIsolationViolationError):
            raise
        except Exception as e:
            self.logger.error(
                "Failed to create credential",
                extra={
                    "system_name": system_name,
                    "auth_type": auth_type,
                    "error": str(e),
                    "error_type": type(e).__name__
                }
            )
            raise RepositoryError(
                message="Failed to create credential",
                error_code=ErrorCode.DATABASE_ERROR,
                details={
                    "system_name": system_name,
                    "auth_type": auth_type,
                    "error": str(e)
                },
                cause=e
            ) from e

    @tenant_aware
    def update_credentials(
        self, 
        system_name: str, 
        credentials: Dict[str, Any],
        expires_at: Optional[datetime] = None
    ) -> ExternalCredential:
        """
        Update existing credentials for a system.
        
        Args:
            system_name: Name of the external system
            credentials: New credential data to encrypt
            expires_at: Optional new expiration datetime
            
        Returns:
            Updated ExternalCredential object
            
        Raises:
            CredentialNotFoundError: If credential doesn't exist
            RepositoryError: If update fails
        """
        try:
            tenant_id = self._get_current_tenant_id()
            
            # Get existing credential
            credential = self.get_by_system_name(system_name)
            if not credential:
                raise CredentialNotFoundError(
                    f"No credential found for system '{system_name}' "
                    f"in tenant '{tenant_id}'"
                )
            
            self.logger.info(
                "Updating credential",
                extra={
                    "credential_id": credential.id,
                    "system_name": system_name,
                    "old_expires_at": credential.expires_at.isoformat() if credential.expires_at else None,
                    "new_expires_at": expires_at.isoformat() if expires_at else None
                }
            )
            
            # Update credentials (triggers re-encryption)
            credential.set_credentials(credentials, self.session)
            
            # Update expiration if provided
            if expires_at is not None:
                credential.expires_at = expires_at
            
            # Update timestamp is handled automatically by BaseModel
            
            self.logger.info(
                "Credential updated successfully",
                extra={
                    "credential_id": credential.id,
                    "system_name": system_name
                }
            )
            
            return credential
            
        except (CredentialNotFoundError, TenantIsolationViolationError):
            raise
        except Exception as e:
            self.logger.error(
                "Failed to update credential",
                extra={
                    "system_name": system_name,
                    "error": str(e),
                    "error_type": type(e).__name__
                }
            )
            raise RepositoryError(
                message="Failed to update credential",
                error_code=ErrorCode.DATABASE_ERROR,
                details={"system_name": system_name, "error": str(e)},
                cause=e
            ) from e

    @tenant_aware
    def list_credentials(self, include_expired: bool = False) -> List[ExternalCredential]:
        """
        List all credentials for the current tenant.
        
        Args:
            include_expired: Whether to include expired credentials
            
        Returns:
            List of ExternalCredential objects
        """
        try:
            tenant_id = self._get_current_tenant_id()
            
            self.logger.debug(
                "Listing credentials",
                extra={
                    "tenant_id": tenant_id,
                    "include_expired": include_expired
                }
            )
            
            query = self.session.query(ExternalCredential).filter(
                ExternalCredential.tenant_id == tenant_id
            )
            
            if not include_expired:
                query = query.filter(
                    or_(
                        ExternalCredential.expires_at.is_(None),
                        ExternalCredential.expires_at > datetime.utcnow()
                    )
                )
            
            credentials = query.order_by(ExternalCredential.system_name).all()
            
            # Paranoid check: validate all credentials belong to correct tenant
            for credential in credentials:
                self._validate_tenant_isolation(credential, tenant_id)
            
            self.logger.debug(
                "Listed credentials successfully",
                extra={
                    "tenant_id": tenant_id,
                    "credential_count": len(credentials),
                    "include_expired": include_expired
                }
            )
            
            return credentials
            
        except TenantIsolationViolationError:
            raise
        except Exception as e:
            self.logger.error(
                "Failed to list credentials",
                extra={
                    "include_expired": include_expired,
                    "error": str(e),
                    "error_type": type(e).__name__
                }
            )
            raise RepositoryError(
                message="Failed to list credentials",
                error_code=ErrorCode.DATABASE_ERROR,
                details={"error": str(e)},
                cause=e
            ) from e

    @tenant_aware
    def delete_credential(self, system_name: str) -> bool:
        """
        Delete a credential by system name.
        
        Args:
            system_name: Name of the external system
            
        Returns:
            True if credential was deleted, False if not found
            
        Raises:
            RepositoryError: If deletion fails
        """
        try:
            tenant_id = self._get_current_tenant_id()
            
            credential = self.get_by_system_name(system_name)
            if not credential:
                self.logger.debug(
                    "Credential not found for deletion",
                    extra={
                        "tenant_id": tenant_id,
                        "system_name": system_name
                    }
                )
                return False
            
            self.logger.info(
                "Deleting credential",
                extra={
                    "credential_id": credential.id,
                    "system_name": system_name
                }
            )
            
            self.session.delete(credential)
            
            self.logger.info(
                "Credential deleted successfully",
                extra={
                    "credential_id": credential.id,
                    "system_name": system_name
                }
            )
            
            return True
            
        except TenantIsolationViolationError:
            raise
        except Exception as e:
            self.logger.error(
                "Failed to delete credential",
                extra={
                    "system_name": system_name,
                    "error": str(e),
                    "error_type": type(e).__name__
                }
            )
            raise RepositoryError(
                message="Failed to delete credential",
                error_code=ErrorCode.DATABASE_ERROR,
                details={"system_name": system_name, "error": str(e)},
                cause=e
            ) from e

    @tenant_aware
    def get_expiring_credentials(self, minutes_ahead: int = 30) -> List[ExternalCredential]:
        """
        Get credentials that will expire within the specified time window.
        
        Args:
            minutes_ahead: Number of minutes to look ahead for expiring credentials
            
        Returns:
            List of credentials expiring soon
        """
        try:
            tenant_id = self._get_current_tenant_id()
            
            from datetime import timedelta
            warning_time = datetime.utcnow() + timedelta(minutes=minutes_ahead)
            
            credentials = self.session.query(ExternalCredential).filter(
                and_(
                    ExternalCredential.tenant_id == tenant_id,
                    ExternalCredential.expires_at.isnot(None),
                    ExternalCredential.expires_at <= warning_time,
                    ExternalCredential.is_active == "active"
                )
            ).all()
            
            # Paranoid check
            for credential in credentials:
                self._validate_tenant_isolation(credential, tenant_id)
            
            self.logger.debug(
                "Found expiring credentials",
                extra={
                    "tenant_id": tenant_id,
                    "minutes_ahead": minutes_ahead,
                    "expiring_count": len(credentials)
                }
            )
            
            return credentials
            
        except TenantIsolationViolationError:
            raise
        except Exception as e:
            self.logger.error(
                "Failed to get expiring credentials",
                extra={
                    "minutes_ahead": minutes_ahead,
                    "error": str(e)
                }
            )
            raise RepositoryError(
                message="Failed to get expiring credentials",
                error_code=ErrorCode.DATABASE_ERROR,
                details={"error": str(e)},
                cause=e
            ) from e

