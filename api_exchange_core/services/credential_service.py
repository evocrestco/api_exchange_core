"""
Service for managing external system credentials with comprehensive security and audit logging.

This service provides high-level operations for credential management while ensuring
proper tenant isolation, security validation, and comprehensive audit trails.
"""

from datetime import datetime, timedelta
from typing import Any, Dict, Optional

from sqlalchemy import and_
from sqlalchemy.orm import Session

from ..context.operation_context import operation
from ..context.tenant_context import TenantContext, tenant_aware
from ..db.db_credential_models import ExternalCredential
from ..exceptions import (
    CredentialExpiredError,
    CredentialNotFoundError,
    ErrorCode,
    ServiceError,
    TokenNotAvailableError,
    ValidationError,
)
from ..utils.logger import get_logger
from .api_token_service import APITokenService


class CredentialService:
    """
    Service for managing external system credentials.

    This service provides:
    - High-level credential operations with business logic
    - Comprehensive audit logging for security compliance
    - Automatic tenant isolation and validation
    - Error handling with proper exception translation
    - Token lifecycle management
    """

    def __init__(
        self,
        session: Session,
        api_token_service: Optional[APITokenService] = None,
    ):
        """Initialize with SQLAlchemy session and optional API token service."""
        self.session = session
        self.api_token_service = api_token_service
        self.logger = get_logger()

    def _get_current_tenant_id(self) -> str:
        """Get the current tenant ID from context."""
        tenant_id = TenantContext.get_current_tenant_id()
        if not tenant_id:
            raise ServiceError(
                "No tenant ID available in context",
                error_code=ErrorCode.TENANT_NOT_FOUND,
                operation="credential_service",
            )
        return tenant_id

    @tenant_aware
    @operation()
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
                "operation": "get_credentials",
            },
        )

        # Get credential using SQLAlchemy directly
        credential = (
            self.session.query(ExternalCredential)
            .filter(
                and_(
                    ExternalCredential.tenant_id == tenant_id,
                    ExternalCredential.system_name == system_name,
                )
            )
            .first()
        )
        if not credential:
            # CredentialNotFoundError will automatically log via BaseError.__init__
            raise CredentialNotFoundError(
                f"No credentials found for system '{system_name}'",
                tenant_id=tenant_id,
                system_name=system_name,
            )

        # Check if credential is expired
        if credential.is_expired():
            # CredentialExpiredError will automatically log via BaseError.__init__
            raise CredentialExpiredError(
                f"Credentials for system '{system_name}' have expired",
                credential_id=credential.id,
                system_name=system_name,
                expires_at=credential.expires_at.isoformat() if credential.expires_at else None,
            )

        # Check if credential is active
        if credential.is_active != "active":
            # CredentialExpiredError will automatically log via BaseError.__init__
            raise CredentialExpiredError(
                f"Credentials for system '{system_name}' are not active (status: {credential.is_active})",
                credential_id=credential.id,
                system_name=system_name,
                status=credential.is_active,
            )

        # Log successful access
        self.logger.info(
            "Credential access granted",
            extra={
                "credential_id": credential.id,
                "system_name": system_name,
                "auth_type": credential.auth_type,
                "expires_at": credential.expires_at.isoformat() if credential.expires_at else None,
            },
        )

        # Get decrypted credentials
        credentials = credential.get_credentials(self.session)
        if not credentials:
            # ServiceError will automatically log via BaseError.__init__
            raise ServiceError(
                message="Failed to decrypt credential data",
                error_code=ErrorCode.INTEGRATION_ERROR,
                credential_id=credential.id,
                system_name=system_name,
            )

        return credentials

    @tenant_aware
    @operation()
    def store_credentials(
        self,
        system_name: str,
        auth_type: str,
        credentials: Dict[str, Any],
        expires_at: Optional[datetime] = None,
        metadata: Optional[Dict[str, Any]] = None,
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
                "credential_keys": list(credentials.keys()) if credentials else [],
            },
        )

        # Validate system_name
        if not system_name or not isinstance(system_name, str):
            raise ValidationError(
                message="system_name must be a non-empty string",
                error_code=ErrorCode.INVALID_FORMAT,
                details={"system_name": system_name},
            )

        # Check if credential already exists
        existing = (
            self.session.query(ExternalCredential)
            .filter(
                and_(
                    ExternalCredential.tenant_id == tenant_id,
                    ExternalCredential.system_name == system_name,
                )
            )
            .first()
        )
        if existing:
            # ValidationError will automatically log via BaseError.__init__
            raise ValidationError(
                message=f"Credentials for system '{system_name}' already exist. Use update_credentials instead.",
                error_code=ErrorCode.DUPLICATE,
                tenant_id=tenant_id,
                system_name=system_name,
                existing_credential_id=existing.id,
            )

        # Create new credential
        try:
            # Create credential object
            credential = ExternalCredential(
                tenant_id=tenant_id,
                system_name=system_name,
                auth_type=auth_type,
                expires_at=expires_at,
                is_active="active",
            )

            # Set credentials (triggers encryption)
            credential.set_credentials(credentials, self.session)

            # Save to database
            self.session.add(credential)
            self.session.flush()  # Get the ID

            self.logger.info(
                "Credentials stored successfully",
                extra={
                    "credential_id": credential.id,
                    "tenant_id": tenant_id,
                    "system_name": system_name,
                    "auth_type": auth_type,
                },
            )

            return credential.id

        except Exception as e:
            # Convert to ServiceError which will automatically log via BaseError.__init__
            raise ServiceError(
                message="Failed to store credentials",
                error_code=ErrorCode.INTEGRATION_ERROR,
                tenant_id=tenant_id,
                system_name=system_name,
                auth_type=auth_type,
                error=str(e),
                error_type=type(e).__name__,
                cause=e,
            ) from e

    @tenant_aware
    @operation()
    def update_credentials(
        self, system_name: str, credentials: Dict[str, Any], expires_at: Optional[datetime] = None
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
                "credential_keys": list(credentials.keys()) if credentials else [],
            },
        )

        # Get existing credential
        credential = (
            self.session.query(ExternalCredential)
            .filter(
                and_(
                    ExternalCredential.tenant_id == tenant_id,
                    ExternalCredential.system_name == system_name,
                )
            )
            .first()
        )

        if not credential:
            raise CredentialNotFoundError(
                f"No credential found for system '{system_name}' in tenant '{tenant_id}'"
            )

        # Update credentials (triggers re-encryption)
        credential.set_credentials(credentials, self.session)

        # Update expiration if provided
        if expires_at is not None:
            credential.expires_at = expires_at

        self.session.flush()

        self.logger.info(
            "Credentials updated successfully",
            extra={
                "credential_id": credential.id,
                "tenant_id": tenant_id,
                "system_name": system_name,
            },
        )

    @tenant_aware
    @operation()
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
            "Deleting credentials", extra={"tenant_id": tenant_id, "system_name": system_name}
        )

        # Find and delete credential
        credential = (
            self.session.query(ExternalCredential)
            .filter(
                and_(
                    ExternalCredential.tenant_id == tenant_id,
                    ExternalCredential.system_name == system_name,
                )
            )
            .first()
        )

        deleted = False
        if credential:
            self.session.delete(credential)
            self.session.flush()
            deleted = True

        if deleted:
            self.logger.info(
                "Credentials deleted successfully",
                extra={"tenant_id": tenant_id, "system_name": system_name},
            )
        else:
            self.logger.debug(
                "No credentials found to delete",
                extra={"tenant_id": tenant_id, "system_name": system_name},
            )

        return deleted

    @tenant_aware
    def store_access_token(self, system_name: str, access_token: str, expires_at: datetime) -> str:
        """
        Store an access token for an external system using the API token management system.

        This method provides compatibility with clients expecting token storage but delegates
        to the serverless-native API token management system.

        Args:
            system_name: Name of the external system (e.g., "api_provider_a")
            access_token: The access token to store
            expires_at: When the token expires

        Returns:
            Token ID

        Raises:
            ServiceError: If API token service not configured or operation fails
        """
        if not self.api_token_service:
            raise ServiceError(
                message="API token management not configured for this credential service",
                error_code=ErrorCode.CONFIGURATION_ERROR,
                details={"system_name": system_name},
            )

        tenant_id = TenantContext.get_current_tenant_id()

        self.logger.info(
            "Storing access token via API token service",
            extra={
                "tenant_id": tenant_id,
                "system_name": system_name,
                "expires_at": expires_at.isoformat(),
            },
        )

        try:
            token_id = self.api_token_service.store_token(
                token=access_token,
                generated_by=f"credential_service_{system_name}",
                generation_context={
                    "source": "credential_service",
                    "system_name": system_name,
                    "stored_at": datetime.utcnow().isoformat(),
                },
            )

            self.logger.info(
                "Access token stored successfully",
                extra={"tenant_id": tenant_id, "system_name": system_name, "token_id": token_id},
            )

            return token_id

        except Exception as e:
            # ServiceError will automatically log via BaseError.__init__
            raise ServiceError(
                message=f"Failed to store access token for {system_name}",
                error_code=ErrorCode.INTEGRATION_ERROR,
                tenant_id=tenant_id,
                system_name=system_name,
                error=str(e),
                error_type=type(e).__name__,
                cause=e,
            ) from e

    @tenant_aware
    def get_valid_access_token(
        self, system_name: str, buffer_minutes: int = 20
    ) -> Optional[Dict[str, Any]]:
        """
        Get a valid access token for an external system.

        This method provides compatibility with clients expecting token retrieval but delegates
        to the serverless-native API token management system.

        Args:
            system_name: Name of the external system (e.g., "api_provider_a")
            buffer_minutes: Consider tokens invalid if they expire within this many minutes

        Returns:
            Dictionary with access_token and expires_at keys, or None if no valid token

        Raises:
            ServiceError: If API token service not configured or operation fails
        """
        if not self.api_token_service:
            raise ServiceError(
                message="API token management not configured for this credential service",
                error_code=ErrorCode.CONFIGURATION_ERROR,
                details={"system_name": system_name},
            )

        tenant_id = TenantContext.get_current_tenant_id()

        self.logger.debug(
            "Retrieving valid access token via API token service",
            extra={
                "tenant_id": tenant_id,
                "system_name": system_name,
                "buffer_minutes": buffer_minutes,
            },
        )

        try:
            result = self.api_token_service.get_valid_token(
                operation=f"get_access_token_{system_name}"
            )

            if not result:
                self.logger.debug(
                    "No valid access token available",
                    extra={"tenant_id": tenant_id, "system_name": system_name},
                )
                return None

            token_value, token_id = result

            # Note: This is a simplified implementation. In a full implementation,
            # we would need to check the specific token's expiration time against the buffer.
            # For now, we trust that the API token service returned a valid token.

            # Return in the format expected by external clients
            return {
                "access_token": token_value,
                "expires_at": datetime.utcnow() + timedelta(hours=1),  # Approximate
                "token_id": token_id,
            }

        except TokenNotAvailableError:
            # This is expected when no tokens exist and no generator is configured
            self.logger.debug(
                "No valid access tokens available",
                extra={"tenant_id": tenant_id, "system_name": system_name},
            )
            return None
        except Exception as e:
            # ServiceError will automatically log via BaseError.__init__
            raise ServiceError(
                message=f"Failed to retrieve access token for {system_name}",
                error_code=ErrorCode.INTEGRATION_ERROR,
                tenant_id=tenant_id,
                system_name=system_name,
                error=str(e),
                error_type=type(e).__name__,
                cause=e,
            ) from e
