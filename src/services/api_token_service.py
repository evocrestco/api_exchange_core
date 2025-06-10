"""
Generic API token service for serverless environments.

This service provides high-level token management operations while ensuring
proper tenant isolation, configurable limits, and comprehensive audit trails.
"""

from datetime import datetime
from typing import Dict, Any, Optional, Tuple, Callable

from src.context.service_decorators import handle_repository_errors
from src.context.tenant_context import TenantContext, tenant_aware
from src.exceptions import (
    ErrorCode, 
    ServiceError, 
    ValidationError,
    TokenNotAvailableError
)
from src.repositories.api_token_repository import APITokenRepository
from src.services.base_service import BaseService
from src.utils.logger import get_logger


class APITokenService:
    """
    Service for managing API tokens in serverless environments.
    
    This service provides:
    - High-level token operations with business logic
    - Configurable for any API provider
    - Automatic tenant isolation and validation
    - Error handling with proper exception translation
    - Token lifecycle management with cleanup
    - Circuit breaker pattern for token generation failures
    """

    def __init__(
        self, 
        token_repository: APITokenRepository,
        token_generator: Optional[Callable[[], str]] = None
    ):
        """
        Initialize service with repository and optional token generator.
        
        Args:
            token_repository: Repository for token operations
            token_generator: Optional function to generate new tokens
        """
        self.token_repository = token_repository
        self.token_generator = token_generator
        self.logger = get_logger()
    
    @tenant_aware
    @handle_repository_errors("get_token")
    def get_valid_token(self, operation: str = "api_call") -> Tuple[str, str]:
        """
        Get a valid token for API operations, generating one if necessary.
        
        Args:
            operation: Description of operation needing token
            
        Returns:
            Tuple of (token_value, token_id)
            
        Raises:
            TokenNotAvailableError: If no tokens available and can't generate
            ServiceError: If operation fails
        """
        tenant_id = TenantContext.get_current_tenant_id()
        
        self.logger.info(
            "Token request received",
            extra={
                "api_provider": self.token_repository.api_provider,
                "tenant_id": tenant_id,
                "operation": operation
            }
        )
        
        # Try to get existing valid token first
        token_result = self.token_repository.get_valid_token(operation)
        
        if token_result:
            token_value, token_id = token_result
            self.logger.info(
                "Existing token provided",
                extra={
                    "api_provider": self.token_repository.api_provider,
                    "token_id": token_id,
                    "operation": operation
                }
            )
            return token_value, token_id
        
        # No existing tokens, try to generate a new one
        if not self.token_generator:
            self.logger.warning(
                "No valid tokens available and no token generator configured",
                extra={
                    "api_provider": self.token_repository.api_provider,
                    "tenant_id": tenant_id,
                    "operation": operation
                }
            )
            raise TokenNotAvailableError(
                f"No valid tokens available for {self.token_repository.api_provider} "
                f"and no token generator configured"
            )
        
        # Try to generate and store new token
        try:
            new_token = self.token_generator()
            
            if not new_token:
                raise ServiceError(
                    message="Token generator returned empty token",
                    error_code=ErrorCode.INTEGRATION_ERROR,
                    details={
                        "api_provider": self.token_repository.api_provider,
                        "operation": operation
                    }
                )
            
            # Store the new token
            token_id = self.store_token(
                token=new_token,
                generated_by="api_token_service",
                generation_context={
                    "trigger": "no_valid_tokens",
                    "operation": operation,
                    "generated_at": datetime.utcnow().isoformat()
                }
            )
            
            self.logger.info(
                "New token generated and stored",
                extra={
                    "api_provider": self.token_repository.api_provider,
                    "token_id": token_id,
                    "operation": operation
                }
            )
            
            return new_token, token_id
            
        except ValidationError as e:
            if e.error_code == ErrorCode.LIMIT_EXCEEDED:
                # Token limit reached, perform cleanup and try once more
                self.logger.info(
                    "Token limit reached, attempting cleanup",
                    extra={
                        "api_provider": self.token_repository.api_provider,
                        "tenant_id": tenant_id
                    }
                )
                
                cleaned = self.cleanup_expired_tokens(force_cleanup=True)
                
                if cleaned > 0:
                    # Try to store token again after cleanup
                    try:
                        token_id = self.store_token(
                            token=new_token,
                            generated_by="api_token_service_after_cleanup",
                            generation_context={
                                "trigger": "post_cleanup",
                                "tokens_cleaned": cleaned,
                                "operation": operation,
                                "generated_at": datetime.utcnow().isoformat()
                            }
                        )
                        
                        self.logger.info(
                            "Token stored after cleanup",
                            extra={
                                "api_provider": self.token_repository.api_provider,
                                "token_id": token_id,
                                "tokens_cleaned": cleaned
                            }
                        )
                        
                        return new_token, token_id
                        
                    except ValidationError:
                        # Still at limit after cleanup
                        pass
                
                raise TokenNotAvailableError(
                    f"Token limit reached for {self.token_repository.api_provider} "
                    f"and cleanup did not free space"
                )
            else:
                raise
        except Exception as e:
            self.logger.error(
                "Failed to generate new token",
                extra={
                    "api_provider": self.token_repository.api_provider,
                    "tenant_id": tenant_id,
                    "operation": operation,
                    "error": str(e),
                    "error_type": type(e).__name__
                }
            )
            raise ServiceError(
                message="Failed to generate new token",
                error_code=ErrorCode.INTEGRATION_ERROR,
                details={
                    "api_provider": self.token_repository.api_provider,
                    "operation": operation,
                    "error": str(e)
                }
            ) from e
    
    @tenant_aware
    @handle_repository_errors("store_token")
    def store_token(
        self,
        token: str,
        generated_by: str,
        generation_context: Optional[Dict[str, Any]] = None
    ) -> str:
        """
        Store a new token.
        
        Args:
            token: The token value to store
            generated_by: Identifier of what generated this token
            generation_context: Metadata about token generation
            
        Returns:
            Token ID
            
        Raises:
            ValidationError: If token is invalid or limits exceeded
            ServiceError: If operation fails
        """
        tenant_id = TenantContext.get_current_tenant_id()
        
        self.logger.info(
            "Storing new token",
            extra={
                "api_provider": self.token_repository.api_provider,
                "tenant_id": tenant_id,
                "generated_by": generated_by
            }
        )
        
        token_id = self.token_repository.store_new_token(
            token=token,
            generated_by=generated_by,
            generation_context=generation_context
        )
        
        self.logger.info(
            "Token stored successfully",
            extra={
                "api_provider": self.token_repository.api_provider,
                "token_id": token_id,
                "generated_by": generated_by
            }
        )
        
        return token_id
    
    @tenant_aware
    @handle_repository_errors("cleanup_tokens")
    def cleanup_expired_tokens(self, force_cleanup: bool = False) -> int:
        """
        Clean up expired tokens.
        
        Args:
            force_cleanup: If True, force cleanup even if locks are busy
            
        Returns:
            Number of tokens cleaned up
            
        Raises:
            ServiceError: If operation fails
        """
        tenant_id = TenantContext.get_current_tenant_id()
        
        self.logger.debug(
            "Starting token cleanup",
            extra={
                "api_provider": self.token_repository.api_provider,
                "tenant_id": tenant_id,
                "force_cleanup": force_cleanup
            }
        )
        
        cleaned_count = self.token_repository.cleanup_expired_tokens(force_cleanup)
        
        if cleaned_count > 0:
            self.logger.info(
                "Token cleanup completed",
                extra={
                    "api_provider": self.token_repository.api_provider,
                    "tenant_id": tenant_id,
                    "tokens_cleaned": cleaned_count
                }
            )
        
        return cleaned_count
    
    @tenant_aware
    @handle_repository_errors("get_token_stats")
    def get_token_statistics(self) -> Dict[str, Any]:
        """
        Get token pool statistics for monitoring.
        
        Returns:
            Dictionary with token statistics
            
        Raises:
            ServiceError: If operation fails
        """
        tenant_id = TenantContext.get_current_tenant_id()
        
        self.logger.debug(
            "Getting token statistics",
            extra={
                "api_provider": self.token_repository.api_provider,
                "tenant_id": tenant_id
            }
        )
        
        stats = self.token_repository.get_token_stats()
        
        # Add service-level information
        stats.update({
            "has_token_generator": self.token_generator is not None,
            "service_class": self.__class__.__name__
        })
        
        return stats
    
    def configure_token_generator(self, token_generator: Callable[[], str]) -> None:
        """
        Configure or update the token generator function.
        
        Args:
            token_generator: Function that returns new token strings
        """
        self.token_generator = token_generator
        
        self.logger.info(
            "Token generator configured",
            extra={
                "api_provider": self.token_repository.api_provider,
                "generator_type": type(token_generator).__name__
            }
        )