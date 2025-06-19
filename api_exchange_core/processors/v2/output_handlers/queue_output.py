"""
Azure Storage Queue output handler.

Provides output handling for Azure Storage Queues, supporting both development
storage (Azurite) and production Azure Storage accounts.
"""

import json
from typing import TYPE_CHECKING, Any, Dict, Optional, Union

from azure.core.exceptions import ResourceNotFoundError, ServiceRequestError
from azure.storage.queue import QueueClient, QueueServiceClient

from .base import OutputHandler, OutputHandlerError, OutputHandlerResult
from .config import OutputHandlerConfigFactory, QueueOutputHandlerConfig

if TYPE_CHECKING:
    from ...processing_result import ProcessingResult
    from ..message import Message


class QueueOutputHandler(OutputHandler):
    """
    Output handler for Azure Storage Queues.

    Sends processed messages to Azure Storage Queues for subsequent processing
    by other components in the pipeline. Supports automatic queue creation
    and handles connection failures gracefully.

    Configuration:
        - connection_string: Azure Storage connection string
        - auto_create_queue: Whether to create queue if it doesn't exist (default: True)
        - message_ttl_seconds: Time-to-live for messages in seconds (default: 604800 = 7 days)
        - visibility_timeout_seconds: Visibility timeout for dequeued messages (default: 30)

    Example:
        handler = QueueOutputHandler(
            destination="next-step-queue",
            config={
                "connection_string": "UseDevelopmentStorage=true",
                "auto_create_queue": True,
                "message_ttl_seconds": 86400  # 1 day
            }
        )
    """

    def __init__(
        self,
        destination: str,
        config: Optional[Union[Dict[str, Any], QueueOutputHandlerConfig]] = None,
    ):
        """
        Initialize the Azure Storage Queue output handler.

        Args:
            destination: Name of the target queue
            config: Configuration dictionary, QueueOutputHandlerConfig object, or None
        """
        # Handle configuration
        if isinstance(config, QueueOutputHandlerConfig):
            # Use the config object directly
            self._handler_config = config
            # Update destination if different
            if self._handler_config.destination != destination:
                self._handler_config.destination = destination
            # Convert to dict for parent class
            config_dict = self._handler_config.to_dict()
        else:
            # Create config object from dict or defaults
            self._handler_config = OutputHandlerConfigFactory.create_config(
                "queue", destination, config
            )
            config_dict = config if config else {}

        # Initialize parent with dict config for backward compatibility
        super().__init__(destination, config_dict)

        # Extract configuration values
        self.connection_string = self._handler_config.connection_string
        self.auto_create_queue = self._handler_config.auto_create_queue
        self.message_ttl_seconds = self._handler_config.message_ttl_seconds
        self.visibility_timeout_seconds = self._handler_config.visibility_timeout_seconds

        # Initialize clients (lazy initialization)
        self._queue_client: Optional[QueueClient] = None
        self._service_client: Optional[QueueServiceClient] = None

    def validate_configuration(self) -> bool:
        """Validate queue handler configuration."""
        if not self.connection_string:
            self.logger.error("No connection string provided for QueueOutputHandler")
            return False

        if not self.destination:
            self.logger.error("No queue name provided for QueueOutputHandler")
            return False

        # Validate queue name format (Azure Storage queue naming rules)
        if not self._is_valid_queue_name(self.destination):
            self.logger.error(f"Invalid queue name: {self.destination}")
            return False

        return True

    def _is_valid_queue_name(self, queue_name: str) -> bool:
        """
        Validate Azure Storage queue name according to naming rules.

        Rules:
        - Must be 3-63 characters long
        - Must contain only lowercase letters, numbers, and hyphens
        - Must start and end with a letter or number
        - Cannot contain consecutive hyphens
        """
        if not queue_name or len(queue_name) < 3 or len(queue_name) > 63:
            return False

        if not queue_name[0].isalnum() or not queue_name[-1].isalnum():
            return False

        if "--" in queue_name:
            return False

        return all(c.islower() or c.isdigit() or c == "-" for c in queue_name)

    def _get_queue_client(self) -> QueueClient:
        """Get or create queue client instance."""
        if self._queue_client is None:
            try:
                self._queue_client = QueueClient.from_connection_string(
                    conn_str=self.connection_string,
                    queue_name=self.destination,
                    connection_timeout=5,  # 5 second connection timeout
                    read_timeout=10,  # 10 second read timeout
                )
            except Exception as e:
                raise OutputHandlerError(
                    f"Failed to create QueueClient for {self.destination}",
                    error_code="QUEUE_CLIENT_CREATION_FAILED",
                    can_retry=False,
                    error_details={"connection_string_provided": bool(self.connection_string)},
                    original_exception=e,
                )

        return self._queue_client

    def _get_service_client(self) -> QueueServiceClient:
        """Get or create queue service client instance."""
        if self._service_client is None:
            try:
                self._service_client = QueueServiceClient.from_connection_string(
                    conn_str=self.connection_string,
                    connection_timeout=5,  # 5 second connection timeout
                    read_timeout=10,  # 10 second read timeout
                )
            except Exception as e:
                raise OutputHandlerError(
                    "Failed to create QueueServiceClient",
                    error_code="QUEUE_SERVICE_CLIENT_CREATION_FAILED",
                    can_retry=False,
                    error_details={"connection_string_provided": bool(self.connection_string)},
                    original_exception=e,
                )

        return self._service_client

    def _ensure_queue_exists(self) -> None:
        """Ensure the target queue exists, creating it if necessary."""
        queue_client = self._get_queue_client()

        # Always check if queue exists first
        try:
            queue_client.get_queue_properties()
            self.logger.debug(f"Queue {self.destination} already exists")
            return
        except ResourceNotFoundError:
            # Queue doesn't exist
            if not self.auto_create_queue:
                raise OutputHandlerError(
                    f"Queue {self.destination} does not exist and auto_create_queue is disabled",
                    error_code="QUEUE_NOT_FOUND",
                    can_retry=False,
                    error_details={
                        "queue_name": self.destination,
                        "auto_create_queue": self.auto_create_queue,
                    },
                )
            # Continue to create queue below

        # Create the queue if auto_create is enabled
        try:
            queue_client.create_queue()
            self.logger.info(f"Created queue: {self.destination}")

        except Exception as e:
            # If queue creation fails, it might already exist (race condition)
            # Try to verify it exists now
            try:
                queue_client.get_queue_properties()
                self.logger.debug(f"Queue {self.destination} exists after creation attempt")
                return
            except Exception as verify_error:
                raise OutputHandlerError(
                    f"Failed to create or verify queue {self.destination}",
                    error_code="QUEUE_CREATION_FAILED",
                    can_retry=True,
                    retry_after_seconds=2,  # Base delay for exponential backoff
                    error_details={
                        "original_error": str(e),
                        "verify_error": str(verify_error),
                        "auto_create_queue": self.auto_create_queue,
                    },
                    original_exception=e,
                )

    def _prepare_message_content(self, message: "Message", result: "ProcessingResult") -> str:
        """
        Prepare message content for queue delivery.

        Creates a simple message that Azure Functions can easily consume.
        Processing results are stored in Entity.processing_results, not in transport messages.
        """
        try:
            # Simple message structure - just transport the essential data
            queue_message = {
                "message_id": message.message_id,
                "correlation_id": message.correlation_id,
                "message_type": message.message_type.value,
                "entity_reference": {
                    "id": message.entity_reference.id,
                    "external_id": message.entity_reference.external_id,
                    "canonical_type": message.entity_reference.canonical_type,
                    "source": message.entity_reference.source,
                    "version": message.entity_reference.version,
                    "tenant_id": message.entity_reference.tenant_id,
                },
                "payload": message.payload,
                "metadata": message.metadata,
                "created_at": message.created_at.isoformat(),
                "retry_count": message.retry_count,
            }

            return json.dumps(queue_message, default=str)

        except Exception as e:
            raise OutputHandlerError(
                "Failed to serialize message for queue delivery",
                error_code="MESSAGE_SERIALIZATION_FAILED",
                can_retry=False,
                error_details={
                    "message_id": message.message_id,
                    "payload_type": type(message.payload).__name__,
                },
                original_exception=e,
            )

    def handle(self, message: "Message", result: "ProcessingResult") -> OutputHandlerResult:
        """
        Send processed message to Azure Storage Queue.

        Args:
            message: Original message that was processed
            result: Processing result containing output data

        Returns:
            OutputHandlerResult indicating success or failure
        """

        def _execute_queue_send():
            # Validate configuration
            if not self.validate_configuration():
                raise OutputHandlerError(
                    "Invalid queue handler configuration",
                    error_code="INVALID_CONFIGURATION",
                    can_retry=False,
                )

            # Ensure queue exists
            self._ensure_queue_exists()

            # Prepare message content
            message_content = self._prepare_message_content(message, result)

            # Send message to queue
            queue_client = self._get_queue_client()

            try:
                send_result = queue_client.send_message(
                    content=message_content,
                    time_to_live=self.message_ttl_seconds,
                    # NOTE: Do not set visibility_timeout when sending - this makes messages invisible!
                    # visibility_timeout is only for receiving/dequeuing messages, not sending them
                )

                self.logger.info(
                    f"Message sent to queue successfully",
                    extra={
                        "queue_name": self.destination,
                        "message_id": message.message_id,
                        "queue_message_id": send_result.id,
                        "correlation_id": message.correlation_id,
                        "content_length": len(message_content),
                    },
                )

                # Safely extract result metadata
                metadata = {
                    "queue_message_id": getattr(send_result, "id", None),
                    "content_length": len(message_content),
                    "queue_name": self.destination,
                }

                # Safely add optional fields
                if hasattr(send_result, "insertion_time") and send_result.insertion_time:
                    metadata["insertion_time"] = send_result.insertion_time.isoformat()

                if hasattr(send_result, "expiration_time") and send_result.expiration_time:
                    metadata["expiration_time"] = send_result.expiration_time.isoformat()

                return metadata

            except ServiceRequestError as e:
                # Azure service errors (network, authentication, etc.)
                raise OutputHandlerError(
                    f"Azure Storage service error when sending to queue {self.destination}",
                    error_code="AZURE_SERVICE_ERROR",
                    can_retry=True,
                    retry_after_seconds=5,  # Base delay for exponential backoff
                    error_details={
                        "error_code": getattr(e, "error_code", None),
                        "status_code": getattr(e, "status_code", None),
                        "message_id": message.message_id,
                    },
                    original_exception=e,
                )

            except Exception as e:
                # Other unexpected errors
                raise OutputHandlerError(
                    f"Unexpected error when sending to queue {self.destination}",
                    error_code="QUEUE_SEND_FAILED",
                    can_retry=True,
                    retry_after_seconds=2,  # Base delay for exponential backoff
                    error_details={
                        "message_id": message.message_id,
                        "error_type": type(e).__name__,
                    },
                    original_exception=e,
                )

        # Execute with timing
        try:
            send_metadata, duration_ms = self._execute_with_timing(_execute_queue_send)

            return self._create_success_result(
                execution_duration_ms=duration_ms, metadata=send_metadata
            )

        except OutputHandlerError as e:
            # Re-raise handler errors with timing
            _, duration_ms = self._execute_with_timing(lambda: None)

            # Use exponential backoff for retryable errors
            if e.can_retry:
                return self._create_failure_result_with_backoff(
                    execution_duration_ms=duration_ms,
                    error_message=e.message,
                    retry_count=message.retry_count,
                    error_code=e.error_code,
                    base_delay=e.retry_after_seconds,
                    error_details=e.error_details,
                )
            else:
                return self._create_failure_result(
                    execution_duration_ms=duration_ms,
                    error_message=e.message,
                    error_code=e.error_code,
                    can_retry=e.can_retry,
                    retry_after_seconds=e.retry_after_seconds,
                    error_details=e.error_details,
                )

        except Exception as e:
            # Handle unexpected exceptions
            _, duration_ms = self._execute_with_timing(lambda: None)

            self.logger.error(
                f"Unexpected error in QueueOutputHandler",
                extra={
                    "queue_name": self.destination,
                    "message_id": message.message_id,
                    "error": str(e),
                    "error_type": type(e).__name__,
                },
                exc_info=True,
            )

            return self._create_failure_result_with_backoff(
                execution_duration_ms=duration_ms,
                error_message=f"Unexpected error: {str(e)}",
                retry_count=message.retry_count,
                error_code="QUEUE_SEND_FAILED",
                base_delay=2,  # Base delay for exponential backoff
                error_details={"error_type": type(e).__name__, "message_id": message.message_id},
            )

    def get_handler_info(self) -> Dict[str, Any]:
        """Get queue handler information."""
        base_info = super().get_handler_info()
        base_info.update(
            {
                "queue_name": self.destination,
                "auto_create_queue": self.auto_create_queue,
                "message_ttl_seconds": self.message_ttl_seconds,
                "visibility_timeout_seconds": self.visibility_timeout_seconds,
                "connection_configured": bool(self.connection_string),
            }
        )
        return base_info
