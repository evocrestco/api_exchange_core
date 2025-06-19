"""
Azure Service Bus output handler.

Provides output handling for Azure Service Bus queues and topics, supporting
both development and production Service Bus namespaces.
"""

import json
from typing import TYPE_CHECKING, Any, Dict, Optional, Union

try:
    from azure.core.exceptions import ServiceRequestError
    from azure.servicebus import ServiceBusClient, ServiceBusMessage

    SERVICEBUS_AVAILABLE = True
except ImportError:
    SERVICEBUS_AVAILABLE = False
    ServiceRequestError = Exception  # Fallback for type hints
    ServiceBusClient = None
    ServiceBusMessage = None

from .base import OutputHandler, OutputHandlerError, OutputHandlerResult
from .config import OutputHandlerConfigFactory, ServiceBusOutputHandlerConfig

if TYPE_CHECKING:
    from ...processing_result import ProcessingResult
    from ..message import Message


class ServiceBusOutputHandler(OutputHandler):
    """
    Output handler for Azure Service Bus queues and topics.

    Sends processed messages to Azure Service Bus for enterprise messaging
    scenarios requiring features like transactions, duplicate detection,
    dead letter queues, and message ordering.

    Configuration:
        - connection_string: Service Bus connection string
        - destination_type: "queue" or "topic" (default: "queue")
        - session_id: For session-aware messaging (optional)
        - time_to_live_seconds: Message TTL in seconds (default: None = no expiration)
        - scheduled_enqueue_time: ISO format datetime for scheduled delivery (optional)
        - message_properties: Additional message properties dict (optional)

    Example:
        handler = ServiceBusOutputHandler(
            destination="processing-queue",
            config={
                "connection_string": "Endpoint=sb://...",
                "destination_type": "queue",
                "time_to_live_seconds": 3600,  # 1 hour
                "message_properties": {"priority": "high"}
            }
        )
    """

    def __init__(
        self,
        destination: str,
        config: Optional[Union[Dict[str, Any], ServiceBusOutputHandlerConfig]] = None,
    ):
        """
        Initialize the Azure Service Bus output handler.

        Args:
            destination: Name of the target queue or topic
            config: Configuration dictionary, ServiceBusOutputHandlerConfig object, or None
        """
        if not SERVICEBUS_AVAILABLE:
            raise ImportError(
                "Azure Service Bus SDK is not installed. "
                "Install it with: pip install azure-servicebus"
            )

        # Handle configuration
        if isinstance(config, ServiceBusOutputHandlerConfig):
            # Use the config object directly
            self._handler_config = config
            # Update destination if different
            if self._handler_config.destination != destination:
                self._handler_config.destination = destination
            # Convert to dict for parent class
            config_dict = self._handler_config.to_dict()
        else:
            # Handle backward compatibility for field name changes
            if config:
                config = dict(config)  # Create a copy
                # Map time_to_live_seconds to message_time_to_live
                if "time_to_live_seconds" in config and "message_time_to_live" not in config:
                    config["message_time_to_live"] = config.pop("time_to_live_seconds")
                # Map scheduled_enqueue_time to scheduled_enqueue_time_utc
                if (
                    "scheduled_enqueue_time" in config
                    and "scheduled_enqueue_time_utc" not in config
                ):
                    config["scheduled_enqueue_time_utc"] = config.pop("scheduled_enqueue_time")

            # Create config object from dict or defaults
            self._handler_config = OutputHandlerConfigFactory.create_config(
                "service_bus", destination, config
            )
            config_dict = config if config else {}

        # Initialize parent with dict config for backward compatibility
        super().__init__(destination, config_dict)

        # Extract configuration values
        self.connection_string = self._handler_config.connection_string
        self.destination_type = self.config.get("destination_type", "queue").lower()
        self.session_id = self._handler_config.session_id
        self.time_to_live_seconds = self._handler_config.message_time_to_live
        self.scheduled_enqueue_time = self._handler_config.scheduled_enqueue_time_utc
        self.message_properties = self.config.get("message_properties", {})

        # Initialize client (lazy initialization)
        self._service_bus_client: Optional[ServiceBusClient] = None

    def validate_configuration(self) -> bool:
        """Validate Service Bus handler configuration."""
        if not self.connection_string:
            self.logger.error("No connection string provided for ServiceBusOutputHandler")
            return False

        if not self.destination:
            self.logger.error("No queue/topic name provided for ServiceBusOutputHandler")
            return False

        if self.destination_type not in ["queue", "topic"]:
            self.logger.error(
                f"Invalid destination_type: {self.destination_type}. Must be 'queue' or 'topic'"
            )
            return False

        # Validate Service Bus entity name format
        if not self._is_valid_entity_name(self.destination):
            self.logger.error(f"Invalid Service Bus entity name: {self.destination}")
            return False

        return True

    def _is_valid_entity_name(self, entity_name: str) -> bool:
        """
        Validate Azure Service Bus entity name according to naming rules.

        Rules:
        - Must be 1-260 characters long
        - Can contain letters, numbers, periods, hyphens, underscores, and forward slashes
        - Cannot start or end with a slash
        - Cannot contain consecutive slashes
        """
        if not entity_name or len(entity_name) < 1 or len(entity_name) > 260:
            return False

        if entity_name.startswith("/") or entity_name.endswith("/"):
            return False

        if "//" in entity_name:
            return False

        # Check allowed characters
        allowed_chars = set("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789.-_/")
        return all(c in allowed_chars for c in entity_name)

    def _get_service_bus_client(self) -> ServiceBusClient:
        """Get or create Service Bus client instance."""
        if self._service_bus_client is None:
            try:
                self._service_bus_client = ServiceBusClient.from_connection_string(
                    conn_str=self.connection_string
                )
            except Exception as e:
                raise OutputHandlerError(
                    "Failed to create ServiceBusClient",
                    error_code="SERVICE_BUS_CLIENT_CREATION_FAILED",
                    can_retry=False,
                    error_details={"connection_string_provided": bool(self.connection_string)},
                    original_exception=e,
                )

        return self._service_bus_client

    def _prepare_service_bus_message(
        self, message: "Message", result: "ProcessingResult"
    ) -> ServiceBusMessage:
        """
        Prepare Service Bus message from processing result.

        Creates a ServiceBusMessage with the processed content and appropriate
        metadata for downstream Service Bus consumers.
        """
        try:
            # Prepare message body
            message_body = {
                "message_id": message.message_id,
                "correlation_id": message.correlation_id,
                "created_at": message.created_at.isoformat(),
                "message_type": message.message_type.value,
                "retry_count": message.retry_count,
                "entity_reference": {
                    "id": message.entity_reference.id,
                    "external_id": message.entity_reference.external_id,
                    "canonical_type": message.entity_reference.canonical_type,
                    "source": message.entity_reference.source,
                    "version": message.entity_reference.version,
                    "tenant_id": message.entity_reference.tenant_id,
                },
                "payload": message.payload,
                "processing_result": {
                    "status": result.status.value,
                    "success": result.success,
                    "entities_created": result.entities_created,
                    "entities_updated": result.entities_updated,
                    "processing_metadata": result.processing_metadata,
                    "processor_info": result.processor_info,
                    "processing_duration_ms": result.processing_duration_ms,
                    "completed_at": result.completed_at.isoformat(),
                },
                "routing_metadata": {
                    "source_handler": self._handler_name,
                    "target_destination": self.destination,
                    "destination_type": self.destination_type,
                    "sent_at": message.created_at.isoformat(),
                },
            }

            # Create Service Bus message
            sb_message = ServiceBusMessage(
                body=json.dumps(message_body, default=str), content_type="application/json"
            )

            # Set message ID and correlation ID
            sb_message.message_id = message.message_id
            sb_message.correlation_id = message.correlation_id

            # Set session ID if configured
            if self.session_id:
                sb_message.session_id = self.session_id

            # Set time to live if configured
            if self.time_to_live_seconds:
                sb_message.time_to_live = self.time_to_live_seconds

            # Set scheduled enqueue time if configured
            if self.scheduled_enqueue_time:
                from datetime import datetime

                sb_message.scheduled_enqueue_time = datetime.fromisoformat(
                    self.scheduled_enqueue_time
                )

            # Add custom application properties
            if self.message_properties:
                for key, value in self.message_properties.items():
                    sb_message.application_properties[key] = value

            # Add framework-specific properties
            sb_message.application_properties.update(
                {
                    "source_processor": result.processor_info.get("name", "unknown"),
                    "entity_external_id": message.entity_reference.external_id,
                    "entity_canonical_type": message.entity_reference.canonical_type,
                    "processing_status": result.status.value,
                    "tenant_id": message.entity_reference.tenant_id,
                }
            )

            return sb_message

        except Exception as e:
            raise OutputHandlerError(
                "Failed to prepare Service Bus message",
                error_code="MESSAGE_PREPARATION_FAILED",
                can_retry=False,
                error_details={
                    "message_id": message.message_id,
                    "payload_type": type(message.payload).__name__,
                },
                original_exception=e,
            )

    def handle(self, message: "Message", result: "ProcessingResult") -> OutputHandlerResult:
        """
        Send processed message to Azure Service Bus.

        Args:
            message: Original message that was processed
            result: Processing result containing output data

        Returns:
            OutputHandlerResult indicating success or failure
        """

        def _execute_service_bus_send():
            # Validate configuration
            if not self.validate_configuration():
                raise OutputHandlerError(
                    "Invalid Service Bus handler configuration",
                    error_code="INVALID_CONFIGURATION",
                    can_retry=False,
                )

            # Prepare Service Bus message
            sb_message = self._prepare_service_bus_message(message, result)

            # Get Service Bus client and sender
            client = self._get_service_bus_client()

            try:
                if self.destination_type == "queue":
                    with client.get_queue_sender(queue_name=self.destination) as sender:
                        sender.send_messages(sb_message)
                elif self.destination_type == "topic":
                    with client.get_topic_sender(topic_name=self.destination) as sender:
                        sender.send_messages(sb_message)
                else:
                    raise OutputHandlerError(
                        f"Unsupported destination type: {self.destination_type}",
                        error_code="UNSUPPORTED_DESTINATION_TYPE",
                        can_retry=False,
                    )

                self.logger.info(
                    f"Message sent to Service Bus {self.destination_type} successfully",
                    extra={
                        "destination": self.destination,
                        "destination_type": self.destination_type,
                        "message_id": message.message_id,
                        "sb_message_id": sb_message.message_id,
                        "correlation_id": message.correlation_id,
                        "session_id": self.session_id,
                        "content_length": len(sb_message.body),
                    },
                )

                return {
                    "service_bus_message_id": sb_message.message_id,
                    "destination": self.destination,
                    "destination_type": self.destination_type,
                    "session_id": self.session_id,
                    "content_length": len(sb_message.body),
                    "time_to_live_seconds": self.time_to_live_seconds,
                    "application_properties_count": len(sb_message.application_properties),
                }

            except ServiceRequestError as e:
                # Service Bus service errors (network, authentication, etc.)
                raise OutputHandlerError(
                    f"Service Bus service error when sending to {self.destination_type} {self.destination}",
                    error_code="SERVICE_BUS_SERVICE_ERROR",
                    can_retry=True,
                    retry_after_seconds=5,  # Base delay for exponential backoff
                    error_details={
                        "error_code": getattr(e, "error_code", None),
                        "status_code": getattr(e, "status_code", None),
                        "message_id": message.message_id,
                        "destination_type": self.destination_type,
                    },
                    original_exception=e,
                )

            except Exception as e:
                # Other unexpected errors
                raise OutputHandlerError(
                    f"Unexpected error when sending to {self.destination_type} {self.destination}",
                    error_code="SERVICE_BUS_SEND_FAILED",
                    can_retry=True,
                    retry_after_seconds=2,  # Base delay for exponential backoff
                    error_details={
                        "message_id": message.message_id,
                        "destination_type": self.destination_type,
                        "error_type": type(e).__name__,
                    },
                    original_exception=e,
                )

        # Execute with timing
        try:
            send_metadata, duration_ms = self._execute_with_timing(_execute_service_bus_send)

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
                "Unexpected error in ServiceBusOutputHandler",
                extra={
                    "destination": self.destination,
                    "destination_type": self.destination_type,
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
                error_code="UNEXPECTED_ERROR",
                base_delay=2,  # Base delay for exponential backoff
                error_details={"error_type": type(e).__name__, "message_id": message.message_id},
            )

    def get_handler_info(self) -> Dict[str, Any]:
        """Get Service Bus handler information."""
        base_info = super().get_handler_info()
        base_info.update(
            {
                "destination": self.destination,
                "destination_type": self.destination_type,
                "session_id": self.session_id,
                "time_to_live_seconds": self.time_to_live_seconds,
                "scheduled_enqueue_time": self.scheduled_enqueue_time,
                "message_properties_count": len(self.message_properties),
                "connection_configured": bool(self.connection_string),
            }
        )
        return base_info

    def __del__(self):
        """Clean up Service Bus client on destruction."""
        if hasattr(self, "_service_bus_client") and self._service_bus_client:
            try:
                self._service_bus_client.close()
            except Exception:
                pass  # Ignore cleanup errors
