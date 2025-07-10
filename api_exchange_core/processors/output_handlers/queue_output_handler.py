"""
Queue output handler for routing messages to Azure Storage Queues.

This handler takes processing results and sends output messages
to configured Azure Storage Queues.
"""

from typing import Any, Dict, Optional

from .base_output_handler import BaseOutputHandler
from ...utils.logger import get_logger
from ...utils.queue_utils import send_message_to_queue_direct
from ..message import Message
from ..processing_result import ProcessingResult


class QueueOutputHandler(BaseOutputHandler):
    """
    Output handler that routes messages to Azure Storage Queues.

    This handler takes output messages from processing results
    and sends them to the configured output queues.
    """

    def __init__(
        self,
        queue_mappings: Dict[str, str],
        connection_string: str,
        default_queue: Optional[str] = None,
    ):
        """
        Initialize the queue output handler.

        Args:
            queue_mappings: Map of output_name -> queue_name
            connection_string: Azure Storage connection string
            default_queue: Default queue name if no specific mapping found
        """
        self.queue_mappings = queue_mappings
        self.connection_string = connection_string
        self.default_queue = default_queue
        self.logger = get_logger()

    def handle_output(
        self, result: ProcessingResult, source_message: Message, context: Dict[str, Any]
    ) -> None:
        """
        Handle output messages by sending them to queues.

        Args:
            result: Processing result with output messages
            source_message: Original message that was processed
            context: Processing context
        """
        if not result.output_messages:
            return

        log_context = {
            "pipeline_id": source_message.pipeline_id,
            "source_message_id": source_message.message_id,
            "tenant_id": source_message.tenant_id,
            "output_messages_count": len(result.output_messages),
        }

        self.logger.info(
            f"Routing {len(result.output_messages)} output messages", extra=log_context
        )

        for output_message in result.output_messages:
            try:
                self._send_message_to_queue(output_message, context)
            except Exception as e:
                self.logger.error(
                    f"Failed to send message to queue: {str(e)}",
                    extra={
                        **log_context,
                        "output_message_id": output_message.message_id,
                        "error_message": str(e),
                    },
                    exc_info=True,
                )
                # Don't fail the entire operation for one message
                continue

    def _send_message_to_queue(self, message: Message, context: Dict[str, Any]) -> None:
        """
        Send a single message to the appropriate queue.

        Args:
            message: Message to send
            context: Processing context
        """
        # Determine target queue
        queue_name = self._get_queue_name(message, context)

        if not queue_name:
            self.logger.warning(
                f"No queue mapping found for message: {message.message_id}",
                extra={
                    "message_id": message.message_id,
                    "pipeline_id": message.pipeline_id,
                    "available_queues": list(self.queue_mappings.keys()),
                },
            )
            return

        # Send message to queue
        try:
            send_message_to_queue_direct(
                connection_string=self.connection_string,
                queue_name=queue_name,
                message_data=message.model_dump(),
            )

            self.logger.debug(
                f"Message sent to queue: {queue_name}",
                extra={
                    "message_id": message.message_id,
                    "pipeline_id": message.pipeline_id,
                    "queue_name": queue_name,
                    "tenant_id": message.tenant_id,
                },
            )

        except Exception as e:
            self.logger.error(
                f"Failed to send message to queue {queue_name}: {str(e)}",
                extra={
                    "message_id": message.message_id,
                    "pipeline_id": message.pipeline_id,
                    "queue_name": queue_name,
                    "tenant_id": message.tenant_id,
                    "error_message": str(e),
                },
                exc_info=True,
            )
            raise

    def _get_queue_name(self, message: Message, context: Dict[str, Any]) -> Optional[str]:
        """
        Determine the target queue for a message.

        Args:
            message: Message to route
            context: Processing context

        Returns:
            Queue name or None if no mapping found
        """
        # Check if message has specific routing info
        output_name = message.get_context("output_name")
        if output_name and output_name in self.queue_mappings:
            return self.queue_mappings[output_name]

        # Check if context has routing info
        output_name = context.get("output_name")
        if output_name and output_name in self.queue_mappings:
            return self.queue_mappings[output_name]

        # Use default queue if available
        if self.default_queue:
            return self.default_queue

        # No queue mapping found
        return None

    def get_handler_name(self) -> str:
        """
        Get the name of this output handler.

        Returns:
            Handler name for logging
        """
        return "QueueOutputHandler"
