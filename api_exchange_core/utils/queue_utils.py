"""
Azure Storage Queue utilities.

This module provides utilities for sending messages to Azure Storage Queues
via both Azure Functions output bindings and direct SDK calls.
"""

import json
from typing import Any, Dict, Optional

import azure.functions as func
from azure.storage.queue import QueueClient
from pydantic_core import to_jsonable_python

from .logger import get_logger


def _send_to_binding_core(output_binding: func.Out[Any], data: Any, binding_name: str = "", logger: Optional[Any] = None) -> None:
    """
    Core binding send logic with error handling and logging.

    Args:
        output_binding: Azure Functions output binding
        data: Pre-formatted data to send
        binding_name: Name/description for logging
        logger: Optional logger instance
    """
    logger = logger or get_logger()

    try:
        logger.debug(f"Sending data to binding: {binding_name}")
        output_binding.set(data)
        logger.debug(f"Successfully sent to binding: {binding_name}")
    except Exception as e:
        logger.error(f"Failed to send to binding {binding_name}: {str(e)}")
        raise


def send_message_to_queue_binding(output_binding: func.Out[str], message: Dict[str, Any], queue_name: str = "") -> None:
    """
    Send a message to a queue using Azure Functions output binding.

    Args:
        output_binding: Azure Functions output binding for the target queue
        message: Message to send (will be JSON serialized)
        queue_name: Name of the queue being sent to (for logging)
    """
    logger = get_logger()

    try:
        json_data = json.dumps(to_jsonable_python(message))
    except Exception as e:
        logger.error(f"Failed to serialize message for queue {queue_name}: {str(e)}")
        raise

    _send_to_binding_core(output_binding, json_data, f"queue:{queue_name}", logger)


def send_message_to_queue_direct(connection_string: str, queue_name: str, message_data: Dict[str, Any]) -> None:
    """
    Send a message directly to Azure Storage Queue using SDK.

    Args:
        connection_string: Azure Storage connection string
        queue_name: Name of the target queue
        message_data: Message data to send (will be JSON serialized)
    """
    logger = get_logger()

    try:
        # Serialize message data
        json_data = json.dumps(to_jsonable_python(message_data))
    except Exception as e:
        logger.error(f"Failed to serialize message for queue {queue_name}: {str(e)}")
        raise

    try:
        # Create queue client and send message
        queue_client = QueueClient.from_connection_string(conn_str=connection_string, queue_name=queue_name)

        logger.debug(f"Sending message to queue: {queue_name}")
        queue_client.send_message(json_data)
        logger.debug(f"Successfully sent message to queue: {queue_name}")

    except Exception as e:
        # Check if queue doesn't exist and try to create it
        if "QueueNotFound" in str(e) or "does not exist" in str(e):
            try:
                logger.debug(f"Queue {queue_name} not found, creating it...")
                queue_client.create_queue()
                # Retry sending the message
                queue_client.send_message(json_data)
                logger.debug(f"Message sent to queue after creation: {queue_name}")
            except Exception as create_error:
                logger.error(f"Failed to create queue or send message to {queue_name}: {str(create_error)}")
                raise
        else:
            logger.error(f"Failed to send message to queue {queue_name}: {str(e)}")
            raise
