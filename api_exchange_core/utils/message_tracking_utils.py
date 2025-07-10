"""
Message tracking utilities.

This module provides utilities for tracking queue message metrics and timing.
"""

from datetime import datetime, timezone

import azure.functions as func

from ..constants import QueueOperation
from ..schemas.metric_model import QueueMetric
from .logger import get_logger


def track_message_receive(
    msg: func.QueueMessage,
    queue_name: str = "",
) -> func.QueueMessage:
    """
    Track metrics for a received queue message and return the original message.

    Args:
        msg: The queue message being processed
        queue_name: Name of the queue the message was received from

    Returns:
        The original message object for further processing
    """
    logger = get_logger()

    # Get message metadata
    insertion_time = getattr(msg, "insertion_time", None)
    dequeue_count = getattr(msg, "dequeue_count", 0)

    # Create metrics list
    metrics = [QueueMetric.message_count(queue_name=queue_name, operation=QueueOperation.RECEIVE.value)]

    # Add dequeue count metric if available
    if dequeue_count:
        metrics.append(QueueMetric.dequeue_count(queue_name=queue_name, count=dequeue_count))

    # Add queue time metric if available
    if insertion_time:
        now = datetime.now(timezone.utc)
        queue_time_ms = int((now - insertion_time).total_seconds() * 1000)

        metrics.append(QueueMetric.queue_time(queue_name=queue_name, time_ms=queue_time_ms))

    # Log metrics for now (could be sent to metrics queue later)
    logger.debug(
        f"Message received from queue {queue_name}",
        extra={
            "queue_name": queue_name,
            "dequeue_count": dequeue_count,
            "queue_time_ms": queue_time_ms if insertion_time else None,
            "metrics_count": len(metrics),
        },
    )

    return msg


def calculate_queue_time(msg: func.QueueMessage) -> int:
    """
    Calculate how long a message has been in the queue.

    Args:
        msg: The queue message

    Returns:
        Queue time in milliseconds, or 0 if not available
    """
    insertion_time = getattr(msg, "insertion_time", None)
    if not insertion_time:
        return 0

    now = datetime.now(timezone.utc)
    return int((now - insertion_time).total_seconds() * 1000)


def get_message_metadata(msg: func.QueueMessage) -> dict:
    """
    Extract metadata from a queue message.

    Args:
        msg: The queue message

    Returns:
        Dictionary with message metadata
    """
    return {
        "message_id": getattr(msg, "id", None),
        "insertion_time": getattr(msg, "insertion_time", None),
        "expiration_time": getattr(msg, "expiration_time", None),
        "dequeue_count": getattr(msg, "dequeue_count", 0),
        "next_visible_time": getattr(msg, "next_visible_time", None),
        "pop_receipt": getattr(msg, "pop_receipt", None),
        "queue_time_ms": calculate_queue_time(msg),
    }
