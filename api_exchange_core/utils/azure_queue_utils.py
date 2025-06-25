import os
from datetime import datetime
from typing import Any, Dict, List, Optional

import azure.functions as func
from azure.storage.queue import QueueClient

from ..constants import EnvironmentVariable, QueueName, QueueOperation
from ..schemas.metric_model import Metric, QueueMetric
from .json_utils import dumps
from .logger import get_logger


def process_metrics(
    metrics: List[Metric],
    queue_name: Optional[str] = None,
    connection_string: Optional[str] = None,
) -> None:
    """
    Send a list of metrics to an Azure Storage Queue using the SDK.

    Args:
        metrics: List of metrics to process
        queue_name: Name of the Azure Storage Queue (defaults to QueueName.METRICS)
        connection_string: Azure Storage connection string
    """
    queue_name = queue_name or QueueName.METRICS.value
    log = get_logger()
    connection_string = connection_string or os.getenv(
        EnvironmentVariable.AZURE_STORAGE_CONNECTION.value
    )
    log.debug(f"Connection string: {connection_string}")
    log.debug(f"Queue name: {queue_name}")

    if not metrics:
        log.info("No metrics to process")
        return

    if not connection_string:
        log.error("No Azure Storage connection string available")
        # Fallback to logging metrics
        for metric in metrics:
            log.warning(f"METRIC: {metric.metric_name}, value={metric.value}")
        return

    try:
        # Create queue client
        queue_client = QueueClient.from_connection_string(
            conn_str=connection_string, queue_name=queue_name
        )

        log.debug(f"Sending {len(metrics)} metrics to queue {queue_name}")

        for idx, metric in enumerate(metrics):
            try:
                json_metric = metric.model_dump_json()
                queue_client.send_message(json_metric)
                log.debug(f"Metric {idx + 1} sent to queue: {json_metric}")
            except Exception as e:
                # Check if queue doesn't exist and try to create it
                if "QueueNotFound" in str(e) or "does not exist" in str(e):
                    try:
                        log.debug(f"Queue {queue_name} not found, creating it...")
                        queue_client.create_queue()
                        # Retry sending the message
                        queue_client.send_message(json_metric)
                        log.debug(f"Metric {idx + 1} sent to queue after creation: {json_metric}")
                    except Exception as create_error:
                        log.error(
                            f"Failed to create queue or send metric {idx + 1}: {str(create_error)}"
                        )
                else:
                    log.error(f"Failed to send metric {idx + 1}: {str(e)}")

        log.debug(f"Processed {len(metrics)} metrics")

    except Exception as e:
        log.error(f"Failed to initialize queue client: {str(e)}")
        # Log metrics locally as fallback
        for metric in metrics:
            log_data = f"METRIC: {metric.type}, name={metric.metric_name}, value={metric.value}"
            for key, value in metric.labels.items():
                log_data += f", {key}={value}"
            log.warning(log_data)


def send_queue_message(
    output_binding: func.Out[str],
    message: Dict[str, Any],
    queue_name: str = "",
) -> None:
    """
    Send a message to a queue and record the metric in a metrics queue.

    Args:
        output_binding: Azure Functions output binding for the target queue
        message: Message to send (will be JSON serialized)
        queue_name: Name of the queue being sent to
    """
    # Send the message using the binding
    output_binding.set(dumps(message))

    # Create metrics (currently unused but kept for future implementation)
    # metrics = [QueueMetric.message_count(
    #     queue_name=queue_name, operation=QueueOperation.SEND.value
    # )]

    # Process the metrics TODO: not sure this is still needed as can kind of be done with @operation
    # process_metrics(metrics, queue_name=metrics_queue_name, logger=log)


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
    # Get message metadata
    insertion_time = getattr(msg, "insertion_time", None)
    dequeue_count = getattr(msg, "dequeue_count", 0)

    # Create metrics list
    metrics = [
        QueueMetric.message_count(queue_name=queue_name, operation=QueueOperation.RECEIVE.value)
    ]

    # Add dequeue count metric if available
    if dequeue_count:
        metrics.append(QueueMetric.dequeue_count(queue_name=queue_name, count=dequeue_count))

    # Add queue time metric if available
    if insertion_time:
        now = datetime.now(insertion_time.tzinfo) if insertion_time.tzinfo else datetime.now()
        queue_time_ms = int((now - insertion_time).total_seconds() * 1000)

        metrics.append(QueueMetric.queue_time(queue_name=queue_name, time_ms=queue_time_ms))

    # TODO: same as above, not sure it is needed/useful?
    # process_metrics(metrics, queue_name=metrics_queue_name, logger=log)
    return msg
