"""
Metrics processing utilities.

This module handles sending metrics to Azure Storage Queues for monitoring.
"""

import os
from typing import List, Optional

from azure.storage.queue import QueueClient

from ..constants import EnvironmentVariable, QueueName
from ..schemas.metric_model import Metric
from .logger import get_logger


def send_metrics_to_queue(
    metrics: List[Metric],
    queue_name: Optional[str] = None,
    connection_string: Optional[str] = None,
) -> None:
    """
    Send a list of metrics to an Azure Storage Queue.

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


# Alias for backward compatibility
process_metrics = send_metrics_to_queue
