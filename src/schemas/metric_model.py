from datetime import datetime
from typing import Any, Dict

from pydantic import BaseModel, ConfigDict, Field
from pydantic.json import pydantic_encoder


class Metric(BaseModel):
    """Base model for any metric sent to the metrics queue."""

    type: str = "metric"
    metric_name: str
    timestamp: datetime = Field(default_factory=datetime.now)
    value: int | float
    labels: Dict[str, Any] = Field(default_factory=dict)

    model_config = ConfigDict(
        # Empty config as we're using a custom serializer instead of json_encoders
    )

    # Custom serializer to replace json_encoders
    @classmethod
    def model_serializer(cls, obj, _):
        if isinstance(obj, datetime):
            return obj.isoformat()
        # Fall back to default pydantic encoder for other types
        return pydantic_encoder(obj)


class QueueMetric(Metric):
    """Specialized metric for queue operations."""

    type: str = "queue_metric"

    @classmethod
    def message_count(cls, queue_name: str, operation: str, count: int = 1) -> "QueueMetric":
        return cls(
            metric_name="azure_queue_message_count",
            value=count,
            labels={"queue_name": queue_name, "operation": operation},
        )

    @classmethod
    def dequeue_count(cls, queue_name: str, count: int) -> "QueueMetric":
        return cls(
            metric_name="azure_queue_dequeue_count",
            value=count,
            labels={"queue_name": queue_name, "operation": "receive"},
        )

    @classmethod
    def queue_time(cls, queue_name: str, time_ms: int) -> "QueueMetric":
        return cls(
            metric_name="azure_queue_time_ms",
            value=time_ms,
            labels={"queue_name": queue_name, "operation": "receive"},
        )


class FileMetric(Metric):
    """Specialized metric for file operations."""

    type: str = "file_metric"

    @classmethod
    def bytes_written(cls, path: str, bytes_count: int) -> "FileMetric":
        return cls(
            metric_name="file_bytes_written",
            value=bytes_count,
            labels={"path": path, "operation": "write"},
        )

    @classmethod
    def processing_time(cls, path: str, time_ms: int) -> "FileMetric":
        return cls(metric_name="file_processing_time_ms", value=time_ms, labels={"path": path})


class OperationMetric(Metric):
    """Specialized metric for operation performance."""

    type: str = "operation_metric"

    @classmethod
    def duration(
        cls,
        operation: str,
        module: str,
        function: str,
        tenant_id: str,
        status: str,
        duration_ms: float,
    ) -> "OperationMetric":
        return cls(
            metric_name="operation_duration_ms",
            value=duration_ms,
            labels={
                "operation": operation,
                "module": module,
                "function": function,
                "tenant_id": tenant_id,
                "status": status,
            },
        )
