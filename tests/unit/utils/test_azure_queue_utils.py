"""Tests for Azure Queue utilities."""

from datetime import UTC, datetime, timedelta
from unittest.mock import MagicMock, patch

import azure.functions as func

from api_exchange_core.constants import QueueName
from api_exchange_core.schemas.metric_model import Metric
from api_exchange_core.utils.azure_queue_utils import process_metrics, send_queue_message, track_message_receive


class TestProcessMetrics:
    """Test process_metrics function."""

    @patch("api_exchange_core.utils.azure_queue_utils.QueueClient")
    def test_process_metrics_success(self, mock_queue_client_class):
        """Test successfully sending metrics to queue."""
        # Setup mock
        mock_queue_client = MagicMock()
        mock_queue_client_class.from_connection_string.return_value = mock_queue_client

        # Create test metrics
        metrics = [
            Metric(
                type="custom_metric", metric_name="test_metric", value=1.0, labels={"env": "test"}
            ),
            Metric(
                type="counter",
                metric_name="request_count",
                value=10.0,
                labels={"endpoint": "/api/test"},
            ),
        ]

        # Call function
        with patch.dict("os.environ", {"AzureWebJobsStorage": "test_connection_string"}):
            process_metrics(metrics)

        # Verify
        mock_queue_client_class.from_connection_string.assert_called_once_with(
            conn_str="test_connection_string", queue_name=QueueName.METRICS.value
        )
        assert mock_queue_client.send_message.call_count == 2

    @patch("api_exchange_core.utils.azure_queue_utils.QueueClient")
    def test_process_metrics_custom_queue(self, mock_queue_client_class):
        """Test sending metrics to custom queue."""
        mock_queue_client = MagicMock()
        mock_queue_client_class.from_connection_string.return_value = mock_queue_client

        metrics = [Metric(type="test", metric_name="test", value=1.0)]

        process_metrics(metrics, queue_name="custom-queue", connection_string="custom_connection")

        mock_queue_client_class.from_connection_string.assert_called_once_with(
            conn_str="custom_connection", queue_name="custom-queue"
        )

    def test_process_metrics_empty_list(self):
        """Test with empty metrics list."""
        # Should return early without errors
        process_metrics([])

    @patch("api_exchange_core.utils.azure_queue_utils.QueueClient")
    @patch("api_exchange_core.utils.azure_queue_utils.get_logger")
    def test_process_metrics_send_failure(self, mock_get_logger, mock_queue_client_class):
        """Test handling of send failures."""
        # Setup mock logger
        mock_logger = MagicMock()
        mock_get_logger.return_value = mock_logger

        # Setup queue client to fail
        mock_queue_client = MagicMock()
        mock_queue_client.send_message.side_effect = Exception("Send failed")
        mock_queue_client_class.from_connection_string.return_value = mock_queue_client

        metrics = [Metric(type="test", metric_name="test", value=1.0)]

        with patch.dict("os.environ", {"AzureWebJobsStorage": "test_connection"}):
            process_metrics(metrics)

        # Should log the error
        mock_logger.error.assert_called()

    @patch("api_exchange_core.utils.azure_queue_utils.QueueClient")
    @patch("api_exchange_core.utils.azure_queue_utils.get_logger")
    def test_process_metrics_queue_init_failure(self, mock_get_logger, mock_queue_client_class):
        """Test handling of queue initialization failures."""
        # Setup mock logger
        mock_logger = MagicMock()
        mock_get_logger.return_value = mock_logger

        # Setup queue client creation to fail
        mock_queue_client_class.from_connection_string.side_effect = Exception("Connection failed")

        metrics = [
            Metric(type="test", metric_name="test_metric", value=1.0, labels={"key": "value"})
        ]

        with patch.dict("os.environ", {"AzureWebJobsStorage": "test_connection"}):
            process_metrics(metrics)

        # Should log the initialization error
        assert any(
            "Failed to initialize queue client" in str(call)
            for call in mock_logger.error.call_args_list
        )
        # Should fallback to logging metrics
        mock_logger.warning.assert_called()
        assert "METRIC: test" in str(mock_logger.warning.call_args_list[0])

    @patch("api_exchange_core.utils.azure_queue_utils.QueueClient")
    @patch("api_exchange_core.utils.azure_queue_utils.get_logger")
    def test_process_metrics_queue_not_found_creates_queue(self, mock_get_logger, mock_queue_client_class):
        """Test that queue is created when QueueNotFound error occurs."""
        # Setup mock logger
        mock_logger = MagicMock()
        mock_get_logger.return_value = mock_logger

        # Setup queue client
        mock_queue_client = MagicMock()
        # First send_message fails with QueueNotFound, second succeeds
        mock_queue_client.send_message.side_effect = [
            Exception("The specified queue does not exist."),
            None  # Success on retry
        ]
        mock_queue_client_class.from_connection_string.return_value = mock_queue_client

        metrics = [Metric(type="test", metric_name="test", value=1.0)]

        with patch.dict("os.environ", {"AzureWebJobsStorage": "test_connection"}):
            process_metrics(metrics)

        # Verify queue was created
        mock_queue_client.create_queue.assert_called_once()
        # Verify message was sent twice (first failed, second succeeded)
        assert mock_queue_client.send_message.call_count == 2

    @patch("api_exchange_core.utils.azure_queue_utils.QueueClient")
    @patch("api_exchange_core.utils.azure_queue_utils.get_logger")
    def test_process_metrics_queue_creation_fails(self, mock_get_logger, mock_queue_client_class):
        """Test handling when queue creation fails."""
        # Setup mock logger
        mock_logger = MagicMock()
        mock_get_logger.return_value = mock_logger

        # Setup queue client
        mock_queue_client = MagicMock()
        # send_message fails with QueueNotFound
        mock_queue_client.send_message.side_effect = Exception("QueueNotFound")
        # create_queue also fails
        mock_queue_client.create_queue.side_effect = Exception("Cannot create queue")
        mock_queue_client_class.from_connection_string.return_value = mock_queue_client

        metrics = [Metric(type="test", metric_name="test", value=1.0)]

        with patch.dict("os.environ", {"AzureWebJobsStorage": "test_connection"}):
            process_metrics(metrics)

        # Verify queue creation was attempted
        mock_queue_client.create_queue.assert_called_once()
        # Verify error was logged
        assert any(
            "Failed to create queue or send metric" in str(call)
            for call in mock_logger.error.call_args_list
        )


class TestSendQueueMessage:
    """Test send_queue_message function."""

    def test_send_queue_message_success(self):
        """Test successfully sending a message."""
        # Create mock output binding
        mock_output_binding = MagicMock()

        message = {"id": "123", "data": "test data"}

        send_queue_message(
            output_binding=mock_output_binding, message=message, queue_name="test-queue"
        )

        # Verify message was sent
        mock_output_binding.set.assert_called_once()
        # Check that JSON was properly serialized
        call_args = mock_output_binding.set.call_args[0][0]
        assert '"id": "123"' in call_args
        assert '"data": "test data"' in call_args


class TestTrackMessageReceive:
    """Test track_message_receive function."""

    def test_track_message_receive_basic(self):
        """Test basic message tracking."""
        # Create mock message
        mock_message = MagicMock(spec=func.QueueMessage)
        mock_message.insertion_time = None
        mock_message.dequeue_count = 0

        result = track_message_receive(msg=mock_message, queue_name="test-queue")

        # Should return the original message
        assert result is mock_message

    def test_track_message_receive_with_metadata(self):
        """Test tracking with dequeue count and insertion time."""
        # Create mock message with metadata
        mock_message = MagicMock(spec=func.QueueMessage)
        mock_message.dequeue_count = 3
        mock_message.insertion_time = datetime.now(UTC) - timedelta(seconds=30)

        result = track_message_receive(msg=mock_message, queue_name="test-queue")

        # Should return the original message
        assert result is mock_message

    def test_track_message_receive_no_dequeue_count(self):
        """Test tracking when dequeue_count attribute is missing."""
        # Create mock message without dequeue_count
        mock_message = MagicMock(spec=func.QueueMessage)
        delattr(mock_message, "dequeue_count")
        mock_message.insertion_time = datetime.now(UTC)

        # Should not raise error
        result = track_message_receive(msg=mock_message, queue_name="test-queue")

        assert result is mock_message

    def test_track_message_receive_no_insertion_time(self):
        """Test tracking when insertion_time attribute is missing."""
        # Create mock message without insertion_time
        mock_message = MagicMock(spec=func.QueueMessage)
        mock_message.dequeue_count = 1
        delattr(mock_message, "insertion_time")

        # Should not raise error
        result = track_message_receive(msg=mock_message, queue_name="test-queue")

        assert result is mock_message
