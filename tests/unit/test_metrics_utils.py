"""
Unit tests for metrics utilities.

Tests the metrics processing functionality for Azure Storage Queues.
"""

import os
from datetime import datetime
from unittest.mock import Mock, patch, MagicMock
import pytest

from api_exchange_core.utils.metrics_utils import send_metrics_to_queue, process_metrics
from api_exchange_core.schemas.metric_model import Metric, QueueMetric, FileMetric, OperationMetric
from api_exchange_core.constants import QueueName, EnvironmentVariable


class TestSendMetricsToQueue:
    """Test send_metrics_to_queue function."""
    
    def test_empty_metrics_list(self):
        """Test handling of empty metrics list."""
        with patch('api_exchange_core.utils.metrics_utils.get_logger') as mock_get_logger:
            logger_instance = Mock()
            mock_get_logger.return_value = logger_instance
            
            # Call with empty list
            send_metrics_to_queue([])
            
            # Verify info log and early return
            logger_instance.info.assert_called_once_with("No metrics to process")
            logger_instance.debug.assert_not_called()
    
    def test_no_connection_string_fallback_logging(self):
        """Test fallback to logging when no connection string is available."""
        # Create test metrics
        metrics = [
            Metric(metric_name="test_metric_1", value=100, labels={"tag": "value1"}),
            Metric(metric_name="test_metric_2", value=200, labels={"tag": "value2"})
        ]
        
        with patch('api_exchange_core.utils.metrics_utils.get_logger') as mock_get_logger:
            with patch.dict(os.environ, {}, clear=True):  # Clear environment
                logger_instance = Mock()
                mock_get_logger.return_value = logger_instance
                
                # Call without connection string
                send_metrics_to_queue(metrics)
                
                # Verify error log and fallback warning logs
                logger_instance.error.assert_called_once_with("No Azure Storage connection string available")
                assert logger_instance.warning.call_count == 2
                logger_instance.warning.assert_any_call("METRIC: test_metric_1, value=100")
                logger_instance.warning.assert_any_call("METRIC: test_metric_2, value=200")
    
    def test_explicit_connection_string_parameter(self):
        """Test passing connection string as parameter."""
        metrics = [Metric(metric_name="test_metric", value=42)]
        
        with patch('api_exchange_core.utils.metrics_utils.QueueClient') as mock_queue_client_class:
            with patch('api_exchange_core.utils.metrics_utils.get_logger') as mock_get_logger:
                logger_instance = Mock()
                mock_get_logger.return_value = logger_instance
                
                mock_queue_client = Mock()
                mock_queue_client_class.from_connection_string.return_value = mock_queue_client
                
                connection_string = "DefaultEndpointsProtocol=https;AccountName=test;AccountKey=key=="
                
                # Call with explicit connection string
                send_metrics_to_queue(metrics, connection_string=connection_string)
                
                # Verify QueueClient was created with explicit connection string
                mock_queue_client_class.from_connection_string.assert_called_once_with(
                    conn_str=connection_string, 
                    queue_name=QueueName.METRICS.value
                )
                
                # Verify metric was sent
                mock_queue_client.send_message.assert_called_once()
    
    def test_environment_variable_connection_string(self):
        """Test using connection string from environment variable."""
        metrics = [Metric(metric_name="env_test", value=123)]
        env_connection_string = "DefaultEndpointsProtocol=https;AccountName=env;AccountKey=envkey=="
        
        with patch('api_exchange_core.utils.metrics_utils.QueueClient') as mock_queue_client_class:
            with patch('api_exchange_core.utils.metrics_utils.get_logger') as mock_get_logger:
                with patch.dict(os.environ, {EnvironmentVariable.AZURE_STORAGE_CONNECTION.value: env_connection_string}):
                    logger_instance = Mock()
                    mock_get_logger.return_value = logger_instance
                    
                    mock_queue_client = Mock()
                    mock_queue_client_class.from_connection_string.return_value = mock_queue_client
                    
                    # Call without explicit connection string
                    send_metrics_to_queue(metrics)
                    
                    # Verify QueueClient was created with environment connection string
                    mock_queue_client_class.from_connection_string.assert_called_once_with(
                        conn_str=env_connection_string, 
                        queue_name=QueueName.METRICS.value
                    )
    
    def test_custom_queue_name(self):
        """Test using custom queue name."""
        metrics = [Metric(metric_name="custom_queue_test", value=456)]
        custom_queue = "custom-metrics-queue"
        
        with patch('api_exchange_core.utils.metrics_utils.QueueClient') as mock_queue_client_class:
            with patch('api_exchange_core.utils.metrics_utils.get_logger') as mock_get_logger:
                logger_instance = Mock()
                mock_get_logger.return_value = logger_instance
                
                mock_queue_client = Mock()
                mock_queue_client_class.from_connection_string.return_value = mock_queue_client
                
                connection_string = "DefaultEndpointsProtocol=https;AccountName=test;AccountKey=key=="
                
                # Call with custom queue name
                send_metrics_to_queue(metrics, queue_name=custom_queue, connection_string=connection_string)
                
                # Verify QueueClient was created with custom queue name
                mock_queue_client_class.from_connection_string.assert_called_once_with(
                    conn_str=connection_string, 
                    queue_name=custom_queue
                )
    
    def test_successful_metrics_processing(self):
        """Test successful processing of multiple metrics."""
        # Create different types of metrics
        metrics = [
            Metric(metric_name="basic_metric", value=100, labels={"type": "basic"}),
            QueueMetric.message_count("test-queue", "send", 5),
            FileMetric.bytes_written("/tmp/test.txt", 1024),
            OperationMetric.duration("process", "module", "function", "tenant1", "success", 250.5)
        ]
        
        with patch('api_exchange_core.utils.metrics_utils.QueueClient') as mock_queue_client_class:
            with patch('api_exchange_core.utils.metrics_utils.get_logger') as mock_get_logger:
                logger_instance = Mock()
                mock_get_logger.return_value = logger_instance
                
                mock_queue_client = Mock()
                mock_queue_client_class.from_connection_string.return_value = mock_queue_client
                
                connection_string = "DefaultEndpointsProtocol=https;AccountName=test;AccountKey=key=="
                
                # Call with multiple metrics
                send_metrics_to_queue(metrics, connection_string=connection_string)
                
                # Verify all metrics were sent
                assert mock_queue_client.send_message.call_count == 4
                
                # Verify debug logs
                logger_instance.debug.assert_any_call(f"Sending {len(metrics)} metrics to queue {QueueName.METRICS.value}")
                logger_instance.debug.assert_any_call(f"Processed {len(metrics)} metrics")
                
                # Verify individual metric debug logs (4 metrics = 8 debug calls total)
                assert logger_instance.debug.call_count >= 6  # At least 2 + 4 individual metric logs
    
    def test_queue_client_initialization_failure(self):
        """Test handling of queue client initialization failure."""
        metrics = [Metric(metric_name="init_fail_test", value=789)]
        
        with patch('api_exchange_core.utils.metrics_utils.QueueClient') as mock_queue_client_class:
            with patch('api_exchange_core.utils.metrics_utils.get_logger') as mock_get_logger:
                logger_instance = Mock()
                mock_get_logger.return_value = logger_instance
                
                # Make QueueClient initialization fail
                mock_queue_client_class.from_connection_string.side_effect = Exception("Connection failed")
                
                connection_string = "DefaultEndpointsProtocol=https;AccountName=test;AccountKey=key=="
                
                # Call and expect fallback logging
                send_metrics_to_queue(metrics, connection_string=connection_string)
                
                # Verify error was logged and fallback occurred
                logger_instance.error.assert_called_with("Failed to initialize queue client: Connection failed")
                
                # Verify fallback logging of metrics
                logger_instance.warning.assert_called_once()
                warning_call = logger_instance.warning.call_args[0][0]
                assert "METRIC: metric" in warning_call  # Check fallback log format
                assert "name=init_fail_test" in warning_call
                assert "value=789" in warning_call
    
    def test_individual_metric_send_failure(self):
        """Test handling of individual metric send failures."""
        metrics = [
            Metric(metric_name="metric_1", value=100),
            Metric(metric_name="metric_2", value=200),
            Metric(metric_name="metric_3", value=300)
        ]
        
        with patch('api_exchange_core.utils.metrics_utils.QueueClient') as mock_queue_client_class:
            with patch('api_exchange_core.utils.metrics_utils.get_logger') as mock_get_logger:
                logger_instance = Mock()
                mock_get_logger.return_value = logger_instance
                
                mock_queue_client = Mock()
                mock_queue_client_class.from_connection_string.return_value = mock_queue_client
                
                # Make second metric send fail
                mock_queue_client.send_message.side_effect = [
                    None,  # Success
                    Exception("Network timeout"),  # Failure
                    None   # Success
                ]
                
                connection_string = "DefaultEndpointsProtocol=https;AccountName=test;AccountKey=key=="
                
                # Call with metrics
                send_metrics_to_queue(metrics, connection_string=connection_string)
                
                # Verify all sends were attempted
                assert mock_queue_client.send_message.call_count == 3
                
                # Verify error was logged for failed metric
                logger_instance.error.assert_called_once()
                error_call = logger_instance.error.call_args[0][0]
                assert "Failed to send metric 2: Network timeout" in error_call
    
    def test_queue_not_found_and_creation_success(self):
        """Test queue creation when queue doesn't exist."""
        metrics = [Metric(metric_name="queue_creation_test", value=42)]
        
        with patch('api_exchange_core.utils.metrics_utils.QueueClient') as mock_queue_client_class:
            with patch('api_exchange_core.utils.metrics_utils.get_logger') as mock_get_logger:
                logger_instance = Mock()
                mock_get_logger.return_value = logger_instance
                
                mock_queue_client = Mock()
                mock_queue_client_class.from_connection_string.return_value = mock_queue_client
                
                # First send fails with QueueNotFound, retry succeeds
                mock_queue_client.send_message.side_effect = [
                    Exception("QueueNotFound: The specified queue does not exist."),
                    None  # Success on retry
                ]
                
                connection_string = "DefaultEndpointsProtocol=https;AccountName=test;AccountKey=key=="
                
                # Call with metric
                send_metrics_to_queue(metrics, connection_string=connection_string)
                
                # Verify queue creation was attempted
                mock_queue_client.create_queue.assert_called_once()
                
                # Verify send_message was called twice (initial fail + retry)
                assert mock_queue_client.send_message.call_count == 2
                
                # Verify debug logs for queue creation
                logger_instance.debug.assert_any_call(f"Queue {QueueName.METRICS.value} not found, creating it...")
    
    def test_queue_creation_failure(self):
        """Test handling of queue creation failures."""
        metrics = [Metric(metric_name="creation_fail_test", value=123)]
        
        with patch('api_exchange_core.utils.metrics_utils.QueueClient') as mock_queue_client_class:
            with patch('api_exchange_core.utils.metrics_utils.get_logger') as mock_get_logger:
                logger_instance = Mock()
                mock_get_logger.return_value = logger_instance
                
                mock_queue_client = Mock()
                mock_queue_client_class.from_connection_string.return_value = mock_queue_client
                
                # Send fails with QueueNotFound, create_queue also fails
                mock_queue_client.send_message.side_effect = Exception("does not exist")
                mock_queue_client.create_queue.side_effect = Exception("Failed to create queue")
                
                connection_string = "DefaultEndpointsProtocol=https;AccountName=test;AccountKey=key=="
                
                # Call with metric
                send_metrics_to_queue(metrics, connection_string=connection_string)
                
                # Verify error was logged for queue creation failure
                logger_instance.error.assert_called_with("Failed to create queue or send metric 1: Failed to create queue")
    
    def test_metrics_with_complex_labels(self):
        """Test metrics with complex label structures."""
        # Create metric with complex labels
        complex_metric = Metric(
            metric_name="complex_test",
            value=999,
            labels={
                "string_label": "test_value",
                "numeric_label": 42,
                "boolean_label": True,
                "nested_object": {"key": "value"},
                "list_label": ["item1", "item2"]
            }
        )
        
        with patch('api_exchange_core.utils.metrics_utils.QueueClient') as mock_queue_client_class:
            with patch('api_exchange_core.utils.metrics_utils.get_logger') as mock_get_logger:
                logger_instance = Mock()
                mock_get_logger.return_value = logger_instance
                
                mock_queue_client = Mock()
                mock_queue_client_class.from_connection_string.return_value = mock_queue_client
                
                # Make QueueClient fail to test fallback logging
                mock_queue_client_class.from_connection_string.side_effect = Exception("Test failure")
                
                connection_string = "DefaultEndpointsProtocol=https;AccountName=test;AccountKey=key=="
                
                # Call with complex metric
                send_metrics_to_queue([complex_metric], connection_string=connection_string)
                
                # Verify fallback logging includes all labels
                logger_instance.warning.assert_called_once()
                warning_call = logger_instance.warning.call_args[0][0]
                assert "METRIC: metric" in warning_call
                assert "name=complex_test" in warning_call
                assert "value=999" in warning_call
                # Check that labels are included in the log
                assert "string_label=test_value" in warning_call
                assert "numeric_label=42" in warning_call


class TestProcessMetricsAlias:
    """Test process_metrics alias function."""
    
    def test_process_metrics_alias_functionality(self):
        """Test that process_metrics alias works identically to send_metrics_to_queue."""
        metrics = [Metric(metric_name="alias_test", value=555)]
        
        # Since process_metrics is just an alias (process_metrics = send_metrics_to_queue),
        # we need to test that it behaves the same way as the main function
        with patch('api_exchange_core.utils.metrics_utils.QueueClient') as mock_queue_client_class:
            with patch('api_exchange_core.utils.metrics_utils.get_logger') as mock_get_logger:
                logger_instance = Mock()
                mock_get_logger.return_value = logger_instance
                
                mock_queue_client = Mock()
                mock_queue_client_class.from_connection_string.return_value = mock_queue_client
                
                connection_string = "DefaultEndpointsProtocol=https;AccountName=test;AccountKey=key=="
                
                # Call alias function
                process_metrics(metrics, queue_name="test-queue", connection_string=connection_string)
                
                # Verify it behaves the same as send_metrics_to_queue
                mock_queue_client_class.from_connection_string.assert_called_once_with(
                    conn_str=connection_string, 
                    queue_name="test-queue"
                )
                mock_queue_client.send_message.assert_called_once()


class TestMetricsUtilsIntegration:
    """Integration tests for metrics utilities."""
    
    def test_different_metric_types_serialization(self):
        """Test that different metric types serialize correctly."""
        # Create different metric types
        metrics = [
            Metric(metric_name="basic", value=100),
            QueueMetric.message_count("test-queue", "send", 5),
            FileMetric.processing_time("/path/file.txt", 1500),
            OperationMetric.duration("op", "mod", "func", "tenant", "success", 123.45)
        ]
        
        with patch('api_exchange_core.utils.metrics_utils.QueueClient') as mock_queue_client_class:
            with patch('api_exchange_core.utils.metrics_utils.get_logger') as mock_get_logger:
                logger_instance = Mock()
                mock_get_logger.return_value = logger_instance
                
                mock_queue_client = Mock()
                mock_queue_client_class.from_connection_string.return_value = mock_queue_client
                
                connection_string = "DefaultEndpointsProtocol=https;AccountName=test;AccountKey=key=="
                
                # Call with different metric types
                send_metrics_to_queue(metrics, connection_string=connection_string)
                
                # Verify all metrics were sent
                assert mock_queue_client.send_message.call_count == 4
                
                # Verify each metric was serialized to JSON
                sent_messages = [call[0][0] for call in mock_queue_client.send_message.call_args_list]
                
                # Each message should be a JSON string
                for message in sent_messages:
                    assert isinstance(message, str)
                    assert message.startswith('{')
                    assert message.endswith('}')
    
    def test_metric_timestamp_serialization(self):
        """Test that metric timestamps are properly serialized."""
        test_time = datetime(2024, 1, 15, 10, 30, 45)
        metric = Metric(metric_name="timestamp_test", value=42, timestamp=test_time)
        
        with patch('api_exchange_core.utils.metrics_utils.QueueClient') as mock_queue_client_class:
            with patch('api_exchange_core.utils.metrics_utils.get_logger') as mock_get_logger:
                logger_instance = Mock()
                mock_get_logger.return_value = logger_instance
                
                mock_queue_client = Mock()
                mock_queue_client_class.from_connection_string.return_value = mock_queue_client
                
                connection_string = "DefaultEndpointsProtocol=https;AccountName=test;AccountKey=key=="
                
                # Call with timestamp metric
                send_metrics_to_queue([metric], connection_string=connection_string)
                
                # Verify metric was sent and contains ISO timestamp
                mock_queue_client.send_message.assert_called_once()
                sent_message = mock_queue_client.send_message.call_args[0][0]
                
                # Message should contain ISO formatted timestamp
                assert "2024-01-15T10:30:45" in sent_message