#!/usr/bin/env python3
"""
Demo script to show the new unified JSON logging structure.

This script demonstrates:
1. Console logs still show pipe-delimited format for readability
2. AzureQueueHandler creates clean JSON structure with separated concerns  
3. Correlation and operation IDs are properly extracted and promoted
4. Important fields are promoted to top-level while other data goes to context
"""

import json
import logging
import os
from io import StringIO
from unittest.mock import patch

from api_exchange_core.context.operation_context import operation
from api_exchange_core.context.tenant_context import tenant_context
from api_exchange_core.exceptions import set_correlation_id
from api_exchange_core.utils.logger import AzureQueueHandler, ContextAwareLogger, get_logger


class MockQueueClient:
    """Mock queue client to capture queue messages without Azure dependency."""
    
    def __init__(self):
        self.sent_messages = []
    
    def send_message(self, message):
        self.sent_messages.append(message)


def demo_logging():
    """Demonstrate the new unified logging structure."""
    
    print("=== UNIFIED JSON LOGGING DEMO ===\n")
    
    # Set up correlation ID
    set_correlation_id("demo-correlation-123")
    
    # Create logger with both console and mock queue handlers
    logger = logging.getLogger("demo.logger")
    logger.setLevel(logging.INFO)
    
    # Clear any existing handlers
    logger.handlers.clear()
    
    # Console handler - shows pipe-delimited format
    console_stream = StringIO()
    console_handler = logging.StreamHandler(console_stream)
    console_handler.setFormatter(logging.Formatter("%(levelname)s: %(message)s"))
    logger.addHandler(console_handler)
    
    # Mock Azure Queue handler - captures JSON structure
    mock_queue_client = MockQueueClient()
    
    with patch('api_exchange_core.utils.logger.QueueClient') as mock_queue_class:
        mock_queue_class.from_connection_string.return_value = mock_queue_client
        
        # Create queue handler
        queue_handler = AzureQueueHandler(
            queue_name="logs-queue",
            connection_string="mock://test"
        )
        queue_handler.setLevel(logging.INFO)
        logger.addHandler(queue_handler)
        
        # Wrap with ContextAwareLogger
        context_logger = ContextAwareLogger(logger)
        
        # Test 1: Basic logging with extra data
        print("1. Basic logging with extra data:")
        print("   Code: logger.info('Processing started', extra={'entity_id': '123', 'action': 'create'})")
        
        context_logger.info("Processing started", extra={
            "entity_id": "123", 
            "action": "create",
            "batch_size": 10
        })
        
        print(f"   Console output: {console_stream.getvalue().strip()}")
        
        if mock_queue_client.sent_messages:
            queue_msg = json.loads(mock_queue_client.sent_messages[0])
            print("   Queue JSON structure:")
            print(f"     {json.dumps(queue_msg, indent=6)}")
        
        console_stream.truncate(0)
        console_stream.seek(0)
        mock_queue_client.sent_messages.clear()
        
        print("\n" + "="*60 + "\n")
        
        # Test 2: Error logging with promoted fields
        print("2. Error logging with promoted fields:")
        print("   Code: logger.error('Operation failed', extra={'error_code': 'E001', 'tenant_id': 'tenant-456'})")
        
        context_logger.error("Operation failed", extra={
            "error_code": "E001",
            "tenant_id": "tenant-456", 
            "retry_count": 3,
            "error_details": {"reason": "timeout", "duration_ms": 5000}
        })
        
        print(f"   Console output: {console_stream.getvalue().strip()}")
        
        if mock_queue_client.sent_messages:
            queue_msg = json.loads(mock_queue_client.sent_messages[0])
            print("   Queue JSON structure:")
            print(f"     {json.dumps(queue_msg, indent=6)}")
        
        console_stream.truncate(0)
        console_stream.seek(0)
        mock_queue_client.sent_messages.clear()
        
        print("\n" + "="*60 + "\n")
        
        # Test 3: Operation decorator integration
        print("3. Operation decorator with context:")
        
        @operation("demo.process_data")
        def process_data(item_count: int):
            """Demo function with operation decorator."""
            logger = get_logger()
            logger.info("Processing items", extra={
                "item_count": item_count,
                "processor_type": "batch"
            })
            return f"Processed {item_count} items"
        
        with tenant_context("demo-tenant-789"):
            result = process_data(25)
        
        print(f"   Function result: {result}")
        print(f"   Console output: {console_stream.getvalue().strip()}")
        
        if mock_queue_client.sent_messages:
            # Show the ENTER log
            enter_msg = json.loads(mock_queue_client.sent_messages[0])
            print("   ENTER log JSON structure:")
            print(f"     {json.dumps(enter_msg, indent=6)}")
            
            if len(mock_queue_client.sent_messages) > 1:
                # Show the business logic log  
                business_msg = json.loads(mock_queue_client.sent_messages[1])
                print("   Business log JSON structure:")
                print(f"     {json.dumps(business_msg, indent=6)}")
        
        print("\n" + "="*60 + "\n")
        
        print("Key Benefits of New Structure:")
        print("✓ Console logs remain readable with pipe-delimited format")
        print("✓ Queue logs have clean JSON structure with separated concerns")
        print("✓ Important fields (tenant_id, entity_id, error_code) promoted to top-level")
        print("✓ Correlation and operation IDs automatically extracted and promoted")  
        print("✓ Context object contains application-specific data")
        print("✓ Backward compatible - no changes needed in existing logging calls")
        print("✓ Grafana queries can access top-level fields without JSON parsing")


if __name__ == "__main__":
    demo_logging()