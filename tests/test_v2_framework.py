"""
Simple test to verify the V2 framework components work correctly.
"""

from api_exchange_core.processors import (
    Message,
    MessageType,
    ProcessingResult,
    ProcessingStatus,
    SimpleProcessorInterface,
    SimpleProcessorHandler,
    NoOpOutputHandler,
)


class TestProcessor(SimpleProcessorInterface):
    """Simple test processor that transforms order data."""
    
    def process(self, message: Message, context: dict) -> ProcessingResult:
        """Process the message and return a result."""
        try:
            # Simulate processing
            payload = message.payload
            
            # Transform the data
            transformed_payload = {
                "processed_data": payload,
                "processor": self.get_processor_name(),
                "pipeline_id": message.pipeline_id,
            }
            
            # Create output message
            output_message = self.create_output_message(
                payload=transformed_payload,
                source_message=message
            )
            
            return ProcessingResult.success_result(
                output_messages=[output_message],
                records_processed=1
            )
            
        except Exception as e:
            return ProcessingResult.failure_result(
                error_message=str(e),
                error_code="PROCESSING_ERROR"
            )


def test_basic_framework():
    """Test the basic framework components."""
    print("Testing V2 Framework Components...")
    
    # Create test message
    message = Message.create_simple_message(
        payload={"order_id": "123", "customer": "test"},
        tenant_id="test_tenant"
    )
    
    print(f"Created message: {message.message_id}")
    print(f"Pipeline ID: {message.pipeline_id}")
    print(f"Payload: {message.payload}")
    
    # Create processor
    processor = TestProcessor()
    
    # Create handler
    handler = SimpleProcessorHandler(processor)
    
    # Process message
    result = handler.process_message(message, {"tenant_id": "test_tenant"})
    
    print(f"Processing result: {result.status}")
    print(f"Success: {result.success}")
    print(f"Output messages: {len(result.output_messages)}")
    
    if result.output_messages:
        output_msg = result.output_messages[0]
        print(f"Output message ID: {output_msg.message_id}")
        print(f"Output pipeline ID: {output_msg.pipeline_id}")
        print(f"Output payload: {output_msg.payload}")
    
    # Verify pipeline_id propagation
    if result.output_messages:
        output_msg = result.output_messages[0]
        assert output_msg.pipeline_id == message.pipeline_id, "Pipeline ID should propagate"
        print("✓ Pipeline ID propagation works")
    
    print("✓ Basic framework test passed!")


def test_failure_handling():
    """Test failure handling."""
    print("\nTesting failure handling...")
    
    class FailingProcessor(SimpleProcessorInterface):
        def process(self, message: Message, context: dict) -> ProcessingResult:
            return ProcessingResult.failure_result(
                error_message="Test failure",
                error_code="TEST_ERROR"
            )
    
    # Create test message
    message = Message.create_simple_message(
        payload={"test": "data"},
        tenant_id="test_tenant"
    )
    
    # Create failing processor
    processor = FailingProcessor()
    handler = SimpleProcessorHandler(processor)
    
    # Process message
    result = handler.process_message(message, {"tenant_id": "test_tenant"})
    
    print(f"Processing result: {result.status}")
    print(f"Success: {result.success}")
    print(f"Error message: {result.error_message}")
    print(f"Error code: {result.error_code}")
    
    assert result.status == ProcessingStatus.FAILURE
    assert not result.success
    assert result.error_message == "Test failure"
    assert result.error_code == "TEST_ERROR"
    
    print("✓ Failure handling test passed!")


if __name__ == "__main__":
    test_basic_framework()
    test_failure_handling()
    print("\n✅ All V2 framework tests passed!")