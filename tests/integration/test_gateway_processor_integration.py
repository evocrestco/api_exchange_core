"""
Integration tests for the Generic Gateway Processor.

Tests real queue output, database interactions, and full message processing workflow.
"""

import json
import pytest
from datetime import datetime, UTC
from uuid import uuid4

from src.processors.infrastructure.gateway_processor import GatewayProcessor
from src.processors.v2.message import Message, MessageType
from src.processors.v2.processor_interface import ProcessorContext
from src.processing.processing_service import ProcessingService
from src.processing.duplicate_detection import DuplicateDetectionService
from src.processing.entity_attributes import EntityAttributeBuilder
from src.services.entity_service import EntityService
from src.db.db_entity_models import Entity


class TestGatewayProcessorIntegration:
    """Integration tests for GatewayProcessor with real infrastructure."""
    
    @pytest.fixture
    def gateway_routing_config(self, azure_storage_connection_string):
        """Gateway routing configuration with real Azure Storage queue."""
        import random
        import string
        
        # Generate completely random queue names
        def random_queue_name():
            return ''.join(random.choices(string.ascii_lowercase + string.digits, k=12))
        
        high_value_queue = random_queue_name()
        express_queue = random_queue_name()
        default_queue = random_queue_name()
        
        return {
            "rules": [
                {
                    "name": "high_value_orders",
                    "condition": {
                        "field": "payload.amount",
                        "operator": ">",
                        "value": 1000
                    },
                    "destination": high_value_queue
                },
                {
                    "name": "express_shipping", 
                    "condition": {
                        "field": "payload.shipping",
                        "operator": "==",
                        "value": "express"
                    },
                    "destination": express_queue
                }
            ],
            "default_destination": default_queue,
            "queue_config": {
                "connection_string": azure_storage_connection_string,
                "auto_create_queue": True
            },
            # Store queue names for test assertions
            "_test_queues": {
                "high_value": high_value_queue,
                "express": express_queue,
                "default": default_queue
            }
        }
    
    @pytest.fixture
    def processing_service(self, integration_db_session):
        """Create real processing service with database session."""
        entity_service = EntityService(integration_db_session)
        duplicate_detection_service = DuplicateDetectionService(integration_db_session)
        attribute_builder = EntityAttributeBuilder()
        
        return ProcessingService(
            entity_service=entity_service,
            duplicate_detection_service=duplicate_detection_service,
            attribute_builder=attribute_builder
        )
    
    @pytest.fixture
    def processor_context(self, processing_service):
        """Create processor context with real services."""
        return ProcessorContext(
            processing_service=processing_service,
            state_tracking_service=None,
            error_service=None
        )
    
    @pytest.fixture
    def test_entity(self, integration_tenant_context):
        """Create test entity for messages."""
        return Entity.create(
            tenant_id=integration_tenant_context["id"],
            external_id="TEST-ORDER-001",
            canonical_type="order",
            source="test_system",
            version=1
        )
    
    @pytest.fixture
    def test_message(self, test_entity):
        """Create test message with real entity."""
        return Message(
            message_id=str(uuid4()),
            message_type=MessageType.ENTITY_PROCESSING,
            entity=test_entity,
            payload={
                "order_id": "TEST-ORDER-001",
                "amount": 1500,
                "shipping": "express",
                "customer": {"type": "premium"},
                "items": ["item1", "item2"]
            }
        )
    
    def test_single_rule_match_with_real_queue(
        self, 
        gateway_routing_config, 
        processor_context, 
        test_message,
        azure_storage_connection_string
    ):
        """Test gateway processor with single rule match using real Azure Storage queue."""
        # Modify message to match only high_value rule
        test_message.payload["shipping"] = "standard"
        
        processor = GatewayProcessor(gateway_routing_config)
        result = processor.process(test_message, processor_context)
        
        # Verify processing result
        assert result.success
        assert len(result.output_handlers) == 1
        assert result.output_handlers[0].destination == gateway_routing_config["_test_queues"]["high_value"]
        
        # Verify queue configuration
        handler = result.output_handlers[0]
        assert handler.connection_string == azure_storage_connection_string
        assert handler.auto_create_queue is True
        
        # Test actual queue message sending by executing handler
        handler_result = handler.handle(test_message, result)
        assert handler_result.success
        assert "queue_message_id" in handler_result.metadata
    
    def test_multiple_rule_match_with_real_queues(
        self, 
        gateway_routing_config, 
        processor_context, 
        test_message,
        azure_storage_connection_string
    ):
        """Test gateway processor with multiple rule matches using real queues."""
        processor = GatewayProcessor(gateway_routing_config)
        result = processor.process(test_message, processor_context)
        
        # Verify processing result
        assert result.success
        assert len(result.output_handlers) == 2
        
        destinations = [handler.destination for handler in result.output_handlers]
        assert gateway_routing_config["_test_queues"]["high_value"] in destinations
        assert gateway_routing_config["_test_queues"]["express"] in destinations
        
        # Test all output handlers can send to their queues
        for handler in result.output_handlers:
            handler_result = handler.handle(test_message, result)
            assert handler_result.success
            assert "queue_message_id" in handler_result.metadata
    
    def test_default_destination_with_real_queue(
        self, 
        gateway_routing_config, 
        processor_context, 
        test_message
    ):
        """Test gateway processor default destination with real queue."""
        # Modify message to not match any rules
        test_message.payload["amount"] = 500
        test_message.payload["shipping"] = "standard"
        
        processor = GatewayProcessor(gateway_routing_config)
        result = processor.process(test_message, processor_context)
        
        # Verify processing result
        assert result.success
        assert len(result.output_handlers) == 1
        assert result.output_handlers[0].destination == gateway_routing_config["_test_queues"]["default"]
        
        # Test queue message sending
        handler = result.output_handlers[0]
        handler_result = handler.handle(test_message, result)
        assert handler_result.success
    
    def test_complex_field_navigation(
        self, 
        azure_storage_connection_string,
        processor_context, 
        test_entity
    ):
        """Test complex field path navigation with real message and entity."""
        import random
        import string
        
        # Generate random queue names for this test
        def random_queue_name():
            return ''.join(random.choices(string.ascii_lowercase + string.digits, k=12))
        
        premium_queue = random_queue_name()
        item1_queue = random_queue_name()
        entity_queue = random_queue_name()
        
        config = {
            "rules": [
                {
                    "name": "premium_customer",
                    "condition": {
                        "field": "payload.customer.type", 
                        "operator": "==", 
                        "value": "premium"
                    },
                    "destination": premium_queue
                },
                {
                    "name": "first_item_check",
                    "condition": {
                        "field": "payload.items.0", 
                        "operator": "==", 
                        "value": "item1"
                    },
                    "destination": item1_queue
                },
                {
                    "name": "entity_processing_type",
                    "condition": {
                        "field": "message_type.value", 
                        "operator": "==", 
                        "value": "entity_processing"
                    },
                    "destination": entity_queue
                }
            ],
            "queue_config": {
                "connection_string": azure_storage_connection_string,
                "auto_create_queue": True
            }
        }
        
        message = Message(
            message_id=str(uuid4()),
            message_type=MessageType.ENTITY_PROCESSING,
            entity=test_entity,
            payload={
                "customer": {"type": "premium"},
                "items": ["item1", "item2"],
                "amount": 2000
            }
        )
        
        processor = GatewayProcessor(config)
        result = processor.process(message, processor_context)
        
        # All three rules should match
        assert result.success
        assert len(result.output_handlers) == 3
        
        destinations = [handler.destination for handler in result.output_handlers]
        assert premium_queue in destinations
        assert item1_queue in destinations
        assert entity_queue in destinations
        
        # Test all handlers work with real queues
        for handler in result.output_handlers:
            handler_result = handler.handle(message, result)
            assert handler_result.success
    
    def test_message_serialization_in_queue(
        self, 
        gateway_routing_config, 
        processor_context, 
        test_message,
        azure_storage_connection_string
    ):
        """Test that messages are properly serialized when sent to real queues."""
        processor = GatewayProcessor(gateway_routing_config)
        result = processor.process(test_message, processor_context)
        
        # Execute the first output handler to send message to real queue
        handler = result.output_handlers[0]
        handler_result = handler.handle(test_message, result)
        assert handler_result.success
        
        # Create a queue client for the actual destination queue that was used
        from azure.storage.queue import QueueClient
        destination_queue_client = QueueClient.from_connection_string(
            conn_str=azure_storage_connection_string,
            queue_name=handler.destination
        )
        
        # Verify message was actually sent to the destination queue
        # Give it a moment to propagate
        import time
        time.sleep(0.1)
        
        # Peek at the destination queue to see if our message is there
        messages = list(destination_queue_client.peek_messages(max_messages=5))
        assert len(messages) > 0
        
        # Parse the message and verify structure (should be the only message in this unique queue)
        queue_message_content = json.loads(messages[0].content)
        
        # Verify message structure matches QueueOutputHandler format
        assert "message_metadata" in queue_message_content
        assert "entity_reference" in queue_message_content  
        assert "payload" in queue_message_content
        assert "processing_result" in queue_message_content
        assert "routing_metadata" in queue_message_content
        
        # Verify specific content
        assert queue_message_content["message_metadata"]["message_id"] == test_message.message_id
        assert queue_message_content["entity_reference"]["external_id"] == "TEST-ORDER-001"
        assert queue_message_content["payload"]["amount"] == 1500
        assert queue_message_content["processing_result"]["success"] is True
        
        # Clean up the destination queue
        try:
            destination_queue_client.clear_messages()
        except Exception:
            pass
    
    def test_routing_metadata_tracking(
        self, 
        gateway_routing_config, 
        processor_context, 
        test_message
    ):
        """Test that routing metadata is properly tracked."""
        processor = GatewayProcessor(gateway_routing_config)
        result = processor.process(test_message, processor_context)
        
        # Verify routing metadata was added
        assert "routing" in result.processing_metadata
        routing_metadata = result.processing_metadata["routing"]
        
        assert "evaluated_rules" in routing_metadata
        assert "matched_rules" in routing_metadata
        assert "destinations" in routing_metadata
        
        # Should have evaluated 2 rules and matched both
        assert len(routing_metadata["evaluated_rules"]) == 2
        assert len(routing_metadata["matched_rules"]) == 2
        assert len(routing_metadata["destinations"]) == 2
        
        assert "high_value_orders" in routing_metadata["matched_rules"]
        assert "express_shipping" in routing_metadata["matched_rules"]
        # Check that destinations contain the expected queue names
        assert gateway_routing_config["_test_queues"]["high_value"] in routing_metadata["destinations"]
        assert gateway_routing_config["_test_queues"]["express"] in routing_metadata["destinations"]
    
    def test_gateway_processor_info(self, gateway_routing_config):
        """Test gateway processor info generation."""
        processor = GatewayProcessor(gateway_routing_config)
        info = processor.get_processor_info()
        
        assert info["name"] == "GatewayProcessor"
        assert info["type"] == "gateway"
        assert info["rule_count"] == 2
        assert info["default_destination"] == gateway_routing_config["_test_queues"]["default"]
        assert info["has_queue_config"] is True
    
    def test_validate_message_always_true(self, gateway_routing_config, test_message):
        """Test that gateway processor validates all messages."""
        processor = GatewayProcessor(gateway_routing_config)
        assert processor.validate_message(test_message) is True
    
    def test_cannot_retry_errors(self, gateway_routing_config):
        """Test that gateway processor errors are not retryable."""
        processor = GatewayProcessor(gateway_routing_config)
        assert processor.can_retry(Exception("test error")) is False