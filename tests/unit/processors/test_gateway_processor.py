"""
Unit tests for the Generic Gateway Processor.

Tests routing logic, condition evaluation, and output handler configuration.
"""

import pytest
from datetime import datetime, UTC
from uuid import uuid4

from src.processors.infrastructure.gateway_processor import GatewayProcessor
from src.processors.v2.message import Message, MessageType
from src.processors.processing_result import ProcessingResult, ProcessingStatus
from src.processors.v2.processor_interface import ProcessorContext
from src.db.db_entity_models import Entity


class TestGatewayProcessor:
    """Test suite for GatewayProcessor."""
    
    @pytest.fixture
    def basic_routing_config(self):
        """Basic routing configuration for testing."""
        return {
            "rules": [
                {
                    "name": "high_value",
                    "condition": {
                        "field": "payload.amount",
                        "operator": ">",
                        "value": 1000
                    },
                    "destination": "high-value-queue"
                },
                {
                    "name": "express_shipping",
                    "condition": {
                        "field": "payload.shipping",
                        "operator": "==",
                        "value": "express"
                    },
                    "destination": "express-queue"
                }
            ],
            "default_destination": "standard-queue"
        }
    
    @pytest.fixture
    def queue_config(self):
        """Default queue configuration."""
        return {
            "connection_string": "UseDevelopmentStorage=true",
            "auto_create_queue": True
        }
    
    @pytest.fixture
    def sample_message(self):
        """Create a sample message for testing."""
        # Create a real entity object (as it would exist in a message)
        entity = Entity.create(
            tenant_id="test-tenant",
            external_id="TEST-001",
            canonical_type="test_order",
            source="test_system",
            version=1
        )
        
        return Message.from_entity(
            entity=entity,
            payload={
                "order_id": "TEST-001",
                "amount": 1500,
                "shipping": "express",
                "items": ["item1", "item2"]
            },
            correlation_id=str(uuid4())
        )
    
    @pytest.fixture
    def mock_context(self):
        """Create a processor context for testing."""
        # Gateway processor doesn't use context services, so we can pass None
        return ProcessorContext(
            processing_service=None,
            state_tracking_service=None,
            error_service=None
        )
    
    def test_initialization(self, basic_routing_config, queue_config):
        """Test processor initialization."""
        processor = GatewayProcessor(basic_routing_config, queue_config)
        
        assert processor.routing_config == basic_routing_config
        assert len(processor.rules) == 2
        assert processor.default_destination == "standard-queue"
        assert processor.queue_config == queue_config
    
    def test_single_rule_match(self, basic_routing_config, queue_config, sample_message, mock_context):
        """Test routing with single rule match."""
        # Modify message to match only high_value rule
        sample_message.payload["shipping"] = "standard"
        
        processor = GatewayProcessor(basic_routing_config, queue_config)
        result = processor.process(sample_message, mock_context)
        
        assert result.success
        assert len(result.output_handlers) == 1
        assert result.output_handlers[0].destination == "high-value-queue"
        
        # Check metadata
        routing_metadata = result.processing_metadata.get("routing", {})
        assert "high_value" in routing_metadata["matched_rules"]
        assert "high-value-queue" in routing_metadata["destinations"]
    
    def test_multiple_rule_match(self, basic_routing_config, queue_config, sample_message, mock_context):
        """Test routing with multiple rule matches."""
        processor = GatewayProcessor(basic_routing_config, queue_config)
        result = processor.process(sample_message, mock_context)
        
        assert result.success
        assert len(result.output_handlers) == 2
        
        destinations = [handler.destination for handler in result.output_handlers]
        assert "high-value-queue" in destinations
        assert "express-queue" in destinations
        
        # Check metadata
        routing_metadata = result.processing_metadata.get("routing", {})
        assert len(routing_metadata["matched_rules"]) == 2
        assert len(routing_metadata["destinations"]) == 2
    
    def test_no_rule_match_uses_default(self, basic_routing_config, queue_config, sample_message, mock_context):
        """Test routing to default destination when no rules match."""
        # Modify message to not match any rules
        sample_message.payload["amount"] = 500
        sample_message.payload["shipping"] = "standard"
        
        processor = GatewayProcessor(basic_routing_config, queue_config)
        result = processor.process(sample_message, mock_context)
        
        assert result.success
        assert len(result.output_handlers) == 1
        assert result.output_handlers[0].destination == "standard-queue"
    
    def test_stop_on_match(self, queue_config, sample_message, mock_context):
        """Test stop_on_match functionality."""
        config = {
            "rules": [
                {
                    "name": "first_rule",
                    "condition": {"field": "payload.amount", "operator": ">", "value": 1000},
                    "destination": "first-queue",
                    "stop_on_match": True
                },
                {
                    "name": "second_rule",
                    "condition": {"field": "payload.amount", "operator": ">", "value": 500},
                    "destination": "second-queue"
                }
            ]
        }
        
        processor = GatewayProcessor(config, queue_config)
        result = processor.process(sample_message, mock_context)
        
        assert result.success
        assert len(result.output_handlers) == 1
        assert result.output_handlers[0].destination == "first-queue"
        
        # Second rule should not be evaluated
        routing_metadata = result.processing_metadata.get("routing", {})
        assert len(routing_metadata["matched_rules"]) == 1
        assert "first_rule" in routing_metadata["matched_rules"]
    
    def test_field_path_navigation(self, queue_config, mock_context):
        """Test different field path navigation scenarios."""
        config = {
            "rules": [
                {
                    "name": "nested_field",
                    "condition": {"field": "payload.customer.type", "operator": "==", "value": "premium"},
                    "destination": "premium-queue"
                },
                {
                    "name": "array_access",
                    "condition": {"field": "payload.items.0", "operator": "==", "value": "special"},
                    "destination": "special-queue"
                },
                {
                    "name": "message_field",
                    "condition": {"field": "message_type.value", "operator": "==", "value": "entity_processing"},
                    "destination": "entity-queue"
                }
            ]
        }
        
        # Create message with nested structures
        entity = Entity.create(
            tenant_id="test-tenant",
            external_id="TEST-001",
            canonical_type="test_order",
            source="test_system",
            version=1
        )
        
        message = Message.from_entity(
            entity=entity,
            payload={
                "customer": {"type": "premium"},
                "items": ["special", "regular"],
                "value": 1000
            },
            correlation_id=str(uuid4())
        )
        
        processor = GatewayProcessor(config, queue_config)
        result = processor.process(message, mock_context)
        
        assert result.success
        assert len(result.output_handlers) == 3
        
        destinations = [handler.destination for handler in result.output_handlers]
        assert "premium-queue" in destinations
        assert "special-queue" in destinations
        assert "entity-queue" in destinations
    
    def test_operator_types(self, queue_config, sample_message, mock_context):
        """Test different operator types."""
        config = {
            "rules": [
                {
                    "name": "not_equal",
                    "condition": {"field": "payload.shipping", "operator": "!=", "value": "slow"},
                    "destination": "not-slow-queue"
                },
                {
                    "name": "greater_equal",
                    "condition": {"field": "payload.amount", "operator": ">=", "value": 1500},
                    "destination": "ge-queue"
                },
                {
                    "name": "in_list",
                    "condition": {"field": "payload.shipping", "operator": "in", "value": ["express", "priority"]},
                    "destination": "fast-shipping-queue"
                },
                {
                    "name": "contains",
                    "condition": {"field": "payload.items", "operator": "contains", "value": "item1"},
                    "destination": "has-item1-queue"
                }
            ]
        }
        
        processor = GatewayProcessor(config, queue_config)
        result = processor.process(sample_message, mock_context)
        
        assert result.success
        assert len(result.output_handlers) == 4
        
        destinations = [handler.destination for handler in result.output_handlers]
        assert all(d in destinations for d in ["not-slow-queue", "ge-queue", "fast-shipping-queue", "has-item1-queue"])
    
    def test_regex_matching(self, queue_config, sample_message, mock_context):
        """Test regex matching operator."""
        config = {
            "rules": [
                {
                    "name": "order_pattern",
                    "condition": {"field": "payload.order_id", "operator": "matches", "value": r"TEST-\d+"},
                    "destination": "test-orders-queue"
                }
            ]
        }
        
        processor = GatewayProcessor(config, queue_config)
        result = processor.process(sample_message, mock_context)
        
        assert result.success
        assert len(result.output_handlers) == 1
        assert result.output_handlers[0].destination == "test-orders-queue"
    
    def test_missing_field_no_match(self, queue_config, sample_message, mock_context):
        """Test that missing fields don't match."""
        config = {
            "rules": [
                {
                    "name": "missing_field",
                    "condition": {"field": "payload.nonexistent", "operator": "==", "value": "anything"},
                    "destination": "never-reached-queue"
                }
            ],
            "default_destination": "default-queue"
        }
        
        processor = GatewayProcessor(config, queue_config)
        result = processor.process(sample_message, mock_context)
        
        assert result.success
        assert len(result.output_handlers) == 1
        assert result.output_handlers[0].destination == "default-queue"
    
    def test_no_default_no_match(self, queue_config, sample_message, mock_context):
        """Test no output when no rules match and no default."""
        config = {
            "rules": [
                {
                    "name": "never_matches",
                    "condition": {"field": "payload.amount", "operator": "<", "value": 0},
                    "destination": "never-queue"
                }
            ]
        }
        
        processor = GatewayProcessor(config, queue_config)
        result = processor.process(sample_message, mock_context)
        
        assert result.success
        assert len(result.output_handlers) == 0
    
    def test_duplicate_destination_prevention(self, queue_config, sample_message, mock_context):
        """Test that duplicate destinations are prevented."""
        config = {
            "rules": [
                {
                    "name": "rule1",
                    "condition": {"field": "payload.amount", "operator": ">", "value": 1000},
                    "destination": "same-queue"
                },
                {
                    "name": "rule2",
                    "condition": {"field": "payload.shipping", "operator": "==", "value": "express"},
                    "destination": "same-queue"
                }
            ]
        }
        
        processor = GatewayProcessor(config, queue_config)
        result = processor.process(sample_message, mock_context)
        
        assert result.success
        assert len(result.output_handlers) == 1  # Should only have one handler for same-queue
        assert result.output_handlers[0].destination == "same-queue"
    
    def test_error_handling_in_condition(self, queue_config, sample_message, mock_context):
        """Test error handling when condition evaluation fails."""
        config = {
            "rules": [
                {
                    "name": "bad_comparison",
                    "condition": {"field": "payload.amount", "operator": ">", "value": "not_a_number"},
                    "destination": "error-queue"
                },
                {
                    "name": "good_rule",
                    "condition": {"field": "payload.shipping", "operator": "==", "value": "express"},
                    "destination": "good-queue"
                }
            ]
        }
        
        processor = GatewayProcessor(config, queue_config)
        result = processor.process(sample_message, mock_context)
        
        # Should continue processing despite error
        assert result.success
        assert len(result.output_handlers) == 1
        assert result.output_handlers[0].destination == "good-queue"
    
    def test_processor_info(self, basic_routing_config, queue_config):
        """Test processor info generation."""
        processor = GatewayProcessor(basic_routing_config, queue_config)
        info = processor.get_processor_info()
        
        assert info["name"] == "GatewayProcessor"
        assert info["type"] == "gateway"
        assert info["rule_count"] == 2
        assert info["default_destination"] == "standard-queue"
        assert info["has_queue_config"] is True
    
    def test_validate_message(self, basic_routing_config, sample_message):
        """Test message validation (always true for gateway)."""
        processor = GatewayProcessor(basic_routing_config)
        assert processor.validate_message(sample_message) is True
    
    def test_can_retry(self, basic_routing_config):
        """Test retry logic (always false for gateway)."""
        processor = GatewayProcessor(basic_routing_config)
        assert processor.can_retry(Exception("test")) is False