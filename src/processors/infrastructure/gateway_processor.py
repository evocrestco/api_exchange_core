"""
Generic Gateway Processor - Routes messages to different queues based on message content.

This processor inspects incoming messages for routing variables and forwards them
to appropriate output queues. It's designed to be extended by concrete implementations
that define specific routing rules.

Example usage:
    class OrderGatewayProcessor(GatewayProcessor):
        def __init__(self):
            routing_config = {
                "rules": [
                    {
                        "name": "high_value_orders",
                        "condition": {"field": "payload.order_value", "operator": ">", "value": 1000},
                        "destination": "high-value-order-queue"
                    },
                    {
                        "name": "express_orders",
                        "condition": {"field": "payload.shipping_type", "operator": "==", "value": "express"},
                        "destination": "express-order-queue"
                    }
                ],
                "default_destination": "standard-order-queue"
            }
            super().__init__(routing_config)
"""

import operator
from datetime import UTC, datetime
from typing import Any, Dict, List, Optional, Union

from src.processors.processing_result import ProcessingResult
from src.processors.v2.message import Message
from src.processors.v2.output_handlers.queue_output import QueueOutputHandler
from src.processors.v2.processor_interface import ProcessorContext, ProcessorInterface
from src.utils.logger import get_logger


class GatewayProcessor(ProcessorInterface):
    """
    Generic gateway processor that routes messages based on configurable rules.

    Configuration:
        routing_config: {
            "rules": [
                {
                    "name": "rule_name",
                    "condition": {
                        "field": "path.to.field",
                        "operator": "==|!=|>|<|>=|<=|in|not_in|contains|matches",
                        "value": <comparison_value>
                    },
                    "destination": "target-queue-name",
                    "stop_on_match": false  # Optional, default false
                }
            ],
            "default_destination": "default-queue",  # Optional
            "queue_config": {  # Optional, defaults for all queues
                "connection_string": "...",
                "auto_create_queue": true
            }
        }
    """

    # Supported operators
    OPERATORS = {
        "==": operator.eq,
        "!=": operator.ne,
        ">": operator.gt,
        "<": operator.lt,
        ">=": operator.ge,
        "<=": operator.le,
        "in": lambda x, y: x in y,
        "not_in": lambda x, y: x not in y,
        "contains": lambda x, y: y in x if isinstance(x, (str, list, dict)) else False,
        "matches": lambda x, y: bool(re.match(y, str(x))) if "re" in globals() else False,
    }

    def __init__(
        self, routing_config: Dict[str, Any], queue_config: Optional[Dict[str, Any]] = None
    ):
        """
        Initialize the gateway processor with routing configuration.

        Args:
            routing_config: Dictionary containing routing rules and configuration
            queue_config: Optional default configuration for queue output handlers
        """
        super().__init__()
        self.routing_config = routing_config
        self.rules = routing_config.get("rules", [])
        self.default_destination = routing_config.get("default_destination")
        self.queue_config = queue_config or routing_config.get("queue_config", {})
        self.logger = get_logger(__name__)

        # Import regex if needed for matches operator
        if any(rule.get("condition", {}).get("operator") == "matches" for rule in self.rules):
            global re
            import re

    def process(self, message: Message, context: ProcessorContext) -> ProcessingResult:
        """
        Process the message and route to appropriate queue(s) based on rules.

        Args:
            message: Incoming message to route
            context: Processor context with service access

        Returns:
            ProcessingResult with configured output handlers
        """
        start_time = datetime.now(UTC)
        result = ProcessingResult.create_success()

        # Track routing decisions
        routing_metadata = {"evaluated_rules": [], "matched_rules": [], "destinations": []}

        # Evaluate each rule
        matched_any = False
        for rule in self.rules:
            rule_name = rule.get("name", "unnamed")

            try:
                # Evaluate condition
                if self._evaluate_condition(message, rule.get("condition", {})):
                    matched_any = True
                    destination = rule["destination"]

                    routing_metadata["matched_rules"].append(rule_name)
                    routing_metadata["destinations"].append(destination)

                    # Add output handler for this destination
                    self._add_output_handler(result, destination)

                    self.logger.debug(
                        f"Rule '{rule_name}' matched, routing to {destination}",
                        extra={
                            "rule_name": rule_name,
                            "destination": destination,
                            "message_id": message.message_id,
                        },
                    )

                    # Stop processing further rules if configured
                    if rule.get("stop_on_match", False):
                        break

                routing_metadata["evaluated_rules"].append(rule_name)

            except Exception as e:
                self.logger.warning(
                    f"Error evaluating rule '{rule_name}': {str(e)}",
                    extra={
                        "rule_name": rule_name,
                        "error": str(e),
                        "message_id": message.message_id,
                    },
                )

        # Use default destination if no rules matched
        if not matched_any and self.default_destination:
            routing_metadata["destinations"].append(self.default_destination)
            self._add_output_handler(result, self.default_destination)

            self.logger.debug(
                f"No rules matched, using default destination: {self.default_destination}",
                extra={"destination": self.default_destination, "message_id": message.message_id},
            )

        # Add routing metadata to result
        result.add_metadata("routing", routing_metadata)
        result.add_metadata(
            "processing_time_ms", (datetime.now(UTC) - start_time).total_seconds() * 1000
        )

        # Log routing summary
        self.logger.info(
            f"Gateway routing completed: {len(routing_metadata['destinations'])} destination(s)",
            extra={
                "message_id": message.message_id,
                "evaluated_rules": len(routing_metadata["evaluated_rules"]),
                "matched_rules": len(routing_metadata["matched_rules"]),
                "destinations": routing_metadata["destinations"],
            },
        )

        return result

    def _evaluate_condition(self, message: Message, condition: Dict[str, Any]) -> bool:
        """
        Evaluate a routing condition against the message.

        Args:
            message: Message to evaluate
            condition: Condition dictionary with field, operator, and value

        Returns:
            True if condition matches, False otherwise
        """
        if not condition:
            return True  # Empty condition always matches

        field_path = condition.get("field", "")
        operator_name = condition.get("operator", "==")
        expected_value = condition.get("value")

        # Get the actual value from the message
        actual_value = self._get_field_value(message, field_path)

        # Get the operator function
        op_func = self.OPERATORS.get(operator_name)
        if not op_func:
            self.logger.warning(f"Unknown operator: {operator_name}")
            return False

        try:
            # Evaluate the condition
            return op_func(actual_value, expected_value)
        except Exception as e:
            self.logger.debug(
                f"Condition evaluation failed: {str(e)}",
                extra={
                    "field": field_path,
                    "operator": operator_name,
                    "actual_value": actual_value,
                    "expected_value": expected_value,
                },
            )
            return False

    def _get_field_value(self, message: Message, field_path: str) -> Any:
        """
        Extract a value from the message using dot notation path.

        Args:
            message: Message to extract from
            field_path: Dot-separated path to the field (e.g., "payload.order.total")

        Returns:
            The value at the specified path, or None if not found
        """
        if not field_path:
            return None

        # Start with the message object
        current = message

        # Navigate through the path
        for part in field_path.split("."):
            if isinstance(current, dict) and part in current:
                current = current[part]
            elif hasattr(current, part):
                current = getattr(current, part)
            elif isinstance(current, list) and part.isdigit() and int(part) < len(current):
                current = current[int(part)]
            else:
                return None

        return current

    def _add_output_handler(self, result: ProcessingResult, destination: str) -> None:
        """
        Add a queue output handler to the result.

        Args:
            result: Processing result to add handler to
            destination: Queue name to route to
        """
        # Check if we already have a handler for this destination
        for handler in result.output_handlers:
            if hasattr(handler, "destination") and handler.destination == destination:
                return  # Already have a handler for this destination

        # Create and add new handler
        handler = QueueOutputHandler(destination=destination, config=self.queue_config)
        result.add_output_handler(handler)

    def get_processor_info(self) -> Dict[str, Any]:
        """Get processor information."""
        info = super().get_processor_info()
        info.update(
            {
                "type": "gateway",
                "rule_count": len(self.rules),
                "default_destination": self.default_destination,
                "has_queue_config": bool(self.queue_config),
            }
        )
        return info

    def validate_message(self, message: Message) -> bool:
        """Validate that the message can be processed."""
        # Gateway processor accepts all messages
        return True

    def can_retry(self, error: Exception) -> bool:
        """Determine if the processor should retry after an error."""
        # Gateway routing errors are typically not retryable
        return False
