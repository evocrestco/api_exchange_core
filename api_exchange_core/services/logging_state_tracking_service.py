"""
Logging-based state tracking service.

Replaces database-based StateTrackingService with structured logging that can be
consumed by Loki/ELK for monitoring and analysis. Much simpler and more reliable.
"""

import uuid
from datetime import datetime, UTC
from typing import Union

from ..context.operation_context import operation
from ..context.tenant_context import tenant_aware
from ..db import EntityStateEnum
from ..enums import TransitionTypeEnum
from ..utils.logger import get_logger
from ..type_definitions import ProcessorData


class LoggingStateTrackingService:
    """
    Logging-based state tracking service.
    
    Records state transitions as structured log events instead of database records.
    Can be consumed by Loki/ELK for monitoring, alerting, and analysis.
    """

    def __init__(self):
        """Initialize the logging state tracking service."""
        self.logger = get_logger()

    @tenant_aware
    @operation(name="log_state_transition")
    def record_transition(
        self,
        entity_id: str,
        from_state: Union[str, EntityStateEnum],
        to_state: Union[str, EntityStateEnum],
        actor: str,
        transition_type: Union[str, TransitionTypeEnum] = TransitionTypeEnum.NORMAL,
        processor_data: ProcessorData = None,
        queue_source: str = None,
        queue_destination: str = None,
        notes: str = None,
        transition_duration: int = None,
        external_id: str = None,
    ) -> str:
        """
        Record a state transition as a structured log event.

        Args:
            entity_id: ID of the entity
            from_state: Previous state
            to_state: New state
            actor: Actor (processor or user) making the transition
            transition_type: Type of transition (NORMAL, ERROR, etc.)
            processor_data: Additional data related to the transition
            queue_source: Queue from which the message was received
            queue_destination: Queue to which the message was sent
            notes: Additional notes about the transition
            transition_duration: Duration in ms of the previous state

        Returns:
            ID of the logged state transition (for compatibility)
        """
        # Convert enum values to strings if needed
        from_state_val = from_state.value if hasattr(from_state, "value") else from_state
        to_state_val = to_state.value if hasattr(to_state, "value") else to_state
        transition_type_val = (
            transition_type.value
            if hasattr(transition_type, "value")
            else transition_type
        )
        
        # Generate ID for compatibility with existing code
        transition_id = str(uuid.uuid4())
        
        # Get tenant context
        from ..context.tenant_context import TenantContext
        tenant_id = TenantContext.get_current_tenant_id()
        
        # Get correlation ID from context
        from ..exceptions import get_correlation_id
        correlation_id = get_correlation_id()
        
        # Create structured log event
        log_data = {
            "event_type": "state_transition",
            "transition_id": transition_id,
            "entity_id": entity_id,
            "external_id": external_id,
            "tenant_id": tenant_id,
            "correlation_id": correlation_id,
            "from_state": from_state_val,
            "to_state": to_state_val,
            "actor": actor,
            "transition_type": transition_type_val,
            "timestamp": datetime.now(UTC).isoformat(),
            "processor_data": processor_data,
            "queue_source": queue_source,
            "queue_destination": queue_destination,
            "notes": notes,
            "transition_duration_ms": transition_duration,
        }
        
        # Remove None values to keep logs clean
        log_data = {k: v for k, v in log_data.items() if v is not None}
        
        # Log the state transition event
        self.logger.info(
            f"State transition: {from_state_val} → {to_state_val}",
            extra=log_data
        )
        
        return transition_id

    def get_entity_state_history(self, entity_id: str):
        """
        Get entity state history (not supported in logging mode).
        
        Args:
            entity_id: ID of the entity
            
        Returns:
            None - use Loki/ELK queries instead
        """
        self.logger.warning(
            "get_entity_state_history not supported in logging mode - use Loki/ELK queries",
            extra={"entity_id": entity_id, "suggestion": "query logs with entity_id filter"}
        )
        return None

    def get_current_state(self, entity_id: str):
        """
        Get current state (not supported in logging mode).
        
        Args:
            entity_id: ID of the entity
            
        Returns:
            None - use Loki/ELK queries instead
        """
        self.logger.warning(
            "get_current_state not supported in logging mode - use Loki/ELK queries",
            extra={"entity_id": entity_id, "suggestion": "query logs for latest state transition"}
        )
        return None

    def get_entities_in_state(self, state, limit=None, offset=None):
        """
        Get entities in state (not supported in logging mode).
        
        Returns:
            Empty list - use Loki/ELK queries instead
        """
        self.logger.warning(
            "get_entities_in_state not supported in logging mode - use Loki/ELK queries",
            extra={"state": state, "suggestion": "query logs for entities in specific state"}
        )
        return []

    def get_stuck_entities(self, state, threshold_minutes, limit=None):
        """
        Get stuck entities (not supported in logging mode).
        
        Returns:
            Empty list - use Loki/ELK queries instead
        """
        self.logger.warning(
            "get_stuck_entities not supported in logging mode - use Loki/ELK queries",
            extra={
                "state": state,
                "threshold_minutes": threshold_minutes,
                "suggestion": "query logs for entities stuck in state longer than threshold"
            }
        )
        return []

    def get_state_statistics(self, start_time=None, end_time=None):
        """
        Get state statistics (not supported in logging mode).
        
        Returns:
            None - use Loki/ELK queries instead
        """
        self.logger.warning(
            "get_state_statistics not supported in logging mode - use Loki/ELK queries",
            extra={
                "start_time": start_time,
                "end_time": end_time,
                "suggestion": "use Grafana dashboards for state statistics"
            }
        )
        return None

    def calculate_avg_processing_time(self, start_state, end_state):
        """
        Calculate average processing time (not supported in logging mode).
        
        Returns:
            None - use Loki/ELK queries instead
        """
        self.logger.warning(
            "calculate_avg_processing_time not supported in logging mode - use Loki/ELK queries",
            extra={
                "start_state": start_state,
                "end_state": end_state,
                "suggestion": "use Grafana dashboards for processing time analysis"
            }
        )
        return None

    def update_message_with_state(self, message, state):
        """
        Update message with state information.
        
        This method still works as it just modifies the message object.
        """
        state_val = state.value if hasattr(state, "value") else state
        
        # Create a copy of the message to avoid modifying original
        updated_message = message.copy()
        
        # Preserve previous state if it exists
        previous_state = updated_message.get("state")
        
        # Update the state field directly
        updated_message["state"] = state_val
        
        # Add state tracking metadata
        if "metadata" not in updated_message:
            updated_message["metadata"] = {}
        else:
            # Make a copy of metadata to avoid modifying original
            updated_message["metadata"] = updated_message["metadata"].copy()
        
        updated_message["metadata"]["current_state"] = state_val
        updated_message["metadata"]["state_updated_at"] = datetime.now(UTC).isoformat()
        updated_message["metadata"]["state_changed_at"] = datetime.now(UTC).isoformat()
        
        # Add previous state if it existed
        if previous_state is not None:
            updated_message["metadata"]["previous_state"] = previous_state
        
        return updated_message