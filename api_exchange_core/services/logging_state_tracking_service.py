"""
Hybrid state tracking service.

Records state transitions as both structured log events and database records.
Logs provide immediate monitoring via Loki/ELK, while database records enable
efficient querying for GUI and analytics.
All inputs are validated using Pydantic schemas.
"""

import uuid
from datetime import UTC, datetime
from typing import Optional

from sqlalchemy.exc import IntegrityError

from ..context.operation_context import operation
from ..context.tenant_context import tenant_aware
from ..db import PipelineStateHistory
from ..schemas import PipelineStateTransitionCreate, PipelineStateTransitionRead
from ..utils.logger import get_logger


class LoggingStateTrackingService:
    """
    Hybrid state tracking service.

    Records state transitions as both structured log events and database records.
    Logs provide immediate monitoring via Loki/ELK, while database records enable
    efficient querying for GUI and analytics.
    """

    def __init__(self):
        """Initialize the hybrid state tracking service with global database manager."""
        from ..db.db_config import get_db_manager

        self.logger = get_logger()
        self.db_manager = get_db_manager()

    @tenant_aware
    @operation(name="log_state_transition")
    def record_transition(
        self, transition_data: PipelineStateTransitionCreate
    ) -> PipelineStateTransitionRead:
        """
        Record a state transition as a structured log event.

        Args:
            transition_data: PipelineStateTransitionCreate with validated transition data

        Returns:
            PipelineStateTransitionRead with the recorded transition information
        """
        # Generate ID for the transition
        transition_id = str(uuid.uuid4())

        # Get tenant context
        from ..context.tenant_context import TenantContext

        tenant_id = TenantContext.get_current_tenant_id()

        # Remove correlation_id - use entity_id for tracing instead
        correlation_id = None
        timestamp = datetime.now(UTC)

        # Create structured log event
        log_data = {
            "event_type": "state_transition",
            "transition_id": transition_id,
            "entity_id": transition_data.entity_id,
            "external_id": transition_data.external_id,
            "tenant_id": tenant_id,
            "correlation_id": correlation_id,
            "pipeline_id": transition_data.pipeline_id,
            "from_state": transition_data.from_state,
            "to_state": transition_data.to_state,
            "actor": transition_data.actor,
            "transition_type": transition_data.transition_type,
            "timestamp": timestamp.isoformat(),
            "processor_data": transition_data.processor_data,
            "queue_source": transition_data.queue_source,
            "queue_destination": transition_data.queue_destination,
            "notes": transition_data.notes,
            "transition_duration_ms": transition_data.transition_duration,
        }

        # Remove None values to keep logs clean
        log_data = {k: v for k, v in log_data.items() if v is not None}

        # Log the state transition event
        self.logger.info(
            f"State transition: {transition_data.from_state} → {transition_data.to_state}",
            extra=log_data,
        )

        # Also write to database
        self._write_to_database(
            transition_id=transition_id,
            entity_id=transition_data.entity_id,
            external_id=transition_data.external_id,
            pipeline_id=transition_data.pipeline_id,
            tenant_id=tenant_id,
            processor_name=transition_data.actor,
            status=transition_data.to_state,
            log_timestamp=timestamp,
            source_queue=transition_data.queue_source,
            destination_queue=transition_data.queue_destination,
            processing_duration_ms=transition_data.transition_duration,
            processor_data=transition_data.processor_data,
            transition_type=transition_data.transition_type,
            notes=transition_data.notes,
        )

        # Return properly typed response
        return PipelineStateTransitionRead(
            transition_id=transition_id,
            entity_id=transition_data.entity_id,
            tenant_id=tenant_id,
            correlation_id=correlation_id,
            pipeline_id=transition_data.pipeline_id,
            from_state=transition_data.from_state,
            to_state=transition_data.to_state,
            actor=transition_data.actor,
            transition_type=transition_data.transition_type,
            timestamp=timestamp,
            external_id=transition_data.external_id,
            queue_source=transition_data.queue_source,
            queue_destination=transition_data.queue_destination,
            notes=transition_data.notes,
            transition_duration=transition_data.transition_duration,
            processor_data=transition_data.processor_data,
        )

    def _write_to_database(
        self,
        transition_id: str,
        entity_id: str,
        tenant_id: str,
        processor_name: str,
        status: str,
        log_timestamp: datetime,
        external_id: Optional[str] = None,
        pipeline_id: Optional[str] = None,
        source_queue: Optional[str] = None,
        destination_queue: Optional[str] = None,
        processing_duration_ms: Optional[int] = None,
        processor_data: Optional[dict] = None,
        transition_type: Optional[str] = None,
        notes: Optional[str] = None,
    ) -> None:
        """
        Write state transition to database.

        This method creates a PipelineStateHistory record in addition to logging.
        Failures are logged but don't interrupt processing to maintain reliability.
        """
        try:
            # Map transition types to status values for the GUI
            status_mapping = {
                "NORMAL": status,
                "ERROR": "FAILED",
                "RECOVERY": "RETRYING",
                "MANUAL": status,
                "TIMEOUT": "FAILED",
                "RETRY": "RETRYING",
            }

            mapped_status = status_mapping.get(transition_type, status)

            # Create state history record
            state_record = PipelineStateHistory.create(
                tenant_id=tenant_id,
                processor_name=processor_name,
                status=mapped_status,
                log_timestamp=log_timestamp,
                entity_id=entity_id,
                external_id=external_id,
                pipeline_id=pipeline_id,
                source_queue=source_queue,
                destination_queue=destination_queue,
                processing_duration_ms=processing_duration_ms,
                error_message=notes if mapped_status == "FAILED" else None,
            )

            # Use the transition_id as the record ID for consistency
            state_record.id = transition_id

            # Save to database
            session = self.db_manager.get_session()
            try:
                session.add(state_record)
                session.commit()
                self.logger.debug(
                    f"Wrote state record to database: {entity_id} | {processor_name} | {mapped_status}"
                )

            except IntegrityError:
                session.rollback()
                self.logger.debug(
                    f"Duplicate state record, skipping database write: {transition_id}"
                )

            finally:
                session.close()

        except Exception as e:
            # Log but don't raise - database issues shouldn't break processing
            self.logger.warning(f"Failed to write state to database: {e}", exc_info=True)

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
            extra={"entity_id": entity_id, "suggestion": "query logs with entity_id filter"},
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
            extra={"entity_id": entity_id, "suggestion": "query logs for latest state transition"},
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
            extra={"state": state, "suggestion": "query logs for entities in specific state"},
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
                "suggestion": "query logs for entities stuck in state longer than threshold",
            },
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
                "suggestion": "use Grafana dashboards for state statistics",
            },
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
                "suggestion": "use Grafana dashboards for processing time analysis",
            },
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
