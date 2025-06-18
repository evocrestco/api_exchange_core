import uuid
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Union

from sqlalchemy import and_, desc, exists, func
from sqlalchemy.exc import IntegrityError

from src.context.operation_context import operation
from src.context.tenant_context import tenant_aware
from src.db.db_base import EntityStateEnum
from src.db.db_entity_models import Entity
from src.db.db_state_transition_models import StateTransition, TransitionTypeEnum
from src.db.db_tenant_models import Tenant
from src.exceptions import ErrorCode, ServiceError, ValidationError
from src.schemas.state_transition_schema import (
    EntityStateHistory,
    StateTransitionCreate,
    StateTransitionFilter,
    StateTransitionRead,
    StateTransitionStats,
    StateTransitionUpdate,
)
from src.services.base_service import SessionManagedService
from src.type_definitions import MessageDict, ProcessorData


class StateTrackingService(SessionManagedService):
    """
    Pythonic service for managing entity state transitions with direct SQLAlchemy access.

    This service handles state transition recording, retrieval, and analysis operations
    using SQLAlchemy directly - simple, explicit, and efficient.
    """

    def __init__(self, session=None, logger=None):
        """
        Initialize the service with its own session.

        Args:
            session: Optional existing session (for testing or coordination)
            logger: Optional logger instance
        """
        super().__init__(session=session, logger=logger)

    @tenant_aware
    @operation(name="record_state_transition")
    def record_transition(
        self,
        entity_id: str,
        from_state: Union[str, EntityStateEnum],
        to_state: Union[str, EntityStateEnum],
        actor: str,
        transition_type: Union[str, TransitionTypeEnum] = TransitionTypeEnum.NORMAL,
        processor_data: Optional[ProcessorData] = None,
        queue_source: Optional[str] = None,
        queue_destination: Optional[str] = None,
        notes: Optional[str] = None,
        transition_duration: Optional[int] = None,
    ) -> str:
        """
        Record a state transition for an entity.

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
            ID of the created state transition

        Raises:
            ServiceError: If the transition cannot be recorded
        """
        try:
            tenant_id = self._get_current_tenant_id()
            
            # Skip entity validation - ProcessingService already validated the entity
            # This avoids session conflicts when called within another service's transaction
            
            # Convert enum values to strings if needed
            from_state_val = from_state.value if hasattr(from_state, "value") else from_state
            to_state_val = to_state.value if hasattr(to_state, "value") else to_state
            transition_type_val = (
                transition_type
                if isinstance(transition_type, TransitionTypeEnum)
                else TransitionTypeEnum(transition_type)
            )
            
            # Use timestamp-based sequence to avoid database queries during transactions
            # This prevents session conflicts when called within another service's transaction
            import time
            sequence_number = int(time.time() * 1000000)  # microseconds since epoch - fits in BIGINT
            
            # Create state transition directly using SQLAlchemy
            state_transition = StateTransition(
                id=str(uuid.uuid4()),
                entity_id=entity_id,
                from_state=from_state_val,
                to_state=to_state_val,
                actor=actor,
                transition_type=transition_type_val,
                processor_data=processor_data,
                queue_source=queue_source,
                queue_destination=queue_destination,
                notes=notes,
                transition_duration=transition_duration,
                sequence_number=sequence_number,
                tenant_id=tenant_id,
                created_at=datetime.utcnow(),
                updated_at=datetime.utcnow(),
            )

            self.session.add(state_transition)
            # Don't commit - let the calling service manage the transaction
            
            self.logger.info(
                f"Recorded state transition: id={state_transition.id}, entity={entity_id}, "
                f"{from_state_val} -> {to_state_val}, actor={actor}"
            )

            return state_transition.id
            
        except IntegrityError as e:
            # Don't rollback - let the calling service handle transaction management
            # Check if it's a foreign key constraint (entity validation)
            if "foreign key constraint" in str(e).lower():
                raise ServiceError(
                    f"Invalid entity: {entity_id}",
                    error_code=ErrorCode.CONSTRAINT_VIOLATION,
                    operation="record_transition",
                    entity_id=entity_id,
                    tenant_id=tenant_id,
                    cause=e,
                ) from e
            else:
                raise ServiceError(
                    f"State transition recording failed due to data integrity constraints",
                    error_code=ErrorCode.INVALID_DATA,
                    operation="record_transition",
                    entity_id=entity_id,
                    tenant_id=tenant_id,
                    cause=e,
                ) from e
        except Exception as e:
            # Don't rollback - let the calling service handle transaction management
            if isinstance(e, ServiceError):
                raise
            self._handle_service_exception("record_transition", e, entity_id)

    @tenant_aware
    @operation(name="get_entity_state_history")
    def get_entity_state_history(self, entity_id: str) -> Optional[EntityStateHistory]:
        """
        Get the complete state history for an entity.

        Args:
            entity_id: ID of the entity

        Returns:
            EntityStateHistory with state transition history or None if no history exists

        Raises:
            ServiceError: If the state history cannot be retrieved
        """
        try:
            tenant_id = self._get_current_tenant_id()
            
            # Get all transitions for this entity
            transitions = (
                self.session.query(StateTransition)
                .filter(
                    StateTransition.entity_id == entity_id,
                    StateTransition.tenant_id == tenant_id,
                )
                .order_by(StateTransition.sequence_number)
                .all()
            )
            
            if not transitions:
                return None
                
            # Convert to StateTransitionRead objects
            transition_reads = [StateTransitionRead.model_validate(t) for t in transitions]
            
            # Get current state (latest transition's to_state)
            current_state = transitions[-1].to_state
            
            # Calculate required fields for schema
            first_seen = transitions[0].created_at
            last_updated = transitions[-1].created_at
            
            # Calculate total processing time from transition durations
            total_processing_time = sum(
                t.transition_duration or 0 for t in transitions if t.transition_duration
            )
            
            return EntityStateHistory(
                entity_id=entity_id,
                current_state=current_state,
                transitions=transition_reads,
                total_transitions=len(transitions),
                first_seen=first_seen,
                last_updated=last_updated,
                total_processing_time=total_processing_time,
            )
            
        except Exception as e:
            if isinstance(e, ServiceError):
                raise
            self._handle_service_exception("get_entity_state_history", e, entity_id)

    @tenant_aware
    @operation(name="get_current_state")
    def get_current_state(self, entity_id: str) -> Optional[str]:
        """
        Get the current state of an entity.

        Args:
            entity_id: ID of the entity

        Returns:
            Current state string or None if entity not found

        Raises:
            ServiceError: If the current state cannot be determined
        """
        history = self.get_entity_state_history(entity_id)
        if not history:
            return None

        return history.current_state

    @tenant_aware
    @operation(name="get_entities_in_state")
    def get_entities_in_state(
        self, 
        state: Union[str, EntityStateEnum], 
        limit: Optional[int] = None, 
        offset: Optional[int] = None
    ) -> List[str]:
        """
        Get list of entity IDs currently in the specified state.

        Args:
            state: State to search for
            limit: Maximum number of entities to return
            offset: Number of entities to skip

        Returns:
            List of entity IDs currently in the specified state
        """
        try:
            tenant_id = self._get_current_tenant_id()
            state_val = state.value if hasattr(state, "value") else state
            
            # Subquery to get the latest transition for each entity
            latest_transitions = (
                self.session.query(
                    StateTransition.entity_id,
                    func.max(StateTransition.sequence_number).label('max_seq')
                )
                .filter(StateTransition.tenant_id == tenant_id)
                .group_by(StateTransition.entity_id)
                .subquery()
            )
            
            # Get entities where latest transition's to_state matches
            query = (
                self.session.query(StateTransition.entity_id)
                .join(
                    latest_transitions,
                    and_(
                        StateTransition.entity_id == latest_transitions.c.entity_id,
                        StateTransition.sequence_number == latest_transitions.c.max_seq
                    )
                )
                .filter(
                    StateTransition.to_state == state_val,
                    StateTransition.tenant_id == tenant_id
                )
            )
            
            if offset:
                query = query.offset(offset)
            if limit:
                query = query.limit(limit)
            
            return [result[0] for result in query.all()]
            
        except Exception as e:
            self._handle_service_exception("get_entities_in_state", e)

    @tenant_aware
    @operation(name="get_stuck_entities")
    def get_stuck_entities(
        self, 
        state: Union[str, EntityStateEnum], 
        threshold_minutes: int, 
        limit: Optional[int] = None
    ) -> List[str]:
        """
        Get entities stuck in a state longer than threshold.

        Args:
            state: State to check for stuck entities
            threshold_minutes: Minimum minutes to consider stuck
            limit: Maximum number of entities to return

        Returns:
            List of entity IDs stuck in the state
        """
        try:
            tenant_id = self._get_current_tenant_id()
            state_val = state.value if hasattr(state, "value") else state
            threshold_time = datetime.utcnow() - timedelta(minutes=threshold_minutes)
            
            # Subquery to get the latest transition for each entity
            latest_transitions = (
                self.session.query(
                    StateTransition.entity_id,
                    func.max(StateTransition.sequence_number).label('max_seq')
                )
                .filter(StateTransition.tenant_id == tenant_id)
                .group_by(StateTransition.entity_id)
                .subquery()
            )
            
            # Get entities where latest transition's to_state matches and is older than threshold
            query = (
                self.session.query(StateTransition.entity_id)
                .join(
                    latest_transitions,
                    and_(
                        StateTransition.entity_id == latest_transitions.c.entity_id,
                        StateTransition.sequence_number == latest_transitions.c.max_seq
                    )
                )
                .filter(
                    StateTransition.to_state == state_val,
                    StateTransition.tenant_id == tenant_id,
                    StateTransition.created_at < threshold_time
                )
            )
            
            if limit:
                query = query.limit(limit)
            
            return [result[0] for result in query.all()]
            
        except Exception as e:
            self._handle_service_exception("get_stuck_entities", e)

    @tenant_aware
    @operation(name="get_state_statistics")
    def get_state_statistics(
        self, 
        start_time: Optional[datetime] = None, 
        end_time: Optional[datetime] = None
    ) -> StateTransitionStats:
        """
        Get statistics about state transitions.

        Args:
            start_time: Start time for the statistics period
            end_time: End time for the statistics period

        Returns:
            StateTransitionStats with transition statistics
        """
        try:
            tenant_id = self._get_current_tenant_id()
            
            query = self.session.query(StateTransition).filter(
                StateTransition.tenant_id == tenant_id
            )
            
            if start_time:
                query = query.filter(StateTransition.created_at >= start_time)
            if end_time:
                query = query.filter(StateTransition.created_at <= end_time)
            
            transitions = query.all()
            
            if not transitions:
                return StateTransitionStats(
                    total_transitions=0,
                    transitions_by_state={},
                    avg_duration_by_state={},
                    error_rate=0.0,
                    most_common_error_states=[]
                )
            
            # Calculate statistics
            total_transitions = len(transitions)
            
            # Count transitions by state
            transitions_by_state = {}
            for t in transitions:
                transitions_by_state[t.to_state] = transitions_by_state.get(t.to_state, 0) + 1
            
            # Calculate average duration by state
            duration_by_state = {}
            count_by_state = {}
            for t in transitions:
                if t.transition_duration:
                    if t.from_state not in duration_by_state:
                        duration_by_state[t.from_state] = 0
                        count_by_state[t.from_state] = 0
                    duration_by_state[t.from_state] += t.transition_duration
                    count_by_state[t.from_state] += 1
            
            avg_duration_by_state = {
                state: duration_by_state[state] / count_by_state[state]
                for state in duration_by_state
                if count_by_state[state] > 0
            }
            
            # Calculate error rate
            error_transitions = [t for t in transitions if t.transition_type == TransitionTypeEnum.ERROR]
            error_rate = len(error_transitions) / total_transitions if total_transitions > 0 else 0.0
            
            # Get most common error states
            error_states = {}
            for t in error_transitions:
                error_states[t.to_state] = error_states.get(t.to_state, 0) + 1
            
            most_common_error_states = sorted(error_states.keys(), key=lambda k: error_states[k], reverse=True)[:5]
            
            return StateTransitionStats(
                total_transitions=total_transitions,
                transitions_by_state=transitions_by_state,
                avg_duration_by_state=avg_duration_by_state,
                error_rate=error_rate,
                most_common_error_states=most_common_error_states
            )
            
        except Exception as e:
            self._handle_service_exception("get_state_statistics", e)

    @tenant_aware
    @operation(name="calculate_avg_processing_time")
    def calculate_avg_processing_time(
        self, 
        start_state: Union[str, EntityStateEnum], 
        end_state: Union[str, EntityStateEnum]
    ) -> Optional[int]:
        """
        Calculate average processing time between two states in ms.

        Args:
            start_state: Starting state
            end_state: Ending state

        Returns:
            Average processing time in milliseconds or None if no data
        """
        try:
            tenant_id = self._get_current_tenant_id()
            start_state_val = start_state.value if hasattr(start_state, "value") else start_state
            end_state_val = end_state.value if hasattr(end_state, "value") else end_state
            
            # Get all transitions from start_state to end_state
            transitions = (
                self.session.query(StateTransition)
                .filter(
                    StateTransition.tenant_id == tenant_id,
                    StateTransition.from_state == start_state_val,
                    StateTransition.to_state == end_state_val,
                    StateTransition.transition_duration.is_not(None)
                )
                .all()
            )
            
            if not transitions:
                return None  # Return None for no data case (including invalid states)
            
            total_duration = sum(t.transition_duration for t in transitions if t.transition_duration)
            if total_duration == 0:
                return 0.0
                
            return float(total_duration) / len(transitions)  # Use float division
            
        except Exception as e:
            self._handle_service_exception("calculate_avg_processing_time", e)

    @operation(name="update_message_with_state")
    def update_message_with_state(
        self, 
        message: MessageDict, 
        state: Union[str, EntityStateEnum]
    ) -> MessageDict:
        """
        Update message with state information and metadata.

        Args:
            message: Original message dictionary
            state: Current state to add to message

        Returns:
            Updated message dictionary with state metadata
        """
        try:
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
            updated_message["metadata"]["state_updated_at"] = datetime.utcnow().isoformat()
            updated_message["metadata"]["state_changed_at"] = datetime.utcnow().isoformat()
            
            # Add previous state if it existed
            if previous_state is not None:
                updated_message["metadata"]["previous_state"] = previous_state
            
            return updated_message
            
        except Exception as e:
            self._handle_service_exception("update_message_with_state", e)