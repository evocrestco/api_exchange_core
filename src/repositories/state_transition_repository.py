"""
Repository for StateTransition operations with transaction management and tenant isolation.

This module provides data access functions for working with StateTransition objects.
"""

from datetime import datetime
from typing import List, Optional

from sqlalchemy import desc, func

from src.context.tenant_context import TenantContext
from src.db.db_state_transition_models import StateTransition, TransitionTypeEnum
from src.repositories.base_repository import BaseRepository
from src.schemas.state_transition_schema import (
    EntityStateHistory,
    StateTransitionCreate,
    StateTransitionFilter,
    StateTransitionRead,
    StateTransitionStats,
)
from src.utils.logger import get_logger


class StateTransitionRepository(BaseRepository[StateTransition]):
    """
    Repository for StateTransition CRUD operations with tenant isolation.

    This class handles data access operations for state transitions with
    built-in tenant isolation and error handling.
    """

    def __init__(self, db_manager, logger=None):
        """
        Initialize the repository with a database manager.

        Args:
            db_manager: Database manager for session handling
            logger: Optional logger instance
        """
        super().__init__(db_manager, StateTransition, logger)
        if logger is None:
            self.logger = get_logger()  # type: ignore[assignment]

    def create(self, state_transition_data: StateTransitionCreate) -> str:
        """
        Create a new state transition with proper tenant isolation.

        Args:
            state_transition_data: StateTransitionCreate schema with state transition data

        Returns:
            str: ID of the created state transition

        Raises:
            RepositoryError: If the state transition cannot be created
        """
        # Use BaseRepository helper to prepare data
        data_dict = self._prepare_create_data(state_transition_data)

        entity_id = data_dict.get("entity_id")
        if not entity_id or not isinstance(entity_id, str):
            raise ValueError("entity_id is required for state transitions")

        # Auto-assign sequence number if not provided
        if "sequence_number" not in data_dict or data_dict["sequence_number"] is None:
            with self._db_operation("get_last_sequence", entity_id) as session:
                last_sequence = self._get_last_sequence_number(entity_id, session)
                data_dict["sequence_number"] = last_sequence + 1

        # Use BaseRepository's _create method
        transition = self._create(data_dict)
        return transition.id  # type: ignore[return-value]

    def get_by_id(self, id: str) -> Optional[StateTransitionRead]:
        """
        Get a state transition by ID with tenant isolation.

        Args:
            id: ID of the state transition to retrieve

        Returns:
            StateTransitionRead or None if not found

        Raises:
            RepositoryError: If there's an error retrieving the state transition
        """
        # Use BaseRepository's _get_by_id method
        transition = self._get_by_id(id)

        if not transition:
            return None

        return StateTransitionRead.model_validate(transition)

    def get_by_filter(
        self, filter_params: StateTransitionFilter, limit: int = 100, offset: int = 0
    ) -> List[StateTransitionRead] | None:
        """
        Get state transitions by filter criteria with tenant isolation.

        Args:
            filter_params: Filter parameters for the query
            limit: Maximum number of results to return
            offset: Number of results to skip

        Returns:
            List of state transitions matching the filter

        Raises:
            RepositoryError: If there's an error filtering state transitions
        """
        with self._db_operation("get_by_filter") as session:
            tenant_id = TenantContext.get_current_tenant_id()

            query = session.query(StateTransition)

            # Apply tenant isolation
            if tenant_id:
                query = self._apply_tenant_filter(query, tenant_id)
            elif filter_params.tenant_id:
                query = query.filter(StateTransition.tenant_id == filter_params.tenant_id)

            # Build filter map using BaseRepository helper
            field_mapping = {
                "entity_id": "entity_id",
                "from_state": "from_state",
                "to_state": "to_state",
                "actor": "actor",
                "transition_type": "transition_type",
            }
            filters, filter_map = self._build_filter_map(filter_params, field_mapping)

            # Apply filters using BaseRepository helper
            query = self._apply_filters(query, filters, filter_map)

            # Apply sorting by entity_id and sequence_number
            query = query.order_by(StateTransition.entity_id, StateTransition.sequence_number.asc())

            # Apply pagination
            query = self._apply_pagination(query, limit, offset)

            transitions = query.all()
            return [StateTransitionRead.model_validate(transition) for transition in transitions]

    def get_entity_state_history(self, entity_id: str) -> Optional[EntityStateHistory]:
        """
        Get the complete state history for an entity.

        Args:
            entity_id: ID of the entity

        Returns:
            EntityStateHistory with state transition history

        Raises:
            RepositoryError: If there's an error retrieving state history
        """
        with self._db_operation("get_entity_state_history", entity_id) as session:
            tenant_id = TenantContext.get_current_tenant_id()

            query = session.query(StateTransition).filter(StateTransition.entity_id == entity_id)

            if tenant_id:
                query = self._apply_tenant_filter(query, tenant_id)

            query = query.order_by(StateTransition.sequence_number.asc())
            transitions = query.all()

            if not transitions:
                return None

            # Get first and last transitions
            first_transition = transitions[0]
            last_transition = transitions[-1]

            # Calculate total processing time
            total_time = sum(
                t.transition_duration for t in transitions if t.transition_duration is not None
            )

            transition_dicts = [
                StateTransitionRead.model_validate(transition) for transition in transitions
            ]

            return EntityStateHistory(
                entity_id=entity_id,
                current_state=last_transition.to_state,
                transitions=transition_dicts,
                total_transitions=len(transitions),
                first_seen=first_transition.created_at,
                last_updated=last_transition.created_at,
                total_processing_time=total_time,
            )

    def get_entities_in_state(self, state: str, limit: int = 100, offset: int = 0) -> List[str]:
        """
        Get entities currently in a specific state.

        Args:
            state: The state to filter by
            limit: Maximum number of results to return
            offset: Number of results to skip

        Returns:
            List of entity IDs in the specified state

        Raises:
            RepositoryError: If there's an error retrieving entities
        """
        with self._db_operation("get_entities_in_state") as session:
            tenant_id = TenantContext.get_current_tenant_id()

            # Subquery to get the latest state transition for each entity
            latest_transitions_subquery = (
                session.query(
                    StateTransition.entity_id,
                    func.max(StateTransition.sequence_number).label("max_sequence"),
                )
                .group_by(StateTransition.entity_id)
                .subquery()
            )

            # Main query to get entities in the specified state
            query = (
                session.query(StateTransition.entity_id)
                .join(
                    latest_transitions_subquery,
                    (StateTransition.entity_id == latest_transitions_subquery.c.entity_id)
                    & (
                        StateTransition.sequence_number
                        == latest_transitions_subquery.c.max_sequence
                    ),
                )
                .filter(StateTransition.to_state == state)
            )

            # Apply tenant isolation
            if tenant_id:
                query = query.filter(StateTransition.tenant_id == tenant_id)

            # Apply pagination
            query = self._apply_pagination(query, limit, offset)

            # Execute query and extract entity IDs
            result = query.all()
            return [row[0] for row in result]

    def get_state_statistics(
        self,
        tenant_id: Optional[str] = None,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
    ) -> StateTransitionStats:
        """
        Get statistics about state transitions.

        Args:
            tenant_id: Optional tenant ID to filter by
            start_time: Optional start time for the statistics period
            end_time: Optional end time for the statistics period

        Returns:
            StateTransitionStats with statistics about state transitions

        Raises:
            RepositoryError: If there's an error calculating statistics
        """
        with self._db_operation("get_state_statistics") as session:
            # If no tenant_id provided, use current tenant context
            if not tenant_id:
                tenant_id = TenantContext.get_current_tenant_id()

            # Base query
            query = session.query(StateTransition)

            # Apply filters
            if tenant_id:
                query = query.filter(StateTransition.tenant_id == tenant_id)

            if start_time:
                query = query.filter(StateTransition.created_at >= start_time)

            if end_time:
                query = query.filter(StateTransition.created_at <= end_time)

            # Total transitions
            total_transitions = query.count()

            # Transitions by state
            transitions_by_state_query = session.query(
                StateTransition.to_state, func.count(StateTransition.id).label("count")
            ).group_by(StateTransition.to_state)

            if tenant_id:
                transitions_by_state_query = transitions_by_state_query.filter(
                    StateTransition.tenant_id == tenant_id
                )

            if start_time:
                transitions_by_state_query = transitions_by_state_query.filter(
                    StateTransition.created_at >= start_time
                )

            if end_time:
                transitions_by_state_query = transitions_by_state_query.filter(
                    StateTransition.created_at <= end_time
                )

            transitions_by_state_result = transitions_by_state_query.all()
            transitions_by_state = {row.to_state: row.count for row in transitions_by_state_result}

            # Average duration by state
            duration_by_state_query = (
                session.query(
                    StateTransition.from_state,
                    func.avg(StateTransition.transition_duration).label("avg_duration"),
                )
                .filter(StateTransition.transition_duration.isnot(None))
                .group_by(StateTransition.from_state)
            )

            if tenant_id:
                duration_by_state_query = duration_by_state_query.filter(
                    StateTransition.tenant_id == tenant_id
                )

            if start_time:
                duration_by_state_query = duration_by_state_query.filter(
                    StateTransition.created_at >= start_time
                )

            if end_time:
                duration_by_state_query = duration_by_state_query.filter(
                    StateTransition.created_at <= end_time
                )

            duration_by_state_result = duration_by_state_query.all()
            avg_duration_by_state = {
                row.from_state: float(row.avg_duration) for row in duration_by_state_result
            }

            # Error rate
            error_transitions = query.filter(
                StateTransition.transition_type == TransitionTypeEnum.ERROR
            ).count()

            error_rate = error_transitions / total_transitions if total_transitions > 0 else 0

            # Most common error states
            error_states_query = session.query(
                StateTransition.to_state, func.count(StateTransition.id).label("count")
            ).filter(StateTransition.transition_type == TransitionTypeEnum.ERROR)

            # Apply filters before limit and ordering
            if tenant_id:
                error_states_query = error_states_query.filter(
                    StateTransition.tenant_id == tenant_id
                )

            if start_time:
                error_states_query = error_states_query.filter(
                    StateTransition.created_at >= start_time
                )

            if end_time:
                error_states_query = error_states_query.filter(
                    StateTransition.created_at <= end_time
                )

            # Apply group by, ordering and limit after all filters
            error_states_query = (
                error_states_query.group_by(StateTransition.to_state)
                .order_by(desc("count"))
                .limit(5)
            )

            error_states_result = error_states_query.all()
            most_common_error_states = [row.to_state for row in error_states_result]

            return StateTransitionStats(
                total_transitions=total_transitions,
                transitions_by_state=transitions_by_state,
                avg_duration_by_state=avg_duration_by_state,
                error_rate=error_rate,
                most_common_error_states=most_common_error_states,
            )

    def _get_last_sequence_number(self, entity_id: str, session=None) -> int:
        """
        Get the last sequence number for an entity.

        Args:
            entity_id: ID of the entity
            session: Optional database session to use

        Returns:
            Last sequence number or 0 if no transitions exist
        """

        def get_sequence(sess):
            result = (
                sess.query(func.max(StateTransition.sequence_number))
                .filter(StateTransition.entity_id == entity_id)
                .scalar()
            )
            return result or 0

        if session is None:
            with self._db_operation("get_last_sequence_number", entity_id) as new_session:
                return get_sequence(new_session)  # type: ignore[no-any-return]
        else:
            return get_sequence(session)  # type: ignore[no-any-return]
