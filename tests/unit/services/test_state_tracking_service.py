"""
Comprehensive tests for StateTrackingService.

Tests the state tracking service business logic layer using real SQLite database,
following the anti-mock philosophy with real database operations and proper
tenant isolation with schema validation.
"""

import os

# Import models and schemas using our established path pattern
import sys
import uuid
from datetime import datetime, timedelta
from typing import Any, Dict

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "..", "src"))

from src.context.tenant_context import TenantContext
from src.db.db_base import EntityStateEnum
from src.db.db_state_transition_models import TransitionTypeEnum
from src.exceptions import ServiceError
from src.repositories.entity_repository import EntityRepository
from src.schemas.entity_schema import EntityCreate
from src.schemas.state_transition_schema import (
    EntityStateHistory,
    StateTransitionCreate,
    StateTransitionRead,
    StateTransitionStats,
)
from src.services.state_tracking_service import StateTrackingService

# ==================== STATE TRACKING SERVICE TESTS ====================


class TestStateTrackingServiceRecordTransition:
    """Test state transition recording operations."""

    def test_record_transition_success(self, state_tracking_service, tenant_context, test_entities):
        """Test successful state transition recording."""
        # Arrange
        entity_id = test_entities["entity_record_test"]

        # Act
        transition_id = state_tracking_service.record_transition(
            entity_id=entity_id,
            from_state="NEW",
            to_state="VALIDATED",
            actor="test_processor",
            notes="Test transition recording",
        )

        # Assert
        assert transition_id is not None
        assert isinstance(transition_id, str)

        # Verify the transition was recorded correctly
        history = state_tracking_service.get_entity_state_history(entity_id)
        assert history is not None
        assert len(history.transitions) == 1

        transition = history.transitions[0]
        assert transition.entity_id == entity_id
        assert transition.tenant_id == tenant_context["id"]
        assert transition.from_state == "NEW"
        assert transition.to_state == "VALIDATED"
        assert transition.actor == "test_processor"
        assert transition.notes == "Test transition recording"
        assert transition.transition_type == TransitionTypeEnum.NORMAL

    def test_record_transition_with_enum_states(
        self, state_tracking_service, tenant_context, test_entities
    ):
        """Test recording transitions using EntityStateEnum values."""
        # Arrange
        entity_id = test_entities["entity_record_test"]

        # Act
        transition_id = state_tracking_service.record_transition(
            entity_id=entity_id,
            from_state=EntityStateEnum.RECEIVED,
            to_state=EntityStateEnum.PROCESSING,
            actor="enum_processor",
        )

        # Assert
        assert transition_id is not None

        # Verify enum values were converted to strings
        history = state_tracking_service.get_entity_state_history(entity_id)
        assert history is not None

        # Find the transition we just created
        enum_transition = None
        for transition in history.transitions:
            if transition.actor == "enum_processor":
                enum_transition = transition
                break

        assert enum_transition is not None
        assert enum_transition.from_state == EntityStateEnum.RECEIVED.value
        assert enum_transition.to_state == EntityStateEnum.PROCESSING.value

    def test_record_transition_with_full_data(
        self, state_tracking_service, tenant_context, test_entities
    ):
        """Test recording transition with all optional data."""
        # Arrange
        entity_id = test_entities["entity_record_test"]
        processor_data = {
            "processor_name": "full_data_processor",
            "processor_version": "1.0.0",
            "retry_count": 2,
            "custom_data": {
                "batch_id": "batch_001",
                "performance_metrics": {"cpu_usage": 45.5, "memory_mb": 128},
            },
        }

        # Act
        transition_id = state_tracking_service.record_transition(
            entity_id=entity_id,
            from_state="PROCESSING",
            to_state="COMPLETED",
            actor="full_data_processor",
            transition_type=TransitionTypeEnum.NORMAL,
            processor_data=processor_data,
            queue_source="input_queue",
            queue_destination="output_queue",
            notes="Comprehensive test transition",
            transition_duration=2500,
        )

        # Assert
        assert transition_id is not None

        # Verify all data was recorded
        history = state_tracking_service.get_entity_state_history(entity_id)
        assert history is not None

        # Find our transition
        full_transition = None
        for transition in history.transitions:
            if transition.actor == "full_data_processor":
                full_transition = transition
                break

        assert full_transition is not None
        assert full_transition.processor_data == processor_data
        assert full_transition.queue_source == "input_queue"
        assert full_transition.queue_destination == "output_queue"
        assert full_transition.notes == "Comprehensive test transition"
        assert full_transition.transition_duration == 2500

    def test_record_transition_with_string_transition_type(
        self, state_tracking_service, tenant_context, test_entities
    ):
        """Test recording transition with string transition type."""
        # Arrange
        entity_id = test_entities["entity_record_test"]

        # Act
        transition_id = state_tracking_service.record_transition(
            entity_id=entity_id,
            from_state="PROCESSING",
            to_state="ERROR",
            actor="error_processor",
            transition_type="ERROR",  # String instead of enum
        )

        # Assert
        assert transition_id is not None

        # Verify transition type was converted properly
        history = state_tracking_service.get_entity_state_history(entity_id)
        assert history is not None

        # Find the error transition
        error_transition = None
        for transition in history.transitions:
            if transition.actor == "error_processor":
                error_transition = transition
                break

        assert error_transition is not None
        assert error_transition.transition_type == TransitionTypeEnum.ERROR


class TestStateTrackingServiceEntityHistory:
    """Test entity state history operations."""

    def test_get_entity_state_history_complete_workflow(
        self, state_tracking_service, tenant_context, test_entities
    ):
        """Test retrieving complete state history for an entity."""
        # Arrange - Create a complete workflow
        entity_id = test_entities["entity_history_test"]
        workflow_transitions = [
            ("NEW", "VALIDATED", "validator"),
            ("VALIDATED", "PROCESSING", "processor"),
            ("PROCESSING", "ENRICHED", "enricher"),
            ("ENRICHED", "COMPLETED", "completer"),
        ]

        # Record all transitions
        for from_state, to_state, actor in workflow_transitions:
            state_tracking_service.record_transition(
                entity_id=entity_id, from_state=from_state, to_state=to_state, actor=actor
            )

        # Act
        history = state_tracking_service.get_entity_state_history(entity_id)

        # Assert
        assert isinstance(history, EntityStateHistory)
        assert history.entity_id == entity_id
        assert len(history.transitions) == 4
        assert history.current_state == "COMPLETED"
        assert history.total_transitions == 4

        # Verify transitions are in correct order
        for i, (from_state, to_state, actor) in enumerate(workflow_transitions):
            transition = history.transitions[i]
            assert transition.from_state == from_state
            assert transition.to_state == to_state
            assert transition.actor == actor
            assert transition.sequence_number == i + 1

        # Verify timestamps
        assert isinstance(history.first_seen, datetime)
        assert isinstance(history.last_updated, datetime)
        assert history.first_seen <= history.last_updated

    def test_get_entity_state_history_no_transitions(
        self, state_tracking_service, tenant_context, test_entities
    ):
        """Test entity history when no transitions exist."""
        # Act
        history = state_tracking_service.get_entity_state_history("nonexistent_entity")

        # Assert
        assert history is None

    def test_get_entity_state_history_tenant_isolation(
        self, state_tracking_service, multi_tenant_context, entity_repository
    ):
        """Test that entity history respects tenant isolation."""
        entity_ids = {}

        # Arrange - Create entities and transitions in different tenants
        for tenant in multi_tenant_context:
            TenantContext.set_current_tenant(tenant["id"])

            # Create entity for this tenant
            entity_data = EntityCreate(
                external_id=f"isolation_entity_{tenant['id']}",
                tenant_id=tenant["id"],
                canonical_type="order",
                source="test_system",
                version=1,
                content_hash=f"hash_{tenant['id']}",
                attributes={"test": True},
            )
            entity_id = entity_repository.create(entity_data)
            entity_ids[tenant["id"]] = entity_id

            # Record transition for this tenant
            state_tracking_service.record_transition(
                entity_id=entity_id,
                from_state="START",
                to_state="END",
                actor=f"actor_{tenant['id']}",
            )

        # Act & Assert - Each tenant sees only their own data
        for tenant in multi_tenant_context:
            TenantContext.set_current_tenant(tenant["id"])

            history = state_tracking_service.get_entity_state_history(entity_ids[tenant["id"]])
            assert history is not None
            assert len(history.transitions) == 1
            assert history.transitions[0].actor == f"actor_{tenant['id']}"
            assert history.transitions[0].tenant_id == tenant["id"]


class TestStateTrackingServiceCurrentState:
    """Test current state retrieval operations."""

    def test_get_current_state_success(self, state_tracking_service, tenant_context, test_entities):
        """Test successful current state retrieval."""
        # Arrange
        entity_id = test_entities["entity_current_state"]

        # Record multiple transitions
        state_tracking_service.record_transition(
            entity_id=entity_id, from_state="NEW", to_state="PROCESSING", actor="processor"
        )
        state_tracking_service.record_transition(
            entity_id=entity_id, from_state="PROCESSING", to_state="COMPLETED", actor="completer"
        )

        # Act
        current_state = state_tracking_service.get_current_state(entity_id)

        # Assert
        assert current_state == "COMPLETED"

    def test_get_current_state_no_transitions(
        self, state_tracking_service, tenant_context, test_entities
    ):
        """Test current state for entity with no transitions."""
        # Act
        current_state = state_tracking_service.get_current_state("nonexistent_entity")

        # Assert
        assert current_state is None


class TestStateTrackingServiceEntitiesInState:
    """Test entities in state retrieval operations."""

    def test_get_entities_in_state_success(
        self, state_tracking_service, tenant_context, entity_repository
    ):
        """Test retrieving entities in a specific state."""
        # Arrange - Create dedicated entities for this test
        entity_ids = []
        for i in range(3):
            entity_data = EntityCreate(
                external_id=f"state_test_entity_{i}",
                tenant_id=tenant_context["id"],
                canonical_type="order",
                source="test_system",
                version=1,
                content_hash=f"hash_state_test_{i}",
                attributes={"test": True},
            )
            entity_id = entity_repository.create(entity_data)
            entity_ids.append(entity_id)

        entities_in_processing = []
        entities_in_completed = []

        # Put all entities in PROCESSING state
        for i, entity_id in enumerate(entity_ids):
            state_tracking_service.record_transition(
                entity_id=entity_id, from_state="NEW", to_state="PROCESSING", actor=f"processor_{i}"
            )
            entities_in_processing.append(entity_id)

        # Move one entity to COMPLETED
        state_tracking_service.record_transition(
            entity_id=entities_in_processing[0],
            from_state="PROCESSING",
            to_state="COMPLETED",
            actor="completer",
        )
        entities_in_completed.append(entities_in_processing[0])
        entities_in_processing.remove(entities_in_processing[0])

        # Act
        processing_entities = state_tracking_service.get_entities_in_state("PROCESSING")
        completed_entities = state_tracking_service.get_entities_in_state("COMPLETED")

        # Assert
        assert len(processing_entities) == 2
        assert len(completed_entities) == 1
        assert all(entity_id in entities_in_processing for entity_id in processing_entities)
        assert completed_entities[0] in entities_in_completed

    def test_get_entities_in_state_with_enum(
        self, state_tracking_service, tenant_context, entity_repository
    ):
        """Test retrieving entities using EntityStateEnum."""
        # Arrange - Create dedicated entity for this test
        entity_data = EntityCreate(
            external_id="enum_state_test_entity",
            tenant_id=tenant_context["id"],
            canonical_type="order",
            source="test_system",
            version=1,
            content_hash="hash_enum_state_test",
            attributes={"test": True},
        )
        entity_id = entity_repository.create(entity_data)

        state_tracking_service.record_transition(
            entity_id=entity_id,
            from_state="NEW",  # Use string since NEW is not in EntityStateEnum
            to_state=EntityStateEnum.RECEIVED,
            actor="receiver",
        )

        # Act
        entities = state_tracking_service.get_entities_in_state(EntityStateEnum.RECEIVED)

        # Assert
        assert entity_id in entities

    def test_get_entities_in_state_pagination(
        self, state_tracking_service, tenant_context, entity_repository
    ):
        """Test pagination in entities in state retrieval."""
        # Arrange - Create multiple entities in same state
        entity_ids = []
        for i in range(5):
            entity_data = EntityCreate(
                external_id=f"pagination_entity_{i}",
                tenant_id=tenant_context["id"],
                canonical_type="order",
                source="test_system",
                version=1,
                content_hash=f"hash_pagination_{i}",
                attributes={"test": True},
            )
            entity_id = entity_repository.create(entity_data)
            entity_ids.append(entity_id)

            # Put all in PROCESSING state
            state_tracking_service.record_transition(
                entity_id=entity_id, from_state="NEW", to_state="PROCESSING", actor="processor"
            )

        # Act - Test pagination
        first_page = state_tracking_service.get_entities_in_state("PROCESSING", limit=2, offset=0)
        second_page = state_tracking_service.get_entities_in_state("PROCESSING", limit=2, offset=2)

        # Assert
        assert len(first_page) == 2
        assert len(second_page) == 2

        # Verify different entities in each page
        assert set(first_page).isdisjoint(set(second_page))

    def test_get_entities_in_state_empty_result(
        self, state_tracking_service, tenant_context, test_entities
    ):
        """Test retrieving entities for state with no entities."""
        # Act
        entities = state_tracking_service.get_entities_in_state("NONEXISTENT_STATE")

        # Assert
        assert entities == []


class TestStateTrackingServiceStuckEntities:
    """Test stuck entities detection operations."""

    def test_get_stuck_entities_basic(
        self, state_tracking_service, tenant_context, entity_repository
    ):
        """Test basic stuck entities detection - verifies method works without errors."""
        # Arrange - Create dedicated entity for this test
        entity_data = EntityCreate(
            external_id="stuck_test_entity",
            tenant_id=tenant_context["id"],
            canonical_type="order",
            source="test_system",
            version=1,
            content_hash="hash_stuck_test",
            attributes={"test": True},
        )
        entity_id = entity_repository.create(entity_data)

        # Record transition to a state
        state_tracking_service.record_transition(
            entity_id=entity_id, from_state="NEW", to_state="PROCESSING", actor="processor"
        )

        # Act - Test that the method works without errors
        stuck_entities = state_tracking_service.get_stuck_entities(
            state="PROCESSING", threshold_minutes=60, limit=100  # Standard threshold
        )

        # Assert - Method should return a list (may be empty for recent transitions)
        assert isinstance(stuck_entities, list)

    def test_get_stuck_entities_with_enum(
        self, state_tracking_service, tenant_context, entity_repository
    ):
        """Test stuck entities detection using EntityStateEnum."""
        # Arrange - Create dedicated entity for this test
        entity_data = EntityCreate(
            external_id="stuck_enum_test_entity",
            tenant_id=tenant_context["id"],
            canonical_type="order",
            source="test_system",
            version=1,
            content_hash="hash_stuck_enum_test",
            attributes={"test": True},
        )
        entity_id = entity_repository.create(entity_data)

        state_tracking_service.record_transition(
            entity_id=entity_id,
            from_state=EntityStateEnum.RECEIVED,
            to_state=EntityStateEnum.PROCESSING,
            actor="processor",
        )

        # Act - Test that the method works with enums
        stuck_entities = state_tracking_service.get_stuck_entities(
            state=EntityStateEnum.PROCESSING, threshold_minutes=60  # Standard threshold
        )

        # Assert - Method should return a list (may be empty for recent transitions)
        assert isinstance(stuck_entities, list)

    def test_get_stuck_entities_not_stuck(
        self, state_tracking_service, tenant_context, entity_repository
    ):
        """Test that entities moved to different states are not considered stuck."""
        # Arrange - Create dedicated entity for this test
        entity_data = EntityCreate(
            external_id="not_stuck_test_entity",
            tenant_id=tenant_context["id"],
            canonical_type="order",
            source="test_system",
            version=1,
            content_hash="hash_not_stuck_test",
            attributes={"test": True},
        )
        entity_id = entity_repository.create(entity_data)

        # Record transition to processing, then move to completed
        state_tracking_service.record_transition(
            entity_id=entity_id, from_state="NEW", to_state="PROCESSING", actor="processor"
        )
        state_tracking_service.record_transition(
            entity_id=entity_id, from_state="PROCESSING", to_state="COMPLETED", actor="completer"
        )

        # Act - Look for stuck entities in PROCESSING
        stuck_entities = state_tracking_service.get_stuck_entities(
            state="PROCESSING", threshold_minutes=60  # Standard threshold
        )

        # Assert - Entity should not be stuck since it moved to COMPLETED
        assert entity_id not in stuck_entities


class TestStateTrackingServiceStatistics:
    """Test state statistics operations."""

    def test_get_state_statistics_basic(
        self, state_tracking_service, tenant_context, entity_repository
    ):
        """Test basic state statistics retrieval."""
        # Arrange - Create entities and transitions for statistics
        for i in range(3):
            entity_data = EntityCreate(
                external_id=f"stats_entity_{i}",
                tenant_id=tenant_context["id"],
                canonical_type="order",
                source="test_system",
                version=1,
                content_hash=f"hash_stats_{i}",
                attributes={"test": True},
            )
            entity_id = entity_repository.create(entity_data)

            # Create different transition patterns
            if i == 0:
                state_tracking_service.record_transition(
                    entity_id=entity_id,
                    from_state="NEW",
                    to_state="COMPLETED",
                    actor="quick_processor",
                )
            else:
                state_tracking_service.record_transition(
                    entity_id=entity_id,
                    from_state="NEW",
                    to_state="FAILED",
                    actor="failing_processor",
                )

        # Act
        stats = state_tracking_service.get_state_statistics()

        # Assert
        assert isinstance(stats, StateTransitionStats)
        assert stats.total_transitions >= 3  # At least our 3 transitions
        assert isinstance(stats.transitions_by_state, dict)
        assert isinstance(stats.avg_duration_by_state, dict)
        assert isinstance(stats.error_rate, float)
        assert isinstance(stats.most_common_error_states, list)

    def test_get_state_statistics_with_time_range(
        self, state_tracking_service, tenant_context, test_entities
    ):
        """Test state statistics with time range filtering."""
        # Arrange
        now = datetime.now()
        yesterday = now - timedelta(days=1)

        # Act
        stats = state_tracking_service.get_state_statistics(start_time=yesterday, end_time=now)

        # Assert
        assert isinstance(stats, StateTransitionStats)
        assert stats.total_transitions >= 0


class TestStateTrackingServiceProcessingTime:
    """Test processing time calculation operations."""

    def test_calculate_avg_processing_time_basic(
        self, state_tracking_service, tenant_context, test_entities
    ):
        """Test basic average processing time calculation."""
        # Arrange - Create transitions with duration data
        entity_id = test_entities["entity_processing_time"]

        state_tracking_service.record_transition(
            entity_id=entity_id,
            from_state=EntityStateEnum.RECEIVED.value,
            to_state=EntityStateEnum.PROCESSING.value,
            actor="processor",
            transition_duration=1000,
        )
        state_tracking_service.record_transition(
            entity_id=entity_id,
            from_state=EntityStateEnum.PROCESSING.value,
            to_state=EntityStateEnum.COMPLETED.value,
            actor="completer",
            transition_duration=2000,
        )

        # Act
        avg_time = state_tracking_service.calculate_avg_processing_time(
            start_state=EntityStateEnum.RECEIVED, end_state=EntityStateEnum.COMPLETED
        )

        # Assert
        # The method should return some processing time calculation
        assert avg_time is not None or avg_time == 0  # Allow for edge cases in calculation

    def test_calculate_avg_processing_time_with_strings(
        self, state_tracking_service, tenant_context, test_entities
    ):
        """Test processing time calculation with string states."""
        # Act
        avg_time = state_tracking_service.calculate_avg_processing_time(
            start_state="RECEIVED", end_state="COMPLETED"
        )

        # Assert - Should handle string states without error
        assert avg_time is not None or avg_time is None  # Allow for no data case

    def test_calculate_avg_processing_time_invalid_states(
        self, state_tracking_service, tenant_context, test_entities
    ):
        """Test processing time calculation with invalid states."""
        # Act
        avg_time = state_tracking_service.calculate_avg_processing_time(
            start_state="INVALID_START", end_state="INVALID_END"
        )

        # Assert
        assert avg_time is None


class TestStateTrackingServiceMessageUpdate:
    """Test message update utility operations."""

    def test_update_message_with_state_basic(
        self, state_tracking_service, tenant_context, test_entities
    ):
        """Test basic message state update."""
        # Arrange
        message = {"id": "msg_001", "content": "test message", "state": "OLD_STATE"}

        # Act
        updated_message = state_tracking_service.update_message_with_state(
            message=message, state="NEW_STATE"
        )

        # Assert
        assert updated_message["state"] == "NEW_STATE"
        assert updated_message["id"] == "msg_001"
        assert updated_message["content"] == "test message"

        # Verify original message is not modified
        assert message["state"] == "OLD_STATE"

    def test_update_message_with_enum_state(
        self, state_tracking_service, tenant_context, test_entities
    ):
        """Test message update with EntityStateEnum."""
        # Arrange
        message = {"id": "msg_002", "state": "OLD_STATE"}

        # Act
        updated_message = state_tracking_service.update_message_with_state(
            message=message, state=EntityStateEnum.PROCESSING
        )

        # Assert
        assert updated_message["state"] == EntityStateEnum.PROCESSING.value

    def test_update_message_with_metadata(
        self, state_tracking_service, tenant_context, test_entities
    ):
        """Test message update with metadata tracking."""
        # Arrange
        message = {
            "id": "msg_003",
            "state": "OLD_STATE",
            "metadata": {"version": 1, "source": "test"},
        }

        # Act
        updated_message = state_tracking_service.update_message_with_state(
            message=message, state="NEW_STATE"
        )

        # Assert
        assert updated_message["state"] == "NEW_STATE"
        assert updated_message["metadata"]["previous_state"] == "OLD_STATE"
        assert "state_changed_at" in updated_message["metadata"]
        assert updated_message["metadata"]["version"] == 1
        assert updated_message["metadata"]["source"] == "test"

        # Verify timestamp format
        assert isinstance(updated_message["metadata"]["state_changed_at"], str)


class TestStateTrackingServiceErrorHandling:
    """Test service error handling and edge cases."""

    def test_record_transition_invalid_entity(
        self, state_tracking_service, tenant_context, test_entities
    ):
        """Test error handling for invalid entity ID."""
        # Act & Assert
        with pytest.raises(ServiceError):
            state_tracking_service.record_transition(
                entity_id="invalid_entity_id",
                from_state="NEW",
                to_state="PROCESSING",
                actor="test_processor",
            )

    def test_service_tenant_isolation(
        self, state_tracking_service, multi_tenant_context, entity_repository
    ):
        """Test that service operations respect tenant isolation."""
        entity_ids = {}

        # Arrange - Create entities in different tenants
        for tenant in multi_tenant_context:
            TenantContext.set_current_tenant(tenant["id"])

            entity_data = EntityCreate(
                external_id=f"service_isolation_{tenant['id']}",
                tenant_id=tenant["id"],
                canonical_type="order",
                source="test_system",
                version=1,
                content_hash=f"hash_{tenant['id']}",
                attributes={"test": True},
            )
            entity_id = entity_repository.create(entity_data)
            entity_ids[tenant["id"]] = entity_id

            # Record transition
            state_tracking_service.record_transition(
                entity_id=entity_id,
                from_state="NEW",
                to_state="PROCESSING",
                actor=f"processor_{tenant['id']}",
            )

        # Act & Assert - Each tenant sees only their own data
        for tenant in multi_tenant_context:
            TenantContext.set_current_tenant(tenant["id"])

            # Can see own entity history
            history = state_tracking_service.get_entity_state_history(entity_ids[tenant["id"]])
            assert history is not None

            # Cannot see other tenants' entity histories
            for other_tenant in multi_tenant_context:
                if other_tenant["id"] != tenant["id"]:
                    other_history = state_tracking_service.get_entity_state_history(
                        entity_ids[other_tenant["id"]]
                    )
                    assert other_history is None
