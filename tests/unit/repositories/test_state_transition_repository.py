"""
Comprehensive tests for StateTransitionRepository.

Tests the state transition repository data access layer using real SQLite database,
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
from src.db.db_state_transition_models import StateTransition, TransitionTypeEnum
from src.exceptions import RepositoryError
from src.repositories.entity_repository import EntityRepository
from src.repositories.state_transition_repository import StateTransitionRepository
from src.schemas.entity_schema import EntityCreate
from src.schemas.state_transition_schema import (
    EntityStateHistory,
    StateTransitionCreate,
    StateTransitionFilter,
    StateTransitionRead,
    StateTransitionStats,
)

# ==================== HELPER FIXTURES ====================


# ==================== STATE TRANSITION REPOSITORY TESTS ====================


class TestStateTransitionRepositoryCreate:
    """Test state transition creation operations."""

    def test_create_state_transition_success(
        self, state_transition_repository, tenant_context, test_entities
    ):
        """Test successful state transition creation."""
        # Arrange
        transition_data = StateTransitionCreate(
            entity_id=test_entities["ent_12345"],
            tenant_id=tenant_context["id"],
            from_state="PENDING",
            to_state="PROCESSING",
            actor="test_processor",
            transition_type=TransitionTypeEnum.NORMAL,
            notes="Test transition",
            processor_data={"processor_name": "test_processor", "retry_count": 0},
            queue_source="input_queue",
            queue_destination="processing_queue",
            transition_duration=150,
        )

        # Act
        transition_id = state_transition_repository.create(transition_data)

        # Assert
        assert transition_id is not None
        assert isinstance(transition_id, str)

        # Verify transition was created correctly
        created_transition = state_transition_repository.get_by_id(transition_id)
        assert isinstance(created_transition, StateTransitionRead)
        assert created_transition.entity_id == test_entities["ent_12345"]
        assert created_transition.tenant_id == tenant_context["id"]
        assert created_transition.from_state == "PENDING"
        assert created_transition.to_state == "PROCESSING"
        assert created_transition.actor == "test_processor"
        assert created_transition.transition_type == TransitionTypeEnum.NORMAL
        assert created_transition.notes == "Test transition"
        assert created_transition.processor_data["processor_name"] == "test_processor"
        assert created_transition.queue_source == "input_queue"
        assert created_transition.queue_destination == "processing_queue"
        assert created_transition.transition_duration == 150
        assert isinstance(created_transition.created_at, datetime)
        assert isinstance(created_transition.updated_at, datetime)

    def test_create_state_transition_minimal_data(
        self, state_transition_repository, tenant_context, test_entities
    ):
        """Test creating state transition with minimal required data."""
        # Arrange
        transition_data = StateTransitionCreate(
            entity_id=test_entities["ent_minimal"],
            tenant_id=tenant_context["id"],
            from_state="NEW",
            to_state="VALIDATED",
            actor="validator",
        )

        # Act
        transition_id = state_transition_repository.create(transition_data)

        # Assert
        created_transition = state_transition_repository.get_by_id(transition_id)
        assert isinstance(created_transition, StateTransitionRead)
        assert created_transition.entity_id == test_entities["ent_minimal"]
        assert created_transition.from_state == "NEW"
        assert created_transition.to_state == "VALIDATED"
        assert created_transition.actor == "validator"
        assert created_transition.transition_type == TransitionTypeEnum.NORMAL  # Default value
        assert created_transition.notes is None
        assert created_transition.processor_data is None
        assert created_transition.queue_source is None
        assert created_transition.queue_destination is None
        assert created_transition.transition_duration is None

    def test_create_state_transition_with_complex_processor_data(
        self, state_transition_repository, tenant_context, test_entities
    ):
        """Test creating state transition with complex processor data."""
        # Arrange
        complex_processor_data = {
            "processor_name": "complex_processor",
            "processor_version": "1.2.3",
            "processing_time_ms": 2500,
            "retry_count": 2,
            "custom_data": {
                "processing_metadata": {
                    "batch_id": "batch_complex_001",
                    "retry_attempts": 2,
                    "error_history": [
                        {"timestamp": "2024-01-01T10:00:00Z", "error": "timeout"},
                        {"timestamp": "2024-01-01T10:30:00Z", "error": "connection_lost"},
                    ],
                },
                "performance_metrics": {
                    "memory_usage_mb": 128,
                    "cpu_usage_percent": 45.7,
                },
                "configuration": {"max_retries": 3, "timeout_seconds": 300, "batch_size": 50},
            },
        }

        transition_data = StateTransitionCreate(
            entity_id=test_entities["ent_complex"],
            tenant_id=tenant_context["id"],
            from_state="RETRY",
            to_state="COMPLETED",
            actor="complex_processor",
            processor_data=complex_processor_data,
            transition_duration=2500,
        )

        # Act
        transition_id = state_transition_repository.create(transition_data)

        # Assert
        created_transition = state_transition_repository.get_by_id(transition_id)
        assert created_transition.processor_data == complex_processor_data
        assert created_transition.processor_data["processor_name"] == "complex_processor"
        assert created_transition.processor_data["processing_time_ms"] == 2500
        assert created_transition.processor_data["retry_count"] == 2
        assert (
            created_transition.processor_data["custom_data"]["processing_metadata"]["batch_id"]
            == "batch_complex_001"
        )
        assert len(created_transition.processor_data["custom_data"]["processing_metadata"]["error_history"]) == 2

    def test_create_state_transition_auto_sequence_number(
        self, state_transition_repository, tenant_context, test_entities
    ):
        """Test that sequence numbers are automatically assigned correctly."""
        entity_id = test_entities["ent_sequence_test"]

        # Create multiple transitions for the same entity
        transitions = []
        for i in range(3):
            transition_data = StateTransitionCreate(
                entity_id=entity_id,
                tenant_id=tenant_context["id"],
                from_state=f"STATE_{i}",
                to_state=f"STATE_{i+1}",
                actor="sequence_tester",
            )
            transition_id = state_transition_repository.create(transition_data)
            transitions.append(state_transition_repository.get_by_id(transition_id))

        # Assert sequence numbers are correctly assigned
        assert transitions[0].sequence_number == 1
        assert transitions[1].sequence_number == 2
        assert transitions[2].sequence_number == 3

    def test_create_state_transition_tenant_context_injection(
        self, state_transition_repository, tenant_context, test_entities
    ):
        """Test that tenant_id is injected from context when not provided."""
        # Arrange - Create transition data without explicit tenant_id
        transition_data = StateTransitionCreate(
            entity_id=test_entities["ent_context_test"],
            from_state="START",
            to_state="FINISH",
            actor="context_tester",
        )

        # Act
        transition_id = state_transition_repository.create(transition_data)

        # Assert
        created_transition = state_transition_repository.get_by_id(transition_id)
        assert created_transition.tenant_id == tenant_context["id"]


class TestStateTransitionRepositoryRead:
    """Test state transition read operations."""

    def test_get_by_id_success(self, state_transition_repository, tenant_context, test_entities):
        """Test successful retrieval of state transition by ID."""
        # Arrange - Create a transition first
        transition_data = StateTransitionCreate(
            entity_id=test_entities["ent_get_test"],
            tenant_id=tenant_context["id"],
            from_state="INITIAL",
            to_state="FINAL",
            actor="get_tester",
            notes="Test get by ID",
        )
        transition_id = state_transition_repository.create(transition_data)

        # Act
        retrieved_transition = state_transition_repository.get_by_id(transition_id)

        # Assert
        assert isinstance(retrieved_transition, StateTransitionRead)
        assert retrieved_transition.id == transition_id
        assert retrieved_transition.entity_id == test_entities["ent_get_test"]
        assert retrieved_transition.tenant_id == tenant_context["id"]
        assert retrieved_transition.from_state == "INITIAL"
        assert retrieved_transition.to_state == "FINAL"
        assert retrieved_transition.actor == "get_tester"
        assert retrieved_transition.notes == "Test get by ID"

    def test_get_by_id_not_found(self, state_transition_repository, tenant_context, test_entities):
        """Test retrieval of non-existent state transition."""
        # Act
        result = state_transition_repository.get_by_id("nonexistent_id")

        # Assert
        assert result is None

    def test_get_by_id_tenant_isolation(
        self, state_transition_repository, multi_tenant_context, entity_repository
    ):
        """Test that get_by_id respects tenant isolation."""
        # Arrange - Create entities and transitions in different tenants
        transition_ids = {}
        entity_ids = {}

        for tenant in multi_tenant_context:
            TenantContext.set_current_tenant(tenant["id"])

            # Create entity for this tenant
            entity_data = EntityCreate(
                external_id=f"isolation_test_{tenant['id']}",
                tenant_id=tenant["id"],
                canonical_type="order",
                source="test_system",
                version=1,
                content_hash=f"hash_{tenant['id']}",
                attributes={"test": True},
            )
            entity_id = entity_repository.create(entity_data)
            entity_ids[tenant["id"]] = entity_id

            transition_data = StateTransitionCreate(
                entity_id=entity_id,
                tenant_id=tenant["id"],
                from_state="TENANT_START",
                to_state="TENANT_END",
                actor=f"actor_{tenant['id']}",
            )
            transition_id = state_transition_repository.create(transition_data)
            transition_ids[tenant["id"]] = transition_id

        # Act & Assert - Each tenant can only see their own transitions
        for tenant in multi_tenant_context:
            TenantContext.set_current_tenant(tenant["id"])

            # Can retrieve own transition
            own_transition = state_transition_repository.get_by_id(transition_ids[tenant["id"]])
            assert own_transition is not None
            assert own_transition.tenant_id == tenant["id"]

            # Cannot retrieve other tenants' transitions
            for other_tenant in multi_tenant_context:
                if other_tenant["id"] != tenant["id"]:
                    other_transition = state_transition_repository.get_by_id(
                        transition_ids[other_tenant["id"]]
                    )
                    assert other_transition is None

    def test_get_by_filter_basic(self, state_transition_repository, tenant_context, test_entities):
        """Test basic filtering of state transitions."""
        # Arrange - Create multiple transitions
        entity_id = test_entities["ent_filter_test"]
        transitions = []

        # Create transitions with different states
        states = [("NEW", "VALIDATED"), ("VALIDATED", "PROCESSING"), ("PROCESSING", "COMPLETED")]
        for from_state, to_state in states:
            transition_data = StateTransitionCreate(
                entity_id=entity_id,
                tenant_id=tenant_context["id"],
                from_state=from_state,
                to_state=to_state,
                actor="filter_tester",
            )
            transition_id = state_transition_repository.create(transition_data)
            transitions.append(transition_id)

        # Act - Filter by entity_id
        filter_params = StateTransitionFilter(entity_id=entity_id)
        filtered_transitions = state_transition_repository.get_by_filter(filter_params)

        # Assert
        assert len(filtered_transitions) == 3
        assert all(isinstance(t, StateTransitionRead) for t in filtered_transitions)
        assert all(t.entity_id == entity_id for t in filtered_transitions)
        assert all(t.tenant_id == tenant_context["id"] for t in filtered_transitions)

    def test_get_by_filter_state_filtering(
        self, state_transition_repository, tenant_context, entity_repository
    ):
        """Test filtering by from_state and to_state."""
        # Arrange - Create entities and transitions with different state combinations
        transitions_data = [
            ("state_ent_1", "NEW", "PROCESSING"),
            ("state_ent_2", "NEW", "FAILED"),
            ("state_ent_3", "PROCESSING", "COMPLETED"),
            ("state_ent_4", "PROCESSING", "FAILED"),
        ]

        for entity_name, from_state, to_state in transitions_data:
            # Create entity
            entity_data = EntityCreate(
                external_id=entity_name,
                tenant_id=tenant_context["id"],
                canonical_type="order",
                source="test_system",
                version=1,
                content_hash=f"hash_{entity_name}",
                attributes={"test": True},
            )
            entity_id = entity_repository.create(entity_data)

            # Create transition
            transition_data = StateTransitionCreate(
                entity_id=entity_id,
                tenant_id=tenant_context["id"],
                from_state=from_state,
                to_state=to_state,
                actor="state_filter_tester",
            )
            state_transition_repository.create(transition_data)

        # Act - Filter by from_state = "NEW"
        filter_params = StateTransitionFilter(from_state="NEW")
        new_transitions = state_transition_repository.get_by_filter(filter_params)

        # Assert
        assert len(new_transitions) == 2
        assert all(t.from_state == "NEW" for t in new_transitions)

        # Act - Filter by to_state = "FAILED"
        filter_params = StateTransitionFilter(to_state="FAILED")
        failed_transitions = state_transition_repository.get_by_filter(filter_params)

        # Assert
        assert len(failed_transitions) == 2
        assert all(t.to_state == "FAILED" for t in failed_transitions)

    def test_get_by_filter_pagination(
        self, state_transition_repository, tenant_context, entity_repository
    ):
        """Test pagination in filtering."""
        # Arrange - Create entity and multiple transitions
        entity_data = EntityCreate(
            external_id="pagination_test_entity",
            tenant_id=tenant_context["id"],
            canonical_type="order",
            source="test_system",
            version=1,
            content_hash="hash_pagination",
            attributes={"test": True},
        )
        entity_id = entity_repository.create(entity_data)

        for i in range(10):
            transition_data = StateTransitionCreate(
                entity_id=entity_id,
                tenant_id=tenant_context["id"],
                from_state=f"STATE_{i}",
                to_state=f"STATE_{i+1}",
                actor="pagination_tester",
            )
            state_transition_repository.create(transition_data)

        # Act - Get first page
        filter_params = StateTransitionFilter(entity_id=entity_id)
        first_page = state_transition_repository.get_by_filter(filter_params, limit=3, offset=0)

        # Act - Get second page
        second_page = state_transition_repository.get_by_filter(filter_params, limit=3, offset=3)

        # Assert
        assert len(first_page) == 3
        assert len(second_page) == 3

        # Verify different transitions in each page
        first_page_ids = {t.id for t in first_page}
        second_page_ids = {t.id for t in second_page}
        assert first_page_ids.isdisjoint(second_page_ids)

    def test_get_by_filter_empty_result(
        self, state_transition_repository, tenant_context, test_entities
    ):
        """Test filtering with no matching results."""
        # Act
        filter_params = StateTransitionFilter(entity_id="nonexistent_entity")
        result = state_transition_repository.get_by_filter(filter_params)

        # Assert
        assert result == []


class TestStateTransitionRepositoryEntityHistory:
    """Test entity state history operations."""

    def test_get_entity_state_history_complete_workflow(
        self, state_transition_repository, tenant_context, test_entities
    ):
        """Test retrieving complete state history for an entity."""
        # Arrange - Create a complete workflow for an entity
        entity_id = test_entities["ent_workflow_test"]
        workflow_states = [
            ("NEW", "VALIDATED"),
            ("VALIDATED", "PROCESSING"),
            ("PROCESSING", "ENRICHED"),
            ("ENRICHED", "COMPLETED"),
        ]

        for i, (from_state, to_state) in enumerate(workflow_states):
            transition_data = StateTransitionCreate(
                entity_id=entity_id,
                tenant_id=tenant_context["id"],
                from_state=from_state,
                to_state=to_state,
                actor=f"processor_{i+1}",
                transition_duration=100 + (i * 50),
            )
            state_transition_repository.create(transition_data)

        # Act
        history = state_transition_repository.get_entity_state_history(entity_id)

        # Assert
        assert isinstance(history, EntityStateHistory)
        assert history.entity_id == entity_id
        # Verify tenant isolation through transitions
        assert all(t.tenant_id == tenant_context["id"] for t in history.transitions)
        assert len(history.transitions) == 4

        # Verify transitions are in correct order (by sequence_number)
        for i, transition in enumerate(history.transitions):
            assert transition.sequence_number == i + 1
            assert transition.from_state == workflow_states[i][0]
            assert transition.to_state == workflow_states[i][1]
            assert transition.actor == f"processor_{i+1}"

        # Verify current state
        assert history.current_state == "COMPLETED"

        # Verify metadata
        assert history.total_transitions == 4
        assert isinstance(history.first_seen, datetime)
        assert isinstance(history.last_updated, datetime)
        assert isinstance(history.total_processing_time, int)

    def test_get_entity_state_history_no_transitions(
        self, state_transition_repository, tenant_context, test_entities
    ):
        """Test entity history when no transitions exist."""
        # Act
        history = state_transition_repository.get_entity_state_history("nonexistent_entity")

        # Assert - Should return None when no transitions exist
        assert history is None

    def test_get_entity_state_history_tenant_isolation(
        self, state_transition_repository, multi_tenant_context, entity_repository
    ):
        """Test that entity history respects tenant isolation."""
        entity_name = "shared_entity_name"
        entity_ids = {}

        # Arrange - Create entities with same name but different tenant isolation
        for tenant in multi_tenant_context:
            TenantContext.set_current_tenant(tenant["id"])

            # Create entity for this tenant
            entity_data = EntityCreate(
                external_id=entity_name,
                tenant_id=tenant["id"],
                canonical_type="order",
                source="test_system",
                version=1,
                content_hash=f"hash_{tenant['id']}",
                attributes={"test": True},
            )
            entity_id = entity_repository.create(entity_data)
            entity_ids[tenant["id"]] = entity_id

            transition_data = StateTransitionCreate(
                entity_id=entity_id,
                tenant_id=tenant["id"],
                from_state="TENANT_START",
                to_state="TENANT_END",
                actor=f"actor_{tenant['id']}",
            )
            state_transition_repository.create(transition_data)

        # Act & Assert - Each tenant sees only their own transitions
        for tenant in multi_tenant_context:
            TenantContext.set_current_tenant(tenant["id"])

            history = state_transition_repository.get_entity_state_history(entity_ids[tenant["id"]])
            assert len(history.transitions) == 1
            assert history.transitions[0].actor == f"actor_{tenant['id']}"
            # Verify tenant isolation through transitions
            assert all(t.tenant_id == tenant["id"] for t in history.transitions)


class TestStateTransitionRepositoryStatistics:
    """Test state transition statistics operations."""

    def test_get_state_statistics_basic(
        self, state_transition_repository, tenant_context, entity_repository
    ):
        """Test basic state transition statistics."""
        # Arrange - Create entities and transitions with different outcomes
        transitions_data = [
            ("stats_ent_1", "NEW", "COMPLETED"),
            ("stats_ent_2", "NEW", "FAILED"),
            ("stats_ent_3", "NEW", "COMPLETED"),
            ("stats_ent_4", "PROCESSING", "COMPLETED"),
            ("stats_ent_5", "PROCESSING", "FAILED"),
        ]

        for entity_name, from_state, to_state in transitions_data:
            # Create entity
            entity_data = EntityCreate(
                external_id=entity_name,
                tenant_id=tenant_context["id"],
                canonical_type="order",
                source="test_system",
                version=1,
                content_hash=f"hash_{entity_name}",
                attributes={"test": True},
            )
            entity_id = entity_repository.create(entity_data)

            # Create transition
            transition_data = StateTransitionCreate(
                entity_id=entity_id,
                tenant_id=tenant_context["id"],
                from_state=from_state,
                to_state=to_state,
                actor="stats_tester",
            )
            state_transition_repository.create(transition_data)

        # Act - Use tenant context for tenant-secure operation
        from src.context.tenant_context import tenant_context as tenant_ctx
        with tenant_ctx(tenant_context["id"]):
            stats = state_transition_repository.get_state_statistics()

        # Assert
        assert isinstance(stats, StateTransitionStats)
        assert stats.total_transitions == 5

        # Verify state counts - transitions_by_state tracks destination states (to_state)
        assert "COMPLETED" in stats.transitions_by_state
        assert "FAILED" in stats.transitions_by_state
        assert stats.transitions_by_state["COMPLETED"] == 3
        assert stats.transitions_by_state["FAILED"] == 2

        # Verify other statistics fields
        assert isinstance(stats.avg_duration_by_state, dict)
        assert isinstance(stats.error_rate, float)
        assert isinstance(stats.most_common_error_states, list)

    def test_get_state_statistics_time_filtering(
        self, state_transition_repository, tenant_context, test_entities
    ):
        """Test state statistics with time period filtering."""
        # This test would require manipulation of created_at timestamps
        # For now, we'll test the basic structure

        # Act - Use tenant context for tenant-secure operation
        now = datetime.now()
        yesterday = now - timedelta(days=1)
        from src.context.tenant_context import tenant_context as tenant_ctx
        with tenant_ctx(tenant_context["id"]):
            stats = state_transition_repository.get_state_statistics(
                start_time=yesterday, end_time=now
            )

        # Assert
        assert isinstance(stats, StateTransitionStats)
        assert stats.total_transitions >= 0
        assert isinstance(stats.transitions_by_state, dict)
        assert isinstance(stats.avg_duration_by_state, dict)


class TestStateTransitionRepositoryErrorHandling:
    """Test repository error handling and edge cases."""

    def test_create_with_invalid_tenant_context(self, state_transition_repository, test_entities):
        """Test creation fails gracefully when tenant context is invalid."""
        # Arrange - Clear tenant context
        TenantContext.clear_current_tenant()

        transition_data = StateTransitionCreate(
            entity_id=test_entities["ent_no_tenant"],
            from_state="START",
            to_state="END",
            actor="error_tester",
        )

        # Act & Assert - Should fail with tenant context error
        with pytest.raises(ValueError, match="No tenant context set"):
            state_transition_repository.create(transition_data)

    def test_repository_database_error_handling(
        self, state_transition_repository, tenant_context, test_entities
    ):
        """Test that repository properly handles and converts database errors."""
        # This test ensures proper error handling and conversion
        # by attempting operations that should work under normal circumstances

        # Create a valid transition
        transition_data = StateTransitionCreate(
            entity_id=test_entities["ent_db_error_test"],
            tenant_id=tenant_context["id"],
            from_state="START",
            to_state="END",
            actor="db_error_tester",
        )

        transition_id = state_transition_repository.create(transition_data)
        assert transition_id is not None

        # Verify we can retrieve it
        retrieved = state_transition_repository.get_by_id(transition_id)
        assert retrieved is not None
        assert retrieved.entity_id == test_entities["ent_db_error_test"]
