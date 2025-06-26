"""Tests for PipelineStateService with real database integration."""

import uuid
from datetime import datetime, timedelta, timezone

import pytest

from api_exchange_core.context.tenant_context import tenant_context
from api_exchange_core.db import PipelineStateHistory
from api_exchange_core.schemas import (
    EntityQuery,
    PipelineStateQuery,
)
from api_exchange_core.services.pipeline_state_service import PipelineStateService


class TestPipelineStateService:
    """
    Test cases for PipelineStateService using real database operations.
    
    Implementation Reference:
    - get_entity_timeline(entity_id) -> List[Dict] 
    - get_recent_activity(hours=24, limit=100) -> List[Dict]
    - get_processor_metrics(hours=24) -> List[Dict] (aggregation with count, avg, min, max)
    - get_failed_processing(hours=24, limit=50) -> List[Dict]
    - get_status_summary(hours=24) -> Dict (status counts, success rate, health status)
    
    Note: correlation_id based queries have been removed from the system.
    """

    @pytest.fixture
    def service(self, db_session):
        """Create PipelineStateService with real session."""
        return PipelineStateService(session=db_session)

    @pytest.fixture
    def sample_state_records(self, db_session):
        """Create sample pipeline state records for testing."""
        now = datetime.utcnow()
        
        records = [
            # Complete pipeline flow for entity-123
            PipelineStateHistory.create(
                tenant_id="test-tenant",
                entity_id="entity-123",
                external_id="external-456",
                processor_name="TWProcessor",
                status="STARTED",
                log_timestamp=now - timedelta(minutes=10),
                source_queue="timer-queue",
                destination_queue="tw-orders-queue",
                processing_duration_ms=150,
            ),
            PipelineStateHistory.create(
                tenant_id="test-tenant",
                entity_id="entity-123",
                external_id="external-456",
                processor_name="TWProcessor",
                status="COMPLETED",
                log_timestamp=now - timedelta(minutes=9),
                source_queue="timer-queue",
                destination_queue="tw-orders-queue",
                processing_duration_ms=2500,
                result_code="SUCCESS",
            ),
            PipelineStateHistory.create(
                tenant_id="test-tenant",
                entity_id="entity-123",
                external_id="external-456",
                processor_name="NetSuiteProcessor",
                status="STARTED",
                log_timestamp=now - timedelta(minutes=8),
                source_queue="tw-orders-queue",
                destination_queue="netsuite-success-queue",
                processing_duration_ms=100,
            ),
            PipelineStateHistory.create(
                tenant_id="test-tenant",
                entity_id="entity-123",
                external_id="external-456",
                processor_name="NetSuiteProcessor",
                status="COMPLETED",
                log_timestamp=now - timedelta(minutes=7),
                source_queue="tw-orders-queue",
                destination_queue="netsuite-success-queue",
                processing_duration_ms=3200,
                result_code="ORDER_CREATED",
            ),
            
            # Failed processing for entity-456
            PipelineStateHistory.create(
                tenant_id="test-tenant",
                entity_id="entity-456",
                external_id="external-789",
                processor_name="TWProcessor",
                status="STARTED",
                log_timestamp=now - timedelta(minutes=5),
                processing_duration_ms=120,
            ),
            PipelineStateHistory.create(
                tenant_id="test-tenant",
                entity_id="entity-456",
                external_id="external-789",
                processor_name="TWProcessor",
                status="FAILED",
                log_timestamp=now - timedelta(minutes=4),
                processing_duration_ms=800,
                error_message="Connection timeout to TW API",
                result_code="API_TIMEOUT",
            ),
            
            # Different tenant data (should be filtered out)
            PipelineStateHistory.create(
                tenant_id="other-tenant",
                entity_id="other-entity",
                processor_name="OtherProcessor",
                status="COMPLETED",
                log_timestamp=now - timedelta(minutes=3),
                processing_duration_ms=1000,
            ),
            
            # Older data (for time filtering tests)
            PipelineStateHistory.create(
                tenant_id="test-tenant",
                entity_id="old-entity",
                processor_name="OldProcessor",
                status="COMPLETED",
                log_timestamp=now - timedelta(days=2),
                processing_duration_ms=500,
            ),
        ]
        
        for record in records:
            db_session.add(record)
        db_session.commit()
        
        return {
            "records": records,
        }

    def test_service_initialization(self, db_session):
        """Test service initialization with session."""
        service = PipelineStateService(session=db_session)
        assert service.session is db_session

    def test_get_entity_timeline(self, service, sample_state_records, tenant_context_fixture):
        """Test getting processing timeline for a specific entity."""
        query = EntityQuery(entity_id="entity-123")
        
        with tenant_context("test-tenant"):
            timeline = service.get_entity_timeline(query)
        
        # Should get all 4 records for entity-123
        assert len(timeline) == 4
        
        # Verify all records are for the same entity
        for record in timeline:
            assert record.entity_id == "entity-123"
        
        # Verify timeline ordering (chronological)
        timestamps = [record.log_timestamp for record in timeline]
        assert timestamps == sorted(timestamps)

    def test_get_entity_timeline_nonexistent(self, service, tenant_context_fixture):
        """Test getting timeline for non-existent entity."""
        query = EntityQuery(entity_id="nonexistent-entity")
        
        with tenant_context("test-tenant"):
            timeline = service.get_entity_timeline(query)
        
        assert timeline == []

    def test_get_recent_activity_default_timeframe(self, service, sample_state_records, tenant_context_fixture):
        """Test getting recent activity with default 24-hour timeframe."""
        with tenant_context("test-tenant"):
            activity = service.get_recent_activity()
        
        # Should get 6 recent records (excluding the 2-day-old record and other tenant)
        assert len(activity) == 6
        
        # Verify records are ordered by timestamp (newest first)
        timestamps = [record.log_timestamp for record in activity]
        assert timestamps == sorted(timestamps, reverse=True)

    def test_get_recent_activity_custom_timeframe(self, service, sample_state_records, tenant_context_fixture):
        """Test getting recent activity with custom timeframe."""
        # Get only last 6 minutes of activity (0.1 hours)
        query = PipelineStateQuery(hours=1, limit=100)  # Use 1 hour to be more realistic
        
        with tenant_context("test-tenant"):
            activity = service.get_recent_activity(query)
        
        # Should get fewer records due to stricter time filter
        assert len(activity) >= 2  # At least the recent failed processing
        
        # Verify all records are within timeframe
        cutoff = datetime.utcnow() - timedelta(hours=1)
        for record in activity:
            assert record.log_timestamp >= cutoff

    def test_get_recent_activity_with_limit(self, service, sample_state_records, tenant_context_fixture):
        """Test getting recent activity with result limit."""
        query = PipelineStateQuery(hours=24, limit=3)
        
        with tenant_context("test-tenant"):
            activity = service.get_recent_activity(query)
        
        # Should respect the limit
        assert len(activity) <= 3

    def test_get_processor_metrics(self, service, sample_state_records, tenant_context_fixture):
        """Test getting processor performance metrics with aggregation."""
        with tenant_context("test-tenant"):
            response = service.get_processor_metrics()
        
        # Verify response structure
        assert hasattr(response, 'metrics')
        assert hasattr(response, 'period_hours')
        assert hasattr(response, 'generated_at')
        assert response.period_hours == 24
        
        metrics = response.metrics
        
        # Should get metrics grouped by processor and status
        # Expected: TWProcessor (STARTED, COMPLETED, FAILED), NetSuiteProcessor (STARTED, COMPLETED), OldProcessor (COMPLETED - excluded by time)
        # Within 24 hours: TWProcessor should have 2 STARTED, 1 COMPLETED, 1 FAILED; NetSuiteProcessor should have 1 STARTED, 1 COMPLETED
        
        # Verify we have metrics for different processors
        processor_names = {metric.processor_name for metric in metrics}
        assert "TWProcessor" in processor_names
        assert "NetSuiteProcessor" in processor_names
        assert "OldProcessor" not in processor_names  # Excluded by time filter
        
        # Find TWProcessor COMPLETED metrics
        tw_completed = next((m for m in metrics if m.processor_name == "TWProcessor" and m.status == "COMPLETED"), None)
        assert tw_completed is not None
        assert tw_completed.count == 1
        assert tw_completed.avg_duration_ms == 2500.0
        
        # Find TWProcessor FAILED metrics
        tw_failed = next((m for m in metrics if m.processor_name == "TWProcessor" and m.status == "FAILED"), None)
        assert tw_failed is not None
        assert tw_failed.count == 1
        assert tw_failed.avg_duration_ms == 800.0

    def test_get_failed_processing(self, service, sample_state_records, tenant_context_fixture):
        """Test getting recent failed processing attempts."""
        with tenant_context("test-tenant"):
            failed = service.get_failed_processing()
        
        # Should get 1 failed record
        assert len(failed) == 1
        
        failure = failed[0]
        assert failure.status == "FAILED"
        assert failure.processor_name == "TWProcessor"
        assert failure.error_message == "Connection timeout to TW API"
        assert failure.result_code == "API_TIMEOUT"
        assert failure.entity_id == "entity-456"

    def test_get_failed_processing_with_limit(self, service, sample_state_records, tenant_context_fixture):
        """Test getting failed processing with result limit."""
        query = PipelineStateQuery(hours=24, limit=10)
        
        with tenant_context("test-tenant"):
            failed = service.get_failed_processing(query)
        
        # Should respect limit (but we only have 1 failure anyway)
        assert len(failed) <= 10

    def test_get_status_summary(self, service, sample_state_records, tenant_context_fixture):
        """Test getting overall pipeline status summary with health metrics."""
        with tenant_context("test-tenant"):
            summary = service.get_status_summary()
        
        # Verify summary structure
        assert hasattr(summary, 'period_hours')
        assert hasattr(summary, 'total_processing_events')
        assert hasattr(summary, 'status_breakdown')
        assert hasattr(summary, 'success_rate_percentage')
        assert hasattr(summary, 'health_status')
        assert hasattr(summary, 'generated_at')
        
        # Verify calculations
        assert summary.period_hours == 24
        assert summary.total_processing_events == 6  # 6 records within 24 hours
        
        # Verify status breakdown
        status_breakdown = summary.status_breakdown
        assert status_breakdown["STARTED"] == 3  # TWProcessor (2) + NetSuiteProcessor (1) started
        assert status_breakdown["COMPLETED"] == 2  # TWProcessor + NetSuiteProcessor completed  
        assert status_breakdown["FAILED"] == 1  # TWProcessor failed
        
        # Verify success rate calculation: (6 - 1) / 6 * 100 = 83.33%
        assert summary.success_rate_percentage == 83.33
        
        # Verify health status (83.33% should be "degraded" since it's between 80-95%)
        assert summary.health_status == "degraded"

    def test_get_status_summary_healthy(self, service, db_session, tenant_context_fixture):
        """Test status summary with high success rate (healthy status)."""
        # Create mostly successful records
        now = datetime.utcnow()
        
        successful_records = []
        for i in range(10):
            record = PipelineStateHistory.create(
                tenant_id="test-tenant",
                entity_id=f"entity-{i}",
                processor_name="SuccessProcessor",
                status="COMPLETED",
                log_timestamp=now - timedelta(minutes=i),
                processing_duration_ms=1000,
            )
            successful_records.append(record)
            db_session.add(record)
        
        db_session.commit()
        
        with tenant_context("test-tenant"):
            summary = service.get_status_summary()
        
        # Should have 100% success rate and be healthy
        assert summary.success_rate_percentage == 100.0
        assert summary.health_status == "healthy"

    def test_get_status_summary_unhealthy(self, service, db_session, tenant_context_fixture):
        """Test status summary with low success rate (unhealthy status)."""
        # Create mostly failed records
        now = datetime.utcnow()
        
        # Create 1 success, 4 failures = 20% success rate
        success_record = PipelineStateHistory.create(
            tenant_id="test-tenant",
            entity_id="success-entity",
            processor_name="TestProcessor",
            status="COMPLETED",
            log_timestamp=now - timedelta(minutes=1),
            processing_duration_ms=1000,
        )
        db_session.add(success_record)
        
        for i in range(4):
            failed_record = PipelineStateHistory.create(
                tenant_id="test-tenant",
                entity_id=f"fail-entity-{i}",
                processor_name="TestProcessor",
                status="FAILED",
                log_timestamp=now - timedelta(minutes=i+2),
                processing_duration_ms=500,
                error_message="Test failure",
            )
            db_session.add(failed_record)
        
        db_session.commit()
        
        with tenant_context("test-tenant"):
            summary = service.get_status_summary()
        
        # Should have 20% success rate and be unhealthy
        assert summary.success_rate_percentage == 20.0
        assert summary.health_status == "unhealthy"

    def test_tenant_isolation_across_all_methods(self, service, sample_state_records, tenant_context_fixture):
        """Test that all service methods properly isolate by tenant."""
        # All methods should return empty results when called from wrong tenant context
        entity_query = EntityQuery(entity_id="entity-123")
        
        with tenant_context("wrong-tenant"):
            assert service.get_entity_timeline(entity_query) == []
            assert service.get_recent_activity() == []
            
            metrics_response = service.get_processor_metrics()
            assert metrics_response.metrics == []
            
            assert service.get_failed_processing() == []
            
            summary = service.get_status_summary()
            assert summary.total_processing_events == 0
            assert summary.success_rate_percentage == 0.0