"""
Pipeline state service for querying pipeline state history and metrics.

This service provides methods for retrieving and analyzing pipeline state data
that has been projected from logs into the database for efficient querying.
All inputs are validated using Pydantic schemas and outputs are properly typed.
"""

from datetime import datetime, timedelta
from typing import List, Optional

from sqlalchemy import and_, desc, func
from sqlalchemy.orm import Session

from ..context.operation_context import operation
from ..context.tenant_context import tenant_aware
from ..db.db_pipeline_state_models import PipelineStateHistory
from ..exceptions import ErrorCode, ServiceError
from ..schemas import (
    EntityQuery,
    PipelineHistoryResponse,
    PipelineStateHistoryRead,
    PipelineStateQuery,
    ProcessorMetricsRead,
    ProcessorMetricsResponse,
    StatusSummaryRead,
)
from .base_service import SessionManagedService


class PipelineStateService(SessionManagedService):
    """
    Service for querying pipeline state history and generating metrics.

    Provides efficient access to pipeline state data for monitoring,
    debugging, and analytics purposes.
    """

    def __init__(self, session: Optional[Session] = None, logger=None):
        """
        Initialize the pipeline state service.

        Args:
            session: Optional existing session
            logger: Optional logger instance
        """
        super().__init__(session=session, logger=logger)

    # Note: get_pipeline_history_by_correlation_id has been removed
    # Use get_entity_timeline instead for tracking entity processing history

    @tenant_aware
    @operation(name="get_entity_timeline")
    def get_entity_timeline(self, query: EntityQuery) -> List[PipelineStateHistoryRead]:
        """
        Get processing timeline for a specific entity.

        Args:
            query: EntityQuery containing entity_id to track

        Returns:
            List of PipelineStateHistoryRead records for the entity ordered by timestamp
        """
        try:
            tenant_id = self._get_current_tenant_id()

            db_query = (
                self.session.query(PipelineStateHistory)
                .filter(
                    and_(
                        PipelineStateHistory.tenant_id == tenant_id,
                        PipelineStateHistory.entity_id == query.entity_id,
                    )
                )
                .order_by(PipelineStateHistory.log_timestamp)
            )

            records = db_query.all()

            return [PipelineStateHistoryRead.model_validate(record) for record in records]

        except Exception as e:
            self._handle_service_exception("get_entity_timeline", e, query.entity_id)

    @tenant_aware
    @operation(name="get_recent_activity")
    def get_recent_activity(
        self, query: PipelineStateQuery = None
    ) -> List[PipelineStateHistoryRead]:
        """
        Get recent pipeline activity.

        Args:
            query: PipelineStateQuery with hours and limit parameters (defaults to 24 hours, 100 records)

        Returns:
            List of recent PipelineStateHistoryRead records
        """
        try:
            if query is None:
                query = PipelineStateQuery()

            tenant_id = self._get_current_tenant_id()
            cutoff_time = datetime.utcnow() - timedelta(hours=query.hours)

            db_query = (
                self.session.query(PipelineStateHistory)
                .filter(
                    and_(
                        PipelineStateHistory.tenant_id == tenant_id,
                        PipelineStateHistory.log_timestamp >= cutoff_time,
                    )
                )
                .order_by(desc(PipelineStateHistory.log_timestamp))
                .limit(query.limit)
            )

            records = db_query.all()

            return [PipelineStateHistoryRead.model_validate(record) for record in records]

        except Exception as e:
            self._handle_service_exception("get_recent_activity", e)

    @tenant_aware
    @operation(name="get_processor_metrics")
    def get_processor_metrics(self, query: PipelineStateQuery = None) -> ProcessorMetricsResponse:
        """
        Get processor performance metrics.

        Args:
            query: PipelineStateQuery with hours parameter (defaults to 24 hours)

        Returns:
            ProcessorMetricsResponse with aggregated metrics and metadata
        """
        try:
            if query is None:
                query = PipelineStateQuery()

            tenant_id = self._get_current_tenant_id()
            cutoff_time = datetime.utcnow() - timedelta(hours=query.hours)

            # Aggregate metrics by processor and status
            db_query = (
                self.session.query(
                    PipelineStateHistory.processor_name,
                    PipelineStateHistory.status,
                    func.count(PipelineStateHistory.id).label("count"),
                    func.avg(PipelineStateHistory.processing_duration_ms).label("avg_duration_ms"),
                    func.max(PipelineStateHistory.processing_duration_ms).label("max_duration_ms"),
                    func.min(PipelineStateHistory.processing_duration_ms).label("min_duration_ms"),
                )
                .filter(
                    and_(
                        PipelineStateHistory.tenant_id == tenant_id,
                        PipelineStateHistory.log_timestamp >= cutoff_time,
                    )
                )
                .group_by(PipelineStateHistory.processor_name, PipelineStateHistory.status)
                .order_by(PipelineStateHistory.processor_name, PipelineStateHistory.status)
            )

            results = db_query.all()

            metrics = [
                ProcessorMetricsRead(
                    processor_name=result.processor_name,
                    status=result.status,
                    count=result.count,
                    avg_duration_ms=(
                        float(result.avg_duration_ms) if result.avg_duration_ms else None
                    ),
                    max_duration_ms=result.max_duration_ms,
                    min_duration_ms=result.min_duration_ms,
                )
                for result in results
            ]

            return ProcessorMetricsResponse(
                metrics=metrics,
                period_hours=query.hours,
                generated_at=datetime.utcnow().isoformat(),
            )

        except Exception as e:
            self._handle_service_exception("get_processor_metrics", e)

    @tenant_aware
    @operation(name="get_failed_processing")
    def get_failed_processing(
        self, query: PipelineStateQuery = None
    ) -> List[PipelineStateHistoryRead]:
        """
        Get recent failed processing attempts.

        Args:
            query: PipelineStateQuery with hours and limit parameters (defaults to 24 hours, 100 records)

        Returns:
            List of PipelineStateHistoryRead records for failed processing
        """
        try:
            if query is None:
                query = PipelineStateQuery(limit=50)  # Default to 50 for failed processing

            tenant_id = self._get_current_tenant_id()
            cutoff_time = datetime.utcnow() - timedelta(hours=query.hours)

            db_query = (
                self.session.query(PipelineStateHistory)
                .filter(
                    and_(
                        PipelineStateHistory.tenant_id == tenant_id,
                        PipelineStateHistory.status == "FAILED",
                        PipelineStateHistory.log_timestamp >= cutoff_time,
                    )
                )
                .order_by(desc(PipelineStateHistory.log_timestamp))
                .limit(query.limit)
            )

            records = db_query.all()

            return [PipelineStateHistoryRead.model_validate(record) for record in records]

        except Exception as e:
            self._handle_service_exception("get_failed_processing", e)

    @tenant_aware
    @operation(name="get_status_summary")
    def get_status_summary(self, query: PipelineStateQuery = None) -> StatusSummaryRead:
        """
        Get overall status summary for the pipeline.

        Args:
            query: PipelineStateQuery with hours parameter (defaults to 24 hours)

        Returns:
            StatusSummaryRead with status counts and health metrics
        """
        try:
            if query is None:
                query = PipelineStateQuery()

            tenant_id = self._get_current_tenant_id()
            cutoff_time = datetime.utcnow() - timedelta(hours=query.hours)

            # Get status counts
            status_query = (
                self.session.query(
                    PipelineStateHistory.status, func.count(PipelineStateHistory.id).label("count")
                )
                .filter(
                    and_(
                        PipelineStateHistory.tenant_id == tenant_id,
                        PipelineStateHistory.log_timestamp >= cutoff_time,
                    )
                )
                .group_by(PipelineStateHistory.status)
            )

            status_results = status_query.all()
            status_counts = {result.status: result.count for result in status_results}

            # Calculate health metrics
            total_processing = sum(status_counts.values())
            failed_count = status_counts.get("FAILED", 0)
            success_rate = (
                ((total_processing - failed_count) / total_processing * 100)
                if total_processing > 0
                else 0
            )

            health_status = (
                "healthy"
                if success_rate >= 95
                else "degraded" if success_rate >= 80 else "unhealthy"
            )

            return StatusSummaryRead(
                period_hours=query.hours,
                total_processing_events=total_processing,
                status_breakdown=status_counts,
                success_rate_percentage=round(success_rate, 2),
                health_status=health_status,
                generated_at=datetime.utcnow().isoformat(),
            )

        except Exception as e:
            self._handle_service_exception("get_status_summary", e)
