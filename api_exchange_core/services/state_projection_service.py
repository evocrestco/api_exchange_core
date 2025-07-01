"""
State projection service for converting log entries to queryable database records.

This service implements event sourcing pattern by reading log entries and projecting
state changes into the pipeline_state_history table for monitoring and debugging.
"""

import re
from datetime import datetime
from typing import Dict, List, Optional

from sqlalchemy.exc import IntegrityError

from ..db import DatabaseManager, PipelineStateHistory
from ..utils.logger import get_logger

logger = get_logger()


class StateProjectionService:
    """
    Service for projecting log entries into pipeline state history table.

    This service parses log messages and creates structured state records
    that can be queried for monitoring and debugging pipeline flows.
    """

    def __init__(self, db_manager: DatabaseManager):
        self.db_manager = db_manager

    def process_log_entry(self, log_entry: Dict) -> Optional[PipelineStateHistory]:
        """
        Process a single log entry and create a state history record if applicable.

        Args:
            log_entry: Dictionary containing log data with fields like message, timestamp, etc.

        Returns:
            PipelineStateHistory record if created, None if log entry doesn't contain state info
        """
        try:
            # Extract basic log metadata
            timestamp_str = log_entry.get("timestamp") or log_entry.get("@timestamp")

            if not timestamp_str:
                logger.debug("Log entry missing timestamp, skipping")
                return None

            # Parse timestamp
            log_timestamp = self._parse_timestamp(timestamp_str)
            if not log_timestamp:
                logger.debug(f"Could not parse timestamp: {timestamp_str}")
                return None

            # Extract correlation ID
            correlation_id = self._extract_correlation_id(log_entry)
            if not correlation_id:
                logger.debug("Log entry missing correlation_id, skipping state projection")
                return None

            # Extract tenant ID
            tenant_id = self._extract_tenant_id(log_entry)
            if not tenant_id:
                logger.debug("Log entry missing tenant_id, skipping state projection")
                return None

            # Extract processor information
            processor_info = self._extract_processor_info(log_entry)
            if not processor_info:
                logger.debug("Log entry doesn't contain processor state information")
                return None

            # Create and save state record
            state_record = PipelineStateHistory.create(
                tenant_id=tenant_id,
                correlation_id=correlation_id,
                processor_name=processor_info["processor_name"],
                status=processor_info["status"],
                log_timestamp=log_timestamp,
                entity_id=processor_info.get("entity_id"),
                external_id=processor_info.get("external_id"),
                result_code=processor_info.get("result_code"),
                error_message=processor_info.get("error_message"),
                source_queue=processor_info.get("source_queue"),
                destination_queue=processor_info.get("destination_queue"),
                processing_duration_ms=processor_info.get("processing_duration_ms"),
                message_payload_hash=processor_info.get("message_payload_hash"),
            )

            # Save to database
            session = self.db_manager.get_session()
            try:
                session.add(state_record)
                session.commit()
                logger.debug(
                    f"Created state record for {correlation_id} | {processor_info['processor_name']} | {processor_info['status']}"
                )
                return state_record

            except IntegrityError as e:
                session.rollback()
                logger.debug(f"Duplicate state record, skipping: {e}")
                return None

            finally:
                session.close()

        except Exception as e:
            logger.error(f"Error processing log entry: {e}", exc_info=True)
            return None

    def process_log_batch(self, log_entries: List[Dict]) -> List[PipelineStateHistory]:
        """
        Process a batch of log entries efficiently.

        Args:
            log_entries: List of log entry dictionaries

        Returns:
            List of created state history records
        """
        created_records = []
        error_count = 0

        for log_entry in log_entries:
            try:
                record = self.process_log_entry(log_entry)
                if record:
                    created_records.append(record)
            except Exception as e:
                error_count += 1
                logger.warning(
                    f"Failed to process log entry in batch: {str(e)}",
                    extra={"log_entry": log_entry, "error": str(e), "error_type": type(e).__name__},
                )

        logger.info(
            f"Processed {len(log_entries)} log entries, created {len(created_records)} state records, {error_count} errors"
        )
        return created_records

    def _parse_timestamp(self, timestamp_str: str) -> Optional[datetime]:
        """Parse various timestamp formats."""
        try:
            # Try ISO format first
            if "T" in timestamp_str:
                return datetime.fromisoformat(timestamp_str.replace("Z", "+00:00"))

            # Try other common formats
            for fmt in ["%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M:%S.%f"]:
                try:
                    return datetime.strptime(timestamp_str, fmt)
                except ValueError:
                    continue

        except Exception as e:
            logger.debug(f"Failed to parse timestamp {timestamp_str}: {e}")

        return None

    def _extract_correlation_id(self, log_entry: Dict) -> Optional[str]:
        """Extract correlation ID from log entry."""
        # Check common locations for correlation ID
        for key in ["correlation_id", "correlationId", "traceId", "trace_id"]:
            if key in log_entry:
                return log_entry[key]

        # Check if it's in the message text
        message = log_entry.get("message", "")
        match = re.search(r"correlation_id[=:]([a-f0-9-]+)", message)
        if match:
            return match.group(1)

        return None

    def _extract_tenant_id(self, log_entry: Dict) -> Optional[str]:
        """Extract tenant ID from log entry."""
        # Check common locations
        for key in ["tenant_id", "tenantId", "tenant"]:
            if key in log_entry:
                return log_entry[key]

        # Check if it's in the message text
        message = log_entry.get("message", "")
        match = re.search(r"tenant_id[=:]([^|,\s]+)", message)
        if match:
            return match.group(1)

        return None

    def _extract_processor_info(self, log_entry: Dict) -> Optional[Dict]:
        """
        Extract processor state information from log entry.

        Returns:
            Dict with processor_name, status, and optional fields, or None if no state info
        """
        message = log_entry.get("message", "")

        # Look for processor state patterns
        state_patterns = [
            r"Processing started.*processor_class=(\w+)",
            r"Processing completed.*processor_class=(\w+)",
            r"Processing failed.*processor_class=(\w+)",
            r"(\w+Processor).*STARTED",
            r"(\w+Processor).*COMPLETED",
            r"(\w+Processor).*FAILED",
        ]

        processor_name = None
        for pattern in state_patterns:
            match = re.search(pattern, message)
            if match:
                processor_name = match.group(1)
                break

        if not processor_name:
            return None

        # Determine status
        status = "PROCESSING"
        if "started" in message.lower() or "STARTED" in message:
            status = "STARTED"
        elif "completed" in message.lower() or "COMPLETED" in message:
            status = "COMPLETED"
        elif "failed" in message.lower() or "FAILED" in message or "error" in message.lower():
            status = "FAILED"
        elif "retrying" in message.lower() or "RETRYING" in message:
            status = "RETRYING"

        info = {"processor_name": processor_name, "status": status}

        # Extract optional fields
        optional_extractions = {
            "entity_id": r"entity_id[=:]([a-f0-9-]+)",
            "external_id": r"external_id[=:]([^|,\s]+)",
            "result_code": r"result_code[=:]([^|,\s]+)",
            "source_queue": r"source_queue[=:]([^|,\s]+)",
            "destination_queue": r"destination_queue[=:]([^|,\s]+)",
            "processing_duration_ms": r"duration[=:](\d+)",
        }

        for field, pattern in optional_extractions.items():
            match = re.search(pattern, message)
            if match:
                value = match.group(1)
                if field == "processing_duration_ms":
                    try:
                        info[field] = int(value)
                    except ValueError:
                        pass
                else:
                    info[field] = value

        # Extract error message if status is FAILED
        if status == "FAILED":
            error_match = re.search(r"error[=:]([^|]+)", message, re.IGNORECASE)
            if error_match:
                info["error_message"] = error_match.group(1).strip()

        return info
