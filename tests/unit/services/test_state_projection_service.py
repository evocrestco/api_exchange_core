"""Tests for StateProjectionService - log-to-database projection functionality."""

import uuid
from datetime import datetime
from unittest.mock import Mock, patch

import pytest

from api_exchange_core.context.tenant_context import tenant_context
from api_exchange_core.db import DatabaseManager, PipelineStateHistory
from api_exchange_core.exceptions import set_correlation_id, clear_correlation_id
from api_exchange_core.services.state_projection_service import StateProjectionService


class TestStateProjectionService:
    """Test cases for StateProjectionService log-to-database projection."""

    def test_service_initialization_with_db_manager(self, db_manager):
        """Test service initialization with database manager."""
        service = StateProjectionService(db_manager=db_manager)
        
        assert service.db_manager is db_manager

    def test_process_log_entry_returns_none_for_non_state_log(self, db_manager):
        """Test processing a non-state-transition log entry returns None."""
        service = StateProjectionService(db_manager=db_manager)
        
        # Create a regular log entry that's not a state transition
        log_entry = {
            "timestamp": "2024-01-15T10:30:00.000Z",
            "message": "Processing order data validation",
            "entity_id": "entity-123",
            "tenant_id": "test-tenant",
            "processor": "ValidationProcessor"
        }
        
        result = service.process_log_entry(log_entry)
        
        # Should return None for non-state logs
        assert result is None

    def test_process_log_entry_missing_timestamp(self, db_manager):
        """Test processing log entry without timestamp returns None."""
        service = StateProjectionService(db_manager=db_manager)
        
        log_entry = {
            "message": "State transition: RECEIVED → PROCESSING",
            "entity_id": "entity-123",
            "tenant_id": "test-tenant"
        }
        
        result = service.process_log_entry(log_entry)
        
        # Should return None for missing timestamp
        assert result is None

    def test_process_log_entry_missing_correlation_id(self, db_manager):
        """Test processing log entry without correlation ID returns None."""
        service = StateProjectionService(db_manager=db_manager)
        
        log_entry = {
            "timestamp": "2024-01-15T10:30:00.000Z",
            "message": "State transition: RECEIVED → PROCESSING",
            "entity_id": "entity-123",
            "tenant_id": "test-tenant"
        }
        
        result = service.process_log_entry(log_entry)
        
        # Should return None for missing correlation_id
        assert result is None

    def test_process_log_entry_missing_tenant_id(self, db_manager):
        """Test processing log entry without tenant ID returns None."""
        service = StateProjectionService(db_manager=db_manager)
        correlation_id = str(uuid.uuid4())
        
        log_entry = {
            "timestamp": "2024-01-15T10:30:00.000Z",
            "message": "State transition: RECEIVED → PROCESSING",
            "correlation_id": correlation_id,
            "entity_id": "entity-123"
        }
        
        result = service.process_log_entry(log_entry)
        
        # Should return None for missing tenant_id
        assert result is None

    def test_process_log_entry_missing_processor_info(self, db_manager):
        """Test processing log entry without processor info returns None."""
        service = StateProjectionService(db_manager=db_manager)
        correlation_id = str(uuid.uuid4())
        
        log_entry = {
            "timestamp": "2024-01-15T10:30:00.000Z",
            "message": "Some message without state transition info",
            "correlation_id": correlation_id,
            "tenant_id": "test-tenant",
            "entity_id": "entity-123"
        }
        
        result = service.process_log_entry(log_entry)
        
        # Should return None for missing processor info
        assert result is None

    @patch('api_exchange_core.services.state_projection_service.logger')
    def test_process_log_entry_logs_debug_messages(self, mock_logger, db_manager):
        """Test that debug messages are logged for various missing fields."""
        service = StateProjectionService(db_manager=db_manager)
        
        # Test missing timestamp
        log_entry_no_timestamp = {
            "message": "State transition: RECEIVED → PROCESSING"
        }
        
        result = service.process_log_entry(log_entry_no_timestamp)
        assert result is None
        mock_logger.debug.assert_called_with("Log entry missing timestamp, skipping")

    def test_process_log_batch_empty_list(self, db_manager):
        """Test processing empty log batch."""
        service = StateProjectionService(db_manager=db_manager)
        
        result = service.process_log_batch([])
        
        # Should return empty list for empty input
        assert result == []

    def test_process_log_batch_with_invalid_entries(self, db_manager):
        """Test batch processing with invalid entries."""
        service = StateProjectionService(db_manager=db_manager)
        
        # Create batch with invalid entries
        log_entries = [
            {
                "timestamp": "2024-01-15T10:30:00.000Z",
                "message": "Invalid entry - missing correlation_id"
            },
            {
                # Missing timestamp
                "message": "State transition: RECEIVED → PROCESSING",
                "correlation_id": str(uuid.uuid4())
            }
        ]
        
        result = service.process_log_batch(log_entries)
        
        # Should return empty list for all invalid entries
        assert result == []

    @patch('api_exchange_core.services.state_projection_service.StateProjectionService._parse_timestamp')
    def test_process_log_entry_invalid_timestamp_format(self, mock_parse_timestamp, db_manager):
        """Test handling of invalid timestamp format."""
        service = StateProjectionService(db_manager=db_manager)
        correlation_id = str(uuid.uuid4())
        
        # Mock timestamp parsing to return None (invalid format)
        mock_parse_timestamp.return_value = None
        
        log_entry = {
            "timestamp": "invalid-timestamp-format",
            "message": "State transition: RECEIVED → PROCESSING",
            "correlation_id": correlation_id,
            "tenant_id": "test-tenant"
        }
        
        result = service.process_log_entry(log_entry)
        
        # Should return None for invalid timestamp
        assert result is None
        mock_parse_timestamp.assert_called_once_with("invalid-timestamp-format")

    @patch('api_exchange_core.services.state_projection_service.StateProjectionService._extract_correlation_id')
    @patch('api_exchange_core.services.state_projection_service.StateProjectionService._parse_timestamp')
    def test_process_log_entry_extraction_methods_called(self, mock_parse_timestamp, mock_extract_correlation, db_manager):
        """Test that extraction methods are called in correct order."""
        service = StateProjectionService(db_manager=db_manager)
        
        # Mock successful timestamp parsing but failed correlation extraction
        mock_parse_timestamp.return_value = datetime.now()
        mock_extract_correlation.return_value = None
        
        log_entry = {
            "timestamp": "2024-01-15T10:30:00.000Z",
            "message": "State transition: RECEIVED → PROCESSING"
        }
        
        result = service.process_log_entry(log_entry)
        
        # Should return None when correlation extraction fails
        assert result is None
        
        # Verify methods were called
        mock_parse_timestamp.assert_called_once_with("2024-01-15T10:30:00.000Z")
        mock_extract_correlation.assert_called_once_with(log_entry)

    @patch('api_exchange_core.services.state_projection_service.StateProjectionService._extract_tenant_id')
    @patch('api_exchange_core.services.state_projection_service.StateProjectionService._extract_correlation_id')
    @patch('api_exchange_core.services.state_projection_service.StateProjectionService._parse_timestamp')
    def test_process_log_entry_tenant_extraction_called(self, mock_parse_timestamp, mock_extract_correlation, mock_extract_tenant, db_manager):
        """Test that tenant extraction is called after correlation extraction succeeds."""
        service = StateProjectionService(db_manager=db_manager)
        
        # Mock successful parsing steps but failed tenant extraction
        mock_parse_timestamp.return_value = datetime.now()
        mock_extract_correlation.return_value = str(uuid.uuid4())
        mock_extract_tenant.return_value = None
        
        log_entry = {
            "timestamp": "2024-01-15T10:30:00.000Z",
            "message": "State transition: RECEIVED → PROCESSING"
        }
        
        result = service.process_log_entry(log_entry)
        
        # Should return None when tenant extraction fails
        assert result is None
        
        # Verify tenant extraction was called
        mock_extract_tenant.assert_called_once_with(log_entry)

    @patch('api_exchange_core.services.state_projection_service.StateProjectionService._extract_processor_info')
    @patch('api_exchange_core.services.state_projection_service.StateProjectionService._extract_tenant_id')
    @patch('api_exchange_core.services.state_projection_service.StateProjectionService._extract_correlation_id')
    @patch('api_exchange_core.services.state_projection_service.StateProjectionService._parse_timestamp')
    def test_process_log_entry_processor_info_extraction_called(self, mock_parse_timestamp, mock_extract_correlation, mock_extract_tenant, mock_extract_processor, db_manager):
        """Test that processor info extraction is called after other extractions succeed."""
        service = StateProjectionService(db_manager=db_manager)
        
        # Mock successful parsing steps but failed processor extraction
        mock_parse_timestamp.return_value = datetime.now()
        mock_extract_correlation.return_value = str(uuid.uuid4())
        mock_extract_tenant.return_value = "test-tenant"
        mock_extract_processor.return_value = None
        
        log_entry = {
            "timestamp": "2024-01-15T10:30:00.000Z",
            "message": "State transition: RECEIVED → PROCESSING"
        }
        
        result = service.process_log_entry(log_entry)
        
        # Should return None when processor extraction fails
        assert result is None
        
        # Verify processor extraction was called
        mock_extract_processor.assert_called_once_with(log_entry)

    @patch('api_exchange_core.services.state_projection_service.logger')
    def test_process_log_entry_database_error_handling(self, mock_logger, db_manager):
        """Test that database errors are logged and don't crash the service."""
        service = StateProjectionService(db_manager=db_manager)
        
        # Mock db_manager.get_session to raise an exception
        db_manager.get_session = Mock(side_effect=Exception("Database connection failed"))
        
        log_entry = {
            "timestamp": "2024-01-15T10:30:00.000Z",
            "message": "State transition: RECEIVED → PROCESSING",
            "correlation_id": str(uuid.uuid4()),
            "tenant_id": "test-tenant"
        }
        
        # Should not raise exception
        result = service.process_log_entry(log_entry)
        
        # Should return None on database error
        assert result is None

    def test_process_log_batch_calls_process_log_entry_for_each(self, db_manager):
        """Test that process_log_batch calls process_log_entry for each entry."""
        service = StateProjectionService(db_manager=db_manager)
        
        # Create mock for process_log_entry
        with patch.object(service, 'process_log_entry') as mock_process:
            mock_process.return_value = None  # All entries return None
            
            log_entries = [
                {"timestamp": "2024-01-15T10:30:00.000Z", "message": "Entry 1"},
                {"timestamp": "2024-01-15T10:30:01.000Z", "message": "Entry 2"},
                {"timestamp": "2024-01-15T10:30:02.000Z", "message": "Entry 3"}
            ]
            
            result = service.process_log_batch(log_entries)
            
            # Should call process_log_entry for each entry
            assert mock_process.call_count == 3
            
            # Should return empty list when all entries return None
            assert result == []

    def test_process_log_batch_filters_none_results(self, db_manager):
        """Test that process_log_batch filters out None results."""
        service = StateProjectionService(db_manager=db_manager)
        
        # Create mock PipelineStateHistory object
        mock_state_record = Mock(spec=PipelineStateHistory)
        
        # Create mock for process_log_entry with mixed results
        with patch.object(service, 'process_log_entry') as mock_process:
            mock_process.side_effect = [None, mock_state_record, None, mock_state_record]
            
            log_entries = [
                {"message": "Entry 1"},
                {"message": "Entry 2"},
                {"message": "Entry 3"},
                {"message": "Entry 4"}
            ]
            
            result = service.process_log_batch(log_entries)
            
            # Should return only non-None results
            assert len(result) == 2
            assert all(r == mock_state_record for r in result)

    def test_constructor_requires_db_manager(self):
        """Test that constructor requires DatabaseManager parameter."""
        # Should raise TypeError when called without db_manager
        with pytest.raises(TypeError):
            StateProjectionService()

    @patch('api_exchange_core.services.state_projection_service.StateProjectionService.process_log_entry')
    def test_process_log_batch_exception_handling(self, mock_process_entry, db_manager):
        """Test that exceptions in process_log_entry don't stop batch processing."""
        service = StateProjectionService(db_manager=db_manager)
        
        # Mock process_log_entry to raise exception for second entry
        mock_state_record = Mock(spec=PipelineStateHistory)
        mock_process_entry.side_effect = [
            mock_state_record,  # First entry succeeds
            Exception("Processing failed"),  # Second entry fails
            mock_state_record   # Third entry succeeds
        ]
        
        log_entries = [
            {"message": "Entry 1"},
            {"message": "Entry 2"},
            {"message": "Entry 3"}
        ]
        
        # Should not raise exception and continue processing
        result = service.process_log_batch(log_entries)
        
        # Should return successful results only
        assert len(result) == 2
        assert all(r == mock_state_record for r in result)

    @patch('api_exchange_core.services.state_projection_service.logger')
    @patch('api_exchange_core.services.state_projection_service.StateProjectionService.process_log_entry')
    def test_process_log_batch_logs_errors(self, mock_process_entry, mock_logger, db_manager):
        """Test that batch processing logs errors properly."""
        service = StateProjectionService(db_manager=db_manager)
        
        # Mock process_log_entry to raise exception
        mock_process_entry.side_effect = Exception("Processing failed")
        
        log_entries = [{"message": "Entry 1"}]
        
        result = service.process_log_batch(log_entries)
        
        # Should return empty list
        assert result == []
        
        # Should log warning for the error
        mock_logger.warning.assert_called_once()
        warning_call_args = mock_logger.warning.call_args
        assert "Failed to process log entry in batch" in warning_call_args[0][0]
        assert "Processing failed" in warning_call_args[0][0]
        
        # Should log info with error count
        mock_logger.info.assert_called_once()
        info_call_args = mock_logger.info.call_args
        assert "1 errors" in info_call_args[0][0]