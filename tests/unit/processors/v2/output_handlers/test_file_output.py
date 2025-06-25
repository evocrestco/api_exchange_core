"""
Unit tests for FileOutputHandler.

Tests the file system output handler with real file operations, following the 
NO MOCKS policy. These tests verify file writing, directory creation, different
output formats, error handling, and cleanup.

FileOutputHandler Implementation Reference:
Error Codes:
- INVALID_CONFIGURATION (can_retry=False)
- INVALID_FILE_PATTERN (can_retry=False) 
- UNSUPPORTED_OUTPUT_FORMAT (can_retry=False)
- CONTENT_FORMATTING_FAILED (can_retry=False)
- DIRECTORY_CREATION_FAILED (can_retry=True, retry_after=1)
- FILE_PERMISSION_DENIED (can_retry=False)
- FILE_SYSTEM_ERROR (can_retry=True, retry_after=5)
- FILE_WRITE_FAILED (can_retry=True, retry_after=2)
- UNEXPECTED_ERROR (can_retry=False)

Output Formats: json, jsonl, text
File Pattern Variables: message_id, correlation_id, timestamp, date, time, 
                       external_id, canonical_type, tenant_id
"""

import json
import tempfile
from datetime import UTC, datetime
from pathlib import Path

import pytest

from api_exchange_core.processors.processing_result import ProcessingResult, ProcessingStatus
from api_exchange_core.processors.v2.output_handlers.base import (
    OutputHandlerStatus,
)
from api_exchange_core.processors import FileOutputHandler


class TestFileOutputHandler:
    """Test the FileOutputHandler implementation."""
    
    @pytest.fixture
    def temp_dir(self):
        """Create a temporary directory for test files outside source tree."""
        with tempfile.TemporaryDirectory(prefix="file_output_test_") as temp_dir:
            yield Path(temp_dir)
    
    @pytest.fixture
    def handler(self, temp_dir):
        """Create a FileOutputHandler instance for testing."""
        return FileOutputHandler(
            destination=str(temp_dir / "output"),
            config={
                "output_format": "json",
                "append_mode": False,
                "create_directories": True,
                "file_pattern": "{message_id}.json",
                "pretty_print": True,
                "include_timestamp": True
            }
        )
    
    @pytest.fixture
    def jsonl_handler(self, temp_dir):
        """Create a JSONL FileOutputHandler for testing."""
        return FileOutputHandler(
            destination=str(temp_dir / "jsonl_output"),
            config={
                "output_format": "jsonl",
                "append_mode": True,
                "create_directories": True,
                "file_pattern": "data.jsonl"
            }
        )
    
    @pytest.fixture
    def text_handler(self, temp_dir):
        """Create a text FileOutputHandler for testing."""
        return FileOutputHandler(
            destination=str(temp_dir / "text_output"),
            config={
                "output_format": "text",
                "file_pattern": "{date}/{canonical_type}/{external_id}.txt",
                "include_timestamp": True
            }
        )
    
    def test_handler_initialization(self, handler, temp_dir):
        """Test handler initialization with custom config."""
        assert handler.destination == str(temp_dir / "output")
        assert handler.output_format == "json"
        assert handler.append_mode is False
        assert handler.create_directories is True
        assert handler.file_pattern == "{message_id}.json"
        assert handler.encoding == "utf-8"
        assert handler.pretty_print is True
        assert handler.include_timestamp is True
        assert handler.buffer_size == 8192
        assert handler.base_path == Path(temp_dir / "output")
        assert handler._handler_name == "FileOutputHandler"
    
    def test_minimal_handler_initialization(self, temp_dir):
        """Test handler initialization with minimal config."""
        handler = FileOutputHandler(destination=str(temp_dir))
        assert handler.output_format == "json"
        assert handler.append_mode is True  # Default
        assert handler.create_directories is True  # Default
        assert handler.file_pattern == "{message_id}.json"  # Default
    
    def test_validate_configuration_success(self, handler):
        """Test successful configuration validation."""
        assert handler.validate_configuration() is True
    
    def test_validate_configuration_no_destination(self):
        """Test validation fails without destination."""
        handler = FileOutputHandler(destination="")
        assert handler.validate_configuration() is False
    
    def test_validate_configuration_invalid_format(self, temp_dir):
        """Test validation fails with invalid output format."""
        handler = FileOutputHandler(
            destination=str(temp_dir),
            config={"output_format": "xml"}
        )
        assert handler.validate_configuration() is False
    
    def test_generate_file_path_basic_pattern(self, handler, create_test_message):
        """Test file path generation with basic pattern."""
        message = create_test_message()
        result = ProcessingResult(
            status=ProcessingStatus.SUCCESS,
            success=True,
            entities_created=[],
            entities_updated=[],
            processing_metadata={},
            processor_info={},
            processing_duration_ms=100.0,
            completed_at=datetime.now(UTC)
        )
        
        file_path = handler._generate_file_path(message, result)
        expected_path = handler.base_path / f"{message.message_id}.json"
        assert file_path == expected_path
    
    def test_generate_file_path_complex_pattern(self, temp_dir, create_test_entity, create_test_message):
        """Test file path generation with complex pattern."""
        entity = create_test_entity(
            external_id="test-entity-123",
            canonical_type="customer"
        )
        message = create_test_message(entity=entity)
        result = ProcessingResult(
            status=ProcessingStatus.SUCCESS,
            success=True,
            entities_created=[],
            entities_updated=[],
            processing_metadata={},
            processor_info={},
            processing_duration_ms=100.0,
            completed_at=datetime.now(UTC)
        )
        
        handler = FileOutputHandler(
            destination=str(temp_dir),
            config={
                "file_pattern": "{date}/{canonical_type}/{external_id}_{time}.json"
            }
        )
        
        file_path = handler._generate_file_path(message, result)
        path_parts = file_path.parts
        
        # Should have date directory
        assert len(path_parts) >= 3
        # Should contain canonical_type directory
        assert "customer" in str(file_path)
        # Should contain external_id in filename
        assert "test-entity-123" in file_path.name
        # Should end with .json
        assert file_path.suffix == ".json"
    
    def test_generate_file_path_invalid_pattern(self, handler, create_test_message):
        """Test file path generation with invalid pattern variables."""
        message = create_test_message()
        result = ProcessingResult(
            status=ProcessingStatus.SUCCESS,
            success=True,
            entities_created=[],
            entities_updated=[],
            processing_metadata={},
            processor_info={},
            processing_duration_ms=100.0,
            completed_at=datetime.now(UTC)
        )
        
        handler.file_pattern = "{invalid_variable}.json"
        
        from api_exchange_core.processors.v2.output_handlers.base import OutputHandlerError
        with pytest.raises(OutputHandlerError) as exc_info:
            handler._generate_file_path(message, result)
        
        error = exc_info.value
        assert error.error_code.value == "2001"  # ErrorCode.INVALID_FORMAT
        assert error.can_retry is False
        assert "invalid_variable" in str(error)
    
    def test_prepare_file_content_json_format(self, handler, create_test_message):
        """Test content preparation for JSON format."""
        message = create_test_message()
        result = ProcessingResult(
            status=ProcessingStatus.SUCCESS,
            success=True,
            entities_created=["entity-123"],
            entities_updated=["entity-456"],
            processing_metadata={"score": 95},
            processor_info={"name": "TestProcessor", "version": "1.0"},
            processing_duration_ms=150.5,
            completed_at=datetime.now(UTC)
        )
        
        content = handler._prepare_file_content(message, result)
        
        # Should be valid JSON
        data = json.loads(content)
        
        # Verify structure
        assert "message_id" in data
        assert "entity_reference" in data
        assert "payload" in data
        # processing_result not included in simplified format
        assert "file_output_metadata" in data  # include_timestamp=True
        
        # Verify message metadata
        assert data["message_id"] == message.message_id
        assert data["correlation_id"] == message.correlation_id
        assert data["message_type"] == "entity_processing"
        
        # Verify entity reference
        assert data["entity_reference"]["external_id"] == message.entity_reference.external_id
        assert data["entity_reference"]["canonical_type"] == message.entity_reference.canonical_type
        
        # Verify processing result
        assert data["processing_result"]["status"] == "success"
        assert data["processing_result"]["entities_created"] == ["entity-123"]
        assert data["processing_result"]["processor_info"]["name"] == "TestProcessor"
        
        # Verify file output metadata
        assert data["file_output_metadata"]["handler_name"] == "FileOutputHandler"
        assert data["file_output_metadata"]["output_format"] == "json"
    
    def test_prepare_file_content_jsonl_format(self, jsonl_handler, create_test_message):
        """Test content preparation for JSONL format."""
        message = create_test_message()
        result = ProcessingResult(
            status=ProcessingStatus.SUCCESS,
            success=True,
            entities_created=[],
            entities_updated=[],
            processing_metadata={},
            processor_info={},
            processing_duration_ms=100.0,
            completed_at=datetime.now(UTC)
        )
        
        content = jsonl_handler._prepare_file_content(message, result)
        
        # Should be valid JSON (single line)
        data = json.loads(content)
        
        # Should not be pretty-printed (no newlines except at end when written)
        assert "\n" not in content
        assert "message_id" in data
    
    def test_prepare_file_content_text_format(self, text_handler, create_test_message):
        """Test content preparation for text format."""
        message = create_test_message()
        result = ProcessingResult(
            status=ProcessingStatus.SUCCESS,
            success=True,
            entities_created=["entity-1", "entity-2"],
            entities_updated=["entity-3"],
            processing_metadata={"confidence": 0.95},
            processor_info={"name": "TextProcessor"},
            processing_duration_ms=75.3,
            completed_at=datetime.now(UTC)
        )
        
        content = text_handler._prepare_file_content(message, result)
        
        # Should be human-readable text
        lines = content.split("\n")
        
        # Check for expected content
        assert any("Written At:" in line for line in lines)  # include_timestamp=True
        assert any(f"Message ID: {message.message_id}" in line for line in lines)
        assert any(f"Entity: {message.entity_reference.external_id}" in line for line in lines)
        assert any("Status: success" in line for line in lines)
        assert any("Entities Created: 2" in line for line in lines)
        assert any("Entities Updated: 1" in line for line in lines)
        assert any("Processing Duration: 75.3ms" in line for line in lines)
        assert any("Payload:" in line for line in lines)
        assert any("Processing Metadata:" in line for line in lines)
        # TextProcessor name should appear in the JSON processing metadata
        assert "TextProcessor" in content
    
    def test_prepare_file_content_invalid_format(self, temp_dir, create_test_message):
        """Test content preparation with invalid format."""
        handler = FileOutputHandler(
            destination=str(temp_dir),
            config={"output_format": "json"}
        )
        # Manually set invalid format to bypass validation
        handler.output_format = "invalid"
        
        message = create_test_message()
        result = ProcessingResult(
            status=ProcessingStatus.SUCCESS,
            success=True,
            entities_created=[],
            entities_updated=[],
            processing_metadata={},
            processor_info={},
            processing_duration_ms=100.0,
            completed_at=datetime.now(UTC)
        )
        
        from api_exchange_core.processors.v2.output_handlers.base import OutputHandlerError
        with pytest.raises(OutputHandlerError) as exc_info:
            handler._prepare_file_content(message, result)
        
        error = exc_info.value
        # Implementation follows standard pattern: specific errors wrapped by outer except Exception
        assert error.error_code.value == "2001"  # ErrorCode.INVALID_FORMAT
        assert error.can_retry is False
    
    def test_ensure_directory_exists_creates_dirs(self, temp_dir):
        """Test directory creation when create_directories=True."""
        handler = FileOutputHandler(
            destination=str(temp_dir),
            config={"create_directories": True}
        )
        
        nested_path = temp_dir / "level1" / "level2" / "file.json"
        assert not nested_path.parent.exists()
        
        handler._ensure_directory_exists(nested_path)
        
        assert nested_path.parent.exists()
        assert nested_path.parent.is_dir()
    
    def test_ensure_directory_exists_skips_when_disabled(self, temp_dir):
        """Test directory creation when create_directories=False."""
        handler = FileOutputHandler(
            destination=str(temp_dir),
            config={"create_directories": False}
        )
        
        nested_path = temp_dir / "level1" / "level2" / "file.json"
        assert not nested_path.parent.exists()
        
        # Should not raise an error, just skip
        handler._ensure_directory_exists(nested_path)
        
        assert not nested_path.parent.exists()
    
    def test_handle_json_success(self, handler, create_test_message):
        """Test successful JSON file handling."""
        message = create_test_message()
        result = ProcessingResult(
            status=ProcessingStatus.SUCCESS,
            success=True,
            entities_created=["entity-123"],
            entities_updated=[],
            processing_metadata={"test": True},
            processor_info={"name": "TestProcessor"},
            processing_duration_ms=100.0,
            completed_at=datetime.now(UTC)
        )
        
        # Execute handler
        handler_result = handler.handle(message, result)
        
        # Verify result
        assert handler_result.success is True
        assert handler_result.status == OutputHandlerStatus.SUCCESS
        assert handler_result.handler_name == "FileOutputHandler"
        assert handler_result.execution_duration_ms > 0
        
        # Verify metadata
        assert "file_path" in handler_result.metadata
        assert "file_size_bytes" in handler_result.metadata
        assert "content_length" in handler_result.metadata
        assert handler_result.metadata["output_format"] == "json"
        assert handler_result.metadata["append_mode"] is False
        
        # Verify file was created
        file_path = Path(handler_result.metadata["file_path"])
        assert file_path.exists()
        assert file_path.is_file()
        
        # Verify file content
        with open(file_path, 'r', encoding='utf-8') as f:
            content = f.read()
            data = json.loads(content)
            assert data["message_id"] == message.message_id
            assert data["processing_result"]["entities_created"] == ["entity-123"]
    
    def test_handle_jsonl_append_mode(self, jsonl_handler, create_test_message):
        """Test JSONL file handling with append mode."""
        # Create first message
        message1 = create_test_message()
        result1 = ProcessingResult(
            status=ProcessingStatus.SUCCESS,
            success=True,
            entities_created=["entity-1"],
            entities_updated=[],
            processing_metadata={},
            processor_info={},
            processing_duration_ms=100.0,
            completed_at=datetime.now(UTC)
        )
        
        # Write first message
        result1_handler = jsonl_handler.handle(message1, result1)
        assert result1_handler.success is True
        
        # Create second message
        message2 = create_test_message()
        result2 = ProcessingResult(
            status=ProcessingStatus.SUCCESS,
            success=True,
            entities_created=["entity-2"],
            entities_updated=[],
            processing_metadata={},
            processor_info={},
            processing_duration_ms=150.0,
            completed_at=datetime.now(UTC)
        )
        
        # Write second message (should append)
        result2_handler = jsonl_handler.handle(message2, result2)
        assert result2_handler.success is True
        
        # Verify file contains both records
        file_path = Path(result2_handler.metadata["file_path"])
        with open(file_path, 'r', encoding='utf-8') as f:
            lines = f.readlines()
        
        assert len(lines) == 2
        # Each line should be valid JSON
        data1 = json.loads(lines[0].strip())
        data2 = json.loads(lines[1].strip())
        
        assert data1["message_id"] == message1.message_id
        assert data2["message_id"] == message2.message_id
        # processing_result not included in simplified format
    
    def test_handle_text_format_with_complex_path(self, text_handler, create_test_entity, create_test_message):
        """Test text format with complex file path pattern."""
        entity = create_test_entity(
            external_id="customer-456",
            canonical_type="customer"
        )
        message = create_test_message(entity=entity)
        result = ProcessingResult(
            status=ProcessingStatus.SUCCESS,
            success=True,
            entities_created=[],
            entities_updated=[],
            processing_metadata={"region": "us-west"},
            processor_info={"name": "CustomerProcessor"},
            processing_duration_ms=200.0,
            completed_at=datetime.now(UTC)
        )
        
        # Execute handler
        handler_result = text_handler.handle(message, result)
        
        assert handler_result.success is True
        
        # Verify file path structure
        file_path = Path(handler_result.metadata["file_path"])
        assert file_path.exists()
        
        # Should have date/canonical_type/external_id.txt structure
        path_parts = file_path.parts
        assert "customer" in str(file_path)
        assert "customer-456" in file_path.name
        assert file_path.suffix == ".txt"
        
        # Verify text content
        with open(file_path, 'r', encoding='utf-8') as f:
            content = f.read()
        
        assert "customer-456" in content
        assert "region" in content
        assert "us-west" in content
        # CustomerProcessor appears in the JSON processing metadata section
        assert "CustomerProcessor" in content
    
    def test_handle_invalid_configuration(self, create_test_message):
        """Test handling with invalid configuration."""
        handler = FileOutputHandler(
            destination="",  # Empty destination
            config={"output_format": "json"}
        )
        
        message = create_test_message()
        result = ProcessingResult(
            status=ProcessingStatus.SUCCESS,
            success=True,
            entities_created=[],
            entities_updated=[],
            processing_metadata={},
            processor_info={},
            processing_duration_ms=100.0,
            completed_at=datetime.now(UTC)
        )
        
        # Execute handler - should fail validation
        handler_result = handler.handle(message, result)
        
        # Verify failure
        assert handler_result.success is False
        assert handler_result.status == OutputHandlerStatus.FAILED
        assert handler_result.error_code == "1003"  # ErrorCode.CONFIGURATION_ERROR
        assert handler_result.can_retry is False
    
    def test_handle_permission_validation_failure(self, temp_dir, create_test_message):
        """Test handling when validation fails due to permission issues."""
        # Create a directory and remove write permissions
        restricted_dir = temp_dir / "restricted"
        restricted_dir.mkdir()
        
        handler = FileOutputHandler(
            destination=str(restricted_dir),
            config={
                "output_format": "json",
                "file_pattern": "test.json",
                "create_directories": False
            }
        )
        
        message = create_test_message()
        result = ProcessingResult(
            status=ProcessingStatus.SUCCESS,
            success=True,
            entities_created=[],
            entities_updated=[],
            processing_metadata={},
            processor_info={},
            processing_duration_ms=100.0,
            completed_at=datetime.now(UTC)
        )
        
        # Remove write permission from directory
        restricted_dir.chmod(0o444)
        
        try:
            # Execute handler - validation will fail first with INVALID_CONFIGURATION
            # because validate_configuration() checks permissions and returns False
            handler_result = handler.handle(message, result)
            
            # Verify failure
            assert handler_result.success is False
            assert handler_result.status == OutputHandlerStatus.FAILED
            assert handler_result.error_code == "1003"  # ErrorCode.CONFIGURATION_ERROR
            assert handler_result.can_retry is False
        finally:
            # Restore permissions for cleanup
            restricted_dir.chmod(0o755)
    
    def test_handle_file_permission_denied_during_write(self, temp_dir, create_test_message):
        """Test actual FILE_PERMISSION_DENIED during file writing."""
        # Create a file and make it read-only to trigger permission error during write
        test_file = temp_dir / "readonly.json"
        test_file.write_text("existing content")
        test_file.chmod(0o444)  # Read-only file
        
        handler = FileOutputHandler(
            destination=str(temp_dir),
            config={
                "output_format": "json",
                "file_pattern": "readonly.json",
                "append_mode": False,  # Try to overwrite read-only file
                "create_directories": False
            }
        )
        
        message = create_test_message()
        result = ProcessingResult(
            status=ProcessingStatus.SUCCESS,
            success=True,
            entities_created=[],
            entities_updated=[],
            processing_metadata={},
            processor_info={},
            processing_duration_ms=100.0,
            completed_at=datetime.now(UTC)
        )
        
        try:
            # Execute handler - should fail with permission error during actual write
            handler_result = handler.handle(message, result)
            
            # Verify failure
            assert handler_result.success is False
            assert handler_result.status == OutputHandlerStatus.FAILED
            assert handler_result.error_code == "4003"  # ErrorCode.PERMISSION_DENIED
            assert handler_result.can_retry is False
            assert "Permission denied" in handler_result.error_message
        finally:
            # Restore permissions for cleanup
            test_file.chmod(0o644)
    
    def test_handle_directory_creation_failure(self, temp_dir, create_test_message):
        """Test directory creation failure handling."""
        # Create a file where we want to create a directory
        blocking_file = temp_dir / "blocking_file"
        blocking_file.write_text("blocking content")
        
        handler = FileOutputHandler(
            destination=str(temp_dir),
            config={
                "file_pattern": "blocking_file/subdir/test.json",  # Try to create dir where file exists
                "create_directories": True
            }
        )
        
        message = create_test_message()
        result = ProcessingResult(
            status=ProcessingStatus.SUCCESS,
            success=True,
            entities_created=[],
            entities_updated=[],
            processing_metadata={},
            processor_info={},
            processing_duration_ms=100.0,
            completed_at=datetime.now(UTC)
        )
        
        # Execute handler - should fail directory creation
        handler_result = handler.handle(message, result)
        
        # Verify failure
        assert handler_result.success is False
        assert handler_result.status == OutputHandlerStatus.RETRYABLE_ERROR
        assert handler_result.error_code == "5002"  # ErrorCode.EXTERNAL_API_ERROR
        assert handler_result.can_retry is True
        assert handler_result.retry_after_seconds == 1
    
    def test_get_handler_info(self, handler):
        """Test get_handler_info returns expected metadata."""
        info = handler.get_handler_info()
        
        assert info["handler_name"] == "FileOutputHandler"
        assert "base_path" in info
        assert info["output_format"] == "json"
        assert info["append_mode"] is False
        assert info["create_directories"] is True
        assert info["file_pattern"] == "{message_id}.json"
        assert info["encoding"] == "utf-8"
        assert info["pretty_print"] is True
        assert info["include_timestamp"] is True
        assert info["buffer_size"] == 8192
        assert "path_exists" in info
        assert "path_writable" in info
        assert info["supports_retry"] is True
    
    def test_supports_retry(self, handler):
        """Test supports_retry returns True."""
        assert handler.supports_retry() is True
    
    def test_handler_repr(self, handler):
        """Test string representation."""
        expected = f"FileOutputHandler(destination='{handler.destination}')"
        assert repr(handler) == expected
    
    def test_handler_str(self, handler):
        """Test human-readable string."""
        expected = f"FileOutputHandler -> {handler.destination}"
        assert str(handler) == expected