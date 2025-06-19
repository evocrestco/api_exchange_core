"""
File system output handler.

Provides output handling for local file system, useful for development,
testing, debugging, and scenarios where file-based processing is required.
"""

import json
import os
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any, Dict, Optional, Union

from .base import OutputHandler, OutputHandlerError, OutputHandlerResult
from .config import FileOutputHandlerConfig, OutputHandlerConfigFactory

if TYPE_CHECKING:
    from ...processing_result import ProcessingResult
    from ..message import Message


class FileOutputHandler(OutputHandler):
    """
    Output handler for local file system operations.

    Writes processed messages to files on the local file system. Supports
    various output formats and file organization strategies.

    Configuration:
        - output_format: "json", "jsonl", or "text" (default: "json")
        - append_mode: Whether to append to existing files (default: True)
        - create_directories: Whether to create parent directories (default: True)
        - file_pattern: Pattern for generating filenames (default: "{message_id}.json")
        - encoding: File encoding (default: "utf-8")
        - pretty_print: Whether to pretty-print JSON (default: True)
        - include_timestamp: Whether to include timestamp in output (default: True)
        - buffer_size: Buffer size for file operations (default: 8192)

    File pattern variables:
        - {message_id}: Original message ID
        - {correlation_id}: Message correlation ID
        - {timestamp}: Current timestamp (ISO format)
        - {date}: Current date (YYYY-MM-DD)
        - {time}: Current time (HH-MM-SS)
        - {external_id}: Entity external ID
        - {canonical_type}: Entity canonical type
        - {tenant_id}: Entity tenant ID

    Example:
        handler = FileOutputHandler(
            destination="/data/processed",
            config={
                "output_format": "jsonl",
                "file_pattern": "{date}/{canonical_type}/{external_id}.jsonl",
                "append_mode": True,
                "create_directories": True
            }
        )
    """

    def __init__(
        self,
        destination: str,
        config: Optional[Union[Dict[str, Any], FileOutputHandlerConfig]] = None,
    ):
        """
        Initialize the file system output handler.

        Args:
            destination: Base directory path or file path for output
            config: Configuration dictionary, FileOutputHandlerConfig object, or None
        """
        # Handle configuration
        if isinstance(config, FileOutputHandlerConfig):
            # Use the config object directly
            self._handler_config = config
            # Update destination if different
            if self._handler_config.destination != destination:
                self._handler_config.destination = destination
            # Update base_path if destination changed
            if self._handler_config.base_path == "./output":  # Default value
                self._handler_config.base_path = destination
            # Convert to dict for parent class
            config_dict = self._handler_config.to_dict()
        else:
            # Create config object from dict or defaults
            self._handler_config = OutputHandlerConfigFactory.create_config(
                "file", destination, config
            )
            # If no base_path specified, use destination
            if config is None or "base_path" not in config:
                self._handler_config.base_path = destination
            config_dict = config if config else {}

        # Initialize parent with dict config for backward compatibility
        super().__init__(destination, config_dict)

        # Extract configuration values
        self.output_format = self._handler_config.output_format.lower()
        self.append_mode = self._handler_config.append_mode
        self.create_directories = self._handler_config.create_directories
        self.file_pattern = self.config.get("file_pattern", "{message_id}.json")
        self.encoding = self.config.get("encoding", "utf-8")
        self.pretty_print = self.config.get("pretty_print", True)
        self.include_timestamp = self.config.get("include_timestamp", True)
        self.buffer_size = self.config.get("buffer_size", 8192)

        # Convert base path to Path object
        self.base_path = Path(self._handler_config.base_path)

    def validate_configuration(self) -> bool:
        """Validate file handler configuration."""
        if not self.destination:
            self.logger.error("No destination path provided for FileOutputHandler")
            return False

        if self.output_format not in ["json", "jsonl", "text"]:
            self.logger.error(
                f"Invalid output_format: {self.output_format}. Must be 'json', 'jsonl', or 'text'"
            )
            return False

        # Check if base path is valid
        try:
            self.base_path.resolve()
        except Exception as e:
            self.logger.error(f"Invalid destination path: {self.destination}. Error: {e}")
            return False

        # Check write permissions if path exists
        if self.base_path.exists():
            if not os.access(self.base_path, os.W_OK):
                self.logger.error(f"No write permission for path: {self.destination}")
                return False

        return True

    def _generate_file_path(self, message: "Message", result: "ProcessingResult") -> Path:
        """
        Generate the output file path based on the configured pattern.

        Args:
            message: Message being processed
            result: Processing result

        Returns:
            Path object for the output file
        """
        now = datetime.now(UTC)

        # Prepare pattern variables
        pattern_vars = {
            "message_id": message.message_id,
            "correlation_id": message.correlation_id,
            "timestamp": now.isoformat(),
            "date": now.strftime("%Y-%m-%d"),
            "time": now.strftime("%H-%M-%S"),
            "external_id": message.entity_reference.external_id,
            "canonical_type": message.entity_reference.canonical_type,
            "tenant_id": message.entity_reference.tenant_id,
        }

        # Generate filename from pattern
        try:
            filename = self.file_pattern.format(**pattern_vars)
        except KeyError as e:
            raise OutputHandlerError(
                f"Invalid variable in file_pattern: {e}",
                error_code="INVALID_FILE_PATTERN",
                can_retry=False,
                error_details={
                    "file_pattern": self.file_pattern,
                    "available_vars": list(pattern_vars.keys()),
                },
            )

        # Combine with base path
        file_path = self.base_path / filename

        return file_path

    def _prepare_file_content(self, message: "Message", result: "ProcessingResult") -> str:
        """
        Prepare the content to write to the file based on output format.

        Args:
            message: Message being processed
            result: Processing result

        Returns:
            String content to write to file
        """
        # Prepare data structure
        output_data = {
            "message_id": message.message_id,
            "correlation_id": message.correlation_id,
            "created_at": message.created_at.isoformat(),
            "message_type": message.message_type.value,
            "retry_count": message.retry_count,
            "entity_reference": {
                "id": message.entity_reference.id,
                "external_id": message.entity_reference.external_id,
                "canonical_type": message.entity_reference.canonical_type,
                "source": message.entity_reference.source,
                "version": message.entity_reference.version,
                "tenant_id": message.entity_reference.tenant_id,
            },
            "payload": message.payload,
            "processing_result": {
                "status": result.status.value,
                "success": result.success,
                "entities_created": result.entities_created,
                "entities_updated": result.entities_updated,
                "processing_metadata": result.processing_metadata,
                "processor_info": result.processor_info,
                "processing_duration_ms": result.processing_duration_ms,
                "completed_at": result.completed_at.isoformat(),
            },
        }

        # Add timestamp if configured
        if self.include_timestamp:
            output_data["file_output_metadata"] = {
                "written_at": datetime.now(UTC).isoformat(),
                "handler_name": self._handler_name,
                "output_format": self.output_format,
            }

        # Format content based on output format
        try:
            if self.output_format == "json":
                if self.pretty_print:
                    return json.dumps(output_data, indent=2, default=str)
                else:
                    return json.dumps(output_data, default=str)

            elif self.output_format == "jsonl":
                # JSON Lines format (one JSON object per line)
                return json.dumps(output_data, default=str)

            elif self.output_format == "text":
                # Human-readable text format
                lines = [
                    f"Message ID: {message.message_id}",
                    f"Correlation ID: {message.correlation_id}",
                    f"Entity: {message.entity_reference.external_id} ({message.entity_reference.canonical_type})",
                    f"Processor: {result.processor_info.get('name', 'unknown')}",
                    f"Status: {result.status.value}",
                    f"Success: {result.success}",
                    f"Processing Duration: {result.processing_duration_ms}ms",
                    f"Entities Created: {len(result.entities_created)}",
                    f"Entities Updated: {len(result.entities_updated)}",
                    "",
                    "Payload:",
                    json.dumps(message.payload, indent=2, default=str),
                ]

                if result.processing_metadata:
                    lines.extend(
                        [
                            "",
                            "Processing Metadata:",
                            json.dumps(result.processing_metadata, indent=2, default=str),
                        ]
                    )

                if self.include_timestamp:
                    lines.insert(0, f"Written At: {datetime.now(UTC).isoformat()}")
                    lines.insert(1, "")

                return "\n".join(lines)

            else:
                raise OutputHandlerError(
                    f"Unsupported output format: {self.output_format}",
                    error_code="UNSUPPORTED_OUTPUT_FORMAT",
                    can_retry=False,
                )

        except Exception as e:
            raise OutputHandlerError(
                f"Failed to format content for output format {self.output_format}",
                error_code="CONTENT_FORMATTING_FAILED",
                can_retry=False,
                error_details={
                    "output_format": self.output_format,
                    "message_id": message.message_id,
                },
                original_exception=e,
            )

    def _ensure_directory_exists(self, file_path: Path) -> None:
        """Ensure the parent directory exists, creating it if necessary."""
        if not self.create_directories:
            return

        parent_dir = file_path.parent
        if not parent_dir.exists():
            try:
                parent_dir.mkdir(parents=True, exist_ok=True)
                self.logger.debug(f"Created directory: {parent_dir}")
            except Exception as e:
                raise OutputHandlerError(
                    f"Failed to create directory: {parent_dir}",
                    error_code="DIRECTORY_CREATION_FAILED",
                    can_retry=True,
                    retry_after_seconds=1,  # Base delay for exponential backoff
                    error_details={"directory_path": str(parent_dir)},
                    original_exception=e,
                )

    def handle(self, message: "Message", result: "ProcessingResult") -> OutputHandlerResult:
        """
        Write processed message to file system.

        Args:
            message: Original message that was processed
            result: Processing result containing output data

        Returns:
            OutputHandlerResult indicating success or failure
        """

        def _execute_file_write():
            # Validate configuration
            if not self.validate_configuration():
                raise OutputHandlerError(
                    "Invalid file handler configuration",
                    error_code="INVALID_CONFIGURATION",
                    can_retry=False,
                )

            # Generate output file path
            file_path = self._generate_file_path(message, result)

            # Ensure directory exists
            self._ensure_directory_exists(file_path)

            # Prepare content
            content = self._prepare_file_content(message, result)

            # Determine write mode
            write_mode = "a" if self.append_mode else "w"

            # Write to file
            try:
                with open(
                    file_path, write_mode, encoding=self.encoding, buffering=self.buffer_size
                ) as f:
                    f.write(content)

                    # Add newline for JSONL format or append mode
                    if self.output_format == "jsonl" or self.append_mode:
                        f.write("\n")

                # Get file statistics
                file_stats = file_path.stat()

                self.logger.info(
                    "Message written to file successfully",
                    extra={
                        "file_path": str(file_path),
                        "message_id": message.message_id,
                        "correlation_id": message.correlation_id,
                        "output_format": self.output_format,
                        "file_size_bytes": file_stats.st_size,
                        "content_length": len(content),
                        "append_mode": self.append_mode,
                    },
                )

                return {
                    "file_path": str(file_path),
                    "file_size_bytes": file_stats.st_size,
                    "content_length": len(content),
                    "output_format": self.output_format,
                    "append_mode": self.append_mode,
                    "encoding": self.encoding,
                    "created_directories": self.create_directories,
                }

            except PermissionError as e:
                raise OutputHandlerError(
                    f"Permission denied when writing to file: {file_path}",
                    error_code="FILE_PERMISSION_DENIED",
                    can_retry=False,
                    error_details={
                        "file_path": str(file_path),
                        "write_mode": write_mode,
                        "message_id": message.message_id,
                    },
                    original_exception=e,
                )

            except OSError as e:
                # Disk space, file system errors, etc.
                raise OutputHandlerError(
                    f"OS error when writing to file: {file_path}",
                    error_code="FILE_SYSTEM_ERROR",
                    can_retry=True,
                    retry_after_seconds=3,  # Base delay for exponential backoff
                    error_details={
                        "file_path": str(file_path),
                        "write_mode": write_mode,
                        "message_id": message.message_id,
                        "os_error_code": getattr(e, "errno", None),
                    },
                    original_exception=e,
                )

            except Exception as e:
                # Other unexpected errors
                raise OutputHandlerError(
                    f"Unexpected error when writing to file: {file_path}",
                    error_code="FILE_WRITE_FAILED",
                    can_retry=True,
                    retry_after_seconds=1,  # Base delay for exponential backoff
                    error_details={
                        "file_path": str(file_path),
                        "message_id": message.message_id,
                        "error_type": type(e).__name__,
                    },
                    original_exception=e,
                )

        # Execute with timing
        try:
            file_metadata, duration_ms = self._execute_with_timing(_execute_file_write)

            return self._create_success_result(
                execution_duration_ms=duration_ms, metadata=file_metadata
            )

        except OutputHandlerError as e:
            # Re-raise handler errors with timing
            _, duration_ms = self._execute_with_timing(lambda: None)

            # Use exponential backoff for retryable errors
            if e.can_retry:
                return self._create_failure_result_with_backoff(
                    execution_duration_ms=duration_ms,
                    error_message=e.message,
                    retry_count=message.retry_count,
                    error_code=e.error_code,
                    base_delay=e.retry_after_seconds,
                    error_details=e.error_details,
                )
            else:
                return self._create_failure_result(
                    execution_duration_ms=duration_ms,
                    error_message=e.message,
                    error_code=e.error_code,
                    can_retry=e.can_retry,
                    retry_after_seconds=e.retry_after_seconds,
                    error_details=e.error_details,
                )

        except Exception as e:
            # Handle unexpected exceptions
            _, duration_ms = self._execute_with_timing(lambda: None)

            self.logger.error(
                "Unexpected error in FileOutputHandler",
                extra={
                    "destination": self.destination,
                    "message_id": message.message_id,
                    "error": str(e),
                    "error_type": type(e).__name__,
                },
                exc_info=True,
            )

            return self._create_failure_result_with_backoff(
                execution_duration_ms=duration_ms,
                error_message=f"Unexpected error: {str(e)}",
                retry_count=message.retry_count,
                error_code="UNEXPECTED_ERROR",
                base_delay=1,  # Base delay for exponential backoff
                error_details={"error_type": type(e).__name__, "message_id": message.message_id},
            )

    def get_handler_info(self) -> Dict[str, Any]:
        """Get file handler information."""
        base_info = super().get_handler_info()
        base_info.update(
            {
                "base_path": str(self.base_path),
                "output_format": self.output_format,
                "append_mode": self.append_mode,
                "create_directories": self.create_directories,
                "file_pattern": self.file_pattern,
                "encoding": self.encoding,
                "pretty_print": self.pretty_print,
                "include_timestamp": self.include_timestamp,
                "buffer_size": self.buffer_size,
                "path_exists": self.base_path.exists(),
                "path_writable": (
                    os.access(self.base_path, os.W_OK) if self.base_path.exists() else None
                ),
            }
        )
        return base_info
