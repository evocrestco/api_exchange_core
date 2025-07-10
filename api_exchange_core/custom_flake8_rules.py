"""
Custom flake8 rules for enforcing framework patterns.

This module provides flake8 plugins that check for:
1. Usage of exceptions that don't inherit from our BaseError class
2. Usage of logging.getLogger() instead of our custom get_logger()
3. Usage of non-UTC datetime objects for timestamps
"""

import ast
from typing import Any, Generator, Optional, Set, Tuple, Type


class FrameworkExceptionChecker:
    """Check that all raised exceptions inherit from BaseError."""

    name = "framework-exception-checker"
    version = "0.1.0"

    def __init__(self, tree: ast.AST, filename: str = "") -> None:
        self.tree = tree
        self.filename = filename

    def run(self) -> Generator[Tuple[int, int, str, Type[Any]], None, None]:
        """Run the checker on the AST."""
        visitor = FrameworkExceptionVisitor()
        visitor.visit(self.tree)

        for line, col, msg in visitor.errors:
            yield line, col, msg, type(self)


class FrameworkExceptionVisitor(ast.NodeVisitor):
    """AST visitor to find exceptions that don't inherit from BaseError."""

    # Framework exception base classes and their subclasses
    FRAMEWORK_EXCEPTIONS = {
        "BaseError",
        "RepositoryError",
        "ServiceError",
        "ValidationError",
        "ExternalServiceError",
        "CredentialError",
        "CredentialNotFoundError",
        "CredentialExpiredError",
        "TenantIsolationViolationError",
        "TokenNotAvailableError",
        "OutputHandlerError",  # Output handler specific exception
    }

    # Exceptions that are allowed (e.g., in special cases)
    ALLOWED_EXCEPTIONS = {
        "StopIteration",  # Used in iterators
        "GeneratorExit",  # Used in generators
        "SystemExit",  # Used for program termination
        "KeyboardInterrupt",  # User interruption
    }

    def __init__(self) -> None:
        self.errors: list[tuple[int, int, str]] = []
        self.imported_exceptions: Set[str] = set()
        self.current_function: Optional[ast.FunctionDef] = None

    def visit_ImportFrom(self, node: ast.ImportFrom) -> None:
        """Track imported exceptions from our exceptions module."""
        if node.module and "exceptions" in node.module:
            for alias in node.names:
                if alias.name != "*":
                    self.imported_exceptions.add(alias.asname or alias.name)
        self.generic_visit(node)

    def visit_FunctionDef(self, node: ast.FunctionDef) -> None:
        """Track current function for context checking."""
        old_function = self.current_function
        self.current_function = node
        self.generic_visit(node)
        self.current_function = old_function

    def visit_Raise(self, node: ast.Raise) -> None:
        """Check raise statements for non-framework exceptions."""
        if node.exc:
            exc_name = self._get_exception_name(node.exc)

            # Skip if we can't determine the exception name
            if not exc_name:
                self.generic_visit(node)
                return

            # Allow ValueError in Pydantic validator functions
            if exc_name == "ValueError" and self._is_in_pydantic_validator():
                self.generic_visit(node)
                return

            # Check if it's a framework exception or allowed exception
            if exc_name not in self.FRAMEWORK_EXCEPTIONS and exc_name not in self.ALLOWED_EXCEPTIONS and exc_name not in self.imported_exceptions:

                # Provide specific suggestions for common exceptions
                suggestion = self._get_suggestion(exc_name)
                msg = f"EXC001 Do not raise {exc_name}. {suggestion}"
                self.errors.append((node.lineno, node.col_offset, msg))

        self.generic_visit(node)

    def _get_exception_name(self, node: ast.AST) -> str:
        """Extract exception name from various AST node types."""
        if isinstance(node, ast.Call):
            if isinstance(node.func, ast.Name):
                return node.func.id
            elif isinstance(node.func, ast.Attribute):
                return node.func.attr
        elif isinstance(node, ast.Name):
            return node.id

        return ""

    def _get_suggestion(self, exc_name: str) -> str:
        """Provide suggestions for common generic exceptions."""
        suggestions = {
            "ValueError": "Use ValidationError from ..exceptions instead",
            "RuntimeError": "Use ServiceError from ..exceptions instead",
            "TypeError": "Use ValidationError from ..exceptions instead",
            "KeyError": "Use ValidationError with field information instead",
            "AttributeError": "Use ServiceError from ..exceptions instead",
            "NotImplementedError": "Use ServiceError with CONFIGURATION_ERROR code instead",
            "IOError": "Use ExternalServiceError from ..exceptions instead",
            "OSError": "Use ExternalServiceError from ..exceptions instead",
            "Exception": "Use BaseError or a more specific framework exception instead",
        }

        return suggestions.get(exc_name, "Use a framework exception from ..exceptions instead")

    def _is_in_pydantic_validator(self) -> bool:
        """Check if current function is a Pydantic validator."""
        if not self.current_function:
            return False

        # Check if function has @validator or @field_validator decorator (Pydantic V1 and V2)
        for decorator in self.current_function.decorator_list:
            if isinstance(decorator, ast.Name) and decorator.id in ("validator", "field_validator"):
                return True
            elif isinstance(decorator, ast.Call) and isinstance(decorator.func, ast.Name) and decorator.func.id in ("validator", "field_validator"):
                return True

        return False


class LoggingStandardsChecker:
    """Check that code uses get_logger() instead of logging.getLogger()."""

    name = "logging-standards-checker"
    version = "0.1.0"

    def __init__(self, tree: ast.AST, filename: str = "") -> None:
        self.tree = tree
        self.filename = filename

    def run(self) -> Generator[Tuple[int, int, str, Type[Any]], None, None]:
        """Run the checker on the AST."""
        visitor = LoggingStandardsVisitor(self.filename)
        visitor.visit(self.tree)

        for line, col, msg in visitor.errors:
            yield line, col, msg, type(self)


class LoggingStandardsVisitor(ast.NodeVisitor):
    """AST visitor to find logging.getLogger() usage instead of get_logger()."""

    def __init__(self, filename: str) -> None:
        self.errors: list[tuple[int, int, str]] = []
        self.filename = filename
        self.has_get_logger_import = False
        self.has_logging_import = False

    def _is_exempt_file(self) -> bool:
        """Check if current file is exempt from logging rules."""
        exempt_patterns = [
            "exceptions.py",  # Framework exception logging
            "logger.py",  # Logging system implementation
            "test_",  # Test files
            "conftest.py",  # Test configuration
            "function_app.py",  # Azure Functions entry point
        ]

        for pattern in exempt_patterns:
            if pattern in self.filename:
                return True
        return False

    def visit_ImportFrom(self, node: ast.ImportFrom) -> None:
        """Track imports to see if get_logger is imported."""
        if node.module in [
            "utils.logger",
            "api_exchange_core.utils.logger",
            "..utils.logger",
        ] and any(alias.name == "get_logger" for alias in node.names):
            self.has_get_logger_import = True
        self.generic_visit(node)

    def visit_Import(self, node: ast.Import) -> None:
        """Track logging import."""
        for alias in node.names:
            if alias.name == "logging":
                self.has_logging_import = True
        self.generic_visit(node)

    def visit_Call(self, node: ast.Call) -> None:
        """Check for logging.getLogger() calls."""
        if self._is_exempt_file():
            self.generic_visit(node)
            return

        # Check for logging.getLogger() calls
        if self._is_logging_get_logger_call(node):
            msg = "LOG001 Use get_logger() from utils.logger instead of logging.getLogger()"
            self.errors.append((node.lineno, node.col_offset, msg))

        # Check for direct Logger creation
        elif self._is_direct_logger_creation(node):
            msg = "LOG002 Do not create Logger instances directly, use get_logger() instead"
            self.errors.append((node.lineno, node.col_offset, msg))

        self.generic_visit(node)

    def _is_logging_get_logger_call(self, node: ast.Call) -> bool:
        """Check if node is a logging.getLogger() call."""
        return (
            isinstance(node.func, ast.Attribute)
            and isinstance(node.func.value, ast.Name)
            and node.func.value.id == "logging"
            and node.func.attr == "getLogger"
        )

    def _is_direct_logger_creation(self, node: ast.Call) -> bool:
        """Check if node creates a Logger directly."""
        return (
            isinstance(node.func, ast.Attribute)
            and isinstance(node.func.value, ast.Name)
            and node.func.value.id == "logging"
            and node.func.attr == "Logger"
        )


class UTCTimestampChecker:
    """Check that all timestamp operations use UTC timezone."""

    name = "utc-timestamp-checker"
    version = "0.1.0"

    def __init__(self, tree: ast.AST, filename: str = "") -> None:
        self.tree = tree
        self.filename = filename

    def run(self) -> Generator[Tuple[int, int, str, Type[Any]], None, None]:
        """Run the checker on the AST."""
        visitor = UTCTimestampVisitor(self.filename)
        visitor.visit(self.tree)

        for line, col, msg in visitor.errors:
            yield line, col, msg, type(self)


class UTCTimestampVisitor(ast.NodeVisitor):
    """AST visitor to find non-UTC datetime usage in timestamp contexts."""

    def __init__(self, filename: str) -> None:
        self.errors: list[tuple[int, int, str]] = []
        self.filename = filename
        self.has_datetime_import = False
        self.has_timezone_import = False

    def _is_exempt_file(self) -> bool:
        """Check if current file is exempt from UTC rules."""
        exempt_patterns = [
            "test_",  # Test files may use local times for testing
            "conftest.py",  # Test configuration
        ]

        for pattern in exempt_patterns:
            if pattern in self.filename:
                return True
        return False

    def visit_ImportFrom(self, node: ast.ImportFrom) -> None:
        """Track datetime imports."""
        if node.module == "datetime":
            for alias in node.names:
                if alias.name in ["datetime", "timezone"]:
                    if alias.name == "datetime":
                        self.has_datetime_import = True
                    elif alias.name == "timezone":
                        self.has_timezone_import = True
        self.generic_visit(node)

    def visit_Import(self, node: ast.Import) -> None:
        """Track datetime module import."""
        for alias in node.names:
            if alias.name == "datetime":
                self.has_datetime_import = True
        self.generic_visit(node)

    def visit_Call(self, node: ast.Call) -> None:
        """Check for datetime calls without UTC."""
        if self._is_exempt_file():
            self.generic_visit(node)
            return

        # Check for datetime.now() without timezone
        if self._is_datetime_now_call(node):
            if not self._has_utc_timezone_arg(node):
                msg = "UTC001 Use datetime.now(timezone.utc) instead of datetime.now() for timestamps"
                self.errors.append((node.lineno, node.col_offset, msg))

        # Check for datetime.datetime() constructor without timezone
        elif self._is_datetime_constructor_call(node):
            if not self._has_utc_timezone_arg(node):
                msg = "UTC002 Use timezone.utc for datetime constructors in timestamp contexts"
                self.errors.append((node.lineno, node.col_offset, msg))

        # Check for datetime.fromtimestamp() without timezone
        elif self._is_datetime_fromtimestamp_call(node):
            if not self._has_utc_timezone_arg(node):
                msg = "UTC003 Use datetime.fromtimestamp(ts, timezone.utc) for timestamp conversion"
                self.errors.append((node.lineno, node.col_offset, msg))

        self.generic_visit(node)

    def _is_datetime_now_call(self, node: ast.Call) -> bool:
        """Check if node is a datetime.now() call."""
        return (
            isinstance(node.func, ast.Attribute)
            and node.func.attr == "now"
            and (
                (isinstance(node.func.value, ast.Name) and node.func.value.id == "datetime")
                or (
                    isinstance(node.func.value, ast.Attribute)
                    and isinstance(node.func.value.value, ast.Name)
                    and node.func.value.value.id == "datetime"
                    and node.func.value.attr == "datetime"
                )
            )
        )

    def _is_datetime_constructor_call(self, node: ast.Call) -> bool:
        """Check if node is a datetime() constructor call."""
        return (isinstance(node.func, ast.Name) and node.func.id == "datetime") or (
            isinstance(node.func, ast.Attribute)
            and isinstance(node.func.value, ast.Name)
            and node.func.value.id == "datetime"
            and node.func.attr == "datetime"
        )

    def _is_datetime_fromtimestamp_call(self, node: ast.Call) -> bool:
        """Check if node is a datetime.fromtimestamp() call."""
        return (
            isinstance(node.func, ast.Attribute)
            and node.func.attr == "fromtimestamp"
            and (
                (isinstance(node.func.value, ast.Name) and node.func.value.id == "datetime")
                or (
                    isinstance(node.func.value, ast.Attribute)
                    and isinstance(node.func.value.value, ast.Name)
                    and node.func.value.value.id == "datetime"
                    and node.func.value.attr == "datetime"
                )
            )
        )

    def _has_utc_timezone_arg(self, node: ast.Call) -> bool:
        """Check if call has timezone.utc as an argument."""
        # Check positional args
        for arg in node.args:
            if self._is_timezone_utc(arg):
                return True

        # Check keyword args
        for keyword in node.keywords:
            if keyword.arg in ["tz", "tzinfo"] and self._is_timezone_utc(keyword.value):
                return True

        return False

    def _is_timezone_utc(self, node: ast.AST) -> bool:
        """Check if node represents timezone.utc or UTC."""
        # Check for timezone.utc
        if isinstance(node, ast.Attribute) and isinstance(node.value, ast.Name) and node.value.id == "timezone" and node.attr == "utc":
            return True

        # Check for UTC (from datetime import UTC)
        if isinstance(node, ast.Name) and node.id == "UTC":
            return True

        return False
