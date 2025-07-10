"""
Unit tests for custom flake8 rules.

Tests the AST-based checker plugins for framework patterns.
"""

import ast
import pytest
from typing import List, Tuple

from api_exchange_core.custom_flake8_rules import (
    FrameworkExceptionChecker,
    FrameworkExceptionVisitor,
    LoggingStandardsChecker,
    LoggingStandardsVisitor,
    UTCTimestampChecker,
    UTCTimestampVisitor
)


class TestFrameworkExceptionChecker:
    """Test framework exception checker and visitor."""
    
    def test_checker_initialization(self):
        """Test that checker initializes correctly."""
        tree = ast.parse("pass")
        checker = FrameworkExceptionChecker(tree, "test.py")
        
        assert checker.tree == tree
        assert checker.filename == "test.py"
        assert checker.name == "framework-exception-checker"
        assert checker.version == "0.1.0"
    
    def test_checker_run_no_errors(self):
        """Test checker run with no errors."""
        code = """
from api_exchange_core.exceptions import BaseError

def test_func():
    raise BaseError("Test error")
"""
        tree = ast.parse(code)
        checker = FrameworkExceptionChecker(tree)
        
        errors = list(checker.run())
        assert errors == []
    
    def test_checker_run_with_errors(self):
        """Test checker run that finds errors."""
        code = """
def test_func():
    raise ValueError("Bad value")
"""
        tree = ast.parse(code)
        checker = FrameworkExceptionChecker(tree)
        
        errors = list(checker.run())
        assert len(errors) == 1
        
        line, col, msg, error_type = errors[0]
        assert line == 3
        assert "EXC001" in msg
        assert "ValueError" in msg
        assert "ValidationError" in msg
        assert error_type == FrameworkExceptionChecker
    
    def test_visitor_framework_exceptions_allowed(self):
        """Test that framework exceptions are allowed."""
        visitor = FrameworkExceptionVisitor()
        
        # Test all framework exceptions are recognized
        framework_exceptions = [
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
            "OutputHandlerError"
        ]
        
        for exc_name in framework_exceptions:
            code = f"raise {exc_name}('test')"
            tree = ast.parse(code)
            visitor.visit(tree)
        
        assert visitor.errors == []
    
    def test_visitor_allowed_exceptions(self):
        """Test that system exceptions are allowed."""
        visitor = FrameworkExceptionVisitor()
        
        allowed_exceptions = [
            "StopIteration",
            "GeneratorExit", 
            "SystemExit",
            "KeyboardInterrupt"
        ]
        
        for exc_name in allowed_exceptions:
            code = f"raise {exc_name}('test')"
            tree = ast.parse(code)
            visitor.visit(tree)
        
        assert visitor.errors == []
    
    def test_visitor_imported_exceptions_tracking(self):
        """Test that imported exceptions are tracked."""
        code = """
from api_exchange_core.exceptions import CustomError, AnotherError

def test_func():
    raise CustomError("test")
    raise AnotherError("test")
"""
        tree = ast.parse(code)
        visitor = FrameworkExceptionVisitor()
        visitor.visit(tree)
        
        assert "CustomError" in visitor.imported_exceptions
        assert "AnotherError" in visitor.imported_exceptions
        assert visitor.errors == []
    
    def test_visitor_disallowed_exceptions(self):
        """Test that disallowed exceptions generate errors."""
        test_cases = [
            ("ValueError", "ValidationError"),
            ("RuntimeError", "ServiceError"), 
            ("TypeError", "ValidationError"),
            ("KeyError", "ValidationError"),
            ("AttributeError", "ServiceError"),
            ("NotImplementedError", "ServiceError"),
            ("IOError", "ExternalServiceError"),
            ("OSError", "ExternalServiceError"),
            ("Exception", "BaseError")
        ]
        
        for exc_name, expected_suggestion in test_cases:
            code = f"raise {exc_name}('test')"
            tree = ast.parse(code)
            visitor = FrameworkExceptionVisitor()
            visitor.visit(tree)
            
            assert len(visitor.errors) == 1
            line, col, msg = visitor.errors[0]
            assert f"EXC001 Do not raise {exc_name}" in msg
            assert expected_suggestion in msg
    
    def test_visitor_exception_name_extraction(self):
        """Test exception name extraction from different AST patterns."""
        visitor = FrameworkExceptionVisitor()
        
        # Test Call node (raise ValueError("message"))
        call_node = ast.Call(
            func=ast.Name(id="ValueError", ctx=ast.Load()),
            args=[],
            keywords=[]
        )
        assert visitor._get_exception_name(call_node) == "ValueError"
        
        # Test Call with Attribute (raise module.ValueError("message"))
        attr_call_node = ast.Call(
            func=ast.Attribute(
                value=ast.Name(id="module", ctx=ast.Load()),
                attr="ValueError",
                ctx=ast.Load()
            ),
            args=[],
            keywords=[]
        )
        assert visitor._get_exception_name(attr_call_node) == "ValueError"
        
        # Test Name node (raise existing_exception)
        name_node = ast.Name(id="ExistingError", ctx=ast.Load())
        assert visitor._get_exception_name(name_node) == "ExistingError"
    
    def test_visitor_suggestion_generation(self):
        """Test suggestion generation for common exceptions."""
        visitor = FrameworkExceptionVisitor()
        
        suggestions = {
            "ValueError": "ValidationError",
            "RuntimeError": "ServiceError",
            "TypeError": "ValidationError", 
            "KeyError": "ValidationError",
            "AttributeError": "ServiceError",
            "NotImplementedError": "CONFIGURATION_ERROR",
            "IOError": "ExternalServiceError",
            "OSError": "ExternalServiceError",
            "Exception": "BaseError"
        }
        
        for exc_name, expected_part in suggestions.items():
            suggestion = visitor._get_suggestion(exc_name)
            assert expected_part in suggestion
        
        # Test unknown exception
        unknown_suggestion = visitor._get_suggestion("UnknownError")
        assert "framework exception" in unknown_suggestion


class TestLoggingStandardsChecker:
    """Test logging standards checker and visitor."""
    
    def test_checker_initialization(self):
        """Test that checker initializes correctly."""
        tree = ast.parse("pass")
        checker = LoggingStandardsChecker(tree, "test.py")
        
        assert checker.tree == tree
        assert checker.filename == "test.py"
        assert checker.name == "logging-standards-checker"
        assert checker.version == "0.1.0"
    
    def test_checker_run_no_errors(self):
        """Test checker run with proper logging usage."""
        code = """
from api_exchange_core.utils.logger import get_logger

def test_func():
    logger = get_logger()
    logger.info("Test message")
"""
        tree = ast.parse(code)
        checker = LoggingStandardsChecker(tree)
        
        errors = list(checker.run())
        assert errors == []
    
    def test_checker_run_with_logging_getlogger_error(self):
        """Test checker finds logging.getLogger() usage."""
        code = """
import logging

def test_func():
    logger = logging.getLogger(__name__)
    logger.info("Test message")
"""
        tree = ast.parse(code)
        checker = LoggingStandardsChecker(tree)
        
        errors = list(checker.run())
        assert len(errors) == 1
        
        line, col, msg, error_type = errors[0]
        assert line == 5
        assert "LOG001" in msg
        assert "get_logger()" in msg
        assert "logging.getLogger()" in msg
        assert error_type == LoggingStandardsChecker
    
    def test_checker_run_with_direct_logger_creation(self):
        """Test checker finds direct Logger creation."""
        code = """
import logging

def test_func():
    logger = logging.Logger("test")
    logger.info("Test message")
"""
        tree = ast.parse(code)
        checker = LoggingStandardsChecker(tree)
        
        errors = list(checker.run())
        assert len(errors) == 1
        
        line, col, msg, error_type = errors[0]
        assert line == 5
        assert "LOG002" in msg
        assert "Logger instances directly" in msg
        assert error_type == LoggingStandardsChecker
    
    def test_visitor_exempt_files(self):
        """Test that exempt files are not checked."""
        exempt_files = [
            "exceptions.py",
            "logger.py", 
            "test_something.py",
            "conftest.py",
            "function_app.py"
        ]
        
        code = """
import logging
logger = logging.getLogger(__name__)
"""
        
        for filename in exempt_files:
            tree = ast.parse(code)
            visitor = LoggingStandardsVisitor(filename)
            visitor.visit(tree)
            
            assert visitor.errors == []
    
    def test_visitor_import_tracking(self):
        """Test that imports are tracked correctly."""
        # Test get_logger import tracking
        import_variations = [
            "from utils.logger import get_logger",
            "from api_exchange_core.utils.logger import get_logger",
            "from ..utils.logger import get_logger"
        ]
        
        for import_code in import_variations:
            tree = ast.parse(import_code)
            visitor = LoggingStandardsVisitor("test.py")
            visitor.visit(tree)
            
            assert visitor.has_get_logger_import is True
        
        # Test logging import tracking
        tree = ast.parse("import logging")
        visitor = LoggingStandardsVisitor("test.py")
        visitor.visit(tree)
        
        assert visitor.has_logging_import is True
    
    def test_visitor_logging_getlogger_detection(self):
        """Test detection of logging.getLogger() calls."""
        code = """
import logging

def test_func():
    # These should be detected
    logger1 = logging.getLogger()
    logger2 = logging.getLogger(__name__)
    logger3 = logging.getLogger("custom")
"""
        tree = ast.parse(code)
        visitor = LoggingStandardsVisitor("test.py")
        visitor.visit(tree)
        
        assert len(visitor.errors) == 3
        for error in visitor.errors:
            line, col, msg = error
            assert "LOG001" in msg
    
    def test_visitor_direct_logger_detection(self):
        """Test detection of direct Logger creation."""
        code = """
import logging

def test_func():
    logger = logging.Logger("test")
"""
        tree = ast.parse(code)
        visitor = LoggingStandardsVisitor("test.py")
        visitor.visit(tree)
        
        assert len(visitor.errors) == 1
        line, col, msg = visitor.errors[0]
        assert "LOG002" in msg


class TestUTCTimestampChecker:
    """Test UTC timestamp checker and visitor."""
    
    def test_checker_initialization(self):
        """Test that checker initializes correctly."""
        tree = ast.parse("pass")
        checker = UTCTimestampChecker(tree, "test.py")
        
        assert checker.tree == tree
        assert checker.filename == "test.py"
        assert checker.name == "utc-timestamp-checker"
        assert checker.version == "0.1.0"
    
    def test_checker_run_no_errors(self):
        """Test checker run with proper UTC usage."""
        code = """
from datetime import datetime, timezone

def test_func():
    now = datetime.now(timezone.utc)
    created_at = datetime(2024, 1, 1, tzinfo=timezone.utc)
"""
        tree = ast.parse(code)
        checker = UTCTimestampChecker(tree)
        
        errors = list(checker.run())
        assert errors == []
    
    def test_checker_run_datetime_now_error(self):
        """Test checker finds datetime.now() without UTC."""
        code = """
from datetime import datetime

def test_func():
    now = datetime.now()
"""
        tree = ast.parse(code)
        checker = UTCTimestampChecker(tree)
        
        errors = list(checker.run())
        assert len(errors) == 1
        
        line, col, msg, error_type = errors[0]
        assert line == 5
        assert "UTC001" in msg
        assert "datetime.now(timezone.utc)" in msg
        assert error_type == UTCTimestampChecker
    
    def test_checker_run_datetime_constructor_error(self):
        """Test checker finds datetime() constructor without UTC."""
        code = """
from datetime import datetime

def test_func():
    created = datetime(2024, 1, 1, 12, 0, 0)
"""
        tree = ast.parse(code)
        checker = UTCTimestampChecker(tree)
        
        errors = list(checker.run())
        assert len(errors) == 1
        
        line, col, msg, error_type = errors[0]
        assert line == 5
        assert "UTC002" in msg
        assert "timezone.utc" in msg
        assert error_type == UTCTimestampChecker
    
    def test_checker_run_fromtimestamp_error(self):
        """Test checker finds fromtimestamp() without UTC."""
        code = """
from datetime import datetime

def test_func():
    dt = datetime.fromtimestamp(1234567890)
"""
        tree = ast.parse(code)
        checker = UTCTimestampChecker(tree)
        
        errors = list(checker.run())
        assert len(errors) == 1
        
        line, col, msg, error_type = errors[0]
        assert line == 5
        assert "UTC003" in msg
        assert "timezone.utc" in msg
        assert error_type == UTCTimestampChecker
    
    def test_visitor_exempt_files(self):
        """Test that exempt files are not checked."""
        exempt_files = [
            "test_something.py",
            "conftest.py"
        ]
        
        code = """
from datetime import datetime
now = datetime.now()
"""
        
        for filename in exempt_files:
            tree = ast.parse(code)
            visitor = UTCTimestampVisitor(filename)
            visitor.visit(tree)
            
            assert visitor.errors == []
    
    def test_visitor_import_tracking(self):
        """Test that datetime imports are tracked."""
        # Test datetime import
        tree = ast.parse("from datetime import datetime")
        visitor = UTCTimestampVisitor("test.py")
        visitor.visit(tree)
        
        assert visitor.has_datetime_import is True
        
        # Test timezone import
        tree = ast.parse("from datetime import timezone")
        visitor = UTCTimestampVisitor("test.py")
        visitor.visit(tree)
        
        assert visitor.has_timezone_import is True
        
        # Test module import
        tree = ast.parse("import datetime")
        visitor = UTCTimestampVisitor("test.py")
        visitor.visit(tree)
        
        assert visitor.has_datetime_import is True
    
    def test_visitor_datetime_now_detection(self):
        """Test detection of datetime.now() calls."""
        test_cases = [
            "datetime.now()",  # Direct import
            "datetime.datetime.now()",  # Module import
        ]
        
        for code_pattern in test_cases:
            code = f"""
from datetime import datetime
result = {code_pattern}
"""
            tree = ast.parse(code)
            visitor = UTCTimestampVisitor("test.py")
            visitor.visit(tree)
            
            assert len(visitor.errors) == 1
            assert "UTC001" in visitor.errors[0][2]
    
    def test_visitor_datetime_constructor_detection(self):
        """Test detection of datetime() constructor calls."""
        test_cases = [
            "datetime(2024, 1, 1)",  # Direct import
            "datetime.datetime(2024, 1, 1)",  # Module import
        ]
        
        for code_pattern in test_cases:
            code = f"""
from datetime import datetime
result = {code_pattern}
"""
            tree = ast.parse(code)
            visitor = UTCTimestampVisitor("test.py")
            visitor.visit(tree)
            
            assert len(visitor.errors) == 1
            assert "UTC002" in visitor.errors[0][2]
    
    def test_visitor_fromtimestamp_detection(self):
        """Test detection of fromtimestamp() calls."""
        test_cases = [
            "datetime.fromtimestamp(123456)",  # Direct import
            "datetime.datetime.fromtimestamp(123456)",  # Module import
        ]
        
        for code_pattern in test_cases:
            code = f"""
from datetime import datetime
result = {code_pattern}
"""
            tree = ast.parse(code)
            visitor = UTCTimestampVisitor("test.py")
            visitor.visit(tree)
            
            assert len(visitor.errors) == 1
            assert "UTC003" in visitor.errors[0][2]
    
    def test_visitor_utc_timezone_detection(self):
        """Test detection of UTC timezone arguments."""
        utc_usage_cases = [
            "datetime.now(timezone.utc)",
            "datetime(2024, 1, 1, tzinfo=timezone.utc)",
            "datetime.fromtimestamp(123456, timezone.utc)",
            "datetime.fromtimestamp(123456, tz=timezone.utc)"
        ]
        
        for code_pattern in utc_usage_cases:
            code = f"""
from datetime import datetime, timezone
result = {code_pattern}
"""
            tree = ast.parse(code)
            visitor = UTCTimestampVisitor("test.py")
            visitor.visit(tree)
            
            # Should not generate errors when UTC is used
            assert visitor.errors == []
    
    def test_visitor_multiple_datetime_calls_same_line(self):
        """Test detection when multiple datetime.now() calls are on same line."""
        code = """
from datetime import datetime, timezone

def test_func():
    # This should trigger UTC001 for the second datetime.now() only
    now = datetime.now(timezone.utc) if some_tz else datetime.now()
"""
        tree = ast.parse(code)
        visitor = UTCTimestampVisitor("test.py")
        visitor.visit(tree)
        
        # Should detect the violation in the second datetime.now() only
        assert len(visitor.errors) == 1
        line, col, msg = visitor.errors[0]
        assert "UTC001" in msg
        assert line == 6  # Line with the problematic code
        assert col == 53  # Column of the second datetime.now()


class TestCustomFlake8RulesIntegration:
    """Integration tests for custom flake8 rules."""
    
    def test_all_checkers_run_without_errors_on_valid_code(self):
        """Test that all checkers pass on properly written code."""
        valid_code = """
from datetime import datetime, timezone
from api_exchange_core.exceptions import BaseError, ValidationError
from api_exchange_core.utils.logger import get_logger

def valid_function():
    logger = get_logger()
    
    try:
        # Proper UTC timestamp
        now = datetime.now(timezone.utc)
        logger.info("Current time", extra={"timestamp": now})
        
        # Proper exception usage
        if not now:
            raise ValidationError("Invalid timestamp")
            
    except ValidationError as e:
        logger.error("Validation failed", extra={"error": str(e)})
        raise BaseError("Processing failed") from e
"""
        tree = ast.parse(valid_code)
        
        # Test all checkers
        checkers = [
            FrameworkExceptionChecker(tree, "test.py"),
            LoggingStandardsChecker(tree, "test.py"),
            UTCTimestampChecker(tree, "test.py")
        ]
        
        for checker in checkers:
            errors = list(checker.run())
            assert errors == [], f"{checker.name} found unexpected errors: {errors}"
    
    def test_all_checkers_find_violations_in_bad_code(self):
        """Test that all checkers find violations in problematic code."""
        bad_code = """
import logging
from datetime import datetime

def bad_function():
    # Bad logging
    logger = logging.getLogger(__name__)
    
    # Bad datetime
    now = datetime.now()
    
    # Bad exception
    raise ValueError("Something went wrong")
"""
        tree = ast.parse(bad_code)
        
        # Test exception checker
        exc_checker = FrameworkExceptionChecker(tree, "bad_test.py")
        exc_errors = list(exc_checker.run())
        assert len(exc_errors) == 1
        assert "EXC001" in exc_errors[0][2]
        
        # Test logging checker
        log_checker = LoggingStandardsChecker(tree, "bad_test.py")
        log_errors = list(log_checker.run())
        assert len(log_errors) == 1
        assert "LOG001" in log_errors[0][2]
        
        # Test UTC checker
        utc_checker = UTCTimestampChecker(tree, "bad_test.py")
        utc_errors = list(utc_checker.run())
        assert len(utc_errors) == 1
        assert "UTC001" in utc_errors[0][2]
    
    def test_checkers_respect_exemption_patterns(self):
        """Test that checkers properly exempt certain files."""
        problem_code = """
import logging
from datetime import datetime

logger = logging.getLogger(__name__)
now = datetime.now()
raise ValueError("Test error")
"""
        tree = ast.parse(problem_code)
        
        # Test files that should be exempt
        exempt_files = [
            "test_something.py",
            "conftest.py", 
            "exceptions.py",
            "logger.py",
            "function_app.py"
        ]
        
        for filename in exempt_files:
            # Some checkers should be exempt, others shouldn't
            log_checker = LoggingStandardsChecker(tree, filename)
            log_errors = list(log_checker.run())
            
            utc_checker = UTCTimestampChecker(tree, filename)
            utc_errors = list(utc_checker.run())
            
            # Logging and UTC checkers should exempt test files
            if "test_" in filename or filename == "conftest.py":
                assert log_errors == []
                assert utc_errors == []
            
            # Logging checker should exempt logger.py and exceptions.py
            if filename in ["logger.py", "exceptions.py", "function_app.py"]:
                assert log_errors == []
    
    def test_error_message_formats(self):
        """Test that error messages are properly formatted."""
        code = """
import logging
from datetime import datetime

def test():
    logging.getLogger()
    datetime.now() 
    raise ValueError("test")
"""
        tree = ast.parse(code)
        
        # Collect all errors
        all_errors = []
        
        checkers = [
            ("EXC", FrameworkExceptionChecker(tree, "test.py")),
            ("LOG", LoggingStandardsChecker(tree, "test.py")),
            ("UTC", UTCTimestampChecker(tree, "test.py"))
        ]
        
        for prefix, checker in checkers:
            errors = list(checker.run())
            all_errors.extend(errors)
        
        # Verify error format
        assert len(all_errors) == 3
        
        for line, col, msg, error_type in all_errors:
            # Check line and column are integers
            assert isinstance(line, int)
            assert isinstance(col, int)
            
            # Check message format
            assert isinstance(msg, str)
            assert len(msg) > 0
            
            # Check error type
            assert error_type in [FrameworkExceptionChecker, LoggingStandardsChecker, UTCTimestampChecker]
            
            # Check error code format (XXX###)
            error_codes = ["EXC001", "LOG001", "LOG002", "UTC001", "UTC002", "UTC003"]
            assert any(code in msg for code in error_codes)