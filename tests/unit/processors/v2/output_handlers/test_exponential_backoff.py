"""
Test exponential backoff functionality in output handlers.

Tests the calculate_exponential_backoff function and verify that output handlers
use exponential backoff correctly for retry logic.
"""

import pytest

from src.processors.v2.output_handlers.base import OutputHandlerError, calculate_exponential_backoff


class TestExponentialBackoff:
    """Test the exponential backoff calculation function."""
    
    def test_exponential_backoff_basic_progression(self):
        """Test exponential backoff follows expected progression."""
        # Test basic exponential progression
        for retry_count in range(8):
            delay = calculate_exponential_backoff(
                retry_count=retry_count,
                base_delay=1,
                max_delay=300,
                multiplier=2.0,
                jitter=False  # No jitter for predictable testing
            )
            expected = min(1 * (2 ** retry_count), 300)
            assert delay == expected, f"Retry {retry_count}: expected {expected}, got {delay}"
    
    def test_exponential_backoff_with_custom_base_delay(self):
        """Test exponential backoff with different base delays."""
        test_cases = [
            {"base_delay": 1, "name": "1 second base"},
            {"base_delay": 2, "name": "2 second base"},
            {"base_delay": 3, "name": "3 second base"},
            {"base_delay": 5, "name": "5 second base"},
        ]
        
        for case in test_cases:
            base_delay = case["base_delay"]
            
            for retry_count in range(6):
                delay = calculate_exponential_backoff(
                    retry_count=retry_count,
                    base_delay=base_delay,
                    max_delay=300,
                    multiplier=2.0,
                    jitter=False
                )
                
                expected = min(base_delay * (2 ** retry_count), 300)
                assert delay == expected, f"{case['name']}, retry {retry_count}: expected {expected}, got {delay}"
    
    def test_exponential_backoff_max_delay_cap(self):
        """Test that exponential backoff respects max_delay."""
        # Test with low max_delay to ensure capping works
        for retry_count in range(10):
            delay = calculate_exponential_backoff(
                retry_count=retry_count,
                base_delay=10,
                max_delay=50,  # Low max to test capping
                multiplier=2.0,
                jitter=False
            )
            
            assert delay <= 50, f"Delay {delay} exceeds max_delay 50"
            
            # Should hit max at retry_count >= 3 (10 * 2^3 = 80 > 50)
            if retry_count >= 3:
                assert delay == 50, f"Should be capped at 50 for retry {retry_count}"
    
    def test_exponential_backoff_with_jitter(self):
        """Test exponential backoff with jitter produces values in expected range."""
        for retry_count in range(5):
            delay = calculate_exponential_backoff(
                retry_count=retry_count,
                base_delay=2,
                max_delay=100,
                multiplier=2.0,
                jitter=True
            )
            
            # Calculate expected base delay
            base_delay_calculated = min(2 * (2 ** retry_count), 100)
            
            # Jitter should keep delay within ±25% but not below the original base_delay (2)
            min_delay = max(base_delay_calculated * 0.75, 2)  # Can't go below original base_delay
            max_delay = base_delay_calculated * 1.25  # +25% jitter
            
            assert min_delay <= delay <= max_delay, \
                f"Retry {retry_count}: delay {delay} outside range [{min_delay}, {max_delay}]"
    
    def test_exponential_backoff_edge_cases(self):
        """Test exponential backoff edge cases."""
        # Negative retry count
        delay = calculate_exponential_backoff(retry_count=-1, base_delay=5)
        assert delay == 5, "Negative retry_count should return base_delay"
        
        # Zero retry count
        delay = calculate_exponential_backoff(retry_count=0, base_delay=3)
        assert delay == 3, "Zero retry_count should return base_delay"
        
        # Very large retry count (with jitter disabled for predictable result)
        delay = calculate_exponential_backoff(retry_count=20, base_delay=1, max_delay=300, jitter=False)
        assert delay == 300, "Large retry_count should be capped at max_delay"
    
    def test_exponential_backoff_custom_multiplier(self):
        """Test exponential backoff with custom multiplier."""
        # Test with multiplier of 3
        for retry_count in range(5):
            delay = calculate_exponential_backoff(
                retry_count=retry_count,
                base_delay=1,
                max_delay=300,
                multiplier=3.0,
                jitter=False
            )
            
            expected = min(1 * (3 ** retry_count), 300)
            assert delay == expected, f"Multiplier 3.0, retry {retry_count}: expected {expected}, got {delay}"


class TestOutputHandlerErrorRetryDelay:
    """Test OutputHandlerError retry delay calculation."""
    
    def test_calculate_retry_delay_basic(self):
        """Test OutputHandlerError calculate_retry_delay method."""
        error = OutputHandlerError(
            "Test error",
            error_code="TEST_ERROR",
            can_retry=True,
            retry_after_seconds=3
        )
        
        # Test progression
        for retry_count in range(5):
            delay = error.calculate_retry_delay(retry_count)
            assert delay >= 3, f"Delay should be at least base delay (3s), got {delay}"
            assert delay <= 300, f"Delay should not exceed max (300s), got {delay}"
    
    def test_calculate_retry_delay_with_override(self):
        """Test calculate_retry_delay with base_delay override."""
        error = OutputHandlerError(
            "Test error",
            error_code="TEST_ERROR",
            can_retry=True,
            retry_after_seconds=5
        )
        
        # Override base delay - but remember jitter is enabled by default
        delay = error.calculate_retry_delay(retry_count=0, base_delay=10)
        # With jitter, should be within ±25% of 10, but not below 10
        assert 10 <= delay <= 12.5, f"Should use override base_delay ±25%, got {delay}"
        
        delay = error.calculate_retry_delay(retry_count=1, base_delay=10)
        # With jitter, should be within ±25% of 20, but not below 10
        assert 15 <= delay <= 25, f"Should double override base_delay ±25%, got {delay}"
    
    def test_calculate_retry_delay_no_retry_after_seconds(self):
        """Test calculate_retry_delay when retry_after_seconds is None."""
        error = OutputHandlerError(
            "Test error",
            error_code="TEST_ERROR",
            can_retry=True,
            retry_after_seconds=None
        )
        
        # Should default to 1 second
        delay = error.calculate_retry_delay(retry_count=0)
        assert delay == 1, f"Should default to 1 second, got {delay}"
        
        delay = error.calculate_retry_delay(retry_count=1)
        assert delay == 2, f"Should double to 2 seconds, got {delay}"


class TestExponentialBackoffIntegration:
    """Test exponential backoff integration with output handlers."""
    
    def test_exponential_backoff_progression_demonstration(self):
        """Demonstrate exponential backoff progression for different handler types."""
        # This test documents the expected behavior for each handler type
        
        handler_configs = [
            {"base_delay": 1, "name": "FILE_WRITE_FAILED (base=1s)"},
            {"base_delay": 2, "name": "QUEUE_SEND_FAILED (base=2s)"},
            {"base_delay": 3, "name": "FILE_SYSTEM_ERROR (base=3s)"},
            {"base_delay": 5, "name": "AZURE_SERVICE_ERROR (base=5s)"},
        ]
        
        expected_progressions = {}
        
        for config in handler_configs:
            base_delay = config["base_delay"]
            name = config["name"]
            progression = []
            
            for retry_count in range(6):
                delay = calculate_exponential_backoff(
                    retry_count=retry_count,
                    base_delay=base_delay,
                    max_delay=300,
                    multiplier=2.0,
                    jitter=False
                )
                progression.append(delay)
            
            expected_progressions[name] = progression
            
            # Verify exponential growth
            for i in range(1, len(progression)):
                if progression[i] < 300:  # Not capped
                    assert progression[i] >= progression[i-1] * 2, \
                        f"{name}: progression[{i}] should be at least double progression[{i-1}]"
        
        # Document expected behavior
        assert expected_progressions["FILE_WRITE_FAILED (base=1s)"] == [1, 2, 4, 8, 16, 32]
        assert expected_progressions["QUEUE_SEND_FAILED (base=2s)"] == [2, 4, 8, 16, 32, 64]
        assert expected_progressions["FILE_SYSTEM_ERROR (base=3s)"] == [3, 6, 12, 24, 48, 96]
        assert expected_progressions["AZURE_SERVICE_ERROR (base=5s)"] == [5, 10, 20, 40, 80, 160]