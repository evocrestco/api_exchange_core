"""
Validation processor for test harness result aggregation.

This processor receives verification results from VerificationProcessor
and produces final test outcomes with detailed reporting. It serves as
the terminal processor in the test harness pipeline.
"""

import time
from typing import Any, Dict, List, Optional
from dataclasses import dataclass
from datetime import datetime

from src.processors.processor_interface import ProcessorInterface
from src.processors.message import Message
from src.processors.processing_result import ProcessingResult, ProcessingStatus
from src.processing.processor_config import ProcessorConfig
from src.utils.logger import get_logger


@dataclass
class TestOutcome:
    """Final outcome of a test harness execution."""
    test_id: str
    test_type: str
    passed: bool
    total_verifications: int
    passed_verifications: int
    failed_verifications: int
    verification_details: List[Dict[str, Any]]
    processing_duration_ms: float
    entity_id: Optional[str] = None
    error_message: Optional[str] = None
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization."""
        return {
            "test_id": self.test_id,
            "test_type": self.test_type,
            "passed": self.passed,
            "total_verifications": self.total_verifications,
            "passed_verifications": self.passed_verifications,
            "failed_verifications": self.failed_verifications,
            "verification_details": self.verification_details,
            "processing_duration_ms": self.processing_duration_ms,
            "entity_id": self.entity_id,
            "error_message": self.error_message,
            "timestamp": datetime.utcnow().isoformat(),
        }
    
    def get_summary(self) -> str:
        """Get human-readable summary of test outcome."""
        status = "✅ PASSED" if self.passed else "❌ FAILED"
        return (
            f"{status} - {self.test_id} ({self.test_type}) - "
            f"{self.passed_verifications}/{self.total_verifications} verifications passed "
            f"({self.processing_duration_ms:.1f}ms)"
        )
    
    def get_detailed_report(self) -> str:
        """Get detailed human-readable report of test outcome."""
        lines = [
            f"Test Results: {self.test_id}",
            f"Type: {self.test_type}",
            f"Status: {'PASSED' if self.passed else 'FAILED'}",
            f"Duration: {self.processing_duration_ms:.1f}ms",
            f"Entity ID: {self.entity_id or 'None'}",
            "",
            f"Verification Summary: {self.passed_verifications}/{self.total_verifications} passed",
        ]
        
        if self.error_message:
            lines.extend(["", f"Error: {self.error_message}"])
        
        if self.verification_details:
            lines.extend(["", "Verification Details:"])
            for i, verification in enumerate(self.verification_details, 1):
                status_icon = "✅" if verification.get("passed") else "❌"
                check_name = verification.get("check_name", "unknown")
                lines.append(f"  {i}. {status_icon} {check_name}")
                
                if not verification.get("passed") and verification.get("error_message"):
                    lines.append(f"     Error: {verification['error_message']}")
        
        return "\n".join(lines)


class ValidationProcessor(ProcessorInterface):
    """
    Terminal processor that aggregates verification results and produces final test outcomes.
    
    This processor receives messages from VerificationProcessor containing
    verification results and produces comprehensive test reports. It serves
    as the final stage in the test harness pipeline.
    """
    
    def __init__(
        self,
        config: ProcessorConfig,
        processing_error_service: Optional[Any] = None,
        **kwargs: Any,
    ):
        """
        Initialize the validation processor.
        
        Args:
            config: Processor configuration
            processing_error_service: Optional error service for recording errors
            **kwargs: Additional processor arguments
        """
        self.config = config
        self.processing_error_service = processing_error_service
        self.logger = get_logger()
    
    def process(self, message: Message) -> ProcessingResult:
        """
        Process verification results and produce final test outcome.
        
        Args:
            message: Message containing verification results from VerificationProcessor
            
        Returns:
            ProcessingResult with final test outcome
        """
        start_time = time.time()
        
        try:
            # Extract verification results from message
            test_id = message.payload.get("test_id", "unknown")
            scenario = message.payload.get("scenario", "unknown")
            verification_results = message.payload.get("verification_results", [])
            entity_id = message.payload.get("entity_id")
            verification_duration = message.payload.get("verification_duration_ms", 0)
            error_injection = message.payload.get("error_injection", {})
            
            # Inject errors for bad/ugly scenarios
            if scenario in ["bad", "ugly"] and error_injection.get("fail_at") == "validation":
                raise Exception(error_injection.get("error_message", "Injected validation error"))
            
            self.logger.info(
                f"Validating test results for: {test_id}",
                extra={
                    "test_id": test_id,
                    "scenario": scenario,
                    "verification_count": len(verification_results),
                    "entity_id": entity_id,
                }
            )
            
            # Analyze verification results
            total_verifications = len(verification_results)
            passed_verifications = sum(1 for result in verification_results if result.get("passed", False))
            failed_verifications = total_verifications - passed_verifications
            
            # Determine overall test outcome
            test_passed = failed_verifications == 0 and total_verifications > 0
            
            # Handle edge case: no verifications performed
            error_message = None
            if total_verifications == 0:
                test_passed = False
                error_message = "No verifications were performed"
            
            # Collect failed verification details
            if not test_passed and not error_message:
                failed_checks = [r for r in verification_results if not r.get("passed", False)]
                error_messages = [r.get("error_message", "Unknown error") for r in failed_checks]
                error_message = f"Failed verifications: {'; '.join(error_messages)}"
            
            # Create test outcome
            processing_duration = (time.time() - start_time) * 1000
            total_duration = verification_duration + processing_duration
            
            outcome = TestOutcome(
                test_id=test_id,
                test_type=scenario,  # Use scenario as test type
                passed=test_passed,
                total_verifications=total_verifications,
                passed_verifications=passed_verifications,
                failed_verifications=failed_verifications,
                verification_details=verification_results,
                processing_duration_ms=total_duration,
                entity_id=entity_id,
                error_message=error_message,
            )
            
            # Log outcome
            if test_passed:
                self.logger.info(
                    f"Test PASSED: {test_id}",
                    extra={
                        "test_id": test_id,
                        "scenario": scenario,
                        "passed_verifications": passed_verifications,
                        "total_verifications": total_verifications,
                        "duration_ms": total_duration,
                        "entity_id": entity_id,
                    }
                )
            else:
                self.logger.error(
                    f"Test FAILED: {test_id}",
                    extra={
                        "test_id": test_id,
                        "scenario": scenario,
                        "failed_verifications": failed_verifications,
                        "total_verifications": total_verifications,
                        "error_message": error_message,
                        "duration_ms": total_duration,
                        "entity_id": entity_id,
                    }
                )
            
            # Log detailed report at debug level
            self.logger.debug(
                f"Detailed test report for {test_id}:\n{outcome.get_detailed_report()}"
            )
            
            # Create processing result
            result = ProcessingResult.create_success(
                output_messages=[],  # Terminal processor - no output messages
                processing_metadata={
                    "test_outcome": outcome.to_dict(),
                    "test_passed": test_passed,
                    "test_summary": outcome.get_summary(),
                    "processor_execution": {
                        "processor_name": self.config.processor_name,
                        "duration_ms": processing_duration,
                    }
                }
            )
            
            return result
            
        except Exception as e:
            processing_duration = (time.time() - start_time) * 1000
            test_id = message.payload.get("test_id", "unknown")
            
            self.logger.error(
                f"Validation processor failed for test: {test_id}",
                extra={
                    "test_id": test_id,
                    "error": str(e),
                    "duration_ms": processing_duration,
                },
                exc_info=True
            )
            
            # Record error in database using ProcessingErrorService
            if self.processing_error_service:
                try:
                    error_id = self.processing_error_service.record_error(
                        entity_id=message.payload.get("entity_id") or "validation_error",
                        error_type="VALIDATION_PROCESSOR_ERROR",
                        message=f"Validation processor failed: {str(e)}",
                        processing_step="validation",
                        stack_trace=str(e),
                    )
                    self.logger.info(f"Recorded processing error: {error_id}")
                except Exception as error_service_exception:
                    self.logger.warning(f"Failed to record error in database: {error_service_exception}")
            
            return ProcessingResult.create_failure(
                error_message=f"Validation processor failed: {str(e)}",
                error_code="VALIDATION_PROCESSOR_ERROR",
                processing_duration_ms=processing_duration,
                can_retry=False,  # Don't retry validation failures
            )
    
    def validate_message(self, message: Message) -> bool:
        """
        Validate that message contains verification results.
        
        Args:
            message: Message to validate
            
        Returns:
            True if message contains verification results
        """
        if not isinstance(message.payload, dict):
            return False
        
        # Check for required fields from VerificationProcessor
        if "scenario" in message.payload:
            # Scenario-based testing
            required_fields = ["test_id", "scenario", "verification_results"]
        else:
            # Explicit configuration testing
            required_fields = ["test_id", "test_type", "verification_results"]
        
        return all(field in message.payload for field in required_fields)
    
    def get_processor_info(self) -> Dict[str, Any]:
        """
        Get processor information.
        
        Returns:
            Processor information dictionary
        """
        return {
            "name": "ValidationProcessor",
            "version": "1.0.0",
            "type": "terminal",
            "description": "Test harness processor for result validation and reporting",
        }
    
    def can_retry(self, error: Exception) -> bool:
        """
        Determine if processing can be retried after an error.
        
        Args:
            error: The error that occurred
            
        Returns:
            False for test harness (don't retry validation failures)
        """
        # Don't retry validation failures - we want to see them immediately
        return False