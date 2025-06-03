"""
Verification processor for test harness.

This processor is responsible for:
1. Creating entities using the framework's ProcessingService
2. Verifying that the framework behaved correctly
3. Passing verification results to the validation processor
"""

import sys
import os

# Add parent directory to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

import time
from typing import Any, Dict, List, Optional

from src.db.db_base import EntityStateEnum
from src.processors.processor_interface import ProcessorInterface
from src.processors.message import Message
from src.processors.processing_result import ProcessingResult
from src.processing.processing_service import ProcessingService
from src.processing.processor_config import ProcessorConfig
from src.services.entity_service import EntityService
from src.services.state_tracking_service import StateTrackingService
from src.services.processing_error_service import ProcessingErrorService
from src.utils.logger import get_logger
from utils.verification_utils import (
    VerificationUtils,
    VerificationResult,
    ExpectedStateTransition,
)


class VerificationProcessor(ProcessorInterface):
    """
    Processor that verifies framework functionality.
    
    This processor acts as both a business processor (creating entities)
    and a test verifier (checking that the framework worked correctly).
    """
    
    def __init__(
        self,
        entity_service: EntityService,
        processing_service: ProcessingService,
        config: ProcessorConfig,
        state_tracking_service: Optional[StateTrackingService] = None,
        processing_error_service: Optional[ProcessingErrorService] = None,
    ):
        """
        Initialize the verification processor.
        
        Args:
            entity_service: Service for entity operations
            processing_service: Service for processing entities
            config: Processor configuration
            state_tracking_service: Optional state tracking service
            processing_error_service: Optional error service
        """
        self.entity_service = entity_service
        self.processing_service = processing_service
        self.config = config
        self.state_tracking_service = state_tracking_service
        self.processing_error_service = processing_error_service
        self.logger = get_logger()
        
        # Initialize verification utilities
        self.verification_utils = VerificationUtils(
            entity_service=entity_service,
            state_tracking_service=state_tracking_service,
            processing_error_service=processing_error_service,
        )
    
    def process(self, message: Message) -> ProcessingResult:
        """
        Process a test message and verify framework behavior.
        
        This method:
        1. Extracts test configuration from the message
        2. Creates an entity using the framework
        3. Verifies the framework behaved correctly
        4. Passes results to the validation processor
        
        Args:
            message: Test message containing verification config
            
        Returns:
            ProcessingResult with verification outcomes
        """
        start_time = time.time()
        test_id = message.payload.get("test_id", "unknown")
        scenario = message.payload.get("scenario", "good")
        
        self.logger.info(
            f"Starting verification processor for test: {test_id}",
            extra={
                "test_id": test_id,
                "scenario": scenario,
                "tenant_id": message.entity_reference.tenant_id,
            }
        )
        
        try:
            # Handle scenario-based configuration
            verification_config, expected_results, test_data = self._get_scenario_config(message)
            
            # Inject errors for bad/ugly scenarios
            if scenario in ["bad", "ugly"]:
                error_injection = message.payload.get("error_injection", {})
                fail_at = error_injection.get("fail_at")
                if fail_at in ["verification", "validation"]:
                    error_message = error_injection.get("error_message", f"Injected {fail_at} error")
                    raise Exception(error_message)
            
            # Step 1: Create entity using the framework
            entity_id = None
            if verification_config.get("create_entity", True):
                entity_id = self._create_test_entity(message, test_data)
            
            # Step 2: Record completion state transition if we created an entity (before verification)
            if entity_id and self.state_tracking_service and self.config.enable_state_tracking:
                try:
                    from src.db.db_base import EntityStateEnum
                    from src.db.db_state_transition_models import TransitionTypeEnum
                    
                    self.state_tracking_service.record_transition(
                        entity_id=entity_id,
                        from_state=EntityStateEnum.PROCESSING,
                        to_state=EntityStateEnum.COMPLETED,
                        actor=self.config.processor_name,
                        transition_type=TransitionTypeEnum.NORMAL,
                        processor_data={
                            "processor_name": self.config.processor_name,
                            "processor_version": self.config.processor_version,
                        },
                        notes=f"Verification processor completed successfully"
                    )
                except Exception as e:
                    self.logger.warning(f"Failed to record completion state transition: {e}")
            
            # Step 3: Run verifications
            verification_results: List[VerificationResult] = []
            
            # Verify entity creation
            if verification_config.get("verify_database", True):
                result = self._verify_entity_creation(message, expected_results, entity_id)
                verification_results.append(result)
            
            # Verify state tracking
            if verification_config.get("verify_state_tracking", True) and entity_id:
                result = self._verify_state_tracking(entity_id, expected_results)
                verification_results.append(result)
            
            # Verify no errors
            if verification_config.get("verify_error_handling", True):
                result = self._verify_no_errors(entity_id, expected_results)
                verification_results.append(result)
            
            # Step 3: Create output message for validation processor
            processing_duration = (time.time() - start_time) * 1000
            all_verifications_passed = all(r.passed for r in verification_results)
            
            output_message = Message.create_entity_message(
                external_id=message.entity_reference.external_id,
                canonical_type=message.entity_reference.canonical_type,
                source=message.entity_reference.source,
                tenant_id=message.entity_reference.tenant_id,
                payload={
                    "test_id": test_id,
                    "scenario": scenario,
                    "verification_results": [result.to_dict() for result in verification_results],
                    "all_verifications_passed": all_verifications_passed,
                    "entity_id": entity_id,
                    "verification_duration_ms": processing_duration,
                    "error_injection": message.payload.get("error_injection", {}),
                },
                entity_id=entity_id,
                version=message.entity_reference.version,
                correlation_id=message.correlation_id,
                metadata={
                    "processor": "verification_processor",
                    "all_verifications_passed": all_verifications_passed,
                },
            )
            
            result = ProcessingResult.create_success(
                output_messages=[output_message],
                entities_created=[entity_id] if entity_id else [],
                processing_metadata={
                    "test_id": test_id,
                    "verification_results": [result.to_dict() for result in verification_results],
                    "all_verifications_passed": all_verifications_passed,
                    "entity_id": entity_id,
                    "processor_execution": {
                        "processor_name": self.config.processor_name,
                        "duration_ms": processing_duration,
                    }
                }
            )
            
            
            self.logger.info(
                f"Verification processor completed for test: {test_id}",
                extra={
                    "test_id": test_id,
                    "all_verifications_passed": all_verifications_passed,
                    "verification_count": len(verification_results),
                    "entity_id": entity_id,
                    "duration_ms": processing_duration,
                }
            )
            
            return result
            
        except Exception as e:
            processing_duration = (time.time() - start_time) * 1000
            self.logger.error(
                f"Verification processor failed for test: {message.payload.get('test_id', 'unknown')}",
                extra={
                    "test_id": message.payload.get("test_id", "unknown"),
                    "error": str(e),
                    "duration_ms": processing_duration,
                },
                exc_info=True
            )
            
            # Record error in database using ProcessingErrorService
            if self.processing_error_service:
                try:
                    error_id = self.processing_error_service.record_error(
                        entity_id=message.entity_reference.external_id,  # Use external_id as fallback
                        error_type="VERIFICATION_PROCESSOR_ERROR",
                        message=f"Verification processor failed: {str(e)}",
                        processing_step="verification",
                        stack_trace=str(e),
                    )
                    self.logger.info(f"Recorded processing error: {error_id}")
                except Exception as error_service_exception:
                    self.logger.warning(f"Failed to record error in database: {error_service_exception}")
            
            return ProcessingResult.create_failure(
                error_message=f"Verification processor failed: {str(e)}",
                error_code="VERIFICATION_PROCESSOR_ERROR",
                processing_duration_ms=processing_duration,
                can_retry=False,  # Don't retry test failures
            )
    
    def _create_test_entity(self, message: Message, test_data: Dict[str, Any]) -> Optional[str]:
        """
        Create a test entity using the framework's processing service.
        
        This directly calls ProcessingService to create an entity, simulating
        what a source processor would do.
        
        Args:
            message: Original test message
            test_data: Test data to create entity with
            
        Returns:
            Entity ID if created successfully, None otherwise
        """
        try:
            # Use ProcessingService to create entity (this tests the real framework path)
            result = self.processing_service.process_entity(
                external_id=message.entity_reference.external_id,
                canonical_type=message.entity_reference.canonical_type,
                source=message.entity_reference.source,
                content=test_data,  # Pass test data directly as content
                config=self.config,
                custom_attributes={
                    "test_harness": {
                        "test_id": message.payload.get("test_id"),
                        "test_type": message.payload.get("test_type"),
                        "created_by": "verification_processor",
                    }
                },
                source_metadata=message.metadata,
            )
            
            return result.entity_id
            
        except Exception as e:
            self.logger.error(f"Failed to create test entity: {e}", exc_info=True)
            raise
    
    def _verify_entity_creation(
        self, 
        message: Message, 
        expected_results: Dict[str, Any],
        entity_id: Optional[str]
    ) -> VerificationResult:
        """
        Verify that entity was created as expected.
        
        Args:
            message: Original test message
            expected_results: Expected results from test configuration
            entity_id: Entity ID that should have been created
            
        Returns:
            VerificationResult for entity creation
        """
        if not expected_results.get("entity_should_be_created", True):
            # Test expects no entity to be created
            if entity_id is None:
                return VerificationResult(
                    passed=True,
                    check_name="entity_creation",
                    details={"expected_no_entity": True, "entity_created": False}
                )
            else:
                return VerificationResult(
                    passed=False,
                    check_name="entity_creation",
                    details={"expected_no_entity": True, "entity_created": True, "entity_id": entity_id},
                    error_message="Entity was created when none was expected"
                )
        
        # Test expects entity to be created
        if entity_id is None:
            return VerificationResult(
                passed=False,
                check_name="entity_creation",
                details={"expected_entity": True, "entity_created": False},
                error_message="Expected entity to be created but none was found"
            )
        
        # Verify entity using VerificationUtils
        return self.verification_utils.verify_entity_created(
            external_id=message.entity_reference.external_id,
            source=message.entity_reference.source,
            expected_canonical_type=message.entity_reference.canonical_type,
            expected_version=expected_results.get("entity_version", 1),
        )
    
    def _verify_state_tracking(
        self, 
        entity_id: str, 
        expected_results: Dict[str, Any]
    ) -> VerificationResult:
        """
        Verify that state transitions were recorded as expected.
        
        Args:
            entity_id: Entity ID to check state transitions for
            expected_results: Expected results from test configuration
            
        Returns:
            VerificationResult for state tracking
        """
        expected_transitions_data = expected_results.get("state_transitions", [])
        
        # Convert to ExpectedStateTransition objects
        expected_transitions = []
        for transition_data in expected_transitions_data:
            expected_transitions.append(
                ExpectedStateTransition(
                    from_state=EntityStateEnum(transition_data["from"]),
                    to_state=EntityStateEnum(transition_data["to"]),
                    processor_name=transition_data.get("processor_name"),
                )
            )
        
        return self.verification_utils.verify_state_transitions(
            entity_id=entity_id,
            expected_transitions=expected_transitions,
            allow_extra_transitions=True,  # Allow framework to add extra transitions
        )
    
    def _verify_no_errors(
        self, 
        entity_id: Optional[str], 
        expected_results: Dict[str, Any]
    ) -> VerificationResult:
        """
        Verify that no processing errors occurred.
        
        Args:
            entity_id: Entity ID to check errors for
            expected_results: Expected results from test configuration
            
        Returns:
            VerificationResult for error checking
        """
        return self.verification_utils.verify_no_errors(
            entity_id=entity_id,
            processor_name=self.config.processor_name,
        )
    
    def _get_scenario_config(self, message: Message) -> tuple:
        """
        Get configuration based on test scenario (good/bad/ugly).
        
        Args:
            message: Test message
            
        Returns:
            Tuple of (verification_config, expected_results, test_data)
        """
        scenario = message.payload.get("scenario", "good")
        
        # Default good scenario config
        if scenario == "good":
            verification_config = message.payload.get("verification_config", {
                "create_entity": True,
                "verify_database": True,
                "verify_state_tracking": True,
                "verify_error_handling": True,
            })
            expected_results = message.payload.get("expected_results", {
                "entity_should_be_created": True,
                "entity_version": 1,
                "state_transitions": [
                    {"from": "RECEIVED", "to": "PROCESSING"},
                    {"from": "PROCESSING", "to": "COMPLETED"}
                ]
            })
            test_data = message.payload.get("test_data", {"name": "Test Entity", "value": 123})
            
        elif scenario == "bad":
            # Bad scenario - validation errors
            verification_config = message.payload.get("verification_config", {
                "create_entity": True,
                "verify_database": True,
                "verify_state_tracking": True,
                "verify_error_handling": True,
            })
            expected_results = message.payload.get("expected_results", {
                "entity_should_be_created": True,
                "entity_version": 1,
                "state_transitions": [
                    {"from": "RECEIVED", "to": "PROCESSING"},
                    {"from": "PROCESSING", "to": "COMPLETED"}
                ]
            })
            # Bad data that should cause validation issues
            test_data = message.payload.get("test_data", {"name": "", "value": -1})
            
        else:  # ugly
            # Ugly scenario - chaos testing
            import random
            verification_config = message.payload.get("verification_config", {
                "create_entity": random.choice([True, False]),
                "verify_database": True,
                "verify_state_tracking": random.choice([True, False]),
                "verify_error_handling": True,
            })
            expected_results = message.payload.get("expected_results", {
                "entity_should_be_created": verification_config["create_entity"],
                "entity_version": 1,
                "state_transitions": [
                    {"from": "RECEIVED", "to": "PROCESSING"},
                    {"from": "PROCESSING", "to": "COMPLETED"}
                ] if verification_config["create_entity"] else []
            })
            # Random test data
            test_data = message.payload.get("test_data", {
                "name": f"Chaos Entity {random.randint(1, 1000)}",
                "value": random.randint(-100, 100),
                "chaos": True
            })
        
        return verification_config, expected_results, test_data
    
    # ProcessorInterface implementation
    def validate_message(self, message: Message) -> bool:
        """
        Validate that message contains required test harness fields.
        
        Args:
            message: Message to validate
            
        Returns:
            True if message is valid for verification processor
        """
        if not isinstance(message.payload, dict):
            return False
        
        # For scenario-based testing, just need scenario and test_id
        if "scenario" in message.payload:
            return "test_id" in message.payload
        
        # For explicit configuration testing
        required_fields = ["test_id", "test_type", "verification_config", "expected_results"]
        return all(field in message.payload for field in required_fields)
    
    def get_processor_info(self) -> Dict[str, Any]:
        """
        Get processor information.
        
        Returns:
            Processor information dictionary
        """
        return {
            "name": "VerificationProcessor",
            "version": "1.0.0",
            "type": "verification",
            "description": "Test harness processor for framework verification",
        }
    
    def can_retry(self, error: Exception) -> bool:
        """
        Determine if processing can be retried after an error.
        
        Args:
            error: The error that occurred
            
        Returns:
            False for test harness (don't retry test failures)
        """
        # Don't retry test failures - we want to see them immediately
        return False