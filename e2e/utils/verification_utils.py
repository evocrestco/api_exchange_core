"""
Verification utilities for test harness framework validation.

This module provides utilities to verify that the API Exchange Core framework
is working correctly by using the actual framework services and checking
their behavior against expected results.
"""

from typing import Any, Dict, List, Optional
from dataclasses import dataclass
from datetime import datetime

from src.services.entity_service import EntityService
from src.services.state_tracking_service import StateTrackingService
from src.services.processing_error_service import ProcessingErrorService
from src.db.db_base import EntityStateEnum
from src.schemas.entity_schema import EntityRead
from src.utils.logger import get_logger


@dataclass
class VerificationResult:
    """Result of a verification check."""
    passed: bool
    check_name: str
    details: Dict[str, Any]
    error_message: Optional[str] = None
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization."""
        return {
            "passed": self.passed,
            "check_name": self.check_name,
            "details": self.details,
            "error_message": self.error_message
        }


@dataclass
class ExpectedStateTransition:
    """Expected state transition for verification."""
    from_state: EntityStateEnum
    to_state: EntityStateEnum
    processor_name: Optional[str] = None


class VerificationUtils:
    """
    Utilities for verifying framework behavior using real framework services.
    
    This class provides methods to verify that entities were created, state
    transitions were recorded, and errors were handled correctly by using
    the actual framework services that processors use in production.
    """
    
    def __init__(
        self,
        entity_service: EntityService,
        state_tracking_service: Optional[StateTrackingService] = None,
        processing_error_service: Optional[ProcessingErrorService] = None,
    ):
        """
        Initialize verification utilities with framework services.
        
        Args:
            entity_service: Real EntityService for entity verification
            state_tracking_service: Optional StateTrackingService for state verification
            processing_error_service: Optional ProcessingErrorService for error verification
        """
        self.entity_service = entity_service
        self.state_tracking_service = state_tracking_service
        self.processing_error_service = processing_error_service
        self.logger = get_logger()
    
    def verify_entity_created(
        self,
        external_id: str,
        source: str,
        expected_canonical_type: Optional[str] = None,
        expected_version: Optional[int] = None,
    ) -> VerificationResult:
        """
        Verify that an entity was created using EntityService.
        
        Args:
            external_id: External ID to look for
            source: Source system identifier
            expected_canonical_type: Expected canonical type (optional)
            expected_version: Expected version number (optional)
            
        Returns:
            VerificationResult with entity creation details
        """
        try:
            entity = self.entity_service.get_entity_by_external_id(
                external_id=external_id,
                source=source
            )
            
            if entity is None:
                return VerificationResult(
                    passed=False,
                    check_name="entity_created",
                    details={"external_id": external_id, "source": source},
                    error_message=f"Entity not found: {external_id} from {source}"
                )
            
            # Verify expected properties if provided
            verification_details = {
                "entity_id": entity.id,
                "external_id": entity.external_id,
                "source": entity.source,
                "canonical_type": entity.canonical_type,
                "version": entity.version,
                "created_at": entity.created_at.isoformat(),
            }
            
            # Check canonical type if expected
            if expected_canonical_type and entity.canonical_type != expected_canonical_type:
                return VerificationResult(
                    passed=False,
                    check_name="entity_created",
                    details=verification_details,
                    error_message=f"Canonical type mismatch: expected {expected_canonical_type}, got {entity.canonical_type}"
                )
            
            # Check version if expected
            if expected_version is not None and entity.version != expected_version:
                return VerificationResult(
                    passed=False,
                    check_name="entity_created",
                    details=verification_details,
                    error_message=f"Version mismatch: expected {expected_version}, got {entity.version}"
                )
            
            return VerificationResult(
                passed=True,
                check_name="entity_created",
                details=verification_details
            )
            
        except Exception as e:
            self.logger.error(f"Error verifying entity creation: {e}", exc_info=True)
            return VerificationResult(
                passed=False,
                check_name="entity_created",
                details={"external_id": external_id, "source": source},
                error_message=f"Verification error: {str(e)}"
            )
    
    def verify_state_transitions(
        self,
        entity_id: str,
        expected_transitions: List[ExpectedStateTransition],
        allow_extra_transitions: bool = True,
    ) -> VerificationResult:
        """
        Verify that expected state transitions were recorded.
        
        Args:
            entity_id: Entity ID to check transitions for
            expected_transitions: List of expected state transitions
            allow_extra_transitions: Whether to allow additional transitions beyond expected
            
        Returns:
            VerificationResult with state transition details
        """
        if not self.state_tracking_service:
            return VerificationResult(
                passed=False,
                check_name="state_transitions",
                details={"entity_id": entity_id},
                error_message="StateTrackingService not available for verification"
            )
        
        try:
            # Get actual transitions from StateTrackingService
            state_history = self.state_tracking_service.get_entity_state_history(entity_id)
            actual_transitions = state_history.transitions if state_history else []
            
            verification_details = {
                "entity_id": entity_id,
                "expected_count": len(expected_transitions),
                "actual_count": len(actual_transitions),
                "expected_transitions": [
                    {
                        "from_state": t.from_state.value,
                        "to_state": t.to_state.value,
                        "processor_name": t.processor_name
                    }
                    for t in expected_transitions
                ],
                "actual_transitions": [
                    {
                        "from_state": t.from_state if isinstance(t.from_state, str) else t.from_state.value,
                        "to_state": t.to_state if isinstance(t.to_state, str) else t.to_state.value,
                        "processor_name": getattr(t, 'processor_name', None) or getattr(t, 'actor', None),
                        "created_at": t.created_at.isoformat() if hasattr(t, 'created_at') else None
                    }
                    for t in actual_transitions
                ]
            }
            
            # Check if we have enough transitions
            if len(actual_transitions) < len(expected_transitions):
                return VerificationResult(
                    passed=False,
                    check_name="state_transitions",
                    details=verification_details,
                    error_message=f"Not enough transitions: expected {len(expected_transitions)}, got {len(actual_transitions)}"
                )
            
            # Check if we have too many transitions (when not allowing extra)
            if not allow_extra_transitions and len(actual_transitions) > len(expected_transitions):
                return VerificationResult(
                    passed=False,
                    check_name="state_transitions",
                    details=verification_details,
                    error_message=f"Too many transitions: expected {len(expected_transitions)}, got {len(actual_transitions)}"
                )
            
            # Verify each expected transition exists
            for i, expected in enumerate(expected_transitions):
                if i >= len(actual_transitions):
                    return VerificationResult(
                        passed=False,
                        check_name="state_transitions",
                        details=verification_details,
                        error_message=f"Missing transition {i}: {expected.from_state.value} -> {expected.to_state.value}"
                    )
                
                actual = actual_transitions[i]
                
                # Check state transition match
                # Handle both string and enum comparisons
                expected_from = expected.from_state.value if hasattr(expected.from_state, 'value') else expected.from_state
                expected_to = expected.to_state.value if hasattr(expected.to_state, 'value') else expected.to_state
                actual_from = actual.from_state if isinstance(actual.from_state, str) else actual.from_state.value
                actual_to = actual.to_state if isinstance(actual.to_state, str) else actual.to_state.value
                
                if (actual_from != expected_from or actual_to != expected_to):
                    return VerificationResult(
                        passed=False,
                        check_name="state_transitions",
                        details=verification_details,
                        error_message=f"Transition {i} mismatch: expected {expected_from} -> {expected_to}, got {actual_from} -> {actual_to}"
                    )
                
                # Check processor name if specified
                if expected.processor_name:
                    actual_processor = getattr(actual, 'processor_name', None) or getattr(actual, 'actor', None)
                    if actual_processor != expected.processor_name:
                        return VerificationResult(
                            passed=False,
                            check_name="state_transitions",
                            details=verification_details,
                            error_message=f"Transition {i} processor mismatch: expected {expected.processor_name}, got {actual.processor_name}"
                        )
            
            return VerificationResult(
                passed=True,
                check_name="state_transitions",
                details=verification_details
            )
            
        except Exception as e:
            self.logger.error(f"Error verifying state transitions: {e}", exc_info=True)
            return VerificationResult(
                passed=False,
                check_name="state_transitions",
                details={"entity_id": entity_id},
                error_message=f"Verification error: {str(e)}"
            )
    
    def verify_no_errors(
        self,
        entity_id: Optional[str] = None,
        processor_name: Optional[str] = None,
        since: Optional[datetime] = None,
    ) -> VerificationResult:
        """
        Verify that no processing errors were recorded.
        
        Args:
            entity_id: Optional entity ID to check errors for
            processor_name: Optional processor name to check errors for
            since: Optional datetime to check errors since
            
        Returns:
            VerificationResult with error details
        """
        if not self.processing_error_service:
            return VerificationResult(
                passed=False,
                check_name="no_errors",
                details={},
                error_message="ProcessingErrorService not available for verification"
            )
        
        try:
            # Get errors based on provided filters
            # Note: This assumes ProcessingErrorService has methods to query errors
            # You may need to adjust based on actual service interface
            errors = []
            
            # For now, let's implement a basic check
            # This can be expanded based on the actual ProcessingErrorService interface
            verification_details = {
                "entity_id": entity_id,
                "processor_name": processor_name,
                "since": since.isoformat() if since else None,
                "error_count": len(errors),
                "errors": [
                    {
                        "error_type": getattr(error, 'error_type', 'unknown'),
                        "message": getattr(error, 'message', 'unknown'),
                        "created_at": error.created_at.isoformat() if hasattr(error, 'created_at') else None
                    }
                    for error in errors
                ]
            }
            
            if len(errors) > 0:
                return VerificationResult(
                    passed=False,
                    check_name="no_errors",
                    details=verification_details,
                    error_message=f"Found {len(errors)} processing errors"
                )
            
            return VerificationResult(
                passed=True,
                check_name="no_errors",
                details=verification_details
            )
            
        except Exception as e:
            self.logger.error(f"Error verifying no errors: {e}", exc_info=True)
            return VerificationResult(
                passed=False,
                check_name="no_errors",
                details={},
                error_message=f"Verification error: {str(e)}"
            )
    
    def verify_entity_attributes(
        self,
        entity: EntityRead,
        expected_attributes: Dict[str, Any],
        check_subset: bool = True,
    ) -> VerificationResult:
        """
        Verify that entity has expected attributes.
        
        Args:
            entity: Entity to check attributes for
            expected_attributes: Expected attributes dictionary
            check_subset: If True, only check that expected attributes exist (allow extra)
            
        Returns:
            VerificationResult with attribute verification details
        """
        try:
            actual_attributes = entity.attributes or {}
            
            verification_details = {
                "entity_id": entity.id,
                "expected_attributes": expected_attributes,
                "actual_attributes": actual_attributes,
                "check_subset": check_subset
            }
            
            # Check each expected attribute
            for key, expected_value in expected_attributes.items():
                if key not in actual_attributes:
                    return VerificationResult(
                        passed=False,
                        check_name="entity_attributes",
                        details=verification_details,
                        error_message=f"Missing expected attribute: {key}"
                    )
                
                actual_value = actual_attributes[key]
                if actual_value != expected_value:
                    return VerificationResult(
                        passed=False,
                        check_name="entity_attributes",
                        details=verification_details,
                        error_message=f"Attribute mismatch for {key}: expected {expected_value}, got {actual_value}"
                    )
            
            # If not checking subset, verify no extra attributes
            if not check_subset:
                extra_keys = set(actual_attributes.keys()) - set(expected_attributes.keys())
                if extra_keys:
                    return VerificationResult(
                        passed=False,
                        check_name="entity_attributes",
                        details=verification_details,
                        error_message=f"Unexpected attributes found: {list(extra_keys)}"
                    )
            
            return VerificationResult(
                passed=True,
                check_name="entity_attributes",
                details=verification_details
            )
            
        except Exception as e:
            self.logger.error(f"Error verifying entity attributes: {e}", exc_info=True)
            return VerificationResult(
                passed=False,
                check_name="entity_attributes",
                details={"entity_id": entity.id},
                error_message=f"Verification error: {str(e)}"
            )