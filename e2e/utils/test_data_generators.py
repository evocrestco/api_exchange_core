"""
Test data generation utilities for the test harness.

This module provides utilities to generate test messages, test data,
and expected results for various test scenarios.
"""

import uuid
from typing import Any, Dict, List, Optional
from datetime import datetime

from src.processors.message import Message
from src.db.db_base import EntityStateEnum


class TestMessageGenerator:
    """
    Generator for test harness messages with embedded expectations.
    
    This class creates properly structured test messages that contain
    both the test data and the verification expectations needed by
    the VerificationProcessor and ValidationProcessor.
    """
    
    @staticmethod
    def create_entity_creation_test(
        tenant_id: str = "test_harness",
        test_data: Optional[Dict[str, Any]] = None,
        external_id: Optional[str] = None,
        canonical_type: str = "test_entity",
        source: str = "test_harness",
        verify_database: bool = True,
        verify_state_tracking: bool = True,
        verify_error_handling: bool = True,
    ) -> Message:
        """
        Create a test message for basic entity creation testing.
        
        Args:
            tenant_id: Tenant ID for the test
            test_data: Custom test data (generated if not provided)
            external_id: External ID for entity (generated if not provided)
            canonical_type: Canonical type for entity
            source: Source system identifier
            verify_database: Whether to verify database operations
            verify_state_tracking: Whether to verify state tracking
            verify_error_handling: Whether to verify error handling
            
        Returns:
            Test message for entity creation scenario
        """
        test_id = f"entity-creation-{uuid.uuid4().hex[:8]}"
        external_id = external_id or f"test-entity-{uuid.uuid4().hex[:8]}"
        
        # Generate test data if not provided
        if test_data is None:
            test_data = {
                "name": f"Test Entity {uuid.uuid4().hex[:8]}",
                "type": "test_object",
                "value": 42,
                "timestamp": datetime.utcnow().isoformat(),
                "metadata": {
                    "generated_by": "test_harness",
                    "test_id": test_id,
                }
            }
        
        # Define expected results
        expected_results = {
            "entity_should_be_created": True,
            "entity_version": 1,
            "state_transitions": [
                {"from": "RECEIVED", "to": "PROCESSING"},
                {"from": "PROCESSING", "to": "COMPLETED"},
            ],
            "entities_created_count": 1,
            "processing_metadata_keys": ["processor_execution", "test_harness"],
        }
        
        # Define verification configuration
        verification_config = {
            "verify_database": verify_database,
            "verify_state_tracking": verify_state_tracking,
            "verify_error_handling": verify_error_handling,
        }
        
        # Create test message payload
        payload = {
            "test_id": test_id,
            "test_type": "entity_creation",
            "expected_results": expected_results,
            "verification_config": verification_config,
            "test_data": test_data,
        }
        
        # Create and return message
        return Message.create_entity_message(
            external_id=external_id,
            canonical_type=canonical_type,
            source=source,
            tenant_id=tenant_id,
            payload=payload,
        )
    
    @staticmethod
    def create_multi_stage_test(
        tenant_id: str = "test_harness",
        stages: int = 3,
        test_data: Optional[Dict[str, Any]] = None,
        external_id: Optional[str] = None,
    ) -> Message:
        """
        Create a test message for multi-stage pipeline testing.
        
        Args:
            tenant_id: Tenant ID for the test
            stages: Number of processing stages expected
            test_data: Custom test data (generated if not provided)
            external_id: External ID for entity (generated if not provided)
            
        Returns:
            Test message for multi-stage pipeline scenario
        """
        test_id = f"multi-stage-{stages}-{uuid.uuid4().hex[:8]}"
        external_id = external_id or f"multi-entity-{uuid.uuid4().hex[:8]}"
        
        # Generate test data if not provided
        if test_data is None:
            test_data = {
                "pipeline_data": {
                    "stage_count": stages,
                    "data": f"Multi-stage test data {uuid.uuid4().hex[:8]}",
                    "created_at": datetime.utcnow().isoformat(),
                },
                "metadata": {
                    "test_type": "multi_stage",
                    "test_id": test_id,
                }
            }
        
        # Generate expected state transitions for multiple stages
        expected_transitions = [{"from": "RECEIVED", "to": "PROCESSING"}]
        for stage in range(1, stages):
            expected_transitions.append({"from": "PROCESSING", "to": "PROCESSING"})
        expected_transitions.append({"from": "PROCESSING", "to": "COMPLETED"})
        
        # Define expected results
        expected_results = {
            "entity_should_be_created": True,
            "entity_version": stages,  # Each stage creates a new version
            "state_transitions": expected_transitions,
            "entities_created_count": 1,
            "processing_stages": stages,
        }
        
        # Create test message payload
        payload = {
            "test_id": test_id,
            "test_type": "multi_stage_pipeline",
            "expected_results": expected_results,
            "verification_config": {
                "verify_database": verify_database,
                "verify_state_tracking": verify_state_tracking,
                "verify_error_handling": verify_error_handling,
            },
            "test_data": test_data,
        }
        
        return Message.create_entity_message(
            external_id=external_id,
            canonical_type="multi_stage_test",
            source="test_harness",
            tenant_id=tenant_id,
            payload=payload,
        )
    
    @staticmethod
    def create_error_handling_test(
        tenant_id: str = "test_harness",
        error_type: str = "validation_error",
        should_retry: bool = False,
    ) -> Message:
        """
        Create a test message for error handling testing.
        
        Args:
            tenant_id: Tenant ID for the test
            error_type: Type of error to simulate
            should_retry: Whether the error should be retryable
            
        Returns:
            Test message for error handling scenario
        """
        test_id = f"error-handling-{error_type}-{uuid.uuid4().hex[:8]}"
        external_id = f"error-test-{uuid.uuid4().hex[:8]}"
        
        # Generate test data that will trigger an error
        test_data = {
            "error_simulation": {
                "error_type": error_type,
                "should_retry": should_retry,
                "trigger_at": "processing",
            },
            "data": f"Error test data {uuid.uuid4().hex[:8]}",
        }
        
        # Expected results for error scenario
        expected_results = {
            "entity_should_be_created": False,  # Expect no entity due to error
            "should_have_error": True,
            "error_type": error_type,
            "should_retry": should_retry,
            "state_transitions": [
                {"from": "RECEIVED", "to": "PROCESSING"},
                {"from": "PROCESSING", "to": "SYSTEM_ERROR"},
            ],
        }
        
        payload = {
            "test_id": test_id,
            "test_type": "error_handling",
            "expected_results": expected_results,
            "verification_config": {
                "verify_database": True,
                "verify_state_tracking": True,
                "verify_error_handling": True,
            },
            "test_data": test_data,
        }
        
        return Message.create_entity_message(
            external_id=external_id,
            canonical_type="error_test",
            source="test_harness",
            tenant_id=tenant_id,
            payload=payload,
        )