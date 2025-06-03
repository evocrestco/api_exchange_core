"""
Basic entity creation test scenario.

This scenario tests the fundamental entity creation capabilities of the
framework by creating a simple entity and verifying that all framework
components work correctly.
"""

from typing import Any, Dict

from tests.harness.utils.test_data_generators import TestMessageGenerator


def create_basic_entity_creation_test(
    tenant_id: str = "entity_test_tenant",
    entity_type: str = "test_entity",
    verify_all: bool = True,
) -> Dict[str, Any]:
    """
    Create a basic entity creation test configuration.
    
    This test verifies that:
    1. Entity is created and persisted in database
    2. State transitions are recorded correctly
    3. No processing errors occur
    4. Entity attributes are set correctly
    
    Args:
        tenant_id: Tenant ID for the test
        entity_type: Type of entity to create
        verify_all: Whether to enable all verification checks
        
    Returns:
        Test message for basic entity creation
    """
    test_data = {
        \"name\": \"Basic Test Entity\",\n        \"description\": \"A simple entity for testing framework capabilities\",\n        \"priority\": \"high\",\n        \"status\": \"active\",\n        \"metadata\": {\n            \"test_scenario\": \"basic_entity_creation\",\n            \"created_for\": \"framework_validation\",\n        },\n        \"properties\": {\n            \"numerical_value\": 42,\n            \"boolean_flag\": True,\n            \"nested_object\": {\n                \"level1\": {\n                    \"level2\": \"deep_value\"\n                }\n            },\n            \"array_data\": [\"item1\", \"item2\", \"item3\"]\n        }\n    }\n    \n    return TestMessageGenerator.create_entity_creation_test(\n        tenant_id=tenant_id,\n        test_data=test_data,\n        canonical_type=entity_type,\n        source=\"entity_creation_scenario\",\n        verify_database=verify_all,\n        verify_state_tracking=verify_all,\n        verify_error_handling=verify_all,\n    )\n\n\ndef create_minimal_entity_creation_test(tenant_id: str = \"minimal_test_tenant\") -> Dict[str, Any]:\n    \"\"\"\n    Create a minimal entity creation test with basic data.\n    \n    This test uses minimal data to verify core functionality without\n    complex nested structures or extensive metadata.\n    \n    Args:\n        tenant_id: Tenant ID for the test\n        \n    Returns:\n        Test message for minimal entity creation\n    \"\"\"\n    test_data = {\n        \"id\": \"minimal_test_001\",\n        \"value\": \"simple_value\",\n    }\n    \n    return TestMessageGenerator.create_entity_creation_test(\n        tenant_id=tenant_id,\n        test_data=test_data,\n        canonical_type=\"minimal_entity\",\n        source=\"minimal_scenario\",\n        verify_database=True,\n        verify_state_tracking=True,\n        verify_error_handling=True,\n    )\n\n\ndef create_complex_entity_creation_test(tenant_id: str = \"complex_test_tenant\") -> Dict[str, Any]:\n    \"\"\"\n    Create a complex entity creation test with rich data structures.\n    \n    This test uses complex nested data, arrays, and metadata to verify\n    that the framework handles sophisticated data structures correctly.\n    \n    Args:\n        tenant_id: Tenant ID for the test\n        \n    Returns:\n        Test message for complex entity creation\n    \"\"\"\n    test_data = {\n        \"business_object\": {\n            \"id\": \"complex_001\",\n            \"type\": \"enterprise_entity\",\n            \"relationships\": [\n                {\n                    \"type\": \"parent\",\n                    \"target_id\": \"parent_001\",\n                    \"metadata\": {\"strength\": 0.9}\n                },\n                {\n                    \"type\": \"sibling\",\n                    \"target_id\": \"sibling_001\",\n                    \"metadata\": {\"strength\": 0.7}\n                }\n            ],\n            \"attributes\": {\n                \"financial\": {\n                    \"revenue\": 1000000.50,\n                    \"currency\": \"USD\",\n                    \"fiscal_year\": 2024\n                },\n                \"operational\": {\n                    \"locations\": [\"NYC\", \"LA\", \"Chicago\"],\n                    \"employee_count\": 150,\n                    \"departments\": {\n                        \"engineering\": 75,\n                        \"sales\": 45,\n                        \"operations\": 30\n                    }\n                }\n            },\n            \"tags\": [\"enterprise\", \"technology\", \"b2b\"],\n            \"compliance\": {\n                \"gdpr_compliant\": True,\n                \"sox_compliant\": True,\n                \"last_audit\": \"2024-01-15\"\n            }\n        },\n        \"processing_hints\": {\n            \"priority\": \"high\",\n            \"requires_encryption\": True,\n            \"retention_years\": 7\n        }\n    }\n    \n    return TestMessageGenerator.create_entity_creation_test(\n        tenant_id=tenant_id,\n        test_data=test_data,\n        canonical_type=\"complex_business_entity\",\n        source=\"complex_scenario\",\n        verify_database=True,\n        verify_state_tracking=True,\n        verify_error_handling=True,\n    )\n\n\ndef get_entity_creation_test_suite() -> list:\n    \"\"\"\n    Get a complete test suite for entity creation scenarios.\n    \n    Returns:\n        List of test configurations for entity creation\n    \"\"\"\n    return [\n        {\n            \"name\": \"basic_entity_creation\",\n            \"description\": \"Basic entity creation with standard data\",\n            \"test_generator\": create_basic_entity_creation_test,\n            \"tenant_id\": \"basic_tenant\",\n        },\n        {\n            \"name\": \"minimal_entity_creation\",\n            \"description\": \"Minimal entity creation with simple data\",\n            \"test_generator\": create_minimal_entity_creation_test,\n            \"tenant_id\": \"minimal_tenant\",\n        },\n        {\n            \"name\": \"complex_entity_creation\",\n            \"description\": \"Complex entity creation with rich data structures\",\n            \"test_generator\": create_complex_entity_creation_test,\n            \"tenant_id\": \"complex_tenant\",\n        },\n        {\n            \"name\": \"multi_tenant_entity_creation\",\n            \"description\": \"Entity creation across multiple tenants\",\n            \"test_generator\": lambda: [\n                create_basic_entity_creation_test(tenant_id=\"tenant_1\"),\n                create_basic_entity_creation_test(tenant_id=\"tenant_2\"),\n                create_basic_entity_creation_test(tenant_id=\"tenant_3\"),\n            ],\n        },\n    ]"