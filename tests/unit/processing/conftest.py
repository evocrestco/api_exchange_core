"""
Processing-specific test fixtures.

Provides fixtures for processing service tests, including v2 message creation
and entity factories that work with the database.
"""

import uuid
from datetime import UTC, datetime

import pytest

from api_exchange_core.processors import Message, MessageType
from api_exchange_core.utils.hash_config import HashConfig


@pytest.fixture(scope="function")
def create_test_entity(entity_service, tenant_context):
    """Factory fixture to create real entities for processing service testing."""
    
    def _create(external_id: str = None, canonical_type: str = "test_type", 
               source: str = "test_source", content: dict = None, **kwargs):
        entity_id = entity_service.create_entity(
            external_id=external_id or f"ext-{uuid.uuid4().hex[:8]}",
            canonical_type=canonical_type,
            source=source,
            content=content or {"data": "test"},
            attributes=kwargs.get('attributes', {}),
            hash_config=HashConfig()
        )
        return entity_service.get_entity(entity_id)
    
    return _create


@pytest.fixture(scope="function")
def create_test_message(create_test_entity):
    """Factory fixture to create v2 test messages with real entities."""
    
    def _create(entity=None, payload=None, **kwargs):
        if entity is None:
            entity = create_test_entity()
        
        return Message(
            message_id=kwargs.get('message_id', f"msg-{uuid.uuid4().hex[:8]}"),
            correlation_id=kwargs.get('correlation_id', f"corr-{uuid.uuid4().hex[:8]}"),
            created_at=kwargs.get('created_at', datetime.now(UTC)),
            message_type=kwargs.get('message_type', MessageType.ENTITY_PROCESSING),
            entity=entity,
            payload=payload or {"data": "test", "value": 123},
            retry_count=kwargs.get('retry_count', 0),
            max_retries=kwargs.get('max_retries', 3)
        )
    return _create