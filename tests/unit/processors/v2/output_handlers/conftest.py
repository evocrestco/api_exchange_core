"""
Fixtures for output handler unit tests.

Provides test data for output handlers following the NO MOCKS policy.
Uses simple data classes instead of SQLAlchemy models to avoid database dependencies.
"""

import uuid
from datetime import UTC, datetime
from typing import Any, Dict

import pytest

from src.processors.processing_result import ProcessingResult, ProcessingStatus
from src.processors.v2.message import Message, MessageType


class SimpleEntity:
    """Simple entity class for testing without SQLAlchemy dependencies."""
    
    def __init__(self, id: str, tenant_id: str, external_id: str, 
                 canonical_type: str, source: str, version: int = 1):
        self.id = id
        self.tenant_id = tenant_id
        self.external_id = external_id
        self.canonical_type = canonical_type
        self.source = source
        self.version = version


@pytest.fixture
def test_entity():
    """Create a simple test entity without database dependencies."""
    return SimpleEntity(
        id=f"entity-{uuid.uuid4().hex[:8]}",
        tenant_id="test_tenant",
        external_id=f"ext-{uuid.uuid4().hex[:8]}",
        canonical_type="test_type",
        source="test_source",
        version=1
    )


@pytest.fixture
def test_message(test_entity):
    """Create a test message with entity."""
    return Message(
        message_id=f"msg-{uuid.uuid4().hex[:8]}",
        correlation_id=f"corr-{uuid.uuid4().hex[:8]}",
        created_at=datetime.now(UTC),
        message_type=MessageType.ENTITY_PROCESSING,
        entity=test_entity,
        payload={"data": "test", "value": 123},
        retry_count=0,
        max_retries=3
    )


@pytest.fixture
def test_processing_result():
    """Create a test processing result."""
    return ProcessingResult(
        status=ProcessingStatus.SUCCESS,
        success=True,
        entities_created=["entity-123", "entity-456"],
        entities_updated=["entity-789"],
        processing_metadata={"test": True, "score": 95},
        processor_info={"name": "TestProcessor", "version": "1.0.0"},
        processing_duration_ms=150.5,
        completed_at=datetime.now(UTC)
    )


@pytest.fixture
def create_test_entity():
    """Factory fixture to create test entities with custom attributes."""
    def _create(external_id: str = None, **kwargs) -> SimpleEntity:
        return SimpleEntity(
            id=kwargs.get('id', f"entity-{uuid.uuid4().hex[:8]}"),
            tenant_id=kwargs.get('tenant_id', 'test_tenant'),
            external_id=external_id or f"ext-{uuid.uuid4().hex[:8]}",
            canonical_type=kwargs.get('canonical_type', 'test_type'),
            source=kwargs.get('source', 'test_source'),
            version=kwargs.get('version', 1)
        )
    return _create


@pytest.fixture
def create_test_message(create_test_entity):
    """Factory fixture to create test messages with custom attributes."""
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


@pytest.fixture
def azurite_connection_string():
    """Azurite connection string for testing."""
    return (
        "DefaultEndpointsProtocol=http;"
        "AccountName=devstoreaccount1;"
        "AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;"
        "BlobEndpoint=http://127.0.0.1:10000/devstoreaccount1;"
        "QueueEndpoint=http://127.0.0.1:10001/devstoreaccount1;"
        "TableEndpoint=http://127.0.0.1:10002/devstoreaccount1;"
    )